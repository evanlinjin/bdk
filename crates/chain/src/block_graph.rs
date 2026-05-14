//! Multi-tip, monotone implementation of [`ChainOracle`].
//!
//! Unlike [`LocalChain`](crate::local_chain::LocalChain), [`BlockGraph`] keeps every
//! observed branch tip rather than only the canonical one. Its [`ChangeSet`] is strictly
//! additive: applying the same changeset twice — or two changesets in either order —
//! yields the same state.
//!
//! Reachable tips live in `Vec<CheckPoint<D>>`; shared ancestry is shared through
//! `Arc<CPInner>` (the linked list *is* the parent index).
//!
//! ## ChangeSet shape
//!
//! Split into two maps so each block's `D` is stored exactly once:
//!
//! - [`ChangeSet::blocks`]: content-addressed `BlockHash → D`.
//! - [`ChangeSet::branches`]: per-tip [`Branches`] index — a `BlockId` set per tip.
//!   BlockIds strictly below the tip are *candidate anchors* (potential splice points).
//!
//! [`BlockGraph::apply_update`] emits **anchored deltas**: only BlockIds from the highest
//! splice point already known to `self` up to the new tip, not the full genesis-to-tip
//! set. The persisted changeset stays linear in chain length without compaction.
//!
//! Reconstruction tries each branch's candidate anchors highest-first via
//! [`Branches::containing`] (an `O(log)` reverse index). If a live tip contains one, the
//! fragment splices onto it. Otherwise the fragment becomes a [`QuarantinedFragment`] and
//! waits — until a future merge lands on any candidate, at which point it's promoted.
//! This tolerates out-of-order multi-source merges without sacrificing monotonicity or
//! order-independence.

use alloc::vec::Vec;
use core::cmp::Reverse;
use core::convert::Infallible;
use core::fmt;
use core::ops::RangeBounds;

use crate::collections::{BTreeMap, BTreeSet};
use crate::{BlockId, ChainOracle, Merge};
pub use bdk_core::{CheckPoint, CheckPointIter};
use bdk_core::ToBlockHash;
use bitcoin::BlockHash;

pub use crate::local_chain::{CannotConnectError, MissingGenesisError};

/// A chain segment whose splice point isn't (yet) reachable from any live tip.
///
/// Released when any [`anchor`](Self::anchors) becomes reachable; the cascade tries them
/// highest-first.
#[derive(Debug, Clone, PartialEq)]
pub struct QuarantinedFragment<D> {
    /// Candidate splice points — BlockIds strictly below the fragment's tip. Non-empty.
    pub anchors: BTreeSet<BlockId>,
    /// Height → data for stored heights. Anchor data may be absent (producers often omit
    /// it).
    pub blocks: BTreeMap<u32, D>,
}

/// Multi-tip, monotone chain tracker. See [module docs](self) for the full design.
#[derive(Debug, Clone)]
pub struct BlockGraph<D = BlockHash> {
    /// Reachable tips, sorted `(Reverse(height), hash)` so `tips[0]` is canonical.
    /// Non-empty. No tip is an ancestor of another, and tip BlockIds are unique.
    tips: Vec<CheckPoint<D>>,
    /// Fragments awaiting a reachable anchor. Keyed by the fragment's tip BlockId.
    quarantined: BTreeMap<BlockId, QuarantinedFragment<D>>,
}

impl<D> PartialEq for BlockGraph<D>
where
    D: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        // `tips` is kept sorted by `(Reverse(height), hash)`, so element-wise comparison
        // is a canonical equality check.
        self.tips.len() == other.tips.len()
            && self.tips.iter().zip(&other.tips).all(|(a, b)| a == b)
            && self.quarantined == other.quarantined
    }
}

pub use crate::branches::Branches;

/// Strictly-additive changeset for [`BlockGraph`].
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct ChangeSet<D = BlockHash> {
    /// Block payloads, content-addressed by [`BlockHash`].
    pub blocks: BTreeMap<BlockHash, D>,
    /// Per-tip branch index. BlockIds below a branch's tip are candidate anchors;
    /// genesis-rooted branches have an anchor at height 0.
    pub branches: Branches,
}

impl<D> Default for ChangeSet<D> {
    fn default() -> Self {
        Self {
            blocks: BTreeMap::default(),
            branches: Branches::default(),
        }
    }
}

impl<D> Merge for ChangeSet<D> {
    fn merge(&mut self, other: Self) {
        // First-write-wins on `blocks` — consensus says hash determines block.
        for (hash, data) in other.blocks {
            self.blocks.entry(hash).or_insert(data);
        }
        self.branches.merge(other.branches);
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty() && self.branches.is_empty()
    }
}

// ---- Methods for any `D` ----

impl<D> BlockGraph<D> {
    /// Get the best (canonical) tip.
    pub fn tip(&self) -> CheckPoint<D> {
        self.tips[0].clone()
    }

    /// Iterate over every known tip in best-first order.
    pub fn tips(&self) -> impl Iterator<Item = &CheckPoint<D>> {
        self.tips.iter()
    }

    /// Number of tips currently retained.
    pub fn tip_count(&self) -> usize {
        self.tips.len()
    }

    /// Get the genesis hash.
    pub fn genesis_hash(&self) -> BlockHash {
        self.tips[0]
            .iter()
            .last()
            .expect("CheckPoint is non-empty")
            .hash()
    }

    /// Iterate checkpoints from the best tip in descending height.
    pub fn iter_checkpoints(&self) -> CheckPointIter<D> {
        self.tips[0].iter()
    }

    /// Get the checkpoint at `height` on the best tip.
    pub fn get(&self, height: u32) -> Option<CheckPoint<D>> {
        self.tips[0].get(height)
    }

    /// Iterate checkpoints over a height range on the best tip.
    pub fn range<R>(&self, range: R) -> impl Iterator<Item = CheckPoint<D>>
    where
        R: RangeBounds<u32>,
    {
        self.tips[0].range(range)
    }

    /// Iterate quarantined fragments. Not visible through [`tips`](Self::tips) or
    /// [`ChainOracle`] until released.
    pub fn quarantined(&self) -> impl Iterator<Item = (&BlockId, &QuarantinedFragment<D>)> {
        self.quarantined.iter()
    }

    /// Number of quarantined fragments.
    pub fn quarantined_count(&self) -> usize {
        self.quarantined.len()
    }
}

impl<D> ChainOracle for BlockGraph<D> {
    type Error = Infallible;

    fn is_block_in_chain(
        &self,
        block: BlockId,
        chain_tip: BlockId,
    ) -> Result<Option<bool>, Self::Error> {
        let tip = match self.tips.iter().find(|t| t.block_id() == chain_tip) {
            Some(t) => t,
            None => return Ok(None),
        };
        match tip.get(block.height) {
            Some(cp) => Ok(Some(cp.hash() == block.hash)),
            None => Ok(None),
        }
    }

    fn get_chain_tip(&self) -> Result<BlockId, Self::Error> {
        Ok(self.tips[0].block_id())
    }
}

// ---- Methods where `D: ToBlockHash` ----

impl<D> BlockGraph<D>
where
    D: ToBlockHash + fmt::Debug + Clone,
{
    /// Construct a `BlockGraph` with a single tip at genesis.
    pub fn from_genesis(data: D) -> (Self, ChangeSet<D>) {
        let cp = CheckPoint::new(0, data);
        let graph = Self {
            tips: alloc::vec![cp],
            quarantined: BTreeMap::new(),
        };
        let changeset = graph.initial_changeset();
        (graph, changeset)
    }

    /// Construct a `BlockGraph` from a single [`CheckPoint`] tip.
    pub fn from_tip(tip: CheckPoint<D>) -> Result<Self, MissingGenesisError> {
        let bottom = tip.iter().last().expect("CheckPoint is non-empty");
        if bottom.height() != 0 {
            return Err(MissingGenesisError);
        }
        Ok(Self {
            tips: alloc::vec![tip],
            quarantined: BTreeMap::new(),
        })
    }

    /// Reconstruct a `BlockGraph` from a complete [`ChangeSet`].
    ///
    /// Fails with [`MissingGenesisError`] iff no `branches` entry has a height-0
    /// [`BlockId`] with self-consistent data in [`ChangeSet::blocks`]. Other
    /// malformations (dangling refs, non-linking `prev_blockhash`, etc.) are silently
    /// skipped so a corrupted changeset still yields the largest recoverable graph.
    pub fn from_changeset(changeset: ChangeSet<D>) -> Result<Self, MissingGenesisError> {
        Self::reconstruct(&changeset)
    }

    /// Apply an `update` tip and return an anchored delta [`ChangeSet`].
    ///
    /// For a new tip the delta's branch entry covers only the BlockIds from the anchor
    /// (the highest BlockId in `update`'s chain already known to `self`) to the new tip
    /// — keeping persisted state linear in chain length.
    ///
    /// Fails with [`CannotConnectError`] iff `update` doesn't descend from genesis.
    pub fn apply_update(
        &mut self,
        update: CheckPoint<D>,
    ) -> Result<ChangeSet<D>, CannotConnectError> {
        let bottom = update.iter().last().expect("CheckPoint is non-empty");
        if bottom.height() != 0 || bottom.hash() != self.genesis_hash() {
            return Err(CannotConnectError {
                try_include_height: 0,
            });
        }
        // Cheap pre-snapshot: hashes + per-tip BlockId sets, no `D` clones.
        let (pre_hashes, pre_branches) = self.snapshot_indexes();
        self.absorb_tip(update);
        // New tip(s) may unlock previously-quarantined fragments.
        self.release_quarantined();
        self.sort_tips();

        let mut delta = ChangeSet::<D>::default();
        for tip in &self.tips {
            match pre_branches.get(&tip.block_id()) {
                Some(pre_set) => {
                    // Existing tip: emit only newly-added BlockIds + their data.
                    let post_bids: BTreeSet<BlockId> =
                        tip.iter().map(|cp| cp.block_id()).collect();
                    let new_bids: BTreeSet<BlockId> =
                        post_bids.difference(pre_set).copied().collect();
                    for bid in &new_bids {
                        if !pre_hashes.contains(&bid.hash) {
                            let data = tip
                                .get(bid.height)
                                .expect("bid is in tip's chain")
                                .data();
                            delta.blocks.insert(bid.hash, data);
                        }
                    }
                    delta.branches.extend_branch(tip.block_id(), new_bids);
                }
                None => {
                    // New tip: find the anchor — highest BlockId in tip.iter() whose
                    // hash is in `pre_hashes`. Emit `{anchor, …, tip}` and the data for
                    // every block at-or-above the anchor that's not in `pre_hashes`.
                    let anchor = tip
                        .iter()
                        .find(|cp| pre_hashes.contains(&cp.hash()))
                        .map(|cp| cp.block_id())
                        .unwrap_or(BlockId {
                            height: 0,
                            hash: self.genesis_hash(),
                        });
                    let mut delta_bids = BTreeSet::<BlockId>::new();
                    for cp in tip.iter() {
                        if cp.height() < anchor.height {
                            break;
                        }
                        delta_bids.insert(cp.block_id());
                        if !pre_hashes.contains(&cp.hash()) {
                            delta.blocks.insert(cp.hash(), cp.data());
                        }
                    }
                    delta.branches.extend_branch(tip.block_id(), delta_bids);
                }
            }
        }
        Ok(delta)
    }

    /// Merge a [`ChangeSet`] (complete or delta) into the graph. Malformed entries are
    /// silently skipped (see [`from_changeset`](Self::from_changeset)).
    pub fn apply_changeset(&mut self, changeset: &ChangeSet<D>) {
        let mut combined = self.initial_changeset();
        combined.merge(changeset.clone());
        *self = Self::reconstruct(&combined).expect("self has genesis ⇒ combined does");
    }

    /// Snapshot the full state as a [`ChangeSet`] — recovers via
    /// [`from_changeset`](Self::from_changeset). Quarantined fragments included.
    pub fn initial_changeset(&self) -> ChangeSet<D> {
        let mut cs = ChangeSet::<D>::default();
        for tip in &self.tips {
            for cp in tip.iter() {
                cs.blocks.entry(cp.hash()).or_insert_with(|| cp.data());
            }
            cs.branches
                .extend_branch(tip.block_id(), tip.iter().map(|cp| cp.block_id()));
        }
        for (tip_id, frag) in &self.quarantined {
            // Emit every candidate anchor BlockId so future reconstructions have the
            // same set of splice points to try. Anchors whose data is not in
            // `frag.blocks` survive as "ghost" entries in `branches` — present as
            // BlockIds, absent from `blocks` — to be resolved when a future merge
            // supplies either the anchor's chain or another candidate's chain.
            for anchor in &frag.anchors {
                cs.branches.insert(*tip_id, *anchor);
            }
            for (h, d) in &frag.blocks {
                let hash = d.to_blockhash();
                cs.blocks.entry(hash).or_insert_with(|| d.clone());
                cs.branches.insert(
                    *tip_id,
                    BlockId {
                        height: *h,
                        hash,
                    },
                );
            }
        }
        cs
    }

    /// Cheap pre-mutation snapshot used to compute apply_update's delta without cloning `D`.
    fn snapshot_indexes(&self) -> (BTreeSet<BlockHash>, BTreeMap<BlockId, BTreeSet<BlockId>>) {
        let mut hashes = BTreeSet::<BlockHash>::new();
        let mut branches = BTreeMap::<BlockId, BTreeSet<BlockId>>::new();
        for tip in &self.tips {
            let mut bids = BTreeSet::<BlockId>::new();
            for cp in tip.iter() {
                hashes.insert(cp.hash());
                bids.insert(cp.block_id());
            }
            branches.insert(tip.block_id(), bids);
        }
        (hashes, branches)
    }

    /// Sort `tips` by `(Reverse(height), hash)` so `tips[0]` is the best tip and the
    /// vector is in a canonical order for equality.
    fn sort_tips(&mut self) {
        self.tips
            .sort_by_key(|cp| (Reverse(cp.height()), cp.hash()));
    }

    fn reconstruct(cs: &ChangeSet<D>) -> Result<Self, MissingGenesisError> {
        // Find a usable genesis: a height-0 BlockId whose hash is present in `blocks`
        // and whose data hashes back to that key (self-consistent).
        let genesis_data = cs
            .branches
            .values()
            .flat_map(|s| s.iter().filter(|b| b.height == 0).copied())
            .find_map(|bid| {
                let data = cs.blocks.get(&bid.hash)?.clone();
                (data.to_blockhash() == bid.hash).then_some(data)
            })
            .ok_or(MissingGenesisError)?;

        let mut graph = Self {
            tips: alloc::vec![CheckPoint::new(0, genesis_data)],
            quarantined: BTreeMap::new(),
        };
        let genesis_hash = graph.genesis_hash();

        // Iterate branches in tip BlockId order — predecessors (lower tips) come first
        // in the no-reorg / append-only case, which matches typical anchor walks.
        for (tip_id, bid_set) in cs.branches.iter() {
            // Candidate anchors: every BlockId in `bid_set` strictly below the tip.
            let anchors: BTreeSet<BlockId> = bid_set
                .iter()
                .filter(|b| b.height < tip_id.height)
                .copied()
                .collect();

            // Genesis-rooted branch: smallest anchor is at height 0, build standalone.
            if anchors.iter().next().is_some_and(|a| a.height == 0) {
                if let Some(cp) = build_branch_lenient(cs, bid_set, genesis_hash) {
                    graph.absorb_tip(cp);
                }
                continue;
            }
            if anchors.is_empty() {
                // No anchor below the tip — degenerate: tip-as-its-own-anchor isn't a
                // valid splice. Skip (or treat tip == genesis as genesis-rooted above).
                continue;
            }

            // Non-genesis: try to splice at the highest reachable candidate anchor.
            let reachable = anchors.iter().rev().find_map(|a| {
                graph.find_predecessor_at(*a).map(|pred| (*a, pred))
            });
            match reachable {
                Some((anchor, pred)) => {
                    if let Some(cp) = lenient_extend_above_anchor(pred, cs, bid_set, anchor) {
                        graph.absorb_tip(cp);
                    }
                }
                None => {
                    // No reachable predecessor yet — quarantine. We keep all anchor
                    // candidates so a future merge that lands on *any* of them can release
                    // the fragment. The anchors' data may not be present in `cs.blocks`
                    // (common in deltas where the producer's pre-state already had it);
                    // anchors are stored as BlockIds either way.
                    let blocks: BTreeMap<u32, D> = bid_set
                        .iter()
                        .filter_map(|bid| {
                            let data = cs.blocks.get(&bid.hash)?.clone();
                            (data.to_blockhash() == bid.hash).then_some((bid.height, data))
                        })
                        .collect();
                    graph
                        .quarantined
                        .insert(*tip_id, QuarantinedFragment { anchors, blocks });
                }
            }
        }
        graph.release_quarantined();
        graph.sort_tips();
        Ok(graph)
    }

    /// `CheckPoint` at `anchor` if any live tip's chain contains it.
    fn find_predecessor_at(&self, anchor: BlockId) -> Option<CheckPoint<D>> {
        self.tips.iter().find_map(|tip| {
            let cp = tip.get(anchor.height)?;
            (cp.block_id() == anchor).then_some(cp)
        })
    }

    /// Fixed-point promotion of quarantined fragments — each tries its anchors
    /// highest-first and splices at the first reachable one.
    fn release_quarantined(&mut self) {
        loop {
            let promotable: Vec<(BlockId, BlockId)> = self
                .quarantined
                .iter()
                .filter_map(|(tip_id, frag)| {
                    frag.anchors
                        .iter()
                        .rev()
                        .find_map(|a| self.find_predecessor_at(*a).map(|_| (*tip_id, *a)))
                })
                .collect();
            if promotable.is_empty() {
                return;
            }
            for (tip_id, anchor) in promotable {
                let frag = self.quarantined.remove(&tip_id).expect("just listed");
                let pred = self
                    .find_predecessor_at(anchor)
                    .expect("just verified reachable");
                // Lenient-push every block in the fragment. `push`'s height check
                // naturally skips entries at or below the chosen anchor's height, so we
                // don't need to pre-filter — and any spurious low-height block silently
                // drops away.
                let mut cp = pred;
                for (h, d) in &frag.blocks {
                    cp = match cp.clone().push(*h, d.clone()) {
                        Ok(extended) => extended,
                        Err(_) => cp,
                    };
                }
                self.absorb_tip(cp);
            }
        }
    }

    /// Integrate `update` into `self.tips`, preserving the no-strict-ancestor invariant.
    fn absorb_tip(&mut self, update: CheckPoint<D>) {
        let old_tips = core::mem::take(&mut self.tips);
        let mut new_tips: Vec<CheckPoint<D>> = Vec::with_capacity(old_tips.len() + 1);
        let mut absorbed = false;
        let mut update_dropped = false;
        let mut update_cur = update;

        for t in old_tips {
            match relate(&t, &update_cur) {
                Relation::Equal => {
                    new_tips.push(t);
                    absorbed = true;
                }
                Relation::SameTipIdMerge => {
                    let merged = merge_sparse(t, update_cur.clone());
                    update_cur = merged.clone();
                    new_tips.push(merged);
                    absorbed = true;
                }
                Relation::UpdateExtendsT => {
                    // t is a strict ancestor of update — drop t.
                }
                Relation::TExtendsUpdate => {
                    new_tips.push(t);
                    update_dropped = true;
                }
                Relation::Diverge => {
                    new_tips.push(t);
                }
            }
        }
        if !absorbed && !update_dropped {
            new_tips.push(update_cur);
        }
        self.tips = new_tips;
        debug_assert!(!self.tips.is_empty(), "BlockGraph must always have a tip");
    }
}

#[derive(Debug)]
enum Relation {
    Equal,
    SameTipIdMerge,
    UpdateExtendsT,
    TExtendsUpdate,
    Diverge,
}

fn relate<D>(t: &CheckPoint<D>, u: &CheckPoint<D>) -> Relation
where
    D: ToBlockHash + fmt::Debug + Clone,
{
    if t.eq_ptr(u) {
        return Relation::Equal;
    }
    let tb = t.block_id();
    let ub = u.block_id();
    if tb == ub {
        return Relation::SameTipIdMerge;
    }
    if ub.height > tb.height {
        if let Some(at_t) = u.get(tb.height) {
            if at_t.block_id() == tb {
                return Relation::UpdateExtendsT;
            }
            return Relation::Diverge;
        }
    } else if tb.height > ub.height {
        if let Some(at_u) = t.get(ub.height) {
            if at_u.block_id() == ub {
                return Relation::TExtendsUpdate;
            }
            return Relation::Diverge;
        }
    }
    Relation::Diverge
}

fn merge_sparse<D>(base: CheckPoint<D>, other: CheckPoint<D>) -> CheckPoint<D>
where
    D: ToBlockHash + fmt::Debug + Clone,
{
    // Build (height -> data) union, keeping base's data on collisions.
    let mut union: BTreeMap<u32, D> = BTreeMap::new();
    for cp in base.iter() {
        union.insert(cp.height(), cp.data());
    }
    for cp in other.iter() {
        union.entry(cp.height()).or_insert_with(|| cp.data());
    }
    // Build incrementally via `push`, skipping non-linking entries. This preserves the
    // shared tip (the tip's prev was already validated in `base`'s construction) and
    // avoids panicking if a caller fed in an internally inconsistent `CheckPoint`.
    let mut iter = union.into_iter();
    let (h0, d0) = iter.next().expect("union is non-empty: base contains at least the tip");
    let mut cp = CheckPoint::new(h0, d0);
    for (h, d) in iter {
        cp = match cp.clone().push(h, d) {
            Ok(extended) => extended,
            Err(_) => cp, // non-linking entry — skip, keep prior cp
        };
    }
    cp
}

/// Build a genesis-rooted [`CheckPoint`] from a branch's [`BlockId`] set, skipping
/// malformed entries.
///
/// Lenience rules:
/// - Dangling refs and data-hash mismatches are skipped.
/// - At each height, candidates are tried in `(height, hash)` order; first to
///   [`CheckPoint::push`] wins (resolves "non-linking sibling" ambiguity).
/// - **If no candidate at height `h` links, the branch is truncated below `h`** —
///   otherwise a non-adjacent push could skip `prev_blockhash` validation against a
///   block we just dropped.
/// - Returns `None` if the bottom doesn't reach genesis.
fn build_branch_lenient<D>(
    cs: &ChangeSet<D>,
    bid_set: &BTreeSet<BlockId>,
    genesis_hash: BlockHash,
) -> Option<CheckPoint<D>>
where
    D: ToBlockHash + fmt::Debug + Clone,
{
    // Group BlockIds by height so we can detect "no candidate at this height linked".
    let mut by_height: BTreeMap<u32, Vec<&BlockId>> = BTreeMap::new();
    for bid in bid_set {
        by_height.entry(bid.height).or_default().push(bid);
    }

    let mut cp: Option<CheckPoint<D>> = None;
    'heights: for (h, candidates) in by_height {
        for bid in candidates {
            let data = match cs.blocks.get(&bid.hash) {
                Some(d) => d.clone(),
                None => continue, // dangling ref
            };
            if data.to_blockhash() != bid.hash {
                continue; // tampered / corrupted entry
            }
            let candidate_cp = match &cp {
                None => CheckPoint::new(h, data),
                Some(existing) => match existing.clone().push(h, data) {
                    Ok(extended) => extended,
                    Err(_) => continue, // push failed; try next candidate at same height
                },
            };
            cp = Some(candidate_cp);
            continue 'heights;
        }
        // No candidate at `h` linked successfully ⇒ truncate the branch here.
        break;
    }

    let cp = cp?;
    let bottom = cp.iter().last().expect("CheckPoint is non-empty");
    if bottom.height() != 0 || bottom.hash() != genesis_hash {
        return None;
    }
    Some(cp)
}

/// Extend a predecessor [`CheckPoint`] at `anchor` with the BlockIds in `bid_set` that
/// are strictly above the anchor, leniently skipping malformed entries (dangling refs,
/// data-hash mismatch, non-linking pushes).
///
/// Mirrors the lenience rules of [`build_branch_lenient`] but starts from a known
/// predecessor instead of building from scratch.
fn lenient_extend_above_anchor<D>(
    pred: CheckPoint<D>,
    cs: &ChangeSet<D>,
    bid_set: &BTreeSet<BlockId>,
    anchor: BlockId,
) -> Option<CheckPoint<D>>
where
    D: ToBlockHash + fmt::Debug + Clone,
{
    // Group entries strictly above the anchor by height.
    let mut by_height: BTreeMap<u32, Vec<&BlockId>> = BTreeMap::new();
    for bid in bid_set {
        if bid.height > anchor.height {
            by_height.entry(bid.height).or_default().push(bid);
        }
    }

    let mut cp = pred;
    'heights: for (h, candidates) in by_height {
        for bid in candidates {
            let data = match cs.blocks.get(&bid.hash) {
                Some(d) => d.clone(),
                None => continue,
            };
            if data.to_blockhash() != bid.hash {
                continue;
            }
            match cp.clone().push(h, data) {
                Ok(extended) => {
                    cp = extended;
                    continue 'heights;
                }
                Err(_) => continue,
            }
        }
        // No candidate at `h` linked successfully ⇒ stop extending here.
        break;
    }
    Some(cp)
}
