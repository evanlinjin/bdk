//! The [`BlockGraph`] is a multi-tip, monotone implementation of [`ChainOracle`].
//!
//! Compared to [`LocalChain`](crate::local_chain::LocalChain), a `BlockGraph` keeps every
//! observed branch tip simultaneously rather than only the canonical tip. Its
//! [`ChangeSet`] is strictly additive: applying the same changeset twice — or applying two
//! changesets in either order — yields the same graph state.
//!
//! Reachable tips (chains that link back to genesis) live in `Vec<CheckPoint<D>>`. Shared
//! ancestry between tips is shared through `Arc<CPInner>` automatically — the
//! [`CheckPoint`] linked list is the parent index.
//!
//! ## The implicit-anchor `ChangeSet` shape
//!
//! [`ChangeSet`] is split into two maps so the `D` payload is stored exactly once per
//! block regardless of how many branches contain it:
//!
//! - [`ChangeSet::blocks`] is content-addressed by [`BlockHash`]. Bitcoin consensus
//!   guarantees the hash uniquely determines the block and its entire ancestry, so the
//!   height is recoverable from [`ChangeSet::branches`].
//! - [`ChangeSet::branches`] is a per-tip [`Branches`] index mapping each branch tip to
//!   the [`BlockId`]s in that branch's sparse chain. **The smallest [`BlockId`] in each
//!   branch's set is the *anchor*** — either genesis (height 0) or a non-genesis BlockId
//!   that links this fragment to a predecessor branch.
//!
//! `apply_update` emits **anchored deltas**: only BlockIds from the anchor (the highest
//! BlockId in the update's chain that was already known to `self`) up to the new tip,
//! not the full genesis-to-tip set. This keeps the persisted changeset linear in chain
//! length even when a wallet syncs incrementally over many sessions — without requiring
//! the persistor to ever compact.
//!
//! Reconstruction looks up each branch's anchor via [`Branches::containing`] (an
//! `O(log)` reverse index). If a reachable predecessor exists, the fragment is spliced
//! onto its chain and absorbed. Otherwise the fragment goes to a [`QuarantinedFragment`] in
//! [`BlockGraph`] and waits — until a future merge supplies the missing predecessor,
//! at which point [`BlockGraph::release_quarantined`] promotes it. This makes the design
//! tolerant of out-of-order multi-source merges without sacrificing monotonicity or
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

/// A chain-segment observation whose splice point isn't (yet) reachable from any live tip.
///
/// `anchors` is the set of candidate splice points — every [`BlockId`] in the producer's
/// `branches[tip]` set that sits below the fragment's tip. At release time the cascade
/// tries them highest-first and splices at the first one reachable from a live tip.
/// Carrying the full candidate set (rather than a single "smallest" anchor) is robust to
/// multi-source merges that produce overlapping but not-fully-overlapping chains: any
/// future tip that lands on *any* candidate suffices to release the fragment.
///
/// `blocks` is the height → data map for the fragment's stored heights. Heights are
/// `< tip.height + 1`. The anchors' own data may or may not appear in `blocks` — a
/// producer commonly omits anchor data because the consumer already had it.
#[derive(Debug, Clone, PartialEq)]
pub struct QuarantinedFragment<D> {
    /// Candidate splice points — BlockIds at heights strictly below the fragment's tip.
    /// Always non-empty (a fragment with no anchors below its tip would just be a
    /// genesis-rooted CheckPoint and would live in [`BlockGraph::tips`] instead).
    pub anchors: BTreeSet<BlockId>,
    /// Height → data for the fragment's stored heights, including the tip if its data
    /// was available. May overlap `anchors` in height when an anchor's data was shipped.
    pub blocks: BTreeMap<u32, D>,
}

/// Multi-tip, monotone chain tracker.
///
/// Maintains every observed branch tip in `tips`. Branches that share history share the
/// underlying `Arc<CPInner>` nodes for free.
///
/// Observations whose chain does not (yet) reach genesis are kept in `quarantined` until a
/// future merge supplies the missing predecessor. They are then promoted via
/// [`BlockGraph::release_quarantined`].
#[derive(Debug, Clone)]
pub struct BlockGraph<D = BlockHash> {
    /// Reachable tips — chain bottoms reach genesis. Always non-empty. Sorted by
    /// `(Reverse(height), hash)` so `tips[0]` is the best (canonical) tip.
    ///
    /// Invariants: (a) no tip is a strict ancestor of another tip; (b) no two tips share a
    /// [`BlockId`] (same-BlockId tips are merged at absorb time, with the union of their
    /// sparse coverage); (c) every tip's chain reaches genesis at `iter().last()`.
    tips: Vec<CheckPoint<D>>,
    /// Fragments whose anchor is not currently reachable from any live tip. Outer key =
    /// fragment's tip BlockId; value = the [`QuarantinedFragment`] (anchor + height → data).
    /// Promoted to `tips` once a predecessor arrives.
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
///
/// See the module docs for the rationale on the two-map shape.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct ChangeSet<D = BlockHash> {
    /// Every observed block's payload, content-addressed by [`BlockHash`].
    pub blocks: BTreeMap<BlockHash, D>,
    /// Per-tip branch index. The smallest [`BlockId`] in each branch's set is the
    /// *anchor* — either genesis (height 0) or a BlockId that links this fragment to a
    /// predecessor branch.
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
        // First-write-wins on `blocks`. Key equality (`BlockHash == BlockHash`) implies
        // block equality by consensus; on malformed input we still stay strictly additive.
        for (hash, data) in other.blocks {
            self.blocks.entry(hash).or_insert(data);
        }
        // Branch union (the wrapper keeps its reverse index in sync automatically).
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

    /// Iterate over quarantined fragments — observations whose chain does not (yet) reach
    /// genesis. Each entry is `(tip_BlockId, &QuarantinedFragment)`. Quarantined fragments are not
    /// visible through [`tips`], [`tip`], or [`ChainOracle`] until their anchor becomes
    /// reachable and they promote via the cascade.
    ///
    /// [`tips`]: Self::tips
    /// [`tip`]: Self::tip
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

    /// Construct a `BlockGraph` from a complete [`ChangeSet`].
    ///
    /// Fails only with [`MissingGenesisError`] if no `branches` entry contains a
    /// height-0 [`BlockId`] whose hash is present in [`ChangeSet::blocks`] with
    /// self-consistent data. All other malformations in `changeset` — dangling refs,
    /// non-linking `prev_blockhash`, branches whose bottom doesn't reach genesis — are
    /// silently skipped so a corrupted persisted changeset still produces the largest
    /// recoverable graph.
    pub fn from_changeset(changeset: ChangeSet<D>) -> Result<Self, MissingGenesisError> {
        Self::reconstruct(&changeset)
    }

    /// Apply an `update` tip to the graph.
    ///
    /// Returns a delta [`ChangeSet`] describing newly observed blocks and branch entries.
    /// For a brand-new tip the branch entry contains only BlockIds from the **anchor**
    /// (the highest BlockId in `update`'s chain already known to `self`) up to the new
    /// tip — *not* the entire genesis-to-tip set. Reconstruction reattaches at the anchor.
    ///
    /// Fails with [`CannotConnectError`] only when `update` does not descend from this
    /// graph's genesis.
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

    /// Apply a [`ChangeSet`] to the graph.
    ///
    /// The changeset may be a complete or partial (delta) representation. It is merged
    /// onto the current state and the graph is reconstructed. Malformed entries in the
    /// changeset (dangling refs, non-linking `prev_blockhash`, non-genesis-reaching
    /// branches) are silently skipped.
    pub fn apply_changeset(&mut self, changeset: &ChangeSet<D>) {
        let mut combined = self.initial_changeset();
        combined.merge(changeset.clone());
        // `combined` is guaranteed to carry self's genesis, so reconstruction cannot
        // fail with `MissingGenesisError`.
        *self = Self::reconstruct(&combined).expect("self has genesis ⇒ combined has genesis");
    }

    /// Derive a [`ChangeSet`] that, applied to an empty graph (via [`from_changeset`]),
    /// recovers this graph's full state — including quarantined fragments.
    ///
    /// [`from_changeset`]: Self::from_changeset
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

    /// Find a live tip whose chain contains `anchor` at exactly `anchor.height`.
    /// Returns the `CheckPoint` at that point of the matching tip's chain.
    fn find_predecessor_at(&self, anchor: BlockId) -> Option<CheckPoint<D>> {
        self.tips.iter().find_map(|tip| {
            let cp = tip.get(anchor.height)?;
            (cp.block_id() == anchor).then_some(cp)
        })
    }

    /// Promote quarantined fragments whose anchor candidates are now reachable from a
    /// live tip. Each fragment tries its anchors highest-first and splices at the first
    /// reachable one. Loops to a fixed point because one promotion may unlock another.
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

/// Build a [`CheckPoint`] from a branch's [`BlockId`] set, leniently skipping malformed
/// entries.
///
/// - A [`BlockId`] whose `hash` is missing from `cs.blocks` is skipped.
/// - An entry whose stored data does not hash back to the [`BlockId`]'s `hash` is skipped.
/// - At each height, candidates are tried in `(height, hash)` order; the first that
///   [`CheckPoint::push`]es successfully is taken. This realises "If there are two blocks
///   at that height where one has a linked `prev_blockhash` and one does not, ignore the
///   one that does not".
/// - **If no candidate at a given height links, the branch is truncated at the prior
///   height — heights at and above the failure are dropped.** This prevents a
///   non-adjacent `push` from silently skipping `prev_blockhash` validation against a
///   block we just dropped.
/// - The reconstructed checkpoint is only returned if its bottom reaches the graph's
///   genesis hash. Otherwise the branch is dropped entirely.
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
