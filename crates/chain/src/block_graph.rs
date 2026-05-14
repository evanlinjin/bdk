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
//! - [`ChangeSet::branches`]: `BTreeMap<BlockId /*tip*/, BTreeSet<BlockId>>` — a sparse
//!   set of BlockIds per tip. BlockIds strictly below the tip are *candidate anchors*
//!   (potential splice points).
//!
//! [`BlockGraph::apply_update`] emits **anchored deltas**: only BlockIds from the highest
//! splice point already known to `self` up to the new tip, not the full genesis-to-tip
//! set. The persisted changeset stays linear in chain length without compaction.
//!
//! Reconstruction tries each branch's candidate anchors highest-first. If a live tip
//! contains one, the fragment splices onto it. Otherwise the fragment becomes a
//! [`QuarantinedFragment`] and waits — until a future merge lands on any candidate, at
//! which point it's promoted. This tolerates out-of-order multi-source merges without
//! sacrificing monotonicity or order-independence.

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

/// Strictly-additive changeset for [`BlockGraph`].
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct ChangeSet<D = BlockHash> {
    /// Block payloads, content-addressed by [`BlockHash`].
    pub blocks: BTreeMap<BlockHash, D>,
    /// Per-tip BlockId sets. BlockIds below a branch's tip are candidate anchors;
    /// genesis-rooted branches have an anchor at height 0.
    pub branches: BTreeMap<BlockId, BTreeSet<BlockId>>,
}

impl<D> Default for ChangeSet<D> {
    fn default() -> Self {
        Self {
            blocks: BTreeMap::default(),
            branches: BTreeMap::default(),
        }
    }
}

impl<D> Merge for ChangeSet<D> {
    fn merge(&mut self, other: Self) {
        // First-write-wins on `blocks` — consensus says hash determines block.
        for (hash, data) in other.blocks {
            self.blocks.entry(hash).or_insert(data);
        }
        for (tip, set) in other.branches {
            self.branches.entry(tip).or_default().extend(set);
        }
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
                    // Existing tip: emit only newly-added BlockIds + their data. Data is
                    // emitted unconditionally so the delta is self-contained for
                    // out-of-order receivers; `Merge` dedupes data via first-write-wins.
                    let post_bids: BTreeSet<BlockId> =
                        tip.iter().map(|cp| cp.block_id()).collect();
                    let new_bids: BTreeSet<BlockId> =
                        post_bids.difference(pre_set).copied().collect();
                    for bid in &new_bids {
                        let data = tip
                            .get(bid.height)
                            .expect("bid is in tip's chain")
                            .data();
                        delta.blocks.insert(bid.hash, data);
                    }
                    if !new_bids.is_empty() {
                        delta.branches.insert(tip.block_id(), new_bids);
                    }
                }
                None => {
                    // New tip. The delta emits BlockIds in the chain that need to be
                    // attributed to *this* tip in the persisted state. A BlockId is
                    // covered by a surviving pre-tip's branches entry (its own
                    // persisted record) — no need to repeat — UNLESS it's an absorbed
                    // pre-tip's tip BlockId (its branches entry is now orphaned and
                    // reconstruction needs `branches[T]` to reference it so the splice
                    // pulls in its history).
                    let post_tip_ids: BTreeSet<BlockId> =
                        self.tips.iter().map(|cp| cp.block_id()).collect();
                    let absorbed_pre_tip_ids: BTreeSet<BlockId> = pre_branches
                        .keys()
                        .filter(|t| !post_tip_ids.contains(t))
                        .copied()
                        .collect();
                    // Internal chain BlockIds of absorbed pre-tips that aren't tip
                    // BlockIds themselves — those still live under the pre-tip's own
                    // branches entry (persisted state retains it monotonically).
                    let absorbed_internal: BTreeSet<BlockId> = pre_branches
                        .iter()
                        .filter(|(t, _)| !post_tip_ids.contains(t))
                        .flat_map(|(_, set)| set.iter().copied())
                        .filter(|bid| !absorbed_pre_tip_ids.contains(bid))
                        .collect();
                    let anchor = tip
                        .iter()
                        .find(|cp| pre_hashes.contains(&cp.hash()))
                        .map(|cp| cp.block_id())
                        .unwrap_or(BlockId {
                            height: 0,
                            hash: self.genesis_hash(),
                        });
                    let mut delta_bids = BTreeSet::<BlockId>::new();
                    delta_bids.insert(anchor);
                    for cp in tip.iter() {
                        let bid = cp.block_id();
                        if !absorbed_internal.contains(&bid) {
                            delta_bids.insert(bid);
                            // Always include data so the delta is self-contained for
                            // out-of-order receivers (Merge first-write-wins dedupes).
                            delta.blocks.insert(bid.hash, cp.data());
                        }
                    }
                    delta.branches.insert(tip.block_id(), delta_bids);
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
            let mut bids = BTreeSet::<BlockId>::new();
            for cp in tip.iter() {
                cs.blocks.entry(cp.hash()).or_insert_with(|| cp.data());
                bids.insert(cp.block_id());
            }
            cs.branches.insert(tip.block_id(), bids);
        }
        for (tip_id, frag) in &self.quarantined {
            // Emit every candidate anchor BlockId so future reconstructions have the
            // same set of splice points. Anchors whose data isn't in `frag.blocks` live
            // as "ghost" entries — referenced in `branches` but absent from `blocks` —
            // until a future merge supplies them.
            let bids = cs.branches.entry(*tip_id).or_default();
            for anchor in &frag.anchors {
                bids.insert(*anchor);
            }
            for (h, d) in &frag.blocks {
                let hash = d.to_blockhash();
                cs.blocks.entry(hash).or_insert_with(|| d.clone());
                bids.insert(BlockId { height: *h, hash });
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

    /// Expand each branch's `bid_set` with the transitive union of branches entries it
    /// references. If `branches[T]` contains a BlockId that is itself a key in
    /// `branches`, that key's entire entry is merged into `branches[T]` — and the
    /// expansion is repeated until fixed point. This lets reconstruction pick up
    /// sparse history that's "vouched for" by another branch's entry.
    fn expand_transitively(
        branches: &BTreeMap<BlockId, BTreeSet<BlockId>>,
    ) -> BTreeMap<BlockId, BTreeSet<BlockId>> {
        let mut out = branches.clone();
        loop {
            let mut changed = false;
            let keys: Vec<BlockId> = out.keys().copied().collect();
            for tip in keys {
                let set = out.get(&tip).expect("just listed").clone();
                let mut additions = BTreeSet::<BlockId>::new();
                for bid in &set {
                    if bid == &tip {
                        continue;
                    }
                    if let Some(other) = branches.get(bid) {
                        for o in other {
                            if !set.contains(o) {
                                additions.insert(*o);
                            }
                        }
                    }
                }
                if !additions.is_empty() {
                    out.get_mut(&tip).expect("just listed").extend(additions);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        out
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

        // Transitively expand branches: any BlockId in a branch's set that's also a tip
        // key brings in its own branches entry. Required for out-of-order delta merging
        // where one delta records (e.g.) `branches[T] = {anchor}` while a later delta
        // adds detail to `branches[anchor]` — reconstruction needs that detail merged
        // into T's bid_set when splicing.
        let expanded = Self::expand_transitively(&cs.branches);

        // Iterate branches in tip BlockId order — predecessors (lower tips) come first
        // in the no-reorg / append-only case, which matches typical anchor walks.
        for (tip_id, bid_set) in expanded.iter() {
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
            // The splice only counts as a successful release if the resulting chain
            // actually reaches `tip_id` (data for every height in the chain must be
            // available); otherwise we quarantine so a future merge can complete it.
            let reachable = anchors.iter().rev().find_map(|a| {
                graph.find_predecessor_at(*a).map(|pred| (*a, pred))
            });
            let materialized = reachable.and_then(|(anchor, pred)| {
                lenient_extend_above_anchor(pred, cs, bid_set, anchor)
                    .filter(|cp| cp.block_id() == *tip_id)
            });
            match materialized {
                Some(cp) => {
                    graph.absorb_tip(cp);
                }
                None => {
                    // No reachable predecessor — or splice couldn't reach `tip_id`.
                    // Quarantine and keep all candidate anchors so a future merge that
                    // lands on any of them can complete the splice.
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
    /// highest-first and splices at the first reachable one. A fragment is only
    /// released if the spliced chain actually materializes its claimed tip BlockId;
    /// otherwise it stays quarantined (so its splice info isn't lost from the
    /// persisted state).
    fn release_quarantined(&mut self) {
        loop {
            let promotable: Vec<(BlockId, CheckPoint<D>)> = self
                .quarantined
                .iter()
                .filter_map(|(tip_id, frag)| {
                    for anchor in frag.anchors.iter().rev() {
                        let pred = match self.find_predecessor_at(*anchor) {
                            Some(p) => p,
                            None => continue,
                        };
                        let mut cp = pred;
                        for (h, d) in &frag.blocks {
                            if let Ok(extended) = cp.clone().push(*h, d.clone()) {
                                cp = extended;
                            }
                        }
                        if cp.block_id() == *tip_id {
                            return Some((*tip_id, cp));
                        }
                    }
                    None
                })
                .collect();
            if promotable.is_empty() {
                break;
            }
            for (tip_id, cp) in promotable {
                self.quarantined.remove(&tip_id);
                self.absorb_tip(cp);
            }
        }
        // Cleanup: any quarantined frag whose tip BlockId is already in a live tip's
        // chain is redundant (the history is captured elsewhere). Drop them.
        let absorbed_into_tips: Vec<BlockId> = self
            .quarantined
            .keys()
            .filter(|tip_id| {
                self.tips.iter().any(|tip| {
                    tip.get(tip_id.height)
                        .map(|cp| cp.block_id() == **tip_id)
                        .unwrap_or(false)
                })
            })
            .copied()
            .collect();
        for tip_id in absorbed_into_tips {
            self.quarantined.remove(&tip_id);
        }
    }

    /// Integrate `update` into `self.tips`, preserving the no-strict-ancestor invariant.
    fn absorb_tip(&mut self, update: CheckPoint<D>) {
        let old_tips = core::mem::take(&mut self.tips);
        let mut new_tips: Vec<CheckPoint<D>> = Vec::with_capacity(old_tips.len() + 1);
        let mut update_cur = update;
        // Tips for which `t` strictly extends `update_cur` are deferred until after the
        // loop so they pick up `update_cur`'s final enrichment from later iterations.
        let mut deferred_t_extends_u: Vec<CheckPoint<D>> = Vec::new();

        // For each existing tip, find the deepest shared BlockId with `update_cur` and
        // merge their sparse coverages on either side of that point.
        for t in old_tips {
            let shared_h = match deepest_shared_height(&t, &update_cur) {
                Some(h) => h,
                None => {
                    // No common BlockId between t and update_cur — keep t as-is.
                    new_tips.push(t);
                    continue;
                }
            };
            let t_tip = t.block_id();
            let u_tip = update_cur.block_id();

            if t_tip == u_tip || (shared_h == t_tip.height && shared_h < u_tip.height) {
                // Same tip OR `update_cur` strictly extends `t`. Fold t into update_cur.
                // (Both cases produce a single tip with u's BlockId at the top.)
                update_cur = merge_sparse(t, update_cur);
            } else if shared_h == u_tip.height && shared_h < t_tip.height {
                // `t` strictly extends `update_cur`. Defer — we'll merge after the loop
                // so t picks up enrichment that later iterations may add to update_cur.
                deferred_t_extends_u.push(t);
            } else {
                // Diverge above the shared height. Enrich both with the union of
                // at-or-below `shared_h` heights — this is the order-independent rule
                // that makes two tips sharing history carry the same sparse view of it.
                let t_enriched = enrich_at_and_below(t, &update_cur, shared_h);
                let u_enriched = enrich_at_and_below(update_cur.clone(), &t_enriched, shared_h);
                update_cur = u_enriched;
                new_tips.push(t_enriched);
            }
        }
        // Now merge deferred t-extends-u tips with the final update_cur (containing all
        // accumulated enrichments). update_cur is subsumed by each deferred tip's
        // resulting chain, so we don't push it separately.
        let had_deferred = !deferred_t_extends_u.is_empty();
        for t in deferred_t_extends_u {
            new_tips.push(merge_sparse(t, update_cur.clone()));
        }
        if !had_deferred {
            new_tips.push(update_cur);
        }
        self.tips = new_tips;

        // Fixed-point pass: enrich pairs of remaining tips with each other's coverage
        // at-or-below their deepest shared BlockId. Required because the first pass
        // only enriches tips against the incoming update, not against each other.
        loop {
            let mut changed = false;
            for i in 0..self.tips.len() {
                for j in 0..self.tips.len() {
                    if i == j {
                        continue;
                    }
                    let ti = self.tips[i].clone();
                    let tj = self.tips[j].clone();
                    let sh = match deepest_shared_height(&ti, &tj) {
                        Some(h) => h,
                        None => continue,
                    };
                    let new_ti = enrich_at_and_below(ti.clone(), &tj, sh);
                    if new_ti != ti {
                        self.tips[i] = new_ti;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        // Enrichment may have promoted one tip to be a strict ancestor of another;
        // drop ancestors so the no-strict-ancestor invariant holds.
        let mut keep = vec![true; self.tips.len()];
        for i in 0..self.tips.len() {
            for j in 0..self.tips.len() {
                if i == j || !keep[i] {
                    continue;
                }
                let ti = &self.tips[i];
                let tj = &self.tips[j];
                if ti.height() < tj.height() {
                    if let Some(at_ti) = tj.get(ti.height()) {
                        if at_ti.block_id() == ti.block_id() {
                            keep[i] = false;
                        }
                    }
                }
            }
        }
        let kept: Vec<CheckPoint<D>> = core::mem::take(&mut self.tips)
            .into_iter()
            .zip(keep)
            .filter_map(|(t, k)| k.then_some(t))
            .collect();
        self.tips = kept;
        debug_assert!(!self.tips.is_empty(), "BlockGraph must always have a tip");
    }
}

/// The deepest (highest-height) BlockId shared by `t` and `u`. Returns `None` if their
/// chains share no BlockIds.
fn deepest_shared_height<D>(t: &CheckPoint<D>, u: &CheckPoint<D>) -> Option<u32>
where
    D: ToBlockHash + fmt::Debug + Clone,
{
    // `iter()` walks descending, so the first match is the deepest in chain terms.
    for u_cp in u.iter() {
        if let Some(t_cp) = t.get(u_cp.height()) {
            if t_cp.block_id() == u_cp.block_id() {
                return Some(u_cp.height());
            }
        }
    }
    None
}

/// Build a new [`CheckPoint`] with the same tip as `base` but whose sparse coverage at
/// heights `<= ceiling_height` is the union of `base`'s and `other`'s. Heights above
/// `ceiling_height` come from `base` only.
fn enrich_at_and_below<D>(
    base: CheckPoint<D>,
    other: &CheckPoint<D>,
    ceiling_height: u32,
) -> CheckPoint<D>
where
    D: ToBlockHash + fmt::Debug + Clone,
{
    let mut union: BTreeMap<u32, D> = BTreeMap::new();
    for cp in base.iter() {
        union.insert(cp.height(), cp.data());
    }
    for cp in other.iter() {
        if cp.height() <= ceiling_height {
            union.entry(cp.height()).or_insert_with(|| cp.data());
        }
    }
    let mut iter = union.into_iter();
    let (h0, d0) = iter.next().expect("base is non-empty");
    let mut cp = CheckPoint::new(h0, d0);
    for (h, d) in iter {
        cp = match cp.clone().push(h, d) {
            Ok(extended) => extended,
            Err(_) => cp,
        };
    }
    cp
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

/// Splice a branch's `bid_set` onto `pred`. Builds a chain that's the union of pred's
/// heights and bid_set's heights (where data is available), rebuilt via `push`.
///
/// `_anchor` is the splice point chosen by the caller — retained for signature
/// compatibility. The union-based build handles BlockIds both above and below the anchor:
/// pred's data wins on collision; non-linking entries (push failure) are skipped.
fn lenient_extend_above_anchor<D>(
    pred: CheckPoint<D>,
    cs: &ChangeSet<D>,
    bid_set: &BTreeSet<BlockId>,
    _anchor: BlockId,
) -> Option<CheckPoint<D>>
where
    D: ToBlockHash + fmt::Debug + Clone,
{
    let mut union: BTreeMap<u32, D> = BTreeMap::new();
    for cp in pred.iter() {
        union.insert(cp.height(), cp.data());
    }
    for bid in bid_set {
        let data = match cs.blocks.get(&bid.hash) {
            Some(d) => d.clone(),
            None => continue,
        };
        if data.to_blockhash() != bid.hash {
            continue;
        }
        union.entry(bid.height).or_insert(data);
    }
    let mut iter = union.into_iter();
    let (h0, d0) = iter.next().expect("pred is non-empty");
    let mut cp = CheckPoint::new(h0, d0);
    for (h, d) in iter {
        cp = match cp.clone().push(h, d) {
            Ok(extended) => extended,
            Err(_) => cp,
        };
    }
    Some(cp)
}
