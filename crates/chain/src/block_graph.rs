//! The [`BlockGraph`] is a multi-tip, monotone implementation of [`ChainOracle`].
//!
//! Compared to [`LocalChain`](crate::local_chain::LocalChain), a `BlockGraph` keeps every
//! observed branch tip simultaneously rather than only the canonical tip. Its
//! [`ChangeSet`] is strictly additive: applying the same changeset twice — or applying two
//! changesets in either order — yields the same graph state.
//!
//! State is held as a `Vec<CheckPoint<D>>` of tips. Shared ancestry between tips is shared
//! through `Arc<CPInner>` automatically — the [`CheckPoint`] linked list is the parent
//! index.
//!
//! The [`ChangeSet`] is split into two maps so the `D` payload is stored exactly once per
//! block regardless of how many branches contain it:
//!
//! - [`ChangeSet::blocks`] is content-addressed by [`BlockHash`]. Bitcoin consensus
//!   guarantees the hash uniquely determines the block and its entire ancestry, so the
//!   height is recoverable from [`ChangeSet::branches`].
//! - [`ChangeSet::branches`] is a per-tip set of [`BlockId`]s. A `BTreeSet<BlockId>`
//!   orders entries lexicographically by `(height, hash)`, i.e. height order on a valid
//!   chain.

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

/// Multi-tip, monotone chain tracker.
///
/// Maintains every observed branch tip in `tips`. Branches that share history share the
/// underlying `Arc<CPInner>` nodes for free.
#[derive(Debug, Clone)]
pub struct BlockGraph<D = BlockHash> {
    /// All known branch tips. Always non-empty. Sorted by `(Reverse(height), hash)` so
    /// `tips[0]` is the best (canonical) tip — max height, lowest hash on ties.
    ///
    /// Invariants: (a) no tip is a strict ancestor of another tip; (b) no two tips share a
    /// [`BlockId`] (same-BlockId tips are merged at absorb time, with the union of their
    /// sparse coverage).
    tips: Vec<CheckPoint<D>>,
}

impl<D> PartialEq for BlockGraph<D> {
    fn eq(&self, other: &Self) -> bool {
        // `tips` is kept sorted by `(Reverse(height), hash)`, so element-wise comparison
        // is a canonical equality check.
        self.tips.len() == other.tips.len()
            && self.tips.iter().zip(&other.tips).all(|(a, b)| a == b)
    }
}

/// Strictly-additive changeset for [`BlockGraph`].
///
/// See the module docs for the rationale on the two-map shape.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct ChangeSet<D = BlockHash> {
    /// Every observed block's payload, content-addressed by [`BlockHash`].
    pub blocks: BTreeMap<BlockHash, D>,
    /// For each tip [`BlockId`], the set of [`BlockId`]s that constitute that branch's
    /// sparse chain. Iterated in `(height, hash)` order (= height order on a valid chain).
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
        // First-write-wins on `blocks`. Key equality (`BlockHash == BlockHash`) implies
        // block equality by consensus; on malformed input we still stay strictly additive.
        for (hash, data) in other.blocks {
            self.blocks.entry(hash).or_insert(data);
        }
        // Outer key union on `branches`; for shared keys, set union of the inner BTreeSet.
        for (tip_id, set) in other.branches {
            self.branches.entry(tip_id).or_default().extend(set);
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
        self.sort_tips();

        let mut delta = ChangeSet::<D>::default();
        for tip in &self.tips {
            let mut post_bids = BTreeSet::<BlockId>::new();
            for cp in tip.iter() {
                if !pre_hashes.contains(&cp.hash()) {
                    delta
                        .blocks
                        .entry(cp.hash())
                        .or_insert_with(|| cp.data());
                }
                post_bids.insert(cp.block_id());
            }
            match pre_branches.get(&tip.block_id()) {
                Some(pre_set) => {
                    let new_bids: BTreeSet<BlockId> =
                        post_bids.difference(pre_set).copied().collect();
                    if !new_bids.is_empty() {
                        delta.branches.insert(tip.block_id(), new_bids);
                    }
                }
                None => {
                    delta.branches.insert(tip.block_id(), post_bids);
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
    /// recovers this graph's full state.
    ///
    /// [`from_changeset`]: Self::from_changeset
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
        };
        let genesis_hash = graph.genesis_hash();

        for bid_set in cs.branches.values() {
            if let Some(cp) = build_branch_lenient(cs, bid_set, genesis_hash) {
                graph.absorb_tip(cp);
            }
        }
        graph.sort_tips();
        Ok(graph)
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
    CheckPoint::from_blocks(union)
        .expect("non-empty union; sparse chains skip prev_blockhash validation")
}

/// Build a [`CheckPoint`] from a branch's [`BlockId`] set, leniently skipping malformed
/// entries.
///
/// - A [`BlockId`] whose `hash` is missing from `cs.blocks` is skipped.
/// - An entry whose stored data does not hash back to the [`BlockId`]'s `hash` is skipped.
/// - A block that fails to [`CheckPoint::push`] (height ordering or `prev_blockhash`
///   mismatch) is skipped. The next candidate at the same height is tried (so a linking
///   block beats a non-linking block at the same height); blocks at higher heights still
///   get a chance to attach to the most recent successful checkpoint.
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
    let mut cp: Option<CheckPoint<D>> = None;
    for bid in bid_set {
        let data = match cs.blocks.get(&bid.hash) {
            Some(d) => d.clone(),
            None => continue, // dangling ref
        };
        if data.to_blockhash() != bid.hash {
            continue; // tampered / corrupted entry
        }
        cp = match cp.take() {
            None => Some(CheckPoint::new(bid.height, data)),
            Some(existing) => match existing.clone().push(bid.height, data) {
                Ok(extended) => Some(extended),
                Err(_) => Some(existing), // push failed; keep prior, skip this entry
            },
        };
    }
    let cp = cp?;
    let bottom = cp.iter().last().expect("CheckPoint is non-empty");
    if bottom.height() != 0 || bottom.hash() != genesis_hash {
        return None;
    }
    Some(cp)
}
