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
    /// All known branch tips. Always non-empty.
    ///
    /// Invariants: (a) no tip is a strict ancestor of another tip; (b) no two tips share a
    /// [`BlockId`] (same-BlockId tips are merged at absorb time, with the union of their
    /// sparse coverage).
    tips: Vec<CheckPoint<D>>,
    /// Index into `tips` of the best (canonical) tip by `(max height, lowest hash)`.
    best: usize,
}

impl<D> PartialEq for BlockGraph<D> {
    fn eq(&self, other: &Self) -> bool {
        if self.tips.len() != other.tips.len() {
            return false;
        }
        let mut a: Vec<&CheckPoint<D>> = self.tips.iter().collect();
        let mut b: Vec<&CheckPoint<D>> = other.tips.iter().collect();
        a.sort_by_key(|cp| cp.block_id());
        b.sort_by_key(|cp| cp.block_id());
        a.into_iter().zip(b).all(|(x, y)| x == y)
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

/// Error returned when reconstructing or applying a [`ChangeSet`] to a [`BlockGraph`].
#[derive(Clone, Debug, PartialEq)]
pub enum ApplyChangeSetError {
    /// The changeset has no genesis block — no `branches` entry contains a [`BlockId`]
    /// at height 0.
    MissingGenesis,
    /// A branch in `branches` references a [`BlockId`] whose `hash` is not present in
    /// [`ChangeSet::blocks`].
    DanglingBranchRef {
        /// The tip whose branch contained the dangling reference.
        tip: BlockId,
        /// The missing [`BlockId`].
        missing: BlockId,
    },
    /// A branch's blocks don't link via `prev_blockhash` (only fires for `D` types that
    /// implement [`ToBlockHash::prev_blockhash`] non-trivially, e.g. `Header`).
    PrevBlockhashMismatch {
        /// The last successfully extended checkpoint before the mismatch.
        expected: BlockId,
    },
    /// The changeset's genesis hash conflicts with the graph's existing genesis.
    GenesisMismatch {
        /// The graph's current genesis hash.
        existing: BlockHash,
        /// The genesis hash claimed by the changeset.
        got: BlockHash,
    },
}

impl fmt::Display for ApplyChangeSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingGenesis => write!(f, "genesis block is missing"),
            Self::DanglingBranchRef { tip, missing } => write!(
                f,
                "branch {tip:?} references block {missing:?} which is not in `blocks`",
            ),
            Self::PrevBlockhashMismatch { expected } => {
                write!(f, "`prev_blockhash` does not match block {expected:?}")
            }
            Self::GenesisMismatch { existing, got } => write!(
                f,
                "genesis mismatch: existing={existing}, got={got}",
            ),
        }
    }
}

impl core::error::Error for ApplyChangeSetError {}

// ---- Methods for any `D` ----

impl<D> BlockGraph<D> {
    /// Get the best (canonical) tip.
    pub fn tip(&self) -> CheckPoint<D> {
        self.tips[self.best].clone()
    }

    /// Iterate over every known tip (including non-canonical forks).
    pub fn tips(&self) -> impl Iterator<Item = &CheckPoint<D>> {
        self.tips.iter()
    }

    /// Number of tips currently retained.
    pub fn tip_count(&self) -> usize {
        self.tips.len()
    }

    /// Get the genesis hash.
    pub fn genesis_hash(&self) -> BlockHash {
        self.tips[self.best]
            .iter()
            .last()
            .expect("CheckPoint is non-empty")
            .hash()
    }

    /// Iterate checkpoints from the best tip in descending height.
    pub fn iter_checkpoints(&self) -> CheckPointIter<D> {
        self.tips[self.best].iter()
    }

    /// Get the checkpoint at `height` on the best tip.
    pub fn get(&self, height: u32) -> Option<CheckPoint<D>> {
        self.tips[self.best].get(height)
    }

    /// Iterate checkpoints over a height range on the best tip.
    pub fn range<R>(&self, range: R) -> impl Iterator<Item = CheckPoint<D>>
    where
        R: RangeBounds<u32>,
    {
        self.tips[self.best].range(range)
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
        Ok(self.tips[self.best].block_id())
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
            best: 0,
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
            best: 0,
        })
    }

    /// Construct a `BlockGraph` from a complete [`ChangeSet`].
    ///
    /// The changeset must contain genesis (a height-0 [`BlockId`] in some branch, with the
    /// corresponding data in `blocks`).
    pub fn from_changeset(changeset: ChangeSet<D>) -> Result<Self, ApplyChangeSetError> {
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
        let pre = self.initial_changeset();
        self.absorb_tip(update);
        self.best = pick_best(&self.tips);
        let post = self.initial_changeset();
        Ok(delta(&pre, post))
    }

    /// Apply a [`ChangeSet`] to the graph.
    ///
    /// The changeset may be a complete or partial (delta) representation. It is merged
    /// onto the current state, then the graph is reconstructed.
    pub fn apply_changeset(
        &mut self,
        changeset: &ChangeSet<D>,
    ) -> Result<(), ApplyChangeSetError> {
        let mut combined = self.initial_changeset();
        combined.merge(changeset.clone());
        *self = Self::reconstruct(&combined)?;
        Ok(())
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

    fn reconstruct(cs: &ChangeSet<D>) -> Result<Self, ApplyChangeSetError> {
        // Locate genesis from any branch entry.
        let genesis_bid = cs
            .branches
            .values()
            .flat_map(|s| s.iter().filter(|b| b.height == 0).copied())
            .next()
            .ok_or(ApplyChangeSetError::MissingGenesis)?;
        let genesis_data = cs
            .blocks
            .get(&genesis_bid.hash)
            .cloned()
            .ok_or(ApplyChangeSetError::DanglingBranchRef {
                tip: genesis_bid,
                missing: genesis_bid,
            })?;
        if genesis_data.to_blockhash() != genesis_bid.hash {
            return Err(ApplyChangeSetError::GenesisMismatch {
                existing: genesis_data.to_blockhash(),
                got: genesis_bid.hash,
            });
        }
        let mut graph = Self {
            tips: alloc::vec![CheckPoint::new(0, genesis_data)],
            best: 0,
        };

        for (tip_id, bid_set) in &cs.branches {
            let mut blocks_vec: Vec<(u32, D)> = Vec::with_capacity(bid_set.len());
            for bid in bid_set {
                let data = cs
                    .blocks
                    .get(&bid.hash)
                    .cloned()
                    .ok_or(ApplyChangeSetError::DanglingBranchRef {
                        tip: *tip_id,
                        missing: *bid,
                    })?;
                blocks_vec.push((bid.height, data));
            }
            if blocks_vec.is_empty() {
                continue;
            }
            let new_tip = CheckPoint::from_blocks(blocks_vec).map_err(|err| match err {
                Some(cp) => ApplyChangeSetError::PrevBlockhashMismatch {
                    expected: cp.block_id(),
                },
                None => ApplyChangeSetError::MissingGenesis,
            })?;
            // Verify the branch reaches genesis with a matching hash.
            let bottom = new_tip.iter().last().expect("non-empty");
            if bottom.height() == 0 && bottom.hash() != graph.genesis_hash() {
                return Err(ApplyChangeSetError::GenesisMismatch {
                    existing: graph.genesis_hash(),
                    got: bottom.hash(),
                });
            }
            graph.absorb_tip(new_tip);
        }
        graph.best = pick_best(&graph.tips);
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

fn pick_best<D>(tips: &[CheckPoint<D>]) -> usize {
    tips.iter()
        .enumerate()
        .max_by_key(|(_, cp)| (cp.height(), Reverse(cp.hash())))
        .map(|(i, _)| i)
        .expect("tips must be non-empty")
}

fn delta<D: Clone>(pre: &ChangeSet<D>, post: ChangeSet<D>) -> ChangeSet<D> {
    let mut out = ChangeSet::<D>::default();
    for (hash, data) in &post.blocks {
        if !pre.blocks.contains_key(hash) {
            out.blocks.insert(*hash, data.clone());
        }
    }
    for (tip_id, post_set) in post.branches {
        match pre.branches.get(&tip_id) {
            Some(pre_set) => {
                let new_bids: BTreeSet<BlockId> =
                    post_set.difference(pre_set).copied().collect();
                if !new_bids.is_empty() {
                    out.branches.insert(tip_id, new_bids);
                }
            }
            None => {
                out.branches.insert(tip_id, post_set);
            }
        }
    }
    out
}
