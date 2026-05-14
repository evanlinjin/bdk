//! Multi-tip, monotone implementation of [`ChainOracle`] over `Header` chains.
//!
//! [`BlockGraph`] keeps every observed tip and a single source-of-truth map of
//! `(height, hash) → (Header, sparse_links)`. Per-block ancestry is implicit in
//! `Header::prev_blockhash` for dense observations; `sparse_links` records the
//! observed predecessors of blocks observed under a sparse `CheckPoint` chain
//! (where the predecessor is *not* at `height - 1`).
//!
//! The [`ChangeSet`] is strictly additive: applying the same changeset twice — or
//! two in either order — yields the same graph state. `Merge` is map-union with
//! set-union on the `sparse_links` of any shared `BlockId` — monotone, commutative,
//! idempotent.
//!
//! Tips are *derived* from the source-of-truth map every time it changes — never
//! stored independently — so absorbed tips can't linger in any parallel structure.

use alloc::vec::Vec;
use core::cmp::Reverse;
use core::convert::Infallible;
use core::ops::RangeBounds;

use crate::collections::{BTreeMap, BTreeSet};
use crate::{BlockId, ChainOracle, Merge};
pub use bdk_core::{CheckPoint, CheckPointIter};
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;

pub use crate::local_chain::{CannotConnectError, MissingGenesisError};

/// Strictly-additive changeset for [`BlockGraph`].
#[derive(Debug, Clone, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct ChangeSet {
    /// Observed blocks. Key = `(height, hash)`; value = `(Header, sparse_links)`.
    ///
    /// `sparse_links` is the set of observed predecessors at non-adjacent heights
    /// (i.e., when the block was seen under a sparse `CheckPoint` chain that
    /// skipped intermediate heights). For dense observations the set is empty —
    /// `Header::prev_blockhash` is the natural adjacent parent.
    pub blocks: BTreeMap<BlockId, (Header, BTreeSet<BlockId>)>,
}

impl Merge for ChangeSet {
    fn merge(&mut self, other: Self) {
        for (bid, (header, sparse_links)) in other.blocks {
            let entry = self.blocks.entry(bid).or_insert((header, BTreeSet::new()));
            // Same `bid` ⇒ same header by consensus; first-write-wins on
            // pathological collisions (we stay strictly additive either way).
            entry.1.extend(sparse_links);
        }
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

/// Multi-tip, monotone chain tracker.
#[derive(Debug, Clone)]
pub struct BlockGraph {
    /// Source of truth: every observed block + its sparse-link metadata.
    blocks: BTreeMap<BlockId, (Header, BTreeSet<BlockId>)>,
    /// Materialised tip chains, derived from `blocks`. Always non-empty.
    tips: BTreeMap<BlockId, CheckPoint<Header>>,
    /// `BlockHash → tip BlockId` for fast lookup by `prev_blockhash`.
    tip_by_hash: BTreeMap<BlockHash, BlockId>,
    /// Observed blocks whose chain back to genesis is broken. Re-evaluated on
    /// every state change; populated when an observed block's parent (via
    /// `Header::prev_blockhash` or any sparse link) isn't itself in `blocks`.
    quarantined: BTreeSet<BlockId>,
    /// The graph's genesis hash. Pinned at construction.
    genesis_hash: BlockHash,
}

impl PartialEq for BlockGraph {
    fn eq(&self, other: &Self) -> bool {
        self.blocks == other.blocks && self.genesis_hash == other.genesis_hash
    }
}

impl BlockGraph {
    /// Construct from a single genesis [`Header`].
    pub fn from_genesis(genesis: Header) -> (Self, ChangeSet) {
        let g_hash = genesis.block_hash();
        let g_bid = BlockId {
            height: 0,
            hash: g_hash,
        };
        let mut blocks = BTreeMap::new();
        blocks.insert(g_bid, (genesis, BTreeSet::new()));
        let mut graph = Self {
            blocks,
            tips: BTreeMap::new(),
            tip_by_hash: BTreeMap::new(),
            quarantined: BTreeSet::new(),
            genesis_hash: g_hash,
        };
        graph.recompute();
        let cs = graph.initial_changeset();
        (graph, cs)
    }

    /// Construct from a single tip [`CheckPoint`]. Fails iff the chain doesn't
    /// bottom at height 0.
    pub fn from_tip(tip: CheckPoint<Header>) -> Result<Self, MissingGenesisError> {
        let bottom = tip.iter().last().expect("CheckPoint is non-empty");
        if bottom.height() != 0 {
            return Err(MissingGenesisError);
        }
        let g_hash = bottom.hash();
        let mut blocks = BTreeMap::new();
        for cp in tip.iter() {
            blocks.insert(cp.block_id(), (cp.data(), BTreeSet::new()));
        }
        // Record sparse links for non-adjacent pairs.
        let chain: Vec<_> = tip.iter().collect();
        for window in chain.windows(2) {
            // chain is descending; window[0] = upper, window[1] = lower.
            let upper = &window[0];
            let lower = &window[1];
            if lower.height() + 1 != upper.height() || upper.data().prev_blockhash != lower.hash() {
                blocks
                    .get_mut(&upper.block_id())
                    .expect("just inserted")
                    .1
                    .insert(lower.block_id());
            }
        }
        let mut graph = Self {
            blocks,
            tips: BTreeMap::new(),
            tip_by_hash: BTreeMap::new(),
            quarantined: BTreeSet::new(),
            genesis_hash: g_hash,
        };
        graph.recompute();
        Ok(graph)
    }

    /// Reconstruct from a complete [`ChangeSet`]. Fails iff no height-0 block
    /// is present and self-consistent.
    pub fn from_changeset(changeset: ChangeSet) -> Result<Self, MissingGenesisError> {
        // Find a usable genesis: a height-0 block whose Header hashes to its
        // declared hash.
        let g_bid = changeset
            .blocks
            .range(
                BlockId {
                    height: 0,
                    hash: BlockHash::all_zeros(),
                }..BlockId {
                    height: 1,
                    hash: BlockHash::all_zeros(),
                },
            )
            .find(|(bid, (header, _))| header.block_hash() == bid.hash)
            .map(|(bid, _)| *bid)
            .ok_or(MissingGenesisError)?;
        let mut graph = Self {
            blocks: changeset.blocks,
            tips: BTreeMap::new(),
            tip_by_hash: BTreeMap::new(),
            quarantined: BTreeSet::new(),
            genesis_hash: g_bid.hash,
        };
        graph.recompute();
        Ok(graph)
    }

    /// Apply an `update` tip. Returns the delta [`ChangeSet`] of newly observed
    /// blocks and sparse links.
    ///
    /// Fails with [`CannotConnectError`] iff `update`'s bottom isn't this
    /// graph's genesis.
    ///
    /// Fast path: when every newly-inserted block extends an existing live tip
    /// via `Header::prev_blockhash` (or a sparse_link target that's itself a
    /// live tip), update `self.tips` incrementally with `CheckPoint::push` in
    /// `O(m)` for `m` new blocks. Falls back to full [`recompute`](Self::recompute)
    /// on any structural change (fork attached below a current tip, orphan,
    /// quarantine-affecting insert) — the fallback is always correct.
    pub fn apply_update(
        &mut self,
        update: CheckPoint<Header>,
    ) -> Result<ChangeSet, CannotConnectError> {
        let bottom = update.iter().last().expect("CheckPoint is non-empty");
        if bottom.height() != 0 || bottom.hash() != self.genesis_hash {
            return Err(CannotConnectError {
                try_include_height: 0,
            });
        }
        let mut delta = ChangeSet::default();
        let chain: Vec<_> = update.iter().collect();
        let mut needs_recompute = false;
        let mut newly_inserted_hashes = BTreeSet::<BlockHash>::new();
        // Bottom-up iteration: parents land in `self.tips`/`tip_by_hash` before
        // their children are processed, so each child's fast-path extension
        // sees its parent as a live tip.
        for (i, cp) in chain.iter().enumerate().rev() {
            let bid = cp.block_id();
            let header = cp.data();
            // Sparse link is to `chain[i + 1]` (the next-lower block in the
            // descending iter) when non-adjacent or hash-mismatched.
            let mut new_sparse_links = BTreeSet::<BlockId>::new();
            if let Some(prev_cp) = chain.get(i + 1) {
                let prev_bid = prev_cp.block_id();
                if prev_bid.height + 1 != bid.height || header.prev_blockhash != prev_bid.hash {
                    new_sparse_links.insert(prev_bid);
                }
            }
            // Insert/merge into `self.blocks`; track newness for classification.
            let (block_is_new, truly_new_links) = match self.blocks.get_mut(&bid) {
                None => {
                    self.blocks
                        .insert(bid, (header, new_sparse_links.clone()));
                    delta
                        .blocks
                        .insert(bid, (header, new_sparse_links.clone()));
                    newly_inserted_hashes.insert(bid.hash);
                    (true, new_sparse_links)
                }
                Some((_, existing_links)) => {
                    let truly_new: BTreeSet<BlockId> = new_sparse_links
                        .difference(existing_links)
                        .copied()
                        .collect();
                    if !truly_new.is_empty() {
                        existing_links.extend(truly_new.iter().copied());
                        delta.blocks.insert(bid, (header, truly_new.clone()));
                    }
                    (false, truly_new)
                }
            };
            // Skip classification once we've decided to recompute, or for genesis.
            if needs_recompute || bid.height == 0 {
                continue;
            }
            // Adding sparse_links to a block we already had may turn it into a
            // tip (or its ancestor) via paths we haven't checked. The fast path
            // doesn't have enough local info to maintain invariants safely.
            if !block_is_new {
                if !truly_new_links.is_empty() {
                    needs_recompute = true;
                }
                continue;
            }
            // Try fast-path: extend a current live tip.
            //   1. Natural parent via `Header::prev_blockhash`.
            //   2. Highest sparse_link target that's itself a live tip.
            let chosen_tip_parent = self
                .tip_by_hash
                .get(&header.prev_blockhash)
                .copied()
                .or_else(|| {
                    truly_new_links
                        .iter()
                        .filter(|sp| self.tip_by_hash.contains_key(&sp.hash))
                        .max_by_key(|sp| sp.height)
                        .copied()
                });
            match chosen_tip_parent {
                Some(parent_bid) => {
                    let parent_cp = self
                        .tips
                        .remove(&parent_bid)
                        .expect("classified as live tip");
                    self.tip_by_hash.remove(&parent_bid.hash);
                    match parent_cp.clone().push(bid.height, header) {
                        Ok(new_cp) => {
                            self.tips.insert(bid, new_cp);
                            self.tip_by_hash.insert(bid.hash, bid);
                        }
                        Err(_) => {
                            // Push rejected the new block (shouldn't happen for
                            // valid updates). Restore the tip and fall back.
                            self.tips.insert(parent_bid, parent_cp);
                            self.tip_by_hash.insert(parent_bid.hash, parent_bid);
                            needs_recompute = true;
                        }
                    }
                }
                None => {
                    // No live tip can absorb this block. Try the fork-creation
                    // fast path: if the block has an observed ancestor (natural
                    // parent in `self.blocks`, or any sparse-link target in
                    // `self.blocks`), it's a fork rooted in the existing block
                    // graph — materialise just *its* chain and add it as a new
                    // tip, leaving all other tips untouched.
                    let natural_parent_observed = self.blocks.contains_key(&BlockId {
                        height: bid.height - 1,
                        hash: header.prev_blockhash,
                    });
                    let sparse_parent_observed = truly_new_links
                        .iter()
                        .any(|sp| self.blocks.contains_key(sp));
                    if natural_parent_observed || sparse_parent_observed {
                        match self.materialise_chain_via_headers(bid) {
                            Some(cp) => {
                                // Verify the fast-path is safe: bid's chain
                                // doesn't contain an existing tip (would absorb
                                // it, violating the no-ancestor invariant), and
                                // bid isn't already inside some existing tip's
                                // chain (then it's not a new tip at all). Either
                                // case requires `recompute` for correctness.
                                let absorbs_existing_tip =
                                    self.tips.keys().any(|t_bid| {
                                        cp.get(t_bid.height)
                                            .is_some_and(|c| c.block_id() == *t_bid)
                                    });
                                let subsumed_by_existing =
                                    self.tips.values().any(|t_cp| {
                                        t_cp.get(bid.height)
                                            .is_some_and(|c| c.block_id() == bid)
                                    });
                                if absorbs_existing_tip || subsumed_by_existing {
                                    needs_recompute = true;
                                } else {
                                    self.tips.insert(bid, cp);
                                    self.tip_by_hash.insert(bid.hash, bid);
                                }
                            }
                            None => {
                                // Chain didn't reach genesis — orphan. Fall
                                // back to full recompute (handles quarantine).
                                needs_recompute = true;
                            }
                        }
                    } else {
                        // Orphan: prev isn't in blocks and no sparse-link
                        // target is observed. Defer to `recompute`, which
                        // will quarantine this block correctly.
                        needs_recompute = true;
                    }
                }
            }
        }
        // If any newly-inserted hash is the missing parent of a currently
        // quarantined block, that block could now be released — but the
        // cascade may absorb existing tips in non-trivial ways. Safer to
        // recompute.
        if !needs_recompute
            && !self.quarantined.is_empty()
            && self.quarantined.iter().any(|bid| {
                self.blocks
                    .get(bid)
                    .is_some_and(|(h, _)| newly_inserted_hashes.contains(&h.prev_blockhash))
            })
        {
            needs_recompute = true;
        }
        if needs_recompute {
            self.recompute();
        }
        Ok(delta)
    }

    /// Merge a [`ChangeSet`] (complete or delta) into the graph.
    pub fn apply_changeset(&mut self, changeset: &ChangeSet) {
        for (bid, (header, sparse_links)) in &changeset.blocks {
            let entry = self
                .blocks
                .entry(*bid)
                .or_insert((*header, BTreeSet::new()));
            entry.1.extend(sparse_links.iter().copied());
        }
        self.recompute();
    }

    /// Snapshot the full state as a [`ChangeSet`].
    pub fn initial_changeset(&self) -> ChangeSet {
        ChangeSet {
            blocks: self.blocks.clone(),
        }
    }

    /// Best (canonical) tip.
    pub fn tip(&self) -> CheckPoint<Header> {
        self.tips
            .values()
            .max_by_key(|cp| (cp.height(), Reverse(cp.hash())))
            .expect("BlockGraph always has at least one tip")
            .clone()
    }

    /// Iterate every tip's CheckPoint (best-tip-first order).
    pub fn tips(&self) -> impl Iterator<Item = &CheckPoint<Header>> {
        let mut tips: Vec<_> = self.tips.values().collect();
        tips.sort_by_key(|cp| (Reverse(cp.height()), cp.hash()));
        tips.into_iter()
    }

    /// Number of tips.
    pub fn tip_count(&self) -> usize {
        self.tips.len()
    }

    /// Number of quarantined blocks.
    pub fn quarantined_count(&self) -> usize {
        self.quarantined.len()
    }

    /// Iterate quarantined BlockIds.
    pub fn quarantined(&self) -> impl Iterator<Item = &BlockId> {
        self.quarantined.iter()
    }

    /// The genesis hash.
    pub fn genesis_hash(&self) -> BlockHash {
        self.genesis_hash
    }

    /// Iterate checkpoints from the best tip in descending height.
    pub fn iter_checkpoints(&self) -> CheckPointIter<Header> {
        self.tip().into_iter()
    }

    /// Get the checkpoint at `height` on the best tip.
    pub fn get(&self, height: u32) -> Option<CheckPoint<Header>> {
        self.tip().get(height)
    }

    /// Iterate checkpoints over a height range on the best tip.
    pub fn range<R>(&self, range: R) -> impl Iterator<Item = CheckPoint<Header>>
    where
        R: RangeBounds<u32>,
    {
        self.tip().range(range)
    }

    // ---- Internal: rebuild derived state from `self.blocks` ----

    fn recompute(&mut self) {
        // hash → BlockId lookup.
        let hash_index: BTreeMap<BlockHash, BlockId> =
            self.blocks.keys().map(|bid| (bid.hash, *bid)).collect();

        // For each block, determine its single chosen observed parent:
        //   1. natural parent via `Header::prev_blockhash` if observed;
        //   2. otherwise the highest-height sparse-link target that's observed.
        // None at height 0 (genesis). None elsewhere ⇒ orphan.
        let mut chosen_parent: BTreeMap<BlockId, Option<BlockId>> = BTreeMap::new();
        for (bid, (header, sparse_links)) in &self.blocks {
            if bid.height == 0 {
                chosen_parent.insert(*bid, None);
                continue;
            }
            let natural = hash_index.get(&header.prev_blockhash).copied();
            let sparse = sparse_links
                .iter()
                .filter(|sp| hash_index.contains_key(&sp.hash))
                .max_by_key(|sp| sp.height)
                .copied();
            chosen_parent.insert(*bid, natural.or(sparse));
        }

        // BFS reachable-from-genesis using parent→children edges.
        let mut children: BTreeMap<BlockId, BTreeSet<BlockId>> = BTreeMap::new();
        for (bid, parent) in &chosen_parent {
            if let Some(p) = parent {
                children.entry(*p).or_default().insert(*bid);
            }
        }
        let genesis_bid = BlockId {
            height: 0,
            hash: self.genesis_hash,
        };
        let mut reachable: BTreeSet<BlockId> = BTreeSet::new();
        if self.blocks.contains_key(&genesis_bid) {
            reachable.insert(genesis_bid);
            let mut stack = alloc::vec![genesis_bid];
            while let Some(node) = stack.pop() {
                if let Some(kids) = children.get(&node) {
                    for k in kids {
                        if reachable.insert(*k) {
                            stack.push(*k);
                        }
                    }
                }
            }
        }

        // Quarantine = observed blocks not reachable from genesis.
        let quarantined: BTreeSet<BlockId> = self
            .blocks
            .keys()
            .filter(|bid| !reachable.contains(bid))
            .copied()
            .collect();

        // Tips = reachable blocks with no reachable children.
        let mut tip_bids: Vec<BlockId> = Vec::new();
        for bid in &reachable {
            let has_child = children
                .get(bid)
                .map(|ks| ks.iter().any(|k| reachable.contains(k)))
                .unwrap_or(false);
            if !has_child {
                tip_bids.push(*bid);
            }
        }

        // Materialise each tip's chain by walking `chosen_parent` backward.
        // Per-tip granularity optimisation: if a tip's BlockId was a tip before
        // this recompute *and* its old chain still agrees with the new
        // `chosen_parent` map, reuse the cached `CheckPoint` Arc — skip the
        // O(chain) re-materialisation (and its N Arc allocations) for tips
        // structurally unaffected by the changes.
        let old_tips = core::mem::take(&mut self.tips);
        let mut tips = BTreeMap::new();
        let mut tip_by_hash = BTreeMap::new();
        for tip_bid in tip_bids {
            let cp = match old_tips.get(&tip_bid) {
                Some(old_cp) if chain_matches_chosen_parent(old_cp, &chosen_parent) => {
                    old_cp.clone()
                }
                _ => match self.materialise_chain(tip_bid, &chosen_parent) {
                    Some(cp) => cp,
                    None => continue,
                },
            };
            tips.insert(tip_bid, cp);
            tip_by_hash.insert(tip_bid.hash, tip_bid);
        }
        // If no tips materialised (e.g., genesis missing), seed with bare genesis.
        if tips.is_empty() {
            if let Some((_, (header, _))) = self.blocks.get_key_value(&genesis_bid) {
                let cp = CheckPoint::new(0, *header);
                tips.insert(genesis_bid, cp);
                tip_by_hash.insert(genesis_bid.hash, genesis_bid);
            }
        }

        self.tips = tips;
        self.tip_by_hash = tip_by_hash;
        self.quarantined = quarantined;
    }

    fn materialise_chain(
        &self,
        tip_bid: BlockId,
        chosen_parent: &BTreeMap<BlockId, Option<BlockId>>,
    ) -> Option<CheckPoint<Header>> {
        let mut chain: Vec<(u32, Header)> = Vec::new();
        let mut current = tip_bid;
        loop {
            let (header, _) = self.blocks.get(&current)?;
            chain.push((current.height, *header));
            match chosen_parent.get(&current).copied().flatten() {
                Some(parent) => current = parent,
                None => break,
            }
        }
        chain.reverse();
        CheckPoint::from_blocks(chain).ok()
    }

    /// Walk `header.prev_blockhash → blocks[parent_bid]` (with sparse_link
    /// fallback) to materialise a tip's chain back to genesis without needing
    /// the global `chosen_parent` map. Used by the `apply_update` fast path
    /// for fork-at-intermediate-height cases.
    fn materialise_chain_via_headers(&self, tip_bid: BlockId) -> Option<CheckPoint<Header>> {
        let mut chain: Vec<(u32, Header)> = Vec::new();
        let mut current = tip_bid;
        loop {
            let (header, sparse_links) = self.blocks.get(&current)?;
            chain.push((current.height, *header));
            if current.height == 0 {
                break;
            }
            // Natural parent: (current.height - 1, header.prev_blockhash) if observed.
            let natural_bid = BlockId {
                height: current.height - 1,
                hash: header.prev_blockhash,
            };
            let next_bid = if self.blocks.contains_key(&natural_bid) {
                natural_bid
            } else {
                // Fall back to highest observed sparse_link target.
                sparse_links
                    .iter()
                    .filter(|sp| self.blocks.contains_key(sp))
                    .max_by_key(|sp| sp.height)
                    .copied()?
            };
            current = next_bid;
        }
        chain.reverse();
        CheckPoint::from_blocks(chain).ok()
    }
}

/// Walk `old_cp` and the new `chosen_parent` map in lockstep. Returns `true`
/// iff every step agrees — i.e., the new structure produces the exact same
/// chain. When `true`, the caller can reuse `old_cp` without re-materialising.
fn chain_matches_chosen_parent(
    old_cp: &CheckPoint<Header>,
    chosen_parent: &BTreeMap<BlockId, Option<BlockId>>,
) -> bool {
    let mut current = Some(old_cp.clone());
    while let Some(node) = current {
        let bid = node.block_id();
        let expected_parent = chosen_parent.get(&bid).copied().flatten();
        let actual_prev = node.prev().map(|p| p.block_id());
        if expected_parent != actual_prev {
            return false;
        }
        current = node.prev();
    }
    true
}

impl ChainOracle for BlockGraph {
    type Error = Infallible;

    fn is_block_in_chain(
        &self,
        block: BlockId,
        chain_tip: BlockId,
    ) -> Result<Option<bool>, Self::Error> {
        // `block` is above `chain_tip` — it can't be on the chain that ends at
        // `chain_tip`.
        if block.height > chain_tip.height {
            return Ok(Some(false));
        }
        // Find any live tip whose chain contains `chain_tip` at `chain_tip.height`.
        // `chain_tip` doesn't have to be a top-level tip — it can be any
        // ancestor on some retained tip's chain.
        for tip in self.tips.values() {
            let at_chain_tip = match tip.get(chain_tip.height) {
                Some(cp) => cp,
                None => continue,
            };
            if at_chain_tip.block_id() != chain_tip {
                continue;
            }
            // `chain_tip` is on this tip's chain. Use it to answer the query.
            return Ok(tip.get(block.height).map(|cp| cp.block_id() == block));
        }
        Ok(None)
    }

    fn get_chain_tip(&self) -> Result<BlockId, Self::Error> {
        Ok(self.tip().block_id())
    }
}
