//! Property-based tests for [`bdk_chain::block_graph::BlockGraph`] under the
//! `D = Header` model.
//!
//! Two generator families:
//!
//! - Legacy chain generator (`arb_checkpoint`): picks one of `MAX_CHAINS` dense
//!   chains diverging at height 1, then applies a random sparseness mask.
//!   Cheap and shrinks well; covers single-fork-at-genesis topologies.
//!
//! - Forest generator (`build_forest` + `arb_branch_spec`): grows a random
//!   tree of headers where each branch roots at an arbitrary previously-grown
//!   block. Updates sample a `CheckPoint` from any leaf in the forest with a
//!   random sparseness mask. Exercises forks at arbitrary depths — the
//!   topology class the legacy generator cannot reach.

#![cfg(feature = "miniscript")]

use std::collections::BTreeSet;

use bdk_chain::{
    block_graph::{BlockGraph, ChangeSet},
    BlockId, ChainOracle, Merge,
};
use bdk_core::CheckPoint;
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, CompactTarget, TxMerkleNode};
use proptest::prelude::*;
use rand::{Rng, SeedableRng};

const MAX_CHAINS: u8 = 3;
const MAX_HEIGHT: u32 = 16;

fn header_at(prev: BlockHash, marker: u32) -> Header {
    Header {
        version: bitcoin::block::Version::ONE,
        prev_blockhash: prev,
        merkle_root: TxMerkleNode::all_zeros(),
        time: marker,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce: 0,
    }
}

fn genesis_header() -> Header {
    header_at(BlockHash::all_zeros(), 0)
}

// ---- Legacy chain generator ------------------------------------------------

/// Build a deterministic chain of `Header`s for `(chain_idx, max_height)`.
/// Index 0 is genesis. The chain at height `h` is `header_at(prev_hash, chain_idx * 1000 + h)`
/// so different chain_idx values yield distinct hashes at every height.
fn build_chain(chain_idx: u8, max_height: u32) -> Vec<Header> {
    let g = header_at(BlockHash::all_zeros(), 0); // genesis is shared across chains
    let mut chain = vec![g];
    let mut prev = g.block_hash();
    for h in 1..=max_height {
        let hdr = header_at(prev, (chain_idx as u32 + 1) * 1000 + h);
        prev = hdr.block_hash();
        chain.push(hdr);
    }
    chain
}

/// Strategy producing a valid `CheckPoint<Header>` from a chain identified by
/// `chain_idx`, ending at some height between 1 and `MAX_HEIGHT`, with a
/// random subset of intermediate heights included (i.e., the chain may be
/// sparse).
fn arb_checkpoint() -> impl Strategy<Value = CheckPoint<Header>> {
    (0u8..MAX_CHAINS, 1u32..MAX_HEIGHT, any::<u32>()).prop_map(|(chain_idx, tip_h, mask)| {
        let chain = build_chain(chain_idx, tip_h);
        let mut blocks: Vec<(u32, Header)> = vec![(0, chain[0])];
        for k in 1..tip_h {
            if mask & (1u32 << (k % 32)) != 0 {
                blocks.push((k, chain[k as usize]));
            }
        }
        blocks.push((tip_h, chain[tip_h as usize]));
        CheckPoint::from_blocks(blocks).expect("chain is monotonic")
    })
}

// ---- Forest generator (forks at arbitrary depths) --------------------------

#[derive(Debug, Clone)]
struct Forest {
    headers: std::collections::BTreeMap<BlockId, Header>,
    parent: std::collections::BTreeMap<BlockId, Option<BlockId>>,
    leaves: BTreeSet<BlockId>,
    next_marker: u32,
}

impl Forest {
    fn new() -> Self {
        let g = genesis_header();
        let g_bid = BlockId {
            height: 0,
            hash: g.block_hash(),
        };
        let mut headers = std::collections::BTreeMap::new();
        headers.insert(g_bid, g);
        let mut parent = std::collections::BTreeMap::new();
        parent.insert(g_bid, None);
        let mut leaves = BTreeSet::new();
        leaves.insert(g_bid);
        Self {
            headers,
            parent,
            leaves,
            next_marker: 1,
        }
    }

    /// Grow `len` new blocks rooted at `parent_bid`. Each block gets a unique
    /// `time` marker so its hash differs from every other block in the forest.
    fn grow_from(&mut self, parent_bid: BlockId, len: u32) {
        let parent_header = self.headers[&parent_bid];
        let mut prev_hash = parent_header.block_hash();
        let mut prev_bid = parent_bid;
        // `parent_bid` may have been a leaf; once it has a child it isn't.
        self.leaves.remove(&parent_bid);
        for _ in 0..len {
            let marker = self.next_marker;
            self.next_marker += 1;
            let h = header_at(prev_hash, marker);
            let bid = BlockId {
                height: prev_bid.height + 1,
                hash: h.block_hash(),
            };
            self.headers.insert(bid, h);
            self.parent.insert(bid, Some(prev_bid));
            prev_hash = h.block_hash();
            prev_bid = bid;
        }
        self.leaves.insert(prev_bid);
    }

    /// Walk from `bid` to genesis, returning the chain in tip-to-genesis order.
    fn ancestor_chain(&self, bid: BlockId) -> Vec<BlockId> {
        let mut chain = Vec::new();
        let mut current = Some(bid);
        while let Some(b) = current {
            chain.push(b);
            current = self.parent.get(&b).copied().flatten();
        }
        chain
    }
}

#[derive(Debug, Clone)]
struct BranchSpec {
    /// `% bids.len()` picks a parent from the forest's current blocks.
    parent_choice: u32,
    length: u32,
}

#[derive(Debug, Clone)]
struct UpdateSpec {
    /// `% leaves.len()` picks the tip the update extends to.
    leaf_choice: u32,
    /// Bitmask: include intermediate block at chain-index `i` iff bit `i % 64` is set.
    sparse_mask: u64,
}

fn arb_branch_spec() -> impl Strategy<Value = BranchSpec> {
    (any::<u32>(), 1u32..=6).prop_map(|(parent_choice, length)| BranchSpec {
        parent_choice,
        length,
    })
}

fn arb_update_spec() -> impl Strategy<Value = UpdateSpec> {
    (any::<u32>(), any::<u64>()).prop_map(|(leaf_choice, sparse_mask)| UpdateSpec {
        leaf_choice,
        sparse_mask,
    })
}

fn build_forest(branches: &[BranchSpec]) -> Forest {
    let mut f = Forest::new();
    for b in branches {
        let bids: Vec<BlockId> = f.headers.keys().copied().collect();
        let parent_bid = bids[b.parent_choice as usize % bids.len()];
        f.grow_from(parent_bid, b.length);
    }
    f
}

fn build_update(forest: &Forest, spec: &UpdateSpec) -> CheckPoint<Header> {
    let leaves: Vec<BlockId> = forest.leaves.iter().copied().collect();
    let leaf_bid = leaves[spec.leaf_choice as usize % leaves.len()];
    let mut chain = forest.ancestor_chain(leaf_bid);
    chain.reverse(); // genesis → leaf
    let n = chain.len();
    let blocks: Vec<(u32, Header)> = chain
        .iter()
        .enumerate()
        .filter(|(i, _)| *i == 0 || *i + 1 == n || (spec.sparse_mask & (1u64 << (i % 64))) != 0)
        .map(|(_, bid)| (bid.height, forest.headers[bid]))
        .collect();
    CheckPoint::from_blocks(blocks).expect("monotonic")
}

fn build_updates(forest: &Forest, specs: &[UpdateSpec]) -> Vec<CheckPoint<Header>> {
    specs.iter().map(|s| build_update(forest, s)).collect()
}

// ---- Shared helpers --------------------------------------------------------

fn apply_all(graph: &mut BlockGraph, updates: &[CheckPoint<Header>]) {
    for cp in updates {
        let _ = graph.apply_update(cp.clone());
    }
}

/// Verify structural invariants. Covers (in order):
///   1. ≥ 1 tip.
///   2. Each tip's chain reaches genesis, heights strictly decreasing,
///      every step references a block in `self.blocks`.
///   3. No tip is a strict ancestor of another (no two tips on the same chain).
///   4. Tip BlockIds unique.
///   5. `tips()` order: descending by height, ascending by hash for ties.
///   6. `reachable_ancestors_of_tips ∪ quarantined == self.blocks.keys()`
///      (every observed block is either on some tip's chain or quarantined,
///      and the two sets are disjoint).
fn check_invariants(graph: &BlockGraph) -> Result<(), TestCaseError> {
    prop_assert!(graph.tip_count() >= 1, "graph has no tips");

    let cs = graph.initial_changeset();
    let observed: BTreeSet<BlockId> = cs.blocks.keys().copied().collect();

    let mut reachable: BTreeSet<BlockId> = BTreeSet::new();
    for tip in graph.tips() {
        let bottom = tip.iter().last().expect("non-empty");
        prop_assert_eq!(bottom.height(), 0, "tip chain does not reach height 0");
        prop_assert_eq!(
            bottom.hash(),
            graph.genesis_hash(),
            "tip chain does not reach genesis"
        );
        let mut prev_height: Option<u32> = None;
        for node in tip.iter() {
            if let Some(ph) = prev_height {
                prop_assert!(node.height() < ph, "non-monotonic heights in CheckPoint");
            }
            prev_height = Some(node.height());
            prop_assert!(
                observed.contains(&node.block_id()),
                "CheckPoint references unobserved block {:?}",
                node.block_id()
            );
            reachable.insert(node.block_id());
        }
    }

    // No tip is a strict ancestor of another.
    let tip_bids: Vec<BlockId> = graph.tips().map(|cp| cp.block_id()).collect();
    for (i, ti) in graph.tips().enumerate() {
        for (j, tj) in graph.tips().enumerate() {
            if i == j {
                continue;
            }
            if let Some(cp) = tj.get(ti.height()) {
                prop_assert_ne!(
                    cp.block_id(),
                    ti.block_id(),
                    "{:?} is ancestor of {:?}",
                    ti.block_id(),
                    tj.block_id(),
                );
            }
        }
    }

    let unique: BTreeSet<_> = tip_bids.iter().copied().collect();
    prop_assert_eq!(unique.len(), tip_bids.len(), "duplicate tip BlockIds");

    // tips() ordering: best-first = descending height, then ascending hash.
    let mut expected = tip_bids.clone();
    expected.sort_by(|a, b| b.height.cmp(&a.height).then(a.hash.cmp(&b.hash)));
    prop_assert_eq!(
        tip_bids,
        expected,
        "tips() not in best-first order (height desc, hash asc)"
    );

    // reachable (tip-chain ancestors) and quarantined partition observed.
    let quarantined: BTreeSet<BlockId> = graph.quarantined().copied().collect();
    prop_assert!(
        reachable.is_disjoint(&quarantined),
        "block is both reachable and quarantined"
    );
    let union: BTreeSet<BlockId> = reachable.union(&quarantined).copied().collect();
    prop_assert_eq!(
        union, observed,
        "reachable ∪ quarantined != observed blocks"
    );

    Ok(())
}

// ---- Existing proptests (legacy chain generator) ---------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Applying the same set of updates in any order produces the same graph.
    #[test]
    fn apply_update_order_independence(
        updates in prop::collection::vec(arb_checkpoint(), 1..10),
        seed: u64,
    ) {
        let mut shuffled = updates.clone();
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        use rand::seq::SliceRandom;
        shuffled.shuffle(&mut rng);

        let (mut a, _) = BlockGraph::from_genesis(genesis_header());
        let (mut b, _) = BlockGraph::from_genesis(genesis_header());
        apply_all(&mut a, &updates);
        apply_all(&mut b, &shuffled);
        prop_assert_eq!(a, b);
    }

    /// `from_changeset(graph.initial_changeset())` round-trips the full state.
    #[test]
    fn initial_changeset_roundtrip(
        updates in prop::collection::vec(arb_checkpoint(), 1..10),
    ) {
        let (mut graph, _) = BlockGraph::from_genesis(genesis_header());
        apply_all(&mut graph, &updates);
        let cs = graph.initial_changeset();
        let rebuilt = BlockGraph::from_changeset(cs).expect("genesis preserved");
        prop_assert_eq!(graph, rebuilt);
    }

    /// Accumulating per-call deltas → `from_changeset` matches direct apply.
    #[test]
    fn delta_accumulation_matches_direct_apply(
        updates in prop::collection::vec(arb_checkpoint(), 1..10),
    ) {
        let (mut direct, init) = BlockGraph::from_genesis(genesis_header());
        let mut persisted = init;
        for cp in &updates {
            if let Ok(delta) = direct.apply_update(cp.clone()) {
                persisted.merge(delta);
            }
        }
        let rebuilt = BlockGraph::from_changeset(persisted).expect("genesis preserved");
        prop_assert_eq!(direct, rebuilt);
    }

    /// Out-of-order delta replay converges to the canonical state.
    #[test]
    fn out_of_order_delta_application_converges(
        updates in prop::collection::vec(arb_checkpoint(), 1..8),
        seed: u64,
    ) {
        let (mut canonical, _) = BlockGraph::from_genesis(genesis_header());
        let mut deltas: Vec<ChangeSet> = Vec::new();
        for cp in &updates {
            if let Ok(delta) = canonical.apply_update(cp.clone()) {
                deltas.push(delta);
            }
        }
        let mut shuffled = deltas.clone();
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        use rand::seq::SliceRandom;
        shuffled.shuffle(&mut rng);

        let (mut replayed, _) = BlockGraph::from_genesis(genesis_header());
        for delta in &shuffled {
            replayed.apply_changeset(delta);
        }
        prop_assert_eq!(canonical, replayed);
    }

    /// `Merge` is commutative AND idempotent on `ChangeSet`s built from real updates.
    #[test]
    fn merge_commutative_and_idempotent(
        a_updates in prop::collection::vec(arb_checkpoint(), 1..6),
        b_updates in prop::collection::vec(arb_checkpoint(), 1..6),
    ) {
        let cs_a = {
            let (mut g, _) = BlockGraph::from_genesis(genesis_header());
            apply_all(&mut g, &a_updates);
            g.initial_changeset()
        };
        let cs_b = {
            let (mut g, _) = BlockGraph::from_genesis(genesis_header());
            apply_all(&mut g, &b_updates);
            g.initial_changeset()
        };
        let mut ab = cs_a.clone();
        ab.merge(cs_b.clone());
        let mut ba = cs_b.clone();
        ba.merge(cs_a.clone());
        prop_assert_eq!(&ab, &ba);

        let mut aa = cs_a.clone();
        aa.merge(cs_a.clone());
        prop_assert_eq!(&aa, &cs_a);
    }

    /// All structural invariants hold after arbitrary apply_update sequences.
    #[test]
    fn invariants_after_apply_updates(
        updates in prop::collection::vec(arb_checkpoint(), 1..10),
    ) {
        let (mut graph, _) = BlockGraph::from_genesis(genesis_header());
        apply_all(&mut graph, &updates);
        check_invariants(&graph)?;
    }
}

// ---- New proptests (forest generator + broader API coverage) ---------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Forest topology: applying the same updates in any order produces the
    /// same graph. Exercises forks at arbitrary depths.
    #[test]
    fn forest_apply_update_order_independence(
        branches in prop::collection::vec(arb_branch_spec(), 2..6),
        update_specs in prop::collection::vec(arb_update_spec(), 1..10),
        seed: u64,
    ) {
        let forest = build_forest(&branches);
        let updates = build_updates(&forest, &update_specs);
        let mut shuffled = updates.clone();
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        use rand::seq::SliceRandom;
        shuffled.shuffle(&mut rng);

        let (mut a, _) = BlockGraph::from_genesis(genesis_header());
        let (mut b, _) = BlockGraph::from_genesis(genesis_header());
        apply_all(&mut a, &updates);
        apply_all(&mut b, &shuffled);
        prop_assert_eq!(a, b);
    }

    /// Forest topology: structural invariants hold.
    #[test]
    fn forest_invariants_after_apply_updates(
        branches in prop::collection::vec(arb_branch_spec(), 2..6),
        update_specs in prop::collection::vec(arb_update_spec(), 1..10),
    ) {
        let forest = build_forest(&branches);
        let updates = build_updates(&forest, &update_specs);
        let (mut graph, _) = BlockGraph::from_genesis(genesis_header());
        apply_all(&mut graph, &updates);
        check_invariants(&graph)?;
    }

    /// `apply_changeset` partition convergence. Build a canonical graph via
    /// `apply_update`, snapshot its changeset, partition randomly into `K`
    /// parts and apply via `apply_changeset` in shuffled order. Intermediate
    /// graphs may have quarantined blocks; the final graph must equal the
    /// canonical one. Exercises the quarantine entry/release path that
    /// `apply_update` alone can't reach.
    #[test]
    fn apply_changeset_random_partition_converges(
        branches in prop::collection::vec(arb_branch_spec(), 2..5),
        update_specs in prop::collection::vec(arb_update_spec(), 1..8),
        partition_seed: u64,
        order_seed: u64,
        num_parts in 1usize..6,
    ) {
        let forest = build_forest(&branches);
        let updates = build_updates(&forest, &update_specs);
        let (mut canonical, _) = BlockGraph::from_genesis(genesis_header());
        apply_all(&mut canonical, &updates);
        let full_cs = canonical.initial_changeset();

        let mut rng = rand::rngs::StdRng::seed_from_u64(partition_seed);
        let mut parts: Vec<ChangeSet> = (0..num_parts).map(|_| ChangeSet::default()).collect();
        for (bid, val) in &full_cs.blocks {
            let p = rng.gen_range(0..num_parts);
            parts[p].blocks.insert(*bid, val.clone());
        }
        let mut order_rng = rand::rngs::StdRng::seed_from_u64(order_seed);
        use rand::seq::SliceRandom;
        parts.shuffle(&mut order_rng);

        let (mut test, _) = BlockGraph::from_genesis(genesis_header());
        for p in &parts {
            test.apply_changeset(p);
        }
        prop_assert_eq!(canonical, test);
    }

    /// `from_tip(cp)` ≡ `from_genesis() + apply_update(cp)`.
    #[test]
    fn from_tip_matches_from_genesis_apply_update(
        branches in prop::collection::vec(arb_branch_spec(), 2..5),
        update_spec in arb_update_spec(),
    ) {
        let forest = build_forest(&branches);
        let cp = build_update(&forest, &update_spec);
        let from_tip = BlockGraph::from_tip(cp.clone()).expect("genesis preserved");
        let (mut from_gen, _) = BlockGraph::from_genesis(genesis_header());
        let _ = from_gen.apply_update(cp);
        prop_assert_eq!(from_tip, from_gen);
    }

    /// For every block on a live tip's CheckPoint chain, `is_block_in_chain`
    /// queried against that tip returns `Some(true)`.
    #[test]
    fn is_block_in_chain_true_for_chain_members(
        branches in prop::collection::vec(arb_branch_spec(), 2..6),
        update_specs in prop::collection::vec(arb_update_spec(), 1..10),
    ) {
        let forest = build_forest(&branches);
        let updates = build_updates(&forest, &update_specs);
        let (mut graph, _) = BlockGraph::from_genesis(genesis_header());
        apply_all(&mut graph, &updates);
        for tip in graph.tips() {
            let tip_bid = tip.block_id();
            for node in tip.iter() {
                let result = graph
                    .is_block_in_chain(node.block_id(), tip_bid)
                    .expect("infallible");
                prop_assert_eq!(
                    result,
                    Some(true),
                    "{:?} should be on chain ending at {:?}",
                    node.block_id(),
                    tip_bid
                );
            }
        }
    }

    /// `is_block_in_chain` returns `Some(false)` when `block.height >
    /// chain_tip.height`, even for synthetic (unobserved) blocks.
    #[test]
    fn is_block_in_chain_false_above_chain_tip(
        branches in prop::collection::vec(arb_branch_spec(), 2..5),
        update_specs in prop::collection::vec(arb_update_spec(), 1..6),
    ) {
        let forest = build_forest(&branches);
        let updates = build_updates(&forest, &update_specs);
        let (mut graph, _) = BlockGraph::from_genesis(genesis_header());
        apply_all(&mut graph, &updates);
        for tip in graph.tips() {
            let tip_bid = tip.block_id();
            let above = BlockId {
                height: tip_bid.height + 1,
                hash: BlockHash::all_zeros(),
            };
            let result = graph.is_block_in_chain(above, tip_bid).expect("infallible");
            prop_assert_eq!(result, Some(false));
        }
    }
}
