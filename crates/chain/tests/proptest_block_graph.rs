//! Property-based tests for [`bdk_chain::block_graph::BlockGraph`] under the
//! `D = Header` model.
//!
//! The strategy builds chains of `Header`s where each block's `prev_blockhash`
//! correctly points to its predecessor, so sparse updates produce
//! mathematically-sound CheckPoint chains. We use `Header.time` as a forking
//! marker to produce distinct hashes at the same height with the same prev.

#![cfg(feature = "miniscript")]

use bdk_chain::{
    block_graph::{BlockGraph, ChangeSet},
    BlockId, Merge,
};
use bdk_core::CheckPoint;
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, CompactTarget, TxMerkleNode};
use proptest::prelude::*;
use rand::SeedableRng;

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

fn genesis_header() -> Header {
    header_at(BlockHash::all_zeros(), 0)
}

fn apply_all(graph: &mut BlockGraph, updates: &[CheckPoint<Header>]) {
    for cp in updates {
        let _ = graph.apply_update(cp.clone());
    }
}

/// Verify structural invariants.
fn check_invariants(graph: &BlockGraph) -> Result<(), TestCaseError> {
    prop_assert!(graph.tip_count() >= 1, "graph has no tips");
    // Each tip's chain reaches genesis.
    for tip in graph.tips() {
        let bottom = tip.iter().last().expect("non-empty");
        prop_assert_eq!(bottom.height(), 0);
        prop_assert_eq!(bottom.hash(), graph.genesis_hash());
    }
    // No tip is a strict ancestor of another (each pair: pick i and j; if i is
    // in j's chain at i's height, that's a violation).
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
    // Tip BlockIds unique.
    let unique: std::collections::BTreeSet<_> = tip_bids.iter().copied().collect();
    prop_assert_eq!(unique.len(), tip_bids.len(), "duplicate tip BlockIds");
    Ok(())
}

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
