#![cfg(feature = "miniscript")]

use bdk_chain::{
    block_graph::{ApplyChangeSetError, BlockGraph, CannotConnectError, ChangeSet},
    BlockId, ChainOracle, Merge,
};
use bdk_testenv::{chain_update, hash};
use bitcoin::BlockHash;

fn block(height: u32, h: &str) -> BlockId {
    BlockId {
        height,
        hash: bitcoin::hashes::Hash::hash(h.as_bytes()),
    }
}

#[test]
fn from_genesis_roundtrip() {
    let (graph, cs) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    assert_eq!(graph.genesis_hash(), hash!("G"));
    assert_eq!(graph.tip_count(), 1);
    assert_eq!(graph.get_chain_tip().unwrap(), block(0, "G"));

    let rebuilt = BlockGraph::<BlockHash>::from_changeset(cs).expect("valid changeset");
    assert_eq!(graph, rebuilt);
}

#[test]
fn sequential_extension_keeps_one_tip() {
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));

    let delta = graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A"))])
        .expect("connect");
    assert_eq!(graph.tip_count(), 1);
    assert_eq!(graph.tip().block_id(), block(1, "A"));
    assert!(!delta.is_empty());

    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A")), (2, hash!("B"))])
        .expect("connect");
    assert_eq!(graph.tip_count(), 1);
    assert_eq!(graph.tip().block_id(), block(2, "B"));
}

#[test]
fn apply_same_update_twice_is_idempotent() {
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let update = chain_update![(0, hash!("G")), (1, hash!("A"))];
    graph.apply_update(update.clone()).unwrap();

    let snapshot = graph.initial_changeset();
    let second = graph.apply_update(update).unwrap();
    assert!(
        second.is_empty(),
        "re-applying an update should be a no-op"
    );
    assert_eq!(graph.initial_changeset(), snapshot);
}

#[test]
fn divergent_forks_both_retained_best_by_height_then_lowest_hash() {
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A"))])
        .unwrap();
    // Add a same-height fork.
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("B"))])
        .unwrap();

    assert_eq!(graph.tip_count(), 2, "both forks retained");

    // Both block_ids should be present among tips.
    let tip_ids: std::collections::BTreeSet<BlockId> =
        graph.tips().map(|t| t.block_id()).collect();
    assert!(tip_ids.contains(&block(1, "A")));
    assert!(tip_ids.contains(&block(1, "B")));

    // Best is the lower hash (by Reverse(hash) max).
    let a_hash: BlockHash = hash!("A");
    let b_hash: BlockHash = hash!("B");
    let expected_best = if a_hash < b_hash {
        block(1, "A")
    } else {
        block(1, "B")
    };
    assert_eq!(graph.get_chain_tip().unwrap(), expected_best);
}

#[test]
fn reorg_moves_best_tip_loser_still_queryable() {
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A"))])
        .unwrap();
    // Add a fork.
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("B"))])
        .unwrap();
    let a_hash: BlockHash = hash!("A");
    let b_hash: BlockHash = hash!("B");
    let losing_tip = if a_hash < b_hash {
        block(1, "B")
    } else {
        block(1, "A")
    };
    // Extend the losing branch — it becomes the longest fork.
    let extension = if losing_tip == block(1, "B") {
        chain_update![(0, hash!("G")), (1, hash!("B")), (2, hash!("C"))]
    } else {
        chain_update![(0, hash!("G")), (1, hash!("A")), (2, hash!("C"))]
    };
    graph.apply_update(extension).unwrap();

    // Best tip should now be the longer fork.
    assert_eq!(graph.get_chain_tip().unwrap(), block(2, "C"));

    // The other branch's tip is still in tips().
    let other_tip = if losing_tip == block(1, "B") {
        block(1, "A")
    } else {
        block(1, "B")
    };
    assert!(
        graph.tips().any(|t| t.block_id() == other_tip),
        "loser's branch not retained: {:?}",
        graph.tips().map(|t| t.block_id()).collect::<Vec<_>>(),
    );

    // ChainOracle can answer queries against the non-canonical tip.
    assert_eq!(
        graph.is_block_in_chain(other_tip, other_tip).unwrap(),
        Some(true),
    );
    // Querying for `block(1, C-side)` against the non-canonical tip's chain.
    let losing_side = block(1, if losing_tip == block(1, "B") { "B" } else { "A" });
    assert_eq!(
        graph.is_block_in_chain(losing_side, other_tip).unwrap(),
        Some(false),
        "the non-canonical tip's chain shouldn't contain a divergent sibling",
    );
}

#[test]
fn sparse_same_tip_id_merge_unions_blocks() {
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    // First update: sparse with heights {0, 2}
    graph
        .apply_update(chain_update![(0, hash!("G")), (2, hash!("X"))])
        .unwrap();
    // Second update: same tip BlockId but adds height 1
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("M")), (2, hash!("X"))])
        .unwrap();

    assert_eq!(graph.tip_count(), 1);
    let tip = graph.tip();
    let heights: Vec<u32> = tip.iter().map(|cp| cp.height()).collect();
    assert_eq!(heights, vec![2, 1, 0]);
    assert_eq!(tip.get(1).unwrap().hash(), hash!("M"));
}

#[test]
fn genesis_mismatch_errors() {
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let err = graph
        .apply_update(chain_update![(0, hash!("X")), (1, hash!("Y"))])
        .unwrap_err();
    assert_eq!(err, CannotConnectError { try_include_height: 0 });
}

#[test]
fn changeset_merge_is_idempotent_and_order_independent() {
    let (graph_a, mut cs_a) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let _ = graph_a;
    let (graph_b, cs_b) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let _ = graph_b;

    let mut merged_once = cs_a.clone();
    merged_once.merge(cs_b.clone());

    let mut merged_twice = merged_once.clone();
    merged_twice.merge(cs_b.clone());
    assert_eq!(
        merged_once, merged_twice,
        "applying the same changeset twice should be a no-op"
    );

    // Order independence: a.merge(b) == b.merge(a)
    cs_a.merge(cs_b.clone());
    let mut cs_b_then_a = cs_b;
    cs_b_then_a.merge(BlockGraph::<BlockHash>::from_genesis(hash!("G")).1);
    assert_eq!(cs_a, cs_b_then_a);
}

#[test]
fn apply_changeset_round_trips_full_state() {
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A"))])
        .unwrap();
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("B"))])
        .unwrap();
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("B")), (2, hash!("C"))])
        .unwrap();

    let cs = graph.initial_changeset();
    let rebuilt = BlockGraph::<BlockHash>::from_changeset(cs).unwrap();
    assert_eq!(graph, rebuilt);
}

#[test]
fn apply_changeset_order_independence() {
    // Build a graph one way, build another graph the reverse way, compare.
    let (mut g1, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    g1.apply_update(chain_update![(0, hash!("G")), (1, hash!("A"))])
        .unwrap();
    g1.apply_update(chain_update![(0, hash!("G")), (1, hash!("B"))])
        .unwrap();

    let (mut g2, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    g2.apply_update(chain_update![(0, hash!("G")), (1, hash!("B"))])
        .unwrap();
    g2.apply_update(chain_update![(0, hash!("G")), (1, hash!("A"))])
        .unwrap();

    assert_eq!(g1, g2, "apply order should not affect resulting state");
    assert_eq!(g1.initial_changeset(), g2.initial_changeset());
}

#[test]
fn from_changeset_rejects_empty() {
    let cs = ChangeSet::<BlockHash>::default();
    let err = BlockGraph::<BlockHash>::from_changeset(cs).unwrap_err();
    assert_eq!(err, ApplyChangeSetError::MissingGenesis);
}

#[test]
fn from_changeset_rejects_dangling_branch_ref() {
    use bdk_chain::collections::BTreeSet;
    let mut cs = ChangeSet::<BlockHash>::default();
    cs.blocks.insert(hash!("G"), hash!("G"));
    let mut set = BTreeSet::<BlockId>::new();
    set.insert(block(0, "G"));
    set.insert(block(1, "MISSING")); // no entry in `blocks` for hash!("MISSING")
    cs.branches.insert(block(1, "MISSING"), set);
    let err = BlockGraph::<BlockHash>::from_changeset(cs).unwrap_err();
    assert!(
        matches!(err, ApplyChangeSetError::DanglingBranchRef { .. }),
        "expected DanglingBranchRef, got {err:?}"
    );
}

#[test]
fn chain_oracle_is_block_in_chain() {
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A")), (2, hash!("B"))])
        .unwrap();
    let chain_tip = graph.get_chain_tip().unwrap();
    assert_eq!(
        graph.is_block_in_chain(block(1, "A"), chain_tip).unwrap(),
        Some(true)
    );
    assert_eq!(
        graph.is_block_in_chain(block(1, "X"), chain_tip).unwrap(),
        Some(false)
    );
    // Unknown chain_tip
    assert_eq!(
        graph.is_block_in_chain(block(1, "A"), block(99, "Z")).unwrap(),
        None
    );
}

