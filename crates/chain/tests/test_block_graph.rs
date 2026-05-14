#![cfg(feature = "miniscript")]
//! Unit tests for [`bdk_chain::block_graph::BlockGraph`] under the `D = Header` model.

use bdk_chain::{
    block_graph::{BlockGraph, CannotConnectError, ChangeSet, MissingGenesisError},
    BlockId, ChainOracle, Merge,
};
use bdk_core::CheckPoint;
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, CompactTarget, TxMerkleNode};

/// Build a `Header` deterministically from `(prev, marker)`. Using `time` as a
/// uniqueness marker so different markers at the same prev produce different
/// hashes — useful for forks-at-same-height tests.
fn header(prev: BlockHash, marker: u32) -> Header {
    Header {
        version: bitcoin::block::Version::ONE,
        prev_blockhash: prev,
        merkle_root: TxMerkleNode::all_zeros(),
        time: marker,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce: 0,
    }
}

fn genesis() -> Header {
    header(BlockHash::all_zeros(), 0)
}

/// Build a dense chain of `Header`s starting with the supplied genesis. Returns
/// the resulting `CheckPoint` (descending iter from tip) and the sequence of
/// `Header`s in genesis-to-tip order. `markers[0]` corresponds to the block at
/// height 1, `markers[1]` to height 2, etc.
fn dense_chain(g: Header, markers: &[u32]) -> (CheckPoint<Header>, Vec<Header>) {
    let mut headers = vec![g];
    let mut prev_hash = g.block_hash();
    for &m in markers {
        let h = header(prev_hash, m);
        prev_hash = h.block_hash();
        headers.push(h);
    }
    let blocks: Vec<(u32, Header)> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| (i as u32, *h))
        .collect();
    (CheckPoint::from_blocks(blocks).unwrap(), headers)
}

/// Build a sparse `CheckPoint` chain using only the heights in `indices` (relative
/// to `headers`). Headers at non-included heights are skipped. `indices` must
/// include 0 (genesis).
fn sparse_chain(headers: &[Header], indices: &[u32]) -> CheckPoint<Header> {
    let blocks: Vec<(u32, Header)> = indices.iter().map(|&i| (i, headers[i as usize])).collect();
    CheckPoint::from_blocks(blocks).unwrap()
}

#[test]
fn from_genesis_has_one_tip() {
    let (graph, cs) = BlockGraph::from_genesis(genesis());
    assert_eq!(graph.tip_count(), 1);
    assert_eq!(graph.tip().height(), 0);
    assert_eq!(graph.tip().hash(), genesis().block_hash());
    assert_eq!(graph.genesis_hash(), genesis().block_hash());
    // ChangeSet has just the genesis entry.
    assert_eq!(cs.blocks.len(), 1);
}

#[test]
fn from_changeset_roundtrips_genesis() {
    let (graph, cs) = BlockGraph::from_genesis(genesis());
    let rebuilt = BlockGraph::from_changeset(cs).unwrap();
    assert_eq!(graph, rebuilt);
}

#[test]
fn from_changeset_missing_genesis_errors() {
    let cs = ChangeSet::default();
    assert!(matches!(
        BlockGraph::from_changeset(cs),
        Err(MissingGenesisError)
    ));
}

#[test]
fn apply_update_extends_canonical_tip() {
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    let (update, _) = dense_chain(g, &[1, 2, 3]);
    let delta = graph.apply_update(update).unwrap();
    assert_eq!(graph.tip().height(), 3);
    assert_eq!(graph.tip_count(), 1);
    // Delta should have 3 new entries (heights 1, 2, 3 — genesis was already known).
    assert_eq!(delta.blocks.len(), 3);
}

#[test]
fn apply_update_idempotent() {
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    let (update, _) = dense_chain(g, &[1, 2, 3]);
    let _ = graph.apply_update(update.clone()).unwrap();
    let snapshot = graph.clone();
    let delta = graph.apply_update(update).unwrap();
    assert_eq!(graph, snapshot);
    assert!(delta.blocks.is_empty());
}

#[test]
fn apply_update_genesis_mismatch_errors() {
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    // A different genesis.
    let other_g = header(BlockHash::all_zeros(), 999);
    let (update, _) = dense_chain(other_g, &[1, 2]);
    assert_eq!(
        graph.apply_update(update).unwrap_err(),
        CannotConnectError {
            try_include_height: 0
        }
    );
}

#[test]
fn divergent_forks_both_retained() {
    // Two chains sharing only the genesis: G→A1 and G→A2. Both stay as tips.
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    let (u1, _) = dense_chain(g, &[1]); // height 1 with marker 1
    let (u2, _) = dense_chain(g, &[2]); // height 1 with marker 2 — different hash
    graph.apply_update(u1).unwrap();
    graph.apply_update(u2).unwrap();
    assert_eq!(graph.tip_count(), 2);
}

#[test]
fn fork_at_higher_height_both_retained_best_is_higher_then_lower_hash() {
    // Two chains G→1A→2A and G→1B→2B with different markers, both reach height 2.
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    let (u1, hs1) = dense_chain(g, &[10, 20]);
    let (u2, hs2) = dense_chain(g, &[11, 21]);
    graph.apply_update(u1).unwrap();
    graph.apply_update(u2).unwrap();
    assert_eq!(graph.tip_count(), 2);
    // Canonical is (max height, lowest hash on ties).
    let tip = graph.tip().block_id();
    let tip1_hash = hs1[2].block_hash();
    let tip2_hash = hs2[2].block_hash();
    let expected = if tip1_hash < tip2_hash {
        tip1_hash
    } else {
        tip2_hash
    };
    assert_eq!(tip.height, 2);
    assert_eq!(tip.hash, expected);
}

#[test]
fn ancestor_update_is_noop() {
    // Apply (3, ...) tip, then re-apply the (2, ...) prefix. State unchanged.
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    let (u_long, hs) = dense_chain(g, &[1, 2, 3]);
    graph.apply_update(u_long).unwrap();
    let pre_state = graph.clone();
    let u_short = CheckPoint::from_blocks(vec![(0, hs[0]), (1, hs[1]), (2, hs[2])]).unwrap();
    let delta = graph.apply_update(u_short).unwrap();
    assert_eq!(graph, pre_state);
    assert!(delta.blocks.is_empty());
}

#[test]
fn sparse_chain_records_sparse_links_in_changeset() {
    // Apply a sparse chain G→3→5 (heights 0, 3, 5).
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    let (_, hs) = dense_chain(g, &[1, 2, 3, 4, 5]); // build all 6 headers
    let sparse = sparse_chain(&hs, &[0, 3, 5]);
    let delta = graph.apply_update(sparse).unwrap();
    // Heights 3 and 5 should be in the delta with sparse_links recorded.
    let bid3 = BlockId {
        height: 3,
        hash: hs[3].block_hash(),
    };
    let bid5 = BlockId {
        height: 5,
        hash: hs[5].block_hash(),
    };
    let bid_g = BlockId {
        height: 0,
        hash: hs[0].block_hash(),
    };
    let (h3, links3) = delta.blocks.get(&bid3).expect("bid3 in delta");
    assert_eq!(h3.block_hash(), bid3.hash);
    assert!(links3.contains(&bid_g), "sparse link 3 → genesis recorded");
    let (h5, links5) = delta.blocks.get(&bid5).expect("bid5 in delta");
    assert_eq!(h5.block_hash(), bid5.hash);
    assert!(links5.contains(&bid3), "sparse link 5 → 3 recorded");
}

#[test]
fn sparse_chain_materialises_in_chain_oracle() {
    // After applying a sparse chain G→3→5, queries against tip at (5, h5) should
    // succeed for the sparse-observed heights and return None for unobserved.
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    let (_, hs) = dense_chain(g, &[1, 2, 3, 4, 5]);
    let sparse = sparse_chain(&hs, &[0, 3, 5]);
    graph.apply_update(sparse).unwrap();
    let tip = graph.get_chain_tip().unwrap();
    let bid3 = BlockId {
        height: 3,
        hash: hs[3].block_hash(),
    };
    let bid5 = BlockId {
        height: 5,
        hash: hs[5].block_hash(),
    };
    let bid_g = BlockId {
        height: 0,
        hash: hs[0].block_hash(),
    };
    assert_eq!(graph.is_block_in_chain(bid_g, tip).unwrap(), Some(true));
    assert_eq!(graph.is_block_in_chain(bid3, tip).unwrap(), Some(true));
    assert_eq!(graph.is_block_in_chain(bid5, tip).unwrap(), Some(true));
    // height 2 isn't in the sparse chain → None (uncertain).
    let bid2 = BlockId {
        height: 2,
        hash: hs[2].block_hash(),
    };
    assert_eq!(graph.is_block_in_chain(bid2, tip).unwrap(), None);
}

#[test]
fn apply_changeset_idempotent() {
    let g = genesis();
    let (mut graph_once, _) = BlockGraph::from_genesis(g);
    let (update, _) = dense_chain(g, &[1, 2, 3]);
    graph_once.apply_update(update).unwrap();
    let cs = graph_once.initial_changeset();

    let (mut graph_twice, _) = BlockGraph::from_genesis(g);
    graph_twice.apply_changeset(&cs);
    graph_twice.apply_changeset(&cs);
    assert_eq!(graph_once, graph_twice);
}

#[test]
fn apply_empty_changeset_is_noop() {
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    let (update, _) = dense_chain(g, &[1, 2]);
    graph.apply_update(update).unwrap();
    let snapshot = graph.clone();
    graph.apply_changeset(&ChangeSet::default());
    assert_eq!(graph, snapshot);
}

#[test]
fn changeset_merge_is_idempotent_and_order_independent() {
    let g = genesis();
    let (mut graph_a, _) = BlockGraph::from_genesis(g);
    let (u_a, _) = dense_chain(g, &[1, 2]);
    graph_a.apply_update(u_a).unwrap();
    let cs_a = graph_a.initial_changeset();

    let (mut graph_b, _) = BlockGraph::from_genesis(g);
    let (u_b, _) = dense_chain(g, &[3]);
    graph_b.apply_update(u_b).unwrap();
    let cs_b = graph_b.initial_changeset();

    // Commutativity.
    let mut ab = cs_a.clone();
    ab.merge(cs_b.clone());
    let mut ba = cs_b.clone();
    ba.merge(cs_a.clone());
    assert_eq!(ab, ba);

    // Idempotence.
    let mut aa = cs_a.clone();
    aa.merge(cs_a.clone());
    assert_eq!(aa, cs_a);
}

#[test]
fn apply_changeset_round_trips_full_state() {
    let g = genesis();
    let (mut original, _) = BlockGraph::from_genesis(g);
    let (u, _) = dense_chain(g, &[1, 2, 3]);
    original.apply_update(u).unwrap();
    let cs = original.initial_changeset();
    let rebuilt = BlockGraph::from_changeset(cs).unwrap();
    assert_eq!(original, rebuilt);
}

#[test]
fn out_of_order_changeset_application_converges() {
    // graph_a applies two updates in order; collect deltas; replay shuffled to
    // graph_b. End state equal.
    let g = genesis();
    let (mut graph_a, _) = BlockGraph::from_genesis(g);
    let (u1, _) = dense_chain(g, &[1, 2]);
    let (u2, _) = dense_chain(g, &[1, 2, 3, 4]); // extension of u1's prefix
    let d1 = graph_a.apply_update(u1).unwrap();
    let d2 = graph_a.apply_update(u2).unwrap();

    let (mut graph_b, _) = BlockGraph::from_genesis(g);
    graph_b.apply_changeset(&d2);
    graph_b.apply_changeset(&d1);
    assert_eq!(graph_a, graph_b);
}

#[test]
fn fully_orphan_blocks_are_quarantined() {
    // Apply genesis, then a changeset that references blocks whose chain doesn't
    // reach genesis. They should land in quarantine.
    use bdk_chain::collections::BTreeSet;
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    // Construct a stranded block: header pointing to an unknown prev.
    let orphan_h = header(BlockHash::from_byte_array([0xab; 32]), 7);
    let orphan_bid = BlockId {
        height: 5,
        hash: orphan_h.block_hash(),
    };
    let mut cs = ChangeSet::default();
    cs.blocks
        .insert(orphan_bid, (orphan_h, BTreeSet::<BlockId>::new()));
    graph.apply_changeset(&cs);
    assert_eq!(graph.tip_count(), 1, "genesis still the only live tip");
    assert_eq!(graph.quarantined_count(), 1, "orphan quarantined");
    assert!(graph.quarantined().any(|bid| *bid == orphan_bid));
}

#[test]
fn quarantined_block_releases_when_predecessor_arrives() {
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    // Build a chain G → h1 → h2.
    let (chain, hs) = dense_chain(g, &[1, 2]);
    let h1 = hs[1];
    let h2 = hs[2];
    let _bid1 = BlockId {
        height: 1,
        hash: h1.block_hash(),
    };
    let bid2 = BlockId {
        height: 2,
        hash: h2.block_hash(),
    };
    // Deliver only (2, h2): orphan because its prev (1, h1) is unknown.
    let mut cs = ChangeSet::default();
    cs.blocks.insert(bid2, (h2, Default::default()));
    graph.apply_changeset(&cs);
    assert_eq!(graph.quarantined_count(), 1);
    assert!(graph.quarantined().any(|bid| *bid == bid2));
    // Now deliver the chain that supplies (1, h1).
    graph.apply_update(chain).unwrap();
    assert_eq!(graph.quarantined_count(), 0);
    assert_eq!(graph.tip().block_id(), bid2);
}

#[test]
fn is_block_in_chain_against_non_canonical_tip() {
    let g = genesis();
    let (mut graph, _) = BlockGraph::from_genesis(g);
    let (u1, hs1) = dense_chain(g, &[1, 2, 5]); // height 2 marker 1, height 3 marker 2, etc.
    let (u2, _hs2) = dense_chain(g, &[1, 3, 6]); // diverges
    graph.apply_update(u1).unwrap();
    graph.apply_update(u2).unwrap();
    assert_eq!(graph.tip_count(), 2);
    let canonical = graph.get_chain_tip().unwrap();
    let non_canonical = graph
        .tips()
        .map(|cp| cp.block_id())
        .find(|b| *b != canonical)
        .unwrap();
    // Genesis is in both.
    let bid_g = BlockId {
        height: 0,
        hash: hs1[0].block_hash(),
    };
    assert_eq!(graph.is_block_in_chain(bid_g, canonical).unwrap(), Some(true));
    assert_eq!(
        graph.is_block_in_chain(bid_g, non_canonical).unwrap(),
        Some(true)
    );
    // Non-canonical tip's own block isn't on canonical chain.
    assert_eq!(
        graph.is_block_in_chain(non_canonical, canonical).unwrap(),
        Some(false)
    );
}

#[test]
fn changeset_size_is_linear_in_chain_length() {
    // After N apply_update extensions of m new blocks each, persisted changeset
    // has N*m + 1 (genesis) entries — linear, not quadratic.
    let g = genesis();
    let (mut graph, init) = BlockGraph::from_genesis(g);
    let mut persisted = init;
    let n = 20_u32;
    let m = 5_u32;
    let mut prev_hash = g.block_hash();
    let mut chain: Vec<(u32, Header)> = vec![(0, g)];
    for sync_i in 1..=n {
        for k in 1..=m {
            let height = (sync_i - 1) * m + k;
            let h = header(prev_hash, height);
            prev_hash = h.block_hash();
            chain.push((height, h));
        }
        let cp = CheckPoint::from_blocks(chain.clone()).unwrap();
        let delta = graph.apply_update(cp).unwrap();
        persisted.merge(delta);
    }
    let expected = (n * m + 1) as usize;
    assert_eq!(persisted.blocks.len(), expected);
}
