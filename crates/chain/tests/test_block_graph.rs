#![cfg(feature = "miniscript")]

use bdk_chain::{
    block_graph::{BlockGraph, CannotConnectError, ChangeSet, MissingGenesisError},
    BlockId, ChainOracle, Merge,
};
use bdk_testenv::{chain_update, hash, local_chain};
use bitcoin::BlockHash;

fn block(height: u32, h: &str) -> BlockId {
    BlockId {
        height,
        hash: bitcoin::hashes::Hash::hash(h.as_bytes()),
    }
}

/// Ported from `test_local_chain::update_local_chain`. Same 18 input cases, but instead
/// of asserting the exact `LocalChain` changeset shape, we assert the **best tip** —
/// because `BlockGraph` retains diverging forks rather than rejecting them.
///
/// There are four observable outcomes:
///
/// - [`SameBest`]: `LocalChain::apply_update` returns `Ok` *without invalidating any
///   existing blocks*. `BlockGraph` also returns `Ok` with a single tip, and
///   `graph.tip()` equals `LocalChain`'s tip.
/// - [`ForkBgWinsLong`]: `LocalChain` returns `Ok` *via invalidation* (it overwrites
///   some existing blocks and shortens the chain). `BlockGraph` keeps both branches as
///   tips, and `(max height, lowest hash)` selects the longer existing branch — so
///   `BlockGraph`'s best tip differs from `LocalChain`'s. This is the fundamental
///   semantic difference: `LocalChain` invalidates; `BlockGraph` preserves.
/// - [`Diverge`]: `LocalChain` returns `CannotConnectError`. `BlockGraph` accepts the
///   update as a coexisting tip. Best is `(max height, lowest hash)` over the two tips.
/// - [`GenesisMismatch`]: both reject with `CannotConnectError { try_include_height: 0 }`.
///
/// [`SameBest`]: LocalChainOutcome::SameBest
/// [`ForkBgWinsLong`]: LocalChainOutcome::ForkBgWinsLong
/// [`Diverge`]: LocalChainOutcome::Diverge
/// [`GenesisMismatch`]: LocalChainOutcome::GenesisMismatch
#[derive(Debug)]
enum LocalChainOutcome {
    SameBest,
    ForkBgWinsLong,
    Diverge,
    GenesisMismatch,
}

struct PortedCase {
    name: &'static str,
    chain: bdk_chain::local_chain::LocalChain,
    update: bdk_chain::local_chain::CheckPoint,
    local_chain_outcome: LocalChainOutcome,
}

impl PortedCase {
    /// Return the `(max height, lowest hash)` winner across `prior_tip` and `update_tip`.
    fn bg_best_of_two(prior: BlockId, update: BlockId) -> BlockId {
        // (max height, then lowest hash) ⇒ rank by (height, Reverse(hash)).
        if (prior.height, core::cmp::Reverse(prior.hash))
            > (update.height, core::cmp::Reverse(update.hash))
        {
            prior
        } else {
            update
        }
    }

    fn run(self) {
        let prior_tip = self.chain.tip();
        let update_tip = self.update.clone();

        let mut graph =
            BlockGraph::<BlockHash>::from_tip(prior_tip.clone()).expect("chain has genesis");

        match self.local_chain_outcome {
            LocalChainOutcome::SameBest => {
                graph
                    .apply_update(update_tip.clone())
                    .unwrap_or_else(|e| panic!("[{}] expected Ok, got {e:?}", self.name));

                let mut reference = self.chain.clone();
                reference
                    .apply_update(update_tip)
                    .expect("LocalChain accepts this case");
                assert_eq!(
                    graph.tip().block_id(),
                    reference.tip().block_id(),
                    "[{}] best tip should match LocalChain's tip",
                    self.name,
                );
                assert_eq!(
                    graph.tip_count(),
                    1,
                    "[{}] no divergence expected, so only one tip",
                    self.name,
                );
            }
            LocalChainOutcome::ForkBgWinsLong => {
                // LocalChain accepts via invalidation. BlockGraph keeps both branches.
                let mut reference = self.chain.clone();
                reference
                    .apply_update(update_tip.clone())
                    .expect("LocalChain accepts (via invalidation) this case");

                graph.apply_update(update_tip.clone()).unwrap_or_else(|e| {
                    panic!("[{}] BlockGraph should accept, got {e:?}", self.name)
                });

                assert_eq!(
                    graph.tip_count(),
                    2,
                    "[{}] fork expected: existing tip + update tip",
                    self.name,
                );
                let expected_best =
                    Self::bg_best_of_two(prior_tip.block_id(), update_tip.block_id());
                assert_eq!(
                    graph.tip().block_id(),
                    expected_best,
                    "[{}] best tip should follow (max height, lowest hash)",
                    self.name,
                );
                // And it should be strictly *different* from LocalChain's tip (LC
                // shortened the chain via invalidation; BG kept the longer existing
                // branch).
                assert_ne!(
                    graph.tip().block_id(),
                    reference.tip().block_id(),
                    "[{}] this case is meant to demonstrate BG/LC divergence",
                    self.name,
                );
            }
            LocalChainOutcome::Diverge => {
                let mut reference = self.chain.clone();
                let _ = reference
                    .apply_update(update_tip.clone())
                    .expect_err("LocalChain should reject this case");

                graph.apply_update(update_tip.clone()).unwrap_or_else(|e| {
                    panic!(
                        "[{}] BlockGraph should accept the divergent update, got {e:?}",
                        self.name,
                    )
                });

                assert_eq!(
                    graph.tip_count(),
                    2,
                    "[{}] both diverging branches should be retained",
                    self.name,
                );
                let expected_best =
                    Self::bg_best_of_two(prior_tip.block_id(), update_tip.block_id());
                assert_eq!(
                    graph.tip().block_id(),
                    expected_best,
                    "[{}] best tip should follow (max height, lowest hash)",
                    self.name,
                );
            }
            LocalChainOutcome::GenesisMismatch => {
                let err = graph.apply_update(update_tip).unwrap_err();
                assert_eq!(
                    err,
                    CannotConnectError {
                        try_include_height: 0,
                    },
                    "[{}] expected genesis-mismatch error",
                    self.name,
                );
                return;
            }
        }

        let cs = graph.initial_changeset();
        let rebuilt = BlockGraph::<BlockHash>::from_changeset(cs)
            .unwrap_or_else(|e| panic!("[{}] round-trip failed: {e:?}", self.name));
        assert_eq!(graph, rebuilt, "[{}] round-trip equality", self.name);
    }
}

#[test]
fn ported_from_test_local_chain_update_cases() {
    [
        PortedCase {
            name: "add first tip",
            chain: local_chain![(0, hash!("A"))],
            update: chain_update![(0, hash!("A"))],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "add second tip",
            chain: local_chain![(0, hash!("A"))],
            update: chain_update![(0, hash!("A")), (1, hash!("B"))],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "two disjoint chains cannot merge",
            chain: local_chain![(0, hash!("_")), (1, hash!("A"))],
            update: chain_update![(0, hash!("_")), (2, hash!("B"))],
            local_chain_outcome: LocalChainOutcome::Diverge,
        },
        PortedCase {
            name: "two disjoint chains, existing longer",
            chain: local_chain![(0, hash!("_")), (2, hash!("A"))],
            update: chain_update![(0, hash!("_")), (1, hash!("B"))],
            local_chain_outcome: LocalChainOutcome::Diverge,
        },
        PortedCase {
            name: "duplicate chains should merge",
            chain: local_chain![(0, hash!("A"))],
            update: chain_update![(0, hash!("A"))],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "can introduce older checkpoint",
            chain: local_chain![(0, hash!("_")), (2, hash!("C")), (3, hash!("D"))],
            update: chain_update![(0, hash!("_")), (1, hash!("B")), (2, hash!("C"))],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "can introduce older checkpoint 2",
            chain: local_chain![(0, hash!("_")), (3, hash!("B")), (4, hash!("C"))],
            update: chain_update![(0, hash!("_")), (2, hash!("A")), (4, hash!("C"))],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "can introduce older checkpoint 3",
            chain: local_chain![(0, hash!("_")), (1, hash!("A")), (3, hash!("C"))],
            update: chain_update![(0, hash!("_")), (2, hash!("B")), (3, hash!("C"))],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "introduce two older checkpoints below PoA",
            chain: local_chain![(0, hash!("_")), (3, hash!("C"))],
            update: chain_update![
                (0, hash!("_")),
                (1, hash!("A")),
                (2, hash!("B")),
                (3, hash!("C"))
            ],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "fix blockhash before agreement point",
            chain: local_chain![
                (0, hash!("_")),
                (1, hash!("im-wrong")),
                (2, hash!("we-agree"))
            ],
            update: chain_update![(0, hash!("_")), (1, hash!("fix")), (2, hash!("we-agree"))],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "two points of agreement",
            chain: local_chain![(0, hash!("_")), (2, hash!("B")), (3, hash!("C"))],
            update: chain_update![
                (0, hash!("_")),
                (1, hash!("A")),
                (2, hash!("B")),
                (3, hash!("C")),
                (4, hash!("D"))
            ],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "update and chain does not connect",
            chain: local_chain![(0, hash!("_")), (2, hash!("B")), (3, hash!("C"))],
            update: chain_update![
                (0, hash!("_")),
                (1, hash!("A")),
                (2, hash!("B")),
                (4, hash!("D"))
            ],
            local_chain_outcome: LocalChainOutcome::Diverge,
        },
        PortedCase {
            name: "transitive invalidation with PoA",
            chain: local_chain![
                (0, hash!("_")),
                (2, hash!("B")),
                (3, hash!("C")),
                (5, hash!("E"))
            ],
            update: chain_update![
                (0, hash!("_")),
                (2, hash!("B'")),
                (3, hash!("C'")),
                (4, hash!("D"))
            ],
            // LocalChain accepts via invalidation: new tip = (4, D), height 5 is
            // erased. BlockGraph keeps (5, E) as best — the longer existing branch
            // wins by (max height, lowest hash) — and (4, D) becomes a fork tip.
            local_chain_outcome: LocalChainOutcome::ForkBgWinsLong,
        },
        PortedCase {
            name: "transitive invalidation no PoA",
            chain: local_chain![
                (0, hash!("_")),
                (1, hash!("B")),
                (2, hash!("C")),
                (4, hash!("E"))
            ],
            update: chain_update![
                (0, hash!("_")),
                (1, hash!("B'")),
                (2, hash!("C'")),
                (3, hash!("D"))
            ],
            // Same shape as the previous case but with no PoA. LC still accepts via
            // invalidation; BG keeps the longer (4, E) branch as best.
            local_chain_outcome: LocalChainOutcome::ForkBgWinsLong,
        },
        PortedCase {
            name: "invalidation but no connection",
            chain: local_chain![
                (0, hash!("_")),
                (1, hash!("A")),
                (2, hash!("B")),
                (3, hash!("C")),
                (5, hash!("E"))
            ],
            update: chain_update![
                (0, hash!("_")),
                (2, hash!("B'")),
                (3, hash!("C'")),
                (4, hash!("D"))
            ],
            local_chain_outcome: LocalChainOutcome::Diverge,
        },
        PortedCase {
            name: "introduce blocks between two points of agreement",
            chain: local_chain![
                (0, hash!("A")),
                (1, hash!("B")),
                (3, hash!("D")),
                (4, hash!("E"))
            ],
            update: chain_update![
                (0, hash!("A")),
                (2, hash!("C")),
                (4, hash!("E")),
                (5, hash!("F"))
            ],
            local_chain_outcome: LocalChainOutcome::SameBest,
        },
        PortedCase {
            name: "shorter update on same chain prefix",
            chain: local_chain![
                (0, hash!("_")),
                (2, hash!("C")),
                (3, hash!("D")),
                (4, hash!("E")),
                (5, hash!("F"))
            ],
            update: chain_update![(0, hash!("_")), (2, hash!("C")), (3, hash!("D'"))],
            // LocalChain invalidates D, E, F and replaces with D'. BlockGraph sees a
            // height-3 fork → keeps the longer existing (5, F) as best, and (3, D')
            // as a non-canonical tip.
            local_chain_outcome: LocalChainOutcome::ForkBgWinsLong,
        },
        PortedCase {
            name: "conflicting genesis without agreement point",
            chain: local_chain![(0, hash!("_")), (2, hash!("B"))],
            update: chain_update![(0, hash!("_'")), (2, hash!("B'"))],
            local_chain_outcome: LocalChainOutcome::GenesisMismatch,
        },
    ]
    .into_iter()
    .for_each(PortedCase::run);
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
    assert_eq!(err, MissingGenesisError);
}

#[test]
fn from_changeset_silently_skips_dangling_branch_refs() {
    use bdk_chain::collections::BTreeSet;
    // A branch references a BlockId whose hash is not in `blocks`. Reconstruction should
    // silently drop the dangling entry, the branch's effective tip falls back to whatever
    // links cleanly (genesis here), so the resulting graph is just the genesis tip.
    let mut cs = ChangeSet::<BlockHash>::default();
    cs.blocks.insert(hash!("G"), hash!("G"));
    let mut set = BTreeSet::<BlockId>::new();
    set.insert(block(0, "G"));
    set.insert(block(1, "MISSING")); // no entry in `blocks` for hash!("MISSING")
    cs.branches.extend_branch(block(1, "MISSING"), set);
    let graph = BlockGraph::<BlockHash>::from_changeset(cs)
        .expect("dangling refs should be silently skipped, not errored");
    assert_eq!(graph.tip_count(), 1);
    assert_eq!(graph.get_chain_tip().unwrap(), block(0, "G"));
}

#[test]
fn from_changeset_silently_skips_prev_blockhash_mismatch() {
    use bdk_chain::collections::BTreeSet;
    use bitcoin::block::Header;
    use bitcoin::{hashes::Hash, CompactTarget, TxMerkleNode};

    fn header(prev: BlockHash) -> Header {
        Header {
            version: bitcoin::block::Version::ONE,
            prev_blockhash: prev,
            merkle_root: TxMerkleNode::all_zeros(),
            time: 0,
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce: 0,
        }
    }
    // Genesis header has prev=all_zeros.
    let zero_hash = BlockHash::all_zeros();
    let h0 = header(zero_hash);
    let g_hash = h0.block_hash();
    // h1 links to h0 properly.
    let h1 = header(g_hash);
    let h1_hash = h1.block_hash();
    // h2_bad links to the *wrong* prev (zero_hash instead of h1_hash). When we try to
    // push it onto the chain at h1, push must fail and the lenient builder must skip it.
    let h2_bad = header(zero_hash);
    let h2_bad_hash = h2_bad.block_hash();

    let mut cs = ChangeSet::<Header>::default();
    cs.blocks.insert(g_hash, h0);
    cs.blocks.insert(h1_hash, h1);
    cs.blocks.insert(h2_bad_hash, h2_bad);
    let mut set = BTreeSet::<BlockId>::new();
    set.insert(BlockId {
        height: 0,
        hash: g_hash,
    });
    set.insert(BlockId {
        height: 1,
        hash: h1_hash,
    });
    set.insert(BlockId {
        height: 2,
        hash: h2_bad_hash,
    });
    let claimed_tip = BlockId {
        height: 2,
        hash: h2_bad_hash,
    };
    cs.branches.extend_branch(claimed_tip, set);

    let graph =
        BlockGraph::<Header>::from_changeset(cs).expect("non-linking entry should be skipped");
    // The non-linking h2_bad must be dropped, leaving the tip at h1.
    assert_eq!(graph.tip_count(), 1);
    assert_eq!(graph.get_chain_tip().unwrap().height, 1);
    assert_eq!(graph.get_chain_tip().unwrap().hash, h1_hash);
}

#[test]
fn from_changeset_truncates_branch_above_unlinkable_height() {
    // When no candidate at height H links, the branch must be truncated at H-1 so that
    // a non-adjacent push doesn't silently re-accept higher blocks (whose prev_blockhash
    // points to the dropped block).
    use bdk_chain::collections::BTreeSet;
    use bitcoin::block::Header;
    use bitcoin::{hashes::Hash, CompactTarget, TxMerkleNode};

    fn header(prev: BlockHash) -> Header {
        Header {
            version: bitcoin::block::Version::ONE,
            prev_blockhash: prev,
            merkle_root: TxMerkleNode::all_zeros(),
            time: 0,
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce: 0,
        }
    }
    let zero = BlockHash::all_zeros();
    let h0 = header(zero);
    let g_hash = h0.block_hash();
    // h1_bad does NOT link to genesis (prev = zero rather than g_hash).
    let h1_bad = header(zero);
    let h1_bad_hash = h1_bad.block_hash();
    // h2 claims to descend from h1_bad. Heights 0 and 2 are non-adjacent, so a naive
    // `push(2, h2)` onto cp(0, g) would skip the prev_blockhash check and silently
    // accept h2 even though h1_bad was dropped.
    let h2 = header(h1_bad_hash);
    let h2_hash = h2.block_hash();

    let mut cs = ChangeSet::<Header>::default();
    cs.blocks.insert(g_hash, h0);
    cs.blocks.insert(h1_bad_hash, h1_bad);
    cs.blocks.insert(h2_hash, h2);
    let mut set = BTreeSet::<BlockId>::new();
    set.insert(BlockId {
        height: 0,
        hash: g_hash,
    });
    set.insert(BlockId {
        height: 1,
        hash: h1_bad_hash,
    });
    set.insert(BlockId {
        height: 2,
        hash: h2_hash,
    });
    cs.branches.extend_branch(
        BlockId {
            height: 2,
            hash: h2_hash,
        },
        set,
    );

    let graph = BlockGraph::<Header>::from_changeset(cs)
        .expect("genesis is present and self-consistent");
    // The branch must be truncated at the unlinkable height — we keep only genesis.
    assert_eq!(graph.tip_count(), 1);
    assert_eq!(graph.get_chain_tip().unwrap().height, 0);
    assert_eq!(graph.get_chain_tip().unwrap().hash, g_hash);
}

#[test]
fn from_changeset_prefers_linking_candidate_at_same_height() {
    // Two candidates at the same height — one links, one doesn't. The linking one must win.
    use bdk_chain::collections::BTreeSet;
    use bitcoin::block::Header;
    use bitcoin::{hashes::Hash, CompactTarget, TxMerkleNode};

    fn header(prev: BlockHash, nonce: u32) -> Header {
        Header {
            version: bitcoin::block::Version::ONE,
            prev_blockhash: prev,
            merkle_root: TxMerkleNode::all_zeros(),
            time: 0,
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce,
        }
    }
    let zero = BlockHash::all_zeros();
    let h0 = header(zero, 0);
    let g_hash = h0.block_hash();
    // h1_bad: at height 1 but doesn't link.
    let h1_bad = header(zero, 1);
    let h1_bad_hash = h1_bad.block_hash();
    // h1_good: at height 1 and DOES link to genesis.
    let h1_good = header(g_hash, 2);
    let h1_good_hash = h1_good.block_hash();

    let mut cs = ChangeSet::<Header>::default();
    cs.blocks.insert(g_hash, h0);
    cs.blocks.insert(h1_bad_hash, h1_bad);
    cs.blocks.insert(h1_good_hash, h1_good);
    let mut set = BTreeSet::<BlockId>::new();
    set.insert(BlockId {
        height: 0,
        hash: g_hash,
    });
    set.insert(BlockId {
        height: 1,
        hash: h1_bad_hash,
    });
    set.insert(BlockId {
        height: 1,
        hash: h1_good_hash,
    });
    // Use h1_good as the declared tip (so the branch entry tries to express the linking tip).
    cs.branches.extend_branch(
        BlockId {
            height: 1,
            hash: h1_good_hash,
        },
        set,
    );

    let graph = BlockGraph::<Header>::from_changeset(cs)
        .expect("the linking candidate at height 1 should be chosen");
    assert_eq!(graph.tip_count(), 1);
    assert_eq!(graph.get_chain_tip().unwrap().height, 1);
    assert_eq!(graph.get_chain_tip().unwrap().hash, h1_good_hash);
}

#[test]
fn branches_reverse_index_stays_consistent_through_merge() {
    use bdk_chain::block_graph::Branches;
    use bdk_chain::collections::BTreeSet;

    fn check_consistency(b: &Branches) {
        // For every (tip, bid) in the forward map, `by_member[bid]` must contain `tip`.
        for (tip, set) in b.iter() {
            for bid in set {
                let containing: BTreeSet<BlockId> = b.containing(bid).collect();
                assert!(
                    containing.contains(tip),
                    "forward → reverse inconsistency: tip={tip:?} bid={bid:?}",
                );
            }
        }
        // And every `containing(bid)` entry must be backed by the forward map.
        // (Iterate by collecting unique bids first.)
        let all_bids: BTreeSet<BlockId> =
            b.iter().flat_map(|(_, s)| s.iter().copied()).collect();
        for bid in all_bids {
            for tip in b.containing(&bid) {
                assert!(
                    b.get(&tip).is_some_and(|s| s.contains(&bid)),
                    "reverse → forward inconsistency: tip={tip:?} bid={bid:?}",
                );
            }
        }
    }

    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A")), (2, hash!("B"))])
        .unwrap();
    graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("C"))])
        .unwrap();
    let cs = graph.initial_changeset();
    check_consistency(&cs.branches);

    // Now merge a delta into a fresh changeset and verify the index still consistent.
    let mut acc = ChangeSet::<BlockHash>::default();
    acc.merge(cs.clone());
    check_consistency(&acc.branches);
    acc.merge(cs);
    check_consistency(&acc.branches);

    // `containing` returns at least one tip for shared ancestors.
    let genesis_bid = block(0, "G");
    let tips_with_genesis: BTreeSet<BlockId> = acc.branches.containing(&genesis_bid).collect();
    assert!(
        tips_with_genesis.len() >= 2,
        "shared genesis should appear in every retained branch's set",
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

#[test]
fn apply_update_delta_is_linear_in_chain_length_not_quadratic_in_update_count() {
    // After N sequential tip-extensions of size H, persisted `branches[…]` BlockId
    // references should be O(H·N), not O(H·N²).
    let (mut graph, init) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let mut persisted = init;

    let n = 20_u32;
    let h = 10_u32;
    let mut last_tip_hash: BlockHash = hash!("G");
    for i in 1..=n {
        // Build a CheckPoint chain from genesis to height i*h.
        // Each new tip extends the previous tip — no fork.
        let mut chain: Vec<(u32, BlockHash)> = (0..=i * h)
            .map(|height| {
                let h: BlockHash = bitcoin::hashes::Hash::hash(
                    format!("blk-{height}").as_bytes(),
                );
                (height, h)
            })
            .collect();
        // Override (0, …) → genesis G so all updates share the same root.
        chain[0] = (0, hash!("G"));
        let cp = bdk_chain::local_chain::CheckPoint::from_blocks(chain)
            .expect("non-empty chain");
        last_tip_hash = cp.hash();
        let delta = graph.apply_update(cp).expect("connects to genesis");
        persisted.merge(delta);
    }
    let _ = last_tip_hash;

    // Total BlockId references across all `branches[…]` entries.
    let total_refs: usize = persisted
        .branches
        .iter()
        .map(|(_, set)| set.len())
        .sum();
    // Implicit-anchor design: each apply emits exactly (h + 1) refs (anchor + h new
    // BlockIds) for the new tip's branch entry. Plus the genesis branch entry from
    // `init` with a single BlockId. Total ≤ 1 + N·(H+1).
    let max_linear = 1 + (n as usize) * ((h as usize) + 1);
    assert!(
        total_refs <= max_linear,
        "branch BlockId refs should be linear: got {} > {}",
        total_refs,
        max_linear,
    );
    // And we should be far below the v1 quadratic bound (~ H·N²/2).
    let v1_quadratic_lower_bound = (h as usize) * (n as usize) * (n as usize) / 4;
    assert!(
        total_refs < v1_quadratic_lower_bound,
        "expected sub-quadratic: got {} (quadratic lower bound was {})",
        total_refs,
        v1_quadratic_lower_bound,
    );
}

#[test]
fn out_of_order_delta_quarantines_then_releases() {
    // Persistor A: synced genesis → (1, A) → (2, B). Two deltas emitted.
    let (mut graph_a, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let delta1 = graph_a
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A"))])
        .unwrap();
    let delta2 = graph_a
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A")), (2, hash!("B"))])
        .unwrap();

    // Persistor B: starts fresh at genesis, then receives delta2 BEFORE delta1.
    let (mut graph_b, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    graph_b.apply_changeset(&delta2);
    // delta2's branch is anchored at (1, A) — not present in graph_b yet → must stage.
    assert_eq!(
        graph_b.quarantined_count(),
        1,
        "delta2 should be quarantined because anchor (1, A) is unreachable",
    );
    assert_eq!(graph_b.tip_count(), 1, "still only the genesis tip live");
    assert_eq!(graph_b.get_chain_tip().unwrap(), block(0, "G"));

    // Now deliver delta1 — supplies the (1, A) anchor.
    graph_b.apply_changeset(&delta1);

    // Both fragments should now be live; the quarantined (2, B) should have promoted.
    assert_eq!(graph_b.quarantined_count(), 0);
    assert_eq!(graph_b.get_chain_tip().unwrap(), block(2, "B"));

    // Order-equivalent state to graph_a (canonical).
    assert_eq!(graph_a, graph_b);
}

#[test]
fn quarantined_fragment_releases_via_highest_reachable_anchor() {
    // A persisted `branches[T]` carries multiple candidate anchors. The fragment must
    // release as soon as ANY of them becomes reachable — picking the highest. Lower
    // anchors that never become reachable must not block release.
    use bdk_chain::collections::BTreeSet;

    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));

    // Craft a changeset where `branches[(10, X)] = {(2, A), (5, B), (10, X)}`. Neither
    // (2, A) nor (5, B) is reachable from any tip — both are "ghost" anchors. (10, X)
    // is the tip and its data is shipped.
    let mut cs = graph.initial_changeset();
    cs.blocks.insert(hash!("X"), hash!("X"));
    let mut set = BTreeSet::<BlockId>::new();
    set.insert(block(2, "A"));
    set.insert(block(5, "B"));
    set.insert(block(10, "X"));
    cs.branches.extend_branch(block(10, "X"), set);
    graph.apply_changeset(&cs);
    assert_eq!(graph.quarantined_count(), 1, "no anchor reachable yet");

    // Now apply a chain that reaches (5, B) but NOT (2, A). The fragment should
    // release via the (5, B) anchor (the higher candidate).
    graph
        .apply_update(chain_update![(0, hash!("G")), (5, hash!("B"))])
        .unwrap();

    assert_eq!(
        graph.quarantined_count(),
        0,
        "fragment should release via (5, B) without (2, A) ever being reachable",
    );
    assert_eq!(graph.get_chain_tip().unwrap(), block(10, "X"));
    // The chain should run G → B → X (sparse) — no entry at height 2 because the
    // (2, A) anchor was never reachable and is correctly absent from the live chain.
    assert!(graph.tip().get(5).is_some_and(|cp| cp.hash() == hash!("B")));
    assert!(graph.tip().get(2).is_none(), "no spurious height-2 entry");
}

#[test]
fn quarantined_fragment_releases_via_lower_anchor_when_higher_unreachable() {
    // Inverse of the above: only the LOWER candidate anchor becomes reachable. The
    // fragment must still release via that anchor.
    use bdk_chain::collections::BTreeSet;

    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let mut cs = graph.initial_changeset();
    cs.blocks.insert(hash!("X"), hash!("X"));
    let mut set = BTreeSet::<BlockId>::new();
    set.insert(block(2, "A"));
    set.insert(block(5, "B"));
    set.insert(block(10, "X"));
    cs.branches.extend_branch(block(10, "X"), set);
    graph.apply_changeset(&cs);
    assert_eq!(graph.quarantined_count(), 1);

    // Reach (2, A) only.
    graph
        .apply_update(chain_update![(0, hash!("G")), (2, hash!("A"))])
        .unwrap();
    assert_eq!(graph.quarantined_count(), 0);
    assert_eq!(graph.get_chain_tip().unwrap(), block(10, "X"));
    assert!(graph.tip().get(2).is_some_and(|cp| cp.hash() == hash!("A")));
}

#[test]
fn stranded_quarantined_fragment_survives_roundtrip() {
    // Build a changeset that *only* describes an anchor-non-genesis branch — no
    // predecessor entry. `from_changeset` should stage it and the round-trip should
    // preserve it.
    use bdk_chain::collections::BTreeSet;

    let (g_only_graph, init) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let _ = g_only_graph;

    // Stranded branch: (3, C) anchored at (2, B) which is not present anywhere.
    let mut stranded = init.clone();
    let stranded_tip = block(3, "C");
    stranded.blocks.insert(hash!("B"), hash!("B"));
    stranded.blocks.insert(hash!("C"), hash!("C"));
    let mut set = BTreeSet::<BlockId>::new();
    set.insert(block(2, "B")); // anchor — height > 0, no predecessor branch
    set.insert(block(3, "C"));
    stranded.branches.extend_branch(stranded_tip, set);

    let graph = BlockGraph::<BlockHash>::from_changeset(stranded)
        .expect("genesis is present");
    assert_eq!(graph.tip_count(), 1, "only genesis is reachable");
    assert_eq!(graph.quarantined_count(), 1, "stranded fragment is quarantined");
    assert_eq!(graph.get_chain_tip().unwrap(), block(0, "G"));

    // Round-trip via initial_changeset → from_changeset preserves the quarantined fragment.
    let cs = graph.initial_changeset();
    let rebuilt = BlockGraph::<BlockHash>::from_changeset(cs).unwrap();
    assert_eq!(graph, rebuilt);
}

#[test]
fn apply_update_delta_shape_uses_anchor_not_full_chain() {
    // After two sequential apply_update calls, the second delta's `branches` entry
    // for the new tip should contain only `{previous_tip, new_tip}` — not the full
    // genesis-to-new-tip set. (This locks in the anchor encoding semantics.)
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let _d1 = graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A"))])
        .unwrap();
    let d2 = graph
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("A")), (2, hash!("B"))])
        .unwrap();

    // d2.branches has exactly one entry: tip (2, B) → {(1, A), (2, B)}.
    let entries: Vec<(&BlockId, &bdk_chain::collections::BTreeSet<BlockId>)> =
        d2.branches.iter().collect();
    assert_eq!(entries.len(), 1, "exactly one new branch entry");
    let (tip_id, set) = entries[0];
    assert_eq!(*tip_id, block(2, "B"));
    let set: Vec<BlockId> = set.iter().copied().collect();
    assert_eq!(set, vec![block(1, "A"), block(2, "B")]);

    // Data for B is new and should be in `blocks`. Data for A should *not* be, because
    // A was already known to `self` before the second apply.
    let b_hash: BlockHash = hash!("B");
    let a_hash: BlockHash = hash!("A");
    assert!(d2.blocks.contains_key(&b_hash));
    assert!(
        !d2.blocks.contains_key(&a_hash),
        "anchor's data should not be re-emitted",
    );
}

#[test]
fn ported_local_chain_disjoint_chains_through_quarantine() {
    // Re-run a `LocalChain` "two disjoint chains cannot merge" case through the
    // quarantined-then-released path: deliver the divergent update to a fresh persistor
    // as a CHANGESET (not an update), and confirm we stage rather than reject.
    let (mut graph_a, _) = BlockGraph::<BlockHash>::from_genesis(hash!("_"));
    let delta_a = graph_a
        .apply_update(chain_update![(0, hash!("_")), (1, hash!("A"))])
        .unwrap();
    let (mut graph_b, _) = BlockGraph::<BlockHash>::from_genesis(hash!("_"));
    let delta_b = graph_b
        .apply_update(chain_update![(0, hash!("_")), (2, hash!("B"))])
        .unwrap();

    // Now build a third persistor that receives both deltas, but in REVERSE order
    // (delta_b first, then delta_a). Both anchor at genesis so neither stages, and
    // we should end up with both branches retained.
    let (mut graph_c, _) = BlockGraph::<BlockHash>::from_genesis(hash!("_"));
    graph_c.apply_changeset(&delta_b);
    graph_c.apply_changeset(&delta_a);
    assert_eq!(graph_c.tip_count(), 2);
    assert_eq!(graph_c.quarantined_count(), 0);

    // Best tip is by `(max height, lowest hash)`.
    let a_hash: BlockHash = hash!("A");
    let b_hash: BlockHash = hash!("B");
    let expected = if 2u32 > 1u32
        || (2 == 1 && b_hash < a_hash)
    {
        block(2, "B")
    } else {
        block(1, "A")
    };
    assert_eq!(graph_c.get_chain_tip().unwrap(), expected);
}

#[test]
fn release_promotes_chained_quarantined_fragments() {
    // A is quarantined anchored at B; B is quarantined anchored at (1, X) where X is genesis-rooted.
    // Once X is delivered, the cascade should promote B then A.
    let (mut graph, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));

    // Build the three pieces as separate updates from graph_a (the canonical source).
    let (mut graph_a, _) = BlockGraph::<BlockHash>::from_genesis(hash!("G"));
    let delta_x = graph_a
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("X"))])
        .unwrap();
    let delta_b = graph_a
        .apply_update(chain_update![(0, hash!("G")), (1, hash!("X")), (2, hash!("B"))])
        .unwrap();
    let delta_a = graph_a
        .apply_update(chain_update![
            (0, hash!("G")),
            (1, hash!("X")),
            (2, hash!("B")),
            (3, hash!("A"))
        ])
        .unwrap();

    // Deliver in reverse order: A first, then B, then X.
    graph.apply_changeset(&delta_a);
    assert_eq!(graph.quarantined_count(), 1);
    graph.apply_changeset(&delta_b);
    assert_eq!(graph.quarantined_count(), 2);
    graph.apply_changeset(&delta_x);

    // X unlocks B which unlocks A — cascade.
    assert_eq!(graph.quarantined_count(), 0, "all promoted via cascade");
    assert_eq!(graph.get_chain_tip().unwrap(), block(3, "A"));
    assert_eq!(graph, graph_a);
}

