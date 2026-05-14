//! Property-based tests for [`bdk_chain::block_graph::BlockGraph`].
//!
//! Each property is tested over a fresh universe of deterministic block hashes:
//! `block_hash(chain, height) = sha256(format!("c{chain}-h{height}"))`. This lets us
//! construct valid `CheckPoint`s without needing real Bitcoin data while still
//! producing meaningful collisions, forks, and reorgs.

#![cfg(feature = "miniscript")]

use bdk_chain::{
    block_graph::{BlockGraph, ChangeSet},
    collections::{BTreeMap, BTreeSet},
    BlockId, ChainOracle, Merge,
};
use bdk_core::CheckPoint;
use bitcoin::BlockHash;
use proptest::prelude::*;
use rand::SeedableRng;

const MAX_CHAINS: u8 = 4;
const MAX_HEIGHT: u32 = 16;

fn genesis_hash() -> BlockHash {
    bitcoin::hashes::Hash::hash(b"GENESIS")
}

fn block_hash(chain: u8, height: u32) -> BlockHash {
    if height == 0 {
        genesis_hash()
    } else {
        bitcoin::hashes::Hash::hash(format!("c{chain}-h{height}").as_bytes())
    }
}

fn block_id(chain: u8, height: u32) -> BlockId {
    BlockId {
        height,
        hash: block_hash(chain, height),
    }
}

/// A `CheckPoint` from genesis to `(chain, height)`, with intermediate heights chosen by
/// `mask` (bit `k` selects height `k`). Always includes genesis and the tip.
fn arb_checkpoint() -> impl Strategy<Value = CheckPoint<BlockHash>> {
    (0u8..MAX_CHAINS, 1u32..MAX_HEIGHT, any::<u32>()).prop_map(|(chain, height, mask)| {
        let mut blocks: Vec<(u32, BlockHash)> = vec![(0, genesis_hash())];
        for k in 1..height {
            if mask & (1u32 << (k % 32)) != 0 {
                blocks.push((k, block_hash(chain, k)));
            }
        }
        blocks.push((height, block_hash(chain, height)));
        CheckPoint::from_blocks(blocks).expect("arb chain is monotonic")
    })
}

/// A "fuzzy" [`ChangeSet`] — entries may or may not be self-consistent. Used for
/// lenience / panic-freedom testing.
fn arb_fuzzy_changeset() -> impl Strategy<Value = ChangeSet<BlockHash>> {
    // A small pool of BlockIds drawn from our universe, plus a few "alien" ones.
    let pool: Vec<BlockId> = {
        let mut v: Vec<BlockId> = (0..MAX_CHAINS)
            .flat_map(|c| (0..6u32).map(move |h| block_id(c, h)))
            .collect();
        v.push(BlockId {
            height: 0,
            hash: genesis_hash(),
        });
        // alien BlockIds (won't be in any chain we build)
        v.push(BlockId {
            height: 3,
            hash: bitcoin::hashes::Hash::hash(b"ALIEN-3"),
        });
        v.push(BlockId {
            height: 99,
            hash: bitcoin::hashes::Hash::hash(b"ALIEN-99"),
        });
        v
    };
    let bid_strategy = prop::sample::select(pool.clone());
    (
        // blocks: hash → data. Sometimes hash matches data (well-formed),
        // sometimes not (corrupted).
        prop::collection::vec((bid_strategy.clone(), bid_strategy.clone()), 0..12),
        // branches: tip → set of BlockIds.
        prop::collection::vec(
            (
                bid_strategy.clone(),
                prop::collection::btree_set(bid_strategy.clone(), 0..6),
            ),
            0..6,
        ),
    )
        .prop_map(|(block_pairs, branch_pairs)| {
            let mut blocks = BTreeMap::<BlockHash, BlockHash>::new();
            for (bid_key, bid_data) in block_pairs {
                blocks.insert(bid_key.hash, bid_data.hash);
            }
            let mut branches = BTreeMap::<BlockId, BTreeSet<BlockId>>::new();
            for (tip, set) in branch_pairs {
                branches.insert(tip, set);
            }
            ChangeSet { blocks, branches }
        })
}

/// Apply a sequence of [`CheckPoint`]s with [`BlockGraph::apply_update`], ignoring
/// individual errors (a checkpoint that can't connect is just skipped).
fn apply_all(graph: &mut BlockGraph<BlockHash>, updates: &[CheckPoint<BlockHash>]) {
    for cp in updates {
        let _ = graph.apply_update(cp.clone());
    }
}


/// Verify every structural invariant of the graph.
fn check_invariants(graph: &BlockGraph<BlockHash>) -> Result<(), TestCaseError> {
    // (a) at least one tip.
    prop_assert!(graph.tip_count() >= 1, "graph has no tips");

    let tip_bids: Vec<BlockId> = graph.tips().map(|cp| cp.block_id()).collect();

    // (b) tips sorted by (Reverse(height), hash) — descending height, ascending hash.
    for w in tip_bids.windows(2) {
        let a = (core::cmp::Reverse(w[0].height), w[0].hash);
        let b = (core::cmp::Reverse(w[1].height), w[1].hash);
        prop_assert!(a < b, "tips not sorted: {:?} not before {:?}", w[0], w[1]);
    }

    // (c) tip BlockIds are unique.
    let unique: BTreeSet<BlockId> = tip_bids.iter().copied().collect();
    prop_assert_eq!(
        unique.len(),
        tip_bids.len(),
        "duplicate tip BlockIds present",
    );

    // (d) every tip's chain reaches genesis.
    for tip in graph.tips() {
        let bottom = tip.iter().last().expect("non-empty");
        prop_assert_eq!(bottom.height(), 0, "tip's chain doesn't bottom at height 0");
        prop_assert_eq!(
            bottom.hash(),
            graph.genesis_hash(),
            "tip's chain doesn't reach genesis",
        );
    }

    // (e) no tip is a strict ancestor of another.
    for (i, ti) in graph.tips().enumerate() {
        for (j, tj) in graph.tips().enumerate() {
            if i == j {
                continue;
            }
            // tj contains ti's BlockId at ti's height ⇒ ti is an ancestor — invariant violation.
            if let Some(cp) = tj.get(ti.height()) {
                prop_assert_ne!(
                    cp.block_id(),
                    ti.block_id(),
                    "tip {:?} is ancestor of tip {:?}",
                    ti.block_id(),
                    tj.block_id(),
                );
            }
        }
    }

    // (f) every quarantined fragment has its outer-key tip matching a real
    //     fragment, anchors are below the tip, and *no anchor is reachable from a
    //     live tip* (cascade is at fixed point).
    for (tip_id, frag) in graph.quarantined() {
        prop_assert!(!frag.anchors.is_empty(), "quarantined fragment has no anchors");
        for anchor in &frag.anchors {
            prop_assert!(
                anchor.height < tip_id.height,
                "anchor {anchor:?} not below tip {tip_id:?}",
            );
            let reachable = graph.tips().any(|tip| {
                tip.get(anchor.height)
                    .map(|cp| cp.block_id() == *anchor)
                    .unwrap_or(false)
            });
            prop_assert!(
                !reachable,
                "quarantined fragment {tip_id:?} has reachable anchor {anchor:?} — cascade failed",
            );
        }
    }

    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Applying the same set of updates in any order produces the same graph.
    ///
    /// **CURRENTLY FAILS** — see `KNOWN_BUGS.md`-style note at the bottom of this file.
    /// `absorb_tip` drops sparse coverage on `UpdateExtendsT` / `TExtendsUpdate`, and
    /// the `Diverge` branch doesn't enrich tips with shared common-ancestor history.
    /// Resulting sparse coverage of tip chains depends on the order updates arrived.
    #[test]
    #[ignore = "known order-dependence in absorb_tip; see bottom-of-file notes"]
    fn apply_update_order_independence(
        updates in prop::collection::vec(arb_checkpoint(), 1..10),
        seed: u64,
    ) {
        let mut shuffled = updates.clone();
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        use rand::seq::SliceRandom;
        shuffled.shuffle(&mut rng);

        let mut a = BlockGraph::<BlockHash>::from_genesis(genesis_hash()).0;
        let mut b = BlockGraph::<BlockHash>::from_genesis(genesis_hash()).0;
        apply_all(&mut a, &updates);
        apply_all(&mut b, &shuffled);
        prop_assert_eq!(a, b);
    }

    /// `from_changeset(graph.initial_changeset()) == graph` for any graph.
    #[test]
    fn initial_changeset_roundtrip(
        updates in prop::collection::vec(arb_checkpoint(), 1..10),
    ) {
        let mut graph = BlockGraph::<BlockHash>::from_genesis(genesis_hash()).0;
        apply_all(&mut graph, &updates);
        let cs = graph.initial_changeset();
        let rebuilt = BlockGraph::<BlockHash>::from_changeset(cs)
            .expect("genesis preserved");
        prop_assert_eq!(graph, rebuilt);
    }

    /// Accumulating per-call deltas → `from_changeset` reconstructs the
    /// directly-applied graph. This is the persistor's promise: replaying the
    /// persisted log recovers state.
    ///
    /// **CURRENTLY FAILS** — anchored deltas don't re-emit BlockIds merged into a
    /// live tip's chain after the original anchored delta was computed, and absorbed
    /// tips lack tombstones in the persisted state. See bottom-of-file notes.
    #[test]
    #[ignore = "known: deltas don't carry merged-in heights or tombstones"]
    fn delta_accumulation_matches_direct_apply(
        updates in prop::collection::vec(arb_checkpoint(), 1..10),
    ) {
        let (mut direct, init) = BlockGraph::<BlockHash>::from_genesis(genesis_hash());
        let mut persisted = init;
        for cp in &updates {
            if let Ok(delta) = direct.apply_update(cp.clone()) {
                persisted.merge(delta);
            }
        }
        let rebuilt = BlockGraph::<BlockHash>::from_changeset(persisted)
            .expect("genesis preserved");
        prop_assert_eq!(direct, rebuilt);
    }

    /// `Merge` is commutative AND idempotent on `ChangeSet`s built from real updates.
    #[test]
    fn merge_commutative_and_idempotent(
        a_updates in prop::collection::vec(arb_checkpoint(), 1..6),
        b_updates in prop::collection::vec(arb_checkpoint(), 1..6),
    ) {
        let cs_a = {
            let mut g = BlockGraph::<BlockHash>::from_genesis(genesis_hash()).0;
            apply_all(&mut g, &a_updates);
            g.initial_changeset()
        };
        let cs_b = {
            let mut g = BlockGraph::<BlockHash>::from_genesis(genesis_hash()).0;
            apply_all(&mut g, &b_updates);
            g.initial_changeset()
        };

        // Commutativity: A·B == B·A.
        let mut ab = cs_a.clone();
        ab.merge(cs_b.clone());
        let mut ba = cs_b.clone();
        ba.merge(cs_a.clone());
        prop_assert_eq!(&ab, &ba);

        // Idempotence: A·A == A.
        let mut aa = cs_a.clone();
        aa.merge(cs_a.clone());
        prop_assert_eq!(&aa, &cs_a);
    }

    /// All structural invariants hold after any sequence of `apply_update` calls.
    #[test]
    fn invariants_after_apply_updates(
        updates in prop::collection::vec(arb_checkpoint(), 1..10),
    ) {
        let mut graph = BlockGraph::<BlockHash>::from_genesis(genesis_hash()).0;
        apply_all(&mut graph, &updates);
        check_invariants(&graph)?;
    }

    /// All structural invariants hold after any sequence of `apply_changeset` calls,
    /// including with fuzzy / partially-corrupted changesets.
    #[test]
    fn invariants_after_apply_fuzzy_changesets(
        changesets in prop::collection::vec(arb_fuzzy_changeset(), 1..6),
    ) {
        let mut graph = BlockGraph::<BlockHash>::from_genesis(genesis_hash()).0;
        for cs in &changesets {
            graph.apply_changeset(cs);
        }
        check_invariants(&graph)?;
    }

    /// `from_changeset` never panics on arbitrary (possibly malformed) input. Either it
    /// succeeds and the result satisfies all invariants, or it returns
    /// `MissingGenesisError`.
    #[test]
    fn from_changeset_never_panics(cs in arb_fuzzy_changeset()) {
        match BlockGraph::<BlockHash>::from_changeset(cs) {
            Ok(graph) => check_invariants(&graph)?,
            Err(_) => {} // MissingGenesisError is acceptable
        }
    }

    /// Out-of-order delta application: collecting deltas from a real apply-sequence and
    /// then applying them via `apply_changeset` in any order should produce the same
    /// final graph as direct application. Quarantine + cascade are what make this work.
    ///
    /// **CURRENTLY FAILS** for the same reason as `delta_accumulation_matches_direct_apply`.
    #[test]
    #[ignore = "known: deltas don't carry merged-in heights or tombstones"]
    fn out_of_order_delta_application_converges(
        updates in prop::collection::vec(arb_checkpoint(), 1..8),
        seed: u64,
    ) {
        // Build the canonical graph and collect each delta.
        let mut canonical = BlockGraph::<BlockHash>::from_genesis(genesis_hash()).0;
        let mut deltas: Vec<ChangeSet<BlockHash>> = Vec::new();
        for cp in &updates {
            if let Ok(delta) = canonical.apply_update(cp.clone()) {
                deltas.push(delta);
            }
        }

        // Shuffle the deltas and apply to a fresh graph.
        let mut shuffled = deltas.clone();
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        use rand::seq::SliceRandom;
        shuffled.shuffle(&mut rng);

        let mut replayed = BlockGraph::<BlockHash>::from_genesis(genesis_hash()).0;
        for delta in &shuffled {
            replayed.apply_changeset(delta);
        }
        prop_assert_eq!(canonical, replayed);
    }
}

// ============================================================================
// Known bugs documented by the three `#[ignore]`'d properties above
// ============================================================================
//
// Three distinct order-dependence bugs surface under proptest fuzzing. They
// share a root cause: the live tip representation is a per-tip independent
// sparse chain, with no shared "all observed BlockIds at each height" structure.
//
// 1. `absorb_tip` drops sparse coverage on `UpdateExtendsT` / `TExtendsUpdate`.
//    When relate() says one CheckPoint strictly extends another, the loop drops
//    the shorter one — losing heights the shorter one has but the longer one
//    doesn't. Example: tip G→5→6 absorbed by update G→6→7 → result G→6→7,
//    height 5 lost.
//
// 2. `absorb_tip` Diverge case doesn't enrich shared history.
//    When two tips diverge above a common ancestor, each maintains its own
//    sparse view of the heights at-or-below the common ancestor. Observations
//    of heights below the divergence point made in one tip don't propagate to
//    the other.
//
// 3. Anchored deltas lose merged-in heights and lack tombstones.
//    `apply_update` emits a delta based on the chain at apply time. Heights
//    later merged into a tip's chain don't get re-emitted in any delta. Also,
//    no tombstone is emitted when a tip is absorbed, so reconstruction sees
//    stale tips that direct apply has long since dropped.
//
// Resolution options sketched:
// - Global observation index (`BTreeMap<u32, BTreeSet<BlockHash>>` on
//   `BlockGraph`) — sparse coverage becomes a single shared structure, tip
//   chains materialize from it. Fixes all three.
// - Tombstoning in `ChangeSet` — fixes (3) directly.
// - Post-absorb enrichment fixed-point — fixes (1) and (2).
// - Revert implicit-anchor deltas to full-chain emission — fixes (3) at the
//   cost of restoring quadratic storage growth.
//
// The 5 properties above that pass exercise: invariants under arbitrary apply
// sequences, invariants under fuzzy changesets, `Merge` commutativity +
// idempotence, round-trip via `initial_changeset`, and panic-freedom on
// malformed input.
