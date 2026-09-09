#![cfg(feature = "miniscript")]

use std::collections::{BTreeMap, HashSet};

use bdk_chain::{
    local_chain::LocalChain, BlockId, CanonicalTx, CanonicalView, ChainPosition,
    ConfirmationBlockTime, Eligibility, Trust, TxGraph,
};
use bdk_testenv::{hash, utils::new_tx};
use bitcoin::{Amount, BlockHash, OutPoint, ScriptBuf, Transaction, TxIn, TxOut, Txid};

/// Builds an `is_settled` predicate requiring at least `min_confirmations` confirmations.
fn settled(
    tip_height: u32,
    min_confirmations: u32,
) -> impl Fn(&ChainPosition<ConfirmationBlockTime>) -> bool {
    let min_confirmations = min_confirmations.max(1); // 0 and 1 behave identically
    move |pos| {
        pos.confirmation_height_upper_bound()
            .is_some_and(|h| tip_height.saturating_sub(h).saturating_add(1) >= min_confirmations)
    }
}

/// A `does_taint` predicate that taints nothing directly, leaving trust entirely to ancestry.
fn no_direct_taint(_: &CanonicalTx<ChainPosition<ConfirmationBlockTime>>) -> bool {
    false
}

/// A [`LocalChain`] of blocks `0..=height`, each with a distinct hash.
fn chain_to_height(height: u32) -> LocalChain {
    let blocks: BTreeMap<u32, BlockHash> = (0..=height)
        .map(|h| {
            (
                h,
                <BlockHash as bitcoin::hashes::Hash>::hash(&h.to_le_bytes()),
            )
        })
        .collect();
    LocalChain::from_blocks(blocks).unwrap()
}

/// A transaction with locktime `lt` spending `spends`, paying each of `values` to its own output.
fn tx_spending(lt: u32, spends: OutPoint, values: &[u64]) -> Transaction {
    Transaction {
        input: vec![TxIn {
            previous_output: spends,
            ..Default::default()
        }],
        output: values
            .iter()
            .map(|value| TxOut {
                value: Amount::from_sat(*value),
                script_pubkey: ScriptBuf::new(),
            })
            .collect(),
        ..new_tx(lt)
    }
}

/// Inserts `tx` into `tx_graph` anchored at `height`, returning its txid.
fn insert_anchored(
    tx_graph: &mut TxGraph<ConfirmationBlockTime>,
    chain: &LocalChain,
    tx: Transaction,
    height: u32,
) -> Txid {
    let txid = tx.compute_txid();
    let _ = tx_graph.insert_tx(tx);
    let _ = tx_graph.insert_anchor(
        txid,
        ConfirmationBlockTime {
            block_id: chain.get(height).unwrap().block_id(),
            confirmation_time: height as u64 * 100,
        },
    );
    txid
}

/// Inserts `tx` into `tx_graph` as unconfirmed, seen in the mempool at `seen_at`.
fn insert_unconfirmed(
    tx_graph: &mut TxGraph<ConfirmationBlockTime>,
    tx: Transaction,
    seen_at: u64,
) -> Txid {
    let txid = tx.compute_txid();
    let _ = tx_graph.insert_tx(tx);
    let _ = tx_graph.insert_seen_at(txid, seen_at);
    txid
}

/// A `does_taint` predicate: a transaction taints when it spends an outpoint outside `owned`.
fn taints_outside(
    owned: &HashSet<OutPoint>,
) -> impl Fn(&CanonicalTx<ChainPosition<ConfirmationBlockTime>>) -> bool + '_ {
    move |c_tx| {
        c_tx.tx
            .input
            .iter()
            .any(|txin| !owned.contains(&txin.previous_output))
    }
}

/// The [`Eligibility`] of a single outpoint, which must be unspent and present in `view`.
fn eligibility_of(
    view: &CanonicalView<ConfirmationBlockTime>,
    op: OutPoint,
    does_taint: impl FnMut(&CanonicalTx<ChainPosition<ConfirmationBlockTime>>) -> bool,
    is_settled: impl Fn(&ChainPosition<ConfirmationBlockTime>) -> bool,
) -> Eligibility {
    view.classify_outpoints([op], does_taint, is_settled)
        .next()
        .expect("outpoint must be unspent and in view")
        .1
}

#[test]
fn test_is_settled_boundary() {
    let chain = chain_to_height(10);
    let mut tx_graph = TxGraph::default();

    let parent = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(0, OutPoint::new(hash!("root"), 0), &[50_000]),
        1,
    );
    // Transaction confirmed at height 5, tip at height 10 (6 confirmations)
    let txid = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(1, OutPoint::new(parent, 0), &[50_000]),
        5,
    );
    let outpoint = OutPoint::new(txid, 0);

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());
    let tip_height = view.tip().height;
    // Test min_confirmations = 1: Should be confirmed (has 6 confirmations)
    let balance_1_conf = view.balance([outpoint], no_direct_taint, settled(tip_height, 1));
    assert_eq!(balance_1_conf.confirmed, Amount::from_sat(50_000));
    assert_eq!(balance_1_conf.trusted_pending, Amount::ZERO);

    // Test min_confirmations = 6: Should be confirmed (has exactly 6 confirmations)
    let balance_6_conf = view.balance([outpoint], no_direct_taint, settled(tip_height, 6));
    assert_eq!(balance_6_conf.confirmed, Amount::from_sat(50_000));
    assert_eq!(balance_6_conf.trusted_pending, Amount::ZERO);

    // Test min_confirmations = 7: Should be trusted pending (only has 6 confirmations)
    let balance_7_conf = view.balance([outpoint], no_direct_taint, settled(tip_height, 7));
    assert_eq!(balance_7_conf.confirmed, Amount::ZERO);
    assert_eq!(balance_7_conf.trusted_pending, Amount::from_sat(50_000));
}

#[test]
fn test_min_confirmations_with_untrusted_tx() {
    let chain = chain_to_height(10);
    let mut tx_graph = TxGraph::default();

    // A settled parent, so ancestry alone would make the child trusted and `does_taint` is what
    // decides the outcome.
    let parent = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(0, OutPoint::new(hash!("root"), 0), &[25_000]),
        1,
    );
    // Anchor at height 8, tip at height 10 (3 confirmations)
    let txid = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(1, OutPoint::new(parent, 0), &[25_000]),
        8,
    );
    let outpoint = OutPoint::new(txid, 0);

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());
    let tip_height = view.tip().height;

    // Test with min_confirmations = 5 and everything tainted
    let tainted = view.balance([outpoint], |_tx| true, settled(tip_height, 5));
    assert_eq!(tainted.confirmed, Amount::ZERO);
    assert_eq!(tainted.trusted_pending, Amount::ZERO);
    assert_eq!(tainted.untrusted_pending, Amount::from_sat(25_000));

    // Without the taint, the settled ancestry makes it trusted instead.
    let untainted = view.balance([outpoint], no_direct_taint, settled(tip_height, 5));
    assert_eq!(untainted.trusted_pending, Amount::from_sat(25_000));
    assert_eq!(untainted.untrusted_pending, Amount::ZERO);
}

#[test]
fn test_min_confirmations_multiple_transactions() {
    let chain = chain_to_height(15);
    let mut tx_graph = TxGraph::default();

    // A deeply-confirmed parent, so every child has a known, trusted ancestry and the test is
    // left measuring the threshold alone.
    let parent = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(
            0,
            OutPoint::new(hash!("root"), 0),
            &[10_000, 20_000, 30_000],
        ),
        1,
    );
    // (value, mined height) -> confirmations at tip 15 are 11, 6 and 3 respectively.
    let outpoints = [(10_000, 5), (20_000, 10), (30_000, 13)]
        .into_iter()
        .enumerate()
        .map(|(vout, (value, height))| {
            let txid = insert_anchored(
                &mut tx_graph,
                &chain,
                tx_spending(
                    vout as u32 + 1,
                    OutPoint::new(parent, vout as u32),
                    &[value],
                ),
                height,
            );
            OutPoint::new(txid, 0)
        })
        .collect::<Vec<_>>();

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());
    let tip_height = view.tip().height;

    // Test with min_confirmations = 5
    // tx0: 11 confirmations -> confirmed
    // tx1: 6 confirmations -> confirmed
    // tx2: 3 confirmations -> trusted pending
    let balance = view.balance(outpoints.clone(), no_direct_taint, settled(tip_height, 5));
    assert_eq!(balance.confirmed, Amount::from_sat(10_000 + 20_000));
    assert_eq!(balance.trusted_pending, Amount::from_sat(30_000));
    assert_eq!(balance.untrusted_pending, Amount::ZERO);

    // Test with min_confirmations = 10
    // tx0: 11 confirmations -> confirmed
    // tx1: 6 confirmations -> trusted pending
    // tx2: 3 confirmations -> trusted pending
    let balance = view.balance(outpoints, no_direct_taint, settled(tip_height, 10));
    assert_eq!(balance.confirmed, Amount::from_sat(10_000));
    assert_eq!(balance.trusted_pending, Amount::from_sat(20_000 + 30_000));
    assert_eq!(balance.untrusted_pending, Amount::ZERO);
}

/// Taint reaches a descendant through unconfirmed ancestors, while a sibling funded only by our own
/// confirmed coin stays trusted.
#[test]
fn test_balance_taint_propagates_through_unconfirmed_ancestry() {
    let chain = chain_to_height(2);
    let mut tx_graph = TxGraph::<ConfirmationBlockTime>::default();

    // A confirmed coin we own.
    let coin = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(0, OutPoint::new(hash!("coinbase"), 0), &[100_000]),
        1,
    );
    // Unconfirmed, spends our own confirmed coin -> not tainted -> trusted_pending.
    let trusted = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(1, OutPoint::new(coin, 0), &[40_000]),
        1000,
    );
    // Unconfirmed, funded by a third party (spends a foreign outpoint) -> taints itself.
    let foreign = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(2, OutPoint::new(hash!("third_party"), 0), &[30_000]),
        1000,
    );
    // Unconfirmed, spends our own `foreign` output -> tainted via its ancestor `foreign`.
    let chained = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(3, OutPoint::new(foreign, 0), &[25_000]),
        1000,
    );

    // The set of outpoints we own.
    let owned = [coin, trusted, foreign, chained]
        .map(|txid| OutPoint::new(txid, 0))
        .into_iter()
        .collect::<HashSet<_>>();

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());

    // Our unspent owned outputs: `trusted` and `chained` (the others are spent).
    let balance = view.balance(
        [OutPoint::new(trusted, 0), OutPoint::new(chained, 0)],
        taints_outside(&owned),
        |pos| pos.is_confirmed(),
    );

    assert_eq!(balance.confirmed, Amount::ZERO);
    assert_eq!(balance.immature, Amount::ZERO);
    // `trusted` spends only our own (confirmed) coin -> trusted.
    assert_eq!(balance.trusted_pending, Amount::from_sat(40_000));
    // `chained` inherits taint from its foreign-funded ancestor `foreign`.
    assert_eq!(balance.untrusted_pending, Amount::from_sat(25_000));
}

/// `is_settled` is the sole authority on the settled boundary: a caller may treat an unconfirmed
/// output as settled, and its value must be counted (as settled), never silently dropped.
#[test]
fn test_balance_is_settled_is_authoritative_for_unconfirmed() {
    let chain = chain_to_height(1);
    let mut tx_graph = TxGraph::<ConfirmationBlockTime>::default();
    let txid = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(1, OutPoint::new(hash!("parent"), 0), &[50_000]),
        1000,
    );

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());

    // An `is_settled` that claims everything is settled counts the (mature, non-coinbase)
    // unconfirmed output as settled rather than dropping it, even when it is tainted.
    let balance = view.balance([OutPoint::new(txid, 0)], |_| true, |_| true);
    assert_eq!(balance.confirmed, Amount::from_sat(50_000));
    assert_eq!(balance.immature, Amount::ZERO);
    assert_eq!(balance.trusted_pending, Amount::ZERO);
    assert_eq!(balance.untrusted_pending, Amount::ZERO);
}

/// Taint must not cross the settled boundary: a settled (mined) ancestor that `does_taint` would
/// flag does not taint its unsettled descendants, because the walk stops at settled transactions
/// and never taints them.
#[test]
fn test_balance_taint_stops_at_settled_ancestor() {
    let chain = chain_to_height(2);
    let mut tx_graph = TxGraph::<ConfirmationBlockTime>::default();

    // A *settled* (confirmed) tx that itself spends a third-party coin — `does_taint` would flag
    // it.
    let settled_foreign = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(0, OutPoint::new(hash!("third_party"), 0), &[50_000]),
        1,
    );
    // Unconfirmed child spending our own (settled) output.
    let child = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(1, OutPoint::new(settled_foreign, 0), &[45_000]),
        1000,
    );

    let owned = [settled_foreign, child]
        .map(|txid| OutPoint::new(txid, 0))
        .into_iter()
        .collect::<HashSet<_>>();

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());

    // The foreign-spending ancestor is settled, so the walk stops there and never taints `child`.
    assert_eq!(
        eligibility_of(
            &view,
            OutPoint::new(child, 0),
            taints_outside(&owned),
            |pos| pos.is_confirmed()
        ),
        Eligibility::Unsettled(Trust::Trusted)
    );
}

/// `classify_outpoints` distinguishes an immature coinbase from a settled output.
#[test]
fn test_classify_immature_and_settled() {
    let chain = chain_to_height(2);
    let mut tx_graph = TxGraph::<ConfirmationBlockTime>::default();

    // Coinbase confirmed at height 1; far below `COINBASE_MATURITY` -> immature.
    let coinbase_tx = tx_spending(0, OutPoint::null(), &[50_000]);
    assert!(
        coinbase_tx.is_coinbase(),
        "a null previous_output is a coinbase"
    );
    let coinbase = insert_anchored(&mut tx_graph, &chain, coinbase_tx, 1);
    // A normal (non-coinbase) confirmed output.
    let normal = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(1, OutPoint::new(hash!("ext"), 0), &[30_000]),
        1,
    );

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());
    let ops = [OutPoint::new(coinbase, 0), OutPoint::new(normal, 0)];

    assert_eq!(
        eligibility_of(&view, ops[0], no_direct_taint, |pos| pos.is_confirmed()),
        Eligibility::Immature
    );
    assert_eq!(
        eligibility_of(&view, ops[1], no_direct_taint, |pos| pos.is_confirmed()),
        Eligibility::Settled
    );

    // The balance buckets reflect the same classification.
    let balance = view.balance(ops, no_direct_taint, |pos| pos.is_confirmed());
    assert_eq!(balance.immature, Amount::from_sat(50_000));
    assert_eq!(balance.confirmed, Amount::from_sat(30_000));
}

/// Two UTXOs that share one tainting ancestor are both untrusted. Because the taint cache is shared
/// across outpoints, that common ancestor is only walked once.
#[test]
fn test_balance_taint_shared_ancestor() {
    let chain = chain_to_height(1);
    let mut tx_graph = TxGraph::<ConfirmationBlockTime>::default();

    // Unconfirmed, funded by a third party -> taints itself. Two outputs.
    let foreign = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(0, OutPoint::new(hash!("third_party"), 0), &[30_000, 20_000]),
        1000,
    );
    // Two children, each spending one of `foreign`'s outputs, so both share the tainting ancestor.
    let child_a = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(1, OutPoint::new(foreign, 0), &[29_000]),
        1000,
    );
    let child_b = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(2, OutPoint::new(foreign, 1), &[19_000]),
        1000,
    );

    let owned = [
        OutPoint::new(foreign, 0),
        OutPoint::new(foreign, 1),
        OutPoint::new(child_a, 0),
        OutPoint::new(child_b, 0),
    ]
    .into_iter()
    .collect::<HashSet<_>>();

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());

    // Both children descend from the same tainting `foreign`, so both are untrusted.
    let balance = view.balance(
        [OutPoint::new(child_a, 0), OutPoint::new(child_b, 0)],
        taints_outside(&owned),
        |pos| pos.is_confirmed(),
    );
    assert_eq!(balance.trusted_pending, Amount::ZERO);
    assert_eq!(balance.untrusted_pending, Amount::from_sat(29_000 + 19_000));
}

/// `classify_outpoints` skips outpoints that are already spent or absent from the view.
#[test]
fn test_classify_skips_spent_and_unknown() {
    let chain = chain_to_height(1);
    let mut tx_graph = TxGraph::<ConfirmationBlockTime>::default();

    // `parent`'s output is spent by `child`.
    let parent = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(0, OutPoint::new(hash!("ext"), 0), &[50_000]),
        1000,
    );
    let child = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(1, OutPoint::new(parent, 0), &[40_000]),
        1000,
    );

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());

    let classified = view
        .classify_outpoints(
            [
                OutPoint::new(parent, 0),           // spent by `child` -> skipped
                OutPoint::new(hash!("unknown"), 0), // not in the view -> skipped
                OutPoint::new(child, 0),            // the only real UTXO
            ],
            no_direct_taint,
            |pos| pos.is_confirmed(),
        )
        .collect::<Vec<_>>();

    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].0.outpoint, OutPoint::new(child, 0));
}

/// An immature coinbase stays `Immature` even when `is_settled` treats it as unsettled.
#[test]
fn test_immature_coinbase_stays_immature_when_unsettled() {
    let chain = chain_to_height(1);
    let mut tx_graph = TxGraph::<ConfirmationBlockTime>::default();
    let txid = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(0, OutPoint::null(), &[50_000]),
        1,
    );

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());
    let tip_height = view.tip().height;

    // Require 3 confirmations to be settled.
    assert_eq!(
        eligibility_of(
            &view,
            OutPoint::new(txid, 0),
            no_direct_taint,
            settled(tip_height, 3)
        ),
        Eligibility::Immature
    );
}

/// Check maturity and settledness are independent axes.
#[test]
fn test_mature_coinbase_is_settled_not_immature() {
    // Tip at `COINBASE_MATURITY`, so a coinbase mined at height 1 has matured.
    let chain = chain_to_height(100);
    let mut tx_graph = TxGraph::<ConfirmationBlockTime>::default();
    let txid = insert_anchored(
        &mut tx_graph,
        &chain,
        tx_spending(0, OutPoint::null(), &[50_000]),
        1,
    );

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());
    let tip_height = view.tip().height;

    assert_eq!(
        eligibility_of(
            &view,
            OutPoint::new(txid, 0),
            no_direct_taint,
            settled(tip_height, 3)
        ),
        Eligibility::Settled
    );
}

/// An unconfirmed chain whose root parent tx is missing from the Canonical set is
/// `Unsettled(Unknown)`.
#[test]
fn test_unsettled_unknown_when_parent_root_missing() {
    let chain = chain_to_height(1);
    let mut tx_graph = TxGraph::<ConfirmationBlockTime>::default();

    // `missing_root` is never inserted.
    let child1 = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(1, OutPoint::new(hash!("missing_root"), 0), &[50_000]),
        1000,
    );
    let child2 = insert_unconfirmed(
        &mut tx_graph,
        tx_spending(2, OutPoint::new(child1, 0), &[50_000]),
        1000,
    );

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());

    assert_eq!(
        eligibility_of(&view, OutPoint::new(child2, 0), no_direct_taint, |pos| pos
            .is_confirmed()),
        Eligibility::Unsettled(Trust::Unknown)
    );
}

/// Txs with stale anchor and that have `last_evicted >= last_seen` should be excluded from
/// canonicalization.
#[test]
fn test_evicted_stale_anchored_tx_not_canonical() {
    let chain = chain_to_height(2);
    let mut tx_graph = TxGraph::default();

    let tx = tx_spending(1, OutPoint::new(hash!("parent"), 0), &[50_000]);
    let txid = tx.compute_txid();
    let _ = tx_graph.insert_tx(tx);
    // Anchored to a hash that is not the chain's block 1, so the anchor is stale.
    let _ = tx_graph.insert_anchor(
        txid,
        ConfirmationBlockTime {
            block_id: BlockId {
                height: 1,
                hash: hash!("stale"),
            },
            confirmation_time: 123456,
        },
    );
    let _ = tx_graph.insert_seen_at(txid, 100);
    let _ = tx_graph.insert_evicted_at(txid, 200);

    let view = chain.canonical_view(&tx_graph, chain.tip().block_id(), Default::default());
    assert!(
        !view.txs().any(|tx| tx.txid == txid),
        "evicted leftover tx must not be canonical"
    );
}
