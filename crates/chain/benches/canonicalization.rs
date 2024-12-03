use bdk_chain::{keychain_txout::KeychainTxOutIndex, local_chain::LocalChain, IndexedTxGraph};
use bdk_core::{BlockId, CheckPoint};
use bdk_core::{ConfirmationBlockTime, TxUpdate};
use bdk_testenv::hash;
use bitcoin::{
    absolute, constants, hashes::Hash, key::Secp256k1, transaction, Amount, BlockHash, Network,
    OutPoint, ScriptBuf, Transaction, TxIn, TxOut,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use miniscript::{Descriptor, DescriptorPublicKey};
use std::sync::Arc;

type Keychain = ();
type KeychainTxGraph = IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<Keychain>>;

/// New tx guaranteed to have at least one output
fn new_tx(lt: u32) -> Transaction {
    Transaction {
        version: transaction::Version::TWO,
        lock_time: absolute::LockTime::from_consensus(lt),
        input: vec![],
        output: vec![TxOut::NULL],
    }
}

fn spk_at_index(txout_index: &KeychainTxOutIndex<Keychain>, index: u32) -> ScriptBuf {
    txout_index
        .get_descriptor(())
        .unwrap()
        .at_derivation_index(index)
        .unwrap()
        .script_pubkey()
}

fn genesis_block_id() -> BlockId {
    BlockId {
        height: 0,
        hash: constants::genesis_block(Network::Regtest).block_hash(),
    }
}

fn tip_block_id() -> BlockId {
    BlockId {
        height: 100,
        hash: BlockHash::all_zeros(),
    }
}

/// Add ancestor tx confirmed at `block_id` with `locktime` (used for uniqueness).
/// The transaction always pays 1 BTC to SPK 0.
fn add_ancestor_tx(graph: &mut KeychainTxGraph, block_id: BlockId, locktime: u32) -> OutPoint {
    let spk_0 = spk_at_index(&graph.index, 0);
    let tx = Transaction {
        input: vec![TxIn {
            previous_output: OutPoint::new(hash!("bogus"), locktime),
            ..Default::default()
        }],
        output: vec![TxOut {
            value: Amount::ONE_BTC,
            script_pubkey: spk_0,
        }],
        ..new_tx(locktime)
    };
    let txid = tx.compute_txid();
    let _ = graph.insert_tx(tx);
    let _ = graph.insert_anchor(
        txid,
        ConfirmationBlockTime {
            block_id,
            confirmation_time: 100,
        },
    );
    OutPoint { txid, vout: 0 }
}

fn setup<F: Fn(&mut KeychainTxGraph, &LocalChain)>(f: F) -> (KeychainTxGraph, LocalChain) {
    const DESC: &str = "tr([ab28dc00/86h/1h/0h]tpubDCdDtzAMZZrkwKBxwNcGCqe4FRydeD9rfMisoi7qLdraG79YohRfPW4YgdKQhpgASdvh612xXNY5xYzoqnyCgPbkpK4LSVcH5Xv4cK7johH/0/*)";
    let cp = CheckPoint::from_block_ids([genesis_block_id(), tip_block_id()])
        .expect("blocks must be chronological");
    let chain = LocalChain::from_tip(cp).unwrap();

    let (desc, _) =
        <Descriptor<DescriptorPublicKey>>::parse_descriptor(&Secp256k1::new(), DESC).unwrap();
    let mut index = KeychainTxOutIndex::new(10);
    index.insert_descriptor((), desc).unwrap();
    let mut tx_graph = KeychainTxGraph::new(index);

    f(&mut tx_graph, &chain);
    (tx_graph, chain)
}

fn run_list_canonical_txs(tx_graph: &KeychainTxGraph, chain: &LocalChain, exp_txs: usize) {
    let txs = tx_graph
        .graph()
        .list_canonical_txs(chain, chain.tip().block_id());
    assert_eq!(txs.count(), exp_txs);
}

fn run_filter_chain_txouts(tx_graph: &KeychainTxGraph, chain: &LocalChain, exp_txos: usize) {
    let utxos = tx_graph.graph().filter_chain_txouts(
        chain,
        chain.tip().block_id(),
        tx_graph.index.outpoints().clone(),
    );
    assert_eq!(utxos.count(), exp_txos);
}

fn run_filter_chain_unspents(tx_graph: &KeychainTxGraph, chain: &LocalChain, exp_utxos: usize) {
    let utxos = tx_graph.graph().filter_chain_unspents(
        chain,
        chain.tip().block_id(),
        tx_graph.index.outpoints().clone(),
    );
    assert_eq!(utxos.count(), exp_utxos);
}

pub fn many_conflicting_unconfirmed(c: &mut Criterion) {
    const CONFLICTING_TX_COUNT: u32 = 2100;
    let (tx_graph, chain) = black_box(setup(|tx_graph, _chain| {
        let previous_output = add_ancestor_tx(tx_graph, tip_block_id(), 0);
        // Create conflicting txs that spend from `previous_output`.
        let spk_1 = spk_at_index(&tx_graph.index, 1);
        for i in 1..=CONFLICTING_TX_COUNT {
            let tx = Transaction {
                input: vec![TxIn {
                    previous_output,
                    ..Default::default()
                }],
                output: vec![TxOut {
                    value: Amount::ONE_BTC - Amount::from_sat(i as u64 * 10),
                    script_pubkey: spk_1.clone(),
                }],
                ..new_tx(i)
            };
            let update = TxUpdate {
                txs: vec![Arc::new(tx)],
                ..Default::default()
            };
            let _ = tx_graph.apply_update_at(update, Some(i as u64));
        }
    }));
    c.bench_function("many_conflicting_unconfirmed::list_canonical_txs", {
        let (tx_graph, chain) = (tx_graph.clone(), chain.clone());
        move |b| b.iter(|| run_list_canonical_txs(&tx_graph, &chain, 2))
    });
    c.bench_function("many_conflicting_unconfirmed::filter_chain_txouts", {
        let (tx_graph, chain) = (tx_graph.clone(), chain.clone());
        move |b| b.iter(|| run_filter_chain_txouts(&tx_graph, &chain, 2))
    });
    c.bench_function("many_conflicting_unconfirmed::filter_chain_unspents", {
        let (tx_graph, chain) = (tx_graph.clone(), chain.clone());
        move |b| b.iter(|| run_filter_chain_unspents(&tx_graph, &chain, 1))
    });
}

pub fn many_chained_unconfirmed(c: &mut Criterion) {
    const TX_CHAIN_COUNT: u32 = 2100;
    let (tx_graph, chain) = black_box(setup(|tx_graph, _chain| {
        let mut previous_output = add_ancestor_tx(tx_graph, tip_block_id(), 0);
        // Create a chain of unconfirmed txs where each subsequent tx spends the output of the
        // previous one.
        for i in 0..TX_CHAIN_COUNT {
            // Create tx.
            let tx = Transaction {
                input: vec![TxIn {
                    previous_output,
                    ..Default::default()
                }],
                ..new_tx(i)
            };
            let txid = tx.compute_txid();
            let update = TxUpdate {
                txs: vec![Arc::new(tx)],
                ..Default::default()
            };
            let _ = tx_graph.apply_update_at(update, Some(i as u64));
            // Store the next prevout.
            previous_output = OutPoint::new(txid, 0);
        }
    }));
    c.bench_function("many_chained_unconfirmed::list_canonical_txs", {
        let (tx_graph, chain) = (tx_graph.clone(), chain.clone());
        move |b| b.iter(|| run_list_canonical_txs(&tx_graph, &chain, 2101))
    });
    c.bench_function("many_chained_unconfirmed::filter_chain_txouts", {
        let (tx_graph, chain) = (tx_graph.clone(), chain.clone());
        move |b| b.iter(|| run_filter_chain_txouts(&tx_graph, &chain, 1))
    });
    c.bench_function("many_chained_unconfirmed::filter_chain_unspents", {
        let (tx_graph, chain) = (tx_graph.clone(), chain.clone());
        move |b| b.iter(|| run_filter_chain_unspents(&tx_graph, &chain, 0))
    });
}

pub fn nested_conflicts(c: &mut Criterion) {
    const TX_COUNT: u32 = 2100;
    let (tx_graph, chain) = black_box(setup(|tx_graph, _chain| {
        let op_a = add_ancestor_tx(tx_graph, tip_block_id(), 0);
        let op_b = add_ancestor_tx(tx_graph, tip_block_id(), 1);
        for i in 0..TX_COUNT {
            let tx = if i == TX_COUNT / 2 {
                // tx spends both A, B
                Transaction {
                    input: vec![
                        TxIn {
                            previous_output: op_a,
                            ..Default::default()
                        },
                        TxIn {
                            previous_output: op_b,
                            ..Default::default()
                        },
                    ],
                    ..new_tx(i)
                }
            } else if i % 2 == 1 {
                // tx spends A
                Transaction {
                    input: vec![TxIn {
                        previous_output: op_a,
                        ..Default::default()
                    }],
                    ..new_tx(i)
                }
            } else {
                // tx spends B
                Transaction {
                    input: vec![TxIn {
                        previous_output: op_b,
                        ..Default::default()
                    }],
                    ..new_tx(i)
                }
            };

            let update = TxUpdate {
                txs: vec![Arc::new(tx)],
                ..Default::default()
            };
            let _ = tx_graph.apply_update_at(update, Some(i as u64));
        }
    }));
    c.bench_function("many_chained_unconfirmed::list_canonical_txs", {
        let (tx_graph, chain) = (tx_graph.clone(), chain.clone());
        move |b| b.iter(|| run_list_canonical_txs(&tx_graph, &chain, 4))
    });
    c.bench_function("many_chained_unconfirmed::filter_chain_txouts", {
        let (tx_graph, chain) = (tx_graph.clone(), chain.clone());
        move |b| b.iter(|| run_filter_chain_txouts(&tx_graph, &chain, 2))
    });
    c.bench_function("many_chained_unconfirmed::filter_chain_unspents", {
        let (tx_graph, chain) = (tx_graph.clone(), chain.clone());
        move |b| b.iter(|| run_filter_chain_unspents(&tx_graph, &chain, 0))
    });
}

criterion_group!(
    benches,
    many_conflicting_unconfirmed,
    many_chained_unconfirmed,
    nested_conflicts,
);
criterion_main!(benches);
