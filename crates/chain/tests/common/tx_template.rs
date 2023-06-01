use std::collections::HashMap;

use bdk_chain::{
    indexed_tx_graph::IndexedTxGraph, keychain::KeychainTxOutIndex, BlockId,
    ConfirmationHeightAnchor,
};
use bitcoin::{hashes::Hash, OutPoint, PackedLockTime, Transaction, TxIn, Txid};
use miniscript::{Descriptor, DescriptorPublicKey};

/// Transaction template.
struct TxTemplate<'a, K> {
    /// Uniquely identifies the transaction, before it can have a txid.
    pub tx_name: &'a str,
    pub spends: &'a [TxInTemplate<'a>],
    pub outputs: &'a [TxOutTemplate<K>],
    pub anchors: &'a [(u32, BlockId)],
    pub last_seen: Option<u64>,
}

enum TxInTemplate<'a> {
    /// This will give a random txid and vout.
    Bogus,

    /// Contains the `tx_name` and `vout` that we are spending. The rule is that we must only spend
    /// a previous transaction.
    PrevTx(&'a str, usize),
}

struct TxOutTemplate<K> {
    pub value: u64,
    pub owned_spk: Option<(K, u32)>, // some = derive a spk from K, none = random spk
}

fn init_graph<'a, K: 'a>(
    descriptors: HashMap<K, Descriptor<DescriptorPublicKey>>,
    tx_templates: impl IntoIterator<Item = TxTemplate<'a, K>>,
) -> (
    IndexedTxGraph<ConfirmationHeightAnchor, KeychainTxOutIndex<K>>,
    HashMap<&'a str, Txid>,
) {
    let mut bogus_txin_vout = 0;
    let mut tx_map = HashMap::<&'a str, Transaction>::new();

    for tx_tmp in tx_templates.into_iter() {
        bogus_txin_vout += 1;

        let tx = Transaction {
            version: 0,
            lock_time: PackedLockTime::ZERO,
            input: tx_tmp
                .spends
                .iter()
                .map(|spend_tmp| {
                    match spend_tmp {
                        TxInTemplate::Bogus => TxIn {
                            previous_output: OutPoint::new(
                                Txid::all_zeros(),
                                bogus_txin_vout,
                            ),
                            ..Default::default() // [TODO] How should we populate `script_sig`, `sequence` and `witness`?
                        },
                        TxInTemplate::PrevTx(prev_name, prev_vout) => {
                            let prev_txid = tx_map.get(prev_name).expect("txin template must spend from tx of template that comes before").txid();
                            TxIn {
                                previous_output: OutPoint::new(prev_txid, *prev_vout as _),
                                ..Default::default()
                            }
                        },
                    }
                })
                .collect(),
            output: Vec::new(),
        };
    }

    todo!()
}
