use std::collections::BTreeSet;
use std::thread::JoinHandle;
use std::usize;

use bdk_chain::collections::BTreeMap;
use bdk_chain::Anchor;
use bdk_chain::{
    bitcoin::{OutPoint, ScriptBuf, TxOut, Txid},
    local_chain::{self, CheckPoint},
    BlockId, ConfirmationTimeHeightAnchor, TxGraph,
};
use esplora_client::TxStatus;

use crate::anchor_from_status;
use crate::FullScanUpdate;
use crate::SyncUpdate;

/// [`esplora_client::Error`]
pub type Error = Box<esplora_client::Error>;

/// Trait to extend the functionality of [`esplora_client::BlockingClient`].
///
/// Refer to [crate-level documentation] for more.
///
/// [crate-level documentation]: crate
pub trait EsploraExt {
    /// Scan keychain scripts for transactions against Esplora, returning an update that can be
    /// applied to the receiving structures.
    ///
    /// * `local_tip`: the previously seen tip from [`LocalChain::tip`].
    /// * `keychain_spks`: keychains that we want to scan transactions for
    ///
    /// The full scan for each keychain stops after a gap of `stop_gap` script pubkeys with no
    /// associated transactions. `parallel_requests` specifies the max number of HTTP requests to
    /// make in parallel.
    ///
    /// ## Note
    ///
    /// `stop_gap` is defined as "the maximum number of consecutive unused addresses".
    /// For example, with a `stop_gap` of  3, `full_scan` will keep scanning
    /// until it encounters 3 consecutive script pubkeys with no associated transactions.
    ///
    /// This follows the same approach as other Bitcoin-related software,
    /// such as [Electrum](https://electrum.readthedocs.io/en/latest/faq.html#what-is-the-gap-limit),
    /// [BTCPay Server](https://docs.btcpayserver.org/FAQ/Wallet/#the-gap-limit-problem),
    /// and [Sparrow](https://www.sparrowwallet.com/docs/faq.html#ive-restored-my-wallet-but-some-of-my-funds-are-missing).
    ///
    /// A `stop_gap` of 0 will be treated as a `stop_gap` of 1.
    ///
    /// [`LocalChain::tip`]: local_chain::LocalChain::tip
    fn full_scan<K: Ord + Clone>(
        &self,
        local_tip: CheckPoint,
        keychain_spks: BTreeMap<K, impl IntoIterator<Item = (u32, ScriptBuf)>>,
        stop_gap: usize,
        parallel_requests: usize,
    ) -> Result<FullScanUpdate<K>, Error>;

    /// Sync a set of scripts with the blockchain (via an Esplora client) for the data
    /// specified and return a [`TxGraph`].
    ///
    /// * `local_tip`: the previously seen tip from [`LocalChain::tip`].
    /// * `misc_spks`: scripts that we want to sync transactions for
    /// * `txids`: transactions for which we want updated [`ConfirmationTimeHeightAnchor`]s
    /// * `outpoints`: transactions associated with these outpoints (residing, spending) that we
    ///     want to include in the update
    ///
    /// If the scripts to sync are unknown, such as when restoring or importing a keychain that
    /// may include scripts that have been used, use [`full_scan`] with the keychain.
    ///
    /// [`LocalChain::tip`]: local_chain::LocalChain::tip
    /// [`full_scan`]: EsploraExt::full_scan
    fn sync(
        &self,
        local_tip: CheckPoint,
        misc_spks: impl IntoIterator<Item = ScriptBuf>,
        txids: impl IntoIterator<Item = Txid>,
        outpoints: impl IntoIterator<Item = OutPoint>,
        parallel_requests: usize,
    ) -> Result<SyncUpdate, Error>;
}

impl EsploraExt for esplora_client::BlockingClient {
    fn full_scan<K: Ord + Clone>(
        &self,
        local_tip: CheckPoint,
        keychain_spks: BTreeMap<K, impl IntoIterator<Item = (u32, ScriptBuf)>>,
        stop_gap: usize,
        parallel_requests: usize,
    ) -> Result<FullScanUpdate<K>, Error> {
        let (tx_graph, last_active_indices) = full_scan_for_index_and_graph_blocking(
            self,
            keychain_spks,
            stop_gap,
            parallel_requests,
        )?;
        let local_chain = chain_update_blocking(self, &local_tip, tx_graph.all_anchors())?;
        Ok(FullScanUpdate {
            local_chain,
            tx_graph,
            last_active_indices,
        })
    }

    fn sync(
        &self,
        local_tip: CheckPoint,
        misc_spks: impl IntoIterator<Item = ScriptBuf>,
        txids: impl IntoIterator<Item = Txid>,
        outpoints: impl IntoIterator<Item = OutPoint>,
        parallel_requests: usize,
    ) -> Result<SyncUpdate, Error> {
        let tx_graph = sync_for_index_and_graph_blocking(
            self,
            misc_spks,
            txids,
            outpoints,
            parallel_requests,
        )?;
        let local_chain = chain_update_blocking(self, &local_tip, tx_graph.all_anchors())?;
        Ok(SyncUpdate {
            local_chain,
            tx_graph,
        })
    }
}

/// Updates the chain making sure to include heights for the anchors
#[doc(hidden)]
pub fn chain_update_blocking<A: Anchor>(
    client: &esplora_client::BlockingClient,
    local_tip: &CheckPoint,
    anchors: &BTreeSet<(A, Txid)>,
) -> Result<local_chain::Update, Error> {
    let mut point_of_agreement = None;
    let mut conflicts = vec![];
    for local_cp in local_tip.iter() {
        let remote_hash = client.get_block_hash(local_cp.height())?;

        if remote_hash == local_cp.hash() {
            point_of_agreement = Some(local_cp.clone());
            break;
        } else {
            // it is not strictly necessary to include all the conflicted heights (we do need the
            // first one) but it seems prudent to make sure the updated chain's heights are a
            // superset of the existing chain after update.
            conflicts.push(BlockId {
                height: local_cp.height(),
                hash: remote_hash,
            });
        }
    }

    let mut tip = point_of_agreement.expect("remote esplora should have same genesis block");

    tip = tip
        .extend(conflicts.into_iter().rev())
        .expect("evicted are in order");

    for anchor in anchors {
        let height = anchor.0.anchor_block().height;
        if tip.query(height).is_none() {
            let hash = client.get_block_hash(height)?;
            tip = tip.insert(BlockId { height, hash });
        }
    }

    // insert the most recent blocks at the tip to make sure we update the tip and make the update
    // robust.
    for block in client.get_blocks(None)? {
        tip = tip.insert(BlockId {
            height: block.time.height,
            hash: block.id,
        });
    }

    Ok(local_chain::Update {
        tip,
        introduce_older_blocks: true,
    })
}

/// This performs a full scan to get an update for the [`TxGraph`] and
/// [`KeychainTxOutIndex`](bdk_chain::keychain::KeychainTxOutIndex).
#[doc(hidden)]
pub fn full_scan_for_index_and_graph_blocking<K: Ord + Clone>(
    client: &esplora_client::BlockingClient,
    keychain_spks: BTreeMap<K, impl IntoIterator<Item = (u32, ScriptBuf)>>,
    stop_gap: usize,
    parallel_requests: usize,
) -> Result<(TxGraph<ConfirmationTimeHeightAnchor>, BTreeMap<K, u32>), Error> {
    type TxsOfSpkIndex = (u32, Vec<esplora_client::Tx>);
    let parallel_requests = Ord::max(parallel_requests, 1);
    let mut tx_graph = TxGraph::<ConfirmationTimeHeightAnchor>::default();
    let mut last_active_indices = BTreeMap::<K, u32>::new();

    for (keychain, spks) in keychain_spks {
        let mut spks = spks.into_iter();
        let mut last_index = Option::<u32>::None;
        let mut last_active_index = Option::<u32>::None;

        loop {
            let handles = spks
                .by_ref()
                .take(parallel_requests)
                .map(|(spk_index, spk)| {
                    std::thread::spawn({
                        let client = client.clone();
                        move || -> Result<TxsOfSpkIndex, Error> {
                            let mut last_seen = None;
                            let mut spk_txs = Vec::new();
                            loop {
                                let txs = client.scripthash_txs(&spk, last_seen)?;
                                let tx_count = txs.len();
                                last_seen = txs.last().map(|tx| tx.txid);
                                spk_txs.extend(txs);
                                if tx_count < 25 {
                                    break Ok((spk_index, spk_txs));
                                }
                            }
                        }
                    })
                })
                .collect::<Vec<JoinHandle<Result<TxsOfSpkIndex, Error>>>>();

            if handles.is_empty() {
                break;
            }

            for handle in handles {
                let (index, txs) = handle.join().expect("thread must not panic")?;
                last_index = Some(index);
                if !txs.is_empty() {
                    last_active_index = Some(index);
                }
                for tx in txs {
                    let _ = tx_graph.insert_tx(tx.to_tx());
                    if let Some(anchor) = anchor_from_status(&tx.status) {
                        let _ = tx_graph.insert_anchor(tx.txid, anchor);
                    }

                    let previous_outputs = tx.vin.iter().filter_map(|vin| {
                        let prevout = vin.prevout.as_ref()?;
                        Some((
                            OutPoint {
                                txid: vin.txid,
                                vout: vin.vout,
                            },
                            TxOut {
                                script_pubkey: prevout.scriptpubkey.clone(),
                                value: prevout.value,
                            },
                        ))
                    });

                    for (outpoint, txout) in previous_outputs {
                        let _ = tx_graph.insert_txout(outpoint, txout);
                    }
                }
            }

            let last_index = last_index.expect("Must be set since handles wasn't empty.");
            let gap_limit_reached = if let Some(i) = last_active_index {
                last_index >= i.saturating_add(stop_gap as u32)
            } else {
                last_index + 1 >= stop_gap as u32
            };
            if gap_limit_reached {
                break;
            }
        }

        if let Some(last_active_index) = last_active_index {
            last_active_indices.insert(keychain, last_active_index);
        }
    }

    Ok((tx_graph, last_active_indices))
}

#[doc(hidden)]
pub fn sync_for_index_and_graph_blocking(
    client: &esplora_client::BlockingClient,
    misc_spks: impl IntoIterator<Item = ScriptBuf>,
    txids: impl IntoIterator<Item = Txid>,
    outpoints: impl IntoIterator<Item = OutPoint>,
    parallel_requests: usize,
) -> Result<TxGraph<ConfirmationTimeHeightAnchor>, Error> {
    let (mut tx_graph, _) = full_scan_for_index_and_graph_blocking(
        client,
        {
            let mut keychains = BTreeMap::new();
            keychains.insert(
                (),
                misc_spks
                    .into_iter()
                    .enumerate()
                    .map(|(i, spk)| (i as u32, spk)),
            );
            keychains
        },
        usize::MAX,
        parallel_requests,
    )?;

    let mut txids = txids.into_iter();
    loop {
        let handles = txids
            .by_ref()
            .take(parallel_requests)
            .filter(|&txid| tx_graph.get_tx(txid).is_none())
            .map(|txid| {
                std::thread::spawn({
                    let client = client.clone();
                    move || {
                        client
                            .get_tx_status(&txid)
                            .map_err(Box::new)
                            .map(|s| (txid, s))
                    }
                })
            })
            .collect::<Vec<JoinHandle<Result<(Txid, TxStatus), Error>>>>();

        if handles.is_empty() {
            break;
        }

        for handle in handles {
            let (txid, status) = handle.join().expect("thread must not panic")?;
            if let Some(anchor) = anchor_from_status(&status) {
                let _ = tx_graph.insert_anchor(txid, anchor);
            }
        }
    }

    for op in outpoints {
        if tx_graph.get_tx(op.txid).is_none() {
            if let Some(tx) = client.get_tx(&op.txid)? {
                let _ = tx_graph.insert_tx(tx);
            }
            let status = client.get_tx_status(&op.txid)?;
            if let Some(anchor) = anchor_from_status(&status) {
                let _ = tx_graph.insert_anchor(op.txid, anchor);
            }
        }

        if let Some(op_status) = client.get_output_status(&op.txid, op.vout as _)? {
            if let Some(txid) = op_status.txid {
                if tx_graph.get_tx(txid).is_none() {
                    if let Some(tx) = client.get_tx(&txid)? {
                        let _ = tx_graph.insert_tx(tx);
                    }
                    let status = client.get_tx_status(&txid)?;
                    if let Some(anchor) = anchor_from_status(&status) {
                        let _ = tx_graph.insert_anchor(txid, anchor);
                    }
                }
            }
        }
    }

    Ok(tx_graph)
}
