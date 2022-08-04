use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::{Debug, Display},
};

use bitcoin::{OutPoint, Transaction, Txid};
use log::info;

use super::*;

/// Represents an error in bdk core.
#[derive(Debug)]
pub enum CoreError {
    /// Generic error.
    Generic(&'static str),
    /// We should check for reorg.
    ReorgDetected,
}

impl Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "core error: {}", self)
    }
}

impl std::error::Error for CoreError {}

/// Saves a [SparseChain] in persistent storage.
pub trait SparseChainPersister {
    /// Write to persistent storage.
    ///
    /// * `from_block`: write blocks from block of this index
    /// * `from_tx`: write transactions from transaction of this index
    ///
    /// Everything after `from_block` and `from_tx` is to be cleared and replaced with `delta`.
    fn rewrite_from<'a, B, T>(
        &self,
        from_block: usize,
        from_tx: usize,
        block_iter: B,
        tx_iter: T,
    ) -> Result<(), CoreError>
    where
        B: Iterator<Item = (&'a u32, &'a PartialHeader)>,
        T: Iterator<Item = (&'a (u32, Txid), &'a Transaction)>;
}

/// SparseChain implementation
pub struct SparseChain {
    // relevant blocks which contain transactions we are interested in: <height: (block_hash, block_time)>
    pub(crate) blocks: BTreeMap<u32, PartialHeader>,
    // relevant transactions lexicographically ordered by (block_height, txid)
    // unconfirmed txs have block_height as `u32::Max`
    pub(crate) txs: BTreeMap<(u32, Txid), Transaction>,
    // last attempted spends that we are aware of <spent_outpoint, (spending_txid, spending_vin)>
    pub(crate) spends: BTreeMap<OutPoint, (Txid, u32)>,
    // ref from tx to block height
    pub(crate) at_height: HashMap<Txid, u32>,
    // records persistence storage state changes (None: no change, Some: changes from...)
    pub(crate) persist_from: Option<(u32, Txid)>,
}

impl SparseChain {
    /// Iterates all txs.
    pub fn iter_txs(&self) -> impl Iterator<Item = ChainTx> + '_ {
        self.txs.iter().map(move |((height, _), tx)| ChainTx {
            tx: tx.clone(),
            confirmed_at: self.get_confirmed_at(height),
        })
    }

    /// Generates a vector of avaliable coins.
    pub fn avaliable_coins<F: CoinFilter>(
        &self,
        f: &F,
    ) -> AvaliableCoins<F::CoinGroup, F::CoinData> {
        let mut coins = AvaliableCoins::new();

        self.iter_txs()
            .filter(|chain_tx| f.filter_tx(chain_tx))
            .flat_map(|chain_tx| {
                let txid = chain_tx.tx.txid();
                let txout_iter = chain_tx.tx.output.iter();
                txout_iter
                    .enumerate()
                    .map(|(vout, txout)| {
                        let outpoint = OutPoint::new(txid, vout as _);
                        (outpoint, txout)
                    })
                    .filter(|(outpoint, _)| self.is_spent(outpoint))
                    .filter_map(|(outpoint, txout)| f.filter_coin(&outpoint, &chain_tx, txout))
                    .collect::<Vec<_>>()
            })
            .for_each(|coin| coins.entry(coin.group).or_default().push(coin));

        coins
    }

    /// A coin is spent when...
    pub fn is_spent(&self, outpoint: &OutPoint) -> bool {
        matches!(self.spends.get(outpoint), Some((txid, _)) if self.at_height.contains_key(txid))
    }

    /// Given the introduced transactions, calculate the deltas to be applied to [SparseChain].
    ///
    /// TODO: Conflict detection.
    pub fn calculate_deltas<I>(&self, mut candidates: I) -> Result<Delta<Unready>, CoreError>
    where
        I: Iterator<Item = CandidateTx>,
    {
        // candidate changes
        let mut deltas = Delta::default();

        candidates.try_for_each(
            |CandidateTx { txid, confirmed_at }| -> Result<(), CoreError> {
                // unconfirmed transactions are internally stored with height `u32::MAX`
                let tx_height = confirmed_at.map(|(h, _)| h).unwrap_or(u32::MAX);
                let tx_key = (tx_height, txid);

                // if tx of (height, txid) already exists, skip
                if deltas.tx_keys.contains(&tx_key) || self.txs.contains_key(&tx_key) {
                    info!(
                        "tx {} at height {} already exists, skipping",
                        txid, tx_height
                    );
                    return Ok(());
                }

                // if txid moved height, and the original height is not u32::MAX (unconfirmed),
                // report reorg
                if matches!(self.at_height.get(&txid), Some(h) if *h != u32::MAX) {
                    return Err(CoreError::ReorgDetected);
                }

                // if candidate tx is confirmed, check that the candidate block does not conflict with
                // blocks we know of
                if let Some((h, candidate_header)) = &confirmed_at {
                    debug_assert_eq!(tx_height, *h);

                    match deltas.blocks.get(h).or_else(|| self.blocks.get(h)) {
                        Some(header) => {
                            // expect candidate block to be the same as existing block of same height,
                            // otherwise we have a reorg
                            if header != candidate_header {
                                return Err(CoreError::ReorgDetected);
                            }
                        }
                        None => {
                            // no block exists at height, introduce candidate block
                            deltas.blocks.insert(*h, *candidate_header);
                        }
                    };
                }

                Ok(())
            },
        )?;

        Ok(deltas)
    }

    /// Flush [SparseChain] changes into persistence storage.
    pub fn flush<P: SparseChainPersister>(&mut self, p: P) -> Result<bool, CoreError> {
        match self.persist_from {
            Some((height, txid)) => {
                let from_block = self.blocks.range(..height).count();
                let from_tx = self.txs.range(..(height, txid)).count();
                let block_iter = self.blocks.range(height..);
                let tx_iter = self.txs.range((height, txid)..);

                p.rewrite_from(from_block, from_tx, block_iter, tx_iter)?;
                self.persist_from = None;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Rollback all transactions from the given height and above.
    pub fn rollback(&mut self, height: u32) -> Delta<Negated> {
        let key = (height, Txid::default());

        let removed_blocks = self.blocks.split_off(&height);
        let removed_txs = self.txs.split_off(&key);
        let removed_tx_keys = removed_txs
            .keys()
            .inspect(|(height, txid)| assert_eq!(self.at_height.remove(txid), Some(*height)))
            .cloned()
            .collect();

        self.update_persist_from(key);

        Delta {
            blocks: removed_blocks,
            tx_keys: removed_tx_keys,
            ..Default::default()
        }
    }

    /// Get transaction of txid.
    pub fn get_tx(&self, txid: Txid) -> Option<ChainTx> {
        self.at_height.get(&txid).map(|&height| {
            let tx = self
                .txs
                .get(&(height, txid))
                .expect("tx back ref was not cleared")
                .clone();

            ChainTx {
                tx,
                confirmed_at: self.get_confirmed_at(&height),
            }
        })
    }

    /// Selectively remove a single transaction of txid.
    pub fn remove_tx(&mut self, txid: Txid) -> bool {
        let height = match self.at_height.remove(&txid) {
            Some(height) => height,
            None => return false,
        };

        let key = (height, txid);
        self.txs.remove(&key).expect("tx back ref was not cleared");
        self.update_persist_from(key);

        return true;
    }

    fn get_confirmed_at(&self, height: &u32) -> Option<BlockTime> {
        let at = self
            .blocks
            .get(height)
            .map(|PartialHeader { time, .. }| BlockTime {
                height: *height,
                time: *time,
            });

        assert_eq!(
            *height == u32::MAX,
            at.is_none(),
            "when height is MAX, it should represent an unconfirmed tx"
        );

        at
    }

    fn update_persist_from(&mut self, from: (u32, Txid)) {
        self.persist_from = Some(match self.persist_from {
            Some(persist_from) => std::cmp::min(persist_from, from),
            None => from,
        });
    }
}
