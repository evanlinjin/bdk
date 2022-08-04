use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
};

use bitcoin::{Transaction, Txid};

use super::*;

/// State of [Deltas].
pub trait DeltaState {}
impl DeltaState for Unready {}
impl DeltaState for Ready {}
impl DeltaState for Negated {}

/// [Deltas] is in an unready state (not all transactions exist).
#[derive(Debug, Default, Clone, Copy)]
pub struct Unready;

/// [Deltas] is in a ready state (we can apply to [SparseChain]).
#[derive(Debug, Default, Clone, Copy)]
pub struct Ready;

/// [Deltas] is in a negated state (to remove data).
#[derive(Debug, Default, Clone, Copy)]
pub struct Negated;

/// Candidate changes of [SparseChain]
#[derive(Debug, Default, Clone)]
pub struct Delta<S: DeltaState> {
    pub(crate) blocks: BTreeMap<u32, PartialHeader>,
    pub(crate) tx_keys: BTreeSet<(u32, Txid)>,

    // needs to be filled for the `Ready` state
    pub(crate) tx_values: HashMap<Txid, Transaction>,

    pub(crate) marker: PhantomData<S>,
}

impl Delta<Unready> {
    /// Iterates through missing txids.
    pub fn missing_txids(&self) -> impl Iterator<Item = Txid> + '_ {
        self.tx_keys
            .iter()
            .map(|(_, txid)| *txid)
            .filter(move |txid| !self.tx_values.contains_key(txid))
    }

    /// Fill all transactions.
    pub fn fill_transactions<I>(mut self, tx_iter: I) -> Result<Delta<Ready>, Self>
    where
        I: Iterator<Item = Transaction>,
    {
        self.tx_values.extend(tx_iter.map(|tx| (tx.txid(), tx)));

        if self.missing_txids().count() == 0 {
            Ok(Delta::<Ready> {
                blocks: self.blocks,
                tx_keys: self.tx_keys,
                tx_values: self.tx_values,
                marker: PhantomData,
            })
        } else {
            Err(self)
        }
    }
}

impl Delta<Ready> {
    /// Applies deltas to the given [SparseChain].
    ///
    /// TODO: This can also be made to be appliable to [AvaliableCoins].
    pub fn apply_to_sparsechain(self, sparsechain: &mut SparseChain) -> Result<(), CoreError> {
        let tx_heights = self
            .tx_keys
            .iter()
            .map(|(height, txid)| (*txid, *height))
            .collect::<Vec<_>>();
        let spends = self
            .tx_values
            .iter()
            .map(|(txid, tx)| {
                tx.input
                    .iter()
                    .enumerate()
                    .map(|(vin, txin)| (txin.previous_output, (tx.txid(), vin as u32)))
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();
        let persistence_state = self.tx_keys.iter().cloned().next();

        // mark txs as confirmed
        self.tx_keys.iter().for_each(|(_, txid)| {
            sparsechain.txs.remove(&(u32::MAX, *txid));
        });

        // update sparse chain
        sparsechain.blocks.extend(self.blocks);
        // sparsechain.txs.extend(todo!()); // TODO: Actually implement this!!!
        sparsechain.at_height.extend(tx_heights);
        sparsechain.spends.extend(spends);
        sparsechain.persist_from = match (sparsechain.persist_from, persistence_state) {
            (None, Some(s)) => Some(s),
            (Some(s), None) => Some(s),
            (None, None) => None,
            (a, b) => std::cmp::min(a, b),
        };

        Ok(())
    }
}
