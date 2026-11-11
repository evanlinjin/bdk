//! [`Indexer`] provides utilities for indexing transaction data.

use bitcoin::{OutPoint, Transaction, TxOut};

use crate::{tx_graph::TxGraph, Merge};

#[cfg(feature = "miniscript")]
pub mod keychain_txout;
pub mod spk_txout;

/// Utilities for indexing transaction data.
///
/// Types which implement this trait can be used to construct an [`IndexedTxGraph`].
/// This trait's methods should rarely be called directly.
///
/// [`IndexedTxGraph`]: crate::IndexedTxGraph
pub trait Indexer {
    /// The resultant "changeset" when new transaction data is indexed.
    type ChangeSet;

    /// Scan and index the given `outpoint` and `txout`.
    fn index_txout(&mut self, outpoint: OutPoint, txout: &TxOut) -> Self::ChangeSet;

    /// Scans a transaction for relevant outpoints, which are stored and indexed internally.
    fn index_tx(&mut self, tx: &Transaction) -> Self::ChangeSet;

    /// Apply changeset to itself.
    fn apply_changeset(&mut self, changeset: Self::ChangeSet);

    /// Determines the [`ChangeSet`](Indexer::ChangeSet) between `self` and an empty [`Indexer`].
    fn initial_changeset(&self) -> Self::ChangeSet;

    /// Determines whether the transaction should be included in the index.
    fn is_tx_relevant(&self, tx: &Transaction) -> bool;

    /// Index everything in `graph` that this indexer has not already accounted for.
    ///
    /// The default implementation offers every full transaction and floating output to
    /// [`index_tx`](Self::index_tx) and [`index_txout`](Self::index_txout) exactly once, which is
    /// all an indexer needs when what it recognizes is fixed up front.
    ///
    /// Override this when a match can *widen* what the indexer recognizes, so that an output
    /// offered earlier in the walk could match on a later look. Only the indexer knows when its
    /// recognition set has stopped growing, so only the indexer can decide when to stop looking.
    fn rescan<A>(&mut self, graph: &TxGraph<A>) -> Self::ChangeSet
    where
        Self::ChangeSet: Merge,
    {
        let mut changeset = Self::ChangeSet::default();
        for tx in graph.full_txs() {
            changeset.merge(self.index_tx(&tx));
        }
        for (op, txout) in graph.floating_txouts() {
            changeset.merge(self.index_txout(op, txout));
        }
        changeset
    }
}
