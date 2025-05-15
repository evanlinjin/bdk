//! [`Indexer`] provides utilities for indexing transaction data.

use std::sync::Arc;

use bdk_core::{BlockId, Merge, TxPosInBlock, TxUpdate};
use bitcoin::{Block, OutPoint, Transaction, TxOut};

use crate::{indexed_tx_graph, Anchor, TxGraph};

#[cfg(feature = "miniscript")]
pub mod keychain_txout;
pub mod spk_txout;

/// Filters and indexes.
pub trait FilteringIndexer<A, U>: Index<A> {
    /// Index and filters.
    fn index_and_filter(&mut self, changeset: &mut Self::ChangeSet, update: &U) -> TxUpdate<A>;
}

/// Indexes
pub trait Indexer<A, U = TxUpdate<A>>: Index<A>
where
    U: Into<TxUpdate<A>>,
{
    /// Index an update.
    fn index(&mut self, changeset: &mut Self::ChangeSet, update: &U);
}

/// A trait representing a persistable index of transaction data.
///
/// This index maintains internal state that can be updated incrementally and persisted.
pub trait Index<A> {
    /// A set of changes that can be applied to or produced by this index.
    type ChangeSet: Merge;

    /// Reindex the internal state using the provided [`TxUpdate`].
    ///
    /// This assumes that all relevant data have already been passed through
    /// [`Indexer::index`] or [`FilteringIndexer::index_and_filter`].
    fn reindex(&mut self, tx_update: &TxUpdate<A>);

    /// Applies the given changeset to the index.
    fn apply_changeset(&mut self, changeset: Self::ChangeSet);

    /// Computes the [`ChangeSet`](Index::ChangeSet) required to represent the entire current
    /// state.
    fn initial_changeset(&self) -> Self::ChangeSet;
}

/// Anchored tx.
pub type AnchoredTx<A> = (Arc<Transaction>, A);

/// Mempool tx
pub type MempoolTx = (Arc<Transaction>, u64);

/// Indexes and item.
pub trait ItemIndexer<A, Item>: Index<A> {
    /// Index item.
    fn index_item(&mut self, changeset: &mut Self::ChangeSet, item: &Item);

    /// Whether item is relevant.
    fn is_item_relevant(&self, item: &Item) -> bool;
}

/// Test api
pub struct ITxG<A, X> {
    graph: TxGraph<A>,
    index: X,
}

impl<A: Anchor, X: Index<A>> ITxG<A, X> {
    /// Apply update.
    pub fn apply_update(
        &mut self,
        update: TxUpdate<A>,
    ) -> indexed_tx_graph::ChangeSet<A, X::ChangeSet>
    where
        X: ItemIndexer<A, Transaction>,
        X: ItemIndexer<A, (OutPoint, TxOut)>,
    {
        let mut changeset = indexed_tx_graph::ChangeSet::default();
        for tx in &update.txs {
            self.index.index_item(&mut changeset.indexer, tx.as_ref());
        }
        for (outpoint, txout) in &update.txouts {
            let item = (*outpoint, txout.clone());
            self.index.index_item(&mut changeset.indexer, &item);
        }
        changeset.tx_graph.merge(self.graph.apply_update(update));
        changeset
    }

    /// Filter and apply from block.
    pub fn filter_and_apply_block(
        &mut self,
        block: &Block,
        height: u32,
    ) -> indexed_tx_graph::ChangeSet<A, X::ChangeSet>
    where
        X: ItemIndexer<A, Transaction>,
        for<'b> A: From<TxPosInBlock<'b>>,
    {
        let hash = block.block_hash();
        let block_id = BlockId { height, hash };

        let mut changeset = indexed_tx_graph::ChangeSet::default();
        for (tx_pos, tx) in block.txdata.iter().enumerate() {
            self.index.index_item(&mut changeset.indexer, tx);
            if !self.index.is_item_relevant(tx) {
                continue;
            }
            let pos = TxPosInBlock {
                block,
                block_id,
                tx_pos,
            };
            changeset.tx_graph.merge(self.graph.insert_tx(tx.clone()));
            changeset
                .tx_graph
                .merge(self.graph.insert_anchor(tx.compute_txid(), pos.into()));
        }
        changeset
    }

    pub fn filter_and_apply_anchored_transactions() {}
}
