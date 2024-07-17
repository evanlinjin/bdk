//! Contains the [`IndexedTxGraph`] and associated types. Refer to the
//! [`IndexedTxGraph`] documentation for more.
use alloc::vec::Vec;
use bitcoin::{Block, OutPoint, Transaction, TxOut, Txid};

use crate::{
    tx_graph::{self, TxGraph},
    Anchor, AnchorMetaFromBlock, BlockId, Indexer, Merge,
};

/// The [`IndexedTxGraph`] combines a [`TxGraph`] and an [`Indexer`] implementation.
///
/// It ensures that [`TxGraph`] and [`Indexer`] are updated atomically.
#[derive(Debug)]
pub struct IndexedTxGraph<AM, I> {
    /// Transaction index.
    pub index: I,
    graph: TxGraph<AM>,
}

impl<AM, I: Default> Default for IndexedTxGraph<AM, I> {
    fn default() -> Self {
        Self {
            graph: Default::default(),
            index: Default::default(),
        }
    }
}

impl<AM, I> IndexedTxGraph<AM, I> {
    /// Construct a new [`IndexedTxGraph`] with a given `index`.
    pub fn new(index: I) -> Self {
        Self {
            index,
            graph: TxGraph::default(),
        }
    }

    /// Get a reference of the internal transaction graph.
    pub fn graph(&self) -> &TxGraph<AM> {
        &self.graph
    }
}

impl<AM, I: Indexer> IndexedTxGraph<AM, I>
where
    AM: Ord + Clone,
{
    /// Applies the [`ChangeSet`] to the [`IndexedTxGraph`].
    pub fn apply_changeset(&mut self, changeset: ChangeSet<AM, I::ChangeSet>) {
        self.index.apply_changeset(changeset.indexer);

        for tx in &changeset.graph.txs {
            self.index.index_tx(tx);
        }
        for (&outpoint, txout) in &changeset.graph.txouts {
            self.index.index_txout(outpoint, txout);
        }

        self.graph.apply_changeset(changeset.graph);
    }

    /// Determines the [`ChangeSet`] between `self` and an empty [`IndexedTxGraph`].
    pub fn initial_changeset(&self) -> ChangeSet<AM, I::ChangeSet> {
        let graph = self.graph.initial_changeset();
        let indexer = self.index.initial_changeset();
        ChangeSet { graph, indexer }
    }
}

impl<AM, I: Indexer> IndexedTxGraph<AM, I>
where
    I::ChangeSet: Default + Merge,
    AM: Ord + Clone,
{
    fn index_tx_graph_changeset(
        &mut self,
        tx_graph_changeset: &tx_graph::ChangeSet<AM>,
    ) -> I::ChangeSet {
        let mut changeset = I::ChangeSet::default();
        for added_tx in &tx_graph_changeset.txs {
            changeset.merge(self.index.index_tx(added_tx));
        }
        for (&added_outpoint, added_txout) in &tx_graph_changeset.txouts {
            changeset.merge(self.index.index_txout(added_outpoint, added_txout));
        }
        changeset
    }

    /// Apply an `update` directly.
    ///
    /// `update` is a [`TxGraph<A>`] and the resultant changes is returned as [`ChangeSet`].
    pub fn apply_update(&mut self, update: TxGraph<AM>) -> ChangeSet<AM, I::ChangeSet> {
        let graph = self.graph.apply_update(update);
        let indexer = self.index_tx_graph_changeset(&graph);
        ChangeSet { graph, indexer }
    }

    /// Insert a floating `txout` of given `outpoint`.
    pub fn insert_txout(
        &mut self,
        outpoint: OutPoint,
        txout: TxOut,
    ) -> ChangeSet<AM, I::ChangeSet> {
        let graph = self.graph.insert_txout(outpoint, txout);
        let indexer = self.index_tx_graph_changeset(&graph);
        ChangeSet { graph, indexer }
    }

    /// Insert and index a transaction into the graph.
    pub fn insert_tx(&mut self, tx: Transaction) -> ChangeSet<AM, I::ChangeSet> {
        let graph = self.graph.insert_tx(tx);
        let indexer = self.index_tx_graph_changeset(&graph);
        ChangeSet { graph, indexer }
    }

    /// Insert an `anchor` for a given transaction.
    pub fn insert_anchor(
        &mut self,
        anchor: Anchor,
        anchor_meta: AM,
    ) -> ChangeSet<AM, I::ChangeSet> {
        self.graph.insert_anchor(anchor, anchor_meta).into()
    }

    /// Insert a unix timestamp of when a transaction is seen in the mempool.
    ///
    /// This is used for transaction conflict resolution in [`TxGraph`] where the transaction with
    /// the later last-seen is prioritized.
    pub fn insert_seen_at(&mut self, txid: Txid, seen_at: u64) -> ChangeSet<AM, I::ChangeSet> {
        self.graph.insert_seen_at(txid, seen_at).into()
    }

    /// Batch insert transactions, filtering out those that are irrelevant.
    ///
    /// Relevancy is determined by the [`Indexer::is_tx_relevant`] implementation of `I`. Irrelevant
    /// transactions in `txs` will be ignored. `txs` do not need to be in topological order.
    pub fn batch_insert_relevant<'t>(
        &mut self,
        txs: impl IntoIterator<Item = (&'t Transaction, impl IntoIterator<Item = (Anchor, AM)>)>,
    ) -> ChangeSet<AM, I::ChangeSet> {
        // The algorithm below allows for non-topologically ordered transactions by using two loops.
        // This is achieved by:
        // 1. insert all txs into the index. If they are irrelevant then that's fine it will just
        //    not store anything about them.
        // 2. decide whether to insert them into the graph depending on whether `is_tx_relevant`
        //    returns true or not. (in a second loop).
        let txs = txs.into_iter().collect::<Vec<_>>();

        let mut indexer = I::ChangeSet::default();
        for (tx, _) in &txs {
            indexer.merge(self.index.index_tx(tx));
        }

        let mut graph = tx_graph::ChangeSet::default();
        for (tx, anchors) in txs {
            if self.index.is_tx_relevant(tx) {
                graph.merge(self.graph.insert_tx(tx.clone()));
                for (anchor, anchor_meta) in anchors {
                    graph.merge(self.graph.insert_anchor(anchor, anchor_meta));
                }
            }
        }

        ChangeSet { graph, indexer }
    }

    /// Batch insert unconfirmed transactions, filtering out those that are irrelevant.
    ///
    /// Relevancy is determined by the internal [`Indexer::is_tx_relevant`] implementation of `I`.
    /// Irrelevant transactions in `txs` will be ignored.
    ///
    /// Items of `txs` are tuples containing the transaction and a *last seen* timestamp. The
    /// *last seen* communicates when the transaction is last seen in the mempool which is used for
    /// conflict-resolution in [`TxGraph`] (refer to [`TxGraph::insert_seen_at`] for details).
    pub fn batch_insert_relevant_unconfirmed<'t>(
        &mut self,
        unconfirmed_txs: impl IntoIterator<Item = (&'t Transaction, u64)>,
    ) -> ChangeSet<AM, I::ChangeSet> {
        // The algorithm below allows for non-topologically ordered transactions by using two loops.
        // This is achieved by:
        // 1. insert all txs into the index. If they are irrelevant then that's fine it will just
        //    not store anything about them.
        // 2. decide whether to insert them into the graph depending on whether `is_tx_relevant`
        //    returns true or not. (in a second loop).
        let txs = unconfirmed_txs.into_iter().collect::<Vec<_>>();

        let mut indexer = I::ChangeSet::default();
        for (tx, _) in &txs {
            indexer.merge(self.index.index_tx(tx));
        }

        let graph = self.graph.batch_insert_unconfirmed(
            txs.into_iter()
                .filter(|(tx, _)| self.index.is_tx_relevant(tx))
                .map(|(tx, seen_at)| (tx.clone(), seen_at)),
        );

        ChangeSet { graph, indexer }
    }

    /// Batch insert unconfirmed transactions.
    ///
    /// Items of `txs` are tuples containing the transaction and a *last seen* timestamp. The
    /// *last seen* communicates when the transaction is last seen in the mempool which is used for
    /// conflict-resolution in [`TxGraph`] (refer to [`TxGraph::insert_seen_at`] for details).
    ///
    /// To filter out irrelevant transactions, use [`batch_insert_relevant_unconfirmed`] instead.
    ///
    /// [`batch_insert_relevant_unconfirmed`]: IndexedTxGraph::batch_insert_relevant_unconfirmed
    pub fn batch_insert_unconfirmed(
        &mut self,
        txs: impl IntoIterator<Item = (Transaction, u64)>,
    ) -> ChangeSet<AM, I::ChangeSet> {
        let graph = self.graph.batch_insert_unconfirmed(txs);
        let indexer = self.index_tx_graph_changeset(&graph);
        ChangeSet { graph, indexer }
    }
}

/// Methods are available if the anchor (`A`) implements [`AnchorFromBlockPosition`].
impl<AM: AnchorMetaFromBlock, I: Indexer> IndexedTxGraph<AM, I>
where
    I::ChangeSet: Default + Merge,
    AM: AnchorMetaFromBlock + core::fmt::Debug + Clone + Eq + PartialOrd + Ord + core::hash::Hash,
{
    /// Batch insert all transactions of the given `block` of `height`, filtering out those that are
    /// irrelevant.
    ///
    /// Each inserted transaction's anchor will be constructed from
    /// [`AnchorFromBlockPosition::from_block_position`].
    ///
    /// Relevancy is determined by the internal [`Indexer::is_tx_relevant`] implementation of `I`.
    /// Irrelevant transactions in `txs` will be ignored.
    pub fn apply_block_relevant(
        &mut self,
        block: &Block,
        height: u32,
    ) -> ChangeSet<AM, I::ChangeSet> {
        let block_id = BlockId {
            hash: block.block_hash(),
            height,
        };
        let mut changeset = ChangeSet::<AM, I::ChangeSet>::default();
        for (tx_pos, tx) in block.txdata.iter().enumerate() {
            changeset.indexer.merge(self.index.index_tx(tx));
            if self.index.is_tx_relevant(tx) {
                let anchor = (tx.compute_txid(), block_id);
                let anchor_meta = AM::from_block(block, block_id, tx_pos);
                changeset.graph.merge(self.graph.insert_tx(tx.clone()));
                changeset
                    .graph
                    .merge(self.graph.insert_anchor(anchor, anchor_meta));
            }
        }
        changeset
    }

    /// Batch insert all transactions of the given `block` of `height`.
    ///
    /// Each inserted transaction's anchor will be constructed from
    /// [`AnchorFromBlockPosition::from_block_position`].
    ///
    /// To only insert relevant transactions, use [`apply_block_relevant`] instead.
    ///
    /// [`apply_block_relevant`]: IndexedTxGraph::apply_block_relevant
    pub fn apply_block(&mut self, block: Block, height: u32) -> ChangeSet<AM, I::ChangeSet> {
        let block_id = BlockId {
            hash: block.block_hash(),
            height,
        };
        let mut graph = tx_graph::ChangeSet::default();
        for (tx_pos, tx) in block.txdata.iter().enumerate() {
            let anchor = (tx.compute_txid(), block_id);
            let anchor_meta = AM::from_block(&block, block_id, tx_pos);
            graph.merge(self.graph.insert_anchor(anchor, anchor_meta));
            graph.merge(self.graph.insert_tx(tx.clone()));
        }
        let indexer = self.index_tx_graph_changeset(&graph);
        ChangeSet { graph, indexer }
    }
}

/// Represents changes to an [`IndexedTxGraph`].
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(
        crate = "serde_crate",
        bound(
            deserialize = "AM: Ord + serde::Deserialize<'de>, IA: serde::Deserialize<'de>",
            serialize = "AM: Ord + serde::Serialize, IA: serde::Serialize"
        )
    )
)]
#[must_use]
pub struct ChangeSet<AM, IA> {
    /// [`TxGraph`] changeset.
    pub graph: tx_graph::ChangeSet<AM>,
    /// [`Indexer`] changeset.
    pub indexer: IA,
}

impl<AM, IA: Default> Default for ChangeSet<AM, IA> {
    fn default() -> Self {
        Self {
            graph: Default::default(),
            indexer: Default::default(),
        }
    }
}

impl<AM, IA: Merge> Merge for ChangeSet<AM, IA>
where
    AM: Ord + Clone,
{
    fn merge(&mut self, other: Self) {
        self.graph.merge(other.graph);
        self.indexer.merge(other.indexer);
    }

    fn is_empty(&self) -> bool {
        self.graph.is_empty() && self.indexer.is_empty()
    }
}

impl<AM, IA: Default> From<tx_graph::ChangeSet<AM>> for ChangeSet<AM, IA> {
    fn from(graph: tx_graph::ChangeSet<AM>) -> Self {
        Self {
            graph,
            ..Default::default()
        }
    }
}

#[cfg(feature = "miniscript")]
impl<AM, K> From<crate::indexer::keychain_txout::ChangeSet<K>>
    for ChangeSet<AM, crate::indexer::keychain_txout::ChangeSet<K>>
{
    fn from(indexer: crate::indexer::keychain_txout::ChangeSet<K>) -> Self {
        Self {
            graph: Default::default(),
            indexer,
        }
    }
}

impl<AM, I> AsRef<TxGraph<AM>> for IndexedTxGraph<AM, I> {
    fn as_ref(&self) -> &TxGraph<AM> {
        &self.graph
    }
}
