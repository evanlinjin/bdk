//! Contains the [`IndexedTxGraph`] and associated types. Refer to the
//! [`IndexedTxGraph`] documentation for more.
use core::fmt::Debug;

use alloc::{sync::Arc, vec::Vec};
use bitcoin::{Block, OutPoint, Transaction, TxOut, Txid};

use crate::{
    tx_graph::{self, TxGraph},
    Anchor, BlockId, CanonicalParams, CanonicalTask, Indexer, Merge, TxPosInBlock,
};

/// A [`TxGraph<A>`] paired with an indexer `I`, enforcing that every insertion into the graph is
/// simultaneously fed through the indexer.
///
/// This guarantees that `tx_graph` and `index` remain in sync: any transaction or floating txout
/// you add to `tx_graph` has already been processed by `index`.
#[derive(Debug, Clone)]
pub struct IndexedTxGraph<A, I> {
    /// The indexer used for filtering transactions and floating txouts that we are interested in.
    pub index: I,
    graph: TxGraph<A>,
}

impl<A, I: Default> Default for IndexedTxGraph<A, I> {
    fn default() -> Self {
        Self {
            graph: Default::default(),
            index: Default::default(),
        }
    }
}

impl<A, I> IndexedTxGraph<A, I> {
    /// Get a reference of the internal transaction graph.
    pub fn graph(&self) -> &TxGraph<A> {
        &self.graph
    }
}

impl<A: Anchor, I: Indexer> IndexedTxGraph<A, I> {
    /// Applies the [`ChangeSet`] to the [`IndexedTxGraph`].
    pub fn apply_changeset(&mut self, changeset: ChangeSet<A, I::ChangeSet>) {
        self.index.apply_changeset(changeset.indexer);

        for tx in &changeset.tx_graph.txs {
            self.index.index_tx(tx);
        }
        for (&outpoint, txout) in &changeset.tx_graph.txouts {
            self.index.index_txout(outpoint, txout);
        }

        self.graph.apply_changeset(changeset.tx_graph);
    }

    /// Determines the [`ChangeSet`] between `self` and an empty [`IndexedTxGraph`].
    pub fn initial_changeset(&self) -> ChangeSet<A, I::ChangeSet> {
        let graph = self.graph.initial_changeset();
        let indexer = self.index.initial_changeset();
        ChangeSet {
            tx_graph: graph,
            indexer,
        }
    }

    // If `tx` replaces a relevant tx, it should also be considered relevant.
    fn is_tx_or_conflict_relevant(&self, tx: &Transaction) -> bool {
        self.index.is_tx_relevant(tx)
            || self
                .graph
                .direct_conflicts(tx)
                .filter_map(|(_, txid)| self.graph.get_tx(txid))
                .any(|tx| self.index.is_tx_relevant(&tx))
    }
}

impl<A: Anchor, I: Indexer> IndexedTxGraph<A, I>
where
    I::ChangeSet: Default + Merge,
{
    /// Create a new, empty [`IndexedTxGraph`].
    ///
    /// The underlying `TxGraph` is initialized with `TxGraph::default()`, and the provided
    /// `index`er is used as‐is (since there are no existing transactions to process).
    pub fn new(index: I) -> Self {
        Self {
            index,
            graph: TxGraph::default(),
        }
    }

    /// Reconstruct an [`IndexedTxGraph`] from persisted graph + indexer state.
    ///
    /// `cs` is read for the persisted state to load from and is then **rewritten** with the
    /// reconstructed state — which includes the original data plus any deltas produced by the
    /// internal `.reindex()` pass. Save `cs` after this call to persist those deltas.
    ///
    /// 1. Rebuilds the `TxGraph` from `cs.tx_graph`.
    /// 2. Calls your `indexer_from_changeset` closure on `cs.indexer` to restore any state your
    ///    indexer needs beyond its raw changeset.
    /// 3. Runs a full `.reindex()`.
    /// 4. Writes the full reconstructed state back into `cs`.
    ///
    /// # Errors
    ///
    /// Returns `Err(E)` if `indexer_from_changeset` fails.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use bdk_chain::IndexedTxGraph;
    /// # use bdk_chain::indexed_tx_graph::ChangeSet;
    /// # use bdk_chain::indexer::keychain_txout::{KeychainTxOutIndex, DEFAULT_LOOKAHEAD};
    /// # use bdk_core::BlockId;
    /// # use bdk_testenv::anyhow;
    /// # use miniscript::{Descriptor, DescriptorPublicKey};
    /// # use std::str::FromStr;
    /// # let mut persisted_changeset = ChangeSet::<BlockId, _>::default();
    /// # let persisted_desc = Some(Descriptor::<DescriptorPublicKey>::from_str("")?);
    /// # let persisted_change_desc = Some(Descriptor::<DescriptorPublicKey>::from_str("")?);
    ///
    /// let graph = IndexedTxGraph::from_changeset(
    ///     move |idx_cs| -> anyhow::Result<_> {
    ///         // e.g. KeychainTxOutIndex needs descriptors that weren’t in its change set.
    ///         let mut idx = KeychainTxOutIndex::from_changeset(DEFAULT_LOOKAHEAD, true, idx_cs);
    ///         if let Some(desc) = persisted_desc {
    ///             idx.insert_descriptor("external", desc)?;
    ///         }
    ///         if let Some(desc) = persisted_change_desc {
    ///             idx.insert_descriptor("internal", desc)?;
    ///         }
    ///         Ok(idx)
    ///     },
    ///     &mut persisted_changeset,
    /// )?;
    /// // `persisted_changeset` now contains the original data plus any reindex deltas.
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn from_changeset<F, E, C>(
        indexer_from_changeset: F,
        cs: &mut C,
    ) -> Result<Self, E>
    where
        F: FnOnce(I::ChangeSet) -> Result<I, E>,
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let inner = cs.as_mut();
        let graph = TxGraph::<A>::from_changeset(core::mem::take(&mut inner.tx_graph));
        let index = indexer_from_changeset(core::mem::take(&mut inner.indexer))?;
        let mut out = Self { graph, index };
        // `reindex` mutates the indexer state to reflect matches against the loaded tx graph.
        // Its delta is subsumed by `initial_changeset` below, so we discard the explicit delta
        // here.
        let mut reindex_delta = ChangeSet::<A, I::ChangeSet>::default();
        out.reindex(&mut reindex_delta);
        *inner = out.initial_changeset();
        Ok(out)
    }

    /// Synchronizes the indexer to reflect every entry in the transaction graph.
    ///
    /// Iterates over **all** full transactions and floating outputs in `self.graph`, passing each
    /// into `self.index`. Any indexer-side changes produced (via `index_tx` or `index_txout`) are
    /// written into `out`.
    pub fn reindex<C>(&mut self, out: &mut C)
    where
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let changeset = out.as_mut();
        for tx in self.graph.full_txs() {
            changeset.indexer.merge(self.index.index_tx(&tx));
        }
        for (op, txout) in self.graph.floating_txouts() {
            changeset.indexer.merge(self.index.index_txout(op, txout));
        }
    }

    fn index_tx_graph_changeset(
        &mut self,
        tx_graph_changeset: &tx_graph::ChangeSet<A>,
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
    /// `update` is a [`tx_graph::TxUpdate<A>`]. Any resultant changes are written into `out`.
    ///
    /// **Note**: Transactions in the `update` without temporal context (anchors or seen_ats)
    /// will be stored but will not be considered canonical. See [`tx_graph::TxUpdate`] for
    /// more details.
    pub fn apply_update<C>(&mut self, update: tx_graph::TxUpdate<A>, out: &mut C)
    where
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let cs = out.as_mut();
        let tx_graph = self.graph.apply_update(update);
        let indexer = self.index_tx_graph_changeset(&tx_graph);
        cs.tx_graph.merge(tx_graph);
        cs.indexer.merge(indexer);
    }

    /// Insert a floating `txout` of given `outpoint`.
    ///
    /// Any resultant changes are written into `out`.
    pub fn insert_txout<C>(&mut self, outpoint: OutPoint, txout: TxOut, out: &mut C)
    where
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let cs = out.as_mut();
        let graph = self.graph.insert_txout(outpoint, txout);
        let indexer = self.index_tx_graph_changeset(&graph);
        cs.tx_graph.merge(graph);
        cs.indexer.merge(indexer);
    }

    /// Insert and index a transaction into the graph.
    ///
    /// Any resultant changes are written into `out`.
    pub fn insert_tx<T, C>(&mut self, tx: T, out: &mut C)
    where
        T: Into<Arc<Transaction>>,
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let cs = out.as_mut();
        let tx_graph = self.graph.insert_tx(tx);
        let indexer = self.index_tx_graph_changeset(&tx_graph);
        cs.tx_graph.merge(tx_graph);
        cs.indexer.merge(indexer);
    }

    /// Insert an `anchor` for a given transaction.
    ///
    /// Any resultant changes are written into `out`.
    pub fn insert_anchor<C>(&mut self, txid: Txid, anchor: A, out: &mut C)
    where
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let tx_graph = self.graph.insert_anchor(txid, anchor);
        out.as_mut().tx_graph.merge(tx_graph);
    }

    /// Insert a unix timestamp of when a transaction is seen in the mempool.
    ///
    /// This is used for transaction conflict resolution in [`TxGraph`] where the transaction with
    /// the later last-seen is prioritized.
    ///
    /// Any resultant changes are written into `out`.
    pub fn insert_seen_at<C>(&mut self, txid: Txid, seen_at: u64, out: &mut C)
    where
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let tx_graph = self.graph.insert_seen_at(txid, seen_at);
        out.as_mut().tx_graph.merge(tx_graph);
    }

    /// Inserts the given `evicted_at` for `txid`.
    ///
    /// The `evicted_at` timestamp represents the last known time when the transaction was observed
    /// to be missing from the mempool. If `txid` was previously recorded with an earlier
    /// `evicted_at` value, it is updated only if the new value is greater.
    ///
    /// Any resultant changes are written into `out`.
    pub fn insert_evicted_at<C>(&mut self, txid: Txid, evicted_at: u64, out: &mut C)
    where
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let tx_graph = self.graph.insert_evicted_at(txid, evicted_at);
        out.as_mut().tx_graph.merge(tx_graph);
    }

    /// Batch inserts `(txid, evicted_at)` pairs for `txid`s that the graph is tracking.
    ///
    /// The `evicted_at` timestamp represents the last known time when the transaction was observed
    /// to be missing from the mempool. If `txid` was previously recorded with an earlier
    /// `evicted_at` value, it is updated only if the new value is greater.
    ///
    /// Any resultant changes are written into `out`.
    pub fn batch_insert_relevant_evicted_at<C>(
        &mut self,
        evicted_ats: impl IntoIterator<Item = (Txid, u64)>,
        out: &mut C,
    ) where
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let tx_graph = self.graph.batch_insert_relevant_evicted_at(evicted_ats);
        out.as_mut().tx_graph.merge(tx_graph);
    }

    /// Batch insert transactions, filtering out those that are irrelevant.
    ///
    /// `txs` do not need to be in topological order.
    ///
    /// Relevancy is determined by the internal [`Indexer::is_tx_relevant`] implementation of `I`.
    /// A transaction that conflicts with a relevant transaction is also considered relevant.
    /// Irrelevant transactions in `txs` will be ignored.
    ///
    /// Any resultant changes are written into `out`.
    pub fn batch_insert_relevant<T, C>(
        &mut self,
        txs: impl IntoIterator<Item = (T, impl IntoIterator<Item = A>)>,
        out: &mut C,
    ) where
        T: Into<Arc<Transaction>>,
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        // The algorithm below allows for non-topologically ordered transactions by using two loops.
        // This is achieved by:
        // 1. insert all txs into the index. If they are irrelevant then that's fine it will just
        //    not store anything about them.
        // 2. decide whether to insert them into the graph depending on whether `is_tx_relevant`
        //    returns true or not. (in a second loop).
        let txs = txs
            .into_iter()
            .map(|(tx, anchors)| (<T as Into<Arc<Transaction>>>::into(tx), anchors))
            .collect::<Vec<_>>();

        let cs = out.as_mut();
        for (tx, _) in &txs {
            cs.indexer.merge(self.index.index_tx(tx));
        }

        for (tx, anchors) in txs {
            if self.is_tx_or_conflict_relevant(&tx) {
                let txid = tx.compute_txid();
                cs.tx_graph.merge(self.graph.insert_tx(tx.clone()));
                for anchor in anchors {
                    cs.tx_graph.merge(self.graph.insert_anchor(txid, anchor));
                }
            }
        }
    }

    /// Batch insert unconfirmed transactions, filtering out those that are irrelevant.
    ///
    /// Relevancy is determined by the internal [`Indexer::is_tx_relevant`] implementation of `I`.
    /// A transaction that conflicts with a relevant transaction is also considered relevant.
    /// Irrelevant transactions in `unconfirmed_txs` will be ignored.
    ///
    /// Items of `txs` are tuples containing the transaction and a *last seen* timestamp. The
    /// *last seen* communicates when the transaction is last seen in the mempool which is used for
    /// conflict-resolution in [`TxGraph`] (refer to [`TxGraph::insert_seen_at`] for details).
    ///
    /// Any resultant changes are written into `out`.
    pub fn batch_insert_relevant_unconfirmed<T, C>(
        &mut self,
        unconfirmed_txs: impl IntoIterator<Item = (T, u64)>,
        out: &mut C,
    ) where
        T: Into<Arc<Transaction>>,
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        // The algorithm below allows for non-topologically ordered transactions by using two loops.
        // This is achieved by:
        // 1. insert all txs into the index. If they are irrelevant then that's fine it will just
        //    not store anything about them.
        // 2. decide whether to insert them into the graph depending on whether `is_tx_relevant`
        //    returns true or not. (in a second loop).
        let txs = unconfirmed_txs
            .into_iter()
            .map(|(tx, last_seen)| (<T as Into<Arc<Transaction>>>::into(tx), last_seen))
            .collect::<Vec<_>>();

        let cs = out.as_mut();
        for (tx, _) in &txs {
            cs.indexer.merge(self.index.index_tx(tx));
        }

        let graph = self.graph.batch_insert_unconfirmed(
            txs.into_iter()
                .filter(|(tx, _)| self.is_tx_or_conflict_relevant(tx))
                .map(|(tx, seen_at)| (tx.clone(), seen_at))
                .collect::<Vec<_>>(),
        );

        cs.tx_graph.merge(graph);
    }

    /// Batch insert unconfirmed transactions.
    ///
    /// Items of `txs` are tuples containing the transaction and a *last seen* timestamp. The
    /// *last seen* communicates when the transaction is last seen in the mempool which is used for
    /// conflict-resolution in [`TxGraph`] (refer to [`TxGraph::insert_seen_at`] for details).
    ///
    /// To filter out irrelevant transactions, use [`batch_insert_relevant_unconfirmed`] instead.
    ///
    /// Any resultant changes are written into `out`.
    ///
    /// [`batch_insert_relevant_unconfirmed`]: IndexedTxGraph::batch_insert_relevant_unconfirmed
    pub fn batch_insert_unconfirmed<T, C>(
        &mut self,
        txs: impl IntoIterator<Item = (T, u64)>,
        out: &mut C,
    ) where
        T: Into<Arc<Transaction>>,
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let cs = out.as_mut();
        let graph = self.graph.batch_insert_unconfirmed(txs);
        let indexer = self.index_tx_graph_changeset(&graph);
        cs.tx_graph.merge(graph);
        cs.indexer.merge(indexer);
    }
}

/// Methods are available if the anchor (`A`) can be created from [`TxPosInBlock`].
impl<A, I> IndexedTxGraph<A, I>
where
    I::ChangeSet: Default + Merge,
    for<'b> A: Anchor + From<TxPosInBlock<'b>>,
    I: Indexer,
{
    /// Batch insert all transactions of the given `block` of `height`, filtering out those that are
    /// irrelevant.
    ///
    /// Each inserted transaction's anchor will be constructed using [`TxPosInBlock`].
    ///
    /// Relevancy is determined by the internal [`Indexer::is_tx_relevant`] implementation of `I`.
    /// A transaction that conflicts with a relevant transaction is also considered relevant.
    /// Irrelevant transactions in `block` will be ignored.
    ///
    /// Any resultant changes are written into `out`.
    pub fn apply_block_relevant<C>(&mut self, block: &Block, height: u32, out: &mut C)
    where
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let block_id = BlockId {
            hash: block.block_hash(),
            height,
        };
        let cs = out.as_mut();
        for (tx_pos, tx) in block.txdata.iter().enumerate() {
            cs.indexer.merge(self.index.index_tx(tx));
            if self.is_tx_or_conflict_relevant(tx) {
                let txid = tx.compute_txid();
                let anchor = TxPosInBlock {
                    block,
                    block_id,
                    tx_pos,
                }
                .into();
                cs.tx_graph.merge(self.graph.insert_tx(tx.clone()));
                cs.tx_graph.merge(self.graph.insert_anchor(txid, anchor));
            }
        }
    }

    /// Batch insert all transactions of the given `block` of `height`.
    ///
    /// Each inserted transaction's anchor will be constructed using [`TxPosInBlock`].
    ///
    /// To only insert relevant transactions, use [`apply_block_relevant`] instead.
    ///
    /// Any resultant changes are written into `out`.
    ///
    /// [`apply_block_relevant`]: IndexedTxGraph::apply_block_relevant
    pub fn apply_block<C>(&mut self, block: Block, height: u32, out: &mut C)
    where
        C: AsMut<ChangeSet<A, I::ChangeSet>>,
    {
        let block_id = BlockId {
            hash: block.block_hash(),
            height,
        };
        let cs = out.as_mut();
        let mut graph = tx_graph::ChangeSet::default();
        for (tx_pos, tx) in block.txdata.iter().enumerate() {
            let anchor = TxPosInBlock {
                block: &block,
                block_id,
                tx_pos,
            }
            .into();
            graph.merge(self.graph.insert_anchor(tx.compute_txid(), anchor));
            graph.merge(self.graph.insert_tx(tx.clone()));
        }
        let indexer = self.index_tx_graph_changeset(&graph);
        cs.tx_graph.merge(graph);
        cs.indexer.merge(indexer);
    }
}

impl<A, I> AsRef<TxGraph<A>> for IndexedTxGraph<A, I> {
    fn as_ref(&self) -> &TxGraph<A> {
        &self.graph
    }
}

impl<A, X> IndexedTxGraph<A, X>
where
    A: Anchor,
{
    /// Creates a [`CanonicalTask`] to determine the [`CanonicalView`](crate::CanonicalView) of
    /// transactions.
    ///
    /// This method delegates to the underlying [`TxGraph`] to create a [`CanonicalTask`]
    /// that can be used to determine which transactions are canonical based on the provided
    /// parameters. The task handles the stateless canonicalization logic and can be polled
    /// for anchor verification requests.
    ///
    /// [`CanonicalView`]: crate::CanonicalView
    pub fn canonical_task(
        &'_ self,
        chain_tip: BlockId,
        params: CanonicalParams,
    ) -> CanonicalTask<'_, A> {
        self.graph.canonical_task(chain_tip, params)
    }
}

/// Represents changes to an [`IndexedTxGraph`].
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(bound(
        deserialize = "A: Ord + serde::Deserialize<'de>, IA: serde::Deserialize<'de>",
        serialize = "A: Ord + serde::Serialize, IA: serde::Serialize"
    ))
)]
#[must_use]
pub struct ChangeSet<A, IA> {
    /// [`TxGraph`] changeset.
    pub tx_graph: tx_graph::ChangeSet<A>,
    /// [`Indexer`] changeset.
    pub indexer: IA,
}

impl<A, IA: Default> Default for ChangeSet<A, IA> {
    fn default() -> Self {
        Self {
            tx_graph: Default::default(),
            indexer: Default::default(),
        }
    }
}

impl<A: Anchor, IA: Merge> Merge for ChangeSet<A, IA> {
    fn merge(&mut self, other: Self) {
        self.tx_graph.merge(other.tx_graph);
        self.indexer.merge(other.indexer);
    }

    fn is_empty(&self) -> bool {
        self.tx_graph.is_empty() && self.indexer.is_empty()
    }
}

impl<A, IA> AsMut<ChangeSet<A, IA>> for ChangeSet<A, IA> {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

impl<A, IA: Default> From<tx_graph::ChangeSet<A>> for ChangeSet<A, IA> {
    fn from(graph: tx_graph::ChangeSet<A>) -> Self {
        Self {
            tx_graph: graph,
            ..Default::default()
        }
    }
}

impl<A, IA> From<(tx_graph::ChangeSet<A>, IA)> for ChangeSet<A, IA> {
    fn from((tx_graph, indexer): (tx_graph::ChangeSet<A>, IA)) -> Self {
        Self { tx_graph, indexer }
    }
}

#[cfg(feature = "miniscript")]
impl<A> From<crate::keychain_txout::ChangeSet> for ChangeSet<A, crate::keychain_txout::ChangeSet> {
    fn from(indexer: crate::keychain_txout::ChangeSet) -> Self {
        Self {
            tx_graph: Default::default(),
            indexer,
        }
    }
}
