use crate::{
    collections::{BTreeMap, BTreeSet, HashSet},
    BlockId,
};
use alloc::{sync::Arc, vec::Vec};
use bitcoin::{OutPoint, Transaction, TxOut, Txid};

/// Set of parameters sufficient to construct an [`Anchor`].
///
/// Typically used as an additional constraint on anchor:
/// `for<'b> A: Anchor + From<TxPosInBlock<'b>>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxPosInBlock<'b> {
    /// Block in which the transaction appeared.
    pub block: &'b bitcoin::Block,
    /// Block's [`BlockId`].
    pub block_id: BlockId,
    /// Position in the block on which the transaction appeared.
    pub tx_pos: usize,
}

/// Block update.
#[derive(Debug, Clone, Copy)]
pub struct BlockUpdate<'b> {
    block: &'b bitcoin::Block,
    height: u32,
}

impl<'b> BlockUpdate<'b> {
    /// Construct.
    pub fn new(block: &'b bitcoin::Block, height: u32) -> Self {
        Self { block, height }
    }
    /// Consume update and iterate transactions.
    pub fn txs<A>(self) -> impl Iterator<Item = (&'b Transaction, A)> + 'b
    where
        A: From<TxPosInBlock<'b>>,
    {
        let block = self.block;
        let block_id = BlockId {
            height: self.height,
            hash: self.block.block_hash(),
        };
        block.txdata.iter().enumerate().map(move |(tx_pos, tx)| {
            let anchor = A::from(TxPosInBlock {
                block,
                block_id,
                tx_pos,
            });
            (tx, anchor)
        })
    }
}

/// Mempool update.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct MempoolUpdate {
    /// Whether the transactions are ordered chronologically.
    pub chronological: bool,
    /// Transactions of the mempool.
    pub txs: Vec<(Arc<Transaction>, u64)>,
}

/// Data object used to communicate updates about relevant transactions from some chain data source
/// to the core model (usually a `bdk_chain::TxGraph`).
///
/// ```rust
/// use bdk_core::TxUpdate;
/// # use std::sync::Arc;
/// # use bitcoin::{Transaction, transaction::Version, absolute::LockTime};
/// # let version = Version::ONE;
/// # let lock_time = LockTime::ZERO;
/// # let tx = Arc::new(Transaction { input: vec![], output: vec![], version, lock_time });
/// # let txid = tx.compute_txid();
/// # let anchor = ();
/// let mut tx_update = TxUpdate::default();
/// tx_update.txs.push(tx);
/// tx_update.anchors.insert((anchor, txid));
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TxUpdate<A = ()> {
    /// Full transactions. These are transactions that were determined to be relevant to the wallet
    /// given the request.
    pub txs: Vec<Arc<Transaction>>,

    /// Floating txouts. These are `TxOut`s that exist but the whole transaction wasn't included in
    /// `txs` since only knowing about the output is important. These are often used to help determine
    /// the fee of a wallet transaction.
    pub txouts: BTreeMap<OutPoint, TxOut>,

    /// Transaction anchors. Anchors tells us a position in the chain where a transaction was
    /// confirmed.
    pub anchors: BTreeSet<(A, Txid)>,

    /// When transactions were seen in the mempool.
    ///
    /// An unconfirmed transaction can only be canonical with a `seen_at` value. It is the
    /// responsibility of the chain-source to include the `seen_at` values for unconfirmed
    /// (unanchored) transactions.
    ///
    /// [`FullScanRequest::start_time`](crate::spk_client::FullScanRequest::start_time) or
    /// [`SyncRequest::start_time`](crate::spk_client::SyncRequest::start_time) can be used to
    /// provide the `seen_at` value.
    pub seen_ats: HashSet<(Txid, u64)>,

    /// When transactions were discovered to be missing (evicted) from the mempool.
    ///
    /// [`SyncRequest::start_time`](crate::spk_client::SyncRequest::start_time) can be used to
    /// provide the `evicted_at` value.
    pub evicted_ats: HashSet<(Txid, u64)>,
}

impl<A> Default for TxUpdate<A> {
    fn default() -> Self {
        Self {
            txs: Default::default(),
            txouts: Default::default(),
            anchors: Default::default(),
            seen_ats: Default::default(),
            evicted_ats: Default::default(),
        }
    }
}

impl<A> From<Transaction> for TxUpdate<A> {
    fn from(tx: Transaction) -> Self {
        let mut update = Self::default();
        update.txs.push(Arc::new(tx));
        update
    }
}

impl<A: Ord> TxUpdate<A> {
    /// Transforms the [`TxUpdate`] to have `anchors` (`A`) of another type (`A2`).
    ///
    /// This takes in a closure with signature `FnMut(A) -> A2` which is called for each anchor to
    /// transform it.
    pub fn map_anchors<A2: Ord, F: FnMut(A) -> A2>(self, mut map: F) -> TxUpdate<A2> {
        TxUpdate {
            txs: self.txs,
            txouts: self.txouts,
            anchors: self
                .anchors
                .into_iter()
                .map(|(a, txid)| (map(a), txid))
                .collect(),
            seen_ats: self.seen_ats,
            evicted_ats: self.evicted_ats,
        }
    }

    /// Extend this update with `other`.
    pub fn extend(&mut self, other: TxUpdate<A>) {
        self.txs.extend(other.txs);
        self.txouts.extend(other.txouts);
        self.anchors.extend(other.anchors);
        self.seen_ats.extend(other.seen_ats);
        self.evicted_ats.extend(other.evicted_ats);
    }
}
