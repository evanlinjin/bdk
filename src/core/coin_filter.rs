use std::{collections::HashMap, hash::Hash};

use bitcoin::{OutPoint, TxOut};

use super::ChainTx;

/// Filters coins and adds metadata. This is the step before coin selection.
pub trait CoinFilter {
    /// Group in which a coin belongs.
    ///
    /// For example, we can group coins by script type and attempt to create transactions of a
    /// single script type (for privacy) initially.
    type CoinGroup: Eq + Hash + Copy;

    /// Custum coin data obtained from output that is kept. Useful for coin selection.
    type CoinData;

    /// Transaction-level filter.
    ///
    /// For example, we can use this to filter out unconfirmed transactions that are not sent by us.
    fn filter_tx(&self, _tx: &ChainTx) -> bool {
        true
    }

    /// Output-level filter.
    fn filter_coin(
        &self,
        outpoint: &OutPoint,
        tx: &ChainTx,
        txout: &TxOut,
    ) -> Option<AvaliableCoin<Self::CoinGroup, Self::CoinData>>;
}

/// Coin that can be considered for coin selection.
pub struct AvaliableCoin<G, D> {
    /// Position of output
    pub outpoint: OutPoint,
    /// The actual output
    pub txout: TxOut,
    /// Coin group
    pub group: G,
    /// Additional data that can be used for coin selection
    pub data: D,
}

/// Coins avaliable for coin selection, grouped by `G`.
pub type AvaliableCoins<G, D> = HashMap<G, Vec<AvaliableCoin<G, D>>>;
