#![doc = include_str!("../README.md")]
// only enables the `doc_cfg` feature when the `docsrs` configuration attribute is defined
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod persist;
mod schema;
mod store;
#[cfg(feature = "wallet")]
#[cfg_attr(docsrs, doc(cfg(feature = "wallet")))]
pub mod wallet;

use bdk_chain::bitcoin::Network;
use bdk_chain::{indexed_tx_graph, keychain, local_chain, Anchor, Append};
pub use rusqlite;
use serde::{Deserialize, Serialize};
pub use store::Store;

/// Structure representing changes to be committed to the SQLite database.
#[derive(Clone, Debug, PartialEq)]
pub struct DbCommitment<K, A> {
    /// Used to save [`Network`] type of the wallet.
    pub network: Option<Network>,
    /// Changes to [`local_chain::LocalChain`].
    pub chain: local_chain::ChangeSet,
    /// Changes to [`indexed_tx_graph::IndexedTxGraph`].
    pub tx_graph: indexed_tx_graph::ChangeSet<A, keychain::ChangeSet<K>>,
}

impl<K: Ord + for<'de> Deserialize<'de> + Serialize, A: Anchor> Default for DbCommitment<K, A> {
    fn default() -> Self {
        DbCommitment {
            network: None,
            chain: Default::default(),
            tx_graph: indexed_tx_graph::ChangeSet::default(),
        }
    }
}

impl<K, A> Append for DbCommitment<K, A>
where
    K: Ord + for<'de> Deserialize<'de> + Serialize,
    A: Anchor,
{
    fn append(&mut self, mut other: Self) {
        match (self.network, other.network) {
            // if current network is Some it can never be changed
            (Some(net), Some(other_net)) => assert_eq!(net, other_net),
            // if current network is None it can be changed to other
            (None, Some(other_net)) => self.network = Some(other_net),
            // if other is None then no change
            (_, None) => (),
        };
        self.chain.append(&mut other.chain);
        self.tx_graph.append(other.tx_graph);
    }

    fn is_empty(&self) -> bool {
        self.chain.is_empty() && self.tx_graph.is_empty()
    }
}

/// Error that occurs while reading or writing change sets with the SQLite database.
#[derive(Debug)]
pub enum Error {
    /// Invalid network, cannot change the one already stored in the database.
    Network { expected: Network, given: Network },
    /// SQLite error.
    Sqlite(rusqlite::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Network { expected, given } => write!(
                f,
                "network error trying to read or write change set, expected {}, given {}",
                expected, given
            ),
            Self::Sqlite(e) => write!(f, "sqlite error reading or writing changeset: {}", e),
        }
    }
}

impl std::error::Error for Error {}
