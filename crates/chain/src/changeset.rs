/// A changeset containing [`crate`] structures typically persisted together.
#[cfg(feature = "miniscript")]
#[derive(Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(crate::serde::Deserialize, crate::serde::Serialize),
    serde(crate = "crate::serde")
)]
pub struct WalletChangeSet {
    /// Descriptor for recipient addresses.
    pub descriptor: Option<miniscript::Descriptor<miniscript::DescriptorPublicKey>>,
    /// Descriptor for change addresses.
    pub change_descriptor: Option<miniscript::Descriptor<miniscript::DescriptorPublicKey>>,
    /// Stores the network type of the transaction data.
    pub network: Option<bitcoin::Network>,
    /// Changes to the [`LocalChain`](crate::local_chain::LocalChain).
    pub chain: crate::local_chain::ChangeSet,
    /// Changes to [`IndexedTxGraph`](crate::indexed_tx_graph::IndexedTxGraph).
    pub indexed_tx_graph: crate::indexed_tx_graph::ChangeSet<
        crate::ConfirmationTimeHeightAnchor,
        crate::keychain::ChangeSet,
    >,
}

#[cfg(feature = "miniscript")]
impl crate::Append for WalletChangeSet {
    /// Merge another [`CombinedChangeSet`] into itself.
    ///
    /// The `keychains_added` field respects the invariants of... TODO: FINISH THIS!
    fn append(&mut self, other: Self) {
        if other.descriptor.is_some() {
            debug_assert!(
                self.descriptor.is_none() || self.descriptor == other.descriptor,
                "descriptor must never change"
            );
            self.descriptor = other.descriptor;
        }
        if other.change_descriptor.is_some() {
            debug_assert!(
                self.change_descriptor.is_none()
                    || self.change_descriptor == other.change_descriptor,
                "change descriptor must never change"
            );
        }
        if other.network.is_some() {
            debug_assert!(
                self.network.is_none() || self.network == other.network,
                "network must never change"
            );
            self.network = other.network;
        }

        crate::Append::append(&mut self.chain, other.chain);
        crate::Append::append(&mut self.indexed_tx_graph, other.indexed_tx_graph);
    }

    fn is_empty(&self) -> bool {
        self.descriptor.is_none()
            && self.change_descriptor.is_none()
            && self.chain.is_empty()
            && self.indexed_tx_graph.is_empty()
            && self.network.is_none()
    }
}

#[cfg(all(feature = "sqlite", feature = "miniscript"))]
#[derive(Default)]
pub struct SqlParams {}

#[cfg(all(feature = "sqlite", feature = "miniscript"))]
impl bdk_sqlite::Storable for WalletChangeSet {
    type Params = SqlParams;

    fn init(
        db_tx: &bdk_sqlite::rusqlite::Transaction,
        params: &Self::Params,
    ) -> bdk_sqlite::rusqlite::Result<()> {
        todo!()
    }

    fn read(
        db_tx: &bdk_sqlite::rusqlite::Transaction,
        params: &Self::Params,
    ) -> bdk_sqlite::rusqlite::Result<Option<Self>> {
        todo!()
    }

    fn write(
        &self,
        db_tx: &bdk_sqlite::rusqlite::Transaction,
        params: &Self::Params,
    ) -> bdk_sqlite::rusqlite::Result<()> {
        todo!()
    }
}

#[cfg(feature = "miniscript")]
impl From<crate::local_chain::ChangeSet> for WalletChangeSet {
    fn from(chain: crate::local_chain::ChangeSet) -> Self {
        Self {
            chain,
            ..Default::default()
        }
    }
}

#[cfg(feature = "miniscript")]
impl
    From<
        crate::indexed_tx_graph::ChangeSet<
            crate::ConfirmationTimeHeightAnchor,
            crate::keychain::ChangeSet,
        >,
    > for WalletChangeSet
{
    fn from(
        indexed_tx_graph: crate::indexed_tx_graph::ChangeSet<
            crate::ConfirmationTimeHeightAnchor,
            crate::keychain::ChangeSet,
        >,
    ) -> Self {
        Self {
            indexed_tx_graph,
            ..Default::default()
        }
    }
}

#[cfg(feature = "miniscript")]
impl From<crate::keychain::ChangeSet> for WalletChangeSet {
    fn from(indexer: crate::keychain::ChangeSet) -> Self {
        Self {
            indexed_tx_graph: crate::indexed_tx_graph::ChangeSet {
                indexer,
                ..Default::default()
            },
            ..Default::default()
        }
    }
}
