type IndexedTxGraphChangeSet = crate::indexed_tx_graph::ChangeSet<
    crate::ConfirmationTimeHeightAnchor,
    crate::keychain::ChangeSet,
>;

/// A changeset containing [`crate`] structures typically persisted together.
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
    pub indexed_tx_graph: IndexedTxGraphChangeSet,
}

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

/// Parameters for persisting a [`WalletChangeSet`] to [`sqlite::Store`](crate::sqlite::Store).
#[cfg(feature = "sqlite")]
#[derive(Clone, Copy, Debug)]
pub struct WalletSqlParams<'p> {
    /// Parameters for the schema table.
    pub wallet_schema: crate::sqlite::SchemaParams<'p>,
    /// Table name for the wallet.
    pub wallet_table_name: &'p str,
    /// Parameters for persisting the inner
    /// [`indexed_tx_graph::ChangeSet`](crate::indexed_tx_graph::ChangeSet).
    pub indexed_tx_graph: crate::indexed_tx_graph::SqlParams<
        'p,
        crate::ConfirmationTimeHeightAnchor,
        crate::keychain::SqlParams<'p>,
    >,
    /// Parameters for persisting the inner
    /// [`local_chain::ChangeSet`](crate::local_chain::ChangeSet).
    pub local_chain: crate::local_chain::SqlParams<'p>,
}

#[cfg(feature = "sqlite")]
impl<'p> Default for WalletSqlParams<'p> {
    fn default() -> Self {
        Self {
            wallet_schema: crate::sqlite::SchemaParams::new("bdk_wallet"),
            wallet_table_name: "bdk_wallet",
            local_chain: crate::local_chain::SqlParams::default(),
            indexed_tx_graph: crate::indexed_tx_graph::SqlParams::default(),
        }
    }
}

#[cfg(feature = "sqlite")]
impl<'p> crate::sqlite::StoreParams for WalletSqlParams<'p> {
    type ChangeSet = WalletChangeSet;

    fn initialize_tables(
        &self,
        db_tx: &rusqlite::Transaction,
    ) -> rusqlite::Result<()> {
        let schema_v0: &[&str] = &[&format!(
            "CREATE TABLE {} ( \
                id INTEGER PRIMARY KEY NOT NULL CHECK (id = 0), \
                descriptor TEXT, \
                change_descriptor TEXT, \
                network TEXT \
                ) STRICT;",
            self.wallet_table_name,
        )];
        let schema_by_version = &[schema_v0];

        let current_version = self.wallet_schema.version(db_tx)?;
        let schemas_to_init = {
            let exec_from = current_version.map_or(0_usize, |v| v as usize + 1);
            schema_by_version.iter().enumerate().skip(exec_from)
        };
        for (version, schema) in schemas_to_init {
            self.wallet_schema.set_version(db_tx, version as u32)?;
            db_tx.execute_batch(&schema.join("\n"))?;
        }

        self.indexed_tx_graph.initialize_tables(db_tx)?;
        self.local_chain.initialize_tables(db_tx)?;
        Ok(())
    }

    fn load_changeset(
        &self,
        db_tx: &rusqlite::Transaction,
    ) -> rusqlite::Result<Option<Self::ChangeSet>> {
        use crate::sqlite::Sql;
        use crate::Append;
        use rusqlite::OptionalExtension;
        use miniscript::{Descriptor, DescriptorPublicKey};

        let mut changeset = WalletChangeSet::default();

        let mut wallet_statement = db_tx.prepare(&format!(
            "SELECT descriptor, change_descriptor, network FROM {}",
            self.wallet_table_name,
        ))?;
        let row = wallet_statement
            .query_row([], |row| {
                Ok((
                    row.get::<_, Sql<Descriptor<DescriptorPublicKey>>>("descriptor")?,
                    row.get::<_, Sql<Descriptor<DescriptorPublicKey>>>("change_descriptor")?,
                    row.get::<_, Sql<bitcoin::Network>>("network")?,
                ))
            })
            .optional()?;
        match row {
            Some((Sql(desc), Sql(change_desc), Sql(network))) => {
                changeset.descriptor = Some(desc);
                changeset.change_descriptor = Some(change_desc);
                changeset.network = Some(network);
            }
            None => return Ok(None),
        };

        changeset.indexed_tx_graph = self
            .indexed_tx_graph
            .load_changeset(db_tx)?
            .unwrap_or_default();
        changeset.chain = self.local_chain.load_changeset(db_tx)?.unwrap_or_default();

        if changeset.is_empty() {
            Ok(None)
        } else {
            Ok(Some(changeset))
        }
    }

    fn write_changeset(
        &self,
        db_tx: &rusqlite::Transaction,
        changeset: &Self::ChangeSet,
    ) -> rusqlite::Result<()> {
        use crate::sqlite::Sql;
        use rusqlite::named_params;

        let mut descriptor_statement = db_tx.prepare_cached(&format!(
            "INSERT INTO {}(id, descriptor) VALUES(:id, :descriptor) ON DUPLICATE KEY UPDATE descriptor=:descriptor",
            self.wallet_table_name,
        ))?;
        if let Some(descriptor) = &changeset.descriptor {
            descriptor_statement.execute(named_params! {
                ":id": 0,
                ":descriptor": Sql(descriptor.clone()),
            })?;
        }

        let mut change_descriptor_statement = db_tx.prepare_cached(&format!(
            "INSERT INTO {}(id, change_descriptor) VALUES(:id, :change_descriptor) ON DUPLICATE KEY UPDATE change_descriptor=:change_descriptor",
            self.wallet_table_name,
        ))?;
        if let Some(change_descriptor) = &changeset.change_descriptor {
            change_descriptor_statement.execute(named_params! {
                ":id": 0,
                ":change_descriptor": Sql(change_descriptor.clone()),
            })?;
        }

        let mut network_statement = db_tx.prepare_cached(&format!(
            "INSERT INTO {}(id, network) VALUES(:id, :network) ON DUPLICATE KEY UPDATE network=:network",
            self.wallet_table_name,
        ))?;
        if let Some(network) = changeset.network {
            network_statement.execute(named_params! {
                ":id": 0,
                ":network": Sql(network),
            })?;
        }

        self.indexed_tx_graph
            .write_changeset(db_tx, &changeset.indexed_tx_graph)?;
        self.local_chain.write_changeset(db_tx, &changeset.chain)?;
        Ok(())
    }
}

impl From<crate::local_chain::ChangeSet> for WalletChangeSet {
    fn from(chain: crate::local_chain::ChangeSet) -> Self {
        Self {
            chain,
            ..Default::default()
        }
    }
}

impl From<IndexedTxGraphChangeSet> for WalletChangeSet {
    fn from(indexed_tx_graph: IndexedTxGraphChangeSet) -> Self {
        Self {
            indexed_tx_graph,
            ..Default::default()
        }
    }
}

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
