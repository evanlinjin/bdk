//! Module for stuff

use core::{fmt::Debug, ops::Deref, str::FromStr};

use alloc::{borrow::ToOwned, boxed::Box, string::ToString, vec::Vec};
use rusqlite::{
    self, named_params,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
    ToSql,
};
use bitcoin::{
    consensus::{Decodable, Encodable},
    Amount, BlockHash, Script, ScriptBuf, Transaction, Txid,
};

use crate::{Anchor, Append};

/// Parameters for [`Store`].
pub trait StoreParams {
    /// Data type that is loaded and written to the database.
    type ChangeSet: Default + Append;

    /// Initialize SQL tables.
    fn initialize_tables(&self, db_tx: &rusqlite::Transaction) -> rusqlite::Result<()>;

    /// Load all data from tables.
    fn load_changeset(
        &self,
        db_tx: &rusqlite::Transaction,
    ) -> rusqlite::Result<Option<Self::ChangeSet>>;

    /// Write data into table(s).
    fn write_changeset(
        &self,
        db_tx: &rusqlite::Transaction,
        changeset: &Self::ChangeSet,
    ) -> rusqlite::Result<()>;
}

// TODO: Use macros
impl<A: StoreParams, B: StoreParams> StoreParams for (A, B) {
    type ChangeSet = (A::ChangeSet, B::ChangeSet);

    fn initialize_tables(&self, db_tx: &rusqlite::Transaction) -> rusqlite::Result<()> {
        self.0.initialize_tables(db_tx)?;
        self.1.initialize_tables(db_tx)?;
        Ok(())
    }

    fn load_changeset(
        &self,
        db_tx: &rusqlite::Transaction,
    ) -> rusqlite::Result<Option<Self::ChangeSet>> {
        let changeset = (
            self.0.load_changeset(db_tx)?.unwrap_or_default(),
            self.1.load_changeset(db_tx)?.unwrap_or_default(),
        );
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
        self.0.write_changeset(db_tx, &changeset.0)?;
        self.1.write_changeset(db_tx, &changeset.1)?;
        Ok(())
    }
}

/// Persists data in to a relational schema based [SQLite] database file.
///
/// The changesets loaded or stored represent changes to keychain and blockchain data.
///
/// [SQLite]: https://www.sqlite.org/index.html
#[derive(Debug)]
pub struct Store<P> {
    conn: rusqlite::Connection,
    params: P,
}

impl<P: StoreParams> Store<P>
where
    P::ChangeSet: Append,
{
    /// Create a new store and load the initial changeset from it.
    pub fn load(
        mut conn: rusqlite::Connection,
        params: P,
    ) -> rusqlite::Result<(Self, Option<P::ChangeSet>)> {
        let db_tx = conn.transaction()?;
        params.initialize_tables(&db_tx)?;
        let changeset = params.load_changeset(&db_tx)?;
        db_tx.commit()?;

        Ok((Self { conn, params }, changeset))
    }

    /// Write to the store.
    pub fn write(&mut self, changeset: &P::ChangeSet) -> rusqlite::Result<()> {
        if !changeset.is_empty() {
            let db_tx = self.conn.transaction()?;
            self.params.write_changeset(&db_tx, changeset)?;
            db_tx.commit()?;
        }
        Ok(())
    }
}

/// Parameters for schema tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaParams<'p> {
    /// The schema table name.
    pub table_name: &'p str,
    /// The schema name.
    pub schema_name: &'p str,
}

impl<'p> SchemaParams<'p> {
    /// Contruct a new [`SchemaParams`] with the given `schema_name`.
    pub fn new(schema_name: &'p str) -> Self {
        let table_name = "bdk_schemas";
        Self {
            table_name,
            schema_name,
        }
    }

    fn init(&self, db_tx: &rusqlite::Transaction) -> rusqlite::Result<()> {
        let sql = format!("CREATE TABLE IF NOT EXISTS {}( name TEXT PRIMARY KEY NOT NULL, version INTEGER NOT NULL ) STRICT;", self.table_name);
        db_tx.execute(&sql, ())?;
        Ok(())
    }

    /// Get the schema version of `schema_name`.
    pub fn version(&self, db_tx: &rusqlite::Transaction) -> rusqlite::Result<Option<u32>> {
        self.init(db_tx)?;

        let sql = format!("SELECT version FROM {} WHERE name=:name", self.table_name);
        let res = db_tx.query_row(&sql, named_params! { ":name": self.schema_name }, |row| {
            row.get::<_, u32>("version")
        });
        match res {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Set the schema version for `schema_name`.
    pub fn set_version(&self, db_tx: &rusqlite::Transaction, version: u32) -> rusqlite::Result<()> {
        self.init(db_tx)?;

        let sql = format!(
            "REPLACE INTO {}(name, version) VALUES(:name, :version)",
            self.table_name,
        );
        db_tx.execute(
            &sql,
            named_params! { ":name": self.schema_name, ":version": version },
        )?;
        Ok(())
    }
}

/// A wrapper so that we can impl [FromSql] and [ToSql] for multiple types.
pub struct Sql<T>(pub T);

impl<T> From<T> for Sql<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for Sql<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromSql for Sql<Txid> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Txid::from_str(value.as_str()?)
            .map(Self)
            .map_err(from_sql_error)
    }
}

impl ToSql for Sql<Txid> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.to_string().into())
    }
}

impl FromSql for Sql<BlockHash> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        BlockHash::from_str(value.as_str()?)
            .map(Self)
            .map_err(from_sql_error)
    }
}

impl ToSql for Sql<BlockHash> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.to_string().into())
    }
}

#[cfg(feature = "miniscript")]
impl FromSql for Sql<crate::KeychainId> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        crate::KeychainId::from_str(value.as_str()?)
            .map(Self)
            .map_err(from_sql_error)
    }
}

#[cfg(feature = "miniscript")]
impl ToSql for Sql<crate::KeychainId> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.to_string().into())
    }
}

impl FromSql for Sql<Transaction> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Transaction::consensus_decode_from_finite_reader(&mut value.as_bytes()?)
            .map(Self)
            .map_err(from_sql_error)
    }
}

impl ToSql for Sql<Transaction> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let mut bytes = Vec::<u8>::new();
        self.consensus_encode(&mut bytes).map_err(to_sql_error)?;
        Ok(bytes.into())
    }
}

impl FromSql for Sql<ScriptBuf> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Script::from_bytes(value.as_bytes()?).to_owned().into())
    }
}

impl ToSql for Sql<ScriptBuf> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.as_bytes().into())
    }
}

impl FromSql for Sql<Amount> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Amount::from_sat(value.as_i64()?.try_into().map_err(from_sql_error)?).into())
    }
}

impl ToSql for Sql<Amount> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let amount: i64 = self.to_sat().try_into().map_err(to_sql_error)?;
        Ok(amount.into())
    }
}

impl<A: Anchor + serde_crate::de::DeserializeOwned> FromSql for Sql<A> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        serde_json::from_slice(value.as_bytes()?)
            .map(Sql)
            .map_err(from_sql_error)
    }
}

impl<A: Anchor + serde_crate::Serialize> ToSql for Sql<A> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        serde_json::to_vec(&self.0)
            .map(Into::into)
            .map_err(to_sql_error)
    }
}

#[cfg(feature = "miniscript")]
impl FromSql for Sql<miniscript::Descriptor<miniscript::DescriptorPublicKey>> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        miniscript::Descriptor::from_str(value.as_str()?)
            .map(Self)
            .map_err(from_sql_error)
    }
}

#[cfg(feature = "miniscript")]
impl ToSql for Sql<miniscript::Descriptor<miniscript::DescriptorPublicKey>> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.to_string().into())
    }
}

impl FromSql for Sql<bitcoin::Network> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        bitcoin::Network::from_str(value.as_str()?)
            .map(Self)
            .map_err(from_sql_error)
    }
}

impl ToSql for Sql<bitcoin::Network> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.to_string().into())
    }
}

fn from_sql_error<E: std::error::Error + Send + Sync + 'static>(err: E) -> FromSqlError {
    FromSqlError::Other(Box::new(err))
}

fn to_sql_error<E: std::error::Error + Send + Sync + 'static>(err: E) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
}
