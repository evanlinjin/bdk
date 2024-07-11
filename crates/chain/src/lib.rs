//! This crate is a collection of core structures for [Bitcoin Dev Kit].
//!
//! The goal of this crate is to give wallets the mechanisms needed to:
//!
//! 1. Figure out what data they need to fetch.
//! 2. Process the data in a way that never leads to inconsistent states.
//! 3. Fully index that data and expose it to be consumed without friction.
//!
//! Our design goals for these mechanisms are:
//!
//! 1. Data source agnostic -- nothing in `bdk_chain` cares about where you get data from or whether
//!    you do it synchronously or asynchronously. If you know a fact about the blockchain, you can just
//!    tell `bdk_chain`'s APIs about it, and that information will be integrated, if it can be done
//!    consistently.
//! 2. Data persistence agnostic -- `bdk_chain` does not care where you cache on-chain data, what you
//!    cache or how you retrieve it from persistent storage.
//!
//! [Bitcoin Dev Kit]: https://bitcoindevkit.org/

#![no_std]
#![warn(missing_docs)]

pub use bitcoin;
mod spk_txout_index;
pub use spk_txout_index::*;
mod chain_data;
pub use chain_data::*;
pub mod indexed_tx_graph;
pub use indexed_tx_graph::IndexedTxGraph;
pub mod keychain;
pub use keychain::{Indexed, KeychainIndexed};
pub mod local_chain;
mod tx_data_traits;
pub mod tx_graph;
pub use tx_data_traits::*;
pub use tx_graph::TxGraph;
mod chain_oracle;
pub use chain_oracle::*;

#[doc(hidden)]
pub mod example_utils;

#[cfg(feature = "miniscript")]
pub use miniscript;
#[cfg(feature = "miniscript")]
mod descriptor_ext;
#[cfg(feature = "miniscript")]
pub use descriptor_ext::{DescriptorExt, KeychainId};
#[cfg(feature = "miniscript")]
mod spk_iter;
#[cfg(feature = "miniscript")]
pub use spk_iter::*;
mod changeset;
pub use changeset::*;
pub mod spk_client;

#[allow(unused_imports)]
#[macro_use]
extern crate alloc;

#[cfg(feature = "serde")]
pub extern crate serde_crate as serde;

#[cfg(feature = "std")]
#[macro_use]
extern crate std;

#[cfg(all(not(feature = "std"), feature = "hashbrown"))]
extern crate hashbrown;

// When no-std use `alloc`'s Hash collections. This is activated by default
#[cfg(all(not(feature = "std"), not(feature = "hashbrown")))]
#[doc(hidden)]
pub mod collections {
    #![allow(dead_code)]
    pub type HashSet<K> = alloc::collections::BTreeSet<K>;
    pub type HashMap<K, V> = alloc::collections::BTreeMap<K, V>;
    pub use alloc::collections::{btree_map as hash_map, *};
}

// When we have std, use `std`'s all collections
#[cfg(all(feature = "std", not(feature = "hashbrown")))]
#[doc(hidden)]
pub mod collections {
    pub use std::collections::{hash_map, *};
}

// With this special feature `hashbrown`, use `hashbrown`'s hash collections, and else from `alloc`.
#[cfg(feature = "hashbrown")]
#[doc(hidden)]
pub mod collections {
    #![allow(dead_code)]
    pub type HashSet<K> = hashbrown::HashSet<K>;
    pub type HashMap<K, V> = hashbrown::HashMap<K, V>;
    pub use alloc::collections::*;
    pub use hashbrown::hash_map;
}

/// How many confirmations are needed f or a coinbase output to be spent.
pub const COINBASE_MATURITY: u32 = 100;

#[cfg(feature = "sqlite")]
mod sqlite_util {
    use core::{ops::Deref, str::FromStr};

    use alloc::{borrow::ToOwned, boxed::Box, string::ToString, vec::Vec};
    use bdk_sqlite::rusqlite::{
        self, named_params,
        types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
        ToSql,
    };
    use bitcoin::{
        consensus::{Decodable, Encodable},
        Amount, BlockHash, Script, ScriptBuf, Transaction, Txid,
    };

    use crate::{Anchor, Append, KeychainId};

    pub trait PersistWith<Conn, Update, Params> {
        type InitError;
        type WriteError;

        fn init(conn: &mut Conn, params: &Params) -> Result<Self, Self::InitError>
        where
            Self: Sized;

        fn write(
            &mut self,
            conn: &mut Conn,
            params: &Params,
            update: &Update,
        ) -> Result<(), Self::WriteError>;
    }

    pub struct Staged<T, U> {
        inner: T,
        stage: U,
    }

    pub struct Persisted<T, U, P> {
        inner: T,
        stage: U,
        params: P,
    }

    impl<T, U: Append + Default, P> Persisted<T, U, P> {
        pub fn new<Conn>(conn: &mut Conn, params: P) -> Result<Self, T::InitError>
        where
            T: PersistWith<Conn, U, P>,
        {
            let inner = T::init(conn, &params)?;
            let stage = U::default();
            Ok(Self {
                inner,
                params,
                stage,
            })
        }

        pub fn stage<Conn, F, R, E>(&mut self, f: F) -> Result<R, E>
        where
            T: PersistWith<Conn, U, P>,
            F: Fn(&mut T) -> Result<(R, U), E>,
        {
            let (r, update) = f(&mut self.inner)?;
            self.stage.append(update);
            Ok(r)
        }

        pub fn commit<Conn>(&mut self, conn: &mut Conn) -> Result<bool, T::WriteError>
        where
            T: PersistWith<Conn, U, P>,
        {
            if self.stage.is_empty() {
                return Ok(false);
            }
            self.inner.write(conn, &self.params, &self.stage)?;
            self.stage.take();
            Ok(true)
        }
    }

    pub enum StageAndCommitError<WriteError, E> {
        WriteError(WriteError),
        InnerError(E),
    }

    pub struct SchemaParams<'p> {
        pub table_name: &'p str,
        pub schema_name: &'p str,
    }

    impl<'p> SchemaParams<'p> {
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

        pub fn set_version(
            &self,
            db_tx: &rusqlite::Transaction,
            version: u32,
        ) -> rusqlite::Result<()> {
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

    impl FromSql for Sql<KeychainId> {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            KeychainId::from_str(value.as_str()?)
                .map(Self)
                .map_err(from_sql_error)
        }
    }

    impl ToSql for Sql<KeychainId> {
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
            bdk_sqlite::serde_json::from_slice(value.as_bytes()?)
                .map(Sql)
                .map_err(from_sql_error)
        }
    }

    impl<A: Anchor + serde_crate::Serialize> ToSql for Sql<A> {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            bdk_sqlite::serde_json::to_vec(&self.0)
                .map(Into::into)
                .map_err(to_sql_error)
        }
    }

    fn from_sql_error<E: std::error::Error + Send + Sync + 'static>(err: E) -> FromSqlError {
        FromSqlError::Other(Box::new(err))
    }

    fn to_sql_error<E: std::error::Error + Send + Sync + 'static>(err: E) -> rusqlite::Error {
        rusqlite::Error::ToSqlConversionFailure(Box::new(err))
    }
}
