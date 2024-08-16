use core::fmt;
use std::prelude::rust_2015::Box;
use chain::FutureResult;
use crate::{descriptor::DescriptorError, Wallet};

/// Represents a persisted wallet.
pub type PersistedWallet = bdk_chain::Persisted<Wallet>;

#[cfg(feature = "sqlx")]
impl<'c> chain::PersistAsyncWith<chain::sqlx::Transaction<'c, chain::sqlx::Postgres>> for Wallet {
    type CreateParams = crate::CreateParams;
    type LoadParams = crate::LoadParams;

    type CreateError = CreateWithPersistError<chain::sqlx::Error>;
    type LoadError = LoadWithPersistError<chain::sqlx::Error>;
    type PersistError = chain::sqlx::Error;

    fn create(
        db: &mut chain::sqlx::Transaction<'c, chain::sqlx::Postgres>,
        params: Self::CreateParams,
    ) -> FutureResult<'c, Self, Self::CreateError> {
        Box::pin(async move {
            let mut wallet = crate::Wallet::create_with_params(params).map_err(CreateWithPersistError::Descriptor)?;
            if let Some(changeset) = wallet.take_staged() {
                changeset
                    .persist_to_postgres(db)
                    .await
                    .map_err(CreateWithPersistError::Persist)?;
            }
            Ok(wallet)
        })
    }

    fn load(
        conn: &mut chain::sqlx::Transaction<'c, chain::sqlx::Postgres>,
        params: Self::LoadParams,
    ) -> FutureResult<'c, Option<Self>, Self::LoadError> {
        Box::pin(async move {
            let changeset = crate::ChangeSet::from_postgres(conn)
                .await
                .map_err(LoadWithPersistError::Persist)?;
            if chain::Merge::is_empty(&changeset) {
                return Ok(None);
            }
            crate::wallet::Wallet::load_with_params(changeset, params).map_err(LoadWithPersistError::InvalidChangeSet)
        })
    }

    fn persist<'a>(
        db: &'a mut chain::sqlx::Transaction<'c, chain::sqlx::Postgres>,
        changeset: &'a <Self as chain::Staged>::ChangeSet,
    ) -> FutureResult<'a, (), Self::PersistError> {
        Box::pin(async move {
            changeset.persist_to_postgres(db).await
        })
    }
}

#[cfg(feature = "sqlx")]
impl chain::PersistAsyncWith<chain::sqlx::PgPool> for Wallet {
    type CreateParams = crate::CreateParams;
    type LoadParams = crate::LoadParams;

    type CreateError = CreateWithPersistError<chain::sqlx::Error>;
    type LoadError = LoadWithPersistError<chain::sqlx::Error>;
    type PersistError = chain::sqlx::Error;

    fn create(
        db: &mut chain::sqlx::PgPool,
        params: Self::CreateParams,
    ) -> FutureResult<Self, Self::CreateError> {
        // async fn create(
        //     db: &mut chain::sqlx::PgPool,
        //     params: <crate::wallet::Wallet as chain::PersistAsyncWith<chain::sqlx::Pool<chain::sqlx::Postgres>>>::CreateParams
        // ) -> Result<Wallet, <crate::wallet::Wallet as chain::PersistAsyncWith<chain::sqlx::Pool<chain::sqlx::Postgres>>>::CreateError> {
        Box::pin( async move {
            let mut db_tx = db.begin().await.map_err(CreateWithPersistError::Persist)?;
            let wallet = chain::PersistAsyncWith::create(&mut db_tx, params).await?;
            db_tx.commit().await.map_err(CreateWithPersistError::Persist)?;
            Ok(wallet)
        })
        // Box::pin(create(db, params))
    }

    fn load(
        db: &mut chain::sqlx::PgPool,
        params: Self::LoadParams,
    ) -> FutureResult<Option<Self>, Self::LoadError> {
        // async fn load(
        //     db: &mut chain::sqlx::PgPool,
        //     params: <crate::wallet::Wallet as chain::PersistAsyncWith<chain::sqlx::Pool<chain::sqlx::Postgres>>>::LoadParams,
        // ) -> Result<Option<Wallet>, <crate::wallet::Wallet as chain::PersistAsyncWith<chain::sqlx::Pool<chain::sqlx::Postgres>>>::LoadError> {
        Box::pin(async move {
            let mut db_tx = db.begin().await.map_err(LoadWithPersistError::Persist)?;
            let wallet_opt = chain::PersistAsyncWith::load (&mut db_tx, params).await?;
            db_tx.commit().await.map_err(LoadWithPersistError::Persist)?;
            Ok(wallet_opt)
        })
        // Box::pin(load(db, params))
    }

    fn persist<'a>(
        db: &'a mut chain::sqlx::PgPool,
        changeset: &'a <Self as chain::Staged>::ChangeSet,
    ) -> FutureResult<'a, (), Self::PersistError> {
        // async fn persist(
        //     db: &mut chain::sqlx::PgPool,
        //     changeset: &<Wallet as chain::Staged>::ChangeSet,
        // ) -> Result<(), <crate::wallet::Wallet as chain::PersistAsyncWith<chain::sqlx::Pool<chain::sqlx::Postgres>>>::PersistError> {
        Box::pin( async move {
            let mut db_tx = db.begin().await?;
            changeset.persist_to_postgres(&mut db_tx).await?;
            db_tx.commit().await?;
            Ok(())
        })
        // Box::pin(persist(db, changeset))
    }
}



#[cfg(feature = "rusqlite")]
impl<'c> chain::PersistWith<bdk_chain::rusqlite::Transaction<'c>> for Wallet {
    type CreateParams = crate::CreateParams;
    type LoadParams = crate::LoadParams;

    type CreateError = CreateWithPersistError<bdk_chain::rusqlite::Error>;
    type LoadError = LoadWithPersistError<bdk_chain::rusqlite::Error>;
    type PersistError = bdk_chain::rusqlite::Error;

    fn create(
        db: &mut bdk_chain::rusqlite::Transaction<'c>,
        params: Self::CreateParams,
    ) -> Result<Self, Self::CreateError> {
        let mut wallet =
            Self::create_with_params(params).map_err(CreateWithPersistError::Descriptor)?;
        if let Some(changeset) = wallet.take_staged() {
            changeset
                .persist_to_sqlite(db)
                .map_err(CreateWithPersistError::Persist)?;
        }
        Ok(wallet)
    }

    fn load(
        conn: &mut bdk_chain::rusqlite::Transaction<'c>,
        params: Self::LoadParams,
    ) -> Result<Option<Self>, Self::LoadError> {
        let changeset =
            crate::ChangeSet::from_sqlite(conn).map_err(LoadWithPersistError::Persist)?;
        if chain::Merge::is_empty(&changeset) {
            return Ok(None);
        }
        Self::load_with_params(changeset, params).map_err(LoadWithPersistError::InvalidChangeSet)
    }

    fn persist(
        db: &mut bdk_chain::rusqlite::Transaction<'c>,
        changeset: &<Self as chain::Staged>::ChangeSet,
    ) -> Result<(), Self::PersistError> {
        changeset.persist_to_sqlite(db)
    }
}

#[cfg(feature = "rusqlite")]
impl chain::PersistWith<bdk_chain::rusqlite::Connection> for Wallet {
    type CreateParams = crate::CreateParams;
    type LoadParams = crate::LoadParams;

    type CreateError = CreateWithPersistError<bdk_chain::rusqlite::Error>;
    type LoadError = LoadWithPersistError<bdk_chain::rusqlite::Error>;
    type PersistError = bdk_chain::rusqlite::Error;

    fn create(
        db: &mut bdk_chain::rusqlite::Connection,
        params: Self::CreateParams,
    ) -> Result<Self, Self::CreateError> {
        let mut db_tx = db.transaction().map_err(CreateWithPersistError::Persist)?;
        let wallet = chain::PersistWith::create(&mut db_tx, params)?;
        db_tx.commit().map_err(CreateWithPersistError::Persist)?;
        Ok(wallet)
    }

    fn load(
        db: &mut bdk_chain::rusqlite::Connection,
        params: Self::LoadParams,
    ) -> Result<Option<Self>, Self::LoadError> {
        let mut db_tx = db.transaction().map_err(LoadWithPersistError::Persist)?;
        let wallet_opt = chain::PersistWith::load(&mut db_tx, params)?;
        db_tx.commit().map_err(LoadWithPersistError::Persist)?;
        Ok(wallet_opt)
    }

    fn persist(
        db: &mut bdk_chain::rusqlite::Connection,
        changeset: &<Self as chain::Staged>::ChangeSet,
    ) -> Result<(), Self::PersistError> {
        let db_tx = db.transaction()?;
        changeset.persist_to_sqlite(&db_tx)?;
        db_tx.commit()
    }
}

#[cfg(feature = "file_store")]
impl chain::PersistWith<bdk_file_store::Store<crate::ChangeSet>> for Wallet {
    type CreateParams = crate::CreateParams;
    type LoadParams = crate::LoadParams;
    type CreateError = CreateWithPersistError<std::io::Error>;
    type LoadError =
        LoadWithPersistError<bdk_file_store::AggregateChangesetsError<crate::ChangeSet>>;
    type PersistError = std::io::Error;

    fn create(
        db: &mut bdk_file_store::Store<crate::ChangeSet>,
        params: Self::CreateParams,
    ) -> Result<Self, Self::CreateError> {
        let mut wallet =
            Self::create_with_params(params).map_err(CreateWithPersistError::Descriptor)?;
        if let Some(changeset) = wallet.take_staged() {
            db.append_changeset(&changeset)
                .map_err(CreateWithPersistError::Persist)?;
        }
        Ok(wallet)
    }

    fn load(
        db: &mut bdk_file_store::Store<crate::ChangeSet>,
        params: Self::LoadParams,
    ) -> Result<Option<Self>, Self::LoadError> {
        let changeset = db
            .aggregate_changesets()
            .map_err(LoadWithPersistError::Persist)?
            .unwrap_or_default();
        Self::load_with_params(changeset, params).map_err(LoadWithPersistError::InvalidChangeSet)
    }

    fn persist(
        db: &mut bdk_file_store::Store<crate::ChangeSet>,
        changeset: &<Self as chain::Staged>::ChangeSet,
    ) -> Result<(), Self::PersistError> {
        db.append_changeset(changeset)
    }
}

/// Error type for [`PersistedWallet::load`].
#[derive(Debug, PartialEq)]
pub enum LoadWithPersistError<E> {
    /// Error from persistence.
    Persist(E),
    /// Occurs when the loaded changeset cannot construct [`Wallet`].
    InvalidChangeSet(crate::LoadError),
}

impl<E: fmt::Display> fmt::Display for LoadWithPersistError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Persist(err) => fmt::Display::fmt(err, f),
            Self::InvalidChangeSet(err) => fmt::Display::fmt(&err, f),
        }
    }
}

#[cfg(feature = "std")]
impl<E: fmt::Debug + fmt::Display> std::error::Error for LoadWithPersistError<E> {}

/// Error type for [`PersistedWallet::create`].
#[derive(Debug)]
pub enum CreateWithPersistError<E> {
    /// Error from persistence.
    Persist(E),
    /// Occurs when the loaded changeset cannot construct [`Wallet`].
    Descriptor(DescriptorError),
}

impl<E: fmt::Display> fmt::Display for CreateWithPersistError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Persist(err) => fmt::Display::fmt(err, f),
            Self::Descriptor(err) => fmt::Display::fmt(&err, f),
        }
    }
}

#[cfg(feature = "std")]
impl<E: fmt::Debug + fmt::Display> std::error::Error for CreateWithPersistError<E> {}
