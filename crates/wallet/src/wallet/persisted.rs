use core::{
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
};

use alloc::boxed::Box;
use chain::{Merge, Staged};

use crate::{descriptor::DescriptorError, ChangeSet, CreateParams, LoadParams, Wallet};

/// A trait for persisting [`Wallet`].
pub trait WalletPersister {
    /// Error when initializing the persister.
    type Error;

    /// Initialize the persister.
    fn initialize(persister: &mut Self) -> Result<(), Self::Error>;

    /// Persist changes.
    fn persist(persister: &mut Self, changeset: &ChangeSet) -> Result<(), Self::Error>;

    /// Load changes.
    fn load(persister: &mut Self) -> Result<Option<ChangeSet>, Self::Error>;
}

type FutureResult<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Async version of [`WalletPersister`].
pub trait AsyncWalletPersister {
    /// Error with persistence.
    type Error;

    fn initialize(persister: &mut Self) -> FutureResult<(), Self::Error>;

    /// Persist changes.
    fn persist<'a>(
        persister: &'a mut Self,
        changeset: &'a ChangeSet,
    ) -> FutureResult<'a, (), Self::Error>;

    /// Load changes.
    fn load(persister: &mut Self) -> FutureResult<Option<ChangeSet>, Self::Error>;
}

/// Represents a persisted wallet.
#[derive(Debug)]
pub struct PersistedWallet(pub(crate) Wallet);

impl Deref for PersistedWallet {
    type Target = Wallet;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PersistedWallet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl PersistedWallet {
    pub fn create<P>(
        persister: &mut P,
        params: CreateParams,
    ) -> Result<Self, CreateWithPersistError<P::Error>>
    where
        P: WalletPersister,
    {
        let mut inner =
            Wallet::create_with_params(params).map_err(CreateWithPersistError::Descriptor)?;
        if let Some(changeset) = inner.take_staged() {
            P::persist(persister, &changeset).map_err(CreateWithPersistError::Persist)?;
        }
        Ok(Self(inner))
    }

    pub async fn create_async<P>(
        persister: &mut P,
        params: CreateParams,
    ) -> Result<Self, CreateWithPersistError<P::Error>>
    where
        P: AsyncWalletPersister,
    {
        let mut inner =
            Wallet::create_with_params(params).map_err(CreateWithPersistError::Descriptor)?;
        if let Some(changeset) = inner.take_staged() {
            P::persist(persister, &changeset)
                .await
                .map_err(CreateWithPersistError::Persist)?;
        }
        Ok(Self(inner))
    }

    pub fn load<P>(
        persister: &mut P,
        params: LoadParams,
    ) -> Result<Option<Self>, LoadWithPersistError<P::Error>>
    where
        P: WalletPersister,
    {
        let changeset = match P::load(persister).map_err(LoadWithPersistError::Persist)? {
            Some(changeset) => changeset,
            None => return Ok(None),
        };
        Wallet::load_with_params(changeset, params)
            .map(|opt| opt.map(|inner| PersistedWallet(inner)))
            .map_err(LoadWithPersistError::InvalidChangeSet)
    }

    pub async fn load_async<P>(
        persister: &mut P,
        params: LoadParams,
    ) -> Result<Option<Self>, LoadWithPersistError<P::Error>>
    where
        P: AsyncWalletPersister,
    {
        let changeset = match P::load(persister)
            .await
            .map_err(LoadWithPersistError::Persist)?
        {
            Some(changeset) => changeset,
            None => return Ok(None),
        };
        Wallet::load_with_params(changeset, params)
            .map(|opt| opt.map(|inner| PersistedWallet(inner)))
            .map_err(LoadWithPersistError::InvalidChangeSet)
    }

    pub fn persist<P>(&mut self, persister: &mut P) -> Result<bool, P::Error>
    where
        P: WalletPersister,
    {
        let stage = Staged::staged(&mut self.0);
        if stage.is_empty() {
            return Ok(false);
        }
        P::persist(persister, &*stage)?;
        stage.take();
        Ok(true)
    }

    pub async fn persist_async<'a, P>(&'a mut self, persister: &mut P) -> Result<bool, P::Error>
    where
        P: AsyncWalletPersister,
    {
        let stage = Staged::staged(&mut self.0);
        if stage.is_empty() {
            return Ok(false);
        }
        P::persist(persister, &*stage).await?;
        stage.take();
        Ok(true)
    }
}

#[cfg(feature = "rusqlite")]
impl<'c> WalletPersister for bdk_chain::rusqlite::Transaction<'c> {
    type Error = bdk_chain::rusqlite::Error;

    fn initialize(persister: &mut Self) -> Result<(), Self::Error> {
        // TODO: expose init methods
        Ok(())
    }

    fn persist(persister: &mut Self, changeset: &ChangeSet) -> Result<(), Self::Error> {
        changeset.persist_to_sqlite(persister)
    }

    fn load(persister: &mut Self) -> Result<Option<ChangeSet>, Self::Error> {
        ChangeSet::from_sqlite(persister).map(Some)
    }
}

#[cfg(feature = "rusqlite")]
impl WalletPersister for bdk_chain::rusqlite::Connection {
    type Error = bdk_chain::rusqlite::Error;

    fn initialize(persister: &mut Self) -> Result<(), Self::Error> {
        // TODO: expose init methods
        Ok(())
    }

    fn persist(persister: &mut Self, changeset: &ChangeSet) -> Result<(), Self::Error> {
        let db_tx = persister.transaction()?;
        changeset.persist_to_sqlite(&db_tx)?;
        db_tx.commit()
    }

    fn load(persister: &mut Self) -> Result<Option<ChangeSet>, Self::Error> {
        let db_tx = persister.transaction()?;
        let changeset = ChangeSet::from_sqlite(&db_tx).map(Some)?;
        db_tx.commit()?;
        Ok(changeset)
    }
}

#[cfg(feature = "file_store")]
#[derive(Debug)]
pub enum FileStoreError {
    Load(bdk_file_store::AggregateChangesetsError<ChangeSet>),
    Write(std::io::Error),
}

#[cfg(feature = "file_store")]
impl core::fmt::Display for FileStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use core::fmt::Display;
        match self {
            FileStoreError::Load(e) => Display::fmt(e, f),
            FileStoreError::Write(e) => Display::fmt(e, f),
        }
    }
}

#[cfg(feature = "file_store")]
impl std::error::Error for FileStoreError {}

#[cfg(feature = "file_store")]
impl WalletPersister for bdk_file_store::Store<ChangeSet> {
    type Error = FileStoreError;

    fn initialize(persister: &mut Self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn persist(persister: &mut Self, changeset: &ChangeSet) -> Result<(), Self::Error> {
        persister.append_changeset(changeset).map_err(FileStoreError::Write)
    }

    fn load(persister: &mut Self) -> Result<Option<ChangeSet>, Self::Error> {
        persister.aggregate_changesets().map_err(FileStoreError::Load)
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
