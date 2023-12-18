//! This is a cached version of [`LocalChain`].
//!
//! [`LocalChain`]: super::local_chain::LocalChain

use core::ops::Deref;

use alloc::collections::BTreeMap;
use bitcoin::BlockHash;

use crate::{
    local_chain::{
        AlterCheckPointError, CannotConnectError, ChangeSet, CheckPoint, LocalChain,
        MissingGenesisError, Update,
    },
    BlockId,
};

/// A cached version of [`LocalChain`].
#[derive(Debug)]
pub struct CachedChain {
    chain: LocalChain,
    // This contains the latest update's tip that connects with the original chain.
    cache: Option<CheckPoint>,
}

impl Deref for CachedChain {
    type Target = LocalChain;

    fn deref(&self) -> &Self::Target {
        &self.chain
    }
}

impl CachedChain {
    /// Create a new [`CachedChain`] from [`LocalChain`].
    pub fn new(chain: LocalChain) -> Self {
        Self { chain, cache: None }
    }

    /// Access the inner [`LocalChain`].
    pub fn inner(&self) -> &LocalChain {
        &self.chain
    }

    /// Applies the given `update` to the chain.
    ///
    /// This wraps the inner [`LocalChain`]'s method of the same name.
    pub fn apply_update(&mut self, update: Update) -> Result<ChangeSet, CannotConnectError> {
        let update_tip = update.tip.clone();
        let changeset = self.chain.apply_update(update)?;
        if !changeset.is_empty() {
            self.cache = Some(update_tip);
        }
        Ok(changeset)
    }

    /// Apply the given `ChangeSet`.
    ///
    /// This wraps the inner [`LocalChain`]'s method of the same name.
    pub fn apply_changeset(&mut self, changeset: &ChangeSet) -> Result<(), MissingGenesisError> {
        self.chain.apply_changeset(changeset)?;
        self.cache = None;
        Ok(())
    }

    /// Apply the given `update`, but only keep relevant checkpoints.
    ///
    /// This cannot introduce older blocks.
    pub fn apply_update_relevant<F>(
        &mut self,
        update_tip: CheckPoint,
        is_relevant: F,
    ) -> Result<ChangeSet, CannotConnectError>
    where
        F: Fn(BlockId) -> bool,
    {
        let original_tip_id = self.chain.tip().block_id();

        let mut relevant_update = BTreeMap::<u32, BlockHash>::new();
        let mut update_iter = update_tip.iter();
        let mut update_pos = update_iter.next();

        let mut connects_with_cache = false;
        if let Some(cached_tip) = &self.cache {
            let mut cached_iter = cached_tip.iter();
            let mut cached_pos = cached_iter.next();

            while let (Some(u_cp), Some(c_cp)) = (update_pos.clone(), cached_pos.clone()) {
                let u_id = u_cp.block_id();
                let c_id = c_cp.block_id();

                // We stop when the update's iterator has reached the chain tip's height.
                if u_id.height <= original_tip_id.height {
                    break;
                }
                if is_relevant(u_id) {
                    relevant_update.insert(u_id.height, u_id.hash);
                }
                if u_id.height == c_id.height {
                    if u_id.hash == c_id.hash {
                        connects_with_cache = true;
                        relevant_update.insert(original_tip_id.height, original_tip_id.hash);
                        break;
                    }
                    update_pos = update_iter.next();
                    cached_pos = cached_iter.next();
                    continue;
                }
                if u_id.height > c_id.height {
                    update_pos = update_iter.next();
                    continue;
                }
                if u_id.height < c_id.height {
                    cached_pos = cached_iter.next();
                    continue;
                }
            }
        }

        if !connects_with_cache {
            let mut original_iter = self.chain.tip().iter();
            let mut original_pos = original_iter.next();

            while let (Some(u_cp), Some(o_cp)) = (update_pos.clone(), original_pos.clone()) {
                let u_id = u_cp.block_id();
                let o_id = o_cp.block_id();

                if is_relevant(u_id) {
                    relevant_update.insert(u_id.height, u_id.hash);
                }
                if u_id.height == o_id.height {
                    relevant_update.insert(u_id.height, u_id.hash);
                    if u_id.hash == o_id.hash {
                        break;
                    }
                    update_pos = update_iter.next();
                    original_pos = original_iter.next();
                    continue;
                }
                if u_id.height > o_id.height {
                    update_pos = update_iter.next();
                    continue;
                }
                if u_id.height < o_id.height {
                    original_pos = original_iter.next();
                    continue;
                }
            }
        }

        let mut relevant_tip = Option::<CheckPoint>::None;
        for (height, hash) in relevant_update {
            let block_id = BlockId { height, hash };
            match &mut relevant_tip {
                Some(tip) => *tip = tip.clone().push(block_id).expect("must push"),
                tip => *tip = Some(CheckPoint::new(block_id)),
            }
        }
        let changeset = self.chain.apply_update(Update {
            tip: relevant_tip.expect("must have at least one checkpoint"),
            introduce_older_blocks: false,
        })?;

        // If it connects, the update tip is cached. Anything that connects with this update tip
        // (above the chain tip's height) can also connect with the original chain.
        if !changeset.is_empty() {
            self.cache = Some(update_tip);
        }

        Ok(changeset)
    }

    /// Insert a block.
    pub fn insert_block(&mut self, block_id: BlockId) -> Result<ChangeSet, AlterCheckPointError> {
        let changeset = self.chain.insert_block(block_id)?;
        if !changeset.is_empty() {
            self.cache = None;
        }
        Ok(changeset)
    }
}
