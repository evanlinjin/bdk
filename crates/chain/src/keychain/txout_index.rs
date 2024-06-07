use crate::{
    collections::*,
    indexed_tx_graph::Indexer,
    miniscript::{Descriptor, DescriptorPublicKey},
    spk_iter::BIP32_MAX_INDEX,
    DescriptorExt, DescriptorId, SpkIterator, SpkTxOutIndex,
};
use alloc::{borrow::ToOwned, vec::Vec};
use bitcoin::{Amount, OutPoint, Script, SignedAmount, Transaction, TxOut, Txid};
use core::{
    fmt::Debug,
    ops::{Bound, RangeBounds},
};

use super::*;
use crate::Append;

/// Represents updates to the derivation index of a [`KeychainTxOutIndex`].
/// It maps each keychain `K` to a descriptor and its last revealed index.
///
/// It can be applied to [`KeychainTxOutIndex`] with [`apply_changeset`]. [`ChangeSet] are
/// monotone in that they will never decrease the revealed derivation index.
///
/// [`KeychainTxOutIndex`]: crate::keychain::KeychainTxOutIndex
/// [`apply_changeset`]: crate::keychain::KeychainTxOutIndex::apply_changeset
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(
        crate = "serde_crate",
        bound(
            deserialize = "K: Ord + serde::Deserialize<'de>",
            serialize = "K: Ord + serde::Serialize"
        )
    )
)]
#[must_use]
pub struct ChangeSet<K> {
    /// Contains the keychains that have been added and their respective descriptor
    pub keychains_added: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
    /// Contains for each descriptor_id the last revealed index of derivation
    pub last_revealed: BTreeMap<DescriptorId, u32>,
}

impl<K: Ord> Append for ChangeSet<K> {
    /// Append another [`ChangeSet`] into self.
    ///
    /// For each keychain in `keychains_added` in the given [`ChangeSet`]:
    /// If the keychain already exist with a different descriptor, we overwrite the old descriptor.
    ///
    /// For each `last_revealed` in the given [`ChangeSet`]:
    /// If the keychain already exists, increase the index when the other's index > self's index.
    fn append(&mut self, other: Self) {
        for (new_keychain, new_descriptor) in other.keychains_added {
            if !self.keychains_added.contains_key(&new_keychain)
                // FIXME: very inefficient
                && self
                    .keychains_added
                    .values()
                    .all(|descriptor| descriptor != &new_descriptor)
            {
                self.keychains_added.insert(new_keychain, new_descriptor);
            }
        }

        // for `last_revealed`, entries of `other` will take precedence ONLY if it is greater than
        // what was originally in `self`.
        for (desc_id, index) in other.last_revealed {
            use crate::collections::btree_map::Entry;
            match self.last_revealed.entry(desc_id) {
                Entry::Vacant(entry) => {
                    entry.insert(index);
                }
                Entry::Occupied(mut entry) => {
                    if *entry.get() < index {
                        entry.insert(index);
                    }
                }
            }
        }
    }

    /// Returns whether the changeset are empty.
    fn is_empty(&self) -> bool {
        self.last_revealed.is_empty() && self.keychains_added.is_empty()
    }
}

impl<K> Default for ChangeSet<K> {
    fn default() -> Self {
        Self {
            last_revealed: BTreeMap::default(),
            keychains_added: BTreeMap::default(),
        }
    }
}

/// The default lookahead for a [`KeychainTxOutIndex`]
pub const DEFAULT_LOOKAHEAD: u32 = 25;

/// [`KeychainTxOutIndex`] controls how script pubkeys are revealed for multiple keychains, and
/// indexes [`TxOut`]s with them.
///
/// A single keychain is a chain of script pubkeys derived from a single [`Descriptor`]. Keychains
/// are identified using the `K` generic. Script pubkeys are identified by the keychain that they
/// are derived from `K`, as well as the derivation index `u32`.
///
/// # Revealed script pubkeys
///
/// Tracking how script pubkeys are revealed is useful for collecting chain data. For example, if
/// the user has requested 5 script pubkeys (to receive money with), we only need to use those
/// script pubkeys to scan for chain data.
///
/// Call [`reveal_to_target`] or [`reveal_next_spk`] to reveal more script pubkeys.
/// Call [`revealed_keychain_spks`] or [`revealed_spks`] to iterate through revealed script pubkeys.
///
/// # Lookahead script pubkeys
///
/// When an user first recovers a wallet (i.e. from a recovery phrase and/or descriptor), we will
/// NOT have knowledge of which script pubkeys are revealed. So when we index a transaction or
/// txout (using [`index_tx`]/[`index_txout`]) we scan the txouts against script pubkeys derived
/// above the last revealed index. These additionally-derived script pubkeys are called the
/// lookahead.
///
/// The [`KeychainTxOutIndex`] is constructed with the `lookahead` and cannot be altered. See
/// [`DEFAULT_LOOKAHEAD`] for the value used in the `Default` implementation. Use [`new`] to set a
/// custom `lookahead`.
///
/// # Unbounded script pubkey iterator
///
/// For script-pubkey-based chain sources (such as Electrum/Esplora), an initial scan is best done
/// by iterating though derived script pubkeys one by one and requesting transaction histories for
/// each script pubkey. We will stop after x-number of script pubkeys have empty histories. An
/// unbounded script pubkey iterator is useful to pass to such a chain source because it doesn't
/// require holding a reference to the index.
///
/// Call [`unbounded_spk_iter`] to get an unbounded script pubkey iterator for a given keychain.
/// Call [`all_unbounded_spk_iters`] to get unbounded script pubkey iterators for all keychains.
///
/// # Change sets
///
/// Methods that can update the last revealed index or add keychains will return [`ChangeSet`] to report
/// these changes. This should be persisted for future recovery.
///
/// ## Synopsis
///
/// ```
/// use bdk_chain::keychain::KeychainTxOutIndex;
/// # use bdk_chain::{ miniscript::{Descriptor, DescriptorPublicKey} };
/// # use core::str::FromStr;
///
/// // imagine our service has internal and external addresses but also addresses for users
/// #[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
/// enum MyKeychain {
///     External,
///     Internal,
///     MyAppUser {
///         user_id: u32
///     }
/// }
///
/// let mut txout_index = KeychainTxOutIndex::<MyKeychain>::default();
///
/// # let secp = bdk_chain::bitcoin::secp256k1::Secp256k1::signing_only();
/// # let (external_descriptor,_) = Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, "tr([73c5da0a/86'/0'/0']xprv9xgqHN7yz9MwCkxsBPN5qetuNdQSUttZNKw1dcYTV4mkaAFiBVGQziHs3NRSWMkCzvgjEe3n9xV8oYywvM8at9yRqyaZVz6TYYhX98VjsUk/0/*)").unwrap();
/// # let (internal_descriptor,_) = Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, "tr([73c5da0a/86'/0'/0']xprv9xgqHN7yz9MwCkxsBPN5qetuNdQSUttZNKw1dcYTV4mkaAFiBVGQziHs3NRSWMkCzvgjEe3n9xV8oYywvM8at9yRqyaZVz6TYYhX98VjsUk/1/*)").unwrap();
/// # let (descriptor_42, _) = Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, "tr([73c5da0a/86'/0'/0']xprv9xgqHN7yz9MwCkxsBPN5qetuNdQSUttZNKw1dcYTV4mkaAFiBVGQziHs3NRSWMkCzvgjEe3n9xV8oYywvM8at9yRqyaZVz6TYYhX98VjsUk/2/*)").unwrap();
/// let _ = txout_index.insert_descriptor(MyKeychain::External, external_descriptor)?;
/// let _ = txout_index.insert_descriptor(MyKeychain::Internal, internal_descriptor)?;
/// let _ = txout_index.insert_descriptor(MyKeychain::MyAppUser { user_id: 42 }, descriptor_42)?;
///
/// let new_spk_for_user = txout_index.reveal_next_spk(&MyKeychain::MyAppUser{ user_id: 42 });
/// # Ok::<_, bdk_chain::keychain::InsertDescriptorError<_>>(())
/// ```
///
/// [`Ord`]: core::cmp::Ord
/// [`SpkTxOutIndex`]: crate::spk_txout_index::SpkTxOutIndex
/// [`Descriptor`]: crate::miniscript::Descriptor
/// [`reveal_to_target`]: KeychainTxOutIndex::reveal_to_target
/// [`reveal_next_spk`]: KeychainTxOutIndex::reveal_next_spk
/// [`revealed_keychain_spks`]: KeychainTxOutIndex::revealed_keychain_spks
/// [`revealed_spks`]: KeychainTxOutIndex::revealed_spks
/// [`index_tx`]: KeychainTxOutIndex::index_tx
/// [`index_txout`]: KeychainTxOutIndex::index_txout
/// [`new`]: KeychainTxOutIndex::new
/// [`unbounded_spk_iter`]: KeychainTxOutIndex::unbounded_spk_iter
/// [`all_unbounded_spk_iters`]: KeychainTxOutIndex::all_unbounded_spk_iters
/// [`outpoints`]: KeychainTxOutIndex::outpoints
/// [`txouts`]: KeychainTxOutIndex::txouts
/// [`unused_spks`]: KeychainTxOutIndex::unused_spks
#[derive(Clone, Debug)]
pub struct KeychainTxOutIndex<K> {
    inner: SpkTxOutIndex<(K, u32)>,
    /// keychain -> (descriptor id) map
    keychains_to_descriptor_ids: BTreeMap<K, DescriptorId>,
    /// descriptor id -> keychain map
    descriptor_ids_to_keychains: BTreeMap<DescriptorId, K>,
    /// descriptor_id -> descriptor map
    /// This is a "monotone" map, meaning that its size keeps growing, i.e., we never delete
    /// descriptors from it. This is useful for revealing spks for descriptors that don't have
    /// keychains associated.
    descriptor_ids_to_descriptors: BTreeMap<DescriptorId, Descriptor<DescriptorPublicKey>>,
    /// last revealed indices for each descriptor.
    last_revealed: HashMap<DescriptorId, u32>,
    /// lookahead setting
    lookahead: u32,
}

impl<K> Default for KeychainTxOutIndex<K> {
    fn default() -> Self {
        Self::new(DEFAULT_LOOKAHEAD)
    }
}

impl<K: Clone + Ord + Debug> Indexer for KeychainTxOutIndex<K> {
    type ChangeSet = ChangeSet<K>;

    fn index_txout(&mut self, outpoint: OutPoint, txout: &TxOut) -> Self::ChangeSet {
        let mut changeset = ChangeSet::default();
        if let Some((keychain, index)) = self.inner.scan_txout(outpoint, txout) {
            let did = self
                .keychains_to_descriptor_ids
                .get(keychain)
                .expect("invariant");
            if self.last_revealed.get(did) < Some(index) {
                self.last_revealed.insert(*did, *index);
                changeset.last_revealed.insert(*did, *index);
                self.replenish_lookahead_did(*did);
            }
        }
        changeset
    }

    fn index_tx(&mut self, tx: &bitcoin::Transaction) -> Self::ChangeSet {
        let mut changeset = ChangeSet::<K>::default();
        for (op, txout) in tx.output.iter().enumerate() {
            changeset.append(self.index_txout(OutPoint::new(tx.txid(), op as u32), txout));
        }
        changeset
    }

    fn initial_changeset(&self) -> Self::ChangeSet {
        ChangeSet {
            keychains_added: self
                .keychains()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            last_revealed: self.last_revealed.clone().into_iter().collect(),
        }
    }

    fn apply_changeset(&mut self, changeset: Self::ChangeSet) {
        self.apply_changeset(changeset)
    }

    fn is_tx_relevant(&self, tx: &bitcoin::Transaction) -> bool {
        self.inner.is_relevant(tx)
    }
}

impl<K> KeychainTxOutIndex<K> {
    /// Construct a [`KeychainTxOutIndex`] with the given `lookahead`.
    ///
    /// The `lookahead` is the number of script pubkeys to derive and cache from the internal
    /// descriptors over and above the last revealed script index. Without a lookahead the index
    /// will miss outputs you own when processing transactions whose output script pubkeys lie
    /// beyond the last revealed index. In certain situations, such as when performing an initial
    /// scan of the blockchain during wallet import, it may be uncertain or unknown what the index
    /// of the last revealed script pubkey actually is.
    ///
    /// Refer to [struct-level docs](KeychainTxOutIndex) for more about `lookahead`.
    pub fn new(lookahead: u32) -> Self {
        Self {
            inner: SpkTxOutIndex::default(),
            keychains_to_descriptor_ids: BTreeMap::new(),
            descriptor_ids_to_descriptors: BTreeMap::new(),
            descriptor_ids_to_keychains: Default::default(),
            last_revealed: Default::default(),
            lookahead,
        }
    }
}

/// Methods that are *re-exposed* from the internal [`SpkTxOutIndex`].
impl<K: Clone + Ord + Debug> KeychainTxOutIndex<K> {
    /// Return a reference to the internal [`SpkTxOutIndex`].
    ///
    /// **WARNING:** The internal index will contain lookahead spks. Refer to
    /// [struct-level docs](KeychainTxOutIndex) for more about `lookahead`.
    pub fn inner(&self) -> &SpkTxOutIndex<(K, u32)> {
        &self.inner
    }

    /// Get the set of indexed outpoints, corresponding to tracked keychains.
    pub fn outpoints(&self) -> &BTreeSet<KeychainIndexed<K, OutPoint>> {
        self.inner.outpoints()
    }

    /// Iterate over known txouts that spend to tracked script pubkeys.
    pub fn txouts(
        &self,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<K, (OutPoint, &TxOut)>> + ExactSizeIterator
    {
        self.inner
            .txouts()
            .map(|(index, op, txout)| (index.clone(), (op, txout)))
    }

    /// Finds all txouts on a transaction that has previously been scanned and indexed.
    pub fn txouts_in_tx(
        &self,
        txid: Txid,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<K, (OutPoint, &TxOut)>> {
        self.inner
            .txouts_in_tx(txid)
            .map(|(index, op, txout)| (index.clone(), (op, txout)))
    }

    /// Return the [`TxOut`] of `outpoint` if it has been indexed, and if it corresponds to a
    /// tracked keychain.
    ///
    /// The associated keychain and keychain index of the txout's spk is also returned.
    ///
    /// This calls [`SpkTxOutIndex::txout`] internally.
    pub fn txout(&self, outpoint: OutPoint) -> Option<KeychainIndexed<K, &TxOut>> {
        self.inner
            .txout(outpoint)
            .map(|(index, txout)| (index.clone(), txout))
    }

    /// Return the script that exists under the given `keychain`'s `index`.
    ///
    /// This calls [`SpkTxOutIndex::spk_at_index`] internally.
    pub fn spk_at_index(&self, keychain: K, index: u32) -> Option<&Script> {
        self.inner.spk_at_index(&(keychain.clone(), index))
    }

    /// Returns the keychain and keychain index associated with the spk.
    ///
    /// This calls [`SpkTxOutIndex::index_of_spk`] internally.
    pub fn index_of_spk(&self, script: &Script) -> Option<&(K, u32)> {
        self.inner.index_of_spk(script)
    }

    /// Returns whether the spk under the `keychain`'s `index` has been used.
    ///
    /// Here, "unused" means that after the script pubkey was stored in the index, the index has
    /// never scanned a transaction output with it.
    ///
    /// This calls [`SpkTxOutIndex::is_used`] internally.
    pub fn is_used(&self, keychain: K, index: u32) -> bool {
        self.inner.is_used(&(keychain, index))
    }

    /// Marks the script pubkey at `index` as used even though the tracker hasn't seen an output
    /// with it.
    ///
    /// This only has an effect when the `index` had been added to `self` already and was unused.
    ///
    /// Returns whether the spk under the given `keychain` and `index` is successfully
    /// marked as used. Returns false either when there is no descriptor under the given
    /// keychain, or when the spk is already marked as used.
    ///
    /// This is useful when you want to reserve a script pubkey for something but don't want to add
    /// the transaction output using it to the index yet. Other callers will consider `index` on
    /// `keychain` used until you call [`unmark_used`].
    ///
    /// This calls [`SpkTxOutIndex::mark_used`] internally.
    ///
    /// [`unmark_used`]: Self::unmark_used
    pub fn mark_used(&mut self, keychain: K, index: u32) -> bool {
        self.inner.mark_used(&(keychain, index))
    }

    /// Undoes the effect of [`mark_used`]. Returns whether the `index` is inserted back into
    /// `unused`.
    ///
    /// Note that if `self` has scanned an output with this script pubkey, then this will have no
    /// effect.
    ///
    /// This calls [`SpkTxOutIndex::unmark_used`] internally.
    ///
    /// [`mark_used`]: Self::mark_used
    pub fn unmark_used(&mut self, keychain: K, index: u32) -> bool {
        self.inner.unmark_used(&(keychain, index))
    }

    /// Computes the total value transfer effect `tx` has on the script pubkeys belonging to the
    /// keychains in `range`. Value is *sent* when a script pubkey in the `range` is on an input and
    /// *received* when it is on an output. For `sent` to be computed correctly, the output being
    /// spent must have already been scanned by the index. Calculating received just uses the
    /// [`Transaction`] outputs directly, so it will be correct even if it has not been scanned.
    pub fn sent_and_received(
        &self,
        tx: &Transaction,
        range: impl RangeBounds<K>,
    ) -> (Amount, Amount) {
        self.inner
            .sent_and_received(tx, self.map_to_inner_bounds(range))
    }

    /// Computes the net value that this transaction gives to the script pubkeys in the index and
    /// *takes* from the transaction outputs in the index. Shorthand for calling
    /// [`sent_and_received`] and subtracting sent from received.
    ///
    /// This calls [`SpkTxOutIndex::net_value`] internally.
    ///
    /// [`sent_and_received`]: Self::sent_and_received
    pub fn net_value(&self, tx: &Transaction, range: impl RangeBounds<K>) -> SignedAmount {
        self.inner.net_value(tx, self.map_to_inner_bounds(range))
    }
}

impl<K: Clone + Ord + Debug> KeychainTxOutIndex<K> {
    /// Return the map of the keychain to descriptors.
    pub fn keychains(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&K, &Descriptor<DescriptorPublicKey>)> + ExactSizeIterator + '_
    {
        self.keychains_to_descriptor_ids.iter().map(|(k, did)| {
            (
                k,
                self.descriptor_ids_to_descriptors
                    .get(did)
                    .expect("invariant"),
            )
        })
    }

    /// Insert a descriptor with a keychain associated to it.
    ///
    /// Adding a descriptor means you will be able to derive new script pubkeys under it and the
    /// txout index will discover transaction outputs with those script pubkeys (once they've been
    /// derived and added to the index).
    ///
    /// keychain <-> descriptor is a one-to-one mapping that cannot be changed. Attempting to do so
    /// will return a [`InsertDescriptorError<K>`].
    pub fn insert_descriptor(
        &mut self,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
    ) -> Result<ChangeSet<K>, InsertDescriptorError<K>> {
        let mut changeset = ChangeSet::<K>::default();
        let desc_id = descriptor.descriptor_id();
        if !self.keychains_to_descriptor_ids.contains_key(&keychain)
            && !self.descriptor_ids_to_keychains.contains_key(&desc_id)
        {
            self.descriptor_ids_to_descriptors
                .insert(desc_id, descriptor.clone());
            self.keychains_to_descriptor_ids
                .insert(keychain.clone(), desc_id);
            self.descriptor_ids_to_keychains
                .insert(desc_id, keychain.clone());
            self.replenish_lookahead(&keychain, self.lookahead);
            changeset
                .keychains_added
                .insert(keychain.clone(), descriptor);
        } else {
            if let Some(existing_desc_id) = self.keychains_to_descriptor_ids.get(&keychain) {
                let descriptor = self
                    .descriptor_ids_to_descriptors
                    .get(existing_desc_id)
                    .expect("invariant");
                if *existing_desc_id != desc_id {
                    return Err(InsertDescriptorError::KeychainAlreadyAssigned {
                        existing_assignment: descriptor.clone(),
                        keychain,
                    });
                }
            }

            if let Some(existing_keychain) = self.descriptor_ids_to_keychains.get(&desc_id) {
                let descriptor = self
                    .descriptor_ids_to_descriptors
                    .get(&desc_id)
                    .expect("invariant")
                    .clone();

                if *existing_keychain != keychain {
                    return Err(InsertDescriptorError::DescriptorAlreadyAssigned {
                        existing_assignment: existing_keychain.clone(),
                        descriptor,
                    });
                }
            }
        }

        Ok(changeset)
    }

    /// Gets the descriptor associated with the keychain. Returns `None` if the keychain doesn't
    /// have a descriptor associated with it.
    pub fn get_descriptor(&self, keychain: &K) -> Option<&Descriptor<DescriptorPublicKey>> {
        let did = self.keychains_to_descriptor_ids.get(keychain)?;
        self.descriptor_ids_to_descriptors.get(did)
    }

    /// Get the lookahead setting.
    ///
    /// Refer to [`new`] for more information on the `lookahead`.
    ///
    /// [`new`]: Self::new
    pub fn lookahead(&self) -> u32 {
        self.lookahead
    }

    /// Store lookahead scripts until `target_index` (inclusive).
    ///
    /// This does not change the global `lookahead` setting.
    pub fn lookahead_to_target(&mut self, keychain: &K, target_index: u32) {
        if let Some((next_index, _)) = self.next_index(keychain) {
            let temp_lookahead = (target_index + 1)
                .checked_sub(next_index)
                .filter(|&index| index > 0);

            if let Some(temp_lookahead) = temp_lookahead {
                self.replenish_lookahead(keychain, temp_lookahead);
            }
        }
    }

    fn replenish_lookahead_did(&mut self, did: DescriptorId) {
        if let Some(keychain) = self.descriptor_ids_to_keychains.get(&did).cloned() {
            self.replenish_lookahead(&keychain, self.lookahead);
        }
    }

    fn replenish_lookahead(&mut self, keychain: &K, lookahead: u32) {
        if let Some(did) = self.keychains_to_descriptor_ids.get(keychain) {
            let descriptor = self
                .descriptor_ids_to_descriptors
                .get(did)
                .expect("invariant");
            let next_store_index = self
                .inner
                .all_spks()
                .range(&(keychain.clone(), u32::MIN)..=&(keychain.clone(), u32::MAX))
                .last()
                .map_or(0, |((_, index), _)| *index + 1);
            let next_reveal_index = self.last_revealed.get(did).map_or(0, |v| *v + 1);
            for (new_index, new_spk) in SpkIterator::new_with_range(
                descriptor,
                next_store_index..next_reveal_index + lookahead,
            ) {
                let _inserted = self
                    .inner
                    .insert_spk((keychain.clone(), new_index), new_spk);
                debug_assert!(_inserted, "replenish lookahead: must not have existing spk: keychain={:?}, lookahead={}, next_store_index={}, next_reveal_index={}", keychain, lookahead, next_store_index, next_reveal_index);
            }
        }
    }

    /// Get an unbounded spk iterator over a given `keychain`. Returns `None` if the provided
    /// keychain doesn't exist
    pub fn unbounded_spk_iter(
        &self,
        keychain: &K,
    ) -> Option<SpkIterator<Descriptor<DescriptorPublicKey>>> {
        let descriptor = self.get_descriptor(keychain)?.clone();
        Some(SpkIterator::new(descriptor))
    }

    /// Get unbounded spk iterators for all keychains.
    pub fn all_unbounded_spk_iters(
        &self,
    ) -> BTreeMap<K, SpkIterator<Descriptor<DescriptorPublicKey>>> {
        self.keychains_to_descriptor_ids
            .iter()
            .map(|(k, did)| {
                (
                    k.clone(),
                    SpkIterator::new(
                        self.descriptor_ids_to_descriptors
                            .get(did)
                            .expect("invariant")
                            .clone(),
                    ),
                )
            })
            .collect()
    }

    /// Iterate over revealed spks of keychains in `range`
    pub fn revealed_spks(
        &self,
        range: impl RangeBounds<K>,
    ) -> impl Iterator<Item = KeychainIndexed<K, &Script>> {
        let start = range.start_bound();
        let end = range.end_bound();
        let mut iter_last_revealed = self
            .keychains_to_descriptor_ids
            .range((start, end))
            .map(|(k, did)| (k, self.last_revealed.get(did).cloned()));
        let mut iter_spks = self
            .inner
            .all_spks()
            .range(self.map_to_inner_bounds((start, end)));
        let mut current_keychain = iter_last_revealed.next();
        // The reason we need a tricky algorithm is because of the "lookahead" feature which means
        // that some of the spks in the SpkTxoutIndex will not have been revealed yet. So we need to
        // filter out those spks that are above the last_revealed for that keychain. To do this we
        // iterate through the last_revealed for each keychain and the spks for each keychain in
        // tandem. This minimizes BTreeMap queries.
        core::iter::from_fn(move || loop {
            let ((keychain, index), spk) = iter_spks.next()?;
            // We need to find the last revealed that matches the current spk we are considering so
            // we skip ahead.
            while current_keychain?.0 < keychain {
                current_keychain = iter_last_revealed.next();
            }
            let (current_keychain, last_revealed) = current_keychain?;

            if current_keychain == keychain && Some(*index) <= last_revealed {
                break Some(((keychain.clone(), *index), spk.as_script()));
            }
        })
    }

    /// Iterate over revealed spks of the given `keychain` with ascending indices.
    ///
    /// This is a double ended iterator so you can easily reverse it to get an iterator where
    /// the script pubkeys that were most recently revealed are first.
    pub fn revealed_keychain_spks<'a>(
        &'a self,
        keychain: &'a K,
    ) -> impl DoubleEndedIterator<Item = Indexed<&Script>> + 'a {
        let end = self
            .last_revealed_index(keychain)
            .map(|v| v + 1)
            .unwrap_or(0);
        self.inner
            .all_spks()
            .range((keychain.clone(), 0)..(keychain.clone(), end))
            .map(|((_, index), spk)| (*index, spk.as_script()))
    }

    /// Iterate over revealed, but unused, spks of all keychains.
    pub fn unused_spks(
        &self,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<K, &Script>> + Clone {
        self.keychains_to_descriptor_ids
            .keys()
            .flat_map(|keychain| {
                self.unused_keychain_spks(keychain)
                    .map(|(i, spk)| ((keychain.clone(), i), spk))
            })
    }

    /// Iterate over revealed, but unused, spks of the given `keychain`.
    /// Returns an empty iterator if the provided keychain doesn't exist.
    pub fn unused_keychain_spks(
        &self,
        keychain: &K,
    ) -> impl DoubleEndedIterator<Item = Indexed<&Script>> + Clone {
        let end = match self.keychains_to_descriptor_ids.get(keychain) {
            Some(did) => self.last_revealed.get(did).map(|v| *v + 1).unwrap_or(0),
            None => 0,
        };

        self.inner
            .unused_spks((keychain.clone(), 0)..(keychain.clone(), end))
            .map(|((_, i), spk)| (*i, spk))
    }

    /// Get the next derivation index for `keychain`. The next index is the index after the last revealed
    /// derivation index.
    ///
    /// The second field in the returned tuple represents whether the next derivation index is new.
    /// There are two scenarios where the next derivation index is reused (not new):
    ///
    /// 1. The keychain's descriptor has no wildcard, and a script has already been revealed.
    /// 2. The number of revealed scripts has already reached 2^31 (refer to BIP-32).
    ///
    /// Not checking the second field of the tuple may result in address reuse.
    ///
    /// Returns None if the provided `keychain` doesn't exist.
    pub fn next_index(&self, keychain: &K) -> Option<(u32, bool)> {
        let did = self.keychains_to_descriptor_ids.get(keychain)?;
        let last_index = self.last_revealed.get(did).cloned();
        let descriptor = self
            .descriptor_ids_to_descriptors
            .get(did)
            .expect("invariant");

        // we can only get the next index if the wildcard exists.
        let has_wildcard = descriptor.has_wildcard();

        Some(match last_index {
            // if there is no index, next_index is always 0.
            None => (0, true),
            // descriptors without wildcards can only have one index.
            Some(_) if !has_wildcard => (0, false),
            // derivation index must be < 2^31 (BIP-32).
            Some(index) if index > BIP32_MAX_INDEX => {
                unreachable!("index is out of bounds")
            }
            Some(index) if index == BIP32_MAX_INDEX => (index, false),
            // get the next derivation index.
            Some(index) => (index + 1, true),
        })
    }

    /// Get the last derivation index that is revealed for each keychain.
    ///
    /// Keychains with no revealed indices will not be included in the returned [`BTreeMap`].
    pub fn last_revealed_indices(&self) -> BTreeMap<K, u32> {
        self.last_revealed
            .iter()
            .filter_map(|(desc_id, index)| {
                let keychain = self.descriptor_ids_to_keychains.get(desc_id)?;
                Some((keychain.clone(), *index))
            })
            .collect()
    }

    /// Get the last derivation index revealed for `keychain`. Returns None if the keychain doesn't
    /// exist, or if the keychain doesn't have any revealed scripts.
    pub fn last_revealed_index(&self, keychain: &K) -> Option<u32> {
        let descriptor_id = self.keychains_to_descriptor_ids.get(keychain)?;
        self.last_revealed.get(descriptor_id).cloned()
    }

    /// Convenience method to call [`Self::reveal_to_target`] on multiple keychains.
    pub fn reveal_to_target_multi(&mut self, keychains: &BTreeMap<K, u32>) -> ChangeSet<K> {
        let mut changeset = ChangeSet::default();

        for (keychain, &index) in keychains {
            if let Some((_, new_changeset)) = self.reveal_to_target(keychain, index) {
                changeset.append(new_changeset);
            }
        }

        changeset
    }

    /// Reveals script pubkeys of the `keychain`'s descriptor **up to and including** the
    /// `target_index`.
    ///
    /// If the `target_index` cannot be reached (due to the descriptor having no wildcard and/or
    /// the `target_index` is in the hardened index range), this method will make a best-effort and
    /// reveal up to the last possible index.
    ///
    /// This returns list of newly revealed indices (alongside their scripts) and a
    /// [`ChangeSet`], which reports updates to the latest revealed index. If no new script
    /// pubkeys are revealed, then both of these will be empty.
    ///
    /// Returns None if the provided `keychain` doesn't exist.
    pub fn reveal_to_target(
        &mut self,
        keychain: &K,
        target_index: u32,
    ) -> Option<(Vec<Indexed<ScriptBuf>>, ChangeSet<K>)> {
        let mut changeset = ChangeSet::default();
        let mut spks: Vec<Indexed<ScriptBuf>> = vec![];
        while let Some((i, new)) = self.next_index(keychain) {
            if !new || i > target_index {
                break;
            }
            match self.reveal_next_spk(keychain) {
                Some(((i, spk), change)) => {
                    spks.push((i, spk));
                    changeset.append(change);
                }
                None => break,
            }
        }

        Some((spks, changeset))
    }

    /// Attempts to reveal the next script pubkey for `keychain`.
    ///
    /// Returns the derivation index of the revealed script pubkey, the revealed script pubkey and a
    /// [`ChangeSet`] which represents changes in the last revealed index (if any).
    /// Returns None if the provided keychain doesn't exist.
    ///
    /// When a new script cannot be revealed, we return the last revealed script and an empty
    /// [`ChangeSet`]. There are two scenarios when a new script pubkey cannot be derived:
    ///
    ///  1. The descriptor has no wildcard and already has one script revealed.
    ///  2. The descriptor has already revealed scripts up to the numeric bound.
    ///  3. There is no descriptor associated with the given keychain.
    pub fn reveal_next_spk(&mut self, keychain: &K) -> Option<(Indexed<ScriptBuf>, ChangeSet<K>)> {
        let (next_index, new) = self.next_index(keychain)?;
        let mut changeset = ChangeSet::default();

        if new {
            let did = self.keychains_to_descriptor_ids.get(keychain)?;
            let descriptor = self.descriptor_ids_to_descriptors.get(did)?;
            let spk = descriptor
                .at_derivation_index(next_index)
                .expect("already checked index is not too high")
                .script_pubkey();
            let _ = self.inner.insert_spk((keychain.clone(), next_index), spk);
            self.last_revealed.insert(*did, next_index);
            changeset.last_revealed.insert(*did, next_index);
            self.replenish_lookahead(keychain, self.lookahead);
        }
        let script = self
            .inner
            .spk_at_index(&(keychain.clone(), next_index))
            .expect("we just inserted it");
        Some(((next_index, script.into()), changeset))
    }

    /// Gets the next unused script pubkey in the keychain. I.e., the script pubkey with the lowest
    /// index that has not been used yet.
    ///
    /// This will derive and reveal a new script pubkey if no more unused script pubkeys exist.
    ///
    /// If the descriptor has no wildcard and already has a used script pubkey or if a descriptor
    /// has used all scripts up to the derivation bounds, then the last derived script pubkey will be
    /// returned.
    ///
    /// Returns `None` if there are no script pubkeys that have been used and no new script pubkey
    /// could be revealed (see [`reveal_next_spk`] for when this happens).
    ///
    /// [`reveal_next_spk`]: Self::reveal_next_spk
    pub fn next_unused_spk(&mut self, keychain: &K) -> Option<(Indexed<ScriptBuf>, ChangeSet<K>)> {
        let next_unused = self
            .unused_keychain_spks(keychain)
            .next()
            .map(|(i, spk)| ((i, spk.to_owned()), ChangeSet::default()));

        next_unused.or_else(|| self.reveal_next_spk(keychain))
    }

    /// Iterate over all [`OutPoint`]s that have `TxOut`s with script pubkeys derived from
    /// `keychain`.
    pub fn keychain_outpoints<'a>(
        &'a self,
        keychain: &'a K,
    ) -> impl DoubleEndedIterator<Item = Indexed<OutPoint>> + 'a {
        self.keychain_outpoints_in_range(keychain..=keychain)
            .map(|((_, i), op)| (i, op))
    }

    /// Iterate over [`OutPoint`]s that have script pubkeys derived from keychains in `range`.
    pub fn keychain_outpoints_in_range<'a>(
        &'a self,
        range: impl RangeBounds<K> + 'a,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<K, OutPoint>> + 'a {
        self.inner
            .outputs_in_range(self.map_to_inner_bounds(range))
            .map(|((k, i), op)| ((k.clone(), *i), op))
    }

    fn map_to_inner_bounds(&self, bound: impl RangeBounds<K>) -> impl RangeBounds<(K, u32)> {
        let start = match bound.start_bound() {
            Bound::Included(keychain) => Bound::Included((keychain.clone(), u32::MIN)),
            Bound::Excluded(keychain) => Bound::Excluded((keychain.clone(), u32::MAX)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match bound.end_bound() {
            Bound::Included(keychain) => Bound::Included((keychain.clone(), u32::MAX)),
            Bound::Excluded(keychain) => Bound::Excluded((keychain.clone(), u32::MIN)),
            Bound::Unbounded => Bound::Unbounded,
        };

        (start, end)
    }

    /// Returns the highest derivation index of the `keychain` where [`KeychainTxOutIndex`] has
    /// found a [`TxOut`] with it's script pubkey.
    pub fn last_used_index(&self, keychain: &K) -> Option<u32> {
        self.keychain_outpoints(keychain).last().map(|(i, _)| i)
    }

    /// Returns the highest derivation index of each keychain that [`KeychainTxOutIndex`] has found
    /// a [`TxOut`] with it's script pubkey.
    pub fn last_used_indices(&self) -> BTreeMap<K, u32> {
        self.keychains_to_descriptor_ids
            .iter()
            .filter_map(|(keychain, _)| {
                self.last_used_index(keychain)
                    .map(|index| (keychain.clone(), index))
            })
            .collect()
    }

    /// Applies the derivation changeset to the [`KeychainTxOutIndex`], as specified in the
    /// [`ChangeSet::append`] documentation:
    /// - Extends the number of derived scripts per keychain
    /// - Adds new descriptors introduced
    /// - If a descriptor is introduced for a keychain that already had a descriptor, overwrites
    /// the old descriptor
    pub fn apply_changeset(&mut self, changeset: ChangeSet<K>) {
        let ChangeSet {
            keychains_added,
            last_revealed,
        } = changeset;
        for (keychain, descriptor) in keychains_added {
            let _ = self.insert_descriptor(keychain, descriptor);
        }

        for (&desc_id, &index) in &last_revealed {
            let v = self.last_revealed.entry(desc_id).or_default();
            *v = index.max(*v);
        }

        for did in last_revealed.keys() {
            self.replenish_lookahead_did(*did);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Error returned from [`KeychainTxOutIndex::insert_descriptor`]
pub enum InsertDescriptorError<K> {
    /// The descriptor has already been assigned to a keychain so you can't assign it to another
    DescriptorAlreadyAssigned {
        /// The descriptor you have attempted to reassign
        descriptor: Descriptor<DescriptorPublicKey>,
        /// The keychain that the descriptor is already assigned to
        existing_assignment: K,
    },
    /// The keychain is already assigned to a descriptor so you can't reassign it
    KeychainAlreadyAssigned {
        /// The keychain that you have attempted to reassign
        keychain: K,
        /// The descriptor that the keychain is already assigned to
        existing_assignment: Descriptor<DescriptorPublicKey>,
    },
}

impl<K: core::fmt::Debug> core::fmt::Display for InsertDescriptorError<K> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            InsertDescriptorError::DescriptorAlreadyAssigned {
                existing_assignment: existing,
                descriptor,
            } => {
                write!(
                    f,
                    "attempt to re-assign descriptor {descriptor:?} already assigned to {existing:?}"
                )
            }
            InsertDescriptorError::KeychainAlreadyAssigned {
                existing_assignment: existing,
                keychain,
            } => {
                write!(
                    f,
                    "attempt to re-assign keychain {keychain:?} already assigned to {existing:?}"
                )
            }
        }
    }
}

#[cfg(feature = "std")]
impl<K: core::fmt::Debug> std::error::Error for InsertDescriptorError<K> {}
