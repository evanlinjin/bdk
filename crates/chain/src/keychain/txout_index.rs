use crate::{
    collections::*,
    indexed_tx_graph::Indexer,
    miniscript::{Descriptor, DescriptorPublicKey},
    spk_iter::BIP32_MAX_INDEX,
    DescriptorExt, KeychainId, SpkIterator, SpkTxOutIndex,
};
use alloc::{borrow::ToOwned, vec::Vec};
use bdk_sqlite::rusqlite::named_params;
use bitcoin::{Amount, OutPoint, Script, SignedAmount, Transaction, TxOut, Txid};
use core::{
    fmt::Debug,
    ops::{Bound, RangeBounds},
};

use super::*;
use crate::Append;

/// The default lookahead for a [`KeychainTxOutIndex`]
pub const DEFAULT_LOOKAHEAD: u32 = 25;

/// [`KeychainTxOutIndex`] controls how script pubkeys are revealed for multiple keychains, and
/// indexes [`TxOut`]s with them.
///
/// Keychains are labelled using the `L` generic, and script pubkeys are identified by a tuple of
/// the keychain's label (`L`) and derivation index (`u32`).
///
/// There is a strict 1-to-1 relationship between keychains and labels. Each label can only be
/// associated with one keychain and each keychain can only be associated with one label. The
/// [`insert_keychain`] method will return an error if you try and violate this invariant. This
/// rule is a proxy for a stronger rule for keychains contained in the `KeychainTxOutIndex`: no two
/// keychains should produce the same script pubkey. Having two keychains produce the same script
/// pubkey should cause whichever keychain derives the script pubkey first to be the effective owner
/// of it but you should not rely on this behaviour. **⚠ It is up you, the developer, not to violate
/// this invariant.**
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
/// let _ = txout_index.insert_keychain(MyKeychain::External, external_descriptor)?;
/// let _ = txout_index.insert_keychain(MyKeychain::Internal, internal_descriptor)?;
/// let _ = txout_index.insert_keychain(MyKeychain::MyAppUser { user_id: 42 }, descriptor_42)?;
///
/// let new_spk_for_user = txout_index.reveal_next_spk(&MyKeychain::MyAppUser{ user_id: 42 });
/// # Ok::<_, bdk_chain::keychain::InsertKeychainError<_>>(())
/// ```
///
/// [`Ord`]: core::cmp::Ord
/// [`SpkTxOutIndex`]: crate::spk_txout_index::SpkTxOutIndex
/// [`Descriptor`]: crate::miniscript::Descriptor
/// [`reveal_to_target`]: Self::reveal_to_target
/// [`reveal_next_spk`]: Self::reveal_next_spk
/// [`revealed_keychain_spks`]: Self::revealed_keychain_spks
/// [`revealed_spks`]: Self::revealed_spks
/// [`index_tx`]: Self::index_tx
/// [`index_txout`]: Self::index_txout
/// [`new`]: Self::new
/// [`unbounded_spk_iter`]: Self::unbounded_spk_iter
/// [`all_unbounded_spk_iters`]: Self::all_unbounded_spk_iters
/// [`outpoints`]: Self::outpoints
/// [`txouts`]: Self::txouts
/// [`unused_spks`]: Self::unused_spks
/// [`insert_keychain`]: Self::insert_keychain
#[derive(Clone, Debug)]
pub struct KeychainTxOutIndex<L> {
    inner: SpkTxOutIndex<(L, u32)>,
    label_to_keychain_id: BTreeMap<L, KeychainId>,
    keychain_id_to_label: HashMap<KeychainId, L>,
    keychains: HashMap<KeychainId, Descriptor<DescriptorPublicKey>>,
    last_revealed: HashMap<KeychainId, u32>,
    lookahead: u32,
}

impl<L> Default for KeychainTxOutIndex<L> {
    fn default() -> Self {
        Self::new(DEFAULT_LOOKAHEAD)
    }
}

impl<L: Clone + Ord + Debug> Indexer for KeychainTxOutIndex<L> {
    type ChangeSet = ChangeSet;

    fn index_txout(&mut self, outpoint: OutPoint, txout: &TxOut) -> Self::ChangeSet {
        let mut changeset = ChangeSet::default();
        if let Some((label, index)) = self.inner.scan_txout(outpoint, txout).cloned() {
            let k_id = self.label_to_keychain_id.get(&label).expect("invariant");
            if self.last_revealed.get(k_id) < Some(&index) {
                self.last_revealed.insert(*k_id, index);
                changeset.last_revealed.insert(*k_id, index);
                self.replenish_inner_index(*k_id, &label, self.lookahead);
            }
        }
        changeset
    }

    fn index_tx(&mut self, tx: &bitcoin::Transaction) -> Self::ChangeSet {
        let mut changeset = ChangeSet::default();
        let txid = tx.compute_txid();
        for (op, txout) in tx.output.iter().enumerate() {
            changeset.append(self.index_txout(OutPoint::new(txid, op as u32), txout));
        }
        changeset
    }

    fn initial_changeset(&self) -> Self::ChangeSet {
        ChangeSet {
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

impl<L> KeychainTxOutIndex<L> {
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
            label_to_keychain_id: Default::default(),
            keychains: Default::default(),
            keychain_id_to_label: Default::default(),
            last_revealed: Default::default(),
            lookahead,
        }
    }
}

/// Methods that are *re-exposed* from the internal [`SpkTxOutIndex`].
impl<L: Clone + Ord + Debug> KeychainTxOutIndex<L> {
    /// Return a reference to the internal [`SpkTxOutIndex`].
    ///
    /// **WARNING**: The internal index will contain lookahead spks. Refer to
    /// [struct-level docs](KeychainTxOutIndex) for more about `lookahead`.
    pub fn inner(&self) -> &SpkTxOutIndex<(L, u32)> {
        &self.inner
    }

    /// Get the set of indexed outpoints, corresponding to tracked keychains.
    pub fn outpoints(&self) -> &BTreeSet<KeychainIndexed<L, OutPoint>> {
        self.inner.outpoints()
    }

    /// Iterate over known txouts that spend to tracked script pubkeys.
    pub fn txouts(
        &self,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<L, (OutPoint, &TxOut)>> + ExactSizeIterator
    {
        self.inner
            .txouts()
            .map(|(index, op, txout)| (index.clone(), (op, txout)))
    }

    /// Finds all txouts on a transaction that has previously been scanned and indexed.
    pub fn txouts_in_tx(
        &self,
        txid: Txid,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<L, (OutPoint, &TxOut)>> {
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
    pub fn txout(&self, outpoint: OutPoint) -> Option<KeychainIndexed<L, &TxOut>> {
        self.inner
            .txout(outpoint)
            .map(|(index, txout)| (index.clone(), txout))
    }

    /// Return the script that exists under the given keychain's `index`.
    ///
    /// This calls [`SpkTxOutIndex::spk_at_index`] internally.
    pub fn spk_at_index(&self, label: L, index: u32) -> Option<&Script> {
        self.inner.spk_at_index(&(label.clone(), index))
    }

    /// Returns the label and keychain index associated with the spk.
    ///
    /// This calls [`SpkTxOutIndex::index_of_spk`] internally.
    pub fn index_of_spk(&self, script: &Script) -> Option<&(L, u32)> {
        self.inner.index_of_spk(script)
    }

    /// Returns whether the spk under the keychain's `index` has been used.
    ///
    /// Here, "unused" means that after the script pubkey was stored in the index, the index has
    /// never scanned a transaction output with it.
    ///
    /// This calls [`SpkTxOutIndex::is_used`] internally.
    pub fn is_used(&self, label: L, index: u32) -> bool {
        self.inner.is_used(&(label, index))
    }

    /// Marks the script pubkey at `index` as used even though the tracker hasn't seen an output
    /// with it.
    ///
    /// This only has an effect when the `index` had been added to `self` already and was unused.
    ///
    /// Returns whether the spk under the given keychain `label` and `index` is successfully
    /// marked as used. Returns false either when there is no descriptor under the given
    /// keychain, or when the spk is already marked as used.
    ///
    /// This is useful when you want to reserve a script pubkey for something but don't want to add
    /// the transaction output using it to the index yet. Other callers will consider `index` on
    /// keychain used until you call [`unmark_used`].
    ///
    /// This calls [`SpkTxOutIndex::mark_used`] internally.
    ///
    /// [`unmark_used`]: Self::unmark_used
    pub fn mark_used(&mut self, label: L, index: u32) -> bool {
        self.inner.mark_used(&(label, index))
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
    pub fn unmark_used(&mut self, label: L, index: u32) -> bool {
        self.inner.unmark_used(&(label, index))
    }

    /// Computes the total value transfer effect `tx` has on the script pubkeys belonging to the
    /// keychains in `range`. Value is *sent* when a script pubkey in the `range` is on an input and
    /// *received* when it is on an output. For `sent` to be computed correctly, the output being
    /// spent must have already been scanned by the index. Calculating received just uses the
    /// [`Transaction`] outputs directly, so it will be correct even if it has not been scanned.
    pub fn sent_and_received(
        &self,
        tx: &Transaction,
        range: impl RangeBounds<L>,
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
    pub fn net_value(&self, tx: &Transaction, range: impl RangeBounds<L>) -> SignedAmount {
        self.inner.net_value(tx, self.map_to_inner_bounds(range))
    }
}

impl<L: Clone + Ord + Debug> KeychainTxOutIndex<L> {
    /// Return all keychains and their corresponding labels.
    pub fn keychains(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&L, &Descriptor<DescriptorPublicKey>)> + ExactSizeIterator + '_
    {
        self.label_to_keychain_id
            .iter()
            .map(|(k, k_id)| (k, self.keychains.get(k_id).expect("invariant")))
    }

    /// Insert a keychain with a label associated to it.
    ///
    /// Returns whether a new keychain is actually introduced.
    ///
    /// Adding a keychain means you will be able to derive new script pubkeys under it and the
    /// txout index will discover transaction outputs with those script pubkeys (once they've been
    /// derived and added to the index).
    ///
    /// # Errors
    ///
    /// keychain <-> label is a one-to-one mapping that cannot be changed. Attempting to do so
    /// will return a [`InsertKeychainError<K>`].
    pub fn insert_keychain(
        &mut self,
        label: L,
        keychain: Descriptor<DescriptorPublicKey>,
    ) -> Result<bool, InsertKeychainError<L>> {
        let k_id = keychain.descriptor_id();
        if !self.label_to_keychain_id.contains_key(&label)
            && !self.keychain_id_to_label.contains_key(&k_id)
        {
            self.keychains.insert(k_id, keychain.clone());
            self.label_to_keychain_id.insert(label.clone(), k_id);
            self.keychain_id_to_label.insert(k_id, label.clone());
            self.replenish_inner_index(k_id, &label, self.lookahead);
            return Ok(true);
        }

        if let Some(existing_desc_id) = self.label_to_keychain_id.get(&label) {
            let descriptor = self.keychains.get(existing_desc_id).expect("invariant");
            if *existing_desc_id != k_id {
                return Err(InsertKeychainError::LabelAlreadyUsed {
                    existing_assignment: descriptor.clone(),
                    label,
                });
            }
        }

        if let Some(existing_keychain) = self.keychain_id_to_label.get(&k_id) {
            let descriptor = self.keychains.get(&k_id).expect("invariant").clone();

            if *existing_keychain != label {
                return Err(InsertKeychainError::DescriptorAlreadyLabelled {
                    existing_assignment: existing_keychain.clone(),
                    keychain: descriptor,
                });
            }
        }
        Ok(false)
    }

    /// Get the keychain associated with the `label`. Returns `None` if the label does not have a
    /// keychain associated with it.
    pub fn keychain(&self, label: &L) -> Option<&Descriptor<DescriptorPublicKey>> {
        let k_id = self.label_to_keychain_id.get(label)?;
        self.keychains.get(k_id)
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
    pub fn lookahead_to_target(&mut self, label: &L, target_index: u32) {
        if let Some((next_index, _)) = self.next_index(label) {
            let temp_lookahead = (target_index + 1)
                .checked_sub(next_index)
                .filter(|&index| index > 0);

            if let Some(temp_lookahead) = temp_lookahead {
                self.replenish_inner_index_label(label, temp_lookahead);
            }
        }
    }

    fn replenish_inner_index_kid(&mut self, k_id: KeychainId, lookahead: u32) {
        if let Some(label) = self.keychain_id_to_label.get(&k_id).cloned() {
            self.replenish_inner_index(k_id, &label, lookahead);
        }
    }

    fn replenish_inner_index_label(&mut self, label: &L, lookahead: u32) {
        if let Some(k_id) = self.label_to_keychain_id.get(label) {
            self.replenish_inner_index(*k_id, label, lookahead);
        }
    }

    /// Syncs the state of the inner spk index after changes to a keychain
    fn replenish_inner_index(&mut self, k_id: KeychainId, label: &L, lookahead: u32) {
        let descriptor = self.keychains.get(&k_id).expect("invariant");
        let next_store_index = self
            .inner
            .all_spks()
            .range(&(label.clone(), u32::MIN)..=&(label.clone(), u32::MAX))
            .last()
            .map_or(0, |((_, index), _)| *index + 1);
        let next_reveal_index = self.last_revealed.get(&k_id).map_or(0, |v| *v + 1);
        for (new_index, new_spk) in
            SpkIterator::new_with_range(descriptor, next_store_index..next_reveal_index + lookahead)
        {
            let _inserted = self.inner.insert_spk((label.clone(), new_index), new_spk);
            debug_assert!(_inserted, "replenish lookahead: must not have existing spk: keychain={:?}, lookahead={}, next_store_index={}, next_reveal_index={}", label, lookahead, next_store_index, next_reveal_index);
        }
    }

    /// Get an unbounded spk iterator over a given `keychain`. Returns `None` if the provided
    /// keychain doesn't exist
    pub fn unbounded_spk_iter(
        &self,
        label: &L,
    ) -> Option<SpkIterator<Descriptor<DescriptorPublicKey>>> {
        let descriptor = self.keychain(label)?.clone();
        Some(SpkIterator::new(descriptor))
    }

    /// Get unbounded spk iterators for all keychains.
    pub fn all_unbounded_spk_iters(
        &self,
    ) -> BTreeMap<L, SpkIterator<Descriptor<DescriptorPublicKey>>> {
        self.label_to_keychain_id
            .iter()
            .map(|(label, k_id)| {
                (
                    label.clone(),
                    SpkIterator::new(self.keychains.get(k_id).expect("invariant").clone()),
                )
            })
            .collect()
    }

    /// Iterate over revealed spks of keychains in `range`
    pub fn revealed_spks(
        &self,
        range: impl RangeBounds<L>,
    ) -> impl Iterator<Item = KeychainIndexed<L, &Script>> {
        let start = range.start_bound();
        let end = range.end_bound();
        let mut iter_last_revealed = self
            .label_to_keychain_id
            .range((start, end))
            .map(|(label, k_id)| (label, self.last_revealed.get(k_id).cloned()));
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

    /// Iterate over revealed spks of the given keychain `label` with ascending indices.
    ///
    /// This is a double ended iterator so you can easily reverse it to get an iterator where
    /// the script pubkeys that were most recently revealed are first.
    pub fn revealed_keychain_spks<'a>(
        &'a self,
        label: &'a L,
    ) -> impl DoubleEndedIterator<Item = Indexed<&Script>> + 'a {
        let end = self.last_revealed_index(label).map(|v| v + 1).unwrap_or(0);
        self.inner
            .all_spks()
            .range((label.clone(), 0)..(label.clone(), end))
            .map(|((_, index), spk)| (*index, spk.as_script()))
    }

    /// Iterate over revealed, but unused, spks of all keychains.
    pub fn unused_spks(
        &self,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<L, &Script>> + Clone {
        self.label_to_keychain_id.keys().flat_map(|keychain| {
            self.unused_keychain_spks(keychain)
                .map(|(i, spk)| ((keychain.clone(), i), spk))
        })
    }

    /// Iterate over revealed, but unused, spks of the given keychain `label`.
    /// Returns an empty iterator if the provided keychain doesn't exist.
    pub fn unused_keychain_spks(
        &self,
        label: &L,
    ) -> impl DoubleEndedIterator<Item = Indexed<&Script>> + Clone {
        let end = match self.label_to_keychain_id.get(label) {
            Some(k_id) => self.last_revealed.get(k_id).map(|v| *v + 1).unwrap_or(0),
            None => 0,
        };

        self.inner
            .unused_spks((label.clone(), 0)..(label.clone(), end))
            .map(|((_, i), spk)| (*i, spk))
    }

    /// Get the next derivation index for keychain `label`. The next index is the index after the
    /// last revealed derivation index.
    ///
    /// The second field in the returned tuple represents whether the next derivation index is new.
    /// There are two scenarios where the next derivation index is reused (not new):
    ///
    /// 1. The keychain has no wildcard, and a script has already been revealed.
    /// 2. The number of revealed scripts has already reached 2^31 (refer to BIP-32).
    ///
    /// Not checking the second field of the tuple may result in address reuse.
    ///
    /// Returns None if the provided keychain of `label` doesn't exist.
    pub fn next_index(&self, label: &L) -> Option<(u32, bool)> {
        let k_id = self.label_to_keychain_id.get(label)?;
        let last_index = self.last_revealed.get(k_id).cloned();
        let descriptor = self.keychains.get(k_id).expect("invariant");

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
    pub fn last_revealed_indices(&self) -> BTreeMap<L, u32> {
        self.last_revealed
            .iter()
            .filter_map(|(desc_id, index)| {
                let keychain = self.keychain_id_to_label.get(desc_id)?;
                Some((keychain.clone(), *index))
            })
            .collect()
    }

    /// Get the last derivation index revealed for keychain of `label`. Returns `None` if the
    /// keychain does not exist, or if the keychain does not have any revealed scripts.
    pub fn last_revealed_index(&self, label: &L) -> Option<u32> {
        let descriptor_id = self.label_to_keychain_id.get(label)?;
        self.last_revealed.get(descriptor_id).cloned()
    }

    /// Convenience method to call [`Self::reveal_to_target`] on multiple keychains.
    pub fn reveal_to_target_multi(&mut self, keychains: &BTreeMap<L, u32>) -> ChangeSet {
        let mut changeset = ChangeSet::default();

        for (label, &index) in keychains {
            if let Some((_, new_changeset)) = self.reveal_to_target(label, index) {
                changeset.append(new_changeset);
            }
        }

        changeset
    }

    /// Reveals script pubkeys of the keychain's descriptor **up to and including** the
    /// `target_index`.
    ///
    /// If the `target_index` cannot be reached (due to the keychain having no wildcard and/or
    /// the `target_index` is in the hardened index range), this method will make a best-effort and
    /// reveal up to the last possible index.
    ///
    /// This returns list of newly revealed indices (alongside their scripts) and a
    /// [`ChangeSet`], which reports updates to the latest revealed index. If no new script
    /// pubkeys are revealed, then both of these will be empty.
    ///
    /// Returns None if the provided keychain of `label` doesn't exist.
    #[must_use]
    pub fn reveal_to_target(
        &mut self,
        label: &L,
        target_index: u32,
    ) -> Option<(Vec<Indexed<ScriptBuf>>, ChangeSet)> {
        let mut changeset = ChangeSet::default();
        let mut spks: Vec<Indexed<ScriptBuf>> = vec![];
        while let Some((i, new)) = self.next_index(label) {
            if !new || i > target_index {
                break;
            }
            match self.reveal_next_spk(label) {
                Some(((i, spk), change)) => {
                    spks.push((i, spk));
                    changeset.append(change);
                }
                None => break,
            }
        }

        Some((spks, changeset))
    }

    /// Attempts to reveal the next script pubkey for keychain of `label`.
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
    pub fn reveal_next_spk(&mut self, label: &L) -> Option<(Indexed<ScriptBuf>, ChangeSet)> {
        let (next_index, new) = self.next_index(label)?;
        let mut changeset = ChangeSet::default();

        if new {
            let k_id = self.label_to_keychain_id.get(label)?;
            self.last_revealed.insert(*k_id, next_index);
            changeset.last_revealed.insert(*k_id, next_index);
            self.replenish_inner_index(*k_id, label, self.lookahead);
        }
        let script = self
            .inner
            .spk_at_index(&(label.clone(), next_index))
            .expect("we just inserted it");
        Some(((next_index, script.into()), changeset))
    }

    /// Get the next unused script pubkey in the keychain of `label`. I.e., the script pubkey with
    /// the lowest index that has not been used yet.
    ///
    /// This will derive and reveal a new script pubkey if no more unused script pubkeys exist.
    ///
    /// If the keychain has no wildcard and already has a used script pubkey or if a keychain has
    /// used all scripts up to the derivation bounds, then the last derived script pubkey will be
    /// returned.
    ///
    /// Returns `None` if there are no script pubkeys that have been used and no new script pubkey
    /// could be revealed (see [`reveal_next_spk`] for when this happens).
    ///
    /// [`reveal_next_spk`]: Self::reveal_next_spk
    pub fn next_unused_spk(&mut self, label: &L) -> Option<(Indexed<ScriptBuf>, ChangeSet)> {
        let next_unused = self
            .unused_keychain_spks(label)
            .next()
            .map(|(i, spk)| ((i, spk.to_owned()), ChangeSet::default()));

        next_unused.or_else(|| self.reveal_next_spk(label))
    }

    /// Iterate over all [`OutPoint`]s that have `TxOut`s with script pubkeys derived from
    /// keychain of `label`.
    pub fn keychain_outpoints<'a>(
        &'a self,
        label: &'a L,
    ) -> impl DoubleEndedIterator<Item = Indexed<OutPoint>> + 'a {
        self.keychain_outpoints_in_range(label..=label)
            .map(|((_, i), op)| (i, op))
    }

    /// Iterate over [`OutPoint`]s that have script pubkeys derived from keychains in `range`.
    pub fn keychain_outpoints_in_range<'a>(
        &'a self,
        range: impl RangeBounds<L> + 'a,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<L, OutPoint>> + 'a {
        self.inner
            .outputs_in_range(self.map_to_inner_bounds(range))
            .map(|((k, i), op)| ((k.clone(), *i), op))
    }

    fn map_to_inner_bounds(&self, bound: impl RangeBounds<L>) -> impl RangeBounds<(L, u32)> {
        let start = match bound.start_bound() {
            Bound::Included(label) => Bound::Included((label.clone(), u32::MIN)),
            Bound::Excluded(label) => Bound::Excluded((label.clone(), u32::MAX)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match bound.end_bound() {
            Bound::Included(label) => Bound::Included((label.clone(), u32::MAX)),
            Bound::Excluded(label) => Bound::Excluded((label.clone(), u32::MIN)),
            Bound::Unbounded => Bound::Unbounded,
        };

        (start, end)
    }

    /// Returns the highest derivation index of the keychain of `label` where [`KeychainTxOutIndex`]
    /// has found a [`TxOut`] with it's script pubkey.
    pub fn last_used_index(&self, label: &L) -> Option<u32> {
        self.keychain_outpoints(label).last().map(|(i, _)| i)
    }

    /// Returns the highest derivation index of each keychain that [`KeychainTxOutIndex`] has found
    /// a [`TxOut`] with it's script pubkey.
    pub fn last_used_indices(&self) -> BTreeMap<L, u32> {
        self.label_to_keychain_id
            .iter()
            .filter_map(|(keychain, _)| {
                self.last_used_index(keychain)
                    .map(|index| (keychain.clone(), index))
            })
            .collect()
    }

    /// Applies the `ChangeSet<L>` to the [`KeychainTxOutIndex<L>`]
    ///
    /// Keychains added by the `keychains_added` field of `ChangeSet<L>` respect the one-to-one
    /// keychain <-> descriptor invariant by silently ignoring attempts to violate it (but will
    /// panic if `debug_assertions` are enabled).
    pub fn apply_changeset(&mut self, changeset: ChangeSet) {
        let last_revealed = changeset.last_revealed;

        for (&k_id, &index) in &last_revealed {
            let v = self.last_revealed.entry(k_id).or_default();
            *v = index.max(*v);
        }

        for k_id in last_revealed.keys() {
            self.replenish_inner_index_kid(*k_id, self.lookahead);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Error returned from [`KeychainTxOutIndex::insert_keychain`]
pub enum InsertKeychainError<L> {
    /// The keychain has already been introduced with another label.
    DescriptorAlreadyLabelled {
        /// The keychain you have attempted to reintroduce with a different label.
        keychain: Descriptor<DescriptorPublicKey>,
        /// The label that the existing keychain is assigned to.
        existing_assignment: L,
    },
    /// The label is already used with a different keychain.
    LabelAlreadyUsed {
        /// The label that you have attempted to reuse.
        label: L,
        /// The keychain that is already assigned to the label.
        existing_assignment: Descriptor<DescriptorPublicKey>,
    },
}

impl<L: core::fmt::Debug> core::fmt::Display for InsertKeychainError<L> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            InsertKeychainError::DescriptorAlreadyLabelled {
                existing_assignment: existing,
                keychain,
            } => {
                write!(
                    f,
                    "attempt to reintroduce keychain {keychain:?} with a label different to {existing:?}"
                )
            }
            InsertKeychainError::LabelAlreadyUsed {
                existing_assignment: existing,
                label,
            } => {
                write!(
                    f,
                    "attempt to reuse label {label:?} with a keychain different to {existing:?}"
                )
            }
        }
    }
}

#[cfg(feature = "std")]
impl<L: core::fmt::Debug> std::error::Error for InsertKeychainError<L> {}

/// Represents updates to the derivation index of a [`KeychainTxOutIndex`].
/// It maps each label `L` to a keychain and its last revealed index.
///
/// It can be applied to [`KeychainTxOutIndex`] with [`apply_changeset`].
///
/// The `last_revealed` field is monotone in that [`append`] will never decrease it.
/// `keychains_added` is *not* monotone, once it is set any attempt to change it is subject to the
/// same *one-to-one* keychain <-> descriptor mapping invariant as [`KeychainTxOutIndex`] itself.
///
/// [`KeychainTxOutIndex`]: crate::keychain::KeychainTxOutIndex
/// [`apply_changeset`]: crate::keychain::KeychainTxOutIndex::apply_changeset
/// [`append`]: Self::append
#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(crate = "serde_crate")
)]
#[must_use]
pub struct ChangeSet {
    /// Contains for each descriptor_id the last revealed index of derivation
    pub last_revealed: BTreeMap<KeychainId, u32>,
}

impl Append for ChangeSet {
    /// Merge another [`ChangeSet<L>`] into self.
    ///
    /// For the `keychains_added` field this method respects the invariants of
    /// [`insert_keychain`]. `last_revealed` always becomes the larger of the two.
    ///
    /// [`insert_keychain`]: KeychainTxOutIndex::insert_keychain
    fn append(&mut self, other: Self) {
        // for `last_revealed`, entries of `other` will take precedence ONLY if it is greater than
        // what was originally in `self`.
        for (k_id, index) in other.last_revealed {
            use crate::collections::btree_map::Entry;
            match self.last_revealed.entry(k_id) {
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
        self.last_revealed.is_empty()
    }
}

#[cfg(feature = "sqlite")]
pub struct SqlParams<'p> {
    pub schema: crate::sqlite_util::SchemaParams<'p>,
    pub last_revealed_table_name: &'p str,
}

impl<'p> Default for SqlParams<'p> {
    fn default() -> Self {
        Self {
            schema: crate::sqlite_util::SchemaParams::new("bdk_keychain"),
            last_revealed_table_name: "bdk_keychain_last_revealed",
        }
    }
}

#[cfg(feature = "sqlite")]
impl bdk_sqlite::Storable for ChangeSet {
    type Params = SqlParams<'static>;

    fn init(
        db_tx: &bdk_sqlite::rusqlite::Transaction,
        params: &Self::Params,
    ) -> bdk_sqlite::rusqlite::Result<()> {
        let schema_v0: &[&str] = &[
            // last revealed
            &format!(
                "CREATE TABLE {} ( \
                keychain_id TEXT PRIMARY KEY NOT NULL, \
                last_revealed INTEGER NOT NULL \
                ) STRICT;",
                params.last_revealed_table_name,
            ),
        ];
        let schema_by_version = &[schema_v0];

        let current_version = params.schema.version(db_tx)?;

        let schemas_to_init = {
            let exec_from = current_version.map_or(0_usize, |v| v as usize + 1);
            schema_by_version.iter().enumerate().skip(exec_from)
        };
        for (version, schema) in schemas_to_init {
            params.schema.set_version(db_tx, version as u32)?;
            db_tx.execute_batch(&schema.join("\n"))?;
        }

        Ok(())
    }

    fn read(
        db_tx: &bdk_sqlite::rusqlite::Transaction,
        params: &Self::Params,
    ) -> bdk_sqlite::rusqlite::Result<Option<Self>> {
        use crate::sqlite_util::Sql;

        let mut changeset = Self::default();

        let mut statement = db_tx.prepare(&format!(
            "SELECT keychain_id, last_revealed FROM {}",
            params.last_revealed_table_name,
        ))?;
        let row_iter = statement.query_map([], |row| {
            Ok((
                row.get::<_, Sql<KeychainId>>("keychain_id")?,
                row.get::<_, u32>("last_revealed")?,
            ))
        })?;
        for row in row_iter {
            let (Sql(keychain_id), last_revealed) = row?;
            changeset.last_revealed.insert(keychain_id, last_revealed);
        }

        if changeset.is_empty() {
            Ok(None)
        } else {
            Ok(Some(changeset))
        }
    }

    fn write(
        &self,
        db_tx: &bdk_sqlite::rusqlite::Transaction,
        params: &Self::Params,
    ) -> bdk_sqlite::rusqlite::Result<()> {
        use crate::sqlite_util::Sql;

        let mut statement = db_tx.prepare_cached(&format!(
            "REPLACE INTO {}(keychain_id, last_revealed) VALUES(:keychain_id, :last_revealed)",
            params.last_revealed_table_name,
        ))?;
        for (&keychain_id, &last_revealed) in &self.last_revealed {
            statement.execute(named_params! {
                ":keychain_id": Sql(keychain_id),
                ":last_revealed": last_revealed,
            })?;
        }

        Ok(())
    }
}
