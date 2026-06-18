//! [`TxOutIndex`] is a unified script-pubkey indexer that tracks descriptor-based keychains
//! (wildcard descriptors) and fixed scripts (non-wildcard descriptors and raw script pubkeys)
//! behind a single API.
//!
//! It is intended as a replacement for the [`KeychainTxOutIndex`] / [`SpkTxOutIndex`] split.
//! Both kinds of tracking are dispatched internally on the supplied keychain identifier `K`, so
//! callers carry one keychain enum, one generic parameter, and one [`ChangeSet`].

use crate::{
    indexer::keychain_txout::{self, KeychainTxOutIndex},
    miniscript::{Descriptor, DescriptorPublicKey},
    spk_iter::BIP32_MAX_INDEX,
    spk_txout::{CreatedTxOut, SpentTxOut, SpkTxOutIndex},
    Indexed, Indexer, KeychainIndexed, SpkIterator,
};
use alloc::vec::Vec;
use bitcoin::{Amount, OutPoint, Script, ScriptBuf, SignedAmount, Transaction, TxOut, Txid};
use core::{
    fmt::Debug,
    ops::{Bound, RangeBounds},
};

use crate::collections::BTreeMap;

pub use keychain_txout::{ChangeSet, DEFAULT_LOOKAHEAD};

/// A unified script-pubkey indexer.
///
/// `TxOutIndex<K>` tracks script pubkeys of interest under a single keychain identifier `K`.
/// A given keychain may be backed by:
///
/// * a **wildcard descriptor** (HD chain) — registered via [`insert_descriptor`]. The
///   [`KeychainTxOutIndex`] under the hood owns the derivation state, lookahead window, and spk
///   cache.
/// * a **non-wildcard descriptor** — also registered via [`insert_descriptor`]. The single spk is
///   derived at insert time and stored as a fixed entry. The descriptor object is not retained.
/// * a **raw script pubkey** — registered via [`insert_spk`]. Stored alongside non-wildcard
///   descriptors in the fixed sub-index, since the two are operationally identical: one script
///   per keychain at index `0`, no derivation.
///
/// The caller does not need to branch on which kind of keychain a `K` corresponds to: calls like
/// [`reveal_next_spk`], [`index_of_spk`], etc., dispatch internally.
///
/// # Overlap policy
///
/// A given script pubkey may belong to exactly one keychain. Inserts that would assign a script to
/// two different keychains return [`InsertError::SpkAlreadyAssigned`]. Inserts that would
/// reassign a keychain to a different descriptor or a different fixed spk return
/// [`InsertError::KeychainAlreadyAssigned`] or [`InsertError::DescriptorAlreadyAssigned`].
///
/// The wildcard-side check happens against the current lookahead window at insert time. A
/// subsequent reveal that would advance a wildcard descriptor into a spk already held on the fixed
/// side will not produce a fresh entry — the fixed keychain retains ownership and the wildcard
/// silently skips that index. (This is rare in practice: fixed spks are typically not derivable
/// from any wildcard descriptor.)
///
/// [`insert_descriptor`]: Self::insert_descriptor
/// [`insert_spk`]: Self::insert_spk
/// [`reveal_next_spk`]: Self::reveal_next_spk
/// [`index_of_spk`]: Self::index_of_spk
#[derive(Clone, Debug)]
pub struct TxOutIndex<K> {
    wildcard: KeychainTxOutIndex<K>,
    fixed: SpkTxOutIndex<(K, u32)>,
}

impl<K> Default for TxOutIndex<K> {
    fn default() -> Self {
        Self {
            wildcard: KeychainTxOutIndex::default(),
            fixed: SpkTxOutIndex::default(),
        }
    }
}

impl<K> TxOutIndex<K> {
    /// Construct a [`TxOutIndex`] with the given `lookahead` and `persist_spks`.
    ///
    /// Both settings only affect wildcard descriptors; fixed scripts ignore them.
    pub fn new(lookahead: u32, persist_spks: bool) -> Self {
        Self {
            wildcard: KeychainTxOutIndex::new(lookahead, persist_spks),
            fixed: SpkTxOutIndex::default(),
        }
    }

    /// Access the inner [`KeychainTxOutIndex`] holding wildcard-descriptor state.
    pub fn wildcard_inner(&self) -> &KeychainTxOutIndex<K> {
        &self.wildcard
    }

    /// Access the inner [`SpkTxOutIndex`] holding fixed-script entries.
    pub fn fixed_inner(&self) -> &SpkTxOutIndex<(K, u32)> {
        &self.fixed
    }
}

impl<K: Clone + Ord + Debug> Indexer for TxOutIndex<K> {
    type ChangeSet = ChangeSet;

    fn index_txout(&mut self, op: OutPoint, txout: &TxOut) -> Self::ChangeSet {
        let cs = self.wildcard.index_txout(op, txout);
        // Overlap policy guarantees no spk is in both, so feeding the txout to both is safe and
        // idempotent.
        self.fixed.index_txout(op, txout);
        cs
    }

    fn index_tx(&mut self, tx: &Transaction) -> Self::ChangeSet {
        let cs = self.wildcard.index_tx(tx);
        self.fixed.index_tx(tx);
        cs
    }

    fn initial_changeset(&self) -> Self::ChangeSet {
        self.wildcard.initial_changeset()
    }

    fn apply_changeset(&mut self, cs: Self::ChangeSet) {
        self.wildcard.apply_changeset(cs)
    }

    fn is_tx_relevant(&self, tx: &Transaction) -> bool {
        self.wildcard.is_tx_relevant(tx) || self.fixed.is_relevant(tx)
    }
}

impl<K: Clone + Ord + Debug> TxOutIndex<K> {
    /// Get the lookahead setting (applies to wildcard descriptors only).
    pub fn lookahead(&self) -> u32 {
        self.wildcard.lookahead()
    }

    /// Construct a `TxOutIndex<K>` from the given `changeset`.
    ///
    /// Note that descriptors and raw spks are not part of the changeset — the caller must
    /// re-register them via [`insert_descriptor`](Self::insert_descriptor) /
    /// [`insert_spk`](Self::insert_spk) after construction.
    pub fn from_changeset(lookahead: u32, persist_spks: bool, changeset: ChangeSet) -> Self {
        Self {
            wildcard: KeychainTxOutIndex::from_changeset(lookahead, persist_spks, changeset),
            fixed: SpkTxOutIndex::default(),
        }
    }

    /// Insert a descriptor — wildcard or non-wildcard — under `keychain`.
    ///
    /// Wildcard descriptors are stored on the [`KeychainTxOutIndex`] side. Non-wildcard
    /// descriptors are derived at index `0` and stored as fixed scripts (the descriptor object is
    /// not retained).
    ///
    /// Re-inserting the same `(K, descriptor)` is a no-op and returns `Ok(false)`. Errors are
    /// returned for keychain/descriptor/spk reassignment conflicts.
    pub fn insert_descriptor(
        &mut self,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
    ) -> Result<bool, InsertError<K>> {
        // Reject if K is already on the fixed side.
        if self.fixed.spk_at_index(&(keychain.clone(), 0)).is_some() {
            return Err(InsertError::KeychainAlreadyAssigned { keychain });
        }

        if descriptor.has_wildcard() {
            // Check that no spk in the lookahead window overlaps with a fixed entry.
            let probe_end = self.wildcard.lookahead().min(BIP32_MAX_INDEX);
            for (i, spk) in SpkIterator::new_with_range(&descriptor, 0..=probe_end) {
                if let Some(existing) = self.fixed.index_of_spk(&spk) {
                    return Err(InsertError::SpkAlreadyAssigned {
                        spk,
                        attempted_keychain: keychain,
                        attempted_index: i,
                        existing_keychain: existing.0.clone(),
                        existing_index: existing.1,
                    });
                }
            }
            self.wildcard
                .insert_descriptor(keychain, descriptor)
                .map_err(InsertError::from)
        } else {
            let spk = descriptor
                .at_derivation_index(0)
                .expect("non-wildcard descriptor must derive at index 0")
                .script_pubkey();
            self.insert_fixed_spk(keychain, spk)
        }
    }

    /// Insert a raw script pubkey under `keychain` (no descriptor).
    ///
    /// Re-inserting the same `(K, spk)` is a no-op and returns `Ok(false)`.
    pub fn insert_spk(&mut self, keychain: K, spk: ScriptBuf) -> Result<bool, InsertError<K>> {
        // Reject if K is already a wildcard-descriptor keychain.
        if self.wildcard.get_descriptor(keychain.clone()).is_some() {
            return Err(InsertError::KeychainAlreadyAssigned { keychain });
        }
        self.insert_fixed_spk(keychain, spk)
    }

    /// Common path for "insert a fixed script under K" — used by both `insert_descriptor`
    /// (non-wildcard branch) and `insert_spk`. Assumes K isn't already on the wildcard side.
    fn insert_fixed_spk(&mut self, keychain: K, spk: ScriptBuf) -> Result<bool, InsertError<K>> {
        // Check overlap with wildcard side.
        if let Some(existing) = self.wildcard.index_of_spk(&spk) {
            return Err(InsertError::SpkAlreadyAssigned {
                spk,
                attempted_keychain: keychain,
                attempted_index: 0,
                existing_keychain: existing.0.clone(),
                existing_index: existing.1,
            });
        }
        // Check overlap with fixed side.
        if let Some(existing) = self.fixed.index_of_spk(&spk) {
            if existing.0 == keychain && existing.1 == 0 {
                // Identical re-insertion.
                return Ok(false);
            }
            return Err(InsertError::SpkAlreadyAssigned {
                spk,
                attempted_keychain: keychain,
                attempted_index: 0,
                existing_keychain: existing.0.clone(),
                existing_index: existing.1,
            });
        }
        // K already used on fixed side under a different spk.
        if self.fixed.spk_at_index(&(keychain.clone(), 0)).is_some() {
            return Err(InsertError::KeychainAlreadyAssigned { keychain });
        }
        let inserted = self.fixed.insert_spk((keychain, 0), spk);
        debug_assert!(inserted);
        Ok(true)
    }

    // ---- Reveals -----------------------------------------------------------------------------

    /// Reveals the next script pubkey for `keychain`.
    ///
    /// For fixed keychains, returns the single registered spk at index `0` with an empty
    /// changeset. For wildcard keychains, behaves like
    /// [`KeychainTxOutIndex::reveal_next_spk`]. Returns `None` for unknown keychains.
    pub fn reveal_next_spk(&mut self, keychain: K) -> Option<(Indexed<ScriptBuf>, ChangeSet)> {
        if self.wildcard.get_descriptor(keychain.clone()).is_some() {
            return self.wildcard.reveal_next_spk(keychain);
        }
        let spk = self.fixed.spk_at_index(&(keychain, 0))?;
        Some(((0, spk), ChangeSet::default()))
    }

    /// Reveals script pubkeys up to and including `target_index`.
    ///
    /// For fixed keychains, returns the single registered spk regardless of `target_index`.
    #[must_use]
    pub fn reveal_to_target(
        &mut self,
        keychain: K,
        target_index: u32,
    ) -> Option<(Vec<Indexed<ScriptBuf>>, ChangeSet)> {
        if self.wildcard.get_descriptor(keychain.clone()).is_some() {
            return self.wildcard.reveal_to_target(keychain, target_index);
        }
        let spk = self.fixed.spk_at_index(&(keychain, 0))?;
        Some((vec![(0, spk)], ChangeSet::default()))
    }

    /// Convenience method to call [`Self::reveal_to_target`] on multiple keychains.
    pub fn reveal_to_target_multi(&mut self, keychains: &BTreeMap<K, u32>) -> ChangeSet {
        let mut cs = ChangeSet::default();
        for (k, &target) in keychains {
            if let Some((_, sub)) = self.reveal_to_target(k.clone(), target) {
                use crate::Merge;
                cs.merge(sub);
            }
        }
        cs
    }

    /// Gets the next unused script pubkey in the keychain.
    pub fn next_unused_spk(&mut self, keychain: K) -> Option<(Indexed<ScriptBuf>, ChangeSet)> {
        if self.wildcard.get_descriptor(keychain.clone()).is_some() {
            return self.wildcard.next_unused_spk(keychain);
        }
        let spk = self.fixed.spk_at_index(&(keychain, 0))?;
        Some(((0, spk), ChangeSet::default()))
    }

    /// Store lookahead scripts until `target_index`.
    ///
    /// No-op for fixed keychains.
    pub fn lookahead_to_target(&mut self, keychain: K, target_index: u32) -> ChangeSet {
        if self.wildcard.get_descriptor(keychain.clone()).is_some() {
            return self.wildcard.lookahead_to_target(keychain, target_index);
        }
        ChangeSet::default()
    }

    // ---- Queries -----------------------------------------------------------------------------

    /// Get the next derivation index for `keychain`.
    ///
    /// `(idx, true)` means the index has not been revealed yet; `(idx, false)` means the keychain
    /// has no fresh index to reveal (non-wildcard, raw spk, or wildcard at `BIP32_MAX_INDEX`).
    pub fn next_index(&self, keychain: K) -> Option<(u32, bool)> {
        if let Some(r) = self.wildcard.next_index(keychain.clone()) {
            return Some(r);
        }
        if self.fixed.spk_at_index(&(keychain, 0)).is_some() {
            Some((0, false))
        } else {
            None
        }
    }

    /// Get the last derivation index revealed for `keychain`. Returns `Some(0)` for fixed
    /// keychains.
    pub fn last_revealed_index(&self, keychain: K) -> Option<u32> {
        self.wildcard
            .last_revealed_index(keychain.clone())
            .or_else(|| self.fixed.spk_at_index(&(keychain, 0)).map(|_| 0))
    }

    /// Get the last derivation index revealed for each known keychain.
    pub fn last_revealed_indices(&self) -> BTreeMap<K, u32> {
        let mut out = self.wildcard.last_revealed_indices();
        for (k, _) in self.fixed.all_spks().keys() {
            out.entry(k.clone()).or_insert(0);
        }
        out
    }

    /// Returns the highest derivation index of `keychain` where the index has seen a [`TxOut`].
    pub fn last_used_index(&self, keychain: K) -> Option<u32> {
        // Only one side actually owns `keychain`, by the overlap policy.
        self.keychain_outpoints(keychain).last().map(|(i, _)| i)
    }

    /// Returns the highest used derivation index of each keychain that has seen a [`TxOut`].
    pub fn last_used_indices(&self) -> BTreeMap<K, u32> {
        let mut out = self.wildcard.last_used_indices();
        for ((k, i), _) in self.fixed.outpoints() {
            out.entry(k.clone())
                .and_modify(|e| *e = (*e).max(*i))
                .or_insert(*i);
        }
        out
    }

    /// Return the script that exists under `(keychain, index)`.
    pub fn spk_at_index(&self, keychain: K, index: u32) -> Option<ScriptBuf> {
        if let Some(spk) = self.wildcard.spk_at_index(keychain.clone(), index) {
            return Some(spk);
        }
        if index == 0 {
            self.fixed.spk_at_index(&(keychain, 0))
        } else {
            None
        }
    }

    /// Returns the keychain and keychain index associated with the spk.
    pub fn index_of_spk<S: AsRef<Script>>(&self, spk: S) -> Option<&(K, u32)> {
        self.wildcard
            .index_of_spk(spk.as_ref())
            .or_else(|| self.fixed.index_of_spk(spk))
    }

    /// Returns whether the spk under `(keychain, index)` has been used.
    pub fn is_used(&self, keychain: K, index: u32) -> bool {
        self.wildcard.is_used(keychain.clone(), index) || self.fixed.is_used(&(keychain, index))
    }

    /// Marks `(keychain, index)` as used. Returns whether the mark was applied.
    pub fn mark_used(&mut self, keychain: K, index: u32) -> bool {
        // The overlap policy guarantees at most one side owns the entry.
        self.wildcard.mark_used(keychain.clone(), index) || self.fixed.mark_used(&(keychain, index))
    }

    /// Undoes [`mark_used`]. Returns whether `(keychain, index)` is now unused.
    ///
    /// [`mark_used`]: Self::mark_used
    pub fn unmark_used(&mut self, keychain: K, index: u32) -> bool {
        self.wildcard.unmark_used(keychain.clone(), index)
            || self.fixed.unmark_used(&(keychain, index))
    }

    /// Get the descriptor associated with a wildcard `keychain`. Returns `None` for fixed
    /// keychains or unknown keychains.
    pub fn get_descriptor(&self, keychain: K) -> Option<&Descriptor<DescriptorPublicKey>> {
        self.wildcard.get_descriptor(keychain)
    }

    /// Returns `true` if `keychain` is registered as a wildcard descriptor.
    pub fn is_wildcard_keychain(&self, keychain: K) -> bool {
        self.wildcard.get_descriptor(keychain).is_some()
    }

    /// Returns `true` if `keychain` is registered as a fixed script (non-wildcard descriptor or
    /// raw spk).
    pub fn is_fixed_keychain(&self, keychain: K) -> bool {
        self.fixed.spk_at_index(&(keychain, 0)).is_some()
    }

    /// Iterate over keychains backed by a wildcard descriptor.
    pub fn keychains(
        &self,
    ) -> impl DoubleEndedIterator<Item = (K, &Descriptor<DescriptorPublicKey>)> + ExactSizeIterator + '_
    {
        self.wildcard.keychains()
    }

    /// Iterate over fixed keychains and their registered spks.
    pub fn fixed_keychains(&self) -> impl DoubleEndedIterator<Item = (K, &ScriptBuf)> + '_ {
        self.fixed
            .all_spks()
            .iter()
            .map(|((k, _), spk)| (k.clone(), spk))
    }

    /// Get an unbounded spk iterator over a wildcard `keychain`. Returns `None` for fixed
    /// keychains.
    pub fn unbounded_spk_iter(
        &self,
        keychain: K,
    ) -> Option<SpkIterator<Descriptor<DescriptorPublicKey>>> {
        self.wildcard.unbounded_spk_iter(keychain)
    }

    /// Get unbounded spk iterators for all wildcard descriptors.
    pub fn all_unbounded_spk_iters(
        &self,
    ) -> BTreeMap<K, SpkIterator<Descriptor<DescriptorPublicKey>>> {
        self.wildcard.all_unbounded_spk_iters()
    }

    // ---- Iterators over outpoints/txouts -----------------------------------------------------

    /// Iterate over the indexed outpoints (chain of both sub-indexers).
    pub fn outpoints(
        &self,
    ) -> impl DoubleEndedIterator<Item = &KeychainIndexed<K, OutPoint>> + Clone + '_ {
        self.wildcard
            .outpoints()
            .iter()
            .chain(self.fixed.outpoints().iter())
    }

    /// Iterate over known txouts that spend to tracked script pubkeys.
    pub fn txouts(
        &self,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<K, (OutPoint, &TxOut)>> + '_ {
        let w = self.wildcard.txouts();
        let f = self
            .fixed
            .txouts()
            .map(|(i, op, txo)| (i.clone(), (op, txo)));
        w.chain(f)
    }

    /// Finds all txouts on a transaction that have been scanned and indexed.
    pub fn txouts_in_tx(
        &self,
        txid: Txid,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<K, (OutPoint, &TxOut)>> + '_ {
        let w = self.wildcard.txouts_in_tx(txid);
        let f = self
            .fixed
            .txouts_in_tx(txid)
            .map(|(i, op, txo)| (i.clone(), (op, txo)));
        w.chain(f)
    }

    /// Return the [`TxOut`] of `outpoint` if it has been indexed.
    pub fn txout(&self, outpoint: OutPoint) -> Option<KeychainIndexed<K, &TxOut>> {
        self.wildcard
            .txout(outpoint)
            .or_else(|| self.fixed.txout(outpoint).map(|(i, txo)| (i.clone(), txo)))
    }

    /// Iterate over [`OutPoint`]s with scripts belonging to `keychain`.
    pub fn keychain_outpoints(
        &self,
        keychain: K,
    ) -> impl DoubleEndedIterator<Item = Indexed<OutPoint>> + '_ {
        let w = self.wildcard.keychain_outpoints(keychain.clone());
        let f_range = (keychain.clone(), u32::MIN)..=(keychain.clone(), u32::MAX);
        let f = self
            .fixed
            .outputs_in_range(f_range)
            .map(|((_, i), op)| (*i, op));
        w.chain(f)
    }

    /// Iterate over [`OutPoint`]s with scripts belonging to keychains in `range`.
    pub fn keychain_outpoints_in_range<'a>(
        &'a self,
        range: impl RangeBounds<K> + 'a + Clone,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<K, OutPoint>> + 'a {
        let w = self.wildcard.keychain_outpoints_in_range(range.clone());
        let f = self
            .fixed
            .outputs_in_range(map_to_inner_bounds(&range))
            .map(|((k, i), op)| ((k.clone(), *i), op));
        w.chain(f)
    }

    /// Iterate over revealed spks of all keychains in `range`.
    pub fn revealed_spks(
        &self,
        range: impl RangeBounds<K> + Clone,
    ) -> impl Iterator<Item = KeychainIndexed<K, ScriptBuf>> + '_ {
        let w = self.wildcard.revealed_spks(range.clone());
        // All fixed spks are considered revealed.
        let f = self
            .fixed
            .all_spks()
            .range(map_to_inner_bounds(&range))
            .map(|((k, i), spk)| ((k.clone(), *i), spk.clone()));
        w.chain(f)
    }

    /// Iterate over revealed spks of the given `keychain`.
    pub fn revealed_keychain_spks(
        &self,
        keychain: K,
    ) -> impl DoubleEndedIterator<Item = Indexed<ScriptBuf>> + '_ {
        let w = self.wildcard.revealed_keychain_spks(keychain.clone());
        let f_range = (keychain.clone(), u32::MIN)..=(keychain.clone(), u32::MAX);
        let f = self
            .fixed
            .all_spks()
            .range(f_range)
            .map(|((_, i), spk)| (*i, spk.clone()));
        w.chain(f)
    }

    /// Iterate over revealed but unused spks of all keychains.
    pub fn unused_spks(
        &self,
    ) -> impl DoubleEndedIterator<Item = KeychainIndexed<K, ScriptBuf>> + Clone + '_ {
        let w = self
            .wildcard
            .unused_spks()
            .map(|((k, i), spk)| ((k, i), spk.clone()));
        let f = self
            .fixed
            .unused_spks(..)
            .map(|((k, i), spk)| ((k.clone(), *i), spk));
        w.chain(f)
    }

    /// Iterate over revealed but unused spks of the given `keychain`.
    pub fn unused_keychain_spks(
        &self,
        keychain: K,
    ) -> impl DoubleEndedIterator<Item = Indexed<ScriptBuf>> + Clone + '_ {
        let w = self
            .wildcard
            .unused_keychain_spks(keychain.clone())
            .map(|(i, spk)| (i, spk.clone()));
        let f_range = (keychain.clone(), u32::MIN)..=(keychain.clone(), u32::MAX);
        let f = self
            .fixed
            .unused_spks(f_range)
            .map(|((_, i), spk)| (*i, spk));
        w.chain(f)
    }

    /// Compute the value transfer effect `tx` has on the script pubkeys of keychains in `range`.
    pub fn sent_and_received(
        &self,
        tx: &Transaction,
        range: impl RangeBounds<K> + Clone,
    ) -> (Amount, Amount) {
        let (sw, rw) = self.wildcard.sent_and_received(tx, range.clone());
        let (sf, rf) = self
            .fixed
            .sent_and_received(tx, map_to_inner_bounds(&range));
        (sw + sf, rw + rf)
    }

    /// Compute the net value `tx` gives to scripts of keychains in `range`.
    pub fn net_value(&self, tx: &Transaction, range: impl RangeBounds<K> + Clone) -> SignedAmount {
        self.wildcard.net_value(tx, range.clone())
            + self.fixed.net_value(tx, map_to_inner_bounds(&range))
    }

    /// Returns the [`SpentTxOut`]s for `tx` across both sub-indexers.
    pub fn spent_txouts<'a>(
        &'a self,
        tx: &'a Transaction,
    ) -> impl Iterator<Item = SpentTxOut<(K, u32)>> + 'a {
        self.wildcard
            .spent_txouts(tx)
            .chain(self.fixed.spent_txouts(tx))
    }

    /// Returns the [`CreatedTxOut`]s for `tx` across both sub-indexers.
    pub fn created_txouts<'a>(
        &'a self,
        tx: &'a Transaction,
    ) -> impl Iterator<Item = CreatedTxOut<(K, u32)>> + 'a {
        self.wildcard
            .created_txouts(tx)
            .chain(self.fixed.created_txouts(tx))
    }
}

fn map_to_inner_bounds<K: Clone>(bound: &impl RangeBounds<K>) -> impl RangeBounds<(K, u32)> {
    let start = match bound.start_bound() {
        Bound::Included(k) => Bound::Included((k.clone(), u32::MIN)),
        Bound::Excluded(k) => Bound::Excluded((k.clone(), u32::MAX)),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match bound.end_bound() {
        Bound::Included(k) => Bound::Included((k.clone(), u32::MAX)),
        Bound::Excluded(k) => Bound::Excluded((k.clone(), u32::MIN)),
        Bound::Unbounded => Bound::Unbounded,
    };
    (start, end)
}

/// Error returned from [`TxOutIndex::insert_descriptor`] and [`TxOutIndex::insert_spk`].
#[derive(Clone, Debug, PartialEq)]
pub enum InsertError<K> {
    /// The descriptor has already been assigned to another keychain.
    DescriptorAlreadyAssigned {
        /// The descriptor that was attempted.
        descriptor: alloc::boxed::Box<Descriptor<DescriptorPublicKey>>,
        /// The keychain that already owns this descriptor.
        existing_assignment: K,
    },
    /// The keychain is already in use under a different descriptor or fixed script.
    KeychainAlreadyAssigned {
        /// The keychain that was attempted.
        keychain: K,
    },
    /// The script pubkey is already tracked by a different keychain.
    SpkAlreadyAssigned {
        /// The conflicting script pubkey.
        spk: ScriptBuf,
        /// The keychain we tried to register the spk under.
        attempted_keychain: K,
        /// The derivation index we tried to register the spk at.
        attempted_index: u32,
        /// The keychain that already owns this spk.
        existing_keychain: K,
        /// The derivation index the spk is owned at.
        existing_index: u32,
    },
}

impl<K> From<keychain_txout::InsertDescriptorError<K>> for InsertError<K> {
    fn from(err: keychain_txout::InsertDescriptorError<K>) -> Self {
        match err {
            keychain_txout::InsertDescriptorError::DescriptorAlreadyAssigned {
                existing_assignment,
                descriptor,
            } => InsertError::DescriptorAlreadyAssigned {
                descriptor,
                existing_assignment,
            },
            keychain_txout::InsertDescriptorError::KeychainAlreadyAssigned { keychain, .. } => {
                InsertError::KeychainAlreadyAssigned { keychain }
            }
        }
    }
}

impl<K: core::fmt::Display> core::fmt::Display for InsertError<K> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            InsertError::DescriptorAlreadyAssigned {
                existing_assignment,
                descriptor,
            } => write!(
                f,
                "descriptor '{descriptor}' is already assigned to keychain '{existing_assignment}'"
            ),
            InsertError::KeychainAlreadyAssigned { keychain } => write!(
                f,
                "keychain '{keychain}' is already assigned to a different descriptor or script"
            ),
            InsertError::SpkAlreadyAssigned {
                spk,
                attempted_keychain,
                attempted_index,
                existing_keychain,
                existing_index,
            } => write!(
                f,
                "script pubkey '{spk}' attempted at ('{attempted_keychain}', {attempted_index}) \
                 is already assigned to ('{existing_keychain}', {existing_index})"
            ),
        }
    }
}

impl<K: core::fmt::Display + core::fmt::Debug> core::error::Error for InsertError<K> {}
