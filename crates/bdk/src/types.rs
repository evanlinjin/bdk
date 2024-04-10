// Bitcoin Dev Kit
// Written in 2020 by Alekos Filini <alekos.filini@gmail.com>
//
// Copyright (c) 2020-2021 Bitcoin Dev Kit Developers
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE
// or http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option.
// You may not use this file except in accordance with one or both of these
// licenses.

use alloc::boxed::Box;
use core::convert::AsRef;

use bdk_chain::ConfirmationTime;
use bitcoin::blockdata::transaction::{OutPoint, Sequence, TxOut};
use bitcoin::psbt;

use serde::{Deserialize, Serialize};

/// Types of keychains
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum KeychainKind {
    /// External keychain, used for deriving recipient addresses.
    External = 0,
    /// Internal keychain, used for deriving change addresses.
    Internal = 1,
}

impl KeychainKind {
    /// Return [`KeychainKind`] as a byte
    pub fn as_byte(&self) -> u8 {
        match self {
            KeychainKind::External => b'e',
            KeychainKind::Internal => b'i',
        }
    }
}

impl AsRef<[u8]> for KeychainKind {
    fn as_ref(&self) -> &[u8] {
        match self {
            KeychainKind::External => b"e",
            KeychainKind::Internal => b"i",
        }
    }
}

/// Represents the keychain derivation of a script pubkey.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KeychainDerivation {
    /// The script pubkey is derived from the external keychain at index.
    External(u32),
    /// The script pubkey is derived from the internal keychain at index.
    Internal(u32),
    /// The script pubkey is derived from both the internal and external keychains at the given
    /// indices.
    ExternalAndInternal {
        /// The derivation index of the external keychain.
        external: u32,
        /// The derivation index of the internal keychain.
        internal: u32,
    },
}

impl KeychainDerivation {
    /// Attempt to construct [`KeychainDerivation`] from an iterator of [`KeychainKind`] coupled
    /// with derivation index.
    ///
    /// Indices that appear later in the iterator have precedence.
    pub fn from_keychain_kinds(
        keychains: impl IntoIterator<Item = (KeychainKind, u32)>,
    ) -> Option<Self> {
        keychains
            .into_iter()
            .fold(Option::<Self>::None, |acc, (k, i)| match acc {
                None => match k {
                    KeychainKind::External => Some(Self::External(i)),
                    KeychainKind::Internal => Some(Self::Internal(i)),
                },
                Some(Self::External(external)) => match k {
                    KeychainKind::External => Some(Self::External(i)),
                    KeychainKind::Internal => {
                        let internal = i;
                        Some(Self::ExternalAndInternal { external, internal })
                    }
                },
                Some(Self::Internal(internal)) => match k {
                    KeychainKind::External => {
                        let external = i;
                        Some(Self::ExternalAndInternal { external, internal })
                    }
                    KeychainKind::Internal => Some(Self::Internal(i)),
                },
                Some(Self::ExternalAndInternal {
                    mut external,
                    mut internal,
                }) => {
                    match k {
                        KeychainKind::External => external = i,
                        KeychainKind::Internal => internal = i,
                    };
                    Some(Self::ExternalAndInternal { external, internal })
                }
            })
    }

    /// Get the external keychain's derivation index (if any).
    pub fn external_index(&self) -> Option<u32> {
        match self {
            KeychainDerivation::External(external) => Some(*external),
            KeychainDerivation::Internal(_) => None,
            KeychainDerivation::ExternalAndInternal { external, .. } => Some(*external),
        }
    }

    /// Get the internal keychain's derivation index (if any).
    pub fn internal_index(&self) -> Option<u32> {
        match self {
            KeychainDerivation::External(_) => None,
            KeychainDerivation::Internal(internal) => Some(*internal),
            KeychainDerivation::ExternalAndInternal { internal, .. } => Some(*internal),
        }
    }
}

/// An unspent output owned by a [`Wallet`].
///
/// [`Wallet`]: crate::Wallet
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocalOutput {
    /// Reference to a transaction output
    pub outpoint: OutPoint,
    /// Transaction output
    pub txout: TxOut,
    /// Whether this UTXO is spent or not
    pub is_spent: bool,
    /// Keychain derivation of this output's script pubkey
    pub keychain_derivation: KeychainDerivation,
    /// The confirmation time for transaction containing this utxo
    pub confirmation_time: ConfirmationTime,
}

/// A [`Utxo`] with its `satisfaction_weight`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedUtxo {
    /// The weight of the witness data and `scriptSig` expressed in [weight units]. This is used to
    /// properly maintain the feerate when adding this input to a transaction during coin selection.
    ///
    /// [weight units]: https://en.bitcoin.it/wiki/Weight_units
    pub satisfaction_weight: usize,
    /// The UTXO
    pub utxo: Utxo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// An unspent transaction output (UTXO).
pub enum Utxo {
    /// A UTXO owned by the local wallet.
    Local(LocalOutput),
    /// A UTXO owned by another wallet.
    Foreign {
        /// The location of the output.
        outpoint: OutPoint,
        /// The nSequence value to set for this input.
        sequence: Option<Sequence>,
        /// The information about the input we require to add it to a PSBT.
        // Box it to stop the type being too big.
        psbt_input: Box<psbt::Input>,
    },
}

impl Utxo {
    /// Get the location of the UTXO
    pub fn outpoint(&self) -> OutPoint {
        match &self {
            Utxo::Local(local) => local.outpoint,
            Utxo::Foreign { outpoint, .. } => *outpoint,
        }
    }

    /// Get the `TxOut` of the UTXO
    pub fn txout(&self) -> &TxOut {
        match &self {
            Utxo::Local(local) => &local.txout,
            Utxo::Foreign {
                outpoint,
                psbt_input,
                ..
            } => {
                if let Some(prev_tx) = &psbt_input.non_witness_utxo {
                    return &prev_tx.output[outpoint.vout as usize];
                }

                if let Some(txout) = &psbt_input.witness_utxo {
                    return txout;
                }

                unreachable!("Foreign UTXOs will always have one of these set")
            }
        }
    }

    /// Get the sequence number if an explicit sequence number has to be set for this input.
    pub fn sequence(&self) -> Option<Sequence> {
        match self {
            Utxo::Local(_) => None,
            Utxo::Foreign { sequence, .. } => *sequence,
        }
    }
}
