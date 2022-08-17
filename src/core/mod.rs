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

//! BDK Core

mod coin_filter;
mod delta;
mod sparse_chain;

use bitcoin::{BlockHash, OutPoint, Transaction, TxOut, Txid};
pub use coin_filter::*;
pub use delta::*;
pub use sparse_chain::*;

/// Contains the time + height of a block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockTime {
    /// confirmation block height
    pub height: u32,
    /// confirmation block timestamp
    pub time: u32,
}

/// This is a transactions with `confirmed_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainTx {
    /// The raw transaction.
    pub tx: Transaction,
    /// Confirmed at (if any).
    pub confirmed_at: Option<BlockTime>,
}

/// Block header data that we are interested in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialHeader {
    /// Block hash
    pub hash: BlockHash,
    /// Block time
    pub time: u32,
}

/// Represents a candidate transaction to be introduced to [SparseChain].
pub struct CandidateTx {
    /// Txid of candidate.
    pub txid: Txid,
    /// Confirmed height and header (if any).
    pub confirmed_at: Option<(u32, PartialHeader)>,
}

/// A `TxOut` with as much data as we can retreive about it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTxOut {
    /// Outpoint
    pub outpoint: OutPoint,
    /// TxOut
    pub txout: TxOut,
    /// Confirmed at (if any)
    pub confirmed_at: Option<BlockTime>,
    /// Spent by (if any)
    pub spent_by: Option<Txid>,
}
