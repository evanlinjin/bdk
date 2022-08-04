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

use bitcoin::{BlockHash, Transaction, Txid};
pub use coin_filter::*;
pub use delta::*;
pub use sparse_chain::*;

#[derive(Debug, Clone, PartialEq)]
pub struct BlockTime {
    /// confirmation block height
    pub height: u32,
    /// confirmation block timestamp
    pub time: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChainTx {
    pub tx: Transaction,
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
