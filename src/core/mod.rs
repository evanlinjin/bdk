
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

mod sparse_chain;
use std::collections::BTreeSet;

use bitcoin::{BlockHash};
pub use sparse_chain::*;

/// Represents the position of a transaction in the blockchain
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TxPos {
    /// height of block this transaction is included in
    pub height: u32,
    /// position of the transaction in the block
    pub pos: u32,
}

/// Represents the blocks that a structure is synced from
pub struct SyncedFrom {
    // set of blocks (height, hash)
    blocks: BTreeSet<(u32, BlockHash)>,
}
