use std::{collections::{BTreeMap, HashMap, BTreeSet}, rc::Rc};

use bitcoin::{Script, Txid, Transaction};

use super::{TxPos, SyncedFrom};

pub struct SortedTxs {
    txs: BTreeMap<TxPos, Txid>, // sorted txs
}

pub struct ChainTx {
    pub pos: TxPos,
    pub tx: Transaction,
}

impl ChainTx {
    fn txid(&self) -> Txid {
        self.tx.txid()
    }
}

pub struct ConfirmedTxs {
    by_pos: BTreeMap<TxPos, Rc<ChainTx>>,
    by_txid: BTreeMap<Txid, Rc<ChainTx>>,
}

impl ConfirmedTxs {
    fn add(&mut self, chain_tx: ChainTx) -> bool {
        assert!(self.by_pos.contains_key(&chain_tx.pos) != self.by_txid.contains_key(&chain_tx.txid()));

        if self.by_pos.contains_key(&chain_tx.pos) {
            return false;
        }

        let v = Rc::new(chain_tx);
        self.by_pos.insert(chain_tx.pos, Rc::clone(&v));
        self.by_txid.insert(v.txid(), v);
        return true;
    }
}

pub struct SparseChain {
    // sorted list of tx positions by block height
    pos_by_height: BTreeMap<u32, BTreeSet<u32>>,
    // sorted list of tx positions by spk
    pos_by_spk: HashMap<Script, (BTreeSet<TxPos>, SyncedFrom)>,
    // confirmed transactions
    confirmed_txs: ConfirmedTxs,
}
