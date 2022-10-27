use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use bdk::{wallet::AddressIndex, Wallet};
use bdk_core::{BlockId, ChangeSet, Update, UpdateFailure};
use bdk_test_client::{RpcApi, RpcError, TestClient};
use bitcoin::{Amount, Network, Txid};

#[derive(Debug)]
enum SyncError {
    Rpc(RpcError),
    Update(UpdateFailure),
}

impl From<RpcError> for SyncError {
    fn from(e: RpcError) -> Self {
        Self::Rpc(e)
    }
}

impl From<UpdateFailure> for SyncError {
    fn from(e: UpdateFailure) -> Self {
        Self::Update(e)
    }
}

struct ClientSession {
    client: Mutex<TestClient>,
    wallet: Mutex<Wallet>,
}

impl ClientSession {
    fn new(desc: &str, change_desc: Option<&str>, initial_blocks: u64) -> Self {
        Self {
            client: Mutex::new({
                let mut client = TestClient::default();
                client.generate(initial_blocks, None);
                client
            }),
            wallet: Mutex::new(
                Wallet::new(desc, change_desc, Network::Regtest)
                    .expect("parsing descriptors failed"),
            ),
        }
    }

    fn inspect<F, R>(&self, inspect: F) -> R
    where
        F: FnOnce(&TestClient, &Wallet) -> R,
    {
        let client = &*self.client.lock().unwrap();
        let wallet = &*self.wallet.lock().unwrap();
        inspect(client, wallet)
    }

    fn broadcast_txs(&self, tx_count: usize, amount: u64) -> Vec<Txid> {
        let addresses = {
            let mut wallet = self.wallet.lock().unwrap();
            (0..tx_count)
                .map(|_| wallet.get_address(AddressIndex::New).address)
                .collect::<Vec<_>>()
        };

        let client = self.client.lock().unwrap();
        addresses
            .iter()
            .map(|address| {
                client
                    .send_to_address(
                        &address,
                        Amount::from_sat(amount),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    .expect("send to address should succeed")
            })
            .collect()
    }

    fn mine_blocks(&self, num_blocks: u64) {
        let mut client = self.client.lock().unwrap();
        client.generate(num_blocks, None);
    }

    fn reorg(&self, num_blocks: u64) {
        let mut client = self.client.lock().unwrap();
        client.reorg(num_blocks)
    }

    fn sync_wallet(
        &self,
        start_height: u64,
        stop_gap: Option<u32>,
    ) -> Result<Option<ChangeSet>, SyncError> {
        let client = self.client.lock().unwrap();
        let mut wallet = self.wallet.lock().unwrap();

        // find last valid result
        let (last_valid_block, invalidate_block) = {
            let mut last_valid = None;
            let mut invalidate = None;

            for block_id in wallet.iter_checkpoints().rev() {
                let block_res = client.get_block_info(&block_id.hash)?;
                if block_res.confirmations < 0 {
                    // NOT in main chain
                    invalidate = Some(block_res);
                    continue;
                } else {
                    // in chain
                    last_valid = Some(block_res);
                    break;
                }
            }

            (last_valid, invalidate)
        };

        let last_valid = last_valid_block.as_ref().map(|r| BlockId {
            height: r.height as _,
            hash: r.hash,
        });
        let invalidate = invalidate_block.as_ref().map(|r| BlockId {
            height: r.height as _,
            hash: r.hash,
        });

        // find new blocks and new tip
        let (new_blocks, new_tip) = {
            let mut blocks = Vec::new();
            let mut tip = match last_valid_block {
                Some(res) => match res.nextblockhash {
                    Some(hash) => BlockId {
                        height: (res.height + 1) as _,
                        hash,
                    },
                    None => return Ok(None), // no new blocks, nothing to update
                },
                None => BlockId {
                    height: start_height as _,
                    hash: client.get_block_hash(start_height)?,
                },
            };
            loop {
                blocks.push((tip, client.get_block(&tip.hash)?));

                let res = client.get_block_info(&tip.hash)?;
                match res.nextblockhash {
                    Some(hash) => {
                        tip = BlockId {
                            height: (res.height + 1) as _,
                            hash,
                        }
                    }
                    None => break,
                };
            }

            (blocks, tip)
        };

        // find txs in mempool
        let unconfirmed_txs = client
            .get_raw_mempool()?
            .iter()
            .map(|txid| {
                client
                    .get_raw_transaction(txid, None)
                    .map(|tx| (tx, Option::<u32>::None))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // find txs in new blocks
        let txids = new_blocks
            .iter()
            .flat_map(|(id, b)| b.txdata.iter().map(move |tx| (tx.clone(), Some(id.height))))
            .chain(unconfirmed_txs)
            // filter out irrelevant, enforce stop_gap of relevant (and also add to graph)
            .filter(|(tx, _)| wallet.graph_insert_if_relevant(tx, stop_gap))
            .map(|(tx, id)| (tx.txid(), id.into()))
            .collect();

        let update = Update {
            txids,
            last_valid,
            invalidate,
            new_tip,
        };

        let change_set = wallet.apply_update(update)?;
        Ok(Some(change_set))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    const DESCRIPTOR: &'static str ="tr([73c5da0a/86'/0'/0']tprv8cSrHfiTQQWzKVejDHvBcvW4pdLEDLMvtVdbUXFfceQ4kbZKMsuFWbd3LUN3omNrQfafQaPwXUFXtcofkE9UjFZ3i9deezBHQTGvYV2xUzz/0/*)";

    let session = Arc::new(ClientSession::new(DESCRIPTOR, None, 100));

    let exp_txs = (1..10)
        .flat_map(|n| {
            if n % 2 == 0 {
                session.mine_blocks(1);
            }
            session.broadcast_txs(n as _, n * 10_000)
        })
        .collect::<Vec<_>>();

    session
        .sync_wallet(0, Some(10))
        .expect("sync should succeed");

    // ensure everything looks okay
    session.inspect(|_client, wallet| {
        let txs = wallet.list_transactions(true);
        assert_eq!(txs.len(), exp_txs.len());
    });

    Ok(())
}
