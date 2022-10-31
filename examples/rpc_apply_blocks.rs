use std::{
    collections::BTreeMap,
    error::Error,
    sync::{Arc, Mutex},
};

use bdk::{wallet::AddressIndex, Wallet};
use bdk_core::{ChangeSet, UpdateFailure};
use bdk_test_client::{RpcApi, RpcClient, RpcError, TestClient};
use bitcoin::{Amount, Block, BlockHash, Network, Transaction, Txid};

use std::sync::mpsc;

#[derive(Debug)]
pub enum SyncError {
    Rpc(RpcError),
    Update(UpdateFailure),
    Send(mpsc::SendError<RpcUpdate>),
    Reorg(usize),
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

impl From<mpsc::SendError<RpcUpdate>> for SyncError {
    fn from(err: mpsc::SendError<RpcUpdate>) -> Self {
        Self::Send(err)
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

    fn start_sync(&self) -> mpsc::Receiver<RpcUpdate> {
        let (send, recv) = mpsc::channel();

        let checkpoints = self.wallet.lock().unwrap().iter_checkpoints().clone();

        let client_params = self.client.lock().unwrap().bitcoind.params.clone();
        let client = RpcClient::new(
            &client_params.rpc_socket.to_string(),
            bdk_test_client::Auth::CookieFile(client_params.cookie_file),
        )
        .expect("failed to connect");

        let send_handle = std::thread::spawn(move || {
            let res = rpc_sync(&client, &send, checkpoints, 0);
            drop(send);
            res
        });

        recv
    }
}

pub enum RpcUpdate {
    Blocks(BTreeMap<usize, Block>),
    Mempool(Vec<Transaction>),
}

pub fn rpc_sync(
    client: &RpcClient,
    chan: &mpsc::Sender<RpcUpdate>,
    checkpoints: BTreeMap<u32, BlockHash>,
    fallback_height: u64,
) -> Result<(), SyncError> {
    let mut last_valid = None;
    let mut must_include = None;

    for (_, cp_hash) in checkpoints.iter().rev() {
        let res = client.get_block_info(cp_hash)?;
        if res.confirmations < 0 {
            must_include = Some(res.height); // NOT in main chain
        } else {
            last_valid = Some(res);
            break;
        }
    }

    // determine first new block
    let mut block_info = match last_valid {
        Some(res) => match res.nextblockhash {
            Some(block_hash) => client.get_block_info(&block_hash)?,
            None => return rpc_mempool_sync(client, &chan),
        },
        None => {
            let block_hash = client.get_block_hash(fallback_height)?;
            client.get_block_info(&block_hash)?
        }
    };

    let mut blocks_cache = BTreeMap::<usize, Block>::new();

    loop {
        if block_info.confirmations < 0 {
            // NOT in main chain, reorg happened during sync
            return Err(SyncError::Reorg(block_info.height));
        }

        // add block to cache
        let replaced_block =
            blocks_cache.insert(block_info.height, client.get_block(&block_info.hash)?);
        debug_assert!(replaced_block.is_none());

        // try push into channel
        if !blocks_cache.is_empty() && blocks_cache.keys().last().cloned() >= must_include {
            chan.send(RpcUpdate::Blocks(blocks_cache.split_off(&0)))?;
        }

        // update block_info
        match block_info.nextblockhash {
            Some(new_hash) => block_info = client.get_block_info(&new_hash)?,
            None => return rpc_mempool_sync(client, &chan),
        }
    }
}

pub fn rpc_mempool_sync(
    client: &RpcClient,
    chan: &mpsc::Sender<RpcUpdate>,
) -> Result<(), SyncError> {
    let chunk_size = 100; // TODO: Do not hardcode

    for chunk in client.get_raw_mempool()?.chunks(chunk_size) {
        let txs = chunk
            .iter()
            .map(|txid| client.get_raw_transaction(txid, None))
            .collect::<Result<Vec<_>, _>>()?;
        chan.send(RpcUpdate::Mempool(txs))?;
    }

    Ok(())
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

    // session
    // .sync_wallet(0, Some(10))
    // .expect("sync should succeed");

    // ensure everything looks okay
    session.inspect(|_client, wallet| {
        let txs = wallet.list_transactions(true);
        assert_eq!(txs.len(), exp_txs.len());
    });

    Ok(())
}
