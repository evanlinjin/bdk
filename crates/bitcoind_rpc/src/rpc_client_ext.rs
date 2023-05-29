use bdk_chain::local_chain::LocalChain;
use bitcoincore_rpc::{Client, RpcApi};

pub trait RpcClientExt {}

fn start_sync(
    client: &Client,
    local_chain: &LocalChain,
    fallback_height: u32,
) -> Result<(), bitcoincore_rpc::Error> {
    let tip = client.get_block_count()? as u32;
    let local_tip = local_chain.tip().map_or(fallback_height, |b| b.height);

    // assume bitcoind is always more up to date (for now)
    if local_tip > tip {
        todo!("handle when local state is more up to date than bitcoind");
    }

    // This contains the block result of the highest block that exists in both the local chain and
    // the rpc chain.
    let mut last_agreement = None;

    // Because of how `LocalChain` operates, if we have a `last_agreement` that is lower than the
    // `local_chain`'s tip, we need to include the block of height
    let mut must_include = None;

    for (height, hash) in local_chain.blocks().iter().rev() {
        match client.get_block_info(hash) {
            Ok(res) => {
                if res.confirmations < 0 {
                    must_include = Some(res.height as u32); // NOT in main chain
                } else {
                    last_agreement = Some(res);
                    break;
                }
            }
            Err(err) => {
                use bitcoincore_rpc::jsonrpc;
                match err {
                    bitcoincore_rpc::Error::JsonRpc(jsonrpc::Error::Rpc(rpc_err))
                        if rpc_err.code == -5 =>
                    {
                        must_include = Some(*height); // NOT in main chain
                    }
                    err => return Err(err.into()),
                }
            }
        };
    }

    Ok(())
}
