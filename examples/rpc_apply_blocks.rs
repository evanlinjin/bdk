use std::error::Error;

use bdk::{wallet::AddressIndex, Wallet};
use bdk_test_client::{RpcApi, TestClient};
use bitcoin::{Amount, Network};

fn main() -> Result<(), Box<dyn Error>> {
    const DESCRIPTOR: &'static str ="tr([73c5da0a/86'/0'/0']tprv8cSrHfiTQQWzKVejDHvBcvW4pdLEDLMvtVdbUXFfceQ4kbZKMsuFWbd3LUN3omNrQfafQaPwXUFXtcofkE9UjFZ3i9deezBHQTGvYV2xUzz/0/*)";

    let mut test_client = TestClient::default();

    let mut wallet =
        Wallet::new(DESCRIPTOR, None, Network::Regtest).expect("parsing descriptors failed");

    test_client.generate(100, None);

    let addr_info = wallet.get_address(AddressIndex::New);
    let exp_txid = test_client
        .send_to_address(
            &addr_info.address,
            Amount::from_sat(10_000),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("tx should send");
    println!("expected txid: {}", exp_txid);

    test_client.generate(1, None);

    // apply each block
    let bc_count = test_client.get_block_count().unwrap() + 1;
    for height in 0..bc_count {
        let hash = test_client.get_block_hash(height).unwrap();
        let block = test_client.get_block(&hash).unwrap();

        dbg!(block.txdata.iter().find(|tx| tx.txid() == exp_txid));

        wallet
            .apply_block(&block, height as _)
            .expect("apply block should not fail");
    }

    // ensure everything looks okay
    let txs = wallet.list_transactions(true);
    assert_eq!(txs.len(), 1);
    assert_eq!(txs[0].txid, exp_txid);

    Ok(())
}
