# BDK Coin Selection

`bdk_coin_select` is a tool to help you select inputs for making Bitcoin (ticker: BTC) transactions. It's got zero dependencies so you can pasta it into your project without concern.


## Synopsis

```rust
use std::str::FromStr;
use bdk_coin_select::{
    CoinSelector,
    Candidate,
    DrainWeights,
    FeeRate,
    Target,
    TR_SPK_WEIGHT,
    TXIN_BASE_WEIGHT,
    TXOUT_BASE_WEIGHT,
};
use bitcoin::{ address::NetworkUnchecked, Address, Network, Transaction, TxIn, TxOut };

// You should use miniscript to figure out the satisfaction weight for your coins!
const TR_SATISFACTION_WEIGHT: u32 = 66;
const TR_INPUT_WEIGHT: u32 = TXIN_BASE_WEIGHT + TR_SATISFACTION_WEIGHT;
const RECIPIENT_ADDRESS_STR: &'static str = "32iVBEu4dxkUQk9dJbZUiBiQdmypcEyJRf";
const CHANGE_ADDRESS_STR: &'static str = "132F25rTsvBdp9JzLLBHP5mvGY66i1xdiM";

let candidates = vec![
    Candidate {
        // How many inputs does this candidate represent. Needed so we can figure out the weight
        // of the varint that encodes the number of inputs.
        input_count: 1,
        // the value of the input
        value: 1_000_000,
        // the total weight of the input(s). This doesn't include
        weight: TR_INPUT_WEIGHT,
        // wether it's a segwit input. Needed so we know whether to include the segwit header
        // in total weight calculations.
        is_segwit: true
    },
    Candidate {
        // A candidate can represent multiple inputs in the case where you always want some inputs
        // to be spent together.
        input_count: 2,
        weight: 2*TR_INPUT_WEIGHT,
        value: 3_000_000,
        is_segwit: true
    },
    Candidate {
        input_count: 1,
        weight: TR_INPUT_WEIGHT,
        value: 5_000_000,
        is_segwit: true,
    }
];

let recipient = TxOut {
    script_pubkey: Address::from_str(RECIPIENT_ADDRESS_STR)
        .unwrap()
        .require_network(Network::Bitcoin)
        .unwrap()
        .script_pubkey(),
    value: 2_000_000,
};

let target = Target::new(
    FeeRate::default_min_relay_fee(),
    core::iter::once((recipient.weight() as u32, recipient.value)),
    core::iter::once(DrainWeights {
        output_weight: TXOUT_BASE_WEIGHT + TR_SPK_WEIGHT,
        spend_weight: TXIN_BASE_WEIGHT + TR_SATISFACTION_WEIGHT,
    }),
);

let mut coin_selector = CoinSelector::new(&candidates, &target);
```
