mod tracker;
use bdk_chain::{
    bitcoin::{
        psbt::Prevouts, secp256k1::Secp256k1, util::sighash::SighashCache, Address, LockTime,
        Network, Sequence, Transaction, TxIn, TxOut,
    },
    keychain::DerivationAdditions,
    miniscript::{
        descriptor::{DescriptorSecretKey, KeyMap},
        Descriptor, DescriptorPublicKey,
    },
    Anchor, Append, BlockId, ChainOracle, DescriptorExt, FullTxOut, Loadable, ObservedAs,
};
use bdk_coin_select::{coin_select_bnb, CoinSelector, CoinSelectorOpt, WeightedValue};
use clap::{Parser, Subcommand};
use std::{cmp::Reverse, collections::HashMap, path::PathBuf, sync::Mutex, time::Duration};

pub use anyhow;
pub use bdk_file_store;
pub use clap;
pub use tracker::*;

#[derive(
    Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, serde::Deserialize, serde::Serialize,
)]
pub enum Keychain {
    External,
    Internal,
}

impl core::fmt::Display for Keychain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Keychain::External => write!(f, "external"),
            Keychain::Internal => write!(f, "internal"),
        }
    }
}

impl Default for Keychain {
    fn default() -> Self {
        Self::External
    }
}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
pub struct Args<C: clap::Subcommand> {
    #[clap(env = "DESCRIPTOR")]
    pub descriptor: String,
    #[clap(env = "CHANGE_DESCRIPTOR")]
    pub change_descriptor: Option<String>,

    #[clap(env = "BITCOIN_NETWORK", long, default_value = "signet")]
    pub network: Network,

    #[clap(env = "BDK_DB_PATH", long, default_value = ".bdk_example_db")]
    pub db_path: PathBuf,

    #[clap(env = "BDK_CP_LIMIT", long, default_value = "20")]
    pub cp_limit: usize,

    #[clap(subcommand)]
    pub command: Commands<C>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands<C: clap::Subcommand> {
    #[clap(flatten)]
    ChainSpecific(C),
    /// Address generation and inspection.
    Address {
        #[clap(subcommand)]
        addr_cmd: AddressCmd,
    },
    /// Get the wallet balance.
    Balance,
    /// TxOut related commands.
    #[clap(name = "txout")]
    TxOut {
        #[clap(subcommand)]
        txout_cmd: TxOutCmd,
    },
    /// Send coins to an address.
    Send {
        value: u64,
        address: Address,
        #[clap(short, default_value = "largest-first")]
        coin_select: CoinSelectionAlgo,
    },
}

#[derive(Clone, Debug)]
pub enum CoinSelectionAlgo {
    LargestFirst,
    SmallestFirst,
    OldestFirst,
    NewestFirst,
    BranchAndBound,
}

impl Default for CoinSelectionAlgo {
    fn default() -> Self {
        Self::LargestFirst
    }
}

impl core::str::FromStr for CoinSelectionAlgo {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use CoinSelectionAlgo::*;
        Ok(match s {
            "largest-first" => LargestFirst,
            "smallest-first" => SmallestFirst,
            "oldest-first" => OldestFirst,
            "newest-first" => NewestFirst,
            "bnb" => BranchAndBound,
            unknown => {
                return Err(anyhow::anyhow!(
                    "unknown coin selection algorithm '{}'",
                    unknown
                ))
            }
        })
    }
}

impl core::fmt::Display for CoinSelectionAlgo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use CoinSelectionAlgo::*;
        write!(
            f,
            "{}",
            match self {
                LargestFirst => "largest-first",
                SmallestFirst => "smallest-first",
                OldestFirst => "oldest-first",
                NewestFirst => "newest-first",
                BranchAndBound => "bnb",
            }
        )
    }
}

#[derive(Subcommand, Debug, Clone)]
pub enum AddressCmd {
    /// Get the next unused address.
    Next,
    /// Get a new address regardless of the existing unused addresses.
    New,
    /// List all addresses
    List {
        #[clap(long)]
        change: bool,
    },
    Index,
}

#[derive(Subcommand, Debug, Clone)]
pub enum TxOutCmd {
    List {
        /// Return only spent outputs.
        #[clap(short, long)]
        spent: bool,
        /// Return only unspent outputs.
        #[clap(short, long)]
        unspent: bool,
        /// Return only confirmed outputs.
        #[clap(long)]
        confirmed: bool,
        /// Return only unconfirmed outputs.
        #[clap(long)]
        unconfirmed: bool,
    },
}

/// A structure defining the output of an [`AddressCmd`]` execution.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AddrsOutput {
    keychain: String,
    index: u32,
    addrs: Address,
    used: bool,
}

pub fn run_address_cmd<A: Anchor, C: ChainOracle + Loadable>(
    tracker: &Mutex<Tracker<A, Keychain, C>>,
    db: &Mutex<TrackerStore<A, Keychain, C>>,
    addr_cmd: AddressCmd,
    network: Network,
) -> anyhow::Result<()>
where
    <Tracker<A, Keychain, C> as Loadable>::ChangeSet:
        serde::de::DeserializeOwned + serde::Serialize,
{
    let mut tracker = tracker.lock().unwrap();
    let txout_index = &mut tracker.indexed_graph.index;

    let addr_cmd_output = match addr_cmd {
        AddressCmd::Next => Some(txout_index.next_unused_spk(&Keychain::External)),
        AddressCmd::New => Some(txout_index.reveal_next_spk(&Keychain::External)),
        _ => None,
    };

    if let Some(((index, spk), additions)) = addr_cmd_output {
        let mut db = db.lock().unwrap();
        // update database since we're about to give out a new address
        db.append_changeset(&additions.into())?;

        let spk = spk.clone();
        let address =
            Address::from_script(&spk, network).expect("should always be able to derive address");
        eprintln!("This is the address at index {}", index);
        println!("{}", address);
    }

    match addr_cmd {
        AddressCmd::Next | AddressCmd::New => {
            /* covered */
            Ok(())
        }
        AddressCmd::Index => {
            for (keychain, derivation_index) in txout_index.last_revealed_indices() {
                println!("{:?}: {}", keychain, derivation_index);
            }
            Ok(())
        }
        AddressCmd::List { change } => {
            let target_keychain = match change {
                true => Keychain::Internal,
                false => Keychain::External,
            };
            for (index, spk) in txout_index.revealed_spks_of_keychain(&target_keychain) {
                let address = Address::from_script(spk, network)
                    .expect("should always be able to derive address");
                println!(
                    "{:?} {} used:{}",
                    index,
                    address,
                    txout_index.is_used(&(target_keychain, index))
                );
            }
            Ok(())
        }
    }
}

pub fn run_balance_cmd<A: Anchor, C: ChainOracle>(
    tracker: &Mutex<Tracker<A, Keychain, C>>,
    chain_tip: BlockId,
) -> anyhow::Result<()>
where
    <C as ChainOracle>::Error: std::error::Error + Send + Sync + 'static,
{
    let tracker = tracker.lock().unwrap();
    let utxos = tracker
        .try_list_owned_unspents(chain_tip)
        .collect::<Result<Vec<_>, C::Error>>()?;

    let (confirmed, unconfirmed) =
        utxos
            .into_iter()
            .fold((0, 0), |(confirmed, unconfirmed), (_, utxo)| {
                match utxo.chain_position {
                    bdk_chain::ObservedAs::Confirmed(_) => {
                        (confirmed + utxo.txout.value, unconfirmed)
                    }
                    bdk_chain::ObservedAs::Unconfirmed(_) => {
                        (confirmed, unconfirmed + utxo.txout.value)
                    }
                }
            });

    println!("confirmed: {}", confirmed);
    println!("unconfirmed: {}", unconfirmed);
    Ok(())
}

pub fn run_txo_cmd<A: Anchor, C: ChainOracle>(
    txout_cmd: TxOutCmd,
    tracker: &Mutex<Tracker<A, Keychain, C>>,
    chain_tip: BlockId,
    network: Network,
) -> anyhow::Result<()>
where
    <C as ChainOracle>::Error: std::error::Error + Send + Sync + 'static,
{
    match txout_cmd {
        TxOutCmd::List {
            unspent,
            spent,
            confirmed,
            unconfirmed,
        } => {
            let tracker = tracker.lock().unwrap();

            let txouts = tracker
                .try_list_owned_txouts(chain_tip)
                .filter(|r| match r {
                    Ok((_, full_txo)) => match (unspent, spent) {
                        (true, false) => full_txo.spent_by.is_none(),
                        (false, true) => full_txo.spent_by.is_some(),
                        _ => true,
                    },
                    Err(_) => true,
                })
                .filter(|r| match r {
                    Ok((_, full_txo)) => match (confirmed, unconfirmed) {
                        (true, false) => full_txo.chain_position.is_confirmed(),
                        (false, true) => !full_txo.chain_position.is_confirmed(),
                        _ => true,
                    },
                    Err(_) => true,
                })
                .collect::<Result<Vec<_>, _>>()?;

            for (spk_index, full_txout) in txouts {
                let address =
                    Address::from_script(&full_txout.txout.script_pubkey, network).unwrap();

                println!(
                    "{:?} {} {} {} spent:{:?}",
                    spk_index,
                    full_txout.txout.value,
                    full_txout.outpoint,
                    address,
                    full_txout.spent_by
                )
            }
            Ok(())
        }
    }
}

#[allow(clippy::type_complexity)] // FIXME
pub fn create_tx<A: Anchor, C: ChainOracle>(
    value: u64,
    address: Address,
    coin_select: CoinSelectionAlgo,
    tracker: &mut Tracker<A, Keychain, C>,
    chain_tip: BlockId,
    keymap: &HashMap<DescriptorPublicKey, DescriptorSecretKey>,
) -> anyhow::Result<(
    Transaction,
    Option<(DerivationAdditions<Keychain>, (Keychain, u32))>,
)>
where
    <C as ChainOracle>::Error: std::error::Error + Send + Sync + 'static,
{
    let mut additions = DerivationAdditions::default();

    let assets = bdk_tmp_plan::Assets {
        keys: keymap.iter().map(|(pk, _)| pk.clone()).collect(),
        ..Default::default()
    };

    // TODO use planning module
    let mut candidates =
        planned_utxos(tracker, chain_tip, &assets).collect::<Result<Vec<_>, C::Error>>()?;

    // apply coin selection algorithm
    match coin_select {
        CoinSelectionAlgo::LargestFirst => {
            candidates.sort_by_key(|(_, utxo)| Reverse(utxo.txout.value))
        }
        CoinSelectionAlgo::SmallestFirst => candidates.sort_by_key(|(_, utxo)| utxo.txout.value),
        CoinSelectionAlgo::OldestFirst => {
            candidates.sort_by_key(|(_, utxo)| utxo.chain_position.clone())
        }
        CoinSelectionAlgo::NewestFirst => {
            candidates.sort_by_key(|(_, utxo)| Reverse(utxo.chain_position.clone()))
        }
        CoinSelectionAlgo::BranchAndBound => {}
    }

    // turn the txos we chose into weight and value
    let wv_candidates = candidates
        .iter()
        .map(|(plan, utxo)| {
            WeightedValue::new(
                utxo.txout.value,
                plan.expected_weight() as _,
                plan.witness_version().is_some(),
            )
        })
        .collect();

    let mut outputs = vec![TxOut {
        value,
        script_pubkey: address.script_pubkey(),
    }];

    let internal_keychain = if tracker
        .indexed_graph
        .index
        .keychains()
        .get(&Keychain::Internal)
        .is_some()
    {
        Keychain::Internal
    } else {
        Keychain::External
    };

    let ((change_index, change_script), change_additions) = tracker
        .indexed_graph
        .index
        .next_unused_spk(&internal_keychain);
    additions.append(change_additions);

    // Clone to drop the immutable reference.
    let change_script = change_script.clone();

    let change_plan = bdk_tmp_plan::plan_satisfaction(
        &tracker
            .indexed_graph
            .index
            .keychains()
            .get(&internal_keychain)
            .expect("must exist")
            .at_derivation_index(change_index),
        &assets,
    )
    .expect("failed to obtain change plan");

    let mut change_output = TxOut {
        value: 0,
        script_pubkey: change_script,
    };

    let cs_opts = CoinSelectorOpt {
        target_feerate: 0.5,
        min_drain_value: tracker
            .indexed_graph
            .index
            .keychains()
            .get(&internal_keychain)
            .expect("must exist")
            .dust_value(),
        ..CoinSelectorOpt::fund_outputs(
            &outputs,
            &change_output,
            change_plan.expected_weight() as u32,
        )
    };

    // TODO: How can we make it easy to shuffle in order of inputs and outputs here?
    // apply coin selection by saying we need to fund these outputs
    let mut coin_selector = CoinSelector::new(&wv_candidates, &cs_opts);

    // just select coins in the order provided until we have enough
    // only use the first result (least waste)
    let selection = match coin_select {
        CoinSelectionAlgo::BranchAndBound => {
            coin_select_bnb(Duration::from_secs(10), coin_selector.clone())
                .map_or_else(|| coin_selector.select_until_finished(), |cs| cs.finish())?
        }
        _ => coin_selector.select_until_finished()?,
    };
    let (_, selection_meta) = selection.best_strategy();

    // get the selected utxos
    let selected_txos = selection.apply_selection(&candidates).collect::<Vec<_>>();

    if let Some(drain_value) = selection_meta.drain_value {
        change_output.value = drain_value;
        // if the selection tells us to use change and the change value is sufficient, we add it as an output
        outputs.push(change_output)
    }

    let mut transaction = Transaction {
        version: 0x02,
        lock_time: LockTime::from_height(chain_tip.height)
            .unwrap_or(LockTime::ZERO)
            .into(),
        input: selected_txos
            .iter()
            .map(|(_, utxo)| TxIn {
                previous_output: utxo.outpoint,
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                ..Default::default()
            })
            .collect(),
        output: outputs,
    };

    let prevouts = selected_txos
        .iter()
        .map(|(_, utxo)| utxo.txout.clone())
        .collect::<Vec<_>>();
    let sighash_prevouts = Prevouts::All(&prevouts);

    // first, set tx values for the plan so that we don't change them while signing
    for (i, (plan, _)) in selected_txos.iter().enumerate() {
        if let Some(sequence) = plan.required_sequence() {
            transaction.input[i].sequence = sequence
        }
    }

    // create a short lived transaction
    let _sighash_tx = transaction.clone();
    let mut sighash_cache = SighashCache::new(&_sighash_tx);

    for (i, (plan, _)) in selected_txos.iter().enumerate() {
        let requirements = plan.requirements();
        let mut auth_data = bdk_tmp_plan::SatisfactionMaterial::default();
        assert!(
            !requirements.requires_hash_preimages(),
            "can't have hash pre-images since we didn't provide any."
        );
        assert!(
            requirements.signatures.sign_with_keymap(
                i,
                keymap,
                &sighash_prevouts,
                None,
                None,
                &mut sighash_cache,
                &mut auth_data,
                &Secp256k1::default(),
            )?,
            "we should have signed with this input."
        );

        match plan.try_complete(&auth_data) {
            bdk_tmp_plan::PlanState::Complete {
                final_script_sig,
                final_script_witness,
            } => {
                if let Some(witness) = final_script_witness {
                    transaction.input[i].witness = witness;
                }

                if let Some(script_sig) = final_script_sig {
                    transaction.input[i].script_sig = script_sig;
                }
            }
            bdk_tmp_plan::PlanState::Incomplete(_) => {
                return Err(anyhow::anyhow!(
                    "we weren't able to complete the plan with our keys."
                ));
            }
        }
    }

    let change_info = if selection_meta.drain_value.is_some() {
        Some((additions, (internal_keychain, change_index)))
    } else {
        None
    };

    Ok((transaction, change_info))
}

pub fn planned_utxos<'a, AK: bdk_tmp_plan::CanDerive + Clone, A: Anchor, C: ChainOracle>(
    tracker: &'a Tracker<A, Keychain, C>,
    chain_tip: BlockId,
    assets: &'a bdk_tmp_plan::Assets<AK>,
) -> impl Iterator<Item = Result<(bdk_tmp_plan::Plan<AK>, FullTxOut<ObservedAs<A>>), C::Error>> + 'a
where
    <C as ChainOracle>::Error: std::error::Error + Send + Sync + 'static,
{
    tracker
        .try_list_owned_unspents(chain_tip)
        .filter_map(|r| match r {
            Ok(((keychain, derivation_index), full_txo)) => {
                let desc = tracker
                    .indexed_graph
                    .index
                    .keychains()
                    .get(keychain)
                    .expect("must exist")
                    .at_derivation_index(*derivation_index);
                let plan = bdk_tmp_plan::plan_satisfaction(&desc, assets)?;
                Some(Ok((plan, full_txo)))
            }
            Err(err) => Some(Err(err)),
        })
}

#[allow(clippy::too_many_arguments)] // FIXME
pub fn handle_commands<S: clap::Subcommand, A: Anchor, C: ChainOracle + Loadable>(
    command: Commands<S>,
    broadcast: impl FnOnce(&Transaction) -> anyhow::Result<()>,
    // we Mutex around these not because we need them for a simple CLI app but to demonstrate how
    // all the stuff we're doing can be made thread-safe and not keep locks up over an IO bound.
    tracker: &Mutex<Tracker<A, Keychain, C>>,
    store: &Mutex<TrackerStore<A, Keychain, C>>,
    chain_tip: BlockId,
    network: Network,
    keymap: &HashMap<DescriptorPublicKey, DescriptorSecretKey>,
) -> anyhow::Result<()>
where
    <C as ChainOracle>::Error: std::error::Error + Send + Sync + 'static,
    <Tracker<A, Keychain, C> as Loadable>::ChangeSet:
        serde::de::DeserializeOwned + serde::Serialize,
{
    match command {
        // TODO: Make these functions return stuffs
        Commands::Address { addr_cmd } => run_address_cmd(tracker, store, addr_cmd, network),
        Commands::Balance => run_balance_cmd(tracker, chain_tip),
        Commands::TxOut { txout_cmd } => run_txo_cmd(txout_cmd, tracker, chain_tip, network),
        Commands::Send {
            value,
            address,
            coin_select,
        } => {
            let (transaction, change_index) = {
                // take mutable ref to construct tx -- it is only open for a short time while building it.
                let tracker = &mut *tracker.lock().unwrap();

                let (transaction, change_info) =
                    create_tx(value, address, coin_select, tracker, chain_tip, keymap)?;

                if let Some((change_derivation_changes, (change_keychain, index))) = change_info {
                    // We must first persist to disk the fact that we've got a new address from the
                    // change keychain so future scans will find the tx we're about to broadcast.
                    // If we're unable to persist this, then we don't want to broadcast.
                    let store = &mut *store.lock().unwrap();
                    store.append_changeset(&change_derivation_changes.into())?;

                    // We don't want other callers/threads to use this address while we're using it
                    // but we also don't want to scan the tx we just created because it's not
                    // technically in the blockchain yet.
                    tracker
                        .indexed_graph
                        .index
                        .mark_used(&change_keychain, index);
                    (transaction, Some((change_keychain, index)))
                } else {
                    (transaction, None)
                }
            };

            match (broadcast)(&transaction) {
                Ok(_) => {
                    println!("Broadcasted Tx : {}", transaction.txid());
                    let now = std::time::SystemTime::elapsed(&std::time::UNIX_EPOCH).unwrap();

                    let mut tracker = tracker.lock().unwrap();
                    let additions =
                        tracker
                            .indexed_graph
                            .insert_tx(&transaction, None, Some(now.as_secs()));
                    if !additions.graph_additions.is_empty()
                        || !additions.index_additions.is_empty()
                    {
                        let store = &mut *store.lock().unwrap();
                        // We know the tx is at least unconfirmed now. Note if persisting here fails,
                        // it's not a big deal since we can always find it again form
                        // blockchain.
                        store.append_changeset(&additions.into())?;
                    }
                    Ok(())
                }
                Err(e) => {
                    let tracker = &mut *tracker.lock().unwrap();
                    if let Some((keychain, index)) = change_index {
                        // We failed to broadcast, so allow our change address to be used in the future
                        tracker.indexed_graph.index.unmark_used(&keychain, index);
                    }
                    Err(e)
                }
            }
        }
        Commands::ChainSpecific(_) => {
            todo!("example code is meant to handle this!")
        }
    }
}

#[allow(clippy::type_complexity)] // FIXME
pub fn init<S: clap::Subcommand, A: Anchor, C: ChainOracle + Loadable>(
    db_magic: &'static [u8],
    db_default_path: &str,
    mut tracker: Tracker<A, Keychain, C>,
) -> anyhow::Result<(
    Args<S>,
    KeyMap,
    // These don't need to have mutexes around them, but we want the cli example code to make it obvious how they
    // are thread-safe, forcing the example developers to show where they would lock and unlock things.
    Mutex<Tracker<A, Keychain, C>>,
    Mutex<TrackerStore<A, Keychain, C>>,
)>
where
    <C as ChainOracle>::Error: std::error::Error + Send + Sync + 'static,
    <Tracker<A, Keychain, C> as Loadable>::ChangeSet:
        serde::de::DeserializeOwned + serde::Serialize,
{
    use bdk_chain::PersistBackend;

    if std::env::var("BDK_DB_PATH").is_err() {
        std::env::set_var("BDK_DB_PATH", db_default_path);
    }

    let args = Args::<S>::parse();
    let secp = Secp256k1::default();
    let (descriptor, mut keymap) =
        Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, &args.descriptor)?;

    tracker
        .indexed_graph
        .index
        .add_keychain(Keychain::External, descriptor);

    let internal = args
        .change_descriptor
        .clone()
        .map(|descriptor| Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, &descriptor))
        .transpose()?;
    if let Some((internal_descriptor, internal_keymap)) = internal {
        keymap.extend(internal_keymap);
        tracker
            .indexed_graph
            .index
            .add_keychain(Keychain::Internal, internal_descriptor);
    };

    let mut db = TrackerStore::<A, Keychain, C>::new_from_path(db_magic, args.db_path.as_path())?;

    if let Err(e) = db.load_into_tracker(&mut tracker) {
        // [TODO] Should we introduce a `TipChainOracle` trait?
        // match tracker.last_seen_height()  {
        //     Some(tip) => eprintln!("Failed to load all changesets from {}. Last checkpoint was at height {}. Error: {}", args.db_path.display(), tip, e),
        //     None => eprintln!("Failed to load any checkpoints from {}: {}", args.db_path.display(), e),
        // }
        eprintln!(
            "Failed to load changesets from {}: {}",
            args.db_path.display(),
            e
        );
        eprintln!("⚠ Consider running a rescan of chain data.");
    }

    Ok((args, keymap, Mutex::new(tracker), Mutex::new(db)))
}