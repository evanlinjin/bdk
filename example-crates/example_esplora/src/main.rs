use std::{
    collections::BTreeMap,
    io::{self, Write},
    sync::Mutex,
};

use bdk_chain::{
    bitcoin::{BlockHash, Network},
    indexed_tx_graph::{IndexedAdditions, IndexedTxGraph},
    keychain::{LocalChangeSet, LocalUpdate},
    local_chain::{ChangeSet, LocalChain},
    Append, ConfirmationTimeAnchor,
};

use bdk_esplora::{esplora_client, EsploraExt};

use example_cli::{
    anyhow::{self, Context},
    clap::{self, Parser, Subcommand},
    Keychain,
};

const DB_MAGIC: &[u8] = b"bdk_example_esplora";
const DB_PATH: &str = ".bdk_esplora_example.db";
const ASSUME_FINAL_DEPTH: usize = 10;

#[derive(Subcommand, Debug, Clone)]
enum EsploraCommands {
    // Just a simple test subcommand
    // Test,
    /// Scans the addresses in the wallet sing the esplora API
    Scan {
        /// When a gap this large has been found for a keychain, it will stop.
        #[clap(long, default_value = "5")]
        stop_gap: usize,
        #[clap(flatten)]
        scan_options: ScanOptions,
    },
}

#[derive(Parser, Debug, Clone, PartialEq)]
pub struct ScanOptions {
    /// Set batch size for each script_history call to electrum client.
    #[clap(long, default_value = "25")]
    pub batch_size: usize,
}

fn main() -> anyhow::Result<()> {
    let (args, keymap, index, db, init_changeset) = example_cli::init::<
        EsploraCommands,
        LocalChangeSet<Keychain, ConfirmationTimeAnchor>,
    >(DB_MAGIC, DB_PATH)?;

    let graph = Mutex::new({
        let mut graph = IndexedTxGraph::new(index);
        graph.apply_additions(init_changeset.indexed_additions);
        graph
    });

    let chain = Mutex::new({
        let mut chain = LocalChain::default();
        chain.apply_changeset(init_changeset.chain_changeset);
        chain
    });

    let esplora_url = match args.network {
        Network::Bitcoin => "https://blockstream.info/api/",
        Network::Testnet => "https://blockstream.info/testnet/api/",
        Network::Regtest => "http://localhost:5001",
        Network::Signet => "http://localhost:5001",
    };

    let client = esplora_client::Builder::new(esplora_url).build_blocking()?;

    // Match the given command. Exectute and return if command is provided by example_cli
    let esplora_cmd = match &args.command {
        // Command that are handled by the specify example
        example_cli::Commands::ChainSpecific(electrum_cmd) => electrum_cmd,
        // General commands handled by example_cli. Execute the cmd and return.
        general_cmd => {
            let res = example_cli::handle_commands(
                &graph,
                &db,
                &chain,
                &keymap,
                args.network,
                |tx| {
                    client
                        .broadcast(tx)
                        .map(|_| ())
                        .map_err(anyhow::Error::from)
                },
                general_cmd.clone(),
            );

            db.lock().unwrap().commit()?;
            return res;
        }
    };

    let response: LocalUpdate<_, _> = match esplora_cmd.clone() {
        EsploraCommands::Scan {
            stop_gap,
            scan_options,
        } => {
            let (keychain_spks, local_chain) = {
                let graph = &*graph.lock().unwrap();
                let chain = &*chain.lock().unwrap();

                let keychain_spks = graph
                    .index
                    .spks_of_all_keychains()
                    .into_iter()
                    .map(|(keychain, iter)| {
                        let mut first = true;
                        let spk_iter = iter.inspect(move |(i, _)| {
                            if first {
                                eprint!("\nscanning {}: ", keychain);
                                first = false;
                            }

                            eprint!("{} ", i);
                            let _ = io::stdout().flush();
                        });
                        (keychain, spk_iter)
                    })
                    .collect::<BTreeMap<_, _>>();

                let c = chain
                    .blocks()
                    .iter()
                    .rev()
                    .take(ASSUME_FINAL_DEPTH)
                    .map(|(k, v)| (*k, *v))
                    .collect::<BTreeMap<u32, BlockHash>>();

                (keychain_spks, c)
            };
            client
                .scan(
                    &local_chain,
                    keychain_spks,
                    core::iter::empty(),
                    core::iter::empty(),
                    stop_gap,
                    scan_options.batch_size,
                )
                .context("scanning the blockchain")?
        }
    };

    // TODO: It would be nice if one could convert a LocalUpdate directly into a LocalChangeSet
    let db_changeset: LocalChangeSet<Keychain, ConfirmationTimeAnchor> = {
        let mut chain = chain.lock().unwrap();
        let mut graph = graph.lock().unwrap();

        let chain_changeset: ChangeSet = chain.apply_update(response.chain).unwrap();

        let indexed_additions = {
            let mut additions = IndexedAdditions::default();
            let (_, index_additions) = graph.index.reveal_to_target_multi(&response.keychain);
            additions.append(IndexedAdditions {
                index_additions,
                ..Default::default()
            });
            additions.append(graph.apply_update(response.graph));
            additions
        };

        LocalChangeSet {
            chain_changeset,
            indexed_additions,
        }
    };

    let mut db = db.lock().unwrap();
    db.stage(db_changeset);
    db.commit()?;
    Ok(())
}
