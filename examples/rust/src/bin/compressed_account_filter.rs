use {
    clap::{Parser, ValueEnum},
    futures::stream::StreamExt,
    log::info,
    solana_pubkey::Pubkey,
    std::{env, str::FromStr},
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        cuckoo::CompressedAccountFilterSet,
        geyser::SubscribeRequestFilterAccounts,
        prelude::{subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest},
    },
};

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
enum Mode {
    /// Explicit account list (control)
    Explicit,
    /// Cuckoo filter on accounts subscription
    Cuckoo,
    /// Cuckoo filter inside a blocks subscription
    CuckooBlocks,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[clap(
    author,
    version,
    about = "Subscribe to Yellowstone gRPC account updates"
)]
struct Args {
    /// Yellowstone gRPC endpoint
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    endpoint: String,

    /// Optional auth token sent as x-token header
    #[clap(long)]
    x_token: Option<String>,

    /// Filter by account pubkey (repeatable)
    #[clap(long)]
    account: Vec<String>,

    /// Stream commitment
    #[clap(long, value_enum, default_value_t)]
    commitment: ArgsCommitment,

    #[arg(long, default_value = "explicit")]
    mode: Mode,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    if args.account.is_empty() {
        anyhow::bail!("at least one --account <PUBKEY> is required");
    }

    let pubkeys: Vec<Pubkey> = args
        .account
        .iter()
        .map(|s| {
            Pubkey::from_str(s)
                .map_err(|err| anyhow::anyhow!("invalid --account pubkey `{s}`: {err}"))
        })
        .collect::<anyhow::Result<_>>()?;

    let mut client = GeyserGrpcClient::build_from_shared(args.endpoint)?
        .x_token(args.x_token)?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    let mut request = SubscribeRequest {
        commitment: Some(CommitmentLevel::from(args.commitment) as i32),
        ..Default::default()
    };

    // local copy for false-positive checks on incoming updates
    let mut local_filter = CompressedAccountFilterSet::with_capacity(pubkeys.len().max(100))?;
    for pk in &pubkeys {
        local_filter.insert(*pk)?;
    }

    match args.mode {
        Mode::Explicit => {
            let filter = SubscribeRequestFilterAccounts {
                account: args.account.clone(),
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
                cuckoo_accounts_filter: None,
            };
            request.accounts.insert("explicit".to_string(), filter);
            info!("mode: explicit account list ({} accounts)", pubkeys.len());
        }
        Mode::Cuckoo => {
            let mut set = CompressedAccountFilterSet::with_capacity(pubkeys.len().max(100))?;
            for pk in &pubkeys {
                set.insert(*pk)?;
            }
            set.insert_into_subscribe_request(&mut request, "cuckoo_accounts");
            info!(
                "mode: cuckoo on accounts subscription ({} accounts)",
                pubkeys.len()
            );
        }
        Mode::CuckooBlocks => {
            let mut set = CompressedAccountFilterSet::with_capacity(pubkeys.len().max(100))?;
            for pk in &pubkeys {
                set.insert(*pk)?;
            }
            let mut block_filter = set.to_block_filter();
            block_filter.include_accounts = Some(true);
            block_filter.include_transactions = Some(true);
            request
                .blocks
                .insert("cuckoo_blocks".to_string(), block_filter);
            info!(
                "mode: cuckoo inside blocks subscription ({} accounts)",
                pubkeys.len()
            );
        }
    }

    info!("connected; waiting for account updates");
    let mut stream = client.subscribe_once(request).await?;

    while let Some(message) = stream.next().await {
        match message?.update_oneof {
            Some(UpdateOneof::Account(update)) => {
                if let Some(account) = update.account {
                    let Ok(pubkey_bytes) = <[u8; 32]>::try_from(account.pubkey.as_slice()) else {
                        continue;
                    };
                    let pubkey = Pubkey::new_from_array(pubkey_bytes);

                    if !local_filter.contains(pubkey) {
                        info!(
                            "false positive: slot={} pubkey={} skipping",
                            update.slot, pubkey,
                        );
                        continue;
                    }

                    info!(
                        "slot={} pubkey={} owner={} lamports={} executable={}",
                        update.slot,
                        pubkey,
                        bs58::encode(account.owner).into_string(),
                        account.lamports,
                        account.executable,
                    );
                }
            }
            Some(UpdateOneof::Block(block)) => {
                info!(
                    "block: slot={} accounts={} txs={}",
                    block.slot,
                    block.accounts.len(),
                    block.transactions.len(),
                );
            }
            _ => {}
        }
    }

    Ok(())
}
