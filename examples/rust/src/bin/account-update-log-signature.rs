use {
    clap::{Parser, ValueEnum},
    futures::{sink::SinkExt, stream::StreamExt},
    log::{error, info},
    maplit::hashmap,
    solana_sdk::pubkey::Pubkey,
    std::{collections::HashMap, env},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
        SubscribeUpdateAccount,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    /// Commitment level: processed, confirmed or finalized
    #[clap(long)]
    commitment: Option<ArgsCommitment>,

    /// Filter by Owner Pubkey
    #[clap(long)]
    owner: Vec<String>,

    /// Filter by Data size
    #[clap(long)]
    datasize: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum ArgsCommitment {
    #[default]
    Processed,
    // Only `Processed` right now because we do not send multiple account updates in `Confirmed` & `Finalized`
    // See https://github.com/rpcpool/yellowstone-grpc/issues/232
    // Confirmed,
    // Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            // ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            // ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let mut client = GeyserGrpcClient::connect(args.endpoint, args.x_token, None)?;
    let (mut subscribe_tx, mut stream) = client.subscribe().await?;

    let mut filters = vec![];
    if let Some(datasize) = args.datasize {
        filters.push(SubscribeRequestFilterAccountsFilter {
            filter: Some(AccountsFilterDataOneof::Datasize(datasize)),
        });
    }
    let commitment: CommitmentLevel = args.commitment.unwrap_or_default().into();
    subscribe_tx
        .send(SubscribeRequest {
            slots: HashMap::new(),
            accounts: hashmap! { "".to_owned() => SubscribeRequestFilterAccounts {
                account: vec![],
                owner: args.owner.clone(),
                filters,
            } },
            transactions: HashMap::new(),
            entry: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: Some(commitment as i32),
            accounts_data_slice: vec![],
            ping: None,
        })
        .await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Account(SubscribeUpdateAccount {
                    is_startup: _is_startup,
                    slot,
                    account,
                })) => {
                    let account = account.expect("should be defined");
                    info!(
                        "Update account {} in slot #{} by tx {}",
                        Pubkey::try_from(account.pubkey).expect("valid pubkey"),
                        slot,
                        bs58::encode(account.txn_signature.unwrap_or_default()).into_string()
                    );
                }
                _ => {}
            },
            Err(error) => {
                error!("stream error: {error:?}");
                break;
            }
        }
    }

    Ok(())
}
