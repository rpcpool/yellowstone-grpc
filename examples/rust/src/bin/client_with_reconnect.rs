use {
    clap::{Parser, ValueEnum},
    futures::stream::StreamExt,
    log::info,
    std::collections::HashMap,
    yellowstone_grpc_client::{
        Backoff, GeyserGrpcClient, ReconnectConfig, ReconnectionPolicy, DEFAULT_SLOT_RETENTION,
    },
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions,
    },
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Policy {
    /// Re-request data produced while disconnected. No gap in the stream.
    Recover,
    /// Continue from the newest data. Anything missed is lost.
    Skip,
}

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC client with auto-reconnect")]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    #[clap(long)]
    slots: bool,

    #[clap(long)]
    accounts: bool,

    #[clap(long)]
    transactions: bool,

    /// What happens to data produced while the connection was down.
    #[clap(long, value_enum, default_value_t = Policy::Recover)]
    policy: Policy,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    let reconnect_config = ReconnectConfig {
        backoff: Backoff::default(),
        policy: match args.policy {
            Policy::Recover => ReconnectionPolicy::RecoverMissedData {
                slot_retention: DEFAULT_SLOT_RETENTION,
            },
            Policy::Skip => ReconnectionPolicy::SkipMissedData,
        },
    };

    let mut client = GeyserGrpcClient::build_from_shared(args.endpoint)?
        .x_token(args.x_token)?
        .set_reconnect_config(reconnect_config)
        .connect()
        .await?;

    let request = SubscribeRequest {
        slots: if args.slots {
            let mut m = HashMap::new();
            m.insert(
                "".to_owned(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(true),
                    interslot_updates: Some(true),
                },
            );
            m
        } else {
            HashMap::new()
        },
        accounts: if args.accounts {
            let mut m = HashMap::new();
            m.insert("".to_owned(), SubscribeRequestFilterAccounts::default());
            m
        } else {
            HashMap::new()
        },
        transactions: if args.transactions {
            let mut m = HashMap::new();
            m.insert("".to_owned(), SubscribeRequestFilterTransactions::default());
            m
        } else {
            HashMap::new()
        },
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    info!("connecting with policy={:?}", args.policy);
    let mut stream = client.subscribe_once(request).await?;
    let mut count = 0u64;

    // Reconnects happen underneath this loop. Nothing here needs to know.
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => {
                count += 1;
                match update.update_oneof.as_ref() {
                    Some(UpdateOneof::Slot(slot)) => {
                        if count.is_multiple_of(10) {
                            info!("slot={} count={count}", slot.slot);
                        }
                    }
                    Some(UpdateOneof::Account(acc)) => {
                        if count.is_multiple_of(100) {
                            info!("account update slot={} count={count}", acc.slot);
                        }
                    }
                    Some(UpdateOneof::Transaction(tx)) => {
                        if count.is_multiple_of(100) {
                            info!("transaction slot={} count={count}", tx.slot);
                        }
                    }
                    _ => {}
                }
            }
            Err(status) => {
                log::error!("fatal: {status}");
                break;
            }
        }
    }

    Ok(())
}
