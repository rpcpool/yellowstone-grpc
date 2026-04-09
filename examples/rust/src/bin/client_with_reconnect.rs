use {
    clap::Parser,
    futures::stream::StreamExt,
    log::info,
    std::collections::HashMap,
    yellowstone_grpc_client::{GeyserGrpcClient, ReconnectConfig},
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions,
    },
};

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    let mut client = GeyserGrpcClient::build_from_shared(args.endpoint)?
        .x_token(args.x_token)?
        .set_reconnect_config(ReconnectConfig::default())
        .connect()
        .await?;

    let request = SubscribeRequest {
        slots: if args.slots {
            let mut m = HashMap::new();
            m.insert(
                "".to_owned(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(true),
                    interslot_updates: None,
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

    info!("connecting with auto-reconnect enabled");
    let mut stream = client.subscribe_once(request).await?;
    let mut count = 0u64;

    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => {
                count += 1;
                match update.update_oneof.as_ref() {
                    Some(UpdateOneof::Slot(slot)) => {
                        if count % 10 == 0 {
                            info!("slot={} count={count}", slot.slot);
                        }
                    }
                    Some(UpdateOneof::Account(acc)) => {
                        if count % 100 == 0 {
                            info!("account update slot={} count={count}", acc.slot);
                        }
                    }
                    Some(UpdateOneof::Transaction(tx)) => {
                        if count % 100 == 0 {
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
