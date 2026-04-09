use {
    arc_swap::ArcSwap,
    clap::Parser,
    futures::{sink::SinkExt, stream::StreamExt},
    std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tonic::{metadata::AsciiMetadataValue, transport::Endpoint},
    yellowstone_grpc_client::{
        test_tools::{Unstable, UnstableConnector},
        AutoReconnect, Backoff, DedupState, DedupStream, InterceptorXToken, ReconnectConfig,
        TonicGrpcConnector,
    },
    yellowstone_grpc_proto::{geyser::geyser_client::GeyserClient, prelude::*},
};

#[derive(Debug, Parser)]
#[clap(about = "Client with unstable connection for testing auto-reconnect")]
struct Args {
    #[clap(short, long, default_value = "http://127.0.0.1:10000")]
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    #[clap(long, default_value_t = 10)]
    drop_interval_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let endpoint = Endpoint::from_shared(args.endpoint)?;
    let channel = endpoint.connect().await?;
    let config = ReconnectConfig::default();
    let x_token: Option<AsciiMetadataValue> = args.x_token.map(|t| t.parse()).transpose()?;

    let interceptor = InterceptorXToken {
        x_token: x_token.clone(),
        x_request_snapshot: false,
    };

    let mut geyser = GeyserClient::with_interceptor(channel, interceptor);

    let (mut tx, rx) = futures::channel::mpsc::channel(1000);

    let request_sink = Arc::new(Mutex::new(tx.clone()));

    let mut slots = HashMap::new();
    slots.insert(
        "".to_owned(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: None,
        },
    );

    let mut blocks_meta = HashMap::new();
    blocks_meta.insert("".to_owned(), SubscribeRequestFilterBlocksMeta::default());

    let mut accounts = HashMap::new();
    accounts.insert("".to_owned(), SubscribeRequestFilterAccounts::default());

    let mut transactions = HashMap::new();
    transactions.insert("".to_owned(), SubscribeRequestFilterTransactions::default());

    let mut entry = HashMap::new();
    entry.insert("".to_owned(), SubscribeRequestFilterEntry::default());

    let request = SubscribeRequest {
        slots,
        blocks_meta,
        accounts,
        transactions,
        entry,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };
    let shared_request = Arc::new(ArcSwap::new(Arc::new(request.clone())));

    tx.send(request.clone()).await?;
    let response = geyser.subscribe(rx).await?;

    let raw_stream = response.into_inner();

    let drop_interval = Duration::from_secs(args.drop_interval_secs);
    let unstable_stream = Unstable::new(raw_stream, drop_interval);

    let tonic_connector = TonicGrpcConnector::new(
        endpoint,
        config,
        x_token.clone(),
        Default::default(),
        request_sink,
    );

    let connector = UnstableConnector::new(tonic_connector, drop_interval);

    let auto_reconnect = AutoReconnect::new(
        DedupStream::new(unstable_stream, DedupState::with_slot_retention(1000)),
        connector,
        Arc::clone(&shared_request),
        Backoff::default(),
    );

    let mut stream = auto_reconnect;
    let mut count = 0u64;

    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => {
                count += 1;
                // Log Tx, Act and Entries in debug since they are more of them, the rest in info
                match update.update_oneof.as_ref() {
                    Some(subscribe_update::UpdateOneof::Slot(s)) => {
                        log::info!("slot={} status={} count={count}", s.slot, s.status);
                    }
                    Some(subscribe_update::UpdateOneof::Account(a)) => {
                        log::debug!("account slot={} count={count}", a.slot);
                    }
                    Some(subscribe_update::UpdateOneof::Transaction(t)) => {
                        log::debug!("tx slot={} count={count}", t.slot);
                    }
                    Some(subscribe_update::UpdateOneof::Entry(e)) => {
                        log::debug!("entry slot={} count={count}", e.slot);
                    }
                    Some(subscribe_update::UpdateOneof::BlockMeta(b)) => {
                        log::info!("block_meta slot={} count={count}", b.slot);
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
