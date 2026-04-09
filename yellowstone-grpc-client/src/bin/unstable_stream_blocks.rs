use {
    arc_swap::ArcSwap,
    clap::Parser,
    futures::{sink::SinkExt, stream::StreamExt},
    solana_commitment_config::CommitmentLevel,
    std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tonic::{metadata::AsciiMetadataValue, transport::Endpoint},
    yellowstone_block_machine::dragonsmouth::{
        stream::{BlockMachineOutput, BlockStream},
        wrapper::RESERVED_FILTER_NAME,
    },
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

    // Build request exactly like subscribe_block does
    let mut slots = HashMap::new();
    slots.insert(
        RESERVED_FILTER_NAME.to_owned(),
        SubscribeRequestFilterSlots {
            interslot_updates: Some(true),
            ..Default::default()
        },
    );

    let mut blocks_meta = HashMap::new();
    blocks_meta.insert(
        RESERVED_FILTER_NAME.to_owned(),
        SubscribeRequestFilterBlocksMeta::default(),
    );

    let mut accounts = HashMap::new();
    accounts.insert("".to_owned(), SubscribeRequestFilterAccounts::default());

    let mut transactions = HashMap::new();
    transactions.insert("".to_owned(), SubscribeRequestFilterTransactions::default());

    let mut entry = HashMap::new();
    entry.insert(
        RESERVED_FILTER_NAME.to_owned(),
        SubscribeRequestFilterEntry::default(),
    );
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

    // Wrap in Unstable to simulate disconnects
    let drop_interval = Duration::from_secs(args.drop_interval_secs);
    let unstable_stream = Unstable::new(raw_stream, drop_interval);

    let tonic_connector =
        TonicGrpcConnector::new(endpoint, config, x_token, Default::default(), request_sink);
    let connector = UnstableConnector::new(tonic_connector, drop_interval);

    let auto_reconnect = AutoReconnect::new(
        DedupStream::new(unstable_stream, DedupState::with_slot_retention(1000)),
        connector,
        Arc::clone(&shared_request),
        Backoff::default(),
    );

    // Feed into BlockStream
    let mut block_stream = BlockStream::new(auto_reconnect, CommitmentLevel::Confirmed);

    let mut block_count = 0u64;
    let mut last_slot = 0u64;

    while let Some(item) = block_stream.next().await {
        match item {
            Ok(BlockMachineOutput::FrozenBlock(block)) => {
                block_count += 1;

                // Check for gaps
                if last_slot > 0 && block.slot != last_slot + 1 {
                    log::warn!(
                        "GAP: expected slot {} but got {} (missed {})",
                        last_slot + 1,
                        block.slot,
                        block.slot - last_slot - 1,
                    );
                }

                log::info!(
                    "BLOCK slot={} txns={} accounts={} entries={} total_blocks={block_count}",
                    block.slot,
                    block.txn_len(),
                    block.account_len(),
                    block.entry_len(),
                );

                last_slot = block.slot;
            }
            Ok(BlockMachineOutput::SlotCommitmentUpdate(u)) => {
                log::debug!("COMMITMENT slot={} level={:?}", u.slot, u.commitment);
            }
            Ok(BlockMachineOutput::ForkDetected(f)) => {
                log::warn!("FORK slot={}", f.slot);
            }
            Ok(BlockMachineOutput::DeadBlockDetect(d)) => {
                log::warn!("DEAD slot={}", d.slot);
            }
            Err(e) => {
                log::error!("block stream error: {e}");
                break;
            }
        }
    }

    Ok(())
}
