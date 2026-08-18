use {
    crate::scenarios::RunConfig,
    anyhow::{Context, Result},
    std::str::FromStr,
    tokio::net::TcpStream,
    yellowstone_block_machine::event::{
        BlockMetaEvInfo, EntryEvInfo, GeyserEventAdapter, GeyserEventInfo, SlotStatusKind,
        SlotUpdateEvInfo,
    },
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient},
    yellowstone_grpc_proto::geyser::{subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate},
};

pub async fn new_client(config: &RunConfig) -> Result<GeyserGrpcClient> {
    let mut builder = GeyserGrpcClient::build_from_shared(config.endpoint.clone())
        .context("endpoint should be a valid URI")?;

    if config.endpoint.starts_with("https://") {
        builder = builder
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .context("failed to configure TLS for HTTPS endpoint")?;
    }

    let builder = builder
        .x_token(config.x_token.clone())
        .context("x-token should be valid ASCII metadata if provided")?;

    let builder = builder
        .max_decoding_message_size(100_000_000)
        .initial_connection_window_size(10_000_000)
        .initial_stream_window_size(8_000_000)
        .http2_adaptive_window(true)
        .accept_compressed(yellowstone_grpc_proto::tonic::codec::CompressionEncoding::Zstd);

    if let Some(dial) = config.dial.clone() {
        builder
            .connect_with_connector(tower::service_fn(move |_uri| {
                let dial = dial.clone();
                async move {
                    let stream = TcpStream::connect(dial).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await
            .context("client should build from endpoint, dial and token")
    } else {
        builder
            .connect()
            .await
            .context("client should build from endpoint and token")
    }
}

const fn slot_status_kind(value: SlotStatus) -> SlotStatusKind {
    match value {
        SlotStatus::SlotFirstShredReceived => SlotStatusKind::FirstShredReceived,
        SlotStatus::SlotCompleted => SlotStatusKind::Completed,
        SlotStatus::SlotCreatedBank => SlotStatusKind::CreatedBank,
        SlotStatus::SlotDead => SlotStatusKind::Dead,
        SlotStatus::SlotProcessed => SlotStatusKind::Processed,
        SlotStatus::SlotConfirmed => SlotStatusKind::Confirmed,
        SlotStatus::SlotFinalized => SlotStatusKind::Finalized,
    }
}

pub struct E2EGeyserEventAdapter;

impl GeyserEventAdapter for E2EGeyserEventAdapter {
    type EventT = SubscribeUpdate;

    fn extract_geyser_ev_info(event: &Self::EventT) -> Option<GeyserEventInfo> {
        let update_oneof = event.update_oneof.as_ref()?;
        match update_oneof {
            UpdateOneof::Slot(slot_update) => Some(GeyserEventInfo::Slot(SlotUpdateEvInfo {
                slot: slot_update.slot,
                parent: slot_update.parent,
                status: slot_status_kind(slot_update.status()),
                dead_error: slot_update.dead_error.is_some(),
            })),
            UpdateOneof::BlockMeta(block_meta) => {
                Some(GeyserEventInfo::BlockMeta(BlockMetaEvInfo {
                    slot: block_meta.slot,
                    parent_slot: block_meta.parent_slot,
                    entries_count: block_meta.entries_count,
                    executed_transaction_count: block_meta.executed_transaction_count,
                    blockhash: solana_hash::Hash::from_str(&block_meta.blockhash)
                        .ok()?
                        .to_bytes(),
                }))
            }
            UpdateOneof::Entry(entry) => Some(GeyserEventInfo::Entry(EntryEvInfo {
                slot: entry.slot,
                index: entry.index,
                starting_transaction_index: entry.starting_transaction_index,
                executed_transaction_count: entry.executed_transaction_count,
                hash: entry.hash.as_slice().try_into().ok()?,
            })),
            UpdateOneof::Transaction(tx) => Some(GeyserEventInfo::Transaction { slot: tx.slot }),
            UpdateOneof::Account(account) => Some(GeyserEventInfo::Account { slot: account.slot }),
            UpdateOneof::TransactionStatus(tx) => {
                Some(GeyserEventInfo::Transaction { slot: tx.slot })
            }
            UpdateOneof::Block(block) => Some(GeyserEventInfo::Other { slot: block.slot }),
            UpdateOneof::Ping(_) => None,
            UpdateOneof::Pong(_) => None,
        }
    }
}
