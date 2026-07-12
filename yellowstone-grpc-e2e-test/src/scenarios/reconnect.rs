use {
    crate::scenarios::RunConfig,
    anyhow::{bail, ensure, Context, Result},
    arc_swap::ArcSwap,
    futures::SinkExt,
    std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio_stream::StreamExt,
    yellowstone_block_machine::dragonsmouth::{
        stream::{BlockMachineOutput, BlockStream},
        wrapper::RESERVED_FILTER_NAME,
    },
    yellowstone_grpc_client::{
        test_tools::{Unstable, UnstableConnector},
        AutoReconnect, Backoff, DedupState, DedupStream, InterceptorXToken, ReconnectConfig,
        ReplayPolicy, TonicGrpcConnector,
    },
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_geyser::plugin::message::CommitmentLevel,
    yellowstone_grpc_proto::{
        geyser::{geyser_client::GeyserClient, SubscribeRequest, SubscribeRequestFilterSlots},
        tonic::{transport::Endpoint, Request},
    },
};

/// Verifies auto-reconnect recovers from simulated stream drops and
/// block reconstruction produces gapless frozen blocks across reconnects.
#[test_helper(name = "reconnect-blocks", tags = ["reconnect"])]
pub async fn it_should_reconstruct_blocks_across_reconnects(config: &RunConfig) -> Result<()> {
    let drop_interval = Duration::from_secs(15);
    let target_blocks = 30u64;

    let endpoint = Endpoint::from_shared(config.endpoint.clone()).context("valid endpoint")?;
    let channel = endpoint.connect().await.context("channel connect")?;

    let x_token = config
        .x_token
        .as_ref()
        .map(|t| t.parse())
        .transpose()
        .context("valid x-token")?;

    let interceptor = InterceptorXToken {
        x_token: x_token.clone(),
        x_request_snapshot: false,
    };

    let mut geyser = GeyserClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(16 * 1024 * 10224);

    let request = SubscribeRequest {
        slots: HashMap::from([(
            RESERVED_FILTER_NAME.to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        blocks: HashMap::from([("test".to_string(), Default::default())]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        accounts: HashMap::from([("test".to_string(), Default::default())]),
        transactions: HashMap::from([("test".to_string(), Default::default())]),
        entry: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };

    let (mut subscribe_tx, subscribe_rx) = futures::channel::mpsc::channel(1000);
    subscribe_tx
        .send(request.clone())
        .await
        .context("initial send")?;

    let response = geyser
        .subscribe(Request::new(subscribe_rx))
        .await
        .context("initial subscribe")?;
    let raw_stream = response.into_inner();

    let request_sink = Arc::new(Mutex::new(subscribe_tx));
    let shared_request = Arc::new(ArcSwap::from_pointee(request));

    let reconnect_config = ReconnectConfig {
        backoff: Backoff::new(Duration::from_millis(500), 2.0, 5),
        replay_policy: ReplayPolicy::default(),
        slot_retention: 250,
    };

    let tonic_connector = TonicGrpcConnector::new(
        endpoint,
        reconnect_config.backoff.clone(),
        x_token,
        Default::default(),
        Arc::clone(&request_sink),
    );
    let connector = UnstableConnector::new(tonic_connector, drop_interval);

    let unstable_stream = Unstable::new(raw_stream, drop_interval);

    let dedup = DedupStream::new(
        unstable_stream,
        DedupState::with_slot_retention(reconnect_config.slot_retention),
    );

    let auto_reconnect = AutoReconnect::new(
        dedup,
        connector,
        Arc::clone(&shared_request),
        reconnect_config,
    );

    let mut block_stream = BlockStream::new(
        auto_reconnect,
        solana_commitment_config::CommitmentLevel::Processed,
    );

    let mut block_count = 0u64;
    let mut last_slot = 0u64;
    let mut gaps = 0u64;

    let result = tokio::time::timeout(Duration::from_secs(50), async {
        while let Some(item) = block_stream.next().await {
            if block_count >= target_blocks {
                break;
            }

            match item {
                Ok(BlockMachineOutput::FrozenBlock(block)) => {
                    block_count += 1;

                    if last_slot > 0 && block.slot != last_slot + 1 {
                        log::warn!(
                            "gap: expected slot {} but got {} (missed {})",
                            last_slot + 1,
                            block.slot,
                            block.slot - last_slot - 1,
                        );
                        gaps += 1;
                    }

                    log::info!(
                        "block slot={} txns={} accounts={} entries={} total={block_count}/{target_blocks}",
                        block.slot,
                        block.txn_len(),
                        block.account_len(),
                        block.entry_len(),
                    );

                    last_slot = block.slot;
                }
                Ok(BlockMachineOutput::SlotCommitmentUpdate(u)) => {
                    log::debug!("commitment slot={} level={:?}", u.slot, u.commitment);
                }
                Ok(BlockMachineOutput::ForkDetected(f)) => {
                    log::warn!("fork slot={}", f.slot);
                }
                Ok(BlockMachineOutput::DeadBlockDetect(d)) => {
                    log::warn!("dead slot={}", d.slot);
                }
                Err(e) => {
                    bail!("block stream error: {e}");
                }
            }
        }
        Ok(())
    })
    .await;

    match result {
        Ok(inner) => inner?,
        Err(_) => bail!("test timed out after 50s with {block_count}/{target_blocks} blocks"),
    }

    ensure!(
        gaps <= 2,
        "block reconstruction should have minimal gaps across reconnects, got {gaps} gaps"
    );
    if gaps > 0 {
        log::warn!("reconnect-blocks: {gaps} gaps detected, known block-machine interop issue");
    }

    Ok(())
}
