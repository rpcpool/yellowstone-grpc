use {
    crate::scenarios::RunConfig,
    anyhow::{ensure, Context, Result},
    arc_swap::ArcSwap,
    futures::channel::mpsc,
    std::{
        collections::{HashMap, HashSet},
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio_stream::StreamExt,
    yellowstone_grpc_client::{
        test_tools::UnstableConnector, AutoReconnect, Backoff, DedupState, DedupStream,
        GrpcConnector, ReconnectConfig, ReconnectionPolicy, TonicGrpcConnector, DEFAULT_SLOT_RETENTION,
    },
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeUpdate,
        },
        tonic::{transport::Endpoint, Status},
    },
};

const DROP_INTERVAL: Duration = Duration::from_secs(5);
const SLOTS_TO_OBSERVE: usize = 40;

/// Slots plus blocks_meta. BlockMeta sets the checkpoint, so without it neither
/// policy ever replays and the scenario proves nothing.
fn subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        )]),
        blocks_meta: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterBlocksMeta::default(),
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    }
}

fn connector(
    config: &RunConfig,
    policy: ReconnectionPolicy,
) -> Result<(UnstableConnector, Arc<ArcSwap<SubscribeRequest>>)> {
    let endpoint =
        Endpoint::from_shared(config.endpoint.clone()).context("endpoint should be a valid URI")?;
    let x_token = config
        .x_token
        .clone()
        .map(|t| t.parse())
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid x-token: {e}"))?;

    // AutoReconnect swaps this sender on every reconnect; the scenario never
    // sends mid-stream requests, so the receiver is dropped.
    let (tx, _rx) = mpsc::channel(1000);

    let inner = TonicGrpcConnector::new(
        endpoint,
        ReconnectConfig {
            backoff: Backoff::default(),
            policy,
        },
        x_token,
        Default::default(),
        Arc::new(Mutex::new(tx)),
    );

    Ok((
        UnstableConnector::new(inner, DROP_INTERVAL),
        Arc::new(ArcSwap::from_pointee(subscribe_request())),
    ))
}

#[derive(Default)]
struct Observed {
    seen: HashSet<(u64, i32)>,
    duplicates: u64,
}

impl Observed {
    /// Distinct slots, sorted. Derived from `seen` so there is one source of truth.
    fn slots(&self) -> Vec<u64> {
        let mut slots: Vec<u64> = self.seen.iter().map(|(slot, _)| *slot).collect();
        slots.sort_unstable();
        slots.dedup();
        slots
    }

    fn gaps(&self) -> Vec<(u64, u64)> {
        self.slots()
            .windows(2)
            .filter(|w| w[1] > w[0] + 1)
            .map(|w| (w[0], w[1]))
            .collect()
    }
}

/// Consumes until `SLOTS_TO_OBSERVE` distinct slots have been seen, recording
/// every (slot, status) pair. Duplicates fail both policies: Recover dedups them,
/// Skip never replays.
async fn observe<S>(mut stream: S) -> Result<Observed>
where
    S: futures::Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    let mut observed = Observed::default();
    let mut distinct = 0usize;

    while let Some(update) = stream.next().await {
        let update = update.context("stream should yield updates without error")?;
        let Some(UpdateOneof::Slot(slot)) = update.update_oneof else {
            continue;
        };

        if observed.seen.insert((slot.slot, slot.status)) {
            distinct = observed.slots().len();
        } else {
            observed.duplicates += 1;
            log::warn!("duplicate: slot={} status={}", slot.slot, slot.status);
        }

        if distinct >= SLOTS_TO_OBSERVE {
            break;
        }
    }

    Ok(observed)
}

/// Auto-reconnect replays missed slots after forced disconnects, without duplicates.
#[test_helper(name = "reconnect-recover", tags = ["client", "reconnect"])]
pub async fn reconnect_should_recover_missed_slots(config: &RunConfig) -> Result<()> {
    let (connector, request) = connector(
        config,
        ReconnectionPolicy::RecoverMissedData {
            slot_retention: DEFAULT_SLOT_RETENTION,
        },
    )?;

    let first = connector
        .connect(request.load_full(), None)
        .await
        .context("initial connection should succeed")?;

    let stream = DedupStream::new(
        AutoReconnect::new(first, connector, request, Backoff::default()),
        DedupState::with_slot_retention(DEFAULT_SLOT_RETENTION),
    );

    let observed = observe(stream).await?;
    ensure!(
        observed.duplicates == 0,
        "{} duplicates delivered",
        observed.duplicates
    );
    ensure!(
        observed.gaps().is_empty(),
        "recover policy should leave no gaps, got {:?}",
        observed.gaps()
    );

    Ok(())
}

/// SkipMissedData never replays, so forced disconnects leave gaps and no duplicates.
#[test_helper(name = "reconnect-skip", tags = ["client", "reconnect"])]
pub async fn reconnect_should_skip_missed_slots(config: &RunConfig) -> Result<()> {
    let (connector, request) = connector(config, ReconnectionPolicy::SkipMissedData)?;

    let first = connector
        .connect(request.load_full(), None)
        .await
        .context("initial connection should succeed")?;

    let stream =
        AutoReconnect::new(first, connector, request, Backoff::default()).without_checkpoint();

    let observed = observe(stream).await?;
    ensure!(
        observed.duplicates == 0,
        "{} duplicates delivered",
        observed.duplicates
    );
    ensure!(
        !observed.gaps().is_empty(),
        "skip policy should leave gaps; disconnects may not have fired"
    );

    Ok(())
}