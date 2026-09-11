use {
    crate::scenarios::RunConfig,
    anyhow::{bail, ensure, Context, Result},
    std::{
        collections::{HashMap, HashSet},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::time::timeout,
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterBlockFooter, SubscribeRequestFilterBlocksMeta,
        SubscribeUpdateBlockFooter,
    },
};

const FILTER_NAME: &str = "test";
// Blocks land every ~400ms, so a gap this long means the stream is stuck.
const UPDATE_TIMEOUT: Duration = Duration::from_secs(30);
// block_producer_time_nanos is the leader's wall clock when it built the block.
const MAX_PRODUCER_CLOCK_SKEW: Duration = Duration::from_secs(600);

fn check_footer_fields(footer: &SubscribeUpdateBlockFooter) -> Result<()> {
    ensure!(
        footer.bank_hash.len() == 32,
        "slot {}: bank_hash should be 32 bytes, got {}",
        footer.slot,
        footer.bank_hash.len()
    );
    ensure!(
        footer.bank_hash.iter().any(|byte| *byte != 0),
        "slot {}: bank_hash should not be all zeros",
        footer.slot
    );

    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock should be after the unix epoch")?
        .as_nanos() as u64;
    let skew = Duration::from_nanos(now_nanos.abs_diff(footer.block_producer_time_nanos));
    ensure!(
        skew <= MAX_PRODUCER_CLOCK_SKEW,
        "slot {}: block_producer_time_nanos {} is {skew:?} away from local time",
        footer.slot,
        footer.block_producer_time_nanos
    );
    Ok(())
}

/// Verifies block footers stream on their own filter and each one matches the block meta of the same bank.
#[test_helper(name = "block-footer", tags = ["block-footer", "alpenglow"])]
pub async fn block_footer_should_match_block_meta(config: &RunConfig) -> Result<()> {
    const TARGET_MATCHED: usize = 5;
    // Only Alpenglow blocks carry a footer. Give up after this many block metas.
    const MAX_BLOCK_META: usize = 150;

    let mut client = crate::grpc::new_client(config).await?;

    // Footers go out at processed only; they never join the block machine.
    let subscription = SubscribeRequest {
        block_footer: HashMap::from([(
            FILTER_NAME.to_string(),
            SubscribeRequestFilterBlockFooter {},
        )]),
        blocks_meta: HashMap::from([(
            FILTER_NAME.to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;

    // Keyed by (slot, bank_id): a slot can have more than one bank across forks.
    let mut footers: HashSet<(u64, u64)> = HashSet::new();
    let mut block_metas: HashSet<(u64, u64)> = HashSet::new();
    let mut matched = 0usize;

    while matched < TARGET_MATCHED {
        let update = timeout(UPDATE_TIMEOUT, stream.next())
            .await
            .with_context(|| format!("no update within {UPDATE_TIMEOUT:?}"))?
            .context("stream ended before enough footers matched")?
            .context("stream should yield updates without error")?;

        match update.update_oneof {
            Some(UpdateOneof::BlockFooter(footer)) => {
                ensure!(
                    update.filters == [FILTER_NAME],
                    "slot {}: footer should match only filter '{FILTER_NAME}', got {:?}",
                    footer.slot,
                    update.filters
                );
                check_footer_fields(&footer)?;

                let key = (footer.slot, footer.bank_id);
                ensure!(
                    footers.insert(key),
                    "received duplicate footer for slot {} bank_id {}",
                    footer.slot,
                    footer.bank_id
                );
                if block_metas.contains(&key) {
                    matched += 1;
                    log::info!(
                        "slot {} bank_id {}: footer matched {matched}/{TARGET_MATCHED}",
                        footer.slot,
                        footer.bank_id
                    );
                }
            }
            Some(UpdateOneof::BlockMeta(block_meta)) => {
                let key = (block_meta.slot, block_meta.bank_id);
                if block_metas.insert(key) && footers.contains(&key) {
                    matched += 1;
                    log::info!(
                        "slot {} bank_id {}: footer matched {matched}/{TARGET_MATCHED}",
                        block_meta.slot,
                        block_meta.bank_id
                    );
                }
                if block_metas.len() >= MAX_BLOCK_META && matched < TARGET_MATCHED {
                    if footers.is_empty() {
                        bail!(
                            "no block footer in {MAX_BLOCK_META} block metas; the cluster may not run Alpenglow"
                        );
                    }
                    bail!(
                        "only {matched}/{TARGET_MATCHED} footers matched a block meta on (slot, bank_id) after {MAX_BLOCK_META} block metas ({} footers received)",
                        footers.len()
                    );
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Verifies a block meta subscription never receives block footers.
#[test_helper(name = "block-footer-not-in-block-meta", tags = ["block-footer", "filters"])]
pub async fn block_meta_subscription_should_not_receive_footers(config: &RunConfig) -> Result<()> {
    const TARGET_BLOCK_META: usize = 20;

    let mut client = crate::grpc::new_client(config).await?;

    let subscription = SubscribeRequest {
        blocks_meta: HashMap::from([(
            FILTER_NAME.to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;

    let mut block_meta_count = 0usize;
    while block_meta_count < TARGET_BLOCK_META {
        let update = timeout(UPDATE_TIMEOUT, stream.next())
            .await
            .with_context(|| format!("no update within {UPDATE_TIMEOUT:?}"))?
            .context("stream ended before enough block metas arrived")?
            .context("stream should yield updates without error")?;

        match update.update_oneof {
            Some(UpdateOneof::BlockFooter(footer)) => bail!(
                "slot {}: block meta only subscription received a block footer",
                footer.slot
            ),
            Some(UpdateOneof::BlockMeta(_)) => block_meta_count += 1,
            _ => {}
        }
    }

    Ok(())
}
