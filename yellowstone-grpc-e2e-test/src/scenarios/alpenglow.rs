//! Scenarios that need an Alpenglow cluster: block footers only exist there.
//! They run only with `--module alpenglow`.

use {
    anyhow::{ensure, Context, Result},
    std::{
        collections::HashMap,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlockFooter,
        SubscribeRequestFilterBlocksMeta, SubscribeUpdateBlockFooter,
    },
};

pub mod block_footer;
pub mod filters;
pub mod replay;

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

fn footer_and_block_meta_request(
    commitment: CommitmentLevel,
    from_slot: Option<u64>,
) -> SubscribeRequest {
    SubscribeRequest {
        block_footer: HashMap::from([(
            FILTER_NAME.to_string(),
            SubscribeRequestFilterBlockFooter::default(),
        )]),
        blocks_meta: HashMap::from([(
            FILTER_NAME.to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]),
        commitment: Some(commitment as i32),
        from_slot,
        ..Default::default()
    }
}
