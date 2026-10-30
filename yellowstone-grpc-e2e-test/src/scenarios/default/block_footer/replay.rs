use {
    super::{check_footer_fields, footer_and_block_meta_request, UPDATE_TIMEOUT},
    crate::scenarios::RunConfig,
    anyhow::{ensure, Context, Result},
    std::collections::HashSet,
    tokio::time::timeout,
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::{subscribe_update::UpdateOneof, CommitmentLevel},
};

/// Verifies `from_slot` replay sends each bank's block footer before its block meta.
#[test_helper(name = "block-footer-replay", tags = ["block-footer", "replay"])]
pub async fn block_footer_should_replay_before_block_meta(config: &RunConfig) -> Result<()> {
    const REPLAY_DEPTH: u64 = 10;
    // Slots skipped by their leader have no block, so require only some replayed banks.
    const MIN_REPLAYED_BANKS: usize = 3;

    let mut client = crate::grpc::new_client(config).await?;
    let tip = client.get_slot(None).await.context("get_slot")?.slot;
    let from_slot = tip.saturating_sub(REPLAY_DEPTH);
    log::info!("current tip slot is {tip}, replaying from slot {from_slot}");

    let mut stream = client
        .subscribe_once(footer_and_block_meta_request(
            CommitmentLevel::Processed,
            Some(from_slot),
        ))
        .await
        .context("subscription should succeed")?;

    let mut footers: HashSet<(u64, u64)> = HashSet::new();
    let mut replayed_banks = 0usize;
    loop {
        let update = timeout(UPDATE_TIMEOUT, stream.next())
            .await
            .with_context(|| format!("no update within {UPDATE_TIMEOUT:?}"))?
            .context("stream ended before replay caught up with the tip")?
            .context("stream should yield updates without error")?;

        match update.update_oneof {
            Some(UpdateOneof::BlockFooter(footer)) => {
                check_footer_fields(&footer)?;
                ensure!(
                    footers.insert((footer.slot, footer.bank_id)),
                    "received duplicate footer for slot {} bank_id {}",
                    footer.slot,
                    footer.bank_id
                );
            }
            Some(UpdateOneof::BlockMeta(block_meta)) => {
                if block_meta.slot >= tip {
                    break;
                }
                ensure!(
                    block_meta.slot >= from_slot,
                    "slot {}: replay sent a block meta before from_slot {from_slot}",
                    block_meta.slot
                );
                ensure!(
                    footers.contains(&(block_meta.slot, block_meta.bank_id)),
                    "slot {} bank_id {}: replayed block meta arrived without a footer before it",
                    block_meta.slot,
                    block_meta.bank_id
                );
                replayed_banks += 1;
            }
            _ => {}
        }
    }

    ensure!(
        replayed_banks >= MIN_REPLAYED_BANKS,
        "only {replayed_banks} banks replayed between slot {from_slot} and tip {tip}, expected at least {MIN_REPLAYED_BANKS}"
    );
    Ok(())
}
