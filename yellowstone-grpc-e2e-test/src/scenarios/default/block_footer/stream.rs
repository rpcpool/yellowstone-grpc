use {
    super::{check_footer_fields, footer_and_block_meta_request, FILTER_NAME, UPDATE_TIMEOUT},
    crate::scenarios::RunConfig,
    anyhow::{bail, ensure, Context, Result},
    std::collections::HashSet,
    tokio::time::timeout,
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::{subscribe_update::UpdateOneof, CommitmentLevel},
};

/// Verifies block footers stream on their own filter and each one comes before the block meta of the same bank.
#[test_helper(name = "block-footer", tags = ["block-footer"])]
pub async fn block_footer_should_match_block_meta(config: &RunConfig) -> Result<()> {
    footers_should_precede_block_meta(config, CommitmentLevel::Processed).await
}

/// Verifies confirmed subscribers receive block footers, each before the block meta of the same bank.
#[test_helper(name = "block-footer-confirmed", tags = ["block-footer"])]
pub async fn block_footer_should_match_block_meta_at_confirmed(config: &RunConfig) -> Result<()> {
    footers_should_precede_block_meta(config, CommitmentLevel::Confirmed).await
}

async fn footers_should_precede_block_meta(
    config: &RunConfig,
    commitment: CommitmentLevel,
) -> Result<()> {
    const TARGET_MATCHED: usize = 5;
    // Give up after this many block metas without a footer.
    const MAX_BLOCK_META: usize = 150;

    let mut client = crate::grpc::new_client(config).await?;
    let subscription = footer_and_block_meta_request(commitment, None);

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
                ensure!(
                    !block_metas.contains(&key),
                    "slot {} bank_id {}: footer arrived after its block meta",
                    footer.slot,
                    footer.bank_id
                );
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
                        bail!("no block footer in {MAX_BLOCK_META} block metas");
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
