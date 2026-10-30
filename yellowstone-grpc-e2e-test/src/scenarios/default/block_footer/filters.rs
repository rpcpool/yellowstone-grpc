use {
    super::{FILTER_NAME, UPDATE_TIMEOUT},
    crate::scenarios::RunConfig,
    anyhow::{bail, Context, Result},
    std::collections::HashMap,
    tokio::time::timeout,
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterBlocksMeta,
    },
};

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
