use {
    crate::scenarios::RunConfig,
    anyhow::{ensure, Context, Result},
    std::{
        collections::{HashMap, HashSet},
        time::Duration,
    },
    tokio::time::timeout,
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SlotStatus, SubscribeRequest,
        SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
    },
};

const FILTER_NAME: &str = "test";
// Blocks land every ~400ms, so a gap this long means the stream is stuck.
const UPDATE_TIMEOUT: Duration = Duration::from_secs(30);

/// Verifies `from_slot` replay sends each bank's CreatedBank before its entries and block meta.
#[test_helper(name = "replay-created-bank", tags = ["replay", "slots"])]
pub async fn replay_should_send_created_bank_before_bank_data(config: &RunConfig) -> Result<()> {
    const REPLAY_DEPTH: u64 = 10;
    // Slots skipped by their leader have no block, so require only some replayed banks.
    const MIN_REPLAYED_BANKS: usize = 3;

    let mut client = crate::grpc::new_client(config).await?;
    let tip = client.get_slot(None).await.context("get_slot")?.slot;
    let from_slot = tip.saturating_sub(REPLAY_DEPTH);
    log::info!("current tip slot is {tip}, replaying from slot {from_slot}");

    let mut stream = client
        .subscribe_once(SubscribeRequest {
            slots: HashMap::from([(
                FILTER_NAME.to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false),
                    interslot_updates: Some(true),
                },
            )]),
            entry: HashMap::from([(
                FILTER_NAME.to_string(),
                SubscribeRequestFilterEntry::default(),
            )]),
            blocks_meta: HashMap::from([(
                FILTER_NAME.to_string(),
                SubscribeRequestFilterBlocksMeta::default(),
            )]),
            commitment: Some(CommitmentLevel::Processed as i32),
            from_slot: Some(from_slot),
            ..Default::default()
        })
        .await
        .context("subscription should succeed")?;

    // Keyed by (slot, bank_id): a slot can have more than one bank across forks.
    let mut created_banks: HashSet<(u64, u64)> = HashSet::new();
    let mut replayed_banks = 0usize;
    loop {
        let update = timeout(UPDATE_TIMEOUT, stream.next())
            .await
            .with_context(|| format!("no update within {UPDATE_TIMEOUT:?}"))?
            .context("stream ended before replay caught up with the tip")?
            .context("stream should yield updates without error")?;

        match update.update_oneof {
            Some(UpdateOneof::Slot(slot)) if slot.status() == SlotStatus::SlotCreatedBank => {
                let bank_id = slot
                    .bank_id
                    .with_context(|| format!("slot {}: CreatedBank has no bank_id", slot.slot))?;
                created_banks.insert((slot.slot, bank_id));
            }
            Some(UpdateOneof::Entry(entry)) if entry.slot < tip => {
                ensure!(
                    created_banks.contains(&(entry.slot, entry.bank_id)),
                    "slot {} bank_id {}: replayed entry arrived without a CreatedBank before it",
                    entry.slot,
                    entry.bank_id
                );
            }
            Some(UpdateOneof::BlockMeta(block_meta)) => {
                if block_meta.slot >= tip {
                    break;
                }
                ensure!(
                    created_banks.contains(&(block_meta.slot, block_meta.bank_id)),
                    "slot {} bank_id {}: replayed block meta arrived without a CreatedBank before it",
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
