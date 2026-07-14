use {
    crate::{grpc::E2EGeyserEventAdapter, scenarios::RunConfig},
    anyhow::ensure,
    futures::StreamExt,
    solana_commitment_config::CommitmentLevel,
    solana_signature::Signature,
    std::{
        collections::{HashMap, HashSet},
        str::FromStr,
    },
    yellowstone_block_machine::stream::{
        Block, BlockEventStore, BlockMachineOutput, BlockStream, SimpleBlockAccumulator,
        SimpleBlockStore,
    },
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterSlots,
        SubscribeUpdate,
    },
};

fn ensure_all_account_txn_sig_exist(
    block: &Block<SimpleBlockStore<SubscribeUpdate>>,
) -> anyhow::Result<()> {
    let mut account_txn_sig_set: HashSet<Signature> = HashSet::new();
    let mut txn_sig_index_map: HashMap<Signature, u64> = HashMap::new();
    for ev in block.events.iter() {
        let Some(update) = ev.update_oneof.as_ref() else {
            continue;
        };
        match update {
            UpdateOneof::Account(subscribe_update_account) => {
                let Some(sig) = subscribe_update_account
                    .account
                    .as_ref()
                    .and_then(|a| a.txn_signature.as_ref())
                else {
                    continue;
                };
                let sig = Signature::try_from(sig.as_slice()).expect("signature");
                account_txn_sig_set.insert(sig);
            }
            UpdateOneof::Transaction(subscribe_update_transaction) => {
                let Some(txn) = subscribe_update_transaction.transaction.as_ref() else {
                    continue;
                };
                let sig = Signature::try_from(txn.signature.as_slice()).expect("signature");
                txn_sig_index_map.insert(sig, txn.index);
            }
            _ => {}
        }
    }

    for sig in account_txn_sig_set {
        if !txn_sig_index_map.contains_key(&sig) {
            ensure!(
                txn_sig_index_map.contains_key(&sig),
                "Missing txn for account update sig: {}",
                sig
            );
        }
    }
    Ok(())
}

/// Scenario: Block Machine
#[test_helper(name = "blockmachine")]
pub async fn blockmachine_scenario(run_config: &RunConfig) -> anyhow::Result<()> {
    let mut client = crate::grpc::new_client(run_config).await?;
    let block_st_req = SubscribeRequest {
        entry: HashMap::from([("filter".to_string(), Default::default())]),
        accounts: HashMap::from([("filter".to_string(), Default::default())]),
        transactions: HashMap::from([("filter".to_string(), Default::default())]),
        blocks_meta: HashMap::from([("filter".to_string(), Default::default())]),
        slots: HashMap::from([(
            "filter".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: None,
                interslot_updates: Some(true),
            },
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };
    let blockmeta_st_req = SubscribeRequest {
        blocks_meta: HashMap::from([("filter".to_string(), Default::default())]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };
    let st = Box::pin(client.subscribe_once(block_st_req).await?);
    let mut block_meta_st = client.subscribe_once(blockmeta_st_req).await?;
    let mut blockstream = BlockStream::<_, E2EGeyserEventAdapter, SimpleBlockAccumulator<_>>::new(
        st,
        SimpleBlockAccumulator::default(),
        CommitmentLevel::Processed,
    );

    const MAX_BLOCKS: usize = 10;
    let mut block_match = 0;
    let mut blockmeta_map = HashMap::new();
    let mut block_map = HashMap::new();
    while block_match < MAX_BLOCKS {
        tokio::select! {
            block_meta_output = block_meta_st.next() => {
                let result = block_meta_output.ok_or_else(|| anyhow::anyhow!("Block meta stream ended unexpectedly"))?;
                let Some(update) = result?.update_oneof else {
                    continue;
                };
                if let UpdateOneof::BlockMeta(block_meta) = update {
                    let slot = block_meta.slot;
                    log::info!("Received block meta for slot {}", slot);
                    blockmeta_map.insert(block_meta.slot, block_meta);
                    if block_map.contains_key(&slot) {
                        block_match += 1;
                    }
                }
            },
            maybe = blockstream.next() => {
                let block_output = maybe.ok_or_else(|| anyhow::anyhow!("Block stream ended unexpectedly"))??;
                match block_output {
                    BlockMachineOutput::FrozenBlock(block) => {
                        let slot = block.slot;
                        log::info!("Received frozen block for slot {}", slot);
                        block_map.insert(slot, block);
                        if blockmeta_map.contains_key(&slot) {
                            block_match += 1;
                        }
                    },
                    _ => continue,
                };
            }
        }
    }

    for (slot, block) in block_map {
        let Some(block_meta) = blockmeta_map.remove(&slot) else {
            continue;
        };
        ensure_all_account_txn_sig_exist(&block)?;
        let blockmeta_blockhash = solana_hash::Hash::from_str(&block_meta.blockhash)?.to_bytes();
        ensure!(
            block.blockhash == blockmeta_blockhash,
            "Block hash in block meta does not match block hash in block"
        );
        ensure!(block.events.account_len() > 0, "Block has no accounts");
        ensure!(
            block_meta.slot == block.slot,
            "Block meta slot does not match block slot"
        );
        ensure!(
            block_meta.executed_transaction_count as usize == block.events.transaction_len(),
            "Block meta executed transaction count does not match block transaction count"
        );
        ensure!(
            block_meta.entries_count as usize == block.events.entry_len(),
            "Block meta entries count does not match block entries count"
        );
    }
    Ok(())
}
