use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use once_cell::sync::Lazy;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::time::{interval, sleep};

pub const HEALTH_CHECK_SLOT_DISTANCE: u64 = 100;

pub static LATEST_SLOT: Lazy<Arc<AtomicU64>> = Lazy::new(|| Arc::new(AtomicU64::new(0)));

pub async fn fetch_current_slot_with_infinite_retry(client: &RpcClient) -> u64 {
    loop {
        match client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await
        {
            Ok(slot) => {
                return slot;
            }
            Err(e) => {
                log::error!("Failed to fetch current slot: {}", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

pub async fn update_latest_slot(rpc_client: &RpcClient) {
    let slot = fetch_current_slot_with_infinite_retry(rpc_client).await;
    LATEST_SLOT.fetch_max(slot, Ordering::SeqCst);
}

pub async fn update_latest_slot_loop(rpc_client: RpcClient) {
    let mut interval = interval(Duration::from_millis(100));
    loop {
        interval.tick().await;
        update_latest_slot(&rpc_client).await;
    }
}
