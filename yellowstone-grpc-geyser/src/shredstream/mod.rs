

use std::sync::Arc;

use log::{error, info};
use solana_entry::entry::Entry as SolanaEntry;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use yellowstone_grpc_proto::plugin::preprocessed::PreprocessedEntries;
use crate::shredstream::shredstream::shredstream_proxy_client::ShredstreamProxyClient;
use crate::shredstream::shredstream::SubscribeEntriesRequest;

pub mod shredstream {
    tonic::include_proto!("shredstream");
}
pub mod shared {
    tonic::include_proto!("shared");
}


pub fn subscribe_shredstream_entries(
    endpoint: String,
    capacity: usize,
    task_tracker: TaskTracker,
    client_cancellation_token: CancellationToken,
) -> Arc<broadcast::Sender<Arc<PreprocessedEntries>>> {
    info!("Subscribing to shredstream transactions; endpoint={endpoint}");
    let (tx, _rx) = broadcast::channel(capacity);
    let tx = Arc::new(tx);
    task_tracker.spawn({
        let tx = tx.clone();
        async move {
        loop {
            if client_cancellation_token.is_cancelled() {
                break;
            }
            let client = ShredstreamProxyClient::connect(endpoint.to_string()).await;
            if let Err(e) = client {
                error!("Error connecting to shredstream at {endpoint}: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            let mut client = client.unwrap();
            let stream = client
                .subscribe_entries(SubscribeEntriesRequest {})
                .await;

            if client_cancellation_token.is_cancelled() {
                break;
            }

            if let Err(e) = stream {
                error!("Error subscribing to shredstream at {endpoint}: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            let mut stream = stream.unwrap();

            loop {
                let slot_entry = stream.get_mut().message().await;
                if client_cancellation_token.is_cancelled() {
                    break;
                }
                if let Err(e) = slot_entry {
                    error!("Stream error on {endpoint}: {e}, switching to next endpoint");
                    break;
                }
                let slot_entry = slot_entry.unwrap();
                if slot_entry.is_none() {
                    error!("No slot entry from {endpoint}, switching to next endpoint");
                    break;
                }
                let slot_entry = slot_entry.unwrap();
                let entries = match bincode::deserialize::<Vec<SolanaEntry>>(&slot_entry.entries) {
                    Ok(e) => e,
                    Err(e) => {
                        error!("Deserialization failed with err: {e} (endpoint: {endpoint})");
                        continue;
                    }
                };
                let _ = tx.send(Arc::new(PreprocessedEntries::new(slot_entry.slot, entries)));
            }
        }
    }});
    tx
}
