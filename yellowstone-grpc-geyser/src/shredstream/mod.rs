

use futures::{Stream};
use log::{error, info};
use solana_entry::entry::Entry as SolanaEntry;
use crate::shredstream::shredstream::shredstream_proxy_client::ShredstreamProxyClient;
use crate::shredstream::shredstream::{Entry, SubscribeEntriesRequest};

pub mod shredstream {
    tonic::include_proto!("shredstream");
}
pub mod shared {
    tonic::include_proto!("shared");
}


pub async fn subscribe_shredstream_entries(
    endpoint: String,
) -> impl Stream<Item = Entry> {
    info!("Subscribing to shredstream transactions; endpoint={endpoint}");
    async_stream::stream! {
        loop {
            info!("Connecting to shredstream endpoint: {endpoint}");
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
            if let Err(e) = stream {
                error!("Error subscribing to shredstream at {endpoint}: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            let mut stream = stream.unwrap().into_inner();
            info!("Subscribed to shredstream endpoint: {endpoint}");

            loop {
                let slot_entry = stream.message().await;
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
                // let entries = match bincode::deserialize::<Vec<SolanaEntry>>(&slot_entry.entries) {
                //     Ok(e) => e,
                //     Err(e) => {
                //         error!("Deserialization failed with err: {e} (endpoint: {endpoint})");
                //         continue;
                //     }
                // };
                yield slot_entry
            }
        }
    }
}

