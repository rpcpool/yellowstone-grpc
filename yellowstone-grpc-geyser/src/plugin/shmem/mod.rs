pub mod codec;
pub mod decoder;

use std::path::Path;
use std::time::SystemTime;

use prost_types::Timestamp;
use tokio::sync::mpsc;

use yellowstone_shmem_client::client::ShmemClient;
use yellowstone_shmem_client::ClientError;
use yellowstone_shmem_common::GeyserMessage;
use yellowstone_shmem_plugin::plugin::YellowstoneShmemPlugin;

use crate::metrics;
use crate::plugin::message::Message;

use self::codec::ProstGeyserCodec;
use self::decoder::ProstShmemDecoder;

/// Creates the shmem geyser plugin with the prost codec injected.
///
/// Called from `_create_plugin` — this is the only place in the
/// codebase that couples the plugin to a concrete codec.
pub fn create_plugin() -> YellowstoneShmemPlugin<ProstGeyserCodec> {
    YellowstoneShmemPlugin::new(ProstGeyserCodec)
}

/// Spawns a task that polls the shmem ring and forwards decoded
/// [`Message`]s to `messages_tx` — the same channel `geyser_loop`
/// already reads from.
///
/// This replaces the crossbeam channel sender that the geyser plugin
/// callbacks previously wrote to directly.
pub async fn run_shmem_reader(
    shmem_path: &Path,
    messages_tx: mpsc::UnboundedSender<Message>,
) -> Result<(), ClientError> {
    let mut client = ShmemClient::open(shmem_path, ProstShmemDecoder)?;

    std::thread::spawn(move || loop {
        client.wait_for_data();
        loop {
            match client.try_recv() {
                None => break,
                Some(Ok(msg)) => {
                    let plugin_ts_ns = match &msg {
                        GeyserMessage::Account(a) => a.plugin_ts_ns,
                        _ => 0,
                    };
                    let ts = Timestamp::from(SystemTime::now());
                    match ProstShmemDecoder::to_dm_message(msg, ts) {
                        Ok(dm) => {
                            if plugin_ts_ns > 0 {
                                let latency = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos() as i64
                                    - plugin_ts_ns;
                                metrics::observe_shmem_handover_latency_ns(latency);
                            }
                            if messages_tx.send(dm).is_err() {
                                return;
                            }
                        }
                        Err(e) => log::warn!("shmem decode: {e}"),
                    }
                }
                Some(Err(ClientError::MidWrite)) => continue,
                Some(Err(ClientError::Lagged(n))) => {
                    log::warn!("shmem reader lagged: lost {n} entries");
                }
                Some(Err(ClientError::Decode(e))) => {
                    log::warn!("shmem reader decode error (skipping): {e}");
                }
                Some(Err(ClientError::Io(e))) => {
                    log::error!("shmem reader io error: {e}");
                    return;
                }
            }
        }
    });

    Ok(())
}