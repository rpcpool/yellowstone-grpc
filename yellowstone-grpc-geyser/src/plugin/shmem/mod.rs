pub mod codec;
pub mod decoder;

use std::path::Path;

use tokio::sync::mpsc;
use yellowstone_shmem_client::client::ShmemClient;
use yellowstone_shmem_plugin::plugin::YellowstonePlugin;

use crate::plugin::message::Message;

use self::codec::ProstGeyserCodec;
use self::decoder::ProstShmemDecoder;

/// Creates the shmem geyser plugin with the prost codec injected.
///
/// Called from `_create_plugin` — this is the only place in the
/// codebase that couples the plugin to a concrete codec.
pub fn create_plugin() -> YellowstonePlugin {
    YellowstonePlugin::new(Box::new(ProstGeyserCodec))
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
) -> Result<(), yellowstone_shmem_client::client::ClientError> {
    let mut client = ShmemClient::open(shmem_path, Box::new(ProstShmemDecoder))?;

    loop {
        match client.try_recv() {
            None => {
                tokio::task::yield_now().await;
            }
            Some(Ok(geyser_msg)) => {
                match ProstShmemDecoder::to_dm_message(geyser_msg) {
                    Ok(dm_msg) => {
                        if messages_tx.send(dm_msg).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        log::warn!("shmem decoder: {e}");
                    }
                }
            }
            Some(Err(yellowstone_shmem_client::client::ClientError::Lagged(n))) => {
                log::warn!("shmem reader lagged: lost {n} entries");
            }
            Some(Err(e)) => {
                log::error!("shmem reader error: {e}");
                break;
            }
        }
    }

    Ok(())
}
