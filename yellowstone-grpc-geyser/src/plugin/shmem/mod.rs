pub mod codec;
pub mod decoder;

use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use prost_types::Timestamp;
use tokio::sync::mpsc;
use yellowstone_shmem_client::ClientError;
use yellowstone_shmem_client::client::ShmemClient;
use yellowstone_shmem_plugin::plugin::YellowstonePlugin;

use crate::plugin::message::Message;

use self::codec::ProstGeyserCodec;
use self::decoder::ProstShmemDecoder;

/// Creates the shmem geyser plugin with the prost codec injected.
///
/// Called from `_create_plugin` — this is the only place in the
/// codebase that couples the plugin to a concrete codec.
pub fn create_plugin() -> YellowstonePlugin<ProstGeyserCodec> {
    YellowstonePlugin::new(ProstGeyserCodec)
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
    let waiter = ShmemClient::open(shmem_path, ProstShmemDecoder)?;  // second reader, just for wait_for_data
    let notify = Arc::new(tokio::sync::Notify::new());
    let notify_clone = Arc::clone(&notify);

    // bridge thread: blocks on futex, wakes async side
    std::thread::spawn(move || {
        loop {
            waiter.wait_for_data();
            notify_clone.notify_one();
        }
    });

    loop {
        notify.notified().await;
        let now = Timestamp::from(SystemTime::now()); // once per batch

        // drain everything available after wakeup
        loop {
            match client.try_recv() {
                None => break,
                Some(Ok(geyser_msg)) => match ProstShmemDecoder::to_dm_message(geyser_msg, now) {
                    Ok(dm_msg) => {
                        if messages_tx.send(dm_msg).is_err() {
                            return Ok(());
                        }
                    }
                    Err(e) => log::warn!("shmem decoder: {e}"),
                },
                Some(Err(ClientError::Lagged(n))) => {
                    log::warn!("shmem reader lagged: lost {n} entries");
                }
                Some(Err(e)) => {
                    log::error!("shmem reader error: {e}");
                    return Err(e);
                }
            }
        }
    }
}