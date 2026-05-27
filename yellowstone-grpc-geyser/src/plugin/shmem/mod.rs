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


#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_conduit::{EventWriter, ShmemRegion};
    use yellowstone_shmem_plugin::codec::GeyserCodec;

    const TEST_SHMEM: &str = "/tmp/shmem-reader-test";

    fn make_writer() -> EventWriter {
        let _ = std::fs::remove_file(TEST_SHMEM);
        let region = ShmemRegion::create(
            std::path::Path::new(TEST_SHMEM),
            1 << 14,
            64 * 1024 * 1024,
        )
        .unwrap();
        EventWriter::new(region)
    }

    fn write_slot_event(writer: &EventWriter, slot: u64) {
        use yellowstone_shmem_common::{GeyserMessage, MessageSlot, SlotStatus};
        let msg = GeyserMessage::Slot(MessageSlot {
            slot,
            parent: Some(slot - 1),
            status: SlotStatus::Processed,
        });
        let codec = ProstGeyserCodec;
        let size = codec.encoded_size(&msg);
        let (offset, len) = writer.claim_mcache(size);
        unsafe {
            let buf = std::slice::from_raw_parts_mut(
                writer.region_mcache_ptr(offset as usize),
                size,
            );
            codec.encode_into(&msg, buf);
        }
        writer.write_event(msg.slot(), msg.event_type() as u8, offset, len);
    }

   #[tokio::test]
    async fn test_run_shmem_reader_receives_messages() {
        let writer = make_writer();
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
        let path = std::path::PathBuf::from(TEST_SHMEM);

        // Start reader first.
        tokio::spawn(async move {
            let _ = run_shmem_reader(&path, tx).await;
        });

        // Yield to let the reader task start and reach notified().await.
        tokio::task::yield_now().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Now write events — bridge thread is running, will wake reader.
        for i in 1..=10u64 {
            write_slot_event(&writer, i);
        }

        // Expect messages within 2 seconds.
        let mut received = 0usize;
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);

        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(
                tokio::time::Duration::from_millis(100),
                rx.recv(),
            )
            .await
            {
                Ok(Some(_)) => {
                    received += 1;
                    if received >= 5 {
                        break;
                    }
                }
                _ => {}
            }
        }

        assert!(received > 0, "run_shmem_reader received no messages");
    }
}