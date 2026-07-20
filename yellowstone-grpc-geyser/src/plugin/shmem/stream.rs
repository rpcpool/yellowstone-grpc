use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use prost_types::Timestamp;
use yellowstone_shmem_client::{ClientError, ShmemSource, SourceItem};

use crate::plugin::message::Message;
use crate::plugin::shmem::{ProstShmemDecoder, ShmemHealthReporter};
use crate::stream::BatchStream;

/// Adapts any `ShmemSource` (live ring or snapshot ring) to a
/// `BatchStream<Item = Message>`. Drains the source, converts each
/// `GeyserMessage` to a dragons-mouth `Message`, batches up to the
/// vec's capacity, and parks on the futex when the source is empty.
///
/// Snapshot sources yield `SourceItem::Complete` at the sentinel,
/// which closes the stream. Live sources never yield `Complete`.
pub struct ShmemBatchStream<S: ShmemSource> {
    inner: S,
    health: ShmemHealthReporter,
    wait_in_flight: bool,
}

impl<S: ShmemSource> ShmemBatchStream<S> {
    pub fn new(inner: S, health_interval_secs: Duration) -> Self {
        Self {
            inner,
            health: ShmemHealthReporter::new(health_interval_secs),
            wait_in_flight: false,
        }
    }
}

impl<S: ShmemSource + Unpin> BatchStream for ShmemBatchStream<S> {
    type Item = Message;

    fn poll_recv_batch(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch: &mut Vec<Self::Item>,
    ) -> Poll<Option<usize>> {
        let this = self.get_mut();
        this.wait_in_flight = false;
        let mut count = 0;

        let batch_timestamp = Timestamp::from(SystemTime::now());
        while batch.len() < batch.capacity() {
            match this.inner.try_recv() {
                Some(Ok(SourceItem::Message(gm))) => {
                    match ProstShmemDecoder::to_dm_message(gm, batch_timestamp.clone()) {
                        Ok(msg) => {
                            this.health.observe(&msg);
                            batch.push(msg);
                            count += 1;
                        }
                        Err(e) => {
                            log::error!("conversion error: {e}");
                        }
                    }
                }
                Some(Ok(SourceItem::Complete)) => {
                    if count > 0 {
                        return Poll::Ready(Some(count));
                    }
                    return Poll::Ready(None);
                }
                Some(Err(ClientError::Lagged(n))) => {
                    this.health.observe_lagged(n);
                    log::warn!("shmem reader lagged, lost {n} entries");
                }
                Some(Err(ClientError::MidWrite)) => continue,
                Some(Err(e)) => {
                    log::error!("shmem read error: {e}");
                }
                None => break,
            }
        }

        if count > 0 {
            return Poll::Ready(Some(count));
        }

        match this.inner.check_region() {
            Ok(true) => {}
            Ok(false) => {
                // Producer recreated the ring file. Our mapping is stale;
                // recovery requires reopening from the current generation.
                // Panicking is intentional: no in-loop recovery is possible.
                panic!("shmem: region was re-created, consumer must rejoin");
            }
            Err(e) => {
                log::error!("shmem: check_region failed: {e}");
            }
        }

        if !this.wait_in_flight {
            this.wait_in_flight = true;
            let waker = cx.waker().clone();
            let wait_handle = this.inner.wait_handle();
            tokio::task::spawn_blocking(move || {
                wait_handle.wait_for_data();
                waker.wake();
            });
        }

        Poll::Pending
    }
}
