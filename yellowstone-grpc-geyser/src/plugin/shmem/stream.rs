use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use prost_types::Timestamp;
use yellowstone_shmem_client::{ClientError, Event, ShmemSource};

use crate::plugin::message::Message;
use crate::plugin::shmem::{ProstShmemDecoder, ShmemHealthReporter};
use crate::stream::BatchStream;

/// Adapts any `ShmemSource` (live ring or snapshot ring) to a
/// `BatchStream<Item = Message>`. Drains the source, converts each
/// `GeyserMessage` to a dragons-mouth `Message`, batches up to the
/// vec's capacity, and parks on the futex when the source is empty.
///
/// Live streams pass a `restart_check` closure that reads the ring
/// file's current generation and returns `Ok(false)` if the producer
/// recreated it. Snapshot streams pass `None` -- snapshot rings are
/// single-shot, no restart semantics.
pub struct ShmemBatchStream<S, F = fn() -> Result<bool, std::io::Error>> {
    inner: S,
    health: ShmemHealthReporter,
    wait_in_flight: bool,
    restart_check: Option<F>,
    check_interval: Option<tokio::time::Interval>,
}

impl<S: ShmemSource> ShmemBatchStream<S, fn() -> Result<bool, std::io::Error>> {
    /// Snapshot construction: no restart detection.
    pub fn new(inner: S, health_interval: Duration) -> Self {
        Self {
            inner,
            health: ShmemHealthReporter::new(health_interval),
            wait_in_flight: false,
            restart_check: None,
            check_interval: None,
        }
    }
}

impl<S: ShmemSource, F> ShmemBatchStream<S, F>
where
    F: Fn() -> Result<bool, std::io::Error>,
{
    /// Live construction: `restart_check` is called on `check_interval`;
    /// `Ok(false)` panics (ring was recreated, mapping is stale).
    pub fn with_restart_check(
        inner: S,
        health_interval: Duration,
        restart_check: F,
        check_interval: Duration,
    ) -> Self {
        Self {
            inner,
            health: ShmemHealthReporter::new(health_interval),
            wait_in_flight: false,
            restart_check: Some(restart_check),
            check_interval: Some(tokio::time::interval(check_interval)),
        }
    }
}

impl<S, F> BatchStream for ShmemBatchStream<S, F>
where
    S: ShmemSource + Unpin,
    F: Fn() -> Result<bool, std::io::Error> + Unpin,
{
    type Item = Message;

    fn poll_recv_batch(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch: &mut Vec<Self::Item>,
    ) -> Poll<Option<usize>> {
        let this = self.get_mut();
        this.wait_in_flight = false;
        let mut count = 0;

        // Periodic restart check (live only). Interval ticks in seconds --
        // cost is negligible compared to per-batch drain.
        if let (Some(check), Some(interval)) =
            (this.restart_check.as_ref(), this.check_interval.as_mut())
        {
            if interval.poll_tick(cx).is_ready() {
                match check() {
                    Ok(true) => {}
                    Ok(false) => {
                        // Producer recreated the ring file. Our mapping is stale;
                        // recovery requires reopening from the current generation.
                        // Panicking is intentional: no in-loop recovery is possible.
                        panic!("shmem: region was re-created, consumer must rejoin");
                    }
                    Err(e) => {
                        log::error!("shmem: restart check failed: {e}");
                    }
                }
            }
        }

        let batch_timestamp = Timestamp::from(SystemTime::now());
        while batch.len() < batch.capacity() {
            match this.inner.try_recv() {
                Some(Ok(Event::Message(gm))) => {
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
                Some(Ok(Event::End)) => {
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
