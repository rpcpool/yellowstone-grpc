use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use prost_types::Timestamp;
use yellowstone_shmem_client::{ClientError, Event, ShmemSource};

use crate::metrics;
use crate::plugin::message::Message;
use crate::plugin::shmem::{ProstShmemDecoder, ShmemHealthReporter};
use crate::stream::BatchStream;

/// Adapts any `ShmemSource` (live ring or snapshot ring) to a
/// `BatchStream<Item = Message>`. Drains the source, converts each
/// `GeyserMessage` to a dragons-mouth `Message`, batches up to the vec's
/// capacity, and parks on the futex when the source is empty.
///
/// Live streams pass a `region_is_current` closure that compares the ring
/// file's generation stamp against the one captured at open. It returns
/// `Ok(false)` once the producer has replaced the file, which means our
/// mapping points at bytes nobody writes to any more. Snapshot streams pass
/// `None`: a snapshot ring is single-shot and is never replaced under a
/// reader.
///
/// Both timers are created on the first poll rather than at construction.
/// A tokio `Interval` needs a live runtime, and these streams are built
/// before the runtime starts.
pub struct ShmemBatchStream<S, F = fn() -> Result<bool, std::io::Error>> {
    inner: S,
    health: ShmemHealthReporter,
    wait_in_flight: bool,
    region_is_current: Option<F>,
    /// How often to compare generations. `None` for snapshot streams.
    generation_check_period: Option<Duration>,
    generation_timer: Option<tokio::time::Interval>,
    health_timer: Option<tokio::time::Interval>,
    last_poll: Option<Instant>,
    last_poll_had_data: bool,
}

impl<S: ShmemSource> ShmemBatchStream<S, fn() -> Result<bool, std::io::Error>> {
    /// Snapshot construction: the region is never replaced, so no generation
    /// checking.
    pub fn new(inner: S, health_period: Duration) -> Self {
        Self {
            inner,
            health: ShmemHealthReporter::new(health_period),
            wait_in_flight: false,
            region_is_current: None,
            generation_check_period: None,
            generation_timer: None,
            health_timer: None,
            last_poll: None,
            last_poll_had_data: false,
        }
    }
}

impl<S: ShmemSource, F> ShmemBatchStream<S, F>
where
    F: Fn() -> Result<bool, std::io::Error>,
{
    /// Live construction. `region_is_current` is polled every
    /// `generation_check_period`; a `false` result panics, because a replaced
    /// region cannot be recovered from inside the loop.
    pub fn with_generation_check(
        inner: S,
        health_period: Duration,
        region_is_current: F,
        generation_check_period: Duration,
    ) -> Self {
        Self {
            inner,
            health: ShmemHealthReporter::new(health_period),
            wait_in_flight: false,
            region_is_current: Some(region_is_current),
            generation_check_period: Some(generation_check_period),
            generation_timer: None,
            health_timer: None,
            last_poll: None,
            last_poll_had_data: false,
        }
    }
}

impl<S, F> ShmemBatchStream<S, F>
where
    S: ShmemSource + Unpin,
    F: Fn() -> Result<bool, std::io::Error> + Unpin,
{
    /// Creates the timers on first poll, where a runtime is guaranteed.
    fn start_timers(&mut self) {
        if self.generation_timer.is_none() {
            if let Some(period) = self.generation_check_period {
                self.generation_timer = Some(tokio::time::interval(period));
            }
        }
        if self.health_timer.is_none() && self.health.enabled() {
            self.health_timer = Some(tokio::time::interval(self.health.period()));
        }
    }

    /// Panics if the producer has replaced the region since we opened it.
    fn check_generation(&mut self, cx: &mut Context<'_>) {
        let (Some(is_current), Some(timer)) = (
            self.region_is_current.as_ref(),
            self.generation_timer.as_mut(),
        ) else {
            return;
        };
        if !timer.poll_tick(cx).is_ready() {
            return;
        }
        match is_current() {
            Ok(true) => {}
            Ok(false) => {
                // The file was recreated, so our mapping is stale and nothing
                // will ever be written to it again. Rejoining means reopening
                // at the new generation, which this loop cannot do.
                panic!("shmem: region was re-created, consumer must rejoin");
            }
            Err(e) => log::error!("shmem: generation check failed: {e}"),
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

        let now = Instant::now();
        if let Some(prev) = this.last_poll.replace(now) {
            if this.last_poll_had_data {
                metrics::shmem_poll_interval_observe(now - prev);
            }
        }

        metrics::shmem_gap_observe(this.inner.gap());

        this.wait_in_flight = false;

        let mut count = 0;

        this.start_timers();
        this.check_generation(cx);

        if let Some(timer) = this.health_timer.as_mut() {
            if timer.poll_tick(cx).is_ready() {
                this.health.report();
            }
        }

        let batch_timestamp = Timestamp::from(SystemTime::now());
        while batch.len() < batch.capacity() {
            match this.inner.try_recv() {
                Some(Ok(Event::Message(gm))) => {
                    let started = Instant::now();
                    match ProstShmemDecoder::to_dm_message(gm, batch_timestamp.clone()) {
                        Ok(msg) => {
                            metrics::shmem_convert_observe(started.elapsed());
                            this.health.observe(&msg);
                            batch.push(msg);
                            count += 1;
                        }
                        Err(e) => log::error!("conversion error: {e}"),
                    }
                }
                Some(Ok(Event::End)) => {
                    if count > 0 {
                        this.last_poll_had_data = true;
                        return Poll::Ready(Some(count));
                    }
                    return Poll::Ready(None);
                }
                Some(Err(ClientError::Lagged(n))) => {
                    this.health.observe_lagged(n);
                    metrics::shmem_lagged_observed(n);
                    panic!("shmem reader lagged, lost {n} entries"); // crash on lag
                }
                Some(Err(e)) => log::error!("shmem read error: {e}"),
                None => break,
            }
        }

        if count > 0 {
            this.last_poll_had_data = true;
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
        this.last_poll_had_data = false;

        Poll::Pending
    }
}
