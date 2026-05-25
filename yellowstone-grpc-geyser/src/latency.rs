//! Hot-path latency instrumentation.
//!
//! Records latency/queue samples into lock-free atomic histograms and, on a
//! fixed interval, emits one JSON line per metric to the `yellowstone_latency`
//! log target. The emitted lines are meant to be captured from a production
//! machine and rendered offline (e.g. into an HTML report).
//!
//! Recording is designed to be cheap enough to live on the per-message path:
//! one `partition_point` over a small static slice plus a handful of relaxed
//! atomic ops. When disabled (`interval == 0` in config) every `record_*`
//! short-circuits on a single relaxed bool load.
//!
//! Four measure points are captured (matching the four hot-path costs):
//!   - `end_to_end`  (microseconds): geyser ingestion (`created_at`) -> just
//!     before the update is handed to the client stream.
//!   - `producer_send` (microseconds): time spent in a single broadcast send
//!     on the `geyser_loop` critical path (the O(N) wake-all cost).
//!   - `filter_encode` (microseconds): per-message `Filter::get_updates`
//!     (filter match + `FilteredUpdate` construction) on the per-subscriber path.
//!   - `client_queue_depth` (items): depth of a subscriber's outbound queue,
//!     sampled each client-loop iteration (per-client backpressure / queue wait).

use {
    log::info,
    prost_types::Timestamp,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

/// Log-spaced upper bounds (inclusive) for the time histograms, in microseconds.
/// Covers ~1us to ~10s; anything larger lands in the overflow bucket.
const TIME_BUCKETS_US: &[u64] = &[
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000,
    200_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000,
];

/// Upper bounds (inclusive) for the queue-depth histogram, in queued items.
const DEPTH_BUCKETS: &[u64] = &[
    0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 4_096, 16_384, 65_536, 262_144,
];

/// A lock-free histogram backed by atomic bucket counters.
///
/// `buckets` has one slot per bound plus a trailing overflow bucket for values
/// greater than the largest bound. `record` is wait-free; `drain` snapshots and
/// resets every counter (a concurrent `record` racing a `drain` may be counted
/// in either window, which is acceptable for monitoring).
#[derive(Debug)]
struct AtomicHistogram {
    name: &'static str,
    unit: &'static str,
    bounds: &'static [u64],
    buckets: Vec<AtomicU64>,
    sum: AtomicU64,
    max: AtomicU64,
}

impl AtomicHistogram {
    fn new(name: &'static str, unit: &'static str, bounds: &'static [u64]) -> Self {
        let buckets = (0..bounds.len() + 1).map(|_| AtomicU64::new(0)).collect();
        Self {
            name,
            unit,
            bounds,
            buckets,
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }

    #[inline]
    fn record(&self, value: u64) {
        // First bucket whose upper bound is >= value; len == overflow bucket.
        let idx = self.bounds.partition_point(|&b| b < value);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
    }

    /// Snapshot the current window and reset all counters. Returns `None` when
    /// no samples were recorded since the last drain.
    fn drain(&self) -> Option<HistogramSnapshot> {
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|b| b.swap(0, Ordering::Relaxed))
            .collect();
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return None;
        }
        let sum = self.sum.swap(0, Ordering::Relaxed);
        let max = self.max.swap(0, Ordering::Relaxed);

        Some(HistogramSnapshot {
            name: self.name,
            unit: self.unit,
            total,
            sum,
            max,
            p50: percentile(&counts, self.bounds, total, 0.50, max),
            p90: percentile(&counts, self.bounds, total, 0.90, max),
            p99: percentile(&counts, self.bounds, total, 0.99, max),
        })
    }
}

/// Conservative percentile estimate: the upper bound of the bucket that
/// contains the target rank, capped at the observed max so a sparse sample
/// never reports a percentile larger than any value actually seen.
fn percentile(counts: &[u64], bounds: &[u64], total: u64, q: f64, max: u64) -> u64 {
    let target = (q * total as f64).ceil() as u64;
    let target = target.max(1);
    let mut cum = 0u64;
    for (i, &c) in counts.iter().enumerate() {
        cum += c;
        if cum >= target {
            return if i < bounds.len() {
                bounds[i].min(max)
            } else {
                max
            };
        }
    }
    max
}

struct HistogramSnapshot {
    name: &'static str,
    unit: &'static str,
    total: u64,
    sum: u64,
    max: u64,
    p50: u64,
    p90: u64,
    p99: u64,
}

impl HistogramSnapshot {
    fn to_json(&self, window_secs: f64) -> String {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mean = if self.total > 0 {
            self.sum as f64 / self.total as f64
        } else {
            0.0
        };
        format!(
            "{{\"ts_ms\":{ts_ms},\"metric\":\"{}\",\"unit\":\"{}\",\"window_secs\":{:.1},\
             \"count\":{},\"p50\":{},\"p90\":{},\"p99\":{},\"max\":{},\"mean\":{:.2}}}",
            self.name, self.unit, window_secs, self.total, self.p50, self.p90, self.p99, self.max,
            mean
        )
    }
}

/// Shared latency metrics handle. Cloneable via `Arc`; passed into the geyser
/// loop and every client loop.
#[derive(Debug)]
pub struct LatencyMetrics {
    enabled: AtomicBool,
    end_to_end: AtomicHistogram,
    producer_send: AtomicHistogram,
    filter_encode: AtomicHistogram,
    client_queue_depth: AtomicHistogram,
}

impl LatencyMetrics {
    pub fn new(enabled: bool) -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(enabled),
            end_to_end: AtomicHistogram::new("end_to_end", "microseconds", TIME_BUCKETS_US),
            producer_send: AtomicHistogram::new("producer_send", "microseconds", TIME_BUCKETS_US),
            filter_encode: AtomicHistogram::new("filter_encode", "microseconds", TIME_BUCKETS_US),
            client_queue_depth: AtomicHistogram::new("client_queue_depth", "items", DEPTH_BUCKETS),
        })
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Record geyser-ingestion-to-client latency from a message `created_at`.
    #[inline]
    pub fn record_end_to_end(&self, created_at: &Timestamp) {
        if !self.enabled() {
            return;
        }
        self.end_to_end.record(micros_since(created_at));
    }

    /// Record the duration of one broadcast send on the producer critical path.
    #[inline]
    pub fn record_producer_send(&self, elapsed: Duration) {
        if !self.enabled() {
            return;
        }
        self.producer_send.record(duration_us(elapsed));
    }

    /// Record the per-message filter + encode duration on the subscriber path.
    #[inline]
    pub fn record_filter_encode(&self, elapsed: Duration) {
        if !self.enabled() {
            return;
        }
        self.filter_encode.record(duration_us(elapsed));
    }

    /// Record the current outbound queue depth for a subscriber.
    #[inline]
    pub fn record_queue_depth(&self, depth: u64) {
        if !self.enabled() {
            return;
        }
        self.client_queue_depth.record(depth);
    }

    const fn histograms(&self) -> [&AtomicHistogram; 4] {
        [
            &self.end_to_end,
            &self.producer_send,
            &self.filter_encode,
            &self.client_queue_depth,
        ]
    }

    fn emit(&self, window_secs: f64) {
        for hist in self.histograms() {
            if let Some(snapshot) = hist.drain() {
                info!(target: "yellowstone_latency", "{}", snapshot.to_json(window_secs));
            }
        }
    }

    /// Spawn the background reporter that drains and logs every `interval`.
    /// No-op (returns without spawning) when the metrics are disabled.
    pub fn spawn_reporter(
        self: Arc<Self>,
        interval: Duration,
        cancellation_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) {
        if !self.enabled() {
            return;
        }
        let window_secs = interval.as_secs_f64();
        task_tracker.spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            info!("latency reporter started (interval {:.1}s)", window_secs);
            loop {
                tokio::select! {
                    () = cancellation_token.cancelled() => {
                        break;
                    }
                    _ = tick.tick() => {
                        self.emit(window_secs);
                    }
                }
            }
            // Flush whatever accumulated in the final partial window.
            self.emit(window_secs);
            info!("latency reporter exiting");
        });
    }
}

#[inline]
fn duration_us(elapsed: Duration) -> u64 {
    elapsed.as_micros().min(u64::MAX as u128) as u64
}

/// Microseconds elapsed since `created_at`, clamped to 0 (clock skew / future
/// timestamps never produce a negative or absurd value).
#[inline]
fn micros_since(created_at: &Timestamp) -> u64 {
    let now_us = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_micros() as i128,
        Err(_) => return 0,
    };
    let ts_us = created_at.seconds as i128 * 1_000_000 + (created_at.nanos as i128) / 1_000;
    let diff = now_us - ts_us;
    if diff < 0 {
        0
    } else {
        diff as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_percentiles_match_known_distribution() {
        let hist = AtomicHistogram::new("t", "microseconds", TIME_BUCKETS_US);
        // 100 samples: 90 at ~5us, 9 at ~500us, 1 at ~5ms.
        for _ in 0..90 {
            hist.record(5);
        }
        for _ in 0..9 {
            hist.record(500);
        }
        hist.record(5_000);

        let snap = hist.drain().expect("non-empty");
        assert_eq!(snap.total, 100);
        // p50 sits inside the 5us bucket.
        assert_eq!(snap.p50, 5);
        // p90 is the 90th sample (still in the 5us bucket).
        assert_eq!(snap.p90, 5);
        // p99 is the 99th sample, which lands in the 500us bucket.
        assert_eq!(snap.p99, 500);
        // max is the single 5ms outlier.
        assert_eq!(snap.max, 5_000);
    }

    #[test]
    fn percentile_is_capped_at_observed_max() {
        let hist = AtomicHistogram::new("t", "microseconds", TIME_BUCKETS_US);
        // A single sample of 3us: every percentile must report 3, not the
        // bucket's nominal upper bound (5us).
        hist.record(3);
        let snap = hist.drain().expect("non-empty");
        assert_eq!(snap.p50, 3);
        assert_eq!(snap.p99, 3);
        assert_eq!(snap.max, 3);
    }

    #[test]
    fn drain_resets_between_windows() {
        let hist = AtomicHistogram::new("t", "items", DEPTH_BUCKETS);
        hist.record(10);
        assert!(hist.drain().is_some());
        // Second drain with no new samples yields nothing.
        assert!(hist.drain().is_none());
    }

    #[test]
    fn overflow_bucket_reports_max() {
        let hist = AtomicHistogram::new("t", "microseconds", TIME_BUCKETS_US);
        // Larger than the largest bound (10s); lands in overflow.
        hist.record(60_000_000);
        let snap = hist.drain().expect("non-empty");
        assert_eq!(snap.p99, 60_000_000);
        assert_eq!(snap.max, 60_000_000);
    }

    #[test]
    fn disabled_metrics_record_nothing() {
        let metrics = LatencyMetrics::new(false);
        metrics.record_producer_send(Duration::from_micros(123));
        metrics.record_queue_depth(50);
        // Disabled: histograms stay empty.
        assert!(metrics.producer_send.drain().is_none());
        assert!(metrics.client_queue_depth.drain().is_none());
    }

    #[test]
    fn enabled_metrics_record_samples() {
        let metrics = LatencyMetrics::new(true);
        metrics.record_producer_send(Duration::from_micros(123));
        let snap = metrics.producer_send.drain().expect("recorded");
        assert_eq!(snap.total, 1);
    }

    #[test]
    fn micros_since_clamps_future_timestamps() {
        let future = Timestamp {
            seconds: i64::MAX / 2,
            nanos: 0,
        };
        assert_eq!(micros_since(&future), 0);
    }

    #[test]
    fn json_is_well_formed() {
        let hist = AtomicHistogram::new("end_to_end", "microseconds", TIME_BUCKETS_US);
        hist.record(42);
        let json = hist.drain().unwrap().to_json(10.0);
        assert!(json.starts_with('{') && json.ends_with('}'));
        assert!(json.contains("\"metric\":\"end_to_end\""));
        assert!(json.contains("\"unit\":\"microseconds\""));
        assert!(json.contains("\"count\":1"));
    }
}
