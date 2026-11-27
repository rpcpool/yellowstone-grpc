//! Each Subscribe, deshred and gossip stream counts into its own lines, and a process-wide
//! registry keeps the totals per subscriber id until `reset_metrics`, for `message_sent_count` and
//! `subscriber_queue_size`.

use {
    prometheus::{
        core::{Collector, Desc},
        proto::{Bucket, Counter, Histogram, LabelPair, Metric, MetricFamily, MetricType},
    },
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, LazyLock, Mutex, MutexGuard, PoisonError,
        },
    },
};

/// `subscriber_queue_size`'s bucket bounds, as 8666a6c declares them.
pub const QUEUE_SIZE_BOUNDS: [f64; 12] = [
    1.0,
    10.0,
    100.0,
    1000.0,
    10000.0,
    20000.0,
    50000.0,
    100_000.0,
    175_000.0,
    250_000.0,
    500_000.0,
    1_000_000.0,
];

/// A bucket per bound, then the one past the last bound, which only the sample count shows.
const QUEUE_SIZE_BUCKETS: usize = QUEUE_SIZE_BOUNDS.len() + 1;

/// The queue a stream's size samples describe: `subscriber_queue_size`'s `kind` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamQueueKind {
    /// A Subscribe stream's queue.
    Normal,
    /// A deshred stream's queue.
    Deshred,
}

impl StreamQueueKind {
    pub const ALL: [Self; 2] = [Self::Normal, Self::Deshred];

    pub const fn label_value(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Deshred => "deshred",
        }
    }
}

/// A stream's sent updates and whether its series is made, on a line of their own: the router,
/// `client_loop` and the gossip loop add here.
#[derive(Debug, Default)]
#[repr(align(128))]
struct SentLine {
    sent_updates: AtomicU64,
    sent_series_made: AtomicBool,
}

/// A stream's queue size samples per bucket and their sum, on a line of their own: the stream's
/// wakes, its ping task and its session's drop add here.
#[derive(Debug, Default)]
#[repr(align(128))]
struct QueueSizeLine {
    bucket_samples: [AtomicU64; QUEUE_SIZE_BUCKETS],
    sample_sum: AtomicU64,
}

/// One stream's counters, which only the stream's own producers write.
#[derive(Debug, Default)]
pub struct StreamCounters {
    sent_line: SentLine,
    queue_size_line: QueueSizeLine,
}

impl StreamCounters {
    /// Counts `update_count` updates queued on the stream.
    pub fn add_sent_updates(&self, update_count: u64) {
        (self.sent_line.sent_updates).fetch_add(update_count, Ordering::Relaxed);
    }

    /// Makes the stream's `message_sent_count` series, which renders 0 until the stream counts.
    /// A Subscribe stream makes it when it goes live.
    pub fn make_sent_series(&self) {
        (self.sent_line.sent_series_made).store(true, Ordering::Relaxed);
    }

    /// Takes one `subscriber_queue_size` sample of the stream's queue.
    pub fn observe_queue_size(&self, queue_size: u64) {
        let bucket_index =
            QUEUE_SIZE_BOUNDS.partition_point(|bucket_bound| *bucket_bound < queue_size as f64);
        self.queue_size_line.bucket_samples[bucket_index].fetch_add(1, Ordering::Relaxed);
        (self.queue_size_line.sample_sum).fetch_add(queue_size, Ordering::Relaxed);
    }

    /// The updates this stream counted.
    pub fn sent_updates(&self) -> u64 {
        self.sent_line.sent_updates.load(Ordering::Relaxed)
    }

    /// Whether this stream made its `message_sent_count` series.
    pub fn sent_series_made(&self) -> bool {
        self.sent_line.sent_series_made.load(Ordering::Relaxed)
    }

    fn queue_size_totals(&self) -> QueueSizeTotals {
        let queue_size_line = &self.queue_size_line;
        QueueSizeTotals {
            bucket_samples: std::array::from_fn(|bucket_index| {
                queue_size_line.bucket_samples[bucket_index].load(Ordering::Relaxed)
            }),
            sample_sum: queue_size_line.sample_sum.load(Ordering::Relaxed),
        }
    }
}

/// Queue size samples per bucket, and their sum.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct QueueSizeTotals {
    bucket_samples: [u64; QUEUE_SIZE_BUCKETS],
    sample_sum: u64,
}

impl QueueSizeTotals {
    fn add(&mut self, added_totals: &Self) {
        let bucket_pairs = self
            .bucket_samples
            .iter_mut()
            .zip(added_totals.bucket_samples);
        for (bucket_samples, added_samples) in bucket_pairs {
            *bucket_samples += added_samples;
        }
        self.sample_sum += added_totals.sample_sum;
    }

    /// Every sample, the ones past the last bound included, so the count never trails a bucket.
    fn sample_count(&self) -> u64 {
        self.bucket_samples.iter().sum()
    }
}

/// A stream the registry keeps until no producer holds it.
#[derive(Debug)]
struct RegisteredStream {
    queue_kind: Option<StreamQueueKind>,
    stream_counters: Arc<StreamCounters>,
}

/// One subscriber id's ended streams, folded, and the streams that may still count.
#[derive(Debug, Default)]
struct SubscriberEntry {
    folded_sent_updates: u64,
    folded_sent_series_made: bool,
    folded_queue_sizes: [QueueSizeTotals; StreamQueueKind::ALL.len()],
    registered_streams: Vec<RegisteredStream>,
}

impl SubscriberEntry {
    /// Folds each stream that only the registry still holds, which no producer can count into
    /// again, and drops it.
    fn fold_released_streams(&mut self) {
        let Self {
            folded_sent_updates,
            folded_sent_series_made,
            folded_queue_sizes,
            registered_streams,
        } = self;
        registered_streams.retain_mut(|registered_stream| {
            let Some(released_counters) = Arc::get_mut(&mut registered_stream.stream_counters)
            else {
                return true;
            };
            *folded_sent_updates += released_counters.sent_updates();
            *folded_sent_series_made |= released_counters.sent_series_made();
            if let Some(queue_kind) = registered_stream.queue_kind {
                folded_queue_sizes[queue_kind as usize].add(&released_counters.queue_size_totals());
            }
            false
        });
    }

    /// The entry's series values: its folded totals plus what its registered streams counted.
    fn series_values(&self, subscriber_id: &str) -> SubscriberSeriesValues {
        let mut sent_updates = self.folded_sent_updates;
        let mut sent_series_made = self.folded_sent_series_made;
        let mut queue_sizes = self.folded_queue_sizes;
        for registered_stream in &self.registered_streams {
            let stream_counters = &registered_stream.stream_counters;
            sent_updates += stream_counters.sent_updates();
            sent_series_made |= stream_counters.sent_series_made();
            if let Some(queue_kind) = registered_stream.queue_kind {
                queue_sizes[queue_kind as usize].add(&stream_counters.queue_size_totals());
            }
        }
        SubscriberSeriesValues {
            subscriber_id: subscriber_id.to_owned(),
            sent_updates: (sent_series_made || sent_updates > 0).then_some(sent_updates),
            queue_sizes: queue_sizes.map(|queue_size_totals| {
                (queue_size_totals.sample_count() > 0).then_some(queue_size_totals)
            }),
        }
    }
}

/// One subscriber id's series, copied out of the registry for a scrape.
#[derive(Debug)]
struct SubscriberSeriesValues {
    subscriber_id: String,
    /// `None` until a stream made the series or counted an update.
    sent_updates: Option<u64>,
    /// Per [`StreamQueueKind`], `None` until a stream of that kind took a sample.
    queue_sizes: [Option<QueueSizeTotals>; StreamQueueKind::ALL.len()],
}

/// The per-subscriber-id totals behind `message_sent_count` and `subscriber_queue_size`. Its lock
/// is taken at a session's start and end and at a scrape, never on a data path.
#[derive(Debug, Default)]
pub struct SubscriberCounts {
    subscriber_entries: Mutex<HashMap<String, SubscriberEntry>>,
}

/// The process's subscriber counts, which `reset_metrics` clears at each load.
pub static SUBSCRIBER_COUNTS: LazyLock<SubscriberCounts> = LazyLock::new(SubscriberCounts::new);

impl SubscriberCounts {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new stream of `subscriber_id`, whose size samples describe `queue_kind`'s
    /// queue, or none for a gossip stream, and folds the id's released streams.
    pub fn open_stream(
        &self,
        subscriber_id: &str,
        queue_kind: Option<StreamQueueKind>,
    ) -> Arc<StreamCounters> {
        let stream_counters = Arc::new(StreamCounters::default());
        let mut subscriber_entries = self.lock_entries();
        let subscriber_entry = subscriber_entries
            .entry(subscriber_id.to_owned())
            .or_default();
        subscriber_entry.fold_released_streams();
        subscriber_entry.registered_streams.push(RegisteredStream {
            queue_kind,
            stream_counters: Arc::clone(&stream_counters),
        });
        stream_counters
    }

    /// Ends the session's hold of `stream_counters`, then folds `subscriber_id`'s released
    /// streams. An id that a reset forgot is left alone.
    pub fn close_stream(&self, subscriber_id: &str, stream_counters: Arc<StreamCounters>) {
        drop(stream_counters);
        if let Some(subscriber_entry) = self.lock_entries().get_mut(subscriber_id) {
            subscriber_entry.fold_released_streams();
        }
    }

    /// Forgets every subscriber id, as `reset_metrics` does at each load.
    pub fn reset(&self) {
        self.lock_entries().clear();
    }

    fn lock_entries(&self) -> MutexGuard<'_, HashMap<String, SubscriberEntry>> {
        (self.subscriber_entries.lock()).unwrap_or_else(PoisonError::into_inner)
    }

    /// Each subscriber id's series values, after folding its released streams.
    fn series_values(&self) -> Vec<SubscriberSeriesValues> {
        let mut subscriber_entries = self.lock_entries();
        (subscriber_entries.iter_mut())
            .map(|(subscriber_id, subscriber_entry)| {
                subscriber_entry.fold_released_streams();
                subscriber_entry.series_values(subscriber_id)
            })
            .collect()
    }
}

/// Renders `message_sent_count{subscriber_id}` and `subscriber_queue_size{subscriber_id, kind}`
/// from a [`SubscriberCounts`], as 8666a6c's counter and histogram families render.
pub struct SubscriberSeriesCollector {
    sent_desc: Desc,
    queue_size_desc: Desc,
    subscriber_counts: &'static SubscriberCounts,
}

impl SubscriberSeriesCollector {
    pub fn new(subscriber_counts: &'static SubscriberCounts) -> Self {
        let family_desc = |fq_name: &str, help: &str, label_names: &[&str]| {
            let variable_labels = label_names
                .iter()
                .map(|label_name| (*label_name).to_owned());
            Desc::new(
                fq_name.to_owned(),
                help.to_owned(),
                variable_labels.collect(),
                HashMap::new(),
            )
            .expect("a valid family name and labels")
        };
        Self {
            sent_desc: family_desc(
                "yellowstone_grpc_geyser_message_sent_count",
                "Number of message sent over grpc to downstream client",
                &["subscriber_id"],
            ),
            queue_size_desc: family_desc(
                "yellowstone_grpc_geyser_subscriber_queue_size",
                "Current size of subscriber channel queue",
                &["subscriber_id", "kind"],
            ),
            subscriber_counts,
        }
    }
}

/// A label pair; a metric's pairs go in name order, as `MetricVec` sorts them.
pub fn label_pair(label_name: &str, label_value: &str) -> LabelPair {
    let mut series_label = LabelPair::default();
    series_label.set_name(label_name.to_owned());
    series_label.set_value(label_value.to_owned());
    series_label
}

/// A family named and described by `family_desc`, of `family_type`, with no series yet.
pub fn empty_family(family_desc: &Desc, family_type: MetricType) -> MetricFamily {
    let mut metric_family = MetricFamily::default();
    metric_family.set_name(family_desc.fq_name.clone());
    metric_family.set_help(family_desc.help.clone());
    metric_family.set_field_type(family_type);
    metric_family
}

/// A histogram of `queue_size_totals` with cumulative buckets at [`QUEUE_SIZE_BOUNDS`].
fn queue_size_histogram(queue_size_totals: &QueueSizeTotals) -> Histogram {
    let mut series_histogram = Histogram::default();
    series_histogram.set_sample_count(queue_size_totals.sample_count());
    series_histogram.set_sample_sum(queue_size_totals.sample_sum as f64);
    let mut cumulative_samples = 0;
    let bound_samples = QUEUE_SIZE_BOUNDS
        .iter()
        .zip(queue_size_totals.bucket_samples);
    let histogram_buckets = bound_samples.map(|(bucket_bound, bucket_samples)| {
        cumulative_samples += bucket_samples;
        let mut histogram_bucket = Bucket::default();
        histogram_bucket.set_cumulative_count(cumulative_samples);
        histogram_bucket.set_upper_bound(*bucket_bound);
        histogram_bucket
    });
    series_histogram.set_bucket(histogram_buckets.collect());
    series_histogram
}

impl Collector for SubscriberSeriesCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.sent_desc, &self.queue_size_desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let series_values = self.subscriber_counts.series_values();
        let mut sent_family = empty_family(&self.sent_desc, MetricType::COUNTER);
        let mut queue_size_family = empty_family(&self.queue_size_desc, MetricType::HISTOGRAM);
        for subscriber_values in &series_values {
            let subscriber_id = subscriber_values.subscriber_id.as_str();
            if let Some(sent_updates) = subscriber_values.sent_updates {
                let mut sent_counter = Counter::default();
                sent_counter.set_value(sent_updates as f64);
                let mut sent_metric = Metric::default();
                sent_metric.set_label(vec![label_pair("subscriber_id", subscriber_id)]);
                sent_metric.set_counter(sent_counter);
                sent_family.mut_metric().push(sent_metric);
            }
            for queue_kind in StreamQueueKind::ALL {
                let Some(queue_size_totals) = &subscriber_values.queue_sizes[queue_kind as usize]
                else {
                    continue;
                };
                let mut queue_size_metric = Metric::default();
                queue_size_metric.set_label(vec![
                    label_pair("kind", queue_kind.label_value()),
                    label_pair("subscriber_id", subscriber_id),
                ]);
                queue_size_metric.set_histogram(queue_size_histogram(queue_size_totals));
                queue_size_family.mut_metric().push(queue_size_metric);
            }
        }
        vec![sent_family, queue_size_family]
    }
}
