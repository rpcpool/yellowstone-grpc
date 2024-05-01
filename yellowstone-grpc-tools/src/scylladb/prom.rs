use {
    prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge},
    std::time::Duration,
};

lazy_static::lazy_static! {
    pub(crate) static ref SCYLLADB_BATCH_DELIVERED: IntCounter = IntCounter::new(
        "scylladb_batch_sent_total", "Total number of batch delivered"
    ).unwrap();

    pub(crate) static ref SCYLLADB_BATCH_SIZE: Histogram = Histogram::with_opts(
        HistogramOpts::new("scylladb_batch_size", "The batch size sent to Scylladb"),
    ).unwrap();

    pub(crate) static ref SCYLLADB_BATCH_REQUEST_LAG: IntGauge = IntGauge::new(
      "scylladb_batch_request_lag", "The amount of batch request not being handle by a batching task"
    ).unwrap();

    pub(crate) static ref SCYLLADB_BATCHITEM_DELIVERED: IntCounter = IntCounter::new(
        "scylladb_batchitem_sent_total", "Total number of batch items delivered"
    ).unwrap();

    pub(crate) static ref SCYLLADB_PEAK_BATCH_LINGER_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("scylladb_peak_batch_linger_seconds", "The actual batch linger of the next batch to sent"),
    ).unwrap();

    pub(crate) static ref SCYLLADB_BATCH_QUEUE: IntGauge = IntGauge::new(
      "scylladb_batch_queue_size", "The amount of batch concurrently being linger."
    ).unwrap();

}

pub fn scylladb_batch_sent_inc() {
    SCYLLADB_BATCH_DELIVERED.inc()
}

pub fn scylladb_batchitem_sent_inc_by(amount: u64) {
    SCYLLADB_BATCHITEM_DELIVERED.inc_by(amount)
}

pub fn scylladb_batch_size_observe(batch_size: usize) {
    SCYLLADB_BATCH_SIZE.observe(batch_size as f64)
}

pub fn scylladb_peak_batch_linger_observe(batch_linger: Duration) {
    SCYLLADB_PEAK_BATCH_LINGER_SECONDS.observe(batch_linger.as_secs_f64())
}

pub fn scylladb_batch_queue_inc() {
    SCYLLADB_BATCH_QUEUE.inc()
}

pub fn scylladb_batch_queue_dec() {
    SCYLLADB_BATCH_QUEUE.dec()
}

pub fn scylladb_batch_request_lag_inc() {
    SCYLLADB_BATCH_REQUEST_LAG.inc()
}

pub fn scylladb_batch_request_lag_sub(amount: i64) {
    SCYLLADB_BATCH_REQUEST_LAG.sub(amount)
}
