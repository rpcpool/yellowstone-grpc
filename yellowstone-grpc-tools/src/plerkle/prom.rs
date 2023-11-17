use {
    crate::plerkle::PlerkleStream,
    prometheus::{Gauge, IntCounterVec, Opts},
};

lazy_static::lazy_static! {
    pub(crate) static ref PLERKLE_MESSAGE_QUEUE_SIZE: Gauge = Gauge::new(
        "plerkle_message_queue_size", "Number of messages to upload",
    ).unwrap();

    pub(crate) static ref PLERKLE_SENT_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("plerkle_sent_total", "Total number of uploaded messages by stream and status"),
        &["stream", "status"]
    ).unwrap();
}

pub fn message_queue_size_inc() {
    PLERKLE_MESSAGE_QUEUE_SIZE.inc();
}

pub fn message_queue_size_dec() {
    PLERKLE_MESSAGE_QUEUE_SIZE.dec();
}

pub fn sent_inc(stream: PlerkleStream, status: Result<(), ()>) {
    PLERKLE_SENT_TOTAL
        .with_label_values(&[stream.as_str(), if status.is_ok() { "ok" } else { "err" }])
        .inc()
}
