use {
    crate::prom::GprcMessageKind,
    prometheus::{Gauge, IntCounterVec, IntGaugeVec, Opts},
    yellowstone_grpc_proto::prelude::CommitmentLevel,
};

lazy_static::lazy_static! {
    pub(crate) static ref GOOGLE_PUBSUB_RECV_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("google_pubsub_recv_total", "Total number of received messages from gRPC by type"),
        &["kind"]
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_SENT_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("google_pubsub_sent_total", "Total number of uploaded messages to pubsub by type"),
        &["kind", "status"]
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_SEND_BATCHES_IN_PROGRESS: Gauge = Gauge::new(
        "google_pubsub_send_batches_in_progress", "Number of batches in progress"
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("google_pubsub_awaiters_in_progress", "Number of awaiters in progress by type"),
        &["kind"]
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_DROP_OVERSIZED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("google_pubsub_drop_oversized_total", "Total number of dropped oversized messages"),
        &["kind"]
    ).unwrap();

    pub(crate) static ref GOOGLE_PUBSUB_SLOT_TIP: IntGaugeVec = IntGaugeVec::new(
        Opts::new("google_pubsub_slot_tip", "Latest received slot from gRPC by commitment"),
        &["commitment"]
    ).unwrap();
}

pub fn recv_inc(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_RECV_TOTAL
        .with_label_values(&[kind.as_str()])
        .inc();
    GOOGLE_PUBSUB_RECV_TOTAL.with_label_values(&["total"]).inc()
}

pub fn sent_inc(kind: GprcMessageKind, status: Result<(), ()>) {
    let status = if status.is_ok() { "success" } else { "failed" };

    GOOGLE_PUBSUB_SENT_TOTAL
        .with_label_values(&[kind.as_str(), status])
        .inc();
    GOOGLE_PUBSUB_SENT_TOTAL
        .with_label_values(&["total", status])
        .inc()
}

pub fn send_batches_inc() {
    GOOGLE_PUBSUB_SEND_BATCHES_IN_PROGRESS.inc()
}

pub fn send_batches_dec() {
    GOOGLE_PUBSUB_SEND_BATCHES_IN_PROGRESS.dec()
}

pub fn send_awaiters_inc(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS
        .with_label_values(&[kind.as_str()])
        .inc();
    GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS
        .with_label_values(&["total"])
        .inc()
}

pub fn send_awaiters_dec(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS
        .with_label_values(&[kind.as_str()])
        .dec();
    GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS
        .with_label_values(&["total"])
        .dec()
}

pub fn drop_oversized_inc(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_DROP_OVERSIZED_TOTAL
        .with_label_values(&[kind.as_str()])
        .inc()
}

pub fn set_slot_tip(commitment: CommitmentLevel, slot: i64) {
    GOOGLE_PUBSUB_SLOT_TIP
        .with_label_values(&[match commitment {
            CommitmentLevel::Processed => "processed",
            CommitmentLevel::Confirmed => "confirmed",
            CommitmentLevel::Finalized => "finalized",
        }])
        .set(slot)
}
