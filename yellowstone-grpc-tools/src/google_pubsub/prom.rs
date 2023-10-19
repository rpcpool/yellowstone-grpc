use {
    crate::prom::GprcMessageKind,
    prometheus::{IntCounterVec, Opts},
};

lazy_static::lazy_static! {
    pub(crate) static ref GOOGLE_PUBSUB_SENT_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("google_pubsub_sent_total", "Total number of uploaded messages by type"),
        &["kind"]
    ).unwrap();
}

pub fn sent_inc(kind: GprcMessageKind) {
    GOOGLE_PUBSUB_SENT_TOTAL
        .with_label_values(&[kind.as_str()])
        .inc()
}
