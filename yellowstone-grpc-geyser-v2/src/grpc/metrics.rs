use {
    lazy_static::lazy_static,
    prometheus::{IntCounterVec, IntGauge, Opts},
};

lazy_static! {
    static ref TOTAL_CLIENT_CONNECTED: IntGauge = IntGauge::new(
        "grpc_total_client_connected",
        "Total number of connected clients"
    )
    .unwrap();
    static ref MESSAGE_SEND_COUNTER_PER_CLIENT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "grpc_message_send_counter_per_client",
            "Number of messages sent to each client"
        ),
        &["x_subscription_id", "address"]
    )
    .unwrap();
    static ref TOTAL_DISCONNECTION_COUNT: IntGauge = IntGauge::new(
        "grpc_total_disconnection_count",
        "Total number of disconnections from clients since start"
    )
    .unwrap();
}

pub(crate) fn set_total_client_connected(value: usize) {
    TOTAL_CLIENT_CONNECTED.set(value as i64);
}

pub(crate) fn inc_message_send_counter_per_client(x_subscription_id: &str, address: &str) {
    MESSAGE_SEND_COUNTER_PER_CLIENT
        .with_label_values(&[x_subscription_id, address])
        .inc();
}

pub(crate) fn inc_total_disconnection_count() {
    TOTAL_DISCONNECTION_COUNT.inc();
}

pub fn register_metrics(registry: &prometheus::Registry) -> Result<(), prometheus::Error> {
    registry.register(Box::new(TOTAL_CLIENT_CONNECTED.clone()))?;
    registry.register(Box::new(MESSAGE_SEND_COUNTER_PER_CLIENT.clone()))?;
    registry.register(Box::new(TOTAL_DISCONNECTION_COUNT.clone()))?;
    Ok(())
}
