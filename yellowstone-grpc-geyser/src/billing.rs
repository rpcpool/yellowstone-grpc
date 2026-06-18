use {
    crate::auth::SubscriptionInfo,
    bytesize::ByteSize,
    std::{
        sync::Arc,
        time::{Duration, Instant, SystemTime},
    },
    tokio::sync::mpsc::UnboundedReceiver,
    tonic::service::Interceptor,
    triton_grpc_tools::server::tonic::metered::MeteredBandwidthHooks,
};

#[derive(Debug, serde::Serialize)]
pub struct BandwidthBillingEvent {
    pub timestamp: SystemTime,
    pub subscription_id: String,
    pub method: String,
    pub bytes: ByteSize,
}

#[derive(Debug, serde::Serialize)]
pub struct RequestBillingEvent {
    pub timestamp: SystemTime,
    pub subscription_id: String,
    pub method: String,
}

pub enum ServiceBillingEvent {
    Bandwidth(BandwidthBillingEvent),
    Request(RequestBillingEvent),
}

pub struct FileBillingAppender {
    rx: UnboundedReceiver<ServiceBillingEvent>,
}

/// So you will do two types of lines

/// timestamp
/// Sub_id
/// Method
/// Count
/// And
/// timestamp
/// sub_id
/// Method
/// Bytes

///
/// Lago billing bandwidth metered hooks implementation.
/// For each emitted byte frame, the hook will emit a billing event to Lago with the cumulative bytes emitted in the current minute window.
///
pub struct BillingBandwidthMeteredHooks {
    subscriber_id: String,
    uri_path: String,
}
impl MeteredBandwidthHooks for BillingBandwidthMeteredHooks {
    fn on_emit_bytes(&mut self, byte_count: u64, _now: Instant, system_now: SystemTime) {
        // convert Intant to seconds since epoch, then divide by window duration to get the current window id (e.g. 60s window means all timestamps in the same minute share the same window id)

        // TODO
    }
}

pub struct RequestBillingInterceptor {}

impl Interceptor for RequestBillingInterceptor {
    fn call(
        &mut self,
        request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        let ext = request.extensions().get::<SubscriptionInfo>();
        let subscription_id = ext.map(|mapping| mapping.subscription_id.clone());

        // TODO emit billing event to Lago with method name and subscription id (if available in request metadata)
        Ok(request)
    }
}
