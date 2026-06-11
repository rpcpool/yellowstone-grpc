//
// The transaction_id (Lago's idempotency key) is built identically for both, differing only in the trailing tag:
//
//   <lago_sub_id>-<backend_code>-<lago_source>-<lago_rpc_method>-<window_60s>-<VECTOR_HOSTNAME>-<rps|bw>
//
//   ┌─────────────────┬────────────────────────────────────────┬───────────────────────────────────────────────────┐
//   │     Segment     │                 Source                 │                     For gRPC                      │
//   ├─────────────────┼────────────────────────────────────────┼───────────────────────────────────────────────────┤
//   │ lago_sub_id     │ Lago external subscription id          │ the subscription                                  │
//   ├─────────────────┼────────────────────────────────────────┼───────────────────────────────────────────────────┤
//   │ backend_code    │ HAProxy backend code                   │ grpc                                              │
//   ├─────────────────┼────────────────────────────────────────┼───────────────────────────────────────────────────┤
//   │ lago_source     │ traffic source                         │ e.g. endpoint/source tag                          │
//   ├─────────────────┼────────────────────────────────────────┼───────────────────────────────────────────────────┤
//   │ lago_rpc_method │ RPC method(s), comma-joined if batched │ e.g. Subscribe                                    │
//   ├─────────────────┼────────────────────────────────────────┼───────────────────────────────────────────────────┤
//   │ window_60s      │ minute-aligned epoch window            │ the per-minute bucket                             │
//   ├─────────────────┼────────────────────────────────────────┼───────────────────────────────────────────────────┤
//   │ VECTOR_HOSTNAME │ the LB node name                       │ so multiple LBs don't collide                     │
//   ├─────────────────┼────────────────────────────────────────┼───────────────────────────────────────────────────┤
//   │ suffix          │ event type                             │ -rps for grpc_requests, -bw for grpc_bandwidth_mb │
//   └─────────────────┴────────────────────────────────────────┴───────────────────────────────────────────────────┘
//
//   So the two events for the same subscription/backend/source/method/minute on the same node share everything except the final -rps vs -bw — that suffix is what keeps the requests event and the bandwidth event from clobbering each other's idempotency key in Lago (service-haproxy-vector.yaml:253 and :270).
//
//   The event timestamp is window_60s * 60, and the count/bytes live in properties (properties.count for requests, properties.bytes_out for bandwidth).
//
//   One thing to note: the Go backfill tool (roles/vector/backfill-lago.go:498-526) reproduces this exact format but uses its own -node flag instead of VECTOR_HOSTNAME for the hostname segment — it must match the live Vector format or backfilled events will be treated as distinct rows rather than idempotent duplicates.

use {
    std::{
        sync::Arc,
        time::{Duration, Instant, SystemTime},
    },
    triton_grpc_tools::server::tonic::metered::MeteredBandwidthHooks,
};

pub fn format_txn_id(
    lago_sub_id: &str,
    lago_source: &str,
    lago_rpc_method: &str,
    window_since_epoch: u64,
    hostname: &str,
    suffix: &str,
) -> String {
    let backend_code = "grpc";
    format!(
        "{lago_sub_id}-{backend_code}-{lago_source}-{lago_rpc_method}-{window_since_epoch}-{hostname}{suffix}"
    )
}

#[derive(Debug, serde::Serialize)]
pub struct LagoBillEvent<Props> {
    pub external_subscription_id: String,
    pub txn_id: String,
    pub code: String,
    pub timestamp: SystemTime,
    pub props: Props,
}

pub struct LagoConsumedBandwidthProps {
    pub bytes_out: u64,
    pub backend: Option<String>,
    pub node: Option<String>,
    pub grpc_method: String,
}

///
/// Lago billing bandwidth metered hooks implementation.
/// For each emitted byte frame, the hook will emit a billing event to Lago with the cumulative bytes emitted in the current minute window.
///
pub struct LagoBandwidthMeteredHooks {
    code: String,
    source_tag: String,
    hostname: String,
    subscriber_id: String,
    uri_path: String,
    nodename: Option<String>,
    backend: Option<String>,
    window_dur: Duration,
    last_flush: SystemTime,
    current_window_cumulative_bytes: u64,
}

impl LagoBandwidthMeteredHooks {
    fn flush(&mut self, now: SystemTime) {
        // Round now to the next window_dur boundary
        let now_secs = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        let now_window_aligned_secs =
            (now_secs.div_euclid(self.window_dur.as_secs())) * self.window_dur.as_secs();
        let now_window_aligned_time =
            SystemTime::UNIX_EPOCH + Duration::from_secs(now_window_aligned_secs);
        let next_window_boundary_secs =
            ((now_secs.div_euclid(self.window_dur.as_secs())) + 1) * self.window_dur.as_secs();
        let next_window_boundary =
            SystemTime::UNIX_EPOCH + Duration::from_secs(next_window_boundary_secs);

        let bytes_count = self.current_window_cumulative_bytes;
        self.current_window_cumulative_bytes = 0;
        self.last_flush = next_window_boundary;

        let txn_id = format_txn_id(
            &self.subscriber_id,
            &self.source_tag,
            &self.uri_path,
            now_window_aligned_secs,
            &self.hostname,
            "-bw",
        );
        let event = LagoBillEvent {
            txn_id,
            external_subscription_id: self.subscriber_id.clone(),
            code: self.code.clone(),
            timestamp: now_window_aligned_time,
            props: LagoConsumedBandwidthProps {
                bytes_out: bytes_count,
                backend: self.backend.clone(),
                node: self.nodename.clone(),
                grpc_method: self.uri_path.clone(),
            },
        };
    }
}

impl MeteredBandwidthHooks for LagoBandwidthMeteredHooks {
    fn on_emit_bytes(&mut self, byte_count: u64, _now: Instant, system_now: SystemTime) {
        // convert Intant to seconds since epoch, then divide by window duration to get the current window id (e.g. 60s window means all timestamps in the same minute share the same window id)

        self.current_window_cumulative_bytes += byte_count;
        let delta = system_now
            .duration_since(self.last_flush)
            .unwrap_or(Duration::ZERO);
        if delta >= self.window_dur {
            // flush the current cumulative bytes and reset the counter and last flush time
            self.flush(system_now);
        }
        let window_60s = system_now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
            / self.window_dur.as_secs();
    }
}
