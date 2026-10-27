//! Unary calls at steady rates. GetSlot, GetLatestBlockhash and GetBlockHeight run on the plugin's
//! runtime and read the block-meta store, so they compete with processed delivery.

use {
    super::{open_connection, profile::UnaryLoad, StreamContext},
    crate::{
        fleet::link::LinkSpec,
        hist::{LogHist, Summary},
        sys,
    },
    bytes::{Bytes, BytesMut},
    prost::Message,
    serde::{Deserialize, Serialize},
    std::time::{Duration, Instant},
    yellowstone_grpc_proto::geyser::{
        GetBlockHeightRequest, GetLatestBlockhashRequest, GetSlotRequest, GetVersionRequest,
        PingRequest,
    },
};

/// Method names as the plugin's `method_call_count` metric labels them.
pub const METHODS: [&str; 5] = [
    "get_slot",
    "get_latest_blockhash",
    "get_block_height",
    "get_version",
    "ping",
];

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct UnaryReport {
    pub target_per_sec: f64,
    pub calls_per_sec: f64,
    pub calls: u64,
    pub errors: u64,
    pub latency_ns: Option<Summary>,
    #[serde(default)]
    pub last_error: Option<String>,
}

/// gRPC path and encoded request of a method.
fn request(method: &str) -> Option<(&'static str, Vec<u8>)> {
    Some(match method {
        "get_slot" => ("GetSlot", GetSlotRequest::default().encode_to_vec()),
        "get_latest_blockhash" => (
            "GetLatestBlockhash",
            GetLatestBlockhashRequest::default().encode_to_vec(),
        ),
        "get_block_height" => (
            "GetBlockHeight",
            GetBlockHeightRequest::default().encode_to_vec(),
        ),
        "get_version" => ("GetVersion", GetVersionRequest::default().encode_to_vec()),
        "ping" => ("Ping", PingRequest { count: 1 }.encode_to_vec()),
        _ => return None,
    })
}

/// Runs one method's load until the fleet stops.
pub async fn run(context: StreamContext, load: UnaryLoad) -> (String, UnaryReport) {
    let connections = load.connections.max(1);
    let per_connection = load.rate_per_sec / connections as f64;
    let started = Instant::now();
    let handles: Vec<_> = (0..connections)
        .map(|index| {
            tokio::spawn(connection(
                context.clone(),
                load.method.clone(),
                per_connection,
                index as u64,
            ))
        })
        .collect();
    let mut report = UnaryReport {
        target_per_sec: load.rate_per_sec,
        ..Default::default()
    };
    let mut latency = LogHist::default();
    for handle in handles {
        if let Ok(result) = handle.await {
            report.calls += result.calls;
            report.errors += result.errors;
            latency.merge(&result.latency);
            if result.last_error.is_some() {
                report.last_error = result.last_error;
            }
        }
    }
    report.calls_per_sec = report.calls as f64 / started.elapsed().as_secs_f64().max(1e-9);
    report.latency_ns = Some(latency.summary());
    (load.method, report)
}

#[derive(Default)]
struct ConnectionResult {
    calls: u64,
    errors: u64,
    latency: LogHist,
    last_error: Option<String>,
}

/// One client polling one method on one connection; it reconnects after a failed call.
async fn connection(
    context: StreamContext,
    method: String,
    rate: f64,
    index: u64,
) -> ConnectionResult {
    let mut result = ConnectionResult::default();
    let Some((path, payload)) = request(&method) else {
        result.errors = 1;
        result.last_error = Some(format!("unknown unary method {method}"));
        return result;
    };
    let mut stop = context.stop_rx.clone();
    let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / rate.max(0.001)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut send_request: Option<h2::client::SendRequest<Bytes>> = None;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = stop.changed() => break,
        }
        let sender = match send_request.take() {
            Some(sender) => sender,
            None => {
                match open_connection(&context, &LinkSpec::default(), None, context.seed ^ index)
                    .await
                {
                    Ok(sender) => sender,
                    Err(error) => {
                        result.errors += 1;
                        result.last_error = Some(format!("connect: {error:#}"));
                        continue;
                    }
                }
            }
        };
        let begin = sys::mono_ns();
        match call(&sender, &context.target, path, &payload).await {
            Ok(()) => {
                result.calls += 1;
                result.latency.record(sys::mono_ns() - begin);
                send_request = Some(sender);
            }
            Err(error) => {
                result.errors += 1;
                result.last_error = Some(format!("{error:#}"));
            }
        }
    }
    result
}

async fn call(
    sender: &h2::client::SendRequest<Bytes>,
    target: &str,
    path: &str,
    payload: &[u8],
) -> anyhow::Result<()> {
    let request = http::Request::builder()
        .method("POST")
        .uri(format!("http://{target}/geyser.Geyser/{path}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("user-agent", "ygb-fleet")
        .body(())?;
    let mut sender = sender.clone().ready().await?;
    let (response, mut stream) = sender.send_request(request, false)?;
    let mut frame = BytesMut::with_capacity(5 + payload.len());
    frame.extend_from_slice(&[0]);
    frame.extend_from_slice(&u32::try_from(payload.len())?.to_be_bytes());
    frame.extend_from_slice(payload);
    stream.send_data(frame.freeze(), true)?;
    let response = response.await?;
    anyhow::ensure!(
        response.status() == 200,
        "HTTP status {}",
        response.status()
    );
    // A trailers-only response carries grpc-status in the headers.
    let mut status = response.headers().get("grpc-status").cloned();
    let mut body = response.into_body();
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        let _ = body.flow_control().release_capacity(chunk.len());
    }
    if status.is_none() {
        status = body
            .trailers()
            .await?
            .and_then(|trailers| trailers.get("grpc-status").cloned());
    }
    let status = status
        .as_ref()
        .and_then(|value| value.to_str().ok())
        .unwrap_or("missing");
    anyhow::ensure!(status == "0", "grpc-status {status}");
    Ok(())
}
