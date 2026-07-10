#[cfg(test)]
use serde::Deserialize;
use {
    crate::{
        auth::SubscriptionInfo,
        util::uri::{path_name_serde, PathName},
    },
    bytes::{BufMut, BytesMut},
    futures::{future::BoxFuture, FutureExt, Stream},
    http::request::Parts,
    serde::Serialize,
    std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    },
    tokio::sync::mpsc::UnboundedSender,
    url::Url,
    yellowstone_grpc_tools::server::tonic::metered::{MeteredBandwidthHooks, MeteredManager},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[cfg_attr(test, derive(Deserialize))]
pub struct BandwidthBillingEvent {
    pub subscription_id: Arc<String>,
    #[serde(with = "path_name_serde")]
    pub method: PathName,
    pub quantity: u64,
}

///
/// Similar to [`BandwidthBillingEvent`], but we want distinct types for the two different billing events, so we can handle them differently in the billing system.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[cfg_attr(test, derive(Deserialize))]
pub struct RequestBillingEvent {
    pub subscription_id: String,
    #[serde(with = "path_name_serde")]
    pub method: PathName,
    pub quantity: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "code", rename_all = "snake_case")]
#[cfg_attr(test, derive(Deserialize))]
pub enum BillingEvent {
    Bandwidth(BandwidthBillingEvent),
    Requests(RequestBillingEvent),
}

#[derive(Debug, Clone)]
pub struct BillingMeteredManager {
    usage_tx: UnboundedSender<BillingEvent>,
}

impl BillingMeteredManager {
    pub const fn new(usage_tx: UnboundedSender<BillingEvent>) -> Self {
        Self { usage_tx }
    }
}

pub struct BillingBandwidthMeteredHooks {
    subscriber_id: Arc<String>,
    uri_path: PathName,
    usage_sink: UnboundedSender<BillingEvent>,
}

impl MeteredBandwidthHooks for BillingBandwidthMeteredHooks {
    fn on_emit_bytes(
        &mut self,
        byte_count: u64,
        _now: std::time::Instant,
        _system_now: std::time::SystemTime,
    ) {
        if self.subscriber_id.is_empty() && self.uri_path.as_ref().is_empty() {
            return;
        }

        let event = BillingEvent::Bandwidth(BandwidthBillingEvent {
            subscription_id: Arc::clone(&self.subscriber_id),
            method: self.uri_path.clone(),
            quantity: byte_count,
        });

        self.usage_sink.send(event).unwrap_or_else(|e| {
            log::error!("Failed to send billing event: {:?}", e);
        });
    }
}

impl MeteredManager for BillingMeteredManager {
    type BandwidthMeteredHooks = BillingBandwidthMeteredHooks;

    fn build_hooks(&self, parts: &Parts) -> Option<Self::BandwidthMeteredHooks> {
        let subscriber_id = parts
            .extensions
            .get::<SubscriptionInfo>()
            .map(|info| info.subscription_id.clone())?;

        let uri_path = parts.uri.path_and_query().map(PathName::from)?;

        Some(BillingBandwidthMeteredHooks {
            subscriber_id: Arc::new(subscriber_id),
            uri_path,
            usage_sink: self.usage_tx.clone(),
        })
    }

    fn on_method(&self, request: &http::request::Parts) {
        let Some(method) = request.uri.path_and_query().map(PathName::from) else {
            return;
        };
        // Default implementation does nothing.
        self.usage_tx
            .send(BillingEvent::Requests(RequestBillingEvent {
                subscription_id: request
                    .extensions
                    .get::<SubscriptionInfo>()
                    .map(|info| info.subscription_id.clone())
                    .unwrap_or_default(),
                method,
                quantity: 1, // Assuming each method call counts as 1 request
            }))
            .unwrap_or_else(|e| {
                log::error!("Failed to send billing event: {:?}", e);
            });
    }
}

/// Once the buffered NDJSON payload reaches this size, flush it eagerly instead of waiting
/// for the source to go idle, so a busy producer doesn't grow the buffer without bound.
pub const FLUSH_THRESHOLD_BYTES: usize = 4 * 1024;

/// The interval at which the [`HttpBillingEventSink`] will log the number of events sent to the remote endpoint.
pub const REPORT_INTERVAL: Duration = Duration::from_secs(5);

///
/// A sink that consumes a stream of `BillingEvent`s, serializes them to NDJSON, and sends them to a remote HTTP endpoint.
///
/// # Generics
///
/// - `St`: The type of the source stream that produces `BillingEvent`s.
///
pub struct HttpBillingEventSink<St> {
    client: reqwest::Client,
    endpoint: Url,
    source: St,
    ndjson_buffer: BytesMut,
    source_closed: bool,
    ongoing_flush: Option<BoxFuture<'static, reqwest::Result<usize>>>,
    report_interval: Duration,
    events_since_last_flush: usize,
    events_flushed_since_last_report: usize,
    last_report_time: std::time::Instant,
}

enum FlushBufferError {
    Empty,
    OngoingFlush,
}

impl<St> HttpBillingEventSink<St> {
    ///
    /// Creates a new [`HttpBillingEventSink`].
    ///
    /// # Arguments
    ///
    /// - `client`: The [`reqwest::Client`] to use for sending HTTP requests.
    /// - `endpoint`: The [`Url`] of the remote endpoint to which billing events will be sent.
    /// - `source`: The source stream that produces [`BillingEvent`]s.
    /// - `reporting_interval`: Optional duration for how often to log the number of events, if not provided, defaults to [`REPORT_INTERVAL`].
    ///
    /// # Notes
    ///
    /// The `reporting_interval` is not a precise timer, but rather a threshold for logging the number of events sent to the remote endpoint.
    /// The actual logging may occur less frequently depending on the flow of events and the state of the source stream.
    ///
    pub fn new(
        client: reqwest::Client,
        endpoint: Url,
        source: St,
        reporting_interval: Option<Duration>,
    ) -> Self {
        Self {
            client,
            endpoint,
            source,
            ndjson_buffer: BytesMut::with_capacity(1024),
            source_closed: false,
            ongoing_flush: None,
            report_interval: reporting_interval.unwrap_or(REPORT_INTERVAL),
            events_since_last_flush: 0,
            events_flushed_since_last_report: 0,
            last_report_time: std::time::Instant::now(),
        }
    }

    ///
    /// Tries to flush the buffer if there are any events in it.
    /// If the buffer is empty or if there is an ongoing flush, it will return early.
    ///
    /// # Warnings
    ///
    /// This function does not poll the ongoing flush future.
    /// It only initiates a new flush if there isn't one already in progress.
    ///
    fn try_flush_buffer_without_polling(&mut self) -> Result<(), FlushBufferError> {
        if self.ndjson_buffer.is_empty() {
            return Err(FlushBufferError::Empty);
        }

        if self.ongoing_flush.is_some() {
            return Err(FlushBufferError::OngoingFlush);
        }

        let num_events_in_ndjson = self.events_since_last_flush;
        self.events_since_last_flush = 0;
        // `body` requires an `Into<Body>` where `Body` is a wrapper around `Bytes`, so better work with referenced-counter Bytes structure.
        let payload = self.ndjson_buffer.split().freeze();
        let request = self
            .client
            .post(self.endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(payload.clone());
        let fut = async move {
            request
                .timeout(Duration::from_millis(500))
                .send()
                .await
                .and_then(|response| response.error_for_status())
                .map(|_| num_events_in_ndjson)
        };
        self.ongoing_flush = Some(fut.boxed());
        Ok(())
    }

    fn poll_ongoing_flush(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(fut) = &mut self.ongoing_flush {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(num_events_flushed)) => {
                    self.events_flushed_since_last_report += num_events_flushed;
                    self.ongoing_flush = None;
                    Poll::Ready(())
                }
                Poll::Ready(Err(e)) => {
                    log::error!("Failed to send billing events: {:?}", e);
                    self.ongoing_flush = None;
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(())
        }
    }

    fn report_if_needed(&mut self, now: std::time::Instant) {
        if self.last_report_time.elapsed() >= self.report_interval {
            log::info!(
                "Flushed {} billing events in the last {:?}",
                self.events_flushed_since_last_report,
                now.duration_since(self.last_report_time)
            );
            self.events_flushed_since_last_report = 0;
            self.last_report_time = now;
        }
    }
}

struct BytesMutWriter<'buf> {
    bufmut: &'buf mut BytesMut,
}

impl std::io::Write for BytesMutWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bufmut.put_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<St> Future for HttpBillingEventSink<St>
where
    St: Stream<Item = BillingEvent> + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            this.report_if_needed(std::time::Instant::now());

            match this.poll_ongoing_flush(cx) {
                Poll::Ready(()) => {}
                Poll::Pending => {
                    // If we have an ongoing flush, we should not poll the source until it's done.
                    return Poll::Pending;
                }
            }

            if this.source_closed {
                // Source is exhausted: drain whatever remains in the buffer, then finish.
                // `poll_ongoing_flush` above already guarantees no flush is in progress here.
                return match this.try_flush_buffer_without_polling() {
                    Ok(()) => continue,
                    Err(FlushBufferError::Empty) => {
                        log::warn!("Billing event stream closed, and buffer is empty. HTTP Sink will complete.");
                        Poll::Ready(())
                    }
                    Err(FlushBufferError::OngoingFlush) => unreachable!(
                        "poll_ongoing_flush already drained any in-flight flush this iteration"
                    ),
                };
            }

            match Pin::new(&mut this.source).poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    // Serialize the event to JSON and buffer it
                    let mut writer = BytesMutWriter {
                        bufmut: &mut this.ndjson_buffer,
                    };
                    match serde_json::to_writer(&mut writer, &event) {
                        Ok(_) => {
                            this.events_since_last_flush =
                                this.events_since_last_flush.saturating_add(1);
                            this.ndjson_buffer.put_u8(b'\n');
                        }
                        Err(e) => {
                            log::error!("Failed to serialize billing event: {:?}", e);
                        }
                    }

                    if this.ndjson_buffer.len() >= FLUSH_THRESHOLD_BYTES {
                        // `poll_ongoing_flush` above already guarantees no flush is in
                        // progress, and we just appended to the buffer, so this always
                        // starts a flush; ignore the `Result` since there's nothing to
                        // react to either way.
                        let _ = this.try_flush_buffer_without_polling();
                    }
                }
                Poll::Ready(None) => {
                    this.source_closed = true;
                }
                Poll::Pending => {
                    // If we have buffered events, send them
                    match this.try_flush_buffer_without_polling() {
                        Ok(()) => {
                            // The polling of the ongoing flush will be handled in the next iteration of the loop
                            continue;
                        }
                        Err(FlushBufferError::Empty) => {
                            // No events to flush, just return Pending
                            return Poll::Pending;
                        }
                        Err(FlushBufferError::OngoingFlush) => {
                            // Already flushing, just return Pending
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests_serde {
    use {super::*, http::uri::PathAndQuery};

    fn path_name(path: &str) -> PathName {
        let path_and_query: PathAndQuery = path.parse().unwrap();
        PathName::from(&path_and_query)
    }

    #[test]
    fn serde_request_billing_event() {
        let req_event = RequestBillingEvent {
            subscription_id: "sub-1".to_string(),
            method: path_name("/geyser.Geyser/Subscribe"),
            quantity: 1,
        };
        let event = BillingEvent::Requests(req_event);
        let expected_json = r#"{"code":"requests","subscription_id":"sub-1","method":"/geyser.Geyser/Subscribe","quantity":1}"#;
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(json, expected_json);
        let decoded: BillingEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, event);
    }

    #[test]
    fn serde_bandwidth_billing_event() {
        let bw_event = BandwidthBillingEvent {
            subscription_id: Arc::new("sub-1".to_string()),
            method: path_name("/geyser.Geyser/Subscribe"),
            quantity: 1024,
        };
        let event = BillingEvent::Bandwidth(bw_event);
        let expected_json = r#"{"code":"bandwidth","subscription_id":"sub-1","method":"/geyser.Geyser/Subscribe","quantity":1024}"#;
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(json, expected_json);
        let decoded: BillingEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, event);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        http::uri::PathAndQuery,
        http_body_util::{BodyExt, Full as BodyFull},
        hyper::{body::Incoming as BodyIncoming, service::service_fn, Request, Response},
        hyper_util::{
            rt::tokio::{TokioExecutor, TokioIo},
            server::conn::auto::Builder as ServerBuilder,
        },
        std::sync::Mutex,
        tokio::{net::TcpListener, sync::mpsc},
        tokio_stream::wrappers::UnboundedReceiverStream,
    };

    fn requests_event(subscription_id: &str, path: &str, quantity: u64) -> BillingEvent {
        let path_and_query: PathAndQuery = path.parse().unwrap();
        BillingEvent::Requests(RequestBillingEvent {
            subscription_id: subscription_id.to_string(),
            method: PathName::from(&path_and_query),
            quantity,
        })
    }

    /// Spins up a minimal local HTTP server that records the body of every request it receives.
    async fn spawn_recording_server() -> (Url, Arc<Mutex<Vec<Bytes>>>) {
        let listener: TcpListener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received: Arc<Mutex<Vec<Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let received = Arc::clone(&received_clone);
                tokio::spawn(async move {
                    let _ = ServerBuilder::new(TokioExecutor::new())
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |req: Request<BodyIncoming>| {
                                let received = Arc::clone(&received);
                                async move {
                                    let body = req
                                        .into_body()
                                        .collect()
                                        .await
                                        .expect("failed to read request body")
                                        .to_bytes();
                                    received.lock().unwrap().push(body);
                                    Ok::<_, std::convert::Infallible>(Response::new(BodyFull::new(
                                        Bytes::new(),
                                    )))
                                }
                            }),
                        )
                        .await;
                });
            }
        });

        let endpoint = Url::parse(&format!("http://{addr}/ingest")).unwrap();
        (endpoint, received)
    }

    // Decoded as generic JSON rather than back into `BillingEvent`: `path_name_serde`'s
    // deserializer has its own (pre-existing, unrelated) round-trip bug, which isn't what
    // this test is about. Comparing raw JSON values checks exactly what the sink put on the
    // wire without tripping over that.
    fn decode_events(bodies: &[Bytes]) -> Vec<serde_json::Value> {
        bodies
            .iter()
            .flat_map(|body| body.split(|&b| b == b'\n'))
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<serde_json::Value>(line).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn resolves_immediately_when_source_is_already_closed() {
        // Regression test: the sink used to keep re-polling an exhausted source forever
        // instead of completing, hanging the task. An empty stream yields `None` on the
        // very first poll, so this future must resolve without ever needing a waker.
        let client = reqwest::Client::new();
        let endpoint = Url::parse("http://127.0.0.1:1/unused").unwrap();
        let sink = HttpBillingEventSink::new(
            client,
            endpoint,
            futures::stream::empty::<BillingEvent>(),
            None,
        );

        tokio::time::timeout(Duration::from_secs(5), sink)
            .await
            .expect("sink future must complete when the source stream is already closed");
    }

    #[tokio::test]
    async fn flushes_buffered_and_trailing_events_then_completes() {
        let (endpoint, received) = spawn_recording_server().await;
        let client = reqwest::Client::new();

        let (tx, rx) = mpsc::unbounded_channel::<BillingEvent>();
        let source = UnboundedReceiverStream::new(rx);

        let event_a = requests_event("sub-a", "/subscribe", 1);
        let event_b = requests_event("sub-b", "/ping", 2);

        let sender = {
            let event_a = event_a.clone();
            let event_b = event_b.clone();
            tokio::spawn(async move {
                tx.send(event_a).unwrap();
                // Give the sink a chance to observe an empty channel (`Poll::Pending`) and
                // flush what it has buffered so far, before the second event arrives.
                tokio::time::sleep(Duration::from_millis(50)).await;
                tx.send(event_b).unwrap();
                // Dropping `tx` here closes the stream, exercising the final drain-on-close path.
            })
        };

        let sink = HttpBillingEventSink::new(client, endpoint, source, None);
        tokio::time::timeout(Duration::from_secs(5), sink)
            .await
            .expect("sink future must complete once the source stream closes");
        sender.await.unwrap();

        let received_events = decode_events(&received.lock().unwrap());
        assert_eq!(received_events.len(), 2);
        assert!(received_events.contains(&serde_json::to_value(&event_a).unwrap()));
        assert!(received_events.contains(&serde_json::to_value(&event_b).unwrap()));
    }

    #[tokio::test]
    async fn flushes_eagerly_once_buffer_crosses_size_threshold() {
        let (endpoint, received) = spawn_recording_server().await;
        let client = reqwest::Client::new();

        // `futures::stream::iter` never yields `Pending`, so without a size-triggered
        // flush every event below would sit in the buffer until the stream closes and
        // go out as a single request. Enough events to clear `FLUSH_THRESHOLD_BYTES`
        // several times over proves the sink flushes mid-stream instead.
        let events: Vec<BillingEvent> = (0..200)
            .map(|i| requests_event(&format!("sub-{i}"), "/subscribe", 1))
            .collect();
        let source = futures::stream::iter(events.clone());

        let sink = HttpBillingEventSink::new(client, endpoint, source, None);
        tokio::time::timeout(Duration::from_secs(5), sink)
            .await
            .expect("sink future must complete once the source stream closes");

        let bodies = received.lock().unwrap();
        assert!(
            bodies.len() > 1,
            "expected multiple flushes triggered by the size threshold, got {}",
            bodies.len()
        );
        assert!(
            bodies
                .iter()
                .all(|body| body.len() <= FLUSH_THRESHOLD_BYTES + 512),
            "no single flush should be allowed to grow far past the threshold"
        );

        let received_events = decode_events(&bodies);
        assert_eq!(received_events.len(), events.len());
        for event in &events {
            assert!(received_events.contains(&serde_json::to_value(event).unwrap()));
        }
    }
}
