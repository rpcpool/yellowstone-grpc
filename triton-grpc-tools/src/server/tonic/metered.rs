use {
    bytes::Buf,
    bytesize::ByteSize,
    http::{HeaderMap, Request, Response},
    hyper::body::{Frame, SizeHint},
    pin_project::pin_project,
    std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{ready, Context, Poll},
        time::Instant,
    },
    tonic::codegen::{Body as HttpBody, Service, StdError},
    tower_layer::Layer,
};

pub const DEFAULT_TRAFFIC_REPORTING_THRESHOLD: ByteSize = ByteSize::kib(32);

pub trait MeteredHooks {
    /// Records bytes emitted by a response body frame.
    ///
    /// Implementations can forward this event to metrics systems, tracing,
    /// or request-scoped accounting.
    ///
    /// # Arguments
    /// - `byte_count`: Number of payload bytes emitted by the current frame.
    /// - `now`: [`Instant`] sampled when the frame was observed.
    fn on_emit_bytes(&self, byte_count: u64, now: Instant);
}

pub trait MeteredManager {
    /// Concrete hooks type produced for each request.
    type Hooks: MeteredHooks + Send + Sync + 'static;

    /// Builds per-request hooks used to meter the response body.
    ///
    /// # Parameters
    /// - `header_map`: Incoming request headers.
    /// - `uri_path`: Request URI path (for example, `/geyser.Geyser/Subscribe`).
    ///
    /// # Returns
    /// A hooks instance attached to the response body wrapper.
    fn build_hooks(&self, header_map: &HeaderMap, uri_path: &str) -> Self::Hooks;
}

/// Tower layer that wraps services with byte metering for response bodies.
///
/// The layer clones a shared manager and applies a [`MeteredService`] wrapper
/// to each inner service instance.
#[derive(Debug, Clone, Default)]
pub struct MeteredLayer<MM> {
    metered_manager: Arc<MM>,
    traffic_reporting_threshold: ByteSize,
}

impl<MM> MeteredLayer<MM> {
    /// Creates a new metering layer from a manager implementation.
    pub fn new(metered_manager: MM, traffic_reporting_threshold: ByteSize) -> Self {
        Self {
            metered_manager: Arc::new(metered_manager),
            traffic_reporting_threshold,
        }
    }
}

impl<S, MM> Layer<S> for MeteredLayer<MM>
where
    MM: MeteredManager,
{
    type Service = MeteredService<S, MM>;

    /// Wraps the provided service with metering behavior.
    fn layer(&self, service: S) -> Self::Service {
        MeteredService {
            inner: service,
            metered_manager: Arc::clone(&self.metered_manager),
            traffic_reporting_threshold: self.traffic_reporting_threshold,
        }
    }
}

/// Service wrapper that attaches per-request metering hooks to response bodies.
///
/// Each call creates hooks via [`MeteredManager::build_hooks`] and returns a
/// [`MeteredFuture`] that wraps the eventual response body with [`MeteredBody`].
#[derive(Debug, Clone)]
pub struct MeteredService<S, MM> {
    inner: S,
    metered_manager: Arc<MM>,
    traffic_reporting_threshold: ByteSize,
}

/// Future returned by [`MeteredService`] that wraps successful responses.
///
/// On completion, this future replaces the original response body with a
/// [`MeteredBody`] that reports emitted byte counts through request-scoped hooks.
#[pin_project]
pub struct MeteredFuture<F, B, E, MH> {
    #[pin]
    future: F,
    metered_hooks: Option<MH>,
    _marker: std::marker::PhantomData<(B, E)>,
    traffic_reporting_threshold: ByteSize,
}

/// Response body wrapper that reports emitted data-frame byte counts.
///
/// For each yielded data frame, the wrapper calls [`MeteredHooks::on_emit_bytes`]
/// with the number of remaining bytes in the frame payload.
#[pin_project]
pub struct MeteredBody<B, MH> {
    #[pin]
    inner: B,
    metered_hooks: MH,
    traffic_reporting_threshold: ByteSize,
    cumulative_bytes: u64,
}

impl<B, MH> HttpBody for MeteredBody<B, MH>
where
    B: HttpBody,
    MH: MeteredHooks,
{
    type Data = B::Data;
    type Error = B::Error;

    /// Polls the next frame and emits metering events for data frames.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.as_mut().project();
        match ready!(this.inner.as_mut().poll_frame(cx)) {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    *this.cumulative_bytes += data.remaining() as u64;
                    if *this.cumulative_bytes >= this.traffic_reporting_threshold.as_u64() {
                        this.metered_hooks
                            .on_emit_bytes(*this.cumulative_bytes, Instant::now());
                        *this.cumulative_bytes = 0;
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }

    /// Returns whether the inner body has reached end-of-stream.
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    /// Forwards the size hint of the wrapped body.
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<F, B, E, MH> Future for MeteredFuture<F, B, E, MH>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: HttpBody + Send + 'static,
    B::Error: Into<StdError>,
    MH: MeteredHooks,
{
    type Output = Result<Response<MeteredBody<B, MH>>, E>;

    /// Polls the inner future and wraps successful response bodies.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        let result = ready!(this.future.poll(cx));
        match result {
            Ok(response) => {
                let (parts, body) = response.into_parts();
                // increment_active_metered_bodies_for_subscriber_and_path(&subscriber_id, &uri_path);
                let metered_body = MeteredBody {
                    inner: body,
                    metered_hooks: this.metered_hooks.take().expect("poll after completed"),
                    traffic_reporting_threshold: *this.traffic_reporting_threshold,
                    cumulative_bytes: 0,
                };
                Poll::Ready(Ok(Response::from_parts(parts, metered_body)))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl<S, ReqBody, ResBody, MM> Service<Request<ReqBody>> for MeteredService<S, MM>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
    ResBody: HttpBody + Send + 'static,
    ResBody::Error: Into<StdError>,
    MM: MeteredManager + Send + Sync + 'static,
{
    type Response = Response<MeteredBody<ResBody, MM::Hooks>>;
    type Error = S::Error;
    type Future = MeteredFuture<S::Future, ResBody, S::Error, MM::Hooks>;

    /// Delegates readiness to the wrapped inner service.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// Builds request hooks and delegates request execution to the inner service.
    ///
    /// The returned future will wrap successful responses with [`MeteredBody`]
    /// so byte emission can be observed frame by frame.
    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let hooks = self
            .metered_manager
            .build_hooks(request.headers(), request.uri().path());
        let future = self.inner.call(request);
        MeteredFuture {
            future,
            metered_hooks: Some(hooks),
            traffic_reporting_threshold: self.traffic_reporting_threshold,
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        http_body_util::{combinators::BoxBody, BodyExt, Full},
        std::sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        tonic::codegen::Service,
    };

    fn make_body(data: &'static [u8]) -> BoxBody<Bytes, std::io::Error> {
        Full::new(Bytes::from_static(data))
            .map_err(|never| match never {})
            .boxed()
    }

    struct TestHooks {
        total_bytes: Arc<AtomicU64>,
    }

    impl MeteredHooks for TestHooks {
        fn on_emit_bytes(&self, byte_count: u64, _now: Instant) {
            self.total_bytes.fetch_add(byte_count, Ordering::Relaxed);
        }
    }

    struct TestManager {
        total_bytes: Arc<AtomicU64>,
        build_hooks_calls: Arc<AtomicUsize>,
        last_path: Arc<Mutex<Option<String>>>,
    }

    impl MeteredManager for TestManager {
        type Hooks = TestHooks;

        fn build_hooks(&self, _header_map: &HeaderMap, uri_path: &str) -> Self::Hooks {
            self.build_hooks_calls.fetch_add(1, Ordering::Relaxed);
            *self.last_path.lock().expect("poisoned mutex") = Some(uri_path.to_owned());
            TestHooks {
                total_bytes: Arc::clone(&self.total_bytes),
            }
        }
    }

    struct TestInnerService {
        body_data: &'static [u8],
    }

    impl Service<Request<()>> for TestInnerService {
        type Response = Response<BoxBody<Bytes, std::io::Error>>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<()>) -> Self::Future {
            std::future::ready(Ok(Response::new(make_body(self.body_data))))
        }
    }

    #[tokio::test]
    async fn metered_body_emits_hook_for_data_frames() {
        let total_bytes = Arc::new(AtomicU64::new(0));
        let hooks = TestHooks {
            total_bytes: Arc::clone(&total_bytes),
        };

        let mut body = MeteredBody {
            inner: Full::new(Bytes::from_static(b"hello")),
            metered_hooks: hooks,
            traffic_reporting_threshold: ByteSize::b(0),
            cumulative_bytes: 0,
        };

        let frame = body
            .frame()
            .await
            .expect("expected first frame")
            .expect("expected successful frame");
        assert_eq!(
            frame.data_ref().expect("expected data frame").remaining(),
            5
        );
        assert_eq!(total_bytes.load(Ordering::Relaxed), 5);

        assert!(body.frame().await.is_none());
        assert_eq!(total_bytes.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn metered_future_wraps_response_body_and_counts_bytes() {
        let total_bytes = Arc::new(AtomicU64::new(0));
        let hooks = TestHooks {
            total_bytes: Arc::clone(&total_bytes),
        };

        let future = std::future::ready(Ok::<Response<BoxBody<Bytes, std::io::Error>>, ()>(
            Response::new(make_body(b"abc")),
        ));
        let metered_future = MeteredFuture {
            future,
            metered_hooks: Some(hooks),
            traffic_reporting_threshold: ByteSize::b(0),
            _marker: std::marker::PhantomData,
        };

        let response = metered_future.await.expect("expected successful response");
        let mut body = response.into_body();

        let frame = body
            .frame()
            .await
            .expect("expected first frame")
            .expect("expected successful frame");
        assert_eq!(
            frame.data_ref().expect("expected data frame").remaining(),
            3
        );
        assert_eq!(total_bytes.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn metered_service_builds_hooks_and_counts_emitted_bytes() {
        let total_bytes = Arc::new(AtomicU64::new(0));
        let build_hooks_calls = Arc::new(AtomicUsize::new(0));
        let last_path = Arc::new(Mutex::new(None));

        let manager = TestManager {
            total_bytes: Arc::clone(&total_bytes),
            build_hooks_calls: Arc::clone(&build_hooks_calls),
            last_path: Arc::clone(&last_path),
        };
        let inner = TestInnerService {
            body_data: b"world!",
        };
        let mut service = MeteredService {
            inner,
            metered_manager: Arc::new(manager),
            traffic_reporting_threshold: ByteSize::b(0),
        };

        let request = Request::builder()
            .uri("http://localhost/stream")
            .body(())
            .expect("request should build");
        let response = service
            .call(request)
            .await
            .expect("service call should succeed");
        let mut body = response.into_body();

        let frame = body
            .frame()
            .await
            .expect("expected first frame")
            .expect("expected successful frame");
        assert_eq!(
            frame.data_ref().expect("expected data frame").remaining(),
            6
        );

        assert_eq!(build_hooks_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            last_path
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("path should be captured"),
            "/stream"
        );
        assert_eq!(total_bytes.load(Ordering::Relaxed), 6);
    }
}
