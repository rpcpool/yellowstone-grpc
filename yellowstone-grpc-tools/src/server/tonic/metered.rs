use {
    bytes::Buf,
    bytesize::ByteSize,
    http::{request::Parts, Request, Response},
    hyper::body::{Frame, SizeHint},
    pin_project::{pin_project, pinned_drop},
    std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{ready, Context, Poll},
        time::{Instant, SystemTime},
    },
    tonic::codegen::{Body as HttpBody, Service, StdError},
    tower_layer::Layer,
};

pub const DEFAULT_TRAFFIC_REPORTING_THRESHOLD: ByteSize = ByteSize::kib(32);

/// Hooks fanout used by [`StackMeteredManager`].
#[derive(Debug, Clone)]
pub struct StackMeteredHooks<MH1, MH2> {
    hooks1: MH1,
    hooks2: MH2,
}

impl<MH1, MH2> MeteredBandwidthHooks for StackMeteredHooks<MH1, MH2>
where
    MH1: MeteredBandwidthHooks,
    MH2: MeteredBandwidthHooks,
{
    fn on_emit_bytes(&mut self, byte_count: u64, now: Instant, system_now: SystemTime) {
        self.hooks1.on_emit_bytes(byte_count, now, system_now);
        self.hooks2.on_emit_bytes(byte_count, now, system_now);
    }
}

impl<MH> MeteredBandwidthHooks for Option<MH>
where
    MH: MeteredBandwidthHooks,
{
    fn on_emit_bytes(&mut self, byte_count: u64, now: Instant, system_now: SystemTime) {
        if let Some(hooks) = self.as_mut() {
            hooks.on_emit_bytes(byte_count, now, system_now);
        }
    }
}

/// Manager composition that forwards hook construction to both managers.
#[derive(Debug, Clone)]
pub struct StackMeteredManager<MM1, MM2> {
    manager1: MM1,
    manager2: MM2,
}

impl<MM1, MM2> StackMeteredManager<MM1, MM2> {
    pub fn new(manager1: MM1, manager2: MM2) -> Self {
        Self { manager1, manager2 }
    }
}

impl<MM1, MM2> MeteredManager for StackMeteredManager<MM1, MM2>
where
    MM1: MeteredManager,
    MM2: MeteredManager,
{
    type BandwidthMeteredHooks =
        StackMeteredHooks<Option<MM1::BandwidthMeteredHooks>, Option<MM2::BandwidthMeteredHooks>>;

    fn build_hooks(&self, parts: &Parts) -> Option<Self::BandwidthMeteredHooks> {
        let hooks1 = self.manager1.build_hooks(parts);
        let hooks2 = self.manager2.build_hooks(parts);

        if hooks1.is_none() && hooks2.is_none() {
            return None;
        }

        Some(StackMeteredHooks { hooks1, hooks2 })
    }

    fn on_method(&self, method: &Parts) {
        self.manager1.on_method(method);
        self.manager2.on_method(method);
    }
}

pub trait MeteredBandwidthHooks {
    /// Records bytes emitted by a response body frame.
    ///
    /// Implementations can forward this event to metrics systems, tracing,
    /// or request-scoped accounting.
    ///
    /// # Arguments
    /// - `byte_count`: Number of payload bytes emitted by the current frame.
    /// - `now`: [`Instant`] sampled when the frame was observed.
    fn on_emit_bytes(&mut self, byte_count: u64, now: Instant, system_now: SystemTime);
}

pub trait MeteredManager {
    /// Concrete hooks type produced for each request.
    type BandwidthMeteredHooks: MeteredBandwidthHooks + Send + Sync + 'static;

    /// Builds per-request hooks used to meter the response body.
    ///
    /// # Parameters
    /// - `parts`: Incoming request parts, including headers and URI path.
    ///
    /// # Returns
    /// A hooks instance attached to the response body wrapper.
    fn build_hooks(&self, parts: &Parts) -> Option<Self::BandwidthMeteredHooks>;

    fn stack<Next>(self, next: Next) -> StackMeteredManager<Self, Next>
    where
        Self: Sized,
        Next: MeteredManager,
    {
        StackMeteredManager::new(self, next)
    }

    ///
    /// Invoked when a request is received, allowing the manager to inspect the request method and path.
    ///
    fn on_method(&self, _parts: &Parts) {
        // Default implementation does nothing.
    }
}

impl<MM> MeteredManager for Option<MM>
where
    MM: MeteredManager,
{
    type BandwidthMeteredHooks = MM::BandwidthMeteredHooks;

    fn build_hooks(&self, parts: &Parts) -> Option<Self::BandwidthMeteredHooks> {
        self.as_ref().and_then(|manager| manager.build_hooks(parts))
    }

    fn on_method(&self, method: &Parts) {
        if let Some(manager) = self.as_ref() {
            manager.on_method(method);
        }
    }
}

/// Tower layer that wraps services with byte metering for response bodies.
///
/// The layer clones a shared manager and applies a [`MeteredService`] wrapper
/// to each inner service instance.
#[derive(Debug, Clone)]
pub struct MeteredBandwidthLayer<MM> {
    metered_manager: Arc<MM>,
    traffic_reporting_threshold: ByteSize,
}

impl<MM> MeteredBandwidthLayer<MM> {
    /// Creates a new metering layer from a manager implementation.
    pub fn new(metered_manager: MM, traffic_reporting_threshold: ByteSize) -> Self {
        Self {
            metered_manager: Arc::new(metered_manager),
            traffic_reporting_threshold,
        }
    }
}

impl<S, MM> Layer<S> for MeteredBandwidthLayer<MM>
where
    MM: MeteredManager,
{
    type Service = MeteredBandwidthService<S, MM>;

    /// Wraps the provided service with metering behavior.
    fn layer(&self, service: S) -> Self::Service {
        MeteredBandwidthService {
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
pub struct MeteredBandwidthService<S, MM> {
    inner: S,
    metered_manager: Arc<MM>,
    traffic_reporting_threshold: ByteSize,
}

/// Future returned by [`MeteredService`] that wraps successful responses.
///
/// On completion, this future replaces the original response body with a
/// [`MeteredBody`] that reports emitted byte counts through request-scoped hooks.
#[pin_project]
pub struct MeteredBandwidthFuture<F, B, E, MH> {
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
#[pin_project(PinnedDrop)]
pub struct MeteredBandwidthBody<B, MH: MeteredBandwidthHooks> {
    #[pin]
    inner: B,
    metered_hooks: Option<MH>,
    traffic_reporting_threshold: ByteSize,
    cumulative_bytes: u64,
}

#[pinned_drop]
impl<B, MH> PinnedDrop for MeteredBandwidthBody<B, MH>
where
    MH: MeteredBandwidthHooks,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if let Some(hooks) = this.metered_hooks.as_mut() {
            hooks.on_emit_bytes(*this.cumulative_bytes, Instant::now(), SystemTime::now());
        }
    }
}

impl<B, MH> HttpBody for MeteredBandwidthBody<B, MH>
where
    B: HttpBody,
    MH: MeteredBandwidthHooks,
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
                        if let Some(hooks) = this.metered_hooks.as_mut() {
                            hooks.on_emit_bytes(
                                *this.cumulative_bytes,
                                Instant::now(),
                                SystemTime::now(),
                            );
                        }
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

impl<F, B, E, MH> Future for MeteredBandwidthFuture<F, B, E, MH>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: HttpBody + Send + 'static,
    B::Error: Into<StdError>,
    MH: MeteredBandwidthHooks,
{
    type Output = Result<Response<MeteredBandwidthBody<B, MH>>, E>;

    /// Polls the inner future and wraps successful response bodies.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        let result = ready!(this.future.poll(cx));
        match result {
            Ok(response) => {
                let (parts, body) = response.into_parts();
                // increment_active_metered_bodies_for_subscriber_and_path(&subscriber_id, &uri_path);
                let metered_body = MeteredBandwidthBody {
                    inner: body,
                    metered_hooks: this.metered_hooks.take(),
                    traffic_reporting_threshold: *this.traffic_reporting_threshold,
                    cumulative_bytes: 0,
                };
                Poll::Ready(Ok(Response::from_parts(parts, metered_body)))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl<S, ReqBody, ResBody, MM> Service<Request<ReqBody>> for MeteredBandwidthService<S, MM>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
    ResBody: HttpBody + Send + 'static,
    ResBody::Error: Into<StdError>,
    MM: MeteredManager + Send + Sync + 'static,
{
    type Response = Response<MeteredBandwidthBody<ResBody, MM::BandwidthMeteredHooks>>;
    type Error = S::Error;
    type Future = MeteredBandwidthFuture<S::Future, ResBody, S::Error, MM::BandwidthMeteredHooks>;

    /// Delegates readiness to the wrapped inner service.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// Builds request hooks and delegates request execution to the inner service.
    ///
    /// The returned future will wrap successful responses with [`MeteredBody`]
    /// so byte emission can be observed frame by frame.
    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let (parts, body) = request.into_parts();
        self.metered_manager.on_method(&parts);
        let hooks = self.metered_manager.build_hooks(&parts);
        let request = Request::from_parts(parts, body);
        let future = self.inner.call(request);
        MeteredBandwidthFuture {
            future,
            metered_hooks: hooks,
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

    impl MeteredBandwidthHooks for TestHooks {
        fn on_emit_bytes(&mut self, byte_count: u64, _now: Instant, _system_now: SystemTime) {
            self.total_bytes.fetch_add(byte_count, Ordering::Relaxed);
        }
    }

    struct TestManager {
        total_bytes: Arc<AtomicU64>,
        build_hooks_calls: Arc<AtomicUsize>,
        last_path: Arc<Mutex<Option<String>>>,
    }

    impl MeteredManager for TestManager {
        type BandwidthMeteredHooks = TestHooks;

        fn build_hooks(&self, parts: &Parts) -> Option<Self::BandwidthMeteredHooks> {
            self.build_hooks_calls.fetch_add(1, Ordering::Relaxed);
            *self.last_path.lock().expect("poisoned mutex") = Some(parts.uri.path().to_owned());
            Some(TestHooks {
                total_bytes: Arc::clone(&self.total_bytes),
            })
        }
    }

    struct TestManagerWithMethodHooks {
        total_bytes: Arc<AtomicU64>,
        build_hooks_calls: Arc<AtomicUsize>,
        method_calls: Arc<AtomicUsize>,
        last_build_path: Arc<Mutex<Option<String>>>,
        last_method_path: Arc<Mutex<Option<String>>>,
    }

    impl MeteredManager for TestManagerWithMethodHooks {
        type BandwidthMeteredHooks = TestHooks;

        fn build_hooks(&self, parts: &Parts) -> Option<Self::BandwidthMeteredHooks> {
            self.build_hooks_calls.fetch_add(1, Ordering::Relaxed);
            *self.last_build_path.lock().expect("poisoned mutex") =
                Some(parts.uri.path().to_owned());
            Some(TestHooks {
                total_bytes: Arc::clone(&self.total_bytes),
            })
        }

        fn on_method(&self, method: &Parts) {
            self.method_calls.fetch_add(1, Ordering::Relaxed);
            *self.last_method_path.lock().expect("poisoned mutex") =
                Some(method.uri.path().to_owned());
        }
    }

    struct NoHooksManager;

    impl MeteredManager for NoHooksManager {
        type BandwidthMeteredHooks = TestHooks;

        fn build_hooks(&self, _parts: &Parts) -> Option<Self::BandwidthMeteredHooks> {
            None
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

        let mut body = MeteredBandwidthBody {
            inner: Full::new(Bytes::from_static(b"hello")),
            metered_hooks: Some(hooks),
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
        let metered_future = MeteredBandwidthFuture {
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
        let mut service = MeteredBandwidthService {
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

    #[tokio::test]
    async fn metered_manager_stack_api_fanouts_to_all_managers() {
        let total_bytes_1 = Arc::new(AtomicU64::new(0));
        let build_hooks_calls_1 = Arc::new(AtomicUsize::new(0));
        let last_path_1 = Arc::new(Mutex::new(None));

        let total_bytes_2 = Arc::new(AtomicU64::new(0));
        let build_hooks_calls_2 = Arc::new(AtomicUsize::new(0));
        let last_path_2 = Arc::new(Mutex::new(None));

        let total_bytes_3 = Arc::new(AtomicU64::new(0));
        let build_hooks_calls_3 = Arc::new(AtomicUsize::new(0));
        let last_path_3 = Arc::new(Mutex::new(None));

        let manager_1 = TestManager {
            total_bytes: Arc::clone(&total_bytes_1),
            build_hooks_calls: Arc::clone(&build_hooks_calls_1),
            last_path: Arc::clone(&last_path_1),
        };
        let manager_2 = TestManager {
            total_bytes: Arc::clone(&total_bytes_2),
            build_hooks_calls: Arc::clone(&build_hooks_calls_2),
            last_path: Arc::clone(&last_path_2),
        };
        let manager_3 = TestManager {
            total_bytes: Arc::clone(&total_bytes_3),
            build_hooks_calls: Arc::clone(&build_hooks_calls_3),
            last_path: Arc::clone(&last_path_3),
        };

        let composed_manager = manager_1.stack(manager_2).stack(manager_3);

        let inner = TestInnerService {
            body_data: b"fanout",
        };
        let mut service = MeteredBandwidthService {
            inner,
            metered_manager: Arc::new(composed_manager),
            traffic_reporting_threshold: ByteSize::b(0),
        };

        let request = Request::builder()
            .uri("http://localhost/fluent")
            .body(())
            .expect("request should build");
        let response = service
            .call(request)
            .await
            .expect("service call should succeed");
        let mut body = response.into_body();
        let _ = body
            .frame()
            .await
            .expect("expected first frame")
            .expect("expected successful frame");

        assert_eq!(build_hooks_calls_1.load(Ordering::Relaxed), 1);
        assert_eq!(build_hooks_calls_2.load(Ordering::Relaxed), 1);
        assert_eq!(build_hooks_calls_3.load(Ordering::Relaxed), 1);
        assert_eq!(
            last_path_1
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("path should be captured"),
            "/fluent"
        );
        assert_eq!(
            last_path_2
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("path should be captured"),
            "/fluent"
        );
        assert_eq!(
            last_path_3
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("path should be captured"),
            "/fluent"
        );

        assert_eq!(total_bytes_1.load(Ordering::Relaxed), 6);
        assert_eq!(total_bytes_2.load(Ordering::Relaxed), 6);
        assert_eq!(total_bytes_3.load(Ordering::Relaxed), 6);
    }

    #[test]
    fn stack_metered_manager_new_fanouts_method_and_hooks() {
        let total_bytes_1 = Arc::new(AtomicU64::new(0));
        let build_hooks_calls_1 = Arc::new(AtomicUsize::new(0));
        let method_calls_1 = Arc::new(AtomicUsize::new(0));
        let last_build_path_1 = Arc::new(Mutex::new(None));
        let last_method_path_1 = Arc::new(Mutex::new(None));

        let total_bytes_2 = Arc::new(AtomicU64::new(0));
        let build_hooks_calls_2 = Arc::new(AtomicUsize::new(0));
        let method_calls_2 = Arc::new(AtomicUsize::new(0));
        let last_build_path_2 = Arc::new(Mutex::new(None));
        let last_method_path_2 = Arc::new(Mutex::new(None));

        let manager_1 = TestManagerWithMethodHooks {
            total_bytes: Arc::clone(&total_bytes_1),
            build_hooks_calls: Arc::clone(&build_hooks_calls_1),
            method_calls: Arc::clone(&method_calls_1),
            last_build_path: Arc::clone(&last_build_path_1),
            last_method_path: Arc::clone(&last_method_path_1),
        };
        let manager_2 = TestManagerWithMethodHooks {
            total_bytes: Arc::clone(&total_bytes_2),
            build_hooks_calls: Arc::clone(&build_hooks_calls_2),
            method_calls: Arc::clone(&method_calls_2),
            last_build_path: Arc::clone(&last_build_path_2),
            last_method_path: Arc::clone(&last_method_path_2),
        };

        let stacked = StackMeteredManager::new(manager_1, manager_2);
        let request = Request::builder()
            .uri("http://localhost/direct")
            .body(())
            .expect("request should build");
        let (parts, _) = request.into_parts();

        stacked.on_method(&parts);
        let mut hooks = stacked
            .build_hooks(&parts)
            .expect("stacked manager should return hooks");
        hooks.on_emit_bytes(7, Instant::now(), SystemTime::now());

        assert_eq!(method_calls_1.load(Ordering::Relaxed), 1);
        assert_eq!(method_calls_2.load(Ordering::Relaxed), 1);
        assert_eq!(build_hooks_calls_1.load(Ordering::Relaxed), 1);
        assert_eq!(build_hooks_calls_2.load(Ordering::Relaxed), 1);

        assert_eq!(
            last_method_path_1
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("method path should be captured"),
            "/direct"
        );
        assert_eq!(
            last_method_path_2
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("method path should be captured"),
            "/direct"
        );
        assert_eq!(
            last_build_path_1
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("build path should be captured"),
            "/direct"
        );
        assert_eq!(
            last_build_path_2
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("build path should be captured"),
            "/direct"
        );

        assert_eq!(total_bytes_1.load(Ordering::Relaxed), 7);
        assert_eq!(total_bytes_2.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn stack_metered_manager_returns_some_if_one_manager_has_hooks() {
        let total_bytes = Arc::new(AtomicU64::new(0));
        let build_hooks_calls = Arc::new(AtomicUsize::new(0));
        let last_path = Arc::new(Mutex::new(None));

        let manager_with_hooks = TestManager {
            total_bytes: Arc::clone(&total_bytes),
            build_hooks_calls: Arc::clone(&build_hooks_calls),
            last_path: Arc::clone(&last_path),
        };

        let stacked = StackMeteredManager::new(manager_with_hooks, NoHooksManager);
        let request = Request::builder()
            .uri("http://localhost/partial")
            .body(())
            .expect("request should build");
        let (parts, _) = request.into_parts();

        let mut hooks = stacked
            .build_hooks(&parts)
            .expect("stacked manager should return Some when one side has hooks");
        hooks.on_emit_bytes(9, Instant::now(), SystemTime::now());

        assert_eq!(build_hooks_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            last_path
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("path should be captured"),
            "/partial"
        );
        assert_eq!(total_bytes.load(Ordering::Relaxed), 9);
    }

    #[tokio::test]
    async fn optional_metered_manager_none_is_noop() {
        let maybe_manager: Option<TestManager> = None;
        let inner = TestInnerService {
            body_data: b"optional",
        };
        let mut service = MeteredBandwidthService {
            inner,
            metered_manager: Arc::new(maybe_manager),
            traffic_reporting_threshold: ByteSize::b(0),
        };

        let request = Request::builder()
            .uri("http://localhost/optional-none")
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
            8
        );
    }

    #[tokio::test]
    async fn optional_metered_manager_some_forwards_hooks() {
        let total_bytes = Arc::new(AtomicU64::new(0));
        let build_hooks_calls = Arc::new(AtomicUsize::new(0));
        let last_path = Arc::new(Mutex::new(None));

        let maybe_manager = Some(TestManager {
            total_bytes: Arc::clone(&total_bytes),
            build_hooks_calls: Arc::clone(&build_hooks_calls),
            last_path: Arc::clone(&last_path),
        });

        let inner = TestInnerService {
            body_data: b"optional",
        };
        let mut service = MeteredBandwidthService {
            inner,
            metered_manager: Arc::new(maybe_manager),
            traffic_reporting_threshold: ByteSize::b(0),
        };

        let request = Request::builder()
            .uri("http://localhost/optional-some")
            .body(())
            .expect("request should build");
        let response = service
            .call(request)
            .await
            .expect("service call should succeed");
        let mut body = response.into_body();
        let _ = body
            .frame()
            .await
            .expect("expected first frame")
            .expect("expected successful frame");

        assert_eq!(build_hooks_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            last_path
                .lock()
                .expect("poisoned mutex")
                .as_deref()
                .expect("path should be captured"),
            "/optional-some"
        );
        assert_eq!(total_bytes.load(Ordering::Relaxed), 8);
    }
}
