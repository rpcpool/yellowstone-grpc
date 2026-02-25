use {
    crate::metrics,
    bytes::{Buf, Bytes},
    http::{Request, Response},
    hyper::body::{Frame, SizeHint},
    pin_project::{pin_project, pinned_drop},
    std::{
        collections::HashMap,
        future::Future,
        pin::Pin,
        sync::{LazyLock, Mutex},
        task::{ready, Context, Poll},
    },
    tonic::codegen::{Body as HttpBody, Service, StdError},
    tower_layer::Layer,
};

pub const X_SUBSCRIPTION_ID_HEADER: &str = "x-subscription-id";

pub const UNKNOWN_SUBSCRIBER_ID: &str = "unknown";

static ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID: LazyLock<
    Mutex<HashMap<String /* subscriber_id */, u64 /* cumulative bytes */>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn increment_active_metered_bodies_for_subscriber_id(subscriber_id: &str) {
    let mut active_by_subscriber = ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID
        .lock()
        .expect("ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID mutex poisoned");
    active_by_subscriber
        .entry(subscriber_id.to_owned())
        .and_modify(|count| *count += 1)
        .or_insert(1);
}

fn decrement_active_metered_bodies_for_subscriber_id(subscriber_id: &str) -> bool {
    let mut active_by_subscriber = ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID
        .lock()
        .expect("ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID mutex poisoned");
    if let Some(count) = active_by_subscriber.get_mut(subscriber_id) {
        if *count > 1 {
            *count -= 1;
            false
        } else {
            active_by_subscriber.remove(subscriber_id);
            metrics::reset_grpc_service_outbound_bytes(subscriber_id);
            true
        }
    } else {
        true
    }
}

///
/// An extension trait for `http_body::Body` that provides additional combinators for working with gRPC response bodies in tonic services.
///
#[derive(Debug, Clone, Default)]
pub struct MeteredLayer;

impl MeteredLayer {
    pub const fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for MeteredLayer {
    type Service = MeteredService<S>;

    fn layer(&self, service: S) -> Self::Service {
        MeteredService { inner: service }
    }
}

///
/// A service wrapper that counts the total bytes emitted by the response body of a tonic service and exposes it via Prometheus metrics.
///
#[derive(Debug, Clone)]
pub struct MeteredService<S> {
    inner: S,
}

#[pin_project]
pub struct MeteredFuture<F, B, E> {
    #[pin]
    future: F,
    subscriber_id: String,
    _marker: std::marker::PhantomData<(B, E)>,
}

///
/// A wrapper around a tonic response body that counts the total bytes emitted and updates the corresponding Prometheus metric when dropped.
///
#[pin_project(PinnedDrop)]
pub struct MeteredBody<B> {
    #[pin]
    inner: B,
    subscriber_id: String,
}

#[pinned_drop]
impl<B> PinnedDrop for MeteredBody<B> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        decrement_active_metered_bodies_for_subscriber_id(this.subscriber_id);
    }
}

impl<B> HttpBody for MeteredBody<B>
where
    B: HttpBody,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.as_mut().project();
        match ready!(this.inner.as_mut().poll_frame(cx)) {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    metrics::add_grpc_service_outbound_bytes(
                        this.subscriber_id,
                        data.remaining() as u64,
                    );
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<F, B, E> Future for MeteredFuture<F, B, E>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: HttpBody + Send + 'static,
    B::Error: Into<StdError>,
{
    type Output = Result<Response<MeteredBody<B>>, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        let result = ready!(this.future.poll(cx));
        let subscriber_id = this.subscriber_id.clone();
        match result {
            Ok(response) => {
                let (parts, body) = response.into_parts();
                increment_active_metered_bodies_for_subscriber_id(&subscriber_id);
                let metered_body = MeteredBody {
                    inner: body,
                    subscriber_id,
                };
                Poll::Ready(Ok(Response::from_parts(parts, metered_body)))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for MeteredService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
    ResBody: HttpBody<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<StdError>,
{
    type Response = Response<MeteredBody<ResBody>>;
    type Error = S::Error;
    type Future = MeteredFuture<S::Future, ResBody, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let subscriber_id = request
            .headers()
            .get(X_SUBSCRIPTION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or(UNKNOWN_SUBSCRIBER_ID)
            .to_owned();
        let future = self.inner.call(request);
        MeteredFuture {
            future,
            subscriber_id,
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        futures::{future, stream},
        http::Uri,
        http_body_util::{combinators::BoxBody, BodyExt},
        hyper::body::Frame,
        std::{convert::Infallible, sync::LazyLock},
        tokio::sync::Mutex,
        tokio_util::{sync::CancellationToken, task::TaskTracker},
        tonic::codegen::Service,
    };

    static TEST_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[derive(Clone)]
    struct StubService {
        frames: Vec<Bytes>,
    }

    impl Service<Request<()>> for StubService {
        type Response = Response<BoxBody<Bytes, Infallible>>;
        type Error = Infallible;
        type Future = future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<()>) -> Self::Future {
            let frames = self
                .frames
                .clone()
                .into_iter()
                .map(|bytes| Ok::<Frame<Bytes>, Infallible>(Frame::data(bytes)));
            let body = http_body_util::StreamBody::new(stream::iter(frames)).boxed();
            future::ready(Ok(Response::new(body)))
        }
    }

    async fn ensure_metrics_registered() {
        let token = CancellationToken::new();
        let tracker = TaskTracker::new();
        let _ = metrics::PrometheusService::spawn(None, None, token, tracker).await;
    }

    fn outbound_bytes_for_subscriber_id(subscriber_id: &str) -> u64 {
        for metric_family in metrics::REGISTRY.gather() {
            if metric_family.name() == "yellowstone_grpc_service_outbound_bytes" {
                for metric in metric_family.get_metric() {
                    let matched = metric.get_label().iter().any(|label| {
                        label.name() == "subscriber_id" && label.value() == subscriber_id
                    });
                    if matched {
                        return metric.get_gauge().value() as u64;
                    }
                }
            }
        }
        0
    }

    fn clear_active_metered_bodies_map() {
        ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID
            .lock()
            .expect("ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID mutex poisoned")
            .clear();
    }

    #[tokio::test]
    async fn metered_service_counts_outbound_bytes_across_frames() {
        let _guard = TEST_MUTEX.lock().await;
        ensure_metrics_registered().await;
        metrics::reset_metrics();
        clear_active_metered_bodies_map();

        let mut service = MeteredLayer::new().layer(StubService {
            frames: vec![Bytes::from_static(b"abc"), Bytes::from_static(b"hello")],
        });
        let request = Request::builder()
            .uri(Uri::from_static("/geyser.Geyser/Subscribe"))
            .header("x-subscription-id", "sub-1")
            .body(())
            .expect("request build");

        let mut response = service.call(request).await.expect("service call");
        while let Some(frame) = response.body_mut().frame().await {
            frame.expect("frame");
        }

        assert_eq!(outbound_bytes_for_subscriber_id("sub-1"), 8);
        drop(response);
        assert_eq!(outbound_bytes_for_subscriber_id("sub-1"), 0);
    }

    #[tokio::test]
    async fn metered_service_keeps_counts_per_subscriber_id() {
        let _guard = TEST_MUTEX.lock().await;
        ensure_metrics_registered().await;
        metrics::reset_metrics();
        clear_active_metered_bodies_map();

        let mut service = MeteredLayer::new().layer(StubService {
            frames: vec![Bytes::from_static(b"xy")],
        });
        let request = Request::builder()
            .uri(Uri::from_static("/foo.Bar/Baz"))
            .header("x-subscription-id", "sub-2")
            .body(())
            .expect("request build");

        let mut response = service.call(request).await.expect("service call");
        while let Some(frame) = response.body_mut().frame().await {
            frame.expect("frame");
        }

        assert_eq!(outbound_bytes_for_subscriber_id("sub-2"), 2);
        drop(response);
        assert_eq!(outbound_bytes_for_subscriber_id("sub-2"), 0);
    }

    #[tokio::test]
    async fn metered_service_resets_on_drop_without_draining_stream() {
        let _guard = TEST_MUTEX.lock().await;
        ensure_metrics_registered().await;
        metrics::reset_metrics();
        clear_active_metered_bodies_map();

        let mut service = MeteredLayer::new().layer(StubService {
            frames: vec![Bytes::from_static(b"abc"), Bytes::from_static(b"def")],
        });
        let request = Request::builder()
            .uri(Uri::from_static("/geyser.Geyser/Subscribe"))
            .header("x-subscription-id", "sub-drop")
            .body(())
            .expect("request build");

        let mut response = service.call(request).await.expect("service call");
        let first_frame = response
            .body_mut()
            .frame()
            .await
            .expect("expected first frame")
            .expect("frame");
        assert!(first_frame.is_data());
        assert_eq!(outbound_bytes_for_subscriber_id("sub-drop"), 3);

        drop(response);
        assert_eq!(outbound_bytes_for_subscriber_id("sub-drop"), 0);
    }

    #[tokio::test]
    async fn metered_service_only_resets_when_last_body_drops_for_same_subscriber() {
        let _guard = TEST_MUTEX.lock().await;
        ensure_metrics_registered().await;
        metrics::reset_metrics();
        clear_active_metered_bodies_map();

        let mut service = MeteredLayer::new().layer(StubService {
            frames: vec![Bytes::from_static(b"aaa"), Bytes::from_static(b"bbb")],
        });
        let request1 = Request::builder()
            .uri(Uri::from_static("/geyser.Geyser/Subscribe"))
            .header("x-subscription-id", "sub-shared")
            .body(())
            .expect("request build");
        let request2 = Request::builder()
            .uri(Uri::from_static("/geyser.Geyser/Subscribe"))
            .header("x-subscription-id", "sub-shared")
            .body(())
            .expect("request build");

        let mut response1 = service.call(request1).await.expect("service call");
        let response2 = service.call(request2).await.expect("service call");

        let first_frame = response1
            .body_mut()
            .frame()
            .await
            .expect("expected first frame")
            .expect("frame");
        assert!(first_frame.is_data());
        assert_eq!(outbound_bytes_for_subscriber_id("sub-shared"), 3);

        drop(response1);
        assert_eq!(outbound_bytes_for_subscriber_id("sub-shared"), 3);

        drop(response2);
        assert_eq!(outbound_bytes_for_subscriber_id("sub-shared"), 0);
    }
}
