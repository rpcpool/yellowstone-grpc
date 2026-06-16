use {
    crate::server::ratelimit::window::TokenBucketRateLimiter,
    http::{header::RETRY_AFTER, Request, Response, StatusCode},
    pin_project::pin_project,
    std::{
        collections::HashMap,
        future::Future,
        task::{Context, Poll},
        time::{Duration, Instant},
    },
    tonic::codegen::{Body as HttpBody, Service, StdError},
};

#[pin_project(project = MethodRateLimitedFutureProj)]
/// Future returned by [`MethodRateLimitedService`].
///
/// - `Allowed` polls the wrapped inner service future.
/// - `Denied` resolves immediately with a synthetic `429` response.
pub enum MethodRateLimitedFuture<F, ResBody, E> {
    Allowed {
        #[pin]
        future: F,
    },
    Denied {
        result: Option<Result<Response<ResBody>, E>>,
    },
}

impl<F, ResBody, E> MethodRateLimitedFuture<F, ResBody, E> {
    fn denied(response: Response<ResBody>) -> Self {
        Self::Denied {
            result: Some(Ok(response)),
        }
    }
}

impl<F, ResBody, E> Future for MethodRateLimitedFuture<F, ResBody, E>
where
    F: Future<Output = Result<Response<ResBody>, E>>,
{
    type Output = Result<Response<ResBody>, E>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            MethodRateLimitedFutureProj::Allowed { future } => future.poll(cx),
            MethodRateLimitedFutureProj::Denied { result } => Poll::Ready(
                result
                    .take()
                    .expect("denied future polled after completion"),
            ),
        }
    }
}

pub trait RateLimiterStore {
    fn accept_req<B>(&self, request: &Request<B>, now: Instant) -> Result<(), Duration>;
}

impl<S, RL> MethodRateLimitedService<S, RL> {
    /// Creates a service that applies one shared limiter to all extracted keys.
    pub fn new(inner: S, rate_limiter: RL) -> Self {
        Self {
            inner,
            rate_limiter,
        }
    }
}

#[derive(Debug)]
pub struct MethodRateLimitedService<S, RL> {
    inner: S,
    rate_limiter: RL,
}

/// Backward-compatible alias for previous naming.
pub type MethodRatelimitedService<S, RL> = MethodRateLimitedService<S, RL>;

impl<S, RL, ReqBody, ResBody> Service<Request<ReqBody>> for MethodRateLimitedService<S, RL>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static + Unpin,
    ResBody: HttpBody + Send + 'static,
    ResBody::Error: Into<StdError>,
    RL: RateLimiterStore,
    ResBody: Default,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = MethodRateLimitedFuture<S::Future, ResBody, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let now = Instant::now();
        if let Err(retry_after) = self.rate_limiter.accept_req(&request, now) {
            let retry_after_secs = retry_after.as_secs().max(1).to_string();
            let response = Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header(RETRY_AFTER, retry_after_secs)
                .body(ResBody::default())
                .expect("failed to build rate-limited response");
            MethodRateLimitedFuture::denied(response)
        } else {
            MethodRateLimitedFuture::Allowed {
                future: self.inner.call(request),
            }
        }
    }
}

pub struct SlidingWindowRateLimiterStore<K> {
    active_window_map: TokenBucketRateLimiter<K>,
    last_window_update: HashMap<K, Instant>,
    /// Duration after which an inactive key is evicted from the limiter state.
    idle_timeout: Duration,
    // After how many calls to accept_req() should we perform a GC tick to evict inactive keys.
    gc_tick: usize,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        futures::executor::block_on,
        http_body_util::Empty,
        std::{
            convert::Infallible,
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
        },
    };

    #[derive(Clone, Copy)]
    enum StoreDecision {
        Allow,
        Deny(Duration),
    }

    struct TestStore {
        decision: StoreDecision,
        calls: Arc<AtomicUsize>,
    }

    impl TestStore {
        fn new(decision: StoreDecision, calls: Arc<AtomicUsize>) -> Self {
            Self { decision, calls }
        }
    }

    impl RateLimiterStore for TestStore {
        fn accept_req<B>(&self, _request: &Request<B>, _now: Instant) -> Result<(), Duration> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self.decision {
                StoreDecision::Allow => Ok(()),
                StoreDecision::Deny(retry_after) => Err(retry_after),
            }
        }
    }

    struct TestService {
        call_count: Arc<AtomicUsize>,
    }

    impl TestService {
        fn new(call_count: Arc<AtomicUsize>) -> Self {
            Self { call_count }
        }
    }

    impl Service<Request<()>> for TestService {
        type Response = Response<Empty<Bytes>>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<()>) -> Self::Future {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Empty::new())
                .expect("response build")))
        }
    }

    #[test]
    fn allowed_request_forwards_to_inner_service() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let limiter_calls = Arc::new(AtomicUsize::new(0));

        let mut svc = MethodRateLimitedService::new(
            TestService::new(Arc::clone(&call_count)),
            TestStore::new(StoreDecision::Allow, Arc::clone(&limiter_calls)),
        );

        let req = Request::builder().body(()).expect("request build");
        let resp = block_on(svc.call(req)).expect("request should succeed");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(limiter_calls.load(Ordering::Relaxed), 1);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn denied_request_returns_429_without_calling_inner_service() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let limiter_calls = Arc::new(AtomicUsize::new(0));

        let mut svc = MethodRateLimitedService::new(
            TestService::new(Arc::clone(&call_count)),
            TestStore::new(
                StoreDecision::Deny(Duration::from_secs(5)),
                Arc::clone(&limiter_calls),
            ),
        );

        let req = Request::builder().body(()).expect("request build");
        let resp = block_on(svc.call(req)).expect("denied response should still resolve");
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            resp.headers()
                .get(RETRY_AFTER)
                .expect("retry-after header expected"),
            "5"
        );
        assert_eq!(limiter_calls.load(Ordering::Relaxed), 1);
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn denied_request_retry_after_is_at_least_one_second() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let limiter_calls = Arc::new(AtomicUsize::new(0));

        let mut svc = MethodRateLimitedService::new(
            TestService::new(Arc::clone(&call_count)),
            TestStore::new(
                StoreDecision::Deny(Duration::from_millis(250)),
                Arc::clone(&limiter_calls),
            ),
        );

        let req = Request::builder().body(()).expect("request build");
        let resp = block_on(svc.call(req)).expect("denied response should still resolve");
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            resp.headers()
                .get(RETRY_AFTER)
                .expect("retry-after header expected"),
            "1"
        );
        assert_eq!(limiter_calls.load(Ordering::Relaxed), 1);
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
    }
}
