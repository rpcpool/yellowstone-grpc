use {
    crate::server::ratelimit::window::{SlidingWindowRateLimiter, WindowDecision},
    http::{Request, Response, StatusCode, header::RETRY_AFTER},
    pin_project::pin_project,
    std::{
        collections::HashMap,
        future::Future,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::{Duration, Instant},
    },
    tonic::codegen::{Body as HttpBody, Service, StdError},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Key material used by method-level limiters.
///
/// Combines the extracted principal key with its resolved tier.
pub struct TieredRateLimitKey<K> {
    pub key: K,
    pub tier: String,
}

#[derive(Debug)]
enum LimiterStore<K> {
    Shared(Arc<Mutex<SlidingWindowRateLimiter<K>>>),
    ByTier(Arc<Mutex<HashMap<String, SlidingWindowRateLimiter<K>>>>),
}

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
            MethodRateLimitedFutureProj::Denied { result } => {
                Poll::Ready(result.take().expect("denied future polled after completion"))
            }
        }
    }
}

pub trait RateLimitKeyExtractor {
    type Key: Clone + PartialEq + Eq + std::hash::Hash;

    /// Extracts principal key and tier from the incoming request.
    fn extract_key_tier_pair<ReqBody>(&self, request: &Request<ReqBody>) -> TieredRateLimitKey<Self::Key>;
}

impl<S, KE, K> MethodRateLimitedService<S, KE, K> {
    /// Creates a service that applies one shared limiter to all extracted keys.
    pub fn new(
        inner: S,
        key_extractor: KE,
        shared_limiter: Arc<Mutex<SlidingWindowRateLimiter<K>>>,
    ) -> Self {
        Self {
            inner,
            key_extractor,
            limiters: LimiterStore::Shared(shared_limiter),
        }
    }

    /// Creates a service with independent limiters per tier.
    pub fn new_tiered(
        inner: S,
        key_extractor: KE,
        limiters_by_tier: Arc<Mutex<HashMap<String, SlidingWindowRateLimiter<K>>>>,
    ) -> Self {
        Self {
            inner,
            key_extractor,
            limiters: LimiterStore::ByTier(limiters_by_tier),
        }
    }
}

#[derive(Debug)]
pub struct MethodRateLimitedService<S, KE, K> {
    inner: S,
    key_extractor: KE,
    limiters: LimiterStore<K>,
}

/// Backward-compatible alias for previous naming.
pub type MethodRatelimitedService<S, KE, K> = MethodRateLimitedService<S, KE, K>;

impl<S, KE, K, ReqBody, ResBody> Service<Request<ReqBody>> for MethodRateLimitedService<S, KE, K>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static + Unpin,
    ResBody: HttpBody + Send + 'static,
    ResBody::Error: Into<StdError>,
    KE: RateLimitKeyExtractor<Key = K>,
    K: Eq + std::hash::Hash,
    ResBody: Default,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = MethodRateLimitedFuture<S::Future, ResBody, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let TieredRateLimitKey { key, tier } = self.key_extractor.extract_key_tier_pair(&request);
        let now = Instant::now();

        let decision = match &self.limiters {
            LimiterStore::Shared(shared_limiter) => {
                let mut guard = shared_limiter.lock().expect("shared limiter mutex poisoned");
                guard.check_at(key, now)
            }
            LimiterStore::ByTier(by_tier) => {
                let mut guard = by_tier.lock().expect("tiered limiter mutex poisoned");
                if let Some(limiter) = guard.get_mut(&tier) {
                    limiter.check_at(key, now)
                } else {
                    // Unknown tiers are allowed by default so callers can phase in limits.
                    WindowDecision {
                        allowed: true,
                        remaining: u64::MAX,
                        retry_after: Duration::ZERO,
                    }
                }
            }
        };

        if decision.allowed {
            return MethodRateLimitedFuture::Allowed {
                future: self.inner.call(request),
            };
        }

        let retry_after_secs = decision.retry_after.as_secs().max(1).to_string();
        let response = Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(RETRY_AFTER, retry_after_secs)
            .body(ResBody::default())
            .expect("failed to build rate-limited response");

        MethodRateLimitedFuture::denied(response)
    }
}

#[cfg(test)]
mod tests {
    use {
        bytes::Bytes,
        futures::executor::block_on,
        super::*,
        http_body_util::Empty,
        std::sync::{Arc, atomic::{AtomicUsize, Ordering}},
        std::convert::Infallible,
    };

    struct TestExtractor;

    impl RateLimitKeyExtractor for TestExtractor {
        type Key = String;

        fn extract_key_tier_pair<ReqBody>(&self, request: &Request<ReqBody>) -> TieredRateLimitKey<Self::Key> {
            TieredRateLimitKey {
                key: request
                    .headers()
                    .get("x-key")
                    .expect("x-key required")
                    .to_str()
                    .expect("x-key utf8")
                    .to_owned(),
                tier: request
                    .headers()
                    .get("x-tier")
                    .expect("x-tier required")
                    .to_str()
                    .expect("x-tier utf8")
                    .to_owned(),
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
            std::future::ready(Ok(Response::builder().status(StatusCode::OK).body(Empty::new()).expect("response build")))
        }
    }

    #[test]
    fn tiered_mode_selects_tier_specific_limiter() {
        let call_count = Arc::new(AtomicUsize::new(0));

        let mut per_tier = HashMap::new();
        per_tier.insert(
            "free".to_owned(),
            SlidingWindowRateLimiter::<String>::new(1, Duration::from_secs(10), 10),
        );
        per_tier.insert(
            "pro".to_owned(),
            SlidingWindowRateLimiter::<String>::new(2, Duration::from_secs(10), 10),
        );

        let mut svc = MethodRateLimitedService::new_tiered(
            TestService::new(Arc::clone(&call_count)),
            TestExtractor,
            Arc::new(Mutex::new(per_tier)),
        );

        let req_free_1 = Request::builder()
            .header("x-tier", "free")
            .header("x-key", "alice")
            .body(())
            .expect("request build");
        let resp1 = block_on(svc.call(req_free_1)).expect("free request 1 should succeed");
        assert_eq!(resp1.status(), StatusCode::OK);

        let req_free_2 = Request::builder()
            .header("x-tier", "free")
            .header("x-key", "alice")
            .body(())
            .expect("request build");
        let resp2 = block_on(svc.call(req_free_2)).expect("denied response should still be Ok transport-wise");
        assert_eq!(resp2.status(), StatusCode::TOO_MANY_REQUESTS);

        let req_pro_1 = Request::builder()
            .header("x-tier", "pro")
            .header("x-key", "alice")
            .body(())
            .expect("request build");
        let resp3 = block_on(svc.call(req_pro_1)).expect("pro request should be allowed");
        assert_eq!(resp3.status(), StatusCode::OK);

        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn unknown_tier_fails_open_and_forwards() {
        let call_count = Arc::new(AtomicUsize::new(0));

        let mut wrapped = MethodRateLimitedService::new_tiered(
            TestService::new(Arc::clone(&call_count)),
            TestExtractor,
            Arc::new(Mutex::new(HashMap::new())),
        );

        let req = Request::builder()
            .header("x-tier", "unknown")
            .header("x-key", "alice")
            .body(())
            .expect("request build");
        let resp = block_on(wrapped.call(req)).expect("unknown tier should fail open");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }
}
