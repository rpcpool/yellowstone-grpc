use {
    crate::metrics,
    dashmap::DashMap,
    std::{
        net::IpAddr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    },
    yellowstone_grpc_tools::server::{
        tonic::{interceptor::HttpInterceptor, ratelimit::transport::RatelimitedCallbacks},
        window::TokenBucketRateLimiter,
    },
};

pub const DEFAULT_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10);
pub const DEFAULT_RATE_LIMIT_MAX_HITS_FALLBACK: u64 = 1000;
pub const DEFAULT_METHOD_RATELIMIT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
pub const DEFAULT_METHOD_RATELIMIT_GC_TICK: usize = 1024;

pub struct PrometheusRatelimitCallbacks;

impl RatelimitedCallbacks for PrometheusRatelimitCallbacks {
    fn on_rate_limit_exceeded(&self, remote_peer_ip: Option<IpAddr>) {
        metrics::incr_ip_conncur_rate_limit_exceeded(remote_peer_ip.map(|ip| ip.to_string()));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MethodRatelimitKey {
    subscriber_id: String,
    method_name: String,
    /// The ratelimit value used because if we want to support SIGHUP or other dynamic config reloads, we need to ensure that the key is unique per ratelimit value. Otherwise, if the ratelimit value changes for a given subscriber/method, we could end up with a stale limiter in the map that doesn't reflect the new ratelimit.
    /// We expect ratelimit to not change often and not for all users at the same time, so this should be a reasonable tradeoff to avoid having to implement a more complex dynamic config reload mechanism.
    ratelimit: i32,
}

#[derive(Debug, Clone)]
struct MethodWindowState {
    limiter: TokenBucketRateLimiter,
    last_seen: std::time::Instant,
}

#[derive(Debug)]
struct SharedMethodRatelimiterState {
    active_windows_map: DashMap<MethodRatelimitKey, MethodWindowState>,
    request_counter: AtomicUsize,
}

#[derive(Debug, Clone)]
pub struct MethodRatelimiter {
    shared: Arc<SharedMethodRatelimiterState>,
    window: Duration,
    max_hits_fallback: u64,
    idle_timeout: Duration,
    gc_tick: usize,
}

impl MethodRatelimiter {
    pub fn new(max_hits_fallback: u64, window: Duration) -> Self {
        Self::with_gc(
            max_hits_fallback,
            window,
            DEFAULT_METHOD_RATELIMIT_IDLE_TIMEOUT,
            DEFAULT_METHOD_RATELIMIT_GC_TICK,
        )
    }

    pub fn with_gc(
        max_hits_fallback: u64,
        window: Duration,
        idle_timeout: Duration,
        gc_tick: usize,
    ) -> Self {
        Self {
            shared: Arc::new(SharedMethodRatelimiterState {
                active_windows_map: DashMap::new(),
                request_counter: AtomicUsize::new(0),
            }),
            window,
            max_hits_fallback,
            idle_timeout,
            gc_tick,
        }
    }

    fn maybe_run_gc(&mut self, now: std::time::Instant) {
        if self.gc_tick == 0 {
            return;
        }

        let request_counter = self
            .shared
            .request_counter
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if !request_counter.is_multiple_of(self.gc_tick) {
            return;
        }

        self.shared
            .active_windows_map
            .retain(|_, state| now.saturating_duration_since(state.last_seen) <= self.idle_timeout);
    }

    #[cfg(test)]
    fn active_window_count(&self) -> usize {
        self.shared.active_windows_map.len()
    }

    fn inner_call(
        &mut self,
        request: http::Request<()>,
        now: std::time::Instant,
    ) -> Result<http::Request<()>, tonic::Status> {
        self.maybe_run_gc(now);

        let Some(subscription_info) = request.extensions().get::<crate::auth::SubscriptionInfo>()
        else {
            return Ok(request);
        };

        let window_key = MethodRatelimitKey {
            subscriber_id: subscription_info.subscription_id.clone(),
            method_name: request.uri().path().to_string(),
            ratelimit: subscription_info
                .ratelimits
                .as_ref()
                .and_then(|ratelimits| {
                    ratelimits
                        .methods
                        .get(request.uri().path())
                        .or(Some(&ratelimits.default))
                })
                .copied()
                .unwrap_or(-1),
        };

        let mut state = self
            .shared
            .active_windows_map
            .entry(window_key)
            .or_insert_with(|| {
                let limiter = subscription_info
                    .ratelimits
                    .as_ref()
                    .and_then(|ratelimits| {
                        ratelimits
                            .methods
                            .get(request.uri().path())
                            .or(Some(&ratelimits.default))
                    })
                    .map(|ratelimit| {
                        let casted_ratelimit = if *ratelimit < 0 {
                            u64::MAX // effectively no limit
                        } else {
                            *ratelimit as u64
                        };

                        TokenBucketRateLimiter::new(casted_ratelimit, self.window)
                    })
                    .unwrap_or_else(|| {
                        TokenBucketRateLimiter::new(self.max_hits_fallback, self.window)
                    });

                MethodWindowState {
                    limiter,
                    last_seen: now,
                }
            });

        let decision = state.limiter.check_at(now);
        state.last_seen = now;

        decision
            .allowed
            .then_some(())
            .ok_or_else(|| tonic::Status::resource_exhausted("rate limit exceeded"))?;

        Ok(request)
    }
}

impl Default for MethodRatelimiter {
    fn default() -> Self {
        Self::new(
            DEFAULT_RATE_LIMIT_MAX_HITS_FALLBACK,
            DEFAULT_RATE_LIMIT_WINDOW,
        )
    }
}

impl HttpInterceptor for MethodRatelimiter {
    fn call(&mut self, request: http::Request<()>) -> Result<http::Request<()>, tonic::Status> {
        self.inner_call(request, std::time::Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::auth::{SubscriptionInfo, SubscriptionRateLimits},
        std::collections::HashMap,
    };

    fn make_request(path: &str, subscription_info: Option<SubscriptionInfo>) -> http::Request<()> {
        let mut request = http::Request::builder()
            .uri(path)
            .body(())
            .expect("request build");

        if let Some(subscription_info) = subscription_info {
            request.extensions_mut().insert(subscription_info);
        }

        request
    }

    #[test]
    fn call_without_subscription_info_passes_through() {
        let mut limiter = MethodRatelimiter::new(1, Duration::from_secs(10));
        let request = make_request("/geyser.Geyser/Subscribe", None);

        let now = std::time::Instant::now();
        let result = limiter.inner_call(request, now);
        assert!(result.is_ok());
        assert_eq!(limiter.active_window_count(), 0);
    }

    #[test]
    fn uses_method_specific_limit_when_present() {
        let mut limiter = MethodRatelimiter::new(10, Duration::from_secs(10));
        let now = std::time::Instant::now();
        let subscription_info = SubscriptionInfo {
            subscription_id: "sub-1".to_string(),
            ratelimits: Some(SubscriptionRateLimits {
                default: 10,
                methods: HashMap::from([("/geyser.Geyser/Subscribe".to_string(), 1)]),
            }),
        };

        let req1 = make_request("/geyser.Geyser/Subscribe", Some(subscription_info.clone()));
        let req2 = make_request("/geyser.Geyser/Subscribe", Some(subscription_info));

        assert!(limiter.inner_call(req1, now).is_ok());
        let denied = limiter
            .inner_call(req2, now)
            .expect_err("second call should be denied");
        assert_eq!(denied.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn falls_back_to_default_method_limit_when_method_missing() {
        let mut limiter = MethodRatelimiter::new(10, Duration::from_secs(10));
        let now = std::time::Instant::now();
        let subscription_info = SubscriptionInfo {
            subscription_id: "sub-1".to_string(),
            ratelimits: Some(SubscriptionRateLimits {
                default: 1,
                methods: HashMap::new(),
            }),
        };

        let req1 = make_request("/geyser.Geyser/Unknown", Some(subscription_info.clone()));
        let req2 = make_request("/geyser.Geyser/Unknown", Some(subscription_info));

        assert!(limiter.inner_call(req1, now).is_ok());
        let denied = limiter
            .inner_call(req2, now)
            .expect_err("second call should be denied");
        assert_eq!(denied.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn falls_back_to_interceptor_default_when_ratelimits_missing() {
        let mut limiter = MethodRatelimiter::new(1, Duration::from_secs(10));
        let now = std::time::Instant::now();
        let subscription_info = SubscriptionInfo {
            subscription_id: "sub-1".to_string(),
            ratelimits: None,
        };

        let req1 = make_request("/geyser.Geyser/Subscribe", Some(subscription_info.clone()));
        let req2 = make_request("/geyser.Geyser/Subscribe", Some(subscription_info));

        assert!(limiter.inner_call(req1, now).is_ok());
        let denied = limiter
            .inner_call(req2, now)
            .expect_err("second call should be denied");
        assert_eq!(denied.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn rate_limits_are_scoped_by_subscriber_and_method() {
        let mut limiter = MethodRatelimiter::new(10, Duration::from_secs(10));
        let now = std::time::Instant::now();
        let sub_a = SubscriptionInfo {
            subscription_id: "sub-a".to_string(),
            ratelimits: Some(SubscriptionRateLimits {
                default: 10,
                methods: HashMap::from([("/geyser.Geyser/Subscribe".to_string(), 1)]),
            }),
        };
        let sub_b = SubscriptionInfo {
            subscription_id: "sub-b".to_string(),
            ratelimits: Some(SubscriptionRateLimits {
                default: 10,
                methods: HashMap::from([("/geyser.Geyser/Subscribe".to_string(), 1)]),
            }),
        };

        let req_a1 = make_request("/geyser.Geyser/Subscribe", Some(sub_a.clone()));
        let req_a2 = make_request("/geyser.Geyser/Subscribe", Some(sub_a.clone()));
        let req_b1 = make_request("/geyser.Geyser/Subscribe", Some(sub_b));
        let req_a_other_method = make_request("/geyser.Geyser/Ping", Some(sub_a));

        assert!(limiter.inner_call(req_a1, now).is_ok());
        assert!(limiter.inner_call(req_b1, now).is_ok());
        assert!(limiter.inner_call(req_a_other_method, now).is_ok());

        let denied = limiter
            .inner_call(req_a2, now)
            .expect_err("second call for sub-a/method should fail");
        assert_eq!(denied.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn exceeded_method_limit_refreshes_after_window() {
        let window = Duration::from_secs(2);
        let mut limiter = MethodRatelimiter::new(10, window);
        let now = std::time::Instant::now();
        let subscription_info = SubscriptionInfo {
            subscription_id: "sub-refresh".to_string(),
            ratelimits: Some(SubscriptionRateLimits {
                default: 10,
                methods: HashMap::from([("/geyser.Geyser/Subscribe".to_string(), 1)]),
            }),
        };

        let req1 = make_request("/geyser.Geyser/Subscribe", Some(subscription_info.clone()));
        let req2 = make_request("/geyser.Geyser/Subscribe", Some(subscription_info));

        assert!(limiter.inner_call(req1, now).is_ok());
        let denied = limiter
            .inner_call(req2, now)
            .expect_err("second call should be denied before refresh");
        assert_eq!(denied.code(), tonic::Code::ResourceExhausted);

        // Advance logical time past one full window: the limiter should admit again.
        let req3 = make_request(
            "/geyser.Geyser/Subscribe",
            Some(SubscriptionInfo {
                subscription_id: "sub-refresh".to_string(),
                ratelimits: Some(SubscriptionRateLimits {
                    default: 10,
                    methods: HashMap::from([("/geyser.Geyser/Subscribe".to_string(), 1)]),
                }),
            }),
        );
        assert!(limiter
            .inner_call(req3, now + window + Duration::from_secs(1))
            .is_ok());
    }

    #[test]
    fn gc_removes_idle_windows_every_n_calls() {
        let window = Duration::from_secs(10);
        let idle_timeout = Duration::from_secs(2);
        let mut limiter = MethodRatelimiter::with_gc(10, window, idle_timeout, 1);
        let t0 = std::time::Instant::now();

        let sub_a = SubscriptionInfo {
            subscription_id: "gc-a".to_string(),
            ratelimits: Some(SubscriptionRateLimits {
                default: 10,
                methods: HashMap::new(),
            }),
        };
        let sub_b = SubscriptionInfo {
            subscription_id: "gc-b".to_string(),
            ratelimits: Some(SubscriptionRateLimits {
                default: 10,
                methods: HashMap::new(),
            }),
        };

        assert!(limiter
            .inner_call(make_request("/geyser.Geyser/Subscribe", Some(sub_a)), t0)
            .is_ok());
        assert!(limiter
            .inner_call(make_request("/geyser.Geyser/Subscribe", Some(sub_b)), t0)
            .is_ok());
        assert_eq!(limiter.active_window_count(), 2);

        // Only gc-b is refreshed; gc-a should be evicted as idle.
        assert!(limiter
            .inner_call(
                make_request(
                    "/geyser.Geyser/Subscribe",
                    Some(SubscriptionInfo {
                        subscription_id: "gc-b".to_string(),
                        ratelimits: Some(SubscriptionRateLimits {
                            default: 10,
                            methods: HashMap::new(),
                        }),
                    }),
                ),
                t0 + idle_timeout + Duration::from_secs(1)
            )
            .is_ok());

        assert_eq!(limiter.active_window_count(), 1);
    }

    #[test]
    fn gc_disabled_when_tick_is_zero() {
        let window = Duration::from_secs(10);
        let idle_timeout = Duration::from_secs(1);
        let mut limiter = MethodRatelimiter::with_gc(10, window, idle_timeout, 0);
        let t0 = std::time::Instant::now();

        let sub = SubscriptionInfo {
            subscription_id: "gc-off".to_string(),
            ratelimits: Some(SubscriptionRateLimits {
                default: 10,
                methods: HashMap::new(),
            }),
        };

        assert!(limiter
            .inner_call(make_request("/geyser.Geyser/Subscribe", Some(sub)), t0)
            .is_ok());
        assert_eq!(limiter.active_window_count(), 1);

        // Even far in the future, entries remain when gc_tick == 0.
        assert!(limiter
            .inner_call(
                make_request("/geyser.Geyser/Subscribe", None),
                t0 + Duration::from_secs(100)
            )
            .is_ok());
        assert_eq!(limiter.active_window_count(), 1);
    }
}
