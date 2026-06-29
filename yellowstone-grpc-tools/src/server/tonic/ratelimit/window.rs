use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Decision returned by a rate-limiter check.
///
/// # Returns
/// - [`WindowDecision::allowed`]: `true` when the event is admitted.
/// - [`WindowDecision::remaining`]: rough number of additional events that can
///   still be admitted immediately for the same key.
/// - [`WindowDecision::retry_after`]: suggested delay before retry when the
///   event is denied. This is [`Duration::ZERO`] for admitted events.
pub struct WindowDecision {
    pub allowed: bool,
    pub remaining: u64,
    pub retry_after: Duration,
}

#[derive(Debug, Clone)]
struct TokenState {
    tokens: f64,
    last_refill: Instant,
}

/// Token-bucket rate limiter with a single shared state.
///
/// The limiter tracks one global bucket with:
/// - burst capacity derived from `max_hits`
/// - refill rate derived from `max_hits / window`
#[derive(Debug, Clone)]
pub struct TokenBucketRateLimiter {
    burst_capacity: f64,
    refill_per_sec: f64,
    window: Duration,
    state: Option<TokenState>,
}

/// Backward-compatible alias kept to avoid refactoring existing call sites.
pub type SlidingWindowRateLimiter = TokenBucketRateLimiter;

impl TokenBucketRateLimiter {
    /// Creates a new [`TokenBucketRateLimiter`].
    ///
    /// # Arguments
    /// - `max_hits`: burst capacity and nominal events per `window`.
    /// - `window`: period used to derive refill rate (`max_hits / window`).
    ///
    /// # Returns
    /// A limiter instance with no initialized state yet.
    pub fn new(max_hits: u64, window: Duration) -> Self {
        let burst_capacity = max_hits as f64;
        let window_secs = window.as_secs_f64().max(f64::EPSILON);
        let refill_per_sec = burst_capacity / window_secs;

        Self {
            burst_capacity,
            refill_per_sec,
            window,
            state: None,
        }
    }

    /// Returns the configured nominal events per window.
    ///
    /// # Returns
    /// The burst capacity cast to `u64`.
    pub fn max_hits(&self) -> u64 {
        self.burst_capacity as u64
    }

    /// Returns the configured reference window.
    ///
    /// # Returns
    /// The period used to derive token refill rate.
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Compatibility accessor retained for call sites.
    ///
    /// # Returns
    /// Always returns `1` under token-bucket semantics.
    pub fn bucket_count(&self) -> usize {
        1
    }

    /// Returns whether the shared limiter state is currently initialized.
    ///
    /// # Returns
    /// `1` when state exists, otherwise `0`.
    pub fn key_count(&self) -> usize {
        usize::from(self.state.is_some())
    }

    /// Checks and updates the shared rate-limit state at `now`.
    ///
    /// # Arguments
    /// - `now`: current timestamp used for refill calculations.
    ///
    /// # Returns
    /// A [`WindowDecision`] indicating whether one token could be consumed.
    pub fn check_at(&mut self, now: Instant) -> WindowDecision {
        if self.burst_capacity <= 0.0 {
            return WindowDecision {
                allowed: false,
                remaining: 0,
                retry_after: self.window,
            };
        }

        let state = self.state.get_or_insert(TokenState {
            tokens: self.burst_capacity,
            last_refill: now,
        });

        Self::refill_state(state, now, self.burst_capacity, self.refill_per_sec);

        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            return WindowDecision {
                allowed: true,
                remaining: state.tokens.floor() as u64,
                retry_after: Duration::ZERO,
            };
        }

        WindowDecision {
            allowed: false,
            remaining: 0,
            retry_after: Self::retry_after(state.tokens, self.refill_per_sec),
        }
    }

    /// Checks and updates the rate-limit state for a key using [`Instant::now`].
    ///
    /// # Returns
    /// A [`WindowDecision`]. Equivalent to calling [`Self::check_at`] with
    /// [`Instant::now`].
    pub fn check_now(&mut self) -> WindowDecision {
        self.check_at(Instant::now())
    }

    /// Removes the shared state if the bucket is fully refilled at `now`.
    ///
    /// # Arguments
    /// - `now`: timestamp used to refill state before pruning.
    ///
    /// # Returns
    /// This method does not return a value. It drops idle/full state.
    pub fn prune_at(&mut self, now: Instant) {
        if let Some(state) = self.state.as_mut() {
            Self::refill_state(state, now, self.burst_capacity, self.refill_per_sec);
            if state.tokens + f64::EPSILON >= self.burst_capacity {
                self.state = None;
            }
        }
    }

    fn refill_state(
        state: &mut TokenState,
        now: Instant,
        burst_capacity: f64,
        refill_per_sec: f64,
    ) {
        let elapsed = now
            .saturating_duration_since(state.last_refill)
            .as_secs_f64();
        if elapsed > 0.0 {
            state.tokens = (state.tokens + elapsed * refill_per_sec).min(burst_capacity);
            state.last_refill = now;
        }
    }

    fn retry_after(tokens: f64, refill_per_sec: f64) -> Duration {
        if refill_per_sec <= 0.0 {
            return Duration::MAX;
        }
        let missing = (1.0 - tokens).max(0.0);
        Duration::from_secs_f64((missing / refill_per_sec).max(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denies_after_burst_and_returns_retry_after() {
        let mut limiter = SlidingWindowRateLimiter::new(10, Duration::from_secs(10));
        let start = Instant::now();

        for _ in 0..10 {
            assert!(limiter.check_at(start).allowed);
        }

        let denied = limiter.check_at(start);
        assert!(!denied.allowed);
        assert_eq!(denied.remaining, 0);
        assert_eq!(denied.retry_after, Duration::from_secs(1));
    }

    #[test]
    fn recovers_about_one_token_after_one_second() {
        let mut limiter = SlidingWindowRateLimiter::new(10, Duration::from_secs(10));
        let start = Instant::now();

        for _ in 0..10 {
            assert!(limiter.check_at(start).allowed);
        }
        assert!(!limiter.check_at(start).allowed);

        let after_one_sec = limiter.check_at(start + Duration::from_secs(1));
        assert!(after_one_sec.allowed);
        assert_eq!(after_one_sec.remaining, 0);

        let denied_again = limiter.check_at(start + Duration::from_secs(1));
        assert!(!denied_again.allowed);
    }

    #[test]
    fn shared_bucket_applies_globally() {
        let mut limiter = SlidingWindowRateLimiter::new(1, Duration::from_secs(10));
        let start = Instant::now();

        assert!(limiter.check_at(start).allowed);
        assert!(!limiter.check_at(start).allowed);
    }

    #[test]
    fn long_idle_time_refills_bucket_to_capacity() {
        let mut limiter = SlidingWindowRateLimiter::new(2, Duration::from_secs(4));
        let start = Instant::now();

        assert!(limiter.check_at(start).allowed);
        assert!(limiter.check_at(start).allowed);
        assert!(!limiter.check_at(start).allowed);

        assert!(limiter.check_at(start + Duration::from_secs(10)).allowed);
        assert!(limiter.check_at(start + Duration::from_secs(10)).allowed);
    }

    #[test]
    fn prune_removes_fully_refilled_states() {
        let mut limiter = SlidingWindowRateLimiter::new(2, Duration::from_secs(4));
        let start = Instant::now();

        let _ = limiter.check_at(start);
        assert_eq!(limiter.key_count(), 1);

        limiter.prune_at(start + Duration::from_secs(5));
        assert_eq!(limiter.key_count(), 0);
    }
}
