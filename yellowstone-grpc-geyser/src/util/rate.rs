use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

struct Bin {
    timestamp: AtomicU64, // seconds since start_time
    sum: AtomicU64,
    sum_of_squares: AtomicU64, // Sum of squares for variance calculation
}

///
/// Tracks the rate of traffic per second over a specified time window.
///
pub struct RateTracker {
    bins: Box<[Bin]>,
    start_time: Instant,
    window: Duration, // e.g., 1 hour
}

impl fmt::Debug for RateTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateTracker")
            .field("bins", &self.bins.len())
            .field("start_time", &self.start_time)
            .field("window", &self.window)
            .finish()
    }
}

impl RateTracker {
    pub fn new(window: Duration) -> Self {
        let now = Instant::now();
        Self::with_starting_time(window, now)
    }

    pub fn with_starting_time(window: Duration, now: Instant) -> Self {
        let bins = (0..window.as_secs())
            .map(|_| Bin {
                timestamp: AtomicU64::new(0),
                sum: AtomicU64::new(0),
                sum_of_squares: AtomicU64::new(0),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            bins,
            start_time: now,
            window,
        }
    }

    pub fn record(&self, now: Instant, traffic: u32) {
        if now < self.start_time {
            return; // Ignore records before the start time
        }
        let now_secs = now.duration_since(self.start_time).as_secs();
        let bin_index = (now_secs % self.bins.len() as u64) as usize;
        let bin = &self.bins[bin_index];
        let ts = bin.timestamp.load(Ordering::Acquire);
        if ts != now_secs {
            // Try to update timestamp to current second.
            // Use AcqRel on success to synchronize with count reset,
            // Acquire on failure to see fresh state.
            if bin
                .timestamp
                .compare_exchange(ts, now_secs, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // This thread won the race; clear the count.
                // Use Release ordering to ensure the store happens after timestamp update.
                bin.sum.store(0, Ordering::Release);
                bin.sum_of_squares.store(0, Ordering::Release);
            }
            // If CAS failed, another thread updated timestamp & count; do nothing.
        }
        // Increment count atomically. Relaxed ordering is fine here.
        bin.sum.fetch_add(traffic as u64, Ordering::Relaxed);
        bin.sum_of_squares
            .fetch_add((traffic as u64) * (traffic as u64), Ordering::Relaxed);
    }

    pub fn average_rate(&self, now: Instant) -> f64 {
        if now < self.start_time {
            return 0.0; // No events recorded yet
        }
        let now_secs = now.duration_since(self.start_time).as_secs();
        let min_ts = now_secs.saturating_sub(self.window.as_secs());
        let max_bin = self.bins.len().min(now_secs as usize);
        let mut sum = 0.0;
        for bin in self.bins.iter().take(max_bin) {
            let ts = bin.timestamp.load(Ordering::Relaxed);
            if ts < min_ts {
                continue; // Skip bins outside the window
            }
            let bin_sum = bin.sum.load(Ordering::Relaxed);
            sum += bin_sum as f64;
        }
        if (max_bin as f64) == 0.0 {
            return 0.0; // No bins to average
        }
        sum / max_bin as f64
    }

    pub fn variance(&self, now: Instant) -> f64 {
        if now < self.start_time {
            return 0.0; // No events recorded yet
        }
        let now_secs = now.duration_since(self.start_time).as_secs();
        let min_ts = now_secs.saturating_sub(self.window.as_secs());
        let max_bin = self.bins.len().min(now_secs as usize);

        if max_bin == 0 {
            return 0.0; // No bins to calculate variance
        }

        let mut sum = 0u64;
        let mut sum_of_squares = 0u64;

        for bin in self.bins.iter().take(max_bin) {
            let bin_sum = bin.sum.load(Ordering::Relaxed);
            let sq = bin.sum_of_squares.load(Ordering::Relaxed);
            let ts = bin.timestamp.load(Ordering::Relaxed);
            if ts < min_ts {
                continue; // Skip bins outside the window
            }
            sum += bin_sum;
            sum_of_squares += sq;
        }

        let mean = sum as f64 / max_bin as f64;
        let mean_of_squares = sum_of_squares as f64 / max_bin as f64;
        mean_of_squares - (mean * mean)
    }

    pub fn stddev(&self, now: Instant) -> f64 {
        self.variance(now).sqrt()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::time::{Duration, Instant},
    };

    #[test]
    fn test_rate_tracker_initialization() {
        let window = Duration::from_secs(10);
        let tracker = RateTracker::new(window);
        assert_eq!(tracker.bins.len(), 10);
        assert_eq!(tracker.window, window);
    }

    #[test]
    fn test_no_recorded_traffic() {
        let now = Instant::now();
        let window = Duration::from_secs(10);
        let tracker = RateTracker::with_starting_time(window, now);

        let rate = tracker.average_rate(now + Duration::from_secs(10));
        let variance = tracker.variance(now + Duration::from_secs(10));
        let stddev = tracker.stddev(now + Duration::from_secs(10));

        assert_eq!(rate, 0.0);
        assert_eq!(variance, 0.0);
        assert_eq!(stddev, 0.0);
    }

    #[test]
    fn test_record_increments_count() {
        let now = Instant::now();
        let window = Duration::from_secs(5);
        let tracker = RateTracker::with_starting_time(window, now);

        tracker.record(now, 1);
        tracker.record(now, 1);

        let now_secs = Instant::now().duration_since(tracker.start_time).as_secs();
        let bin_index = (now_secs % tracker.bins.len() as u64) as usize;
        let bin = &tracker.bins[bin_index];

        assert_eq!(bin.sum.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_average_rate_calculation() {
        let now = Instant::now();
        let window = Duration::from_secs(5);
        let tracker = RateTracker::with_starting_time(window, now);

        // Simulate recording events
        let now_1s = now + Duration::from_secs(1);
        let now_2s = now + Duration::from_secs(2);
        tracker.record(now, 1);
        tracker.record(now, 1);
        tracker.record(now_1s, 1);

        // (2 + 1) / 2 = 1.5
        let rate = tracker.average_rate(now_2s);
        assert_eq!(rate, 1.5);
    }

    #[test]
    fn test_variance_and_stddev_calculation() {
        let now = Instant::now();
        let window = Duration::from_secs(5);
        let tracker = RateTracker::with_starting_time(window, now);

        // Simulate recording events
        let now_1s = now + Duration::from_secs(1);
        let now_2s = now + Duration::from_secs(2);
        let now_3s = now + Duration::from_secs(3);
        tracker.record(now, 5);
        tracker.record(now_1s, 10);
        tracker.record(now_2s, 15);

        let variance = tracker.variance(now_3s);
        let stddev = tracker.stddev(now_3s);

        assert!(variance >= 16.6 && variance <= 16.7); // Variance should be around 16.67
        assert!((stddev * stddev - variance).abs() < 1e-10);
    }

    #[test]
    fn test_multiple_bins_update() {
        let now = Instant::now();
        let window = Duration::from_secs(10);
        let tracker = RateTracker::with_starting_time(window, now);

        // Simulate events across multiple bins
        for i in 0..10 {
            let event_time = now + Duration::from_secs(i);
            tracker.record(event_time, (i as u32) + 1); // Cast to u32
        }

        // Ensure all bins are within the window by advancing the time
        let end_time = now + Duration::from_secs(10);

        let rate = tracker.average_rate(end_time);
        let variance = tracker.variance(end_time);
        let stddev = tracker.stddev(end_time);

        assert_eq!(rate, 5.5); // Average of 1 through 10
        assert!(variance > 0.0);
        assert!(stddev > 0.0);
    }

    #[test]
    fn test_no_events_recorded() {
        let now = Instant::now();
        let window = Duration::from_secs(10);
        let tracker = RateTracker::with_starting_time(window, now);

        let rate = tracker.average_rate(now + Duration::from_secs(10));
        let variance = tracker.variance(now + Duration::from_secs(10));
        let stddev = tracker.stddev(now + Duration::from_secs(10));

        assert_eq!(rate, 0.0);
        assert_eq!(variance, 0.0);
        assert_eq!(stddev, 0.0);
    }

    #[test]
    fn test_all_events_outside_window() {
        let now = Instant::now();
        let window = Duration::from_secs(10);
        let tracker = RateTracker::with_starting_time(window, now);

        // Record events outside the window
        tracker.record(now - Duration::from_secs(20), 5 as u32); // Cast to u32
        tracker.record(now - Duration::from_secs(15), 10 as u32); // Cast to u32

        let rate = tracker.average_rate(now);
        let variance = tracker.variance(now);
        let stddev = tracker.stddev(now);

        assert_eq!(rate, 0.0);
        assert_eq!(variance, 0.0);
        assert_eq!(stddev, 0.0);
    }

    #[test]
    fn test_no_variance() {
        let now = Instant::now();
        let window = Duration::from_secs(10);
        let tracker = RateTracker::with_starting_time(window, now);
        let now_1s = now + Duration::from_secs(1);
        let now_2s = now + Duration::from_secs(2);
        // Simulate a high volume of events in a single bin
        tracker.record(now, 1_000_000 as u32); // Cast to u32
        tracker.record(now_1s, 1_000_000 as u32); // Cast to u32

        let rate = tracker.average_rate(now_2s);
        let variance = tracker.variance(now_2s);
        let stddev = tracker.stddev(now_2s);

        assert_eq!(rate, 1_000_000.0);
        assert!(variance == 0.0);
        assert!(stddev == 0.0);
    }

    #[test]
    fn test_rate_cyclicity() {
        let now = Instant::now();
        let window = Duration::from_secs(3600); // 1 hour
        let tracker = RateTracker::with_starting_time(window, now);
        let end_first_period = now + Duration::from_secs(3600);
        let end_second_period = now + Duration::from_secs(7200);
        for i in 0..3600 {
            let event_time = now + Duration::from_secs(i);
            tracker.record(event_time, i as u32 + 1);
        }
        // Sum of 1 to 3600 = 6_481_800
        let rate = tracker.average_rate(end_first_period);
        assert_eq!(rate, 6_481_800.0 / 3600.0);

        for i in 3600..7200 {
            let event_time = now + Duration::from_secs(i);
            tracker.record(event_time, i as u32);
        }
        // Sum of 3660 to 7199 = 19_438_200
        let rate = tracker.average_rate(end_second_period);
        assert_eq!(rate, 19_438_200.0 / 3600.0);

        let variance = tracker.variance(end_second_period);
        let stddev = tracker.stddev(end_second_period);
        assert!(variance > 0.0);
        assert!((stddev * stddev - variance).abs() < 1e-5);
    }

    #[test]
    fn test_long_period_of_inactivity() {
        let now = Instant::now();
        let window = Duration::from_secs(3600); // 1 hour
        let tracker = RateTracker::with_starting_time(window, now);
        let end_first_period = now + Duration::from_secs(3600);
        for i in 0..3600 {
            let event_time = now + Duration::from_secs(i);
            tracker.record(event_time, i as u32 + 1);
        }

        let rate = tracker.average_rate(end_first_period);
        assert_eq!(rate, 6_481_800.0 / 3600.0);

        // Simulate a long period of inactivity
        let long_inactivity = now + Duration::from_secs(7200);
        let rate = tracker.average_rate(long_inactivity);
        let var = tracker.variance(long_inactivity);
        let stddev = tracker.stddev(long_inactivity);
        assert_eq!(rate, 0.0); // No events recorded in the second hour
        assert_eq!(var, 0.0); // Variance should also be zero
        assert_eq!(stddev, 0.0); // Standard deviation should also be zero
        assert!((stddev * stddev - var).abs() < 1e-10);
    }

    #[test]
    fn it_should_handle_holes_in_history() {
        let now = Instant::now();
        let window = Duration::from_secs(10);
        let tracker = RateTracker::with_starting_time(window, now);
        let one_sec = Duration::from_secs(1);
        // Record events at 1s and 3s, skipping 2s
        tracker.record(now, 1); // 0th second
        tracker.record(now + one_sec, 2); // 1st second
                                          // 2nd second is skipped
        tracker.record(now + (one_sec * 3), 4); // 3rd second

        // Average rate should consider only the recorded events
        let expected_avg = (1 + 2 + 0 + 4) as f64 / 4.0;
        let rate = tracker.average_rate(now + Duration::from_secs(4));
        assert_eq!(rate, expected_avg);

        let variance = tracker.variance(now + Duration::from_secs(4));
        let stddev = tracker.stddev(now + Duration::from_secs(4));
        assert_eq!(variance, 2.1875);
        assert!((stddev * stddev - variance).abs() < 1e-10);
    }
}
