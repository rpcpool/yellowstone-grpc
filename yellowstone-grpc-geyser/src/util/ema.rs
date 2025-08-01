use std::{
    fmt::{self, Display},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        RwLock,
    },
    time::{Duration, Instant},
};

///
/// Exponential Moving Average (EMA) for load tracking.
///
#[derive(Debug)]
pub struct Ema {
    window: Duration,
    window_load: AtomicU64,
    current_load_ema: AtomicU64,
    reactivity: EMAReactivity,
    last_update: RwLock<Instant>,
}

///
/// Exponential Moving Average (EMA) reactivity categories.
///
/// These categories define how much weight the EMA gives to recent values compared to older ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EMAReactivity {
    /// Very reactive, attribute 50% weight to the most recent value.
    VeryReactive,
    /// Reactive, attribute 33% weight to the most recent value.
    #[default]
    Reactive,
    /// Moderately reactive, attribute 20% weight to the most recent value.
    ModeratelyReactive,
    /// Less reactive, attribute 10% weight to the most recent value.
    LessReactive,
}

impl fmt::Display for EMAReactivity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EMAReactivity::VeryReactive => write!(f, "Very Reactive"),
            EMAReactivity::Reactive => write!(f, "Reactive"),
            EMAReactivity::ModeratelyReactive => write!(f, "Moderately Reactive"),
            EMAReactivity::LessReactive => write!(f, "Less Reactive"),
        }
    }
}

impl FromStr for EMAReactivity {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "very reactive" => Ok(EMAReactivity::VeryReactive),
            "reactive" => Ok(EMAReactivity::Reactive),
            "moderately reactive" => Ok(EMAReactivity::ModeratelyReactive),
            "less reactive" => Ok(EMAReactivity::LessReactive),
            _ => Err("Invalid EMA reactivity"),
        }
    }
}

impl EMAReactivity {
    const fn as_period(self) -> u64 {
        match self {
            EMAReactivity::VeryReactive => 3,
            EMAReactivity::Reactive => 5,
            EMAReactivity::ModeratelyReactive => 10,
            EMAReactivity::LessReactive => 20,
        }
    }

    ///
    /// Precomputed table of effective memory usage for each reactivity level.
    ///
    /// This is how many steps in EMA before the data becomes "irrelevant" to current load computation.
    /// Which is another way of saying after how many steps the last record will only contribute to 1% of the prediction.
    ///
    /// Log_{alpha}(0.01)
    ///
    const fn effective_memory(self) -> u64 {
        match self {
            EMAReactivity::VeryReactive => 7,
            EMAReactivity::Reactive => 5,
            EMAReactivity::ModeratelyReactive => 3,
            EMAReactivity::LessReactive => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EMACurrentLoad {
    ema_load: u64,
    unit: Duration,
}

impl Display for EMACurrentLoad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} / {}ms", self.ema_load, self.unit.as_millis())
    }
}

impl EMACurrentLoad {
    ///
    /// Converts the current traffic load native unit to taffic per seconds.
    ///
    pub fn per_second(&self) -> f64 {
        let scale = Duration::from_secs(1).as_millis() as f64 / self.unit.as_millis() as f64;
        self.ema_load as f64 * scale
    }
}

pub const DEFAULT_EMA_WINDOW: Duration = Duration::from_millis(10);

impl Ema {
    pub fn new(window: Duration, reactivity: EMAReactivity) -> Self {
        Self::with_starting_time(window, reactivity, Instant::now())
    }

    pub const fn with_starting_time(
        window: Duration,
        reactivity: EMAReactivity,
        starting_time: Instant,
    ) -> Self {
        Self {
            window,
            window_load: AtomicU64::new(0),
            reactivity,
            last_update: RwLock::new(starting_time),
            current_load_ema: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn alpha(&self) -> f64 {
        2.0 / (self.reactivity.as_period() as f64 + 1.0)
    }

    fn ema_function(&self, current_ema: u64, recent_load: u32) -> u64 {
        let alpha = self.alpha();
        let beta = 1.0 - alpha;
        (alpha * recent_load as f64 + beta * current_ema as f64) as _
    }

    fn update_ema(&self, duration_since_last_update: Duration) {
        //
        let time_since_last_update = duration_since_last_update.as_millis();

        // If the time since the last update is bigger than the window, we need to catch up and update the EMA
        // for each missed update.
        let missed_updates = time_since_last_update.saturating_sub(1) / self.window.as_millis();

        // Limit the number of extra updates to the effective memory of the reactivity level.
        // This is to prevent excessive computation in case of long delays.
        // This is a trade-off between accuracy and performance.
        // EMA terms forms a geometric series, where each term contributes less and less to the final value.
        // The contribution value of each decay exponentially decreases.
        // after the effective memory steps, the contribution of the last record is less than 1% of the prediction.
        let extra_updates = missed_updates.min(self.reactivity.effective_memory() as u128);

        let load_in_recent_window = self.window_load.swap(0, Ordering::Relaxed) as u32; // Cast to u32

        let mut updated_load_ema = self.ema_function(
            self.current_load_ema.load(Ordering::Relaxed),
            load_in_recent_window,
        );

        if extra_updates > 0 {
            log::trace!(
                "Updating EMA with {} extra updates, current EMA: {}, load in recent window: {}",
                extra_updates,
                updated_load_ema,
                load_in_recent_window
            );
        }
        for _ in 0..extra_updates {
            updated_load_ema = self.ema_function(updated_load_ema, load_in_recent_window);
        }

        self.current_load_ema
            .store(updated_load_ema, Ordering::Relaxed);
    }

    #[inline]
    pub fn current_load_ema_in_native_unit(&self) -> u64 {
        self.current_load_ema.load(Ordering::Relaxed)
    }

    pub fn current_load(&self) -> EMACurrentLoad {
        let ema_load = self.current_load_ema_in_native_unit();
        let unit = self.window;
        EMACurrentLoad { ema_load, unit }
    }

    fn update_ema_if_needed(&self, now: Instant) {
        let last_update = *self.last_update.read().unwrap();
        if now.duration_since(last_update) >= self.window {
            let mut last_update = self.last_update.write().unwrap();
            // check again if the last update is still older than the window since it might have been updated by another thread in the meantime
            let since_last_update = now.duration_since(*last_update);
            if since_last_update >= self.window {
                log::trace!("Updating EMA at {:?}", now);
                *last_update = now;
                self.update_ema(since_last_update);
            } else {
                log::trace!(
                    "Skipping EMA update at {:?}, concurrent update is too recent",
                    now
                );
            }
        }
    }

    pub fn record_load(&self, now: Instant, load: u32) {
        self.window_load.fetch_add(load as u64, Ordering::Relaxed);
        self.update_ema_if_needed(now);
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Write as _, str::FromStr};

    #[test]
    fn test_emareactivity_from_str() {
        assert_eq!(
            EMAReactivity::from_str("Very Reactive").unwrap(),
            EMAReactivity::VeryReactive
        );
        assert_eq!(
            EMAReactivity::from_str("Reactive").unwrap(),
            EMAReactivity::Reactive
        );
        assert_eq!(
            EMAReactivity::from_str("Moderately Reactive").unwrap(),
            EMAReactivity::ModeratelyReactive
        );
        assert_eq!(
            EMAReactivity::from_str("Less Reactive").unwrap(),
            EMAReactivity::LessReactive
        );
        assert!(EMAReactivity::from_str("invalid").is_err());
    }

    #[test]
    fn test_emareactivity_display() {
        let mut s = String::new();
        write!(&mut s, "{}", EMAReactivity::VeryReactive).unwrap();
        assert_eq!(s, "Very Reactive");
        s.clear();
        write!(&mut s, "{}", EMAReactivity::Reactive).unwrap();
        assert_eq!(s, "Reactive");
        s.clear();
        write!(&mut s, "{}", EMAReactivity::ModeratelyReactive).unwrap();
        assert_eq!(s, "Moderately Reactive");
        s.clear();
        write!(&mut s, "{}", EMAReactivity::LessReactive).unwrap();
        assert_eq!(s, "Less Reactive");
    }

    #[test]
    fn test_emareactivity_as_period() {
        assert_eq!(EMAReactivity::VeryReactive.as_period(), 3);
        assert_eq!(EMAReactivity::Reactive.as_period(), 5);
        assert_eq!(EMAReactivity::ModeratelyReactive.as_period(), 10);
        assert_eq!(EMAReactivity::LessReactive.as_period(), 20);
    }

    #[test]
    fn test_ema_alpha_for_all_reactivities() {
        let window = Duration::from_secs(10);
        let very = Ema::new(window, EMAReactivity::VeryReactive);
        let reactive = Ema::new(window, EMAReactivity::Reactive);
        let moderate = Ema::new(window, EMAReactivity::ModeratelyReactive);
        let less = Ema::new(window, EMAReactivity::LessReactive);
        assert!((very.alpha() - 0.5).abs() < 1e-6);
        assert!((reactive.alpha() - 0.3333333).abs() < 1e-6);
        assert!((moderate.alpha() - 0.1818181).abs() < 1e-6);
        assert!((less.alpha() - 0.0952380).abs() < 1e-6);
    }

    #[test]
    fn test_ema_function_computation() {
        let window = Duration::from_secs(10);
        let ema = Ema::new(window, EMAReactivity::Reactive); // period = 5, alpha = 0.333...
                                                             // current_ema = 6, recent_load = 12
                                                             // expected = alpha * recent_load + (1-alpha) * current_ema
        let expected = (0.33333333333 * 12.0 + 0.6666667 * 6.0) as u64;
        assert_eq!(ema.ema_function(6, 12), expected);
    }
    use {
        super::*,
        std::time::{Duration, Instant},
    };
    #[test]
    fn test_ema_new_initializes_fields() {
        let window = Duration::from_secs(10);
        let ema = Ema::new(window, EMAReactivity::Reactive);
        assert_eq!(ema.window, window);
        assert_eq!(ema.reactivity, EMAReactivity::Reactive);
        assert_eq!(ema.current_load_ema_in_native_unit(), 0);
    }

    #[test]
    fn test_record_load_increments_and_updates_ema_after_window() {
        let now = Instant::now();
        const WINDOW: Duration = Duration::from_millis(1);
        let ema = Ema::with_starting_time(WINDOW, EMAReactivity::VeryReactive, now);
        let now_1w = now + WINDOW;
        ema.record_load(now, 1);
        ema.record_load(now, 1);
        ema.record_load(now_1w, 1);
        assert_eq!(ema.current_load_ema_in_native_unit(), 1);
    }

    #[test]
    fn recent_load_change_should_not_change_last_ema() {
        let now = Instant::now();
        const WINDOW: Duration = Duration::from_millis(1);
        let ema = Ema::with_starting_time(WINDOW, EMAReactivity::VeryReactive, now);
        // Add 3 load in the current EMA window.
        ema.record_load(now, 1);
        ema.record_load(now, 1);
        let now_1w = now + WINDOW;
        ema.record_load(now_1w, 1);
        println!("alpha: {}", ema.alpha());
        let expected_ema = ema.ema_function(0, 3);
        println!("Expected EMA: {}", expected_ema);
        assert_eq!(ema.current_load_ema_in_native_unit(), expected_ema);

        // adding more to the new window should not change the EMA
        ema.record_load(now_1w, 1);
        ema.record_load(now_1w, 1);
        ema.record_load(now_1w, 1);

        assert_eq!(ema.current_load_ema_in_native_unit(), expected_ema);
    }

    #[test]
    fn test_alpha_calculation() {
        let ema = Ema::new(Duration::from_secs(10), EMAReactivity::VeryReactive);
        let alpha = ema.alpha();
        assert!(alpha > 0.0 && alpha < 1.0);
    }
}
