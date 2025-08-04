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
/// The implementation is based off Anza stream-throttle module: https://github.com/anza-xyz/agave/blob/v2.3/streamer/src/nonblocking/stream_throttle.rs
///
#[derive(Debug)]
pub struct Ema {
    window: Duration,
    window_load: AtomicU64,
    current_load_ema: AtomicU64,
    reactivity: EmaReactivity,
    last_update: RwLock<Instant>,
}

pub struct EmaBuilder {
    window: Duration,
    reactivity: EmaReactivity,
}

impl Default for EmaBuilder {
    fn default() -> Self {
        Self {
            window: DEFAULT_EMA_WINDOW,
            reactivity: Default::default(),
        }
    }
}

impl EmaBuilder {
    pub const fn window(mut self, window: Duration) -> Self {
        self.window = window;
        self
    }

    pub const fn reactivity(mut self, reactivity: EmaReactivity) -> Self {
        self.reactivity = reactivity;
        self
    }

    pub fn build(self) -> Ema {
        Ema::new(self.window, self.reactivity)
    }
}

///
/// Exponential Moving Average (EMA) reactivity categories.
///
/// These categories define how much weight the EMA gives to recent values compared to older ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EmaReactivity {
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

impl fmt::Display for EmaReactivity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmaReactivity::VeryReactive => write!(f, "Very Reactive"),
            EmaReactivity::Reactive => write!(f, "Reactive"),
            EmaReactivity::ModeratelyReactive => write!(f, "Moderately Reactive"),
            EmaReactivity::LessReactive => write!(f, "Less Reactive"),
        }
    }
}

impl FromStr for EmaReactivity {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "very reactive" => Ok(EmaReactivity::VeryReactive),
            "reactive" => Ok(EmaReactivity::Reactive),
            "moderately reactive" => Ok(EmaReactivity::ModeratelyReactive),
            "less reactive" => Ok(EmaReactivity::LessReactive),
            _ => Err("Invalid EMA reactivity"),
        }
    }
}

impl EmaReactivity {
    const fn as_period(self) -> u64 {
        match self {
            EmaReactivity::VeryReactive => 3,
            EmaReactivity::Reactive => 5,
            EmaReactivity::ModeratelyReactive => 10,
            EmaReactivity::LessReactive => 20,
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
            EmaReactivity::VeryReactive => 7,
            EmaReactivity::Reactive => 5,
            EmaReactivity::ModeratelyReactive => 3,
            EmaReactivity::LessReactive => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EmaCurrentLoad {
    ema_load: u64,
    unit: Duration,
}

impl Display for EmaCurrentLoad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} / {}ms", self.ema_load, self.unit.as_millis())
    }
}

impl EmaCurrentLoad {
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
    pub fn new(window: Duration, reactivity: EmaReactivity) -> Self {
        Self::with_starting_time(window, reactivity, Instant::now())
    }

    pub fn builder() -> EmaBuilder {
        EmaBuilder::default()
    }

    pub const fn with_starting_time(
        window: Duration,
        reactivity: EmaReactivity,
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
        // The contribution value of each term decay exponentially.
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

    pub fn current_load(&self) -> EmaCurrentLoad {
        let ema_load = self.current_load_ema_in_native_unit();
        let unit = self.window;
        EmaCurrentLoad { ema_load, unit }
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

    ///
    /// May trigger an EMA update if the last update was more than `window` ago.
    ///
    pub fn record_no_load(&self, now: Instant) {
        self.update_ema_if_needed(now);
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Write as _, str::FromStr};

    #[test]
    fn test_emareactivity_from_str() {
        assert_eq!(
            EmaReactivity::from_str("Very Reactive").unwrap(),
            EmaReactivity::VeryReactive
        );
        assert_eq!(
            EmaReactivity::from_str("Reactive").unwrap(),
            EmaReactivity::Reactive
        );
        assert_eq!(
            EmaReactivity::from_str("Moderately Reactive").unwrap(),
            EmaReactivity::ModeratelyReactive
        );
        assert_eq!(
            EmaReactivity::from_str("Less Reactive").unwrap(),
            EmaReactivity::LessReactive
        );
        assert!(EmaReactivity::from_str("invalid").is_err());
    }

    #[test]
    fn test_emareactivity_display() {
        let mut s = String::new();
        write!(&mut s, "{}", EmaReactivity::VeryReactive).unwrap();
        assert_eq!(s, "Very Reactive");
        s.clear();
        write!(&mut s, "{}", EmaReactivity::Reactive).unwrap();
        assert_eq!(s, "Reactive");
        s.clear();
        write!(&mut s, "{}", EmaReactivity::ModeratelyReactive).unwrap();
        assert_eq!(s, "Moderately Reactive");
        s.clear();
        write!(&mut s, "{}", EmaReactivity::LessReactive).unwrap();
        assert_eq!(s, "Less Reactive");
    }

    #[test]
    fn test_emareactivity_as_period() {
        assert_eq!(EmaReactivity::VeryReactive.as_period(), 3);
        assert_eq!(EmaReactivity::Reactive.as_period(), 5);
        assert_eq!(EmaReactivity::ModeratelyReactive.as_period(), 10);
        assert_eq!(EmaReactivity::LessReactive.as_period(), 20);
    }

    #[test]
    fn test_ema_alpha_for_all_reactivities() {
        let window = Duration::from_secs(10);
        let very = Ema::new(window, EmaReactivity::VeryReactive);
        let reactive = Ema::new(window, EmaReactivity::Reactive);
        let moderate = Ema::new(window, EmaReactivity::ModeratelyReactive);
        let less = Ema::new(window, EmaReactivity::LessReactive);
        assert!((very.alpha() - 0.5).abs() < 1e-6);
        assert!((reactive.alpha() - 0.3333333).abs() < 1e-6);
        assert!((moderate.alpha() - 0.1818181).abs() < 1e-6);
        assert!((less.alpha() - 0.0952380).abs() < 1e-6);
    }

    #[test]
    fn test_ema_function_computation() {
        let window = Duration::from_secs(10);
        let ema = Ema::new(window, EmaReactivity::Reactive); // period = 5, alpha = 0.333...
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
        let ema = Ema::new(window, EmaReactivity::Reactive);
        assert_eq!(ema.window, window);
        assert_eq!(ema.reactivity, EmaReactivity::Reactive);
        assert_eq!(ema.current_load_ema_in_native_unit(), 0);
    }

    #[test]
    fn test_record_load_increments_and_updates_ema_after_window() {
        let now = Instant::now();
        const WINDOW: Duration = Duration::from_millis(1);
        let ema = Ema::with_starting_time(WINDOW, EmaReactivity::VeryReactive, now);
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
        let ema = Ema::with_starting_time(WINDOW, EmaReactivity::VeryReactive, now);
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
        let ema = Ema::new(Duration::from_secs(10), EmaReactivity::VeryReactive);
        let alpha = ema.alpha();
        assert!(alpha > 0.0 && alpha < 1.0);
    }
}
