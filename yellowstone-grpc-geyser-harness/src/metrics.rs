use hdrhistogram::Histogram;
use std::time::Duration;

pub struct ThreadHistogram {
    pub account: Histogram<u64>,
    pub slot: Histogram<u64>,
    pub errors: u64,
    pub panics: u64,
}

impl ThreadHistogram {
    pub fn new() -> Self {
        Self {
            account: Histogram::new(3).expect("histogram"),
            slot: Histogram::new(3).expect("histogram"),
            errors: 0,
            panics: 0,
        }
    }

    pub fn record_account(&mut self, nanos: u64) {
        let _ = self.account.record(nanos);
    }

    pub fn record_slot(&mut self, nanos: u64) {
        let _ = self.slot.record(nanos);
    }
}

pub struct HistogramReport {
    pub count: u64,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub max: Duration,
    pub mean: Duration,
}

impl HistogramReport {
    fn from_histogram(h: &Histogram<u64>) -> Self {
        Self {
            count: h.len(),
            p50: Duration::from_nanos(h.value_at_quantile(0.50)),
            p95: Duration::from_nanos(h.value_at_quantile(0.95)),
            p99: Duration::from_nanos(h.value_at_quantile(0.99)),
            max: Duration::from_nanos(h.max()),
            mean: Duration::from_nanos(h.mean() as u64),
        }
    }
}

pub struct BenchReport {
    pub duration: Duration,
    pub total_events: u64,
    pub events_per_sec: f64,
    pub account_latency: HistogramReport,
    pub slot_latency: HistogramReport,
    pub errors: u64,
    pub panics: u64,
}

impl BenchReport {
    pub fn from_thread_histograms(
        histograms: Vec<ThreadHistogram>,
        duration: Duration,
    ) -> Self {
        let mut account = Histogram::<u64>::new(3).expect("histogram");
        let mut slot = Histogram::<u64>::new(3).expect("histogram");
        let mut errors = 0u64;
        let mut panics = 0u64;

        for h in &histograms {
            account.add(&h.account).expect("merge");
            slot.add(&h.slot).expect("merge");
            errors += h.errors;
            panics += h.panics;
        }

        let total_events = account.len() + slot.len();

        Self {
            duration,
            total_events,
            events_per_sec: total_events as f64 / duration.as_secs_f64(),
            account_latency: HistogramReport::from_histogram(&account),
            slot_latency: HistogramReport::from_histogram(&slot),
            errors,
            panics,
        }
    }
}

impl std::fmt::Display for BenchReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "duration:        {:?}", self.duration)?;
        writeln!(f, "total events:    {}", self.total_events)?;
        writeln!(f, "events/sec:      {:.0}", self.events_per_sec)?;
        writeln!(f, "errors:          {}", self.errors)?;
        writeln!(f, "panics:          {}", self.panics)?;
        writeln!(f)?;
        write_histogram(f, "account", &self.account_latency)?;
        write_histogram(f, "slot", &self.slot_latency)
    }
}

fn write_histogram(
    f: &mut std::fmt::Formatter<'_>,
    name: &str,
    h: &HistogramReport,
) -> std::fmt::Result {
    writeln!(f, "{name} latency ({} calls):", h.count)?;
    writeln!(f, "  p50:  {:?}", h.p50)?;
    writeln!(f, "  p95:  {:?}", h.p95)?;
    writeln!(f, "  p99:  {:?}", h.p99)?;
    writeln!(f, "  max:  {:?}", h.max)?;
    writeln!(f, "  mean: {:?}", h.mean)
}