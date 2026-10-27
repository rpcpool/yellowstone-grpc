//! One emitter thread per lane: unpaced during prefill, then each event at its recorded offset.

use {
    super::schedule::LaneItem,
    crate::{hist::LogHist, sys},
    agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin,
    crossbeam_channel::Receiver,
    log::{info, warn},
    serde::Serialize,
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Barrier, OnceLock,
        },
        time::Duration,
    },
};

/// Spins this long on a dependency, then sleeps in steps of `NAP`; a SCHED_FIFO lane
/// that only yielded would starve kernel threads on its CPU when RT throttling is off.
const SPIN_NS: u64 = 20_000;
const NAP: std::time::Duration = std::time::Duration::from_micros(20);

/// Per lane, how many events in release order it has finished; lanes wait on each other through it.
pub struct Progress(Vec<AtomicU64>);

impl Progress {
    pub fn new(lanes: usize) -> Self {
        Self((0..lanes).map(|_| AtomicU64::new(0)).collect())
    }

    /// Waits until every (lane, events) pair is reached; returns the nanoseconds spent waiting.
    fn wait(&self, deps: &[(usize, u64)]) -> u64 {
        let mut started = None;
        for (lane, need) in deps {
            while self.0[*lane].load(Ordering::Acquire) < *need {
                let begin = *started.get_or_insert_with(sys::mono_ns);
                if sys::mono_ns() - begin < SPIN_NS {
                    std::hint::spin_loop();
                } else {
                    std::thread::sleep(NAP);
                }
            }
        }
        started.map_or(0, |begin| sys::mono_ns() - begin)
    }

    fn finish(&self, lane: usize, order: u64) {
        self.0[lane].store(order + 1, Ordering::Release);
    }
}

/// Both lanes meet here when pacing starts; the leader holds the attach window, then sets the clock.
pub struct PacedStart {
    barrier: Barrier,
    attach: Duration,
    clock: OnceLock<(u64, u64)>,
}

impl PacedStart {
    pub const fn new(lanes: usize, attach: Duration) -> Self {
        Self {
            barrier: Barrier::new(lanes),
            attach,
            clock: OnceLock::new(),
        }
    }

    /// Returns (created_at origin, monotonic origin) shared by all lanes.
    fn start(&self, leader: bool, created_ns: u64, on_attach: &dyn Fn()) -> (u64, u64) {
        self.barrier.wait();
        if leader {
            on_attach();
            std::thread::sleep(self.attach);
            let _ = self.clock.set((created_ns, sys::mono_ns() + 50_000_000));
        }
        self.barrier.wait();
        *self.clock.get().expect("leader set the clock")
    }
}

/// Callback time of a status or BlockMeta event, used as a reference time by `ygb report`.
#[derive(Debug, Clone, Serialize)]
pub struct StatusRecord {
    pub slot: u64,
    pub bank_id: Option<u64>,
    pub status: &'static str,
    pub unix_ns: u64,
}

#[derive(Default)]
pub struct LaneStats {
    pub prefill_events: u64,
    pub paced_events: u64,
    pub errors: u64,
    pub prefill_secs: f64,
    /// Start of the callback minus its deadline, for paced events.
    pub lag: LogHist,
    /// Lag of events whose lane was idle at the deadline: timer and wake-up error.
    pub timer_lag: LogHist,
    /// Lag of events still queued behind earlier callbacks at their deadline.
    pub queue_lag: LogHist,
    /// Time spent waiting for other lanes to reach an event's dependencies.
    pub gate_wait: LogHist,
    pub callback: BTreeMap<&'static str, LogHist>,
    pub statuses: Vec<StatusRecord>,
}

#[derive(Serialize)]
pub struct LaneReport {
    pub prefill_events: u64,
    pub paced_events: u64,
    pub errors: u64,
    pub prefill_secs: f64,
    pub lag_ns: crate::hist::Summary,
    pub timer_lag_ns: crate::hist::Summary,
    pub queue_lag_ns: crate::hist::Summary,
    pub gate_wait_ns: crate::hist::Summary,
    pub callback_ns: BTreeMap<&'static str, crate::hist::Summary>,
}

impl LaneStats {
    pub fn report(&self) -> LaneReport {
        LaneReport {
            prefill_events: self.prefill_events,
            paced_events: self.paced_events,
            errors: self.errors,
            prefill_secs: self.prefill_secs,
            lag_ns: self.lag.summary(),
            timer_lag_ns: self.timer_lag.summary(),
            queue_lag_ns: self.queue_lag.summary(),
            gate_wait_ns: self.gate_wait.summary(),
            callback_ns: self
                .callback
                .iter()
                .map(|(kind, hist)| (*kind, hist.summary()))
                .collect(),
        }
    }
}

pub struct LaneConfig {
    pub name: String,
    pub index: usize,
    pub leader: bool,
    pub speed: f64,
    pub cpus: Vec<usize>,
    /// SCHED_FIFO priority for this lane, as validators that run agave's notifier threads at FIFO.
    pub fifo_priority: Option<i32>,
}

pub fn run(
    config: &LaneConfig,
    rx: &Receiver<LaneItem>,
    plugin: &'static dyn GeyserPlugin,
    paced_start: &PacedStart,
    progress: &Progress,
    on_attach: &dyn Fn(),
) -> LaneStats {
    if let Err(error) = sys::pin_current_thread(&config.cpus) {
        warn!(
            "{}: failed to pin to {:?}: {error}",
            config.name, config.cpus
        );
    }
    sys::set_min_timer_slack();
    if let Some(priority) = config.fifo_priority {
        if let Err(error) = sys::set_fifo(0, priority) {
            warn!("{}: SCHED_FIFO {priority} failed: {error}", config.name);
        }
    }
    let mut stats = LaneStats::default();
    let started = sys::mono_ns();
    let mut clock: Option<(u64, u64)> = None;
    let mut last_end = 0u64;
    for item in rx {
        match item {
            LaneItem::PacedStart { created_ns } => {
                stats.prefill_secs = (sys::mono_ns() - started) as f64 / 1e9;
                info!(
                    "{}: prefill done in {:.1} s ({} events)",
                    config.name, stats.prefill_secs, stats.prefill_events
                );
                clock = Some(paced_start.start(config.leader, created_ns, on_attach));
            }
            LaneItem::Event {
                created_ns,
                paced,
                event,
                order,
                deps,
            } => {
                let deadline = paced.then(|| {
                    let (created_origin, mono_origin) =
                        clock.expect("paced events follow PacedStart");
                    let offset = created_ns.saturating_sub(created_origin) as f64 / config.speed;
                    mono_origin + offset as u64
                });
                if let Some(deadline) = deadline {
                    if deadline > sys::mono_ns() {
                        sys::sleep_until_mono(deadline);
                    }
                    let lag = sys::mono_ns().saturating_sub(deadline);
                    if last_end > deadline {
                        stats.queue_lag.record(lag);
                    } else {
                        stats.timer_lag.record(lag);
                    }
                }
                let waited = progress.wait(&deps);
                if waited > 0 {
                    stats.gate_wait.record(waited);
                }
                if let Some(deadline) = deadline {
                    stats.lag.record(sys::mono_ns().saturating_sub(deadline));
                    stats.paced_events += 1;
                } else {
                    stats.prefill_events += 1;
                }
                if let Some((slot, bank_id, status)) = event.status_record() {
                    stats.statuses.push(StatusRecord {
                        slot,
                        bank_id,
                        status,
                        unix_ns: sys::unix_ns(),
                    });
                }
                let begin = sys::mono_ns();
                if let Err(error) = event.emit(plugin) {
                    stats.errors += 1;
                    if stats.errors <= 10 {
                        warn!("{}: {error:#}", config.name);
                    }
                }
                last_end = sys::mono_ns();
                progress.finish(config.index, order);
                stats
                    .callback
                    .entry(event.kind())
                    .or_default()
                    .record(last_end - begin);
            }
            LaneItem::End => break,
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use {
        super::Progress,
        std::{sync::Arc, time::Duration},
    };

    #[test]
    fn progress_waits_for_other_lanes() {
        let progress = Arc::new(Progress::new(2));
        let waiter = {
            let progress = Arc::clone(&progress);
            std::thread::spawn(move || progress.wait(&[(1, 3)]))
        };
        std::thread::sleep(Duration::from_millis(5));
        for order in 0..3 {
            progress.finish(1, order);
        }
        assert!(waiter.join().unwrap() > 0);
        assert_eq!(progress.wait(&[(1, 3), (0, 0)]), 0);
    }
}
