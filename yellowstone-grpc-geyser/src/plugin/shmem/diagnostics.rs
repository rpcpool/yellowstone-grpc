use crate::plugin::message::Message;
use std::time::Duration;

/// Message counts since the last report.
#[derive(Default)]
struct Counts {
    accounts: u64,
    transactions: u64,
    slots: u64,
    entries: u64,
    block_meta: u64,
    lagged: u64,
}

/// Periodic health logging for the shmem consumer loop.
///
/// Counts messages as they pass through and logs a summary when the caller
/// decides a period has elapsed. It owns neither a timer nor a client: the
/// consumer loop owns both and passes the ring positions in. That keeps this
/// type constructible outside a Tokio runtime and free of a second cursor on
/// the ring.
pub struct ShmemHealthReporter {
    counts: Counts,
    /// Zero disables reporting entirely.
    period: Duration,
}

impl ShmemHealthReporter {
    pub fn new(period: Duration) -> Self {
        Self {
            counts: Counts::default(),
            period,
        }
    }

    pub fn enabled(&self) -> bool {
        !self.period.is_zero()
    }

    /// Reporting period. The caller drives the clock.
    pub fn period(&self) -> Duration {
        self.period
    }

    #[inline]
    pub fn observe(&mut self, message: &Message) {
        if !self.enabled() {
            return;
        }
        match message {
            Message::Account(_) => self.counts.accounts += 1,
            Message::Transaction(_) => self.counts.transactions += 1,
            Message::Slot(_) => self.counts.slots += 1,
            Message::Entry(_) => self.counts.entries += 1,
            Message::BlockMeta(_) => self.counts.block_meta += 1,
            _ => {}
        }
    }

    #[inline]
    pub fn observe_lagged(&mut self, n: u64) {
        if !self.enabled() {
            return;
        }
        self.counts.lagged += n;
    }

    pub fn report(&mut self) {
        if !self.enabled() {
            return;
        }
        let c = &self.counts;
        log::info!(
            "shmem health: accounts={} tx={} slots={} entries={} blockmeta={} lagged={}",
            c.accounts,
            c.transactions,
            c.slots,
            c.entries,
            c.block_meta,
            c.lagged,
        );
        self.counts = Counts::default();
    }
}
