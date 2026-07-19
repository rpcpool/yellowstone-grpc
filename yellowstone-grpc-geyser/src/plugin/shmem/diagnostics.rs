use crate::plugin::{message::Message, shmem::ProstShmemDecoder};
use std::time::Duration;
use yellowstone_shmem_client::ShmemClient;

pub struct ShmemHealthReporter {
    accounts: u64,
    transactions: u64,
    slots: u64,
    entries: u64,
    block_meta: u64,
    lagged: u64,
    pub interval: tokio::time::Interval,
    enabled: bool,
}

impl ShmemHealthReporter {
    pub fn new(period_secs: Duration) -> Self {
        if period_secs.is_zero() {
            Self {
                accounts: 0,
                transactions: 0,
                slots: 0,
                entries: 0,
                block_meta: 0,
                lagged: 0,
                interval: tokio::time::interval(Duration::from_secs(u64::MAX)),
                enabled: false,
            }
        } else {
            Self {
                accounts: 0,
                transactions: 0,
                slots: 0,
                entries: 0,
                block_meta: 0,
                lagged: 0,
                interval: tokio::time::interval(period_secs),
                enabled: true,
            }
        }
    }

    #[inline]
    pub fn observe(&mut self, message: &Message) {
        if !self.enabled {
            return;
        }
        match message {
            Message::Account(_) => self.accounts += 1,
            Message::Transaction(_) => self.transactions += 1,
            Message::Slot(_) => self.slots += 1,
            Message::Entry(_) => self.entries += 1,
            Message::BlockMeta(_) => self.block_meta += 1,
            _ => {}
        }
    }

    #[inline]
    pub fn observe_lagged(&mut self, n: u64) {
        if !self.enabled {
            return;
        }
        self.lagged += n;
    }

    pub fn report(&mut self, client: &ShmemClient<ProstShmemDecoder>) {
        if !self.enabled {
            return;
        }
        let head = client.writer_head();
        let tail = client.tail();
        log::info!(
            "shmem health: head={} tail={} gap={} | accounts={} tx={} slots={} entries={} blockmeta={} lagged={}",
            head, tail, head.saturating_sub(tail),
            self.accounts, self.transactions, self.slots,
            self.entries, self.block_meta, self.lagged,
        );
        self.accounts = 0;
        self.transactions = 0;
        self.slots = 0;
        self.entries = 0;
        self.block_meta = 0;
        self.lagged = 0;
    }
}
