//! Turns the fixture into one created_at-ordered event sequence per lane.
//! Statuses come from the deshred stream, which carries callback-time `created_at`; see README.md.
//! Each event names the progress of other lanes it must wait for, so agave's ordering guarantees hold.

use {
    super::events::{self, Converter, Event},
    crate::{
        fixture::{FixtureReader, Manifest, StreamId},
        frames::{self, Decoded},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    crossbeam_channel::Sender,
    log::{info, warn},
    serde::Serialize,
    std::{
        cmp::Ordering,
        collections::{BTreeMap, BinaryHeap},
        path::Path,
    },
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, subscribe_update_deshred::UpdateOneof as DeshredOneof,
    },
};

/// agave reports Processed right after BlockMeta; this keeps it behind BlockMeta when both share a stamp.
const PROCESSED_AFTER_META_NS: u64 = 100_000;

/// How callbacks are spread over driver threads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum LaneMode {
    /// One thread for bank data and statuses, one for deshred.
    Single,
    /// Threads shaped like agave's callers: replay, scheduler workers, entry notifier,
    /// transaction-status writer, bank notifier, and shred insertion.
    Agave,
}

pub struct LaneSpec {
    pub name: String,
    /// Agave runs this caller on the notifier cores, next to the plugin's runtime.
    pub notifier: bool,
    /// Lanes whose earlier events must have returned before this lane calls the plugin.
    pub deps: Vec<usize>,
}

/// Routes each event to a lane and says which lanes it waits for.
pub struct Router {
    mode: LaneMode,
    sched: usize,
    pub lanes: Vec<LaneSpec>,
}

impl Router {
    pub fn new(mode: LaneMode, sched: usize) -> Self {
        let sched = sched.max(1);
        let lane = |name: String, notifier: bool, deps: Vec<usize>| LaneSpec {
            name,
            notifier,
            deps,
        };
        let lanes = match mode {
            LaneMode::Single => vec![
                lane("ygbLaneMain".to_owned(), false, vec![]),
                lane("ygbLaneDeshred".to_owned(), false, vec![]),
            ],
            LaneMode::Agave => {
                // 0 replay, 1..=sched scheduler workers, then entry, tx status, bank notifier, shred.
                let workers: Vec<usize> = (1..=sched).collect();
                let mut lanes = vec![lane("ygbReplay".to_owned(), false, workers.clone())];
                lanes
                    .extend((0..sched).map(|k| lane(format!("ygbScHandle{k:02}"), false, vec![0])));
                lanes.push(lane("ygbEntryNotif".to_owned(), true, vec![0]));
                lanes.push(lane(
                    "ygbTxStatus".to_owned(),
                    true,
                    [0].into_iter().chain(workers).collect(),
                ));
                lanes.push(lane("ygbBankNotif".to_owned(), false, vec![0, sched + 2]));
                lanes.push(lane("ygbShred".to_owned(), false, vec![]));
                lanes
            }
        };
        Self { mode, sched, lanes }
    }

    pub fn route(&self, event: &Event) -> usize {
        match self.mode {
            LaneMode::Single => match event {
                Event::Deshred(_) | Event::DeshredUpdateParent { .. } => 1,
                _ => 0,
            },
            LaneMode::Agave => match event {
                // Writes of one pubkey stay on one worker, so they keep their store order.
                Event::Account(account) if account.txn.is_some() => {
                    let key = account.pubkey.get(..8).map_or(0, |bytes| {
                        u64::from_le_bytes(bytes.try_into().expect("8 bytes"))
                    });
                    1 + (key % self.sched as u64) as usize
                }
                Event::Account(_)
                | Event::BlockMeta(_)
                | Event::BankStatus {
                    status: SlotStatus::CreatedBank,
                    ..
                }
                | Event::SlotStatus {
                    status: SlotStatus::Dead(_),
                    ..
                } => 0,
                Event::Entry(_) | Event::EntryUpdateParent { .. } => self.sched + 1,
                Event::Transaction(_) => self.sched + 2,
                Event::BankStatus { .. } => self.sched + 3,
                Event::SlotStatus { .. }
                | Event::Deshred(_)
                | Event::DeshredUpdateParent { .. } => self.sched + 4,
            },
        }
    }
}

pub enum LaneItem {
    Event {
        created_ns: u64,
        paced: bool,
        event: Event,
        /// Position in the global release order.
        order: u64,
        /// (lane, events): wait until that lane has finished this many events in release order.
        deps: Vec<(usize, u64)>,
    },
    PacedStart {
        created_ns: u64,
    },
    End,
}

#[derive(Debug, Clone)]
pub struct ScheduleConfig {
    pub start_slot: Option<u64>,
    pub prefill_slots: u64,
    pub paced_slots: Option<u64>,
    pub tail_ns: u64,
    pub reorder_margin_ns: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct ScheduleStats {
    pub frames: u64,
    pub start_slot: u64,
    pub paced_start_slot: u64,
    pub end_slot: Option<u64>,
    pub prefill_events: u64,
    pub paced_events: u64,
    pub dropped_outside_window: u64,
    pub skipped_frames: BTreeMap<String, u64>,
    pub conversion_errors: u64,
    pub max_pending: usize,
    pub statuses_from: String,
}

struct Pending {
    key: u64,
    seq: u64,
    created_ns: u64,
    event: Event,
}

impl PartialEq for Pending {
    fn eq(&self, other: &Self) -> bool {
        (self.key, self.seq) == (other.key, other.seq)
    }
}
impl Eq for Pending {}
impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Pending {
    // BinaryHeap is a max-heap; reverse so the smallest key pops first.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.key, other.seq).cmp(&(self.key, self.seq))
    }
}

/// Window state: which events are dropped, prefilled, paced, or past the end.
struct Window {
    start_slot: u64,
    paced_start_slot: u64,
    end_slot: Option<u64>,
    tail_ns: u64,
    paced_from_ns: Option<u64>,
    end_from_ns: Option<u64>,
}

enum Placement {
    Drop,
    Emit { paced: bool, starts_pacing: bool },
    Stop,
}

impl Window {
    fn place(&mut self, slot: u64, created_ns: u64) -> Placement {
        if let Some(end_ns) = self.end_from_ns {
            if created_ns > end_ns.saturating_add(self.tail_ns) {
                return Placement::Stop;
            }
        }
        if slot < self.start_slot {
            return Placement::Drop;
        }
        if let Some(end_slot) = self.end_slot {
            if slot >= end_slot {
                self.end_from_ns.get_or_insert(created_ns);
                return Placement::Drop;
            }
        }
        let mut starts_pacing = false;
        if self.paced_from_ns.is_none() && slot >= self.paced_start_slot {
            self.paced_from_ns = Some(created_ns);
            starts_pacing = true;
        }
        Placement::Emit {
            paced: self.paced_from_ns.is_some(),
            starts_pacing,
        }
    }
}

pub fn run(
    dir: &Path,
    manifest: &Manifest,
    config: &ScheduleConfig,
    router: &Router,
    lanes: &[Sender<LaneItem>],
) -> anyhow::Result<ScheduleStats> {
    let has_deshred = manifest.requests.contains_key("deshred");
    let streams: Vec<StreamId> = [StreamId::Main, StreamId::Deshred, StreamId::MainAccounts]
        .into_iter()
        .filter(|stream| manifest.requests.contains_key(stream.name()))
        .collect();
    let start_slot = match config.start_slot {
        Some(slot) => slot,
        None => {
            first_block_meta_slot(dir, manifest)?
                .ok_or_else(|| anyhow::anyhow!("fixture has no BlockMeta"))?
                + 20
        }
    };
    let mut window = Window {
        start_slot,
        paced_start_slot: start_slot + config.prefill_slots,
        end_slot: config
            .paced_slots
            .map(|slots| start_slot + config.prefill_slots + slots),
        tail_ns: config.tail_ns,
        paced_from_ns: None,
        end_from_ns: None,
    };
    let mut stats = ScheduleStats {
        start_slot,
        paced_start_slot: window.paced_start_slot,
        end_slot: window.end_slot,
        statuses_from: if has_deshred { "deshred" } else { "main" }.to_owned(),
        ..Default::default()
    };
    info!(
        "schedule: start slot {start_slot}, paced from slot {}, end slot {:?}, statuses from {} stream",
        window.paced_start_slot, window.end_slot, stats.statuses_from
    );

    let mut converter = Converter::default();
    // Commitment statuses trail their slot by about 32 slots; start a margin earlier than that.
    let mut reader = FixtureReader::open_from(dir, &manifest.chunks, start_slot.saturating_sub(64));
    let mut heap = BinaryHeap::new();
    let mut watermarks: [Option<u64>; 3] = [None, None, None];
    let mut seq = 0u64;
    let mut stopped = false;
    let mut order = 0u64;
    // Per lane: release order of its last event plus one.
    let mut dispatched = vec![0u64; lanes.len()];

    let mut release =
        |pending: Pending, stats: &mut ScheduleStats, stopped: &mut bool| -> anyhow::Result<()> {
            if *stopped {
                return Ok(());
            }
            match window.place(pending.event.slot(), pending.created_ns) {
                Placement::Drop => stats.dropped_outside_window += 1,
                Placement::Stop => *stopped = true,
                Placement::Emit {
                    paced,
                    starts_pacing,
                } => {
                    if starts_pacing {
                        info!("pacing starts at slot {}", pending.event.slot());
                        for tx in lanes {
                            tx.send(LaneItem::PacedStart {
                                created_ns: pending.created_ns,
                            })?;
                        }
                    }
                    if paced {
                        stats.paced_events += 1;
                    } else {
                        stats.prefill_events += 1;
                    }
                    let lane = router.route(&pending.event);
                    let deps = router.lanes[lane]
                        .deps
                        .iter()
                        .filter(|dep| dispatched[**dep] > 0)
                        .map(|dep| (*dep, dispatched[*dep]))
                        .collect();
                    lanes[lane].send(LaneItem::Event {
                        created_ns: pending.created_ns,
                        paced,
                        event: pending.event,
                        order,
                        deps,
                    })?;
                    order += 1;
                    dispatched[lane] = order;
                }
            }
            Ok(())
        };

    loop {
        let Some(frame) = reader.next_frame().unwrap_or_else(|error| {
            warn!("fixture read stopped: {error:#}");
            None
        }) else {
            break;
        };
        stats.frames += 1;
        let decoded = frames::decode(frame.stream, &frame.data)?;
        let info = frames::info(&decoded);
        let Some(created_ns) = info.created_at_ns else {
            continue;
        };
        let stream_index = usize::from(frame.stream.as_u8());
        watermarks[stream_index] =
            Some(watermarks[stream_index].map_or(created_ns, |w| w.max(created_ns)));

        match convert(&mut converter, decoded, has_deshred) {
            Ok(Some(event)) => {
                let bump = match &event {
                    Event::BankStatus {
                        status: SlotStatus::Processed,
                        ..
                    } => PROCESSED_AFTER_META_NS,
                    _ => 0,
                };
                heap.push(Pending {
                    key: created_ns + bump,
                    seq,
                    created_ns,
                    event,
                });
                seq += 1;
            }
            Ok(None) => {
                *stats
                    .skipped_frames
                    .entry(format!("{}.{}", frame.stream.name(), info.kind))
                    .or_default() += 1;
            }
            Err(error) => {
                stats.conversion_errors += 1;
                if stats.conversion_errors <= 10 {
                    warn!("conversion failed: {error:#}");
                }
            }
        }
        stats.max_pending = stats.max_pending.max(heap.len());

        // Release what every stream has moved past; streams lag each other by up to seconds.
        let safe = streams
            .iter()
            .map(|stream| watermarks[usize::from(stream.as_u8())])
            .collect::<Option<Vec<u64>>>()
            .and_then(|marks| marks.into_iter().min());
        if let Some(safe) = safe.map(|w| w.saturating_sub(config.reorder_margin_ns)) {
            while heap.peek().is_some_and(|pending| pending.key <= safe) {
                release(heap.pop().expect("peeked"), &mut stats, &mut stopped)?;
            }
        }
        if stopped {
            break;
        }
    }
    while let Some(pending) = heap.pop() {
        release(pending, &mut stats, &mut stopped)?;
    }
    for tx in lanes {
        tx.send(LaneItem::End)?;
    }
    Ok(stats)
}

/// The first slot with a BlockMeta. The manifest's first slot also counts statuses of older slots.
fn first_block_meta_slot(dir: &Path, manifest: &Manifest) -> anyhow::Result<Option<u64>> {
    let mut reader = FixtureReader::open(dir, &manifest.chunks);
    while let Some(frame) = reader.next_frame()? {
        if frame.stream == StreamId::Deshred {
            continue;
        }
        if let Decoded::Main(update) = frames::decode(frame.stream, &frame.data)? {
            if let Some(UpdateOneof::BlockMeta(meta)) = update.update_oneof {
                return Ok(Some(meta.slot));
            }
        }
    }
    Ok(None)
}

fn convert(
    converter: &mut Converter,
    decoded: Decoded,
    statuses_from_deshred: bool,
) -> anyhow::Result<Option<Event>> {
    Ok(match decoded {
        Decoded::Main(update) => match update.update_oneof {
            Some(UpdateOneof::Account(msg)) => converter.account(msg)?,
            Some(UpdateOneof::Transaction(msg)) => Some(events::transaction(msg)?),
            Some(UpdateOneof::Entry(msg)) => Some(events::entry(msg)?),
            Some(UpdateOneof::BlockMeta(msg)) => Some(events::block_meta(msg)?),
            Some(UpdateOneof::EntryUpdateParent(msg)) => Some(events::entry_update_parent(msg)?),
            Some(UpdateOneof::Slot(msg)) if !statuses_from_deshred => events::slot_status(msg)?,
            _ => None,
        },
        Decoded::Deshred(update) => match update.update_oneof {
            Some(DeshredOneof::DeshredTransaction(msg)) => Some(events::deshred_transaction(msg)?),
            Some(DeshredOneof::DeshredUpdateParent(msg)) => {
                Some(events::deshred_update_parent(msg)?)
            }
            Some(DeshredOneof::Slot(msg)) if statuses_from_deshred => events::slot_status(msg)?,
            _ => None,
        },
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{LaneMode, Router},
        crate::driver::events::{Converter, Event},
        agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
        yellowstone_grpc_proto::geyser::{SubscribeUpdateAccount, SubscribeUpdateAccountInfo},
    };

    fn account(pubkey: u8, signed: bool) -> Event {
        Converter::default()
            .account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: vec![pubkey; 32],
                    owner: vec![0; 32],
                    txn_signature: signed.then(|| vec![7; 64]),
                    ..Default::default()
                }),
                slot: 1,
                bank_id: Some(1),
                ..Default::default()
            })
            .unwrap()
            .unwrap()
    }

    fn bank(status: SlotStatus) -> Event {
        Event::BankStatus {
            slot: 1,
            parent: None,
            status,
            bank_id: 1,
        }
    }

    fn slot(status: SlotStatus) -> Event {
        Event::SlotStatus {
            slot: 1,
            parent: None,
            status,
        }
    }

    #[test]
    fn agave_lanes_route_like_agave_callers() {
        let router = Router::new(LaneMode::Agave, 4);
        let names: Vec<&str> = router.lanes.iter().map(|lane| lane.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "ygbReplay",
                "ygbScHandle00",
                "ygbScHandle01",
                "ygbScHandle02",
                "ygbScHandle03",
                "ygbEntryNotif",
                "ygbTxStatus",
                "ygbBankNotif",
                "ygbShred"
            ]
        );
        assert_eq!(router.route(&account(9, false)), 0);
        let worker = router.route(&account(9, true));
        assert!((1..=4).contains(&worker));
        assert_eq!(
            router.route(&account(9, true)),
            worker,
            "one pubkey stays on one worker"
        );
        assert_eq!(router.route(&bank(SlotStatus::CreatedBank)), 0);
        assert_eq!(router.route(&bank(SlotStatus::Processed)), 7);
        assert_eq!(router.route(&bank(SlotStatus::Rooted)), 7);
        assert_eq!(router.route(&slot(SlotStatus::Dead(String::new()))), 0);
        assert_eq!(router.route(&slot(SlotStatus::FirstShredReceived)), 8);
        let one = Router::new(LaneMode::Agave, 0);
        assert_eq!(one.lanes.len(), 6);
        assert_eq!(one.route(&account(9, true)), 1);
        assert_eq!(
            one.lanes[one.route(&slot(SlotStatus::Completed))].name,
            "ygbShred"
        );
        let single = Router::new(LaneMode::Single, 4);
        assert_eq!(single.lanes.len(), 2);
        assert_eq!(single.route(&account(9, true)), 0);
    }

    #[test]
    fn agave_dependencies_follow_the_ordering_rules() {
        let router = Router::new(LaneMode::Agave, 2);
        let deps = |name: &str| {
            router
                .lanes
                .iter()
                .find(|lane| lane.name == name)
                .unwrap()
                .deps
                .clone()
        };
        // BlockMeta after every account write; writes and entries after CreatedBank.
        assert_eq!(deps("ygbReplay"), vec![1, 2]);
        assert_eq!(deps("ygbScHandle00"), vec![0]);
        assert_eq!(deps("ygbEntryNotif"), vec![0]);
        // A transaction after its account writes; Processed after BlockMeta and the transactions.
        assert_eq!(deps("ygbTxStatus"), vec![0, 1, 2]);
        assert_eq!(deps("ygbBankNotif"), vec![0, 4]);
        assert!(deps("ygbShred").is_empty());
        assert!(router.lanes.iter().filter(|lane| lane.notifier).count() == 2);
    }
}
