//! `ygb inspect`: per-bank completeness checks, rates and timing facts of a fixture.

use {
    crate::{
        fixture::{FixtureReader, Manifest, StreamId},
        frames::{self, Decoded},
    },
    std::{
        collections::{BTreeMap, HashMap},
        path::PathBuf,
    },
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, subscribe_update_deshred::UpdateOneof as DeshredOneof,
        SlotStatus, SubscribeUpdateSlot,
    },
};

#[derive(Debug, clap::Args)]
pub struct InspectArgs {
    /// Fixture directory
    #[clap(long)]
    fixture: PathBuf,
    /// Window length, in slots, for the heaviest-window search
    #[clap(long, default_value_t = 1500)]
    window: usize,
    /// Number of failing banks to print per check
    #[clap(long, default_value_t = 5)]
    examples: usize,
}

#[derive(Debug, Default)]
struct Bank {
    slot: u64,
    bytes: u64,
    accounts: u64,
    txs: u64,
    entries: u64,
    footer: bool,
    block_meta: Option<(u64, u64, u64)>,
    block_meta_ns: Option<u64>,
    main_status_ns: [Option<u64>; 7],
    deshred_status_ns: [Option<u64>; 7],
}

#[derive(Default)]
struct Totals {
    counts: BTreeMap<String, u64>,
    bytes: BTreeMap<String, u64>,
    lag_ms: BTreeMap<StreamId, Vec<f64>>,
    first_created_ns: Option<u64>,
    last_created_ns: Option<u64>,
    deshred_txs_by_slot: HashMap<u64, u64>,
}

pub fn run(args: InspectArgs) -> anyhow::Result<()> {
    let manifest = Manifest::load(&args.fixture)?;
    println!(
        "fixture {}: endpoint {}, {} chunks, end reason '{}'",
        args.fixture.display(),
        manifest.endpoint,
        manifest.chunks.len(),
        manifest.end_reason
    );
    if let Some(version) = &manifest.server_version {
        println!("server version: {version}");
    }

    let mut banks: BTreeMap<u64, Bank> = BTreeMap::new();
    let mut totals = Totals::default();
    let mut reader = FixtureReader::open(&args.fixture, &manifest.chunks);
    let mut frames_read = 0u64;
    let read_error = loop {
        let frame = match reader.next_frame() {
            Ok(Some(frame)) => frame,
            Ok(None) => break None,
            Err(error) => break Some(error),
        };
        frames_read += 1;
        let decoded = frames::decode(frame.stream, &frame.data)?;
        let info = frames::info(&decoded);
        let key = format!("{}.{}", frame.stream.name(), info.kind);
        *totals.counts.entry(key.clone()).or_default() += 1;
        *totals.bytes.entry(key).or_default() += frame.data.len() as u64;
        if let Some(created_ns) = info.created_at_ns {
            if info.kind != "ping" && info.kind != "pong" {
                totals.first_created_ns = Some(
                    totals
                        .first_created_ns
                        .map_or(created_ns, |v| v.min(created_ns)),
                );
                totals.last_created_ns = Some(
                    totals
                        .last_created_ns
                        .map_or(created_ns, |v| v.max(created_ns)),
                );
                totals
                    .lag_ms
                    .entry(frame.stream)
                    .or_default()
                    .push((frame.recv_ns as f64 - created_ns as f64) / 1e6);
            }
        }
        let created_ns = info.created_at_ns.unwrap_or_default();
        match &decoded {
            Decoded::Main(update) => match &update.update_oneof {
                Some(UpdateOneof::Account(msg)) => {
                    if let Some(bank) = msg.bank_id.map(|id| bank_mut(&mut banks, id, msg.slot)) {
                        bank.accounts += 1;
                        bank.bytes += frame.data.len() as u64;
                    }
                }
                Some(UpdateOneof::Transaction(msg)) => {
                    let bank = bank_mut(&mut banks, msg.bank_id, msg.slot);
                    bank.txs += 1;
                    bank.bytes += frame.data.len() as u64;
                }
                Some(UpdateOneof::Entry(msg)) => {
                    let bank = bank_mut(&mut banks, msg.bank_id, msg.slot);
                    bank.entries += 1;
                    bank.bytes += frame.data.len() as u64;
                }
                Some(UpdateOneof::BlockFooter(msg)) => {
                    bank_mut(&mut banks, msg.bank_id, msg.slot).footer = true;
                }
                Some(UpdateOneof::BlockMeta(msg)) => {
                    let bank = bank_mut(&mut banks, msg.bank_id, msg.slot);
                    bank.block_meta = Some((
                        msg.executed_transaction_count,
                        msg.entries_count,
                        msg.parent_slot,
                    ));
                    bank.block_meta_ns = Some(created_ns);
                }
                Some(UpdateOneof::Slot(msg)) => {
                    record_status(&mut banks, msg, created_ns, false);
                }
                _ => {}
            },
            Decoded::Deshred(update) => match &update.update_oneof {
                Some(DeshredOneof::Slot(msg)) => record_status(&mut banks, msg, created_ns, true),
                Some(DeshredOneof::DeshredTransaction(msg)) => {
                    *totals.deshred_txs_by_slot.entry(msg.slot).or_default() += 1;
                }
                _ => {}
            },
        }
    };
    println!("frames read: {frames_read}");
    if let Some(error) = read_error {
        println!("WARNING: reading stopped early: {error:#}");
    }

    print_totals(&totals);
    check_banks(&banks, &totals, args.examples);
    print_timing(&banks);
    print_heaviest_window(&banks, args.window);
    Ok(())
}

fn bank_mut(banks: &mut BTreeMap<u64, Bank>, bank_id: u64, slot: u64) -> &mut Bank {
    let bank = banks.entry(bank_id).or_default();
    bank.slot = slot;
    bank
}

fn record_status(
    banks: &mut BTreeMap<u64, Bank>,
    msg: &SubscribeUpdateSlot,
    created_ns: u64,
    deshred: bool,
) {
    let Some(bank_id) = msg.bank_id else {
        return;
    };
    let index = usize::try_from(msg.status).unwrap_or(usize::MAX);
    let bank = bank_mut(banks, bank_id, msg.slot);
    let statuses = if deshred {
        &mut bank.deshred_status_ns
    } else {
        &mut bank.main_status_ns
    };
    if let Some(slot) = statuses.get_mut(index) {
        slot.get_or_insert(created_ns);
    }
}

fn print_totals(totals: &Totals) {
    let span_s = match (totals.first_created_ns, totals.last_created_ns) {
        (Some(first), Some(last)) if last > first => (last - first) as f64 / 1e9,
        _ => 0.0,
    };
    println!("\ncreated_at span: {span_s:.1} s");
    println!(
        "{:<32} {:>12} {:>10} {:>12}",
        "kind", "count", "per s", "MiB"
    );
    for (key, count) in &totals.counts {
        let bytes = totals.bytes.get(key).copied().unwrap_or_default();
        println!(
            "{:<32} {:>12} {:>10.1} {:>12.1}",
            key,
            count,
            *count as f64 / span_s.max(1e-9),
            bytes as f64 / (1 << 20) as f64
        );
    }
    for (stream, lags) in &totals.lag_ms {
        let q = quantiles(lags);
        println!(
            "recv - created_at, {}: p50 {:.1} ms, p99 {:.1} ms, max {:.1} ms",
            stream.name(),
            q.0,
            q.1,
            q.2
        );
    }
}

fn check_banks(banks: &BTreeMap<u64, Bank>, totals: &Totals, examples: usize) {
    let created = SlotStatus::SlotCreatedBank as usize;
    let processed = SlotStatus::SlotProcessed as usize;
    let any_footer = banks.values().any(|bank| bank.footer);
    let any_deshred = banks
        .values()
        .any(|bank| bank.deshred_status_ns.iter().any(Option::is_some));
    let complete: Vec<(&u64, &Bank)> = banks
        .iter()
        .filter(|(_, bank)| {
            bank.block_meta.is_some()
                && (bank.main_status_ns[created].is_some()
                    || bank.deshred_status_ns[created].is_some())
        })
        .collect();
    println!(
        "\nbanks: {} seen, {} complete (CreatedBank and BlockMeta), footers {}, deshred statuses {}",
        banks.len(),
        complete.len(),
        if any_footer { "present" } else { "absent" },
        if any_deshred { "present" } else { "absent" }
    );

    let mut failures: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (bank_id, bank) in &complete {
        let (executed, entries_count, _) = bank.block_meta.expect("complete bank has BlockMeta");
        let mut fail = |check: &'static str, detail: String| {
            failures
                .entry(check)
                .or_default()
                .push(format!("bank {bank_id} slot {}: {detail}", bank.slot));
        };
        if bank.txs != executed {
            fail(
                "tx count",
                format!("{} txs, BlockMeta says {executed}", bank.txs),
            );
        }
        if entries_count > 0 && bank.entries != entries_count {
            fail(
                "entry count",
                format!("{} entries, BlockMeta says {entries_count}", bank.entries),
            );
        }
        if any_footer && !bank.footer {
            fail("footer", "missing".to_owned());
        }
        if any_deshred && bank.deshred_status_ns[processed].is_none() {
            fail("deshred Processed", "missing".to_owned());
        }
        if bank.main_status_ns[processed].is_none() {
            fail("main Processed", "missing".to_owned());
        }
        if any_deshred
            && totals
                .deshred_txs_by_slot
                .get(&bank.slot)
                .copied()
                .unwrap_or_default()
                == 0
            && bank.txs > 0
        {
            fail("deshred txs", "none recorded for the slot".to_owned());
        }
    }
    if failures.is_empty() {
        println!("all {} complete banks pass every check", complete.len());
    }
    for (check, list) in &failures {
        println!("check '{check}' failed for {} banks, e.g.:", list.len());
        for line in list.iter().take(examples) {
            println!("  {line}");
        }
    }
}

/// Prints the gaps between per-bank events that the driver and the reference times rely on.
fn print_timing(banks: &BTreeMap<u64, Bank>) {
    let processed = SlotStatus::SlotProcessed as usize;
    let mut meta_to_processed = Vec::new();
    let mut seal_delay = Vec::new();
    for bank in banks.values() {
        let deshred_processed = bank.deshred_status_ns[processed];
        if let (Some(meta), Some(status)) = (bank.block_meta_ns, deshred_processed) {
            meta_to_processed.push((status as f64 - meta as f64) / 1e6);
        }
        if let (Some(callback), Some(emitted)) = (deshred_processed, bank.main_status_ns[processed])
        {
            seal_delay.push((emitted as f64 - callback as f64) / 1e6);
        }
    }
    if !meta_to_processed.is_empty() {
        let q = quantiles(&meta_to_processed);
        println!(
            "\nProcessed callback - BlockMeta created_at: p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms ({} banks)",
            q.0,
            q.1,
            q.2,
            meta_to_processed.len()
        );
    }
    if !seal_delay.is_empty() {
        let q = quantiles(&seal_delay);
        println!(
            "source node seal delay (main Processed - callback): p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms",
            q.0, q.1, q.2
        );
    }
}

fn print_heaviest_window(banks: &BTreeMap<u64, Bank>, window: usize) {
    let mut by_slot: BTreeMap<u64, u64> = BTreeMap::new();
    for bank in banks.values() {
        *by_slot.entry(bank.slot).or_default() += bank.bytes;
    }
    let slots: Vec<(u64, u64)> = by_slot.into_iter().collect();
    let mut heaviest: Vec<&(u64, u64)> = slots.iter().collect();
    heaviest.sort_by_key(|(_, bytes)| std::cmp::Reverse(*bytes));
    println!("\nheaviest slots:");
    for (slot, bytes) in heaviest.iter().take(5) {
        println!("  slot {slot}: {:.1} MiB", *bytes as f64 / (1 << 20) as f64);
    }
    if slots.len() > window {
        let mut sum: u64 = slots[..window].iter().map(|(_, bytes)| bytes).sum();
        let mut best = (sum, 0usize);
        for end in window..slots.len() {
            sum = sum + slots[end].1 - slots[end - window].1;
            if sum > best.0 {
                best = (sum, end + 1 - window);
            }
        }
        println!(
            "heaviest {window}-slot window: slots {}..={} ({:.1} GiB)",
            slots[best.1].0,
            slots[best.1 + window - 1].0,
            best.0 as f64 / (1u64 << 30) as f64
        );
    }
}

fn quantiles(values: &[f64]) -> (f64, f64, f64) {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let at = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
    (at(0.5), at(0.99), at(1.0))
}
