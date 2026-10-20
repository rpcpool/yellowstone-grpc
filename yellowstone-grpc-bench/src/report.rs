//! `ygb report`: latency of probe subscribers in one run, by probe class, compression and kind.

use {
    crate::fleet::{scan::Kind, FleetReport, Sample, SAMPLE_LEN},
    anyhow::Context,
    serde::{Deserialize, Serialize},
    std::{
        collections::{BTreeMap, HashMap},
        fs,
        io::Read,
        path::{Path, PathBuf},
    },
};

#[derive(Debug, Clone, clap::Args)]
pub struct ReportArgs {
    /// Run directory written by `ygb run`
    #[clap(long)]
    pub run: PathBuf,
    /// Seconds after the start of pacing excluded as warm-up
    #[clap(long, default_value_t = 60)]
    pub warmup_secs: u64,
}

#[derive(Deserialize)]
struct DriverTimes {
    paced_start_unix_ns: Option<u64>,
}

#[derive(Deserialize)]
struct StatusRecord {
    slot: u64,
    status: String,
    unix_ns: u64,
}

/// Status names as `SlotStatus::as_str` writes them, indexed by the proto enum value.
const STATUS_NAMES: [&str; 7] = [
    "processed",
    "confirmed",
    "rooted",
    "first_shred_received",
    "completed",
    "created_bank",
    "dead",
];

#[derive(Debug, Serialize)]
pub struct Row {
    pub class: String,
    pub compression: String,
    pub kind: String,
    pub count: usize,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
    /// Median over slots of each slot's p99, for slots with at least 5 samples.
    pub slot_p99_median_us: Option<f64>,
    pub stalls_over_5ms: usize,
    pub stalls_over_20ms: usize,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub run: String,
    pub warmup_secs: u64,
    pub disconnects: usize,
    pub rows: Vec<Row>,
}

pub fn run(args: ReportArgs) -> anyhow::Result<()> {
    let report = build(&args.run, args.warmup_secs)?;
    println!(
        "run {}: {} disconnects, warm-up {} s excluded",
        report.run, report.disconnects, report.warmup_secs
    );
    println!(
        "{:<14} {:<6} {:<14} {:>8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>6} {:>6}",
        "class", "comp", "kind", "n", "p50 us", "p90 us", "p99 us", "p99.9 us", "max us", "slot p99", ">5ms", ">20ms"
    );
    for row in &report.rows {
        println!(
            "{:<14} {:<6} {:<14} {:>8} {:>9.0} {:>9.0} {:>9.0} {:>9.0} {:>9.0} {:>10} {:>6} {:>6}",
            row.class,
            row.compression,
            row.kind,
            row.count,
            row.p50_us,
            row.p90_us,
            row.p99_us,
            row.p999_us,
            row.max_us,
            row.slot_p99_median_us.map_or("-".to_owned(), |v| format!("{v:.0}")),
            row.stalls_over_5ms,
            row.stalls_over_20ms,
        );
    }
    fs::write(args.run.join("report.json"), serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}

pub fn build(run: &Path, warmup_secs: u64) -> anyhow::Result<Report> {
    let driver: DriverTimes = serde_json::from_slice(
        &fs::read(run.join("driver.json")).context("driver.json missing; did the run finish?")?,
    )?;
    let fleet: FleetReport = serde_json::from_slice(
        &fs::read(run.join("fleet").join("fleet.json")).context("fleet/fleet.json missing")?,
    )?;
    let samples = read_samples(&run.join("fleet").join("samples.bin"))?;
    let measure_from = driver
        .paced_start_unix_ns
        .map_or(0, |start| start + warmup_secs * 1_000_000_000);
    // Earliest driver callback per (slot, status): the reference for statuses and confirmed streams.
    let mut status_times: HashMap<(u64, String), u64> = HashMap::new();
    if let Ok(data) = fs::read(run.join("driver-statuses.json")) {
        let records: Vec<StatusRecord> = serde_json::from_slice(&data)?;
        for record in records {
            let time = status_times.entry((record.slot, record.status)).or_insert(u64::MAX);
            *time = (*time).min(record.unix_ns);
        }
    }

    let mut groups: BTreeMap<(String, String, String), Vec<(u64, u64)>> = BTreeMap::new();
    let mut unreferenced = 0usize;
    for sample in samples.iter().filter(|sample| sample.recv_ns >= measure_from) {
        let Some(subscriber) = fleet.subscribers.get(usize::from(sample.subscriber)) else {
            continue;
        };
        let kind = Kind::from_u8(sample.kind);
        if matches!(kind, Kind::Ping | Kind::Pong) {
            continue;
        }
        let status = STATUS_NAMES.get(usize::from(sample.status)).copied();
        let label = match (kind, status) {
            (Kind::Slot, Some(status)) => format!("slot:{status}"),
            _ => kind.name().to_owned(),
        };
        let reference = match (subscriber.commitment.as_str(), kind, status) {
            ("confirmed", ..) => status_times.get(&(sample.slot, "confirmed".to_owned())).copied(),
            ("finalized", ..) => status_times.get(&(sample.slot, "rooted".to_owned())).copied(),
            (_, Kind::Slot, Some(status)) => status_times.get(&(sample.slot, status.to_owned())).copied(),
            _ => Some(sample.created_ns),
        };
        let Some(reference) = reference else {
            unreferenced += 1;
            continue;
        };
        groups
            .entry((subscriber.class.clone(), subscriber.compression.clone(), label))
            .or_default()
            .push((sample.slot, sample.recv_ns.saturating_sub(reference)));
    }
    if unreferenced > 0 {
        log::warn!("{unreferenced} samples had no driver reference time and were skipped");
    }

    let rows = groups
        .into_iter()
        .map(|((class, compression, kind), values)| row(class, compression, kind, &values))
        .collect();
    Ok(Report {
        run: run.display().to_string(),
        warmup_secs,
        disconnects: fleet.disconnects,
        rows,
    })
}

fn row(class: String, compression: String, kind: String, values: &[(u64, u64)]) -> Row {
    let mut latencies: Vec<u64> = values.iter().map(|(_, latency)| *latency).collect();
    latencies.sort_unstable();
    let at = |q: f64| latencies[((latencies.len() - 1) as f64 * q).round() as usize] as f64 / 1e3;

    let mut by_slot: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for (slot, latency) in values {
        by_slot.entry(*slot).or_default().push(*latency);
    }
    let mut slot_p99: Vec<u64> = by_slot
        .values_mut()
        .filter(|slot| slot.len() >= 5)
        .map(|slot| {
            slot.sort_unstable();
            slot[((slot.len() - 1) as f64 * 0.99).round() as usize]
        })
        .collect();
    slot_p99.sort_unstable();
    Row {
        class,
        compression,
        kind,
        count: latencies.len(),
        p50_us: at(0.5),
        p90_us: at(0.9),
        p99_us: at(0.99),
        p999_us: at(0.999),
        max_us: at(1.0),
        slot_p99_median_us: (!slot_p99.is_empty()).then(|| slot_p99[slot_p99.len() / 2] as f64 / 1e3),
        stalls_over_5ms: latencies.iter().filter(|latency| **latency > 5_000_000).count(),
        stalls_over_20ms: latencies.iter().filter(|latency| **latency > 20_000_000).count(),
    }
}

pub fn read_samples(path: &Path) -> anyhow::Result<Vec<Sample>> {
    let mut data = Vec::new();
    fs::File::open(path)
        .with_context(|| format!("failed to open {}", path.display()))?
        .read_to_end(&mut data)?;
    anyhow::ensure!(data.len() % SAMPLE_LEN == 0, "truncated samples file");
    Ok(data
        .chunks_exact(SAMPLE_LEN)
        .map(|record| Sample::read(record.try_into().expect("record length")))
        .collect())
}
