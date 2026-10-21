//! `ygb report`: latency of probe subscribers in one run, by probe class, compression and kind.

use {
    crate::fleet::{scan::Kind, FleetReport, Sample, SAMPLES_FILE, SAMPLE_LEN},
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
    /// Also print a timeline of processed-probe stalls longer than this many milliseconds
    #[clap(long)]
    pub stalls: Option<f64>,
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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
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
        "class",
        "comp",
        "kind",
        "n",
        "p50 us",
        "p90 us",
        "p99 us",
        "p99.9 us",
        "max us",
        "slot p99",
        ">5ms",
        ">20ms"
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
            row.slot_p99_median_us
                .map_or("-".to_owned(), |v| format!("{v:.0}")),
            row.stalls_over_5ms,
            row.stalls_over_20ms,
        );
    }
    fs::write(
        args.run.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    print_trace(&args.run)?;
    if let Some(threshold_ms) = args.stalls {
        print_stalls(&args.run, args.warmup_secs, threshold_ms)?;
    }
    Ok(())
}

/// Prints 100 ms windows in which processed probes waited longer than `threshold_ms`, with what
/// happened just before: fleet reconnects, stream ends and scrapes (1 s), Confirmed callbacks (150 ms).
fn print_stalls(run: &Path, warmup_secs: u64, threshold_ms: f64) -> anyhow::Result<()> {
    const WINDOW_NS: u64 = 100_000_000;
    const FLEET_LOOKBACK_NS: u64 = 1_000_000_000;
    // Confirmed seals full blocks for block subscribers; their encode follows within milliseconds.
    const CONFIRMED_LOOKBACK_NS: u64 = 150_000_000;
    let (fleet, groups) = grouped(run, warmup_secs)?;
    let threshold = (threshold_ms * 1e6) as u64;
    let mut windows: BTreeMap<u64, (usize, u64, std::collections::BTreeSet<String>)> =
        BTreeMap::new();
    for ((class, compression, kind), points) in &groups {
        if class.contains("confirmed") {
            continue;
        }
        for point in points.iter().filter(|point| point.latency > threshold) {
            let window = windows.entry(point.recv_ns / WINDOW_NS).or_default();
            window.0 += 1;
            window.1 = window.1.max(point.latency);
            window.2.insert(format!("{class}/{compression}/{kind}"));
        }
    }
    let statuses: Vec<StatusRecord> = fs::read(run.join("driver-statuses.json"))
        .ok()
        .and_then(|data| serde_json::from_slice(&data).ok())
        .unwrap_or_default();
    let mut events: Vec<(u64, u64, String)> = fleet
        .subscribers
        .iter()
        .flat_map(|sub| {
            sub.events.iter().map(|event| {
                (
                    event.unix_ns,
                    FLEET_LOOKBACK_NS,
                    format!("{} {}", sub.name, event.what),
                )
            })
        })
        .chain(fleet.scrapes.iter().map(|scrape| {
            (
                scrape.unix_ns,
                FLEET_LOOKBACK_NS,
                format!(
                    "metrics scrape {:.1} ms, {} KiB",
                    scrape.millis,
                    scrape.bytes / 1024
                ),
            )
        }))
        .chain(
            statuses
                .iter()
                .filter(|record| record.status == "confirmed")
                .map(|record| {
                    (
                        record.unix_ns,
                        CONFIRMED_LOOKBACK_NS,
                        format!("confirmed {}", record.slot),
                    )
                }),
        )
        .collect();
    events.sort();
    println!(
        "\nprocessed-probe stalls over {threshold_ms} ms: {} windows of 100 ms, {} samples",
        windows.len(),
        windows.values().map(|window| window.0).sum::<usize>()
    );
    for (index, (count, worst, groups)) in &windows {
        let start = index * WINDOW_NS;
        let nearby: Vec<&str> = events
            .iter()
            .filter(|(time, lookback, _)| *time + lookback >= start && *time < start + WINDOW_NS)
            .map(|(_, _, what)| what.as_str())
            .collect();
        println!(
            "{} {count:>6} samples, worst {:>6.1} ms  {}{}{}",
            clock(start),
            *worst as f64 / 1e6,
            groups.iter().cloned().collect::<Vec<_>>().join(" "),
            if nearby.is_empty() {
                ""
            } else {
                "  | before: "
            },
            nearby.join("; ")
        );
    }
    Ok(())
}

/// UTC time of day with tenths of a second.
fn clock(unix_ns: u64) -> String {
    let secs = unix_ns / 1_000_000_000;
    format!(
        "{:02}:{:02}:{:02}.{}",
        secs / 3600 % 24,
        secs / 60 % 60,
        secs % 60,
        unix_ns / 100_000_000 % 10
    )
}

/// Prints the plugin's latency-trace stages, when the run used a traced build.
fn print_trace(run: &Path) -> anyhow::Result<()> {
    let Ok(entries) = fs::read_dir(run.join("trace")) else {
        return Ok(());
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        let trace: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        println!(
            "\nplugin trace {} (whole run, prefill included):",
            path.display()
        );
        println!(
            "{:<26} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "stage", "count", "p50", "p90", "p99", "p99.9", "max"
        );
        if let Some(stages) = trace["stages"].as_object() {
            for (name, stage) in stages {
                let value = |key: &str| stage[key].as_u64().unwrap_or_default();
                let unit = |v: u64| {
                    if name.ends_with("_ns") {
                        format!("{:.1}us", v as f64 / 1e3)
                    } else {
                        v.to_string()
                    }
                };
                println!(
                    "{:<26} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
                    name,
                    value("count"),
                    unit(value("p50")),
                    unit(value("p90")),
                    unit(value("p99")),
                    unit(value("p999")),
                    unit(value("max")),
                );
            }
        }
    }
    Ok(())
}

/// One probe sample after the reference time is applied.
#[derive(Debug, Clone, Copy)]
struct Point {
    slot: u64,
    recv_ns: u64,
    latency: u64,
}

type Groups = BTreeMap<(String, String, String), Vec<Point>>;

pub fn build(run: &Path, warmup_secs: u64) -> anyhow::Result<Report> {
    let (fleet, groups) = grouped(run, warmup_secs)?;
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

/// Per probe group (`class/compression/kind`), the p99 of each slot with at least `min_samples`.
pub fn slot_p99s(
    run: &Path,
    warmup_secs: u64,
    min_samples: usize,
) -> anyhow::Result<BTreeMap<String, BTreeMap<u64, u64>>> {
    let (_, groups) = grouped(run, warmup_secs)?;
    Ok(groups
        .into_iter()
        .map(|((class, compression, kind), values)| {
            let mut by_slot: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
            for point in values {
                by_slot.entry(point.slot).or_default().push(point.latency);
            }
            let p99s = by_slot
                .into_iter()
                .filter(|(_, latencies)| latencies.len() >= min_samples)
                .map(|(slot, mut latencies)| {
                    latencies.sort_unstable();
                    (
                        slot,
                        latencies[((latencies.len() - 1) as f64 * 0.99).round() as usize],
                    )
                })
                .collect();
            (format!("{class}/{compression}/{kind}"), p99s)
        })
        .collect())
}

fn grouped(run: &Path, warmup_secs: u64) -> anyhow::Result<(FleetReport, Groups)> {
    let driver: DriverTimes = serde_json::from_slice(
        &fs::read(run.join("driver.json")).context("driver.json missing; did the run finish?")?,
    )?;
    let fleet: FleetReport = serde_json::from_slice(
        &fs::read(run.join("fleet").join("fleet.json")).context("fleet/fleet.json missing")?,
    )?;
    let samples = read_samples(&run.join("fleet"))?;
    let measure_from = driver
        .paced_start_unix_ns
        .map_or(0, |start| start + warmup_secs * 1_000_000_000);
    // Earliest driver callback per (slot, status): the reference for statuses and confirmed streams.
    let mut status_times: HashMap<(u64, String), u64> = HashMap::new();
    if let Ok(data) = fs::read(run.join("driver-statuses.json")) {
        let records: Vec<StatusRecord> = serde_json::from_slice(&data)?;
        for record in records {
            let time = status_times
                .entry((record.slot, record.status))
                .or_insert(u64::MAX);
            *time = (*time).min(record.unix_ns);
        }
    }

    let mut groups: Groups = BTreeMap::new();
    let mut unreferenced = 0usize;
    for sample in samples
        .iter()
        .filter(|sample| sample.recv_ns >= measure_from)
    {
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
            ("confirmed", ..) => status_times
                .get(&(sample.slot, "confirmed".to_owned()))
                .copied(),
            ("finalized", ..) => status_times
                .get(&(sample.slot, "rooted".to_owned()))
                .copied(),
            (_, Kind::Slot, Some(status)) => {
                status_times.get(&(sample.slot, status.to_owned())).copied()
            }
            _ => Some(sample.created_ns),
        };
        let Some(reference) = reference else {
            unreferenced += 1;
            continue;
        };
        groups
            .entry((
                subscriber.class.clone(),
                subscriber.compression.clone(),
                label,
            ))
            .or_default()
            .push(Point {
                slot: sample.slot,
                recv_ns: sample.recv_ns,
                latency: sample.recv_ns.saturating_sub(reference),
            });
    }
    if unreferenced > 0 {
        log::warn!("{unreferenced} samples had no driver reference time and were skipped");
    }
    Ok((fleet, groups))
}

fn row(class: String, compression: String, kind: String, values: &[Point]) -> Row {
    let mut latencies: Vec<u64> = values.iter().map(|point| point.latency).collect();
    latencies.sort_unstable();
    let at = |q: f64| latencies[((latencies.len() - 1) as f64 * q).round() as usize] as f64 / 1e3;

    let mut by_slot: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for point in values {
        by_slot.entry(point.slot).or_default().push(point.latency);
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
        slot_p99_median_us: (!slot_p99.is_empty())
            .then(|| slot_p99[slot_p99.len() / 2] as f64 / 1e3),
        stalls_over_5ms: latencies
            .iter()
            .filter(|latency| **latency > 5_000_000)
            .count(),
        stalls_over_20ms: latencies
            .iter()
            .filter(|latency| **latency > 20_000_000)
            .count(),
    }
}

/// Reads a fleet directory's probe samples; plain `samples.bin` comes from older runs.
pub fn read_samples(fleet_dir: &Path) -> anyhow::Result<Vec<Sample>> {
    let mut data = Vec::new();
    let compressed = fleet_dir.join(SAMPLES_FILE);
    if compressed.exists() {
        zstd::stream::Decoder::new(fs::File::open(&compressed)?)?.read_to_end(&mut data)?;
    } else {
        let path = fleet_dir.join("samples.bin");
        fs::File::open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?
            .read_to_end(&mut data)?;
    }
    anyhow::ensure!(data.len() % SAMPLE_LEN == 0, "truncated samples file");
    Ok(data
        .chunks_exact(SAMPLE_LEN)
        .map(|record| Sample::read(record.try_into().expect("record length")))
        .collect())
}
