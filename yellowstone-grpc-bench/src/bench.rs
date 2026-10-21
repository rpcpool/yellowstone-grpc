//! `ygb bench`: runs a scenario end to end and writes `summary.md` and `summary.json`.
//! Each variant is a directory with its own `ygb` and plugin `.so` from one build, so dlopen stays ABI-safe.
//! A rerun into the same `--out` skips finished runs, so an interrupted bench resumes.

use {
    crate::{
        compare,
        cpu::CpuReport,
        driver::LaneMode,
        fixture::Manifest,
        fleet::FleetReport,
        layout::{self, Layout},
        profiles, report,
        scenario::{CasePoint, Scenario, Window},
    },
    anyhow::Context,
    log::{info, warn},
    serde::{Deserialize, Serialize},
    std::{
        collections::{btree_map::Entry, BTreeMap},
        fs,
        path::{Path, PathBuf},
        process::Command,
    },
};

const PLUGIN_SO: &str = "libyellowstone_grpc_geyser.so";
/// Fleet runtimes beyond this many threads only add scheduling noise.
const MAX_LOAD_THREADS: usize = 16;

#[derive(Debug, Clone, clap::Args)]
pub struct BenchArgs {
    /// Scenario file (see scenarios/)
    #[clap(long)]
    pub scenario: PathBuf,
    /// Fixture directory
    #[clap(long)]
    pub fixture: PathBuf,
    /// Output directory: profiles, layout, one directory per run, summary
    #[clap(long)]
    pub out: PathBuf,
    /// NAME=DIR, where DIR holds `ygb` and the plugin .so from one build; repeat for A/B.
    /// The first variant is the baseline. Defaults to this binary's own directory.
    #[clap(long = "variant")]
    pub variants: Vec<String>,
    /// CPU layout file (TOML); detected from /sys when unset
    #[clap(long)]
    pub layout: Option<PathBuf>,
    /// Physical cores for the plugin in an auto layout (production uses 6 with SMT)
    #[clap(long, default_value_t = 6)]
    pub plugin_cores: usize,
    /// Only run cases whose name contains this
    #[clap(long)]
    pub only: Option<String>,
    /// Override a scenario key for this machine, e.g. `--set plugin.fifo_priority=40`; repeatable
    #[clap(long = "set")]
    pub overrides: Vec<String>,
    /// A run is invalid when other tenants use more than this share of each plugin or probe CPU
    #[clap(long, default_value_t = 0.02)]
    pub max_foreign: f64,
    /// Loopback port of the plugin's gRPC server
    #[clap(long, default_value_t = 10000)]
    pub grpc_port: u16,
    /// Loopback port of the plugin's Prometheus endpoint
    #[clap(long, default_value_t = 9123)]
    pub prometheus_port: u16,
    /// Times to run again a run that failed only an environment check (other tenants, timer lag)
    #[clap(long, default_value_t = 1)]
    pub retries: usize,
    /// Print the plan and exit
    #[clap(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
struct Variant {
    name: String,
    ygb: PathBuf,
    plugin: PathBuf,
    abi: String,
}

/// What a results directory was made from; a rerun into the same directory must match it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct BenchKey {
    scenario: String,
    overrides: Vec<String>,
    fixture: String,
    layout: Layout,
    start_slot: u64,
}

#[derive(Debug, Clone, Serialize)]
struct RunResult {
    label: String,
    group: String,
    variant: String,
    point: CasePoint,
    dir: String,
    valid: bool,
    problems: Vec<String>,
    report: Option<report::Report>,
    plugin_cores: Option<f64>,
    delivery: Option<Delivery>,
}

/// What the fleet received, from fleet.json.
#[derive(Debug, Clone, Default, Serialize)]
struct Delivery {
    seconds: f64,
    load_streams: usize,
    load_mib_per_sec: f64,
    load_msgs_per_sec: f64,
    emulated_links: usize,
    churn_reconnects: u64,
    /// Subscribers on emulated links whose stream ended, e.g. dropped by the plugin as too slow.
    emulated_disconnects: usize,
    local_disconnects: usize,
    probe_disconnects: usize,
    /// Slowest scrape of the plugin's metrics, and its size.
    scrape_max_ms: f64,
    scrape_max_kib: f64,
    unary_calls_per_sec: f64,
    unary_errors: u64,
    /// Slowest method's p99, in milliseconds.
    unary_p99_ms: f64,
    /// Messages per second per `load-*` stream at p50, p90 and p99, against `mix.rate_targets`.
    stream_rates: [f64; 3],
}

#[derive(Debug, Serialize)]
struct Summary {
    scenario: String,
    fixture: String,
    layout: Layout,
    start_slot: u64,
    variants: Vec<Variant>,
    runs: Vec<RunResult>,
    comparisons: Vec<Comparison>,
}

#[derive(Debug, Serialize)]
struct Comparison {
    group: String,
    repeat: usize,
    variant: String,
    rows: Vec<compare::CompareRow>,
}

pub fn run(args: BenchArgs) -> anyhow::Result<()> {
    let scenario_text = fs::read_to_string(&args.scenario)
        .with_context(|| format!("failed to read {}", args.scenario.display()))?;
    let scenario = Scenario::load_with(&args.scenario, &args.overrides)?;
    let layout = match &args.layout {
        Some(path) => Layout::load(path)?,
        None => layout::auto(args.plugin_cores)?,
    };
    info!(
        "layout ({}): plugin {}, driver {}, decoder {}, probes {}, load {}",
        layout.note,
        Layout::cpu_list(&layout.plugin),
        Layout::cpu_list(&layout.driver),
        Layout::cpu_list(&layout.decoder),
        Layout::cpu_list(&layout.probes),
        Layout::cpu_list(&layout.load)
    );
    let variants = variants(&args.variants)?;
    let manifest = Manifest::load(&args.fixture)?;
    let paced_start = window_start(&scenario.window, &manifest)?;
    let start_slot = paced_start.saturating_sub(scenario.window.prefill_slots);
    info!(
        "window: prefill from slot {start_slot}, pacing slots {paced_start}..{}",
        paced_start + scenario.window.slots
    );

    let points: Vec<CasePoint> = scenario
        .points()
        .into_iter()
        .filter(|point| {
            args.only
                .as_ref()
                .is_none_or(|only| point.case.contains(only.as_str()))
        })
        .collect();
    anyhow::ensure!(!points.is_empty(), "no case matches --only");
    let plan: Vec<(CasePoint, Variant)> = points
        .iter()
        .flat_map(|point| {
            // Rotate the variant order on each repeat so drift over time does not favour one variant.
            let shift = point.repeat % variants.len();
            variants[shift..]
                .iter()
                .chain(&variants[..shift])
                .map(|variant| (point.clone(), variant.clone()))
                .collect::<Vec<_>>()
        })
        .collect();
    let minutes = plan.len() as f64
        * (scenario.window.slots as f64 * 0.23
            + scenario.window.prefill_slots as f64 * 0.03
            + 45.0)
        / 60.0;
    info!("plan: {} runs, about {minutes:.0} minutes", plan.len());
    for (point, variant) in &plan {
        info!(
            "  {} on {} (profile {})",
            point.label(),
            variant.name,
            point.profile
        );
    }
    if args.dry_run {
        return Ok(());
    }

    for port in [args.grpc_port, args.prometheus_port] {
        std::net::TcpListener::bind(("127.0.0.1", port)).with_context(|| {
            format!("port {port} is in use; pick free ones with --grpc-port and --prometheus-port")
        })?;
    }
    fs::create_dir_all(&args.out)?;
    let key = BenchKey {
        scenario: scenario_text,
        overrides: args.overrides.clone(),
        fixture: fs::canonicalize(&args.fixture)?.display().to_string(),
        layout: layout.clone(),
        start_slot,
    };
    let key_path = args.out.join("bench-key.json");
    match fs::read(&key_path) {
        Ok(data) => {
            let existing: BenchKey = serde_json::from_slice(&data)?;
            anyhow::ensure!(
                existing == key,
                "{} holds runs of another scenario, fixture or layout; pick a new --out",
                args.out.display()
            );
        }
        Err(_) => fs::write(&key_path, serde_json::to_vec_pretty(&key)?)?,
    }
    fs::copy(&args.scenario, args.out.join("scenario.toml"))?;
    fs::write(args.out.join("layout.toml"), toml::to_string(&layout)?)?;
    let profiles = write_profiles(&args, &points)?;

    let runs_dir = args.out.join("runs");
    fs::create_dir_all(&runs_dir)?;
    let mut results = Vec::new();
    for (index, (point, variant)) in plan.iter().enumerate() {
        let label = format!("{}-{}", point.label(), variant.name);
        let dir = runs_dir.join(&label);
        let mut attempt = 0;
        let result = loop {
            let mut failure = None;
            if dir.join("driver.json").exists() && dir.join("fleet").join("fleet.json").exists() {
                info!("[{}/{}] {label}: done earlier", index + 1, plan.len());
            } else {
                if dir.exists() {
                    // Left by an interrupted bench; its numbers are incomplete.
                    fs::remove_dir_all(&dir)?;
                }
                info!("[{}/{}] running {label}", index + 1, plan.len());
                let profile = &profiles[&point.profile];
                if let Err(error) = run_one(
                    &args, &scenario, &layout, variant, point, start_slot, profile, &dir,
                ) {
                    warn!("{label}: {error:#}");
                    failure = Some(format!("{error:#}"));
                }
            }
            let mut result = evaluate(&args, &scenario, &layout, &label, point, variant, &dir);
            if let Some(failure) = failure {
                result.problems.insert(0, failure);
                result.valid = false;
            }
            let environmental = !result.problems.is_empty()
                && result
                    .problems
                    .iter()
                    .all(|problem| is_environmental(problem));
            if result.valid || !environmental || attempt >= args.retries {
                break result;
            }
            attempt += 1;
            warn!(
                "{label}: {}; running it again ({attempt} of {})",
                result.problems.join("; "),
                args.retries
            );
            // Keep the failed attempt for diagnosis, without its large sample file.
            let kept = runs_dir.join(format!("{label}.attempt{attempt}"));
            if kept.exists() {
                fs::remove_dir_all(&kept)?;
            }
            fs::rename(&dir, &kept)?;
            let _ = fs::remove_file(kept.join("fleet").join(crate::fleet::SAMPLES_FILE));
        };
        if result.valid {
            info!("{label}: {}", headline(&result));
        } else {
            warn!("{label}: invalid: {}", result.problems.join("; "));
        }
        results.push(result);
    }

    let comparisons = compare_variants(&scenario, &variants, &results);
    let summary = Summary {
        scenario: scenario.name.clone(),
        fixture: args.fixture.display().to_string(),
        layout,
        start_slot,
        variants,
        runs: results,
        comparisons,
    };
    fs::write(
        args.out.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    let markdown = summary_markdown(&summary, &scenario);
    fs::write(args.out.join("summary.md"), &markdown)?;
    println!("{markdown}");
    Ok(())
}

fn variants(specs: &[String]) -> anyhow::Result<Vec<Variant>> {
    let own_dir = std::env::current_exe()?
        .parent()
        .context("executable has no directory")?
        .to_path_buf();
    let specs = if specs.is_empty() {
        vec![format!("base={}", own_dir.display())]
    } else {
        specs.to_vec()
    };
    let mut seen = std::collections::HashSet::new();
    specs
        .iter()
        .map(|spec| {
            let (name, dir) = spec
                .split_once('=')
                .with_context(|| format!("variant must be NAME=DIR: {spec}"))?;
            anyhow::ensure!(
                !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.'),
                "variant name {name:?} must use only letters, digits, '_' and '.'"
            );
            anyhow::ensure!(
                seen.insert(name.to_owned()),
                "variant {name} is listed twice"
            );
            let dir = fs::canonicalize(dir).with_context(|| format!("variant directory {dir}"))?;
            let variant = Variant {
                name: name.to_owned(),
                ygb: dir.join("ygb"),
                plugin: dir.join(PLUGIN_SO),
                abi: fs::read_to_string(dir.join("plugin-abi.txt")).unwrap_or_default(),
            };
            anyhow::ensure!(variant.ygb.exists(), "{} not found", variant.ygb.display());
            anyhow::ensure!(
                variant.plugin.exists(),
                "{} not found",
                variant.plugin.display()
            );
            Ok(variant)
        })
        .collect()
}

/// One profile per distinct case mix, generated once and reused by every run of it.
fn write_profiles(
    args: &BenchArgs,
    points: &[CasePoint],
) -> anyhow::Result<BTreeMap<String, PathBuf>> {
    let mut paths = BTreeMap::new();
    let mut activity: BTreeMap<u64, profiles::Activity> = BTreeMap::new();
    for point in points {
        if paths.contains_key(&point.profile) {
            continue;
        }
        let path = args.out.join(format!("profile-{}.json", point.profile));
        if !path.exists() {
            info!("generating subscriber profile {}", point.profile);
            let sampled = match activity.entry(point.mix.sample_slots) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(profiles::sample(&args.fixture, point.mix.sample_slots)?)
                }
            };
            let profile =
                profiles::generate_from(sampled, &args.fixture, &point.mix, &point.probes)?;
            info!("profile {}: {}", point.profile, profile.description);
            fs::write(&path, serde_json::to_vec_pretty(&profile)?)?;
        }
        paths.insert(point.profile.clone(), path);
    }
    Ok(paths)
}

/// Paced start slot of the window. `heaviest` uses chunk sizes, so it needs no decoding.
fn window_start(window: &Window, manifest: &Manifest) -> anyhow::Result<u64> {
    let chunks: Vec<(u64, u64)> = manifest
        .chunks
        .iter()
        .filter_map(|chunk| chunk.max_slot.map(|slot| (slot, chunk.bytes)))
        .collect();
    anyhow::ensure!(chunks.len() > 2, "fixture has too few chunks");
    let first = chunks[0].0 + window.prefill_slots + 64;
    let last = chunks[chunks.len() - 1]
        .0
        .saturating_sub(window.slots + 200);
    anyhow::ensure!(
        last > first,
        "fixture covers too few slots for prefill {} + window {}",
        window.prefill_slots,
        window.slots
    );
    Ok(match window.start.as_str() {
        "first" => first,
        "heaviest" => {
            let mut best = (0u64, first);
            for (start, _) in chunks
                .iter()
                .filter(|(slot, _)| (first..=last).contains(slot))
            {
                let bytes: u64 = chunks
                    .iter()
                    .filter(|(slot, _)| *slot >= *start && *slot < start + window.slots)
                    .map(|(_, bytes)| *bytes)
                    .sum();
                if bytes > best.0 {
                    best = (bytes, *start);
                }
            }
            best.1
        }
        slot => {
            let slot: u64 = slot.parse().with_context(|| {
                format!("window.start must be heaviest, first or a slot: {slot}")
            })?;
            anyhow::ensure!(
                (first..=last).contains(&slot),
                "window.start {slot} is outside {first}..={last}"
            );
            slot
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn run_one(
    args: &BenchArgs,
    scenario: &Scenario,
    layout: &Layout,
    variant: &Variant,
    point: &CasePoint,
    start_slot: u64,
    profile: &Path,
    dir: &Path,
) -> anyhow::Result<()> {
    let mut command = Command::new(&variant.ygb);
    command
        .arg("run")
        .arg("--fixture")
        .arg(&args.fixture)
        .arg("--plugin")
        .arg(&variant.plugin)
        .arg("--out")
        .arg(dir)
        .args(["--grpc-port", &args.grpc_port.to_string()])
        .args(["--prometheus-port", &args.prometheus_port.to_string()])
        .args(["--start-slot", &start_slot.to_string()])
        .args([
            "--prefill-slots",
            &scenario.window.prefill_slots.to_string(),
        ])
        .args(["--slots", &scenario.window.slots.to_string()])
        .args(["--attach-secs", "10"])
        .args(["--plugin-cpus", &Layout::cpu_list(&layout.plugin)])
        .args(["--driver-cpus", &Layout::cpu_list(&layout.driver)])
        .args(["--decoder-cpus", &Layout::cpu_list(&layout.decoder)])
        .arg("--fleet-profile")
        .arg(profile)
        .args(["--probe-cpus", &Layout::cpu_list(&layout.probes)])
        .args(["--load-cpus", &Layout::cpu_list(&layout.load)])
        .args([
            "--probe-threads",
            &layout.probes.len().clamp(1, 4).to_string(),
        ])
        .args([
            "--load-threads",
            &layout.load.len().clamp(1, MAX_LOAD_THREADS).to_string(),
        ])
        .args([
            "--streams-per-connection",
            &scenario.fleet.streams_per_connection.to_string(),
        ])
        .args([
            "--stream-window-mib",
            &scenario.fleet.stream_window_mib.to_string(),
        ])
        .args([
            "--connection-window-mib",
            &scenario.fleet.connection_window_mib.to_string(),
        ])
        .args(["--scrape-secs", &scenario.fleet.scrape_secs.to_string()])
        // Same jitter and churn timing for every variant of a repeat, different across repeats.
        .args([
            "--fleet-seed",
            &(point.mix.seed + point.repeat as u64).to_string(),
        ])
        .args(["--max-load", &point.streams.to_string()])
        .args(["--max-blocks", &point.blocks.to_string()])
        .args(["--max-deshred", &point.deshred.to_string()])
        .args(["--lanes", lane_mode_arg(scenario.driver.lanes)])
        .args(["--sched-lanes", &scenario.driver.sched_lanes.to_string()]);
    if scenario.driver.notifiers_on_plugin_cpus {
        command.args(["--notifier-cpus", &Layout::cpu_list(&layout.plugin)]);
    }
    if let Some(slots) = scenario.plugin.replay_stored_slots {
        command.args(["--replay-stored-slots", &slots.to_string()]);
    }
    if let Some(workers) = scenario.plugin.worker_threads {
        command.args(["--worker-threads", &workers.to_string()]);
    }
    if let Some(template) = &scenario.plugin.config_template {
        // Relative to the scenario file, so scenarios can ship their own config.
        let base = args.scenario.parent().unwrap_or(Path::new("."));
        command.arg("--config-template").arg(base.join(template));
    }
    if let Some(priority) = scenario.plugin.fifo_priority {
        command.args(["--fifo-priority", &priority.to_string()]);
    }
    for (key, value) in &point.env {
        command.args(["--plugin-env", &format!("{key}={value}")]);
    }
    fs::create_dir_all(dir)?;
    let log_path = dir.join("run.log");
    let log = fs::File::create(&log_path)?;
    let status = command
        .stdout(log.try_clone()?)
        .stderr(log)
        .status()
        .with_context(|| format!("failed to start {}", variant.ygb.display()))?;
    anyhow::ensure!(
        status.success(),
        "run failed ({status}); see {}",
        log_path.display()
    );
    Ok(())
}

/// Problems the host causes rather than the plugin or the setup, so a second run may pass.
fn is_environmental(problem: &str) -> bool {
    problem.starts_with("foreign load on")
        || problem.contains(" timer lag p99 ")
        || problem.starts_with("probe thread peaked")
}

const fn lane_mode_arg(mode: LaneMode) -> &'static str {
    match mode {
        LaneMode::Single => "single",
        LaneMode::Agave => "agave",
    }
}

/// Reports a run and checks the guards that make its numbers trustworthy.
fn evaluate(
    args: &BenchArgs,
    scenario: &Scenario,
    layout: &Layout,
    label: &str,
    point: &CasePoint,
    variant: &Variant,
    dir: &Path,
) -> RunResult {
    let mut problems = Vec::new();
    let report = match report::build(dir, scenario.window.warmup_secs) {
        Ok(report) => {
            let _ = fs::write(
                dir.join("report.json"),
                serde_json::to_vec_pretty(&report).unwrap_or_default(),
            );
            Some(report)
        }
        Err(error) => {
            problems.push(format!("report failed: {error:#}"));
            None
        }
    };
    let read_json = |name: &str| -> Option<serde_json::Value> {
        fs::read(dir.join(name))
            .ok()
            .and_then(|data| serde_json::from_slice(&data).ok())
    };
    match read_json("driver.json").and_then(|driver| {
        driver
            .get("lanes")
            .and_then(|lanes| lanes.as_object().cloned())
    }) {
        Some(lanes) => {
            let errors: u64 = lanes
                .values()
                .map(|lane| lane["errors"].as_u64().unwrap_or(0))
                .sum();
            if errors > 0 {
                problems.push(format!("{errors} plugin callback errors"));
            }
            // Past this the replay no longer keeps the recorded pace.
            for (name, lane) in &lanes {
                let timer_p99 = lane["timer_lag_ns"]["p99"].as_u64().unwrap_or(0);
                if timer_p99 > 200_000 {
                    problems.push(format!(
                        "{name} timer lag p99 {} us (over 200 us)",
                        timer_p99 / 1000
                    ));
                }
            }
        }
        None => problems.push("driver.json missing or without lanes".to_owned()),
    }
    let cpu: Option<CpuReport> = fs::read(dir.join("cpu.json"))
        .ok()
        .and_then(|data| serde_json::from_slice(&data).ok());
    if let Some(cpu) = &cpu {
        let limit = args.max_foreign * layout.plugin.len() as f64;
        if cpu.plugin_cpus_foreign.0 > limit {
            problems.push(format!(
                "foreign load on plugin CPUs {:.2} cores (over {limit:.2})",
                cpu.plugin_cpus_foreign.0
            ));
        }
    }
    let fleet = fs::read(dir.join("fleet").join("fleet.json"))
        .ok()
        .and_then(|data| serde_json::from_slice::<FleetReport>(&data).ok());
    let delivery = fleet.as_ref().map(delivery);
    match (&fleet, &delivery) {
        (Some(fleet), Some(delivery)) => {
            if delivery.probe_disconnects > 0 {
                problems.push(format!(
                    "{} probes disconnected",
                    delivery.probe_disconnects
                ));
            }
            // A local subscriber that falls behind means the fleet, not the network, was too slow.
            if delivery.local_disconnects > 0 {
                problems.push(format!(
                    "{} local load subscribers disconnected",
                    delivery.local_disconnects
                ));
            }
            if let Some(peak) = fleet
                .cpu
                .as_ref()
                .and_then(|cpu| cpu.thread_peak.get("fleet_probes"))
            {
                if *peak > 0.7 {
                    problems.push(format!(
                        "probe thread peaked at {:.0}% of a core",
                        peak * 100.0
                    ));
                }
            }
            if let Some(foreign) = fleet.cpu.as_ref().map(|cpu| cpu.plugin_cpus_foreign.0) {
                let limit = args.max_foreign * layout.probes.len() as f64;
                if foreign > limit {
                    problems.push(format!(
                        "foreign load on probe CPUs {foreign:.2} cores (over {limit:.2})"
                    ));
                }
            }
        }
        _ => problems.push("fleet.json missing".to_owned()),
    }
    RunResult {
        label: label.to_owned(),
        group: point.group(),
        variant: variant.name.clone(),
        point: point.clone(),
        dir: dir.display().to_string(),
        valid: problems.is_empty(),
        problems,
        report,
        plugin_cores: cpu.and_then(|cpu| cpu.groups.get("plugin_workers").map(|(mean, _)| *mean)),
        delivery,
    }
}

fn delivery(fleet: &FleetReport) -> Delivery {
    let seconds = fleet.stopped_unix_ns.saturating_sub(fleet.started_unix_ns) as f64 / 1e9;
    let mut out = Delivery {
        seconds,
        ..Default::default()
    };
    let (mut bytes, mut messages) = (0u64, 0u64);
    let mut rates = Vec::new();
    for sub in &fleet.subscribers {
        let emulated = sub.link.as_ref().is_some_and(|link| !link.is_local());
        out.emulated_links += usize::from(emulated);
        out.churn_reconnects += u64::from(sub.reconnects);
        let ended = sub.errors > 0;
        if sub.role == "probe" {
            out.probe_disconnects += usize::from(ended);
            continue;
        }
        out.load_streams += 1;
        bytes += sub.bytes;
        messages += sub.messages;
        if sub.class.starts_with("load-") && seconds > 0.0 {
            rates.push(sub.messages as f64 / seconds);
        }
        if emulated {
            out.emulated_disconnects += usize::from(ended);
        } else {
            out.local_disconnects += usize::from(ended);
        }
    }
    if seconds > 0.0 {
        out.load_mib_per_sec = bytes as f64 / seconds / f64::from(1u32 << 20);
        out.load_msgs_per_sec = messages as f64 / seconds;
    }
    for scrape in fleet.scrapes.iter().filter(|scrape| scrape.error.is_none()) {
        out.scrape_max_ms = out.scrape_max_ms.max(scrape.millis);
        out.scrape_max_kib = out.scrape_max_kib.max(scrape.bytes as f64 / 1024.0);
    }
    rates.sort_by(f64::total_cmp);
    if !rates.is_empty() {
        let at = |q: f64| rates[((rates.len() - 1) as f64 * q).round() as usize];
        out.stream_rates = [at(0.5), at(0.9), at(0.99)];
    }
    for report in fleet.unary.values() {
        out.unary_calls_per_sec += report.calls_per_sec;
        out.unary_errors += report.errors;
        let p99 = report.latency_ns.as_ref().map_or(0, |latency| latency.p99);
        out.unary_p99_ms = out.unary_p99_ms.max(p99 as f64 / 1e6);
    }
    out
}

/// One line per run for the log: the streamlab account, transaction and entry p99, and plugin CPU.
fn headline(result: &RunResult) -> String {
    let cell = |kind: &str| {
        result
            .report
            .as_ref()
            .and_then(|report| {
                report.rows.iter().find(|row| {
                    row.class == "streamlab" && row.compression == "none" && row.kind == kind
                })
            })
            .map_or("-".to_owned(), |row| format!("{:.0}", row.p99_us))
    };
    format!(
        "streamlab p99 account {} tx {} entry {} us; plugin {:.2} cores",
        cell("account"),
        cell("transaction"),
        cell("entry"),
        result.plugin_cores.unwrap_or(0.0)
    )
}

/// Paired per-slot comparison of every variant against the first, per group and repeat.
fn compare_variants(
    scenario: &Scenario,
    variants: &[Variant],
    results: &[RunResult],
) -> Vec<Comparison> {
    let mut out = Vec::new();
    let Some(base) = variants.first() else {
        return out;
    };
    let mut by_key: BTreeMap<(String, usize, String), &RunResult> = BTreeMap::new();
    for result in results {
        by_key.insert(
            (
                result.group.clone(),
                result.point.repeat,
                result.variant.clone(),
            ),
            result,
        );
    }
    for ((group, repeat, variant), result) in &by_key {
        if variant == &base.name {
            continue;
        }
        let Some(base_run) = by_key.get(&(group.clone(), *repeat, base.name.clone())) else {
            continue;
        };
        if !base_run.valid || !result.valid {
            warn!("compare {group} r{repeat} {variant}: skipped, a run is invalid");
            continue;
        }
        match compare::compare_dirs(
            Path::new(&base_run.dir),
            Path::new(&result.dir),
            scenario.window.warmup_secs,
            5,
        ) {
            Ok(rows) => out.push(Comparison {
                group: group.clone(),
                repeat: *repeat,
                variant: variant.clone(),
                rows,
            }),
            Err(error) => warn!("compare {group} r{repeat} {variant}: {error:#}"),
        }
    }
    out
}

fn summary_markdown(summary: &Summary, scenario: &Scenario) -> String {
    let mut md = format!(
        "# {}\n\n{}\n\nFixture `{}`, prefill from slot {}, {} paced slots, first {} s excluded.\nLayout: plugin `{}`, driver `{}`, probes `{}`, load `{}`.\n\n",
        summary.scenario,
        scenario.description,
        summary.fixture,
        summary.start_slot,
        scenario.window.slots,
        scenario.window.warmup_secs,
        Layout::cpu_list(&summary.layout.plugin),
        Layout::cpu_list(&summary.layout.driver),
        Layout::cpu_list(&summary.layout.probes),
        Layout::cpu_list(&summary.layout.load),
    );
    md.push_str("Streamlab-shaped probes: latency from the plugin's `created_at` to receipt in µs, as p50 / p99 / p99.9 / median per-slot p99.\n\n");
    md.push_str("| run | variant | account | transaction | entry | account zstd | confirmed account | plugin cores | valid |\n");
    md.push_str("|---|---|---|---|---|---|---|---|---|\n");
    for run in &summary.runs {
        let cell = |class: &str, compression: &str, kind: &str| {
            run.report
                .as_ref()
                .and_then(|report| {
                    report.rows.iter().find(|row| {
                        row.class == class && row.compression == compression && row.kind == kind
                    })
                })
                .map_or("-".to_owned(), |row| {
                    format!(
                        "{:.0} / {:.0} / {:.0} / {}",
                        row.p50_us,
                        row.p99_us,
                        row.p999_us,
                        row.slot_p99_median_us
                            .map_or("-".to_owned(), |value| format!("{value:.0}"))
                    )
                })
        };
        md.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            run.point.label(),
            run.variant,
            cell("streamlab", "none", "account"),
            cell("streamlab", "none", "transaction"),
            cell("streamlab", "none", "entry"),
            cell("streamlab", "zstd", "account"),
            cell("streamlab-confirmed", "none", "account"),
            run.plugin_cores
                .map_or("-".to_owned(), |cores| format!("{cores:.2}")),
            if run.valid {
                "yes".to_owned()
            } else {
                format!("no: {}", run.problems.join("; "))
            },
        ));
    }
    let targets = scenario.mix.rate_targets;
    md.push_str(&format!(
        "\nLoad the fleet received. Per-stream msg/s targets from production: p50 {:.0}, p90 {:.0}, p99 {:.0}.\n\n",
        targets[0], targets[1], targets[2]
    ));
    md.push_str("| run | variant | streams | msg/s | MiB/s | per-stream msg/s p50 / p90 / p99 | emulated links | churn reconnects | ended (emulated/local) | slowest scrape | unary/s (slowest p99) |\n");
    md.push_str("|---|---|---|---|---|---|---|---|---|---|---|\n");
    for run in &summary.runs {
        let delivery = run.delivery.clone().unwrap_or_default();
        md.push_str(&format!(
            "| {} | {} | {} | {:.0} | {:.1} | {:.1} / {:.1} / {:.0} | {} | {} | {}/{} | {:.1} ms, {:.0} KiB | {:.0} ({:.1} ms) |\n",
            run.point.label(),
            run.variant,
            delivery.load_streams,
            delivery.load_msgs_per_sec,
            delivery.load_mib_per_sec,
            delivery.stream_rates[0],
            delivery.stream_rates[1],
            delivery.stream_rates[2],
            delivery.emulated_links,
            delivery.churn_reconnects,
            delivery.emulated_disconnects,
            delivery.local_disconnects,
            delivery.scrape_max_ms,
            delivery.scrape_max_kib,
            delivery.unary_calls_per_sec,
            delivery.unary_p99_ms,
        ));
    }
    if !summary.comparisons.is_empty() {
        md.push_str("\nPaired per-slot p99 against the first variant: median change, and a 95% block-bootstrap CI of the median difference.\n\n");
        md.push_str("| group | repeat | variant | probe group | slots | change | CI (µs) |\n|---|---|---|---|---|---|---|\n");
        for comparison in &summary.comparisons {
            for row in comparison
                .rows
                .iter()
                .filter(|row| row.group.starts_with("streamlab"))
            {
                md.push_str(&format!(
                    "| {} | r{} | {} | {} | {} | {:+.1}% | {:.0}..{:.0} |\n",
                    comparison.group,
                    comparison.repeat,
                    comparison.variant,
                    row.group,
                    row.slots,
                    row.change_pct,
                    row.ci_low_us,
                    row.ci_high_us
                ));
            }
        }
    }
    md
}
