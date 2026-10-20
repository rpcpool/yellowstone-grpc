//! `ygb run`: load the plugin in-process and replay a fixture into it, prefill first, then paced.

mod events;
mod lane;
mod plugin;
mod schedule;

use {
    crate::{cpu, fixture::Manifest, sys},
    anyhow::Context,
    log::info,
    serde::Serialize,
    std::{
        fs,
        path::{Path, PathBuf},
        process::{Child, Command},
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

const DEFAULT_CONFIG: &str = include_str!("../../configs/geyser-streaming.json");
const LANE_CAPACITY: usize = 65_536;

#[derive(Debug, Clone, clap::Args)]
pub struct RunArgs {
    /// Fixture directory
    #[clap(long)]
    pub fixture: PathBuf,
    /// Plugin shared library to load (`_create_plugin`)
    #[clap(long)]
    pub plugin: PathBuf,
    /// Run directory for the rendered plugin config and the results
    #[clap(long)]
    pub out: PathBuf,
    /// Plugin config template; defaults to the prod streaming template
    #[clap(long)]
    pub config_template: Option<PathBuf>,
    #[clap(long, default_value_t = 10000)]
    pub grpc_port: u16,
    #[clap(long, default_value_t = 9123)]
    pub prometheus_port: u16,
    /// First slot to replay; defaults to 20 slots after the fixture start
    #[clap(long)]
    pub start_slot: Option<u64>,
    /// Slots replayed unpaced before pacing starts, to fill the replay buffer
    #[clap(long, default_value_t = 1000)]
    pub prefill_slots: u64,
    /// Paced slots after the prefill; the rest of the fixture when unset
    #[clap(long)]
    pub slots: Option<u64>,
    /// Seconds between the end of prefill and the first paced event, for subscribers to attach
    #[clap(long, default_value_t = 10)]
    pub attach_secs: u64,
    /// Seconds of status tail replayed after the last window slot
    #[clap(long, default_value_t = 15)]
    pub tail_secs: u64,
    /// Replay speed; 1.0 keeps the recorded pace
    #[clap(long, default_value_t = 1.0)]
    pub speed: f64,
    /// Override tokio.worker_threads
    #[clap(long)]
    pub worker_threads: Option<usize>,
    /// Override grpc.replay_stored_slots
    #[clap(long)]
    pub replay_stored_slots: Option<u64>,
    /// tokio.affinity for the plugin runtime, e.g. `8-13,24-29`
    #[clap(long)]
    pub plugin_cpus: Option<String>,
    /// CPUs for the lane threads that call the plugin
    #[clap(long)]
    pub driver_cpus: Option<String>,
    /// CPUs for the fixture decoder thread
    #[clap(long)]
    pub decoder_cpus: Option<String>,
    /// Seconds to keep the plugin up after the replay ends
    #[clap(long, default_value_t = 5)]
    pub hold_secs: u64,
    /// Subscriber profile; `ygb fleet` starts when prefill completes and stops when the replay ends
    #[clap(long)]
    pub fleet_profile: Option<PathBuf>,
    #[clap(long)]
    pub probe_cpus: Option<String>,
    #[clap(long)]
    pub load_cpus: Option<String>,
    #[clap(long, default_value_t = 2)]
    pub probe_threads: usize,
    #[clap(long, default_value_t = 6)]
    pub load_threads: usize,
    #[clap(long, default_value_t = 1)]
    pub streams_per_connection: usize,
    /// Passed to the fleet: first N `load-*` subscribers
    #[clap(long)]
    pub max_load: Option<usize>,
    /// Passed to the fleet: first N `blocks` subscribers
    #[clap(long)]
    pub max_blocks: Option<usize>,
    /// Passed to the fleet: first N `deshred` subscribers
    #[clap(long)]
    pub max_deshred: Option<usize>,
    /// `KEY=VALUE` set in the environment before the plugin loads, for experiment switches
    #[clap(long = "plugin-env")]
    pub plugin_env: Vec<String>,
}

#[derive(Serialize)]
struct RunReport {
    fixture: String,
    plugin: String,
    started_unix_ns: u64,
    paced_start_unix_ns: Option<u64>,
    finished_unix_ns: u64,
    schedule: schedule::ScheduleStats,
    main_lane: lane::LaneReport,
    deshred_lane: lane::LaneReport,
}

pub fn run(args: RunArgs) -> anyhow::Result<()> {
    let manifest = Manifest::load(&args.fixture)?;
    fs::create_dir_all(&args.out)
        .with_context(|| format!("failed to create {}", args.out.display()))?;
    let config_path = args.out.join("geyser.json");
    write_plugin_config(&args, &config_path)?;
    let started_unix_ns = sys::unix_ns();
    // A plugin built with `latency-trace` writes its checkpoints here; others ignore it.
    std::env::set_var("YELLOWSTONE_LATENCY_TRACE_DIR", args.out.join("trace"));
    for pair in &args.plugin_env {
        let (key, value) = pair
            .split_once('=')
            .with_context(|| format!("--plugin-env must be KEY=VALUE: {pair}"))?;
        std::env::set_var(key, value);
    }
    let plugin = plugin::load(&args.plugin, &config_path)?;

    let (main_tx, main_rx) = crossbeam_channel::bounded(LANE_CAPACITY);
    let (deshred_tx, deshred_rx) = crossbeam_channel::bounded(LANE_CAPACITY);
    let paced_start = Arc::new(lane::PacedStart::new(2, Duration::from_secs(args.attach_secs)));
    let paced_start_unix_ns = Arc::new(std::sync::OnceLock::new());
    let driver_cpus = parse_cpus(args.driver_cpus.as_deref())?;
    let decoder_cpus = parse_cpus(args.decoder_cpus.as_deref())?;
    let fleet_child: Arc<Mutex<Option<Child>>> = Arc::new(Mutex::new(None));
    let cpu_sampler: Arc<Mutex<Option<cpu::CpuSampler>>> = Arc::new(Mutex::new(None));
    let plugin_cpus = parse_cpus(args.plugin_cpus.as_deref())?;

    let lanes = [("ygbLaneMain", true, main_rx), ("ygbLaneDeshred", false, deshred_rx)]
        .into_iter()
        .map(|(name, leader, rx)| {
            let paced_start = Arc::clone(&paced_start);
            let paced_start_unix_ns = Arc::clone(&paced_start_unix_ns);
            let fleet_child = Arc::clone(&fleet_child);
            let fleet_command = fleet_command(&args);
            let cpu_sampler = Arc::clone(&cpu_sampler);
            let plugin_cpus = plugin_cpus.clone();
            let config = lane::LaneConfig {
                name,
                leader,
                speed: args.speed,
                cpus: driver_cpus.clone(),
            };
            let attach_secs = args.attach_secs;
            std::thread::Builder::new()
                .name(name.to_owned())
                .spawn(move || {
                    let on_attach = || {
                        info!("prefill complete: subscribers have {attach_secs} s to attach");
                        let _ = paced_start_unix_ns.set(sys::unix_ns() + attach_secs * 1_000_000_000);
                        *cpu_sampler.lock().expect("cpu sampler lock") =
                            Some(cpu::CpuSampler::start(plugin_cpus.clone()));
                        if let Some(mut command) = fleet_command.take_command() {
                            match command.spawn() {
                                Ok(child) => *fleet_child.lock().expect("fleet lock") = Some(child),
                                Err(error) => log::error!("failed to start the fleet: {error}"),
                            }
                        }
                    };
                    lane::run(&config, &rx, plugin, &paced_start, &on_attach)
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let schedule_config = schedule::ScheduleConfig {
        start_slot: args.start_slot,
        prefill_slots: args.prefill_slots,
        paced_slots: args.slots,
        tail_ns: args.tail_secs * 1_000_000_000,
        reorder_margin_ns: 20_000_000,
    };
    let scheduler = {
        let fixture = args.fixture.clone();
        let manifest = manifest.clone();
        std::thread::Builder::new()
            .name("ygbSchedule".to_owned())
            .spawn(move || {
                if let Err(error) = sys::pin_current_thread(&decoder_cpus) {
                    log::warn!("scheduler: failed to pin: {error}");
                }
                schedule::run(&fixture, &manifest, &schedule_config, &main_tx, &deshred_tx)
            })?
    };

    let schedule_stats = scheduler
        .join()
        .map_err(|_| anyhow::anyhow!("scheduler panicked"))??;
    let mut lane_stats = lanes
        .into_iter()
        .map(|handle| handle.join().map_err(|_| anyhow::anyhow!("lane panicked")))
        .collect::<Result<Vec<_>, _>>()?;
    let deshred_stats = lane_stats.pop().expect("two lanes");
    let main_stats = lane_stats.pop().expect("two lanes");
    let cpu = cpu_sampler
        .lock()
        .expect("cpu sampler lock")
        .take()
        .map(cpu::CpuSampler::finish);
    if let Some(child) = fleet_child.lock().expect("fleet lock").take() {
        stop_fleet(child)?;
    }
    if let Some(cpu) = &cpu {
        fs::write(args.out.join("cpu.json"), serde_json::to_vec_pretty(cpu)?)?;
        println!(
            "cpu (attach to end): plugin workers mean {:.2} peak {:.2} cores; plugin CPUs busy mean {:.2} peak {:.2}, foreign mean {:.2} peak {:.2}",
            cpu.groups.get("plugin_workers").map_or(0.0, |v| v.0),
            cpu.groups.get("plugin_workers").map_or(0.0, |v| v.1),
            cpu.plugin_cpus_busy.0,
            cpu.plugin_cpus_busy.1,
            cpu.plugin_cpus_foreign.0,
            cpu.plugin_cpus_foreign.1,
        );
    }

    let report = RunReport {
        fixture: args.fixture.display().to_string(),
        plugin: args.plugin.display().to_string(),
        started_unix_ns,
        paced_start_unix_ns: paced_start_unix_ns.get().copied(),
        finished_unix_ns: sys::unix_ns(),
        schedule: schedule_stats,
        main_lane: main_stats.report(),
        deshred_lane: deshred_stats.report(),
    };
    fs::write(args.out.join("driver.json"), serde_json::to_vec_pretty(&report)?)?;
    fs::write(
        args.out.join("driver-statuses.json"),
        serde_json::to_vec(&main_stats.statuses)?,
    )?;
    print_summary(&report);
    info!("holding the plugin for {} s", args.hold_secs);
    std::thread::sleep(Duration::from_secs(args.hold_secs));
    // on_unload waits up to 30 s for clients; the process exit releases everything.
    std::process::exit(0);
}

/// The fleet command, built up front and taken once by the leader lane at attach time.
struct FleetCommand(Mutex<Option<Command>>);

impl FleetCommand {
    fn take_command(&self) -> Option<Command> {
        self.0.lock().expect("fleet command lock").take()
    }
}

fn fleet_command(args: &RunArgs) -> FleetCommand {
    let command = args.fleet_profile.as_ref().map(|profile| {
        let mut command = Command::new(std::env::current_exe().expect("current executable"));
        command
            .arg("fleet")
            .arg("--target")
            .arg(format!("127.0.0.1:{}", args.grpc_port))
            .arg("--profile")
            .arg(profile)
            .arg("--out")
            .arg(args.out.join("fleet"))
            .arg("--probe-threads")
            .arg(args.probe_threads.to_string())
            .arg("--load-threads")
            .arg(args.load_threads.to_string())
            .arg("--streams-per-connection")
            .arg(args.streams_per_connection.to_string());
        if let Some(cpus) = &args.probe_cpus {
            command.arg("--probe-cpus").arg(cpus);
        }
        if let Some(cpus) = &args.load_cpus {
            command.arg("--load-cpus").arg(cpus);
        }
        for (flag, value) in [
            ("--max-load", args.max_load),
            ("--max-blocks", args.max_blocks),
            ("--max-deshred", args.max_deshred),
        ] {
            if let Some(value) = value {
                command.arg(flag).arg(value.to_string());
            }
        }
        command
    });
    FleetCommand(Mutex::new(command))
}

fn stop_fleet(mut child: Child) -> anyhow::Result<()> {
    // SAFETY: plain kill(2) on our own child's pid.
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(status) = child.try_wait()? {
            info!("fleet exited: {status}");
            return Ok(());
        }
        anyhow::ensure!(Instant::now() < deadline, "fleet did not exit within 120 s");
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn parse_cpus(list: Option<&str>) -> anyhow::Result<Vec<usize>> {
    list.map(sys::parse_cpu_list).transpose().map(Option::unwrap_or_default)
}

fn write_plugin_config(args: &RunArgs, path: &Path) -> anyhow::Result<()> {
    let template = match &args.config_template {
        Some(template) => fs::read_to_string(template)
            .with_context(|| format!("failed to read {}", template.display()))?,
        None => DEFAULT_CONFIG.to_owned(),
    };
    let mut config: serde_json::Value = serde_json::from_str(&template)?;
    config["grpc"]["address"] = format!("127.0.0.1:{}", args.grpc_port).into();
    config["prometheus"]["address"] = format!("127.0.0.1:{}", args.prometheus_port).into();
    if let Some(cpus) = &args.plugin_cpus {
        config["tokio"]["affinity"] = cpus.clone().into();
    }
    if let Some(workers) = args.worker_threads {
        config["tokio"]["worker_threads"] = workers.into();
    }
    if let Some(slots) = args.replay_stored_slots {
        config["grpc"]["replay_stored_slots"] = slots.into();
    }
    fs::write(path, serde_json::to_vec_pretty(&config)?)?;
    Ok(())
}

fn print_summary(report: &RunReport) {
    let s = &report.schedule;
    println!(
        "schedule: {} frames, prefill {} events, paced {} events, dropped {}, conversion errors {}, max pending {}",
        s.frames, s.prefill_events, s.paced_events, s.dropped_outside_window, s.conversion_errors, s.max_pending
    );
    for (name, lane) in [("main", &report.main_lane), ("deshred", &report.deshred_lane)] {
        println!(
            "{name} lane: prefill {} in {:.1} s, paced {}, errors {}, lag p50 {:.1} us p99 {:.1} us max {:.1} us; timer lag p99 {:.1} us ({}), queue lag p99 {:.1} us ({})",
            lane.prefill_events,
            lane.prefill_secs,
            lane.paced_events,
            lane.errors,
            lane.lag_ns.p50 as f64 / 1e3,
            lane.lag_ns.p99 as f64 / 1e3,
            lane.lag_ns.max as f64 / 1e3,
            lane.timer_lag_ns.p99 as f64 / 1e3,
            lane.timer_lag_ns.count,
            lane.queue_lag_ns.p99 as f64 / 1e3,
            lane.queue_lag_ns.count,
        );
        for (kind, callback) in &lane.callback_ns {
            println!(
                "  {kind:<22} {:>10} calls, callback p50 {:.1} us p99 {:.1} us max {:.1} us",
                callback.count,
                callback.p50 as f64 / 1e3,
                callback.p99 as f64 / 1e3,
                callback.max as f64 / 1e3
            );
        }
    }
}
