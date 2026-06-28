use yellowstone_grpc_geyser_harness::{dispatcher, generator, loader};

use clap::Parser;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "yellowstone-geyser-harness")]
struct Cli {
    /// Path to the geyser plugin .so
    #[arg(long)]
    so: PathBuf,

    /// Path to the plugin config JSON
    #[arg(long)]
    config: Option<PathBuf>,

    /// Number of dispatch threads
    #[arg(long, default_value_t = 4)]
    threads: usize,

    /// Number of account events to generate
    #[arg(long, default_value_t = 1_000_000)]
    accounts: u64,

    /// Size of account data in bytes
    #[arg(long, default_value_t = 256)]
    account_data_size: usize,

    /// Benchmark duration in seconds (excluding warmup)
    #[arg(long, default_value_t = 60)]
    duration: u64,

    /// Warmup period in seconds (discarded from measurements)
    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,

    /// Emit a slot status event every N account events
    #[arg(long, default_value_t = 1000)]
    slot_interval: u64,

    /// RNG seed for deterministic event generation
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

fn main() {
    let cli = Cli::parse();

    let config_path = match &cli.config {
        Some(path) => path.clone(),
        None => {
            let mut tmp = std::env::temp_dir();
            tmp.push("geyser-harness-config.json");
            let default_config = serde_json::json!({
                "grpc": {
                    "shmem_path": "/dev/shm/yellowstone-harness",
                    "shmem_dcache_capacity": 1024 * 16,
                    "shmem_mcache_capacity": 1024 * 1024 * 16
                }
            });
            let mut f = std::fs::File::create(&tmp).expect("failed to create tmp config");
            f.write_all(default_config.to_string().as_bytes())
                .expect("failed to write tmp config");
            eprintln!("no --config given, wrote default shmem config to {tmp:?}");
            tmp
        }
    };

    eprintln!("loading plugin from {:?}", cli.so);
    let handle =
        loader::PluginHandle::load(&cli.so, &config_path).expect("failed to load plugin");

    eprintln!(
        "generating {} accounts ({}B data, slot every {})",
        cli.accounts, cli.account_data_size, cli.slot_interval
    );
    let events = generator::generate(&generator::EventGeneratorConfig {
        account_count: cli.accounts,
        account_data_size: cli.account_data_size,
        slot_interval: cli.slot_interval,
        seed: cli.seed,
    });
    eprintln!("generated {} events", events.len());

    eprintln!(
        "running: {} threads, {}s warmup, {}s benchmark",
        cli.threads, cli.warmup_secs, cli.duration
    );
    let report = dispatcher::run(
        handle.plugin(),
        &events,
        cli.threads,
        Duration::from_secs(cli.warmup_secs),
        Duration::from_secs(cli.duration),
    );

    println!("{report}");
}