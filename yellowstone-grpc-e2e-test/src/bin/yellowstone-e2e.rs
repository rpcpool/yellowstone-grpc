use {
    anyhow::{Context, Result},
    clap::{Parser, Subcommand},
    futures::stream::{self, StreamExt},
    indicatif::{MultiProgress, ProgressBar, ProgressStyle},
    std::{collections::HashMap, env, path::PathBuf, process::ExitCode, time::Duration},
    yellowstone_grpc_intg_test::{
        config::Config,
        grpc::fetch_target_version,
        scenarios::{init_log, RunConfig, Scenario},
    },
};

// 2 is reserved: clap uses it for usage errors.
mod exit_code {
    pub const OK: u8 = 0;
    pub const FAILED: u8 = 1;
    pub const CANNOT_VERIFY: u8 = 3;
}

mod build_info {
    pub const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub const GIT: &str = env!("GIT_VERSION");
    pub const BUILD_TS: &str = env!("VERGEN_BUILD_TIMESTAMP");
    pub const RUSTC: &str = env!("VERGEN_RUSTC_SEMVER");
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// List all available e2e subscriber scenarios.
    List {
        /// Only show scenarios that carry this tag (repeatable; any match passes).
        #[arg(long = "tags", value_name = "TAG")]
        tags: Vec<String>,
        /// Only show scenarios from this module (e.g. `default`, `extra`).
        #[arg(long = "module", value_name = "MODULE", default_value = "default")]
        module: String,
    },
    /// Run all e2e subscriber scenarios.
    All {
        /// Only run scenarios that carry this tag (repeatable; any match passes).
        #[arg(long = "tags", value_name = "TAG")]
        tags: Vec<String>,
        /// Only run scenarios from this module (e.g. `default`, `extra`).
        #[arg(long = "module", value_name = "MODULE", default_value = "default")]
        module: String,
        /// Number of scenarios to run concurrently. Defaults to the number of physical CPU cores.
        #[arg(long = "num-threads", short = 'j', default_value_t = num_cpus::get_physical())]
        num_threads: usize,
    },
    /// Run one specific subscriber scenario.
    Run {
        #[arg(value_name = "SCENARIO")]
        scenario: String,
    },
    /// Verify the plugin version the target reports. Exits 0 on match, 1 on
    /// mismatch, 3 if the version could not be determined.
    VerifyVersion {
        /// Version the target should report (semver, or a git describe prefix).
        expected: Option<String>,
    },
}

#[derive(Debug, Parser)]
#[command(name = "yellowstone-e2e")]
#[command(about = "Clap runner for yellowstone-grpc-e2e-test subscriber scenarios")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Endpoint override. Takes precedence over dotenv and environment variables.
    #[arg(long)]
    endpoint: Option<String>,

    /// Dial string override. Takes precedence over dotenv and environment variables.
    #[arg(long)]
    dial: Option<String>,

    /// x-token override. Takes precedence over dotenv and environment variables.
    #[arg(long)]
    x_token: Option<String>,

    /// Dotenv file path override. If set, only this file is loaded.
    #[arg(long, value_name = "PATH")]
    dotenv: Option<PathBuf>,

    /// TOML config file for scenario-specific parameters.
    #[arg(long, value_name = "PATH")]
    config_file: Option<PathBuf>,
}

fn load_dotenv(dotenv_path_override: Option<&PathBuf>) -> HashMap<String, String> {
    let mut values = HashMap::new();

    if let Some(path) = dotenv_path_override {
        if let Ok(iter) = dotenvy::from_path_iter(path) {
            for entry in iter.flatten() {
                values.insert(entry.0, entry.1);
            }
        }
        return values;
    }

    let cwd = match env::current_dir() {
        Ok(path) => path,
        Err(_) => return values,
    };

    // Prefer .env, then .dotenv if present.
    let candidates = [cwd.join(".env"), cwd.join(".dotenv")];

    for path in candidates {
        if !path.exists() {
            continue;
        }

        if let Ok(iter) = dotenvy::from_path_iter(&path) {
            for entry in iter.flatten() {
                values.insert(entry.0, entry.1);
            }
        }
    }

    values
}

fn resolve_endpoint(cli: &Cli, dotenv_values: &HashMap<String, String>) -> Result<String, String> {
    if let Some(endpoint) = &cli.endpoint {
        return Ok(endpoint.clone());
    }

    if let Some(endpoint) = dotenv_values.get("TEST_ENDPOINT") {
        return Ok(endpoint.clone());
    }

    if let Some(endpoint) = dotenv_values.get("YELLOWSTONE_GRPC_ENDPOINT") {
        return Ok(endpoint.clone());
    }

    if let Ok(endpoint) = env::var("TEST_ENDPOINT") {
        return Ok(endpoint);
    }

    if let Ok(endpoint) = env::var("YELLOWSTONE_GRPC_ENDPOINT") {
        return Ok(endpoint);
    }

    Err(
        "missing endpoint: pass --endpoint, or set TEST_ENDPOINT / YELLOWSTONE_GRPC_ENDPOINT in cwd .env/.dotenv or environment"
            .to_string(),
    )
}

fn resolve_x_token(cli: &Cli, dotenv_values: &HashMap<String, String>) -> Option<String> {
    if let Some(token) = &cli.x_token {
        return Some(token.clone());
    }

    dotenv_values
        .get("TEST_X_TOKEN")
        .cloned()
        .or_else(|| dotenv_values.get("TEST_TOKEN").cloned())
        .or_else(|| dotenv_values.get("YELLOWSTONE_GRPC_X_TOKEN").cloned())
        .or_else(|| env::var("TEST_X_TOKEN").ok())
        .or_else(|| env::var("TEST_TOKEN").ok())
        .or_else(|| env::var("YELLOWSTONE_GRPC_X_TOKEN").ok())
}

fn resolve_dial(cli: &Cli, dotenv_values: &HashMap<String, String>) -> Option<String> {
    if let Some(dial) = &cli.dial {
        return Some(dial.clone());
    }

    dotenv_values
        .get("TEST_DIAL")
        .cloned()
        .or_else(|| dotenv_values.get("YELLOWSTONE_GRPC_DIAL").cloned())
        .or_else(|| env::var("TEST_DIAL").ok())
        .or_else(|| env::var("YELLOWSTONE_GRPC_DIAL").ok())
}

fn matches_tags(scenario: &Scenario, tags: &[String]) -> bool {
    tags.is_empty() || tags.iter().any(|t| scenario.tags.contains(&t.as_str()))
}

fn matches_module(scenario: &Scenario, module: &str) -> bool {
    scenario.module == module
        || scenario.module.ends_with(&format!("::{module}"))
        || scenario.module.contains(&format!("::{module}::"))
}

fn find_scenario(name: &str) -> Result<&'static Scenario> {
    inventory::iter::<Scenario>
        .into_iter()
        .find(|s| s.name == name)
        .ok_or_else(|| {
            let available: Vec<_> = inventory::iter::<Scenario>
                .into_iter()
                .map(|s| s.name)
                .collect();
            anyhow::anyhow!(
                "unknown scenario '{}'; available: {}",
                name,
                available.join(", ")
            )
        })
}

async fn run_scenario(
    scenario: &'static Scenario,
    config: &RunConfig,
    multi: &MultiProgress,
) -> Result<()> {
    let pb = multi.add(ProgressBar::new_spinner());
    pb.set_style(
        ProgressStyle::with_template("{spinner} {msg}")
            .expect("valid template")
            .tick_strings(&["|", "/", "-", "\\"]),
    );
    pb.set_message(format!("running scenario '{}'...", scenario.name));
    pb.enable_steady_tick(Duration::from_millis(120));

    let res = (scenario.run)(config).await;

    pb.disable_steady_tick();

    let message = match &res {
        Ok(()) => format!("✅ scenario '{}' passed", scenario.name),
        Err(err) => format!("❌ scenario '{}' failed: {:#}", scenario.name, err),
    };
    // Print the final line to the scrollback and drop this bar, so the still-running
    // scenarios' spinners stay pinned as the trailing lines instead of being interleaved
    // with finished ones sitting at their original position.
    multi.println(&message).ok();
    pb.finish_and_clear();

    res
}

async fn run(cli: Cli) -> Result<ExitCode> {
    init_log();

    if let Commands::List {
        ref tags,
        ref module,
    } = cli.command
    {
        let scenarios: Vec<&Scenario> = inventory::iter::<Scenario>
            .into_iter()
            .filter(|s| matches_tags(s, tags) && matches_module(s, module))
            .collect();

        if scenarios.is_empty() {
            println!("No scenarios match the specified tags.");
            return Ok(ExitCode::from(exit_code::OK));
        }

        let name_w = scenarios.iter().map(|s| s.name.len()).max().unwrap_or(4);
        let tags_w = scenarios
            .iter()
            .map(|s| s.tags.join(", ").len())
            .max()
            .unwrap_or(4)
            .max(4);

        println!("{:<name_w$}  {:<tags_w$}  DESCRIPTION", "NAME", "TAGS",);
        println!("{}", "─".repeat(name_w + 2 + tags_w + 2 + 40));

        for scenario in scenarios {
            println!(
                "{:<name_w$}  {:<tags_w$}  {}",
                scenario.name,
                scenario.tags.join(", "),
                scenario.description,
            );
        }
        return Ok(ExitCode::from(exit_code::OK));
    }

    let dotenv_values = load_dotenv(cli.dotenv.as_ref());

    let endpoint = resolve_endpoint(&cli, &dotenv_values).map_err(|msg| anyhow::anyhow!(msg))?;
    let dial = resolve_dial(&cli, &dotenv_values);
    let x_token = resolve_x_token(&cli, &dotenv_values);
    let config = match &cli.config_file {
        Some(path) => Config::from_file(path).context("failed to load config file")?,
        None => Config::default(),
    };
    let run_config = RunConfig {
        endpoint,
        dial,
        x_token,
        config,
    };

    println!(
        "yellowstone-e2e version={} git={} built={} rustc={}",
        build_info::VERSION,
        build_info::GIT,
        build_info::BUILD_TS,
        build_info::RUSTC,
    );
    println!("target endpoint: {}", run_config.endpoint);

    if let Commands::VerifyVersion { expected } = &cli.command {
        let target = match fetch_target_version(&run_config).await {
            Ok(target) => target,
            Err(err) => {
                eprintln!("could not determine target plugin version (GetVersion): {err:#}");
                return Ok(ExitCode::from(exit_code::CANNOT_VERIFY));
            }
        };
        println!(
            "target plugin version: version={} git={} proto={} solana={}",
            target.version, target.git, target.proto, target.solana
        );
        return match expected {
            Some(expected) => match target.assert_matches(expected) {
                Ok(()) => {
                    println!("✅ target version matches expected '{expected}'");
                    Ok(ExitCode::from(exit_code::OK))
                }
                Err(err) => {
                    eprintln!("❌ {err:#}");
                    Ok(ExitCode::from(exit_code::FAILED))
                }
            },
            None => Ok(ExitCode::from(exit_code::OK)),
        };
    }

    match fetch_target_version(&run_config).await {
        Ok(target) => println!(
            "target plugin version: version={} git={} proto={} solana={}",
            target.version, target.git, target.proto, target.solana
        ),
        Err(err) => println!("⚠️ could not read target plugin version (GetVersion): {err:#}"),
    }

    match &cli.command {
        Commands::List { .. } | Commands::VerifyVersion { .. } => Ok(ExitCode::from(exit_code::OK)),
        Commands::All {
            ref tags,
            ref module,
            num_threads,
        } => {
            let scenarios: Vec<&'static Scenario> = inventory::iter::<Scenario>
                .into_iter()
                .filter(|s| matches_tags(s, tags) && matches_module(s, module.as_str()))
                .collect();

            let num_threads = (*num_threads).max(1);
            let multi = MultiProgress::new();

            // Run every scenario to completion regardless of earlier failures, so one
            // failing scenario doesn't hide the results of the others.
            let results: Vec<(&'static str, Result<()>)> = stream::iter(scenarios)
                .map(|scenario| {
                    let run_config = &run_config;
                    let multi = &multi;
                    async move {
                        log::info!("running scenario: {}", scenario.name);
                        (scenario.name, run_scenario(scenario, run_config, multi).await)
                    }
                })
                .buffer_unordered(num_threads)
                .collect()
                .await;

            let failed: Vec<&str> = results
                .iter()
                .filter(|(_, res)| res.is_err())
                .map(|(name, _)| *name)
                .collect();

            if failed.is_empty() {
                Ok(ExitCode::from(exit_code::OK))
            } else {
                eprintln!(
                    "❌ {}/{} scenario(s) failed: {}",
                    failed.len(),
                    results.len(),
                    failed.join(", ")
                );
                Ok(ExitCode::from(exit_code::FAILED))
            }
        }
        Commands::Run { scenario } => {
            let entry = find_scenario(scenario)?;
            let multi = MultiProgress::new();
            run_scenario(entry, &run_config, &multi)
                .await
                .with_context(|| format!("scenario '{}' failed", scenario))?;
            Ok(ExitCode::from(exit_code::OK))
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    match run(cli).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err:#}");
            ExitCode::from(exit_code::FAILED)
        }
    }
}
