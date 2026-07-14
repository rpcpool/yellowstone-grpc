use {
    anyhow::{Context, Result},
    clap::{Parser, Subcommand},
    std::{collections::HashMap, env, io::Write, path::PathBuf, process::ExitCode},
    tokio::time::{self, Duration, MissedTickBehavior},
    yellowstone_grpc_intg_test::{
        config::Config,
        scenarios::{init_log, RunConfig, Scenario},
    },
};

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
    },
    /// Run one specific subscriber scenario.
    Run {
        #[arg(value_name = "SCENARIO")]
        scenario: String,
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

async fn run_scenario(scenario: &'static Scenario, config: &RunConfig) -> Result<()> {
    let mut result = Box::pin((scenario.run)(config));
    let mut interval = time::interval(Duration::from_millis(120));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let frames = ["|", "/", "-", "\\"];
    let mut frame_index = 0usize;
    let mut stdout = std::io::stdout();

    loop {
        tokio::select! {
            res = &mut result => {
                write!(stdout, "\r\x1b[2K")?;
                if res.is_ok() {
                    writeln!(stdout, "✅ scenario '{}' passed", scenario.name)?;
                } else {
                    writeln!(
                        stdout,
                        "❌ scenario '{}' failed: {:#}",
                        scenario.name,
                        res.as_ref().expect_err("error should be present on failure")
                    )?;
                }
                stdout.flush()?;
                return res;
            }
            _ = interval.tick() => {
                write!(
                    stdout,
                    "\r{} running scenario '{}'...",
                    frames[frame_index],
                    scenario.name
                )?;
                stdout.flush()?;
                frame_index = (frame_index + 1) % frames.len();
            }
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
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
            return Ok(());
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
        return Ok(());
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

    match &cli.command {
        Commands::List { .. } => Ok(()),
        Commands::All {
            ref tags,
            ref module,
        } => {
            for scenario in inventory::iter::<Scenario> {
                if !matches_tags(scenario, tags) || !matches_module(scenario, module.as_str()) {
                    continue;
                }
                log::info!("running scenario: {}", scenario.name);
                run_scenario(scenario, &run_config)
                    .await
                    .with_context(|| format!("scenario '{}' failed", scenario.name))?;
            }
            Ok(())
        }
        Commands::Run { scenario } => {
            let entry = find_scenario(scenario)?;
            run_scenario(entry, &run_config)
                .await
                .with_context(|| format!("scenario '{}' failed", scenario))
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err:#}");
            ExitCode::from(1)
        }
    }
}
