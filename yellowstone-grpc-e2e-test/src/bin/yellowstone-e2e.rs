use {
    anyhow::{Context, Result},
    clap::{Parser, Subcommand, ValueEnum},
    std::{collections::HashMap, env, path::PathBuf, process::ExitCode},
    yellowstone_grpc_intg_test::scenarios::{
        any_commitment_level_of_subscription_should_return_all_possible_values, init_log,
        it_should_subscribe_to_all_transaction_include_token_ata_to_an_owner,
        it_should_support_replay, scenario_description,
        subscribe_should_only_returns_sysvarclock_account,
        subscribe_should_receive_block_where_sysvarclock1111_account_has_been_updated,
        subscribe_should_receive_full_blocks, test_subscribe_deshred, RunConfig,
    },
};

#[derive(Debug, Clone, ValueEnum)]
enum Scenario {
    SysvarAccount,
    SysvarBlock,
    FullBlocks,
    Replay,
    Deshred,
    AnyCommitment,
    TokenOwnerBalanceChanged,
}

impl Scenario {
    const fn name(&self) -> &'static str {
        match self {
            Self::SysvarAccount => "sysvar-account",
            Self::SysvarBlock => "sysvar-block",
            Self::FullBlocks => "full-blocks",
            Self::Replay => "replay",
            Self::Deshred => "deshred",
            Self::AnyCommitment => "any-commitment",
            Self::TokenOwnerBalanceChanged => "token-owner-balance-changed",
        }
    }

    fn description(&self) -> &'static str {
        scenario_description(self.name()).unwrap_or("No description available")
    }
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// List all available e2e subscriber scenarios.
    List,
    /// Run all e2e subscriber scenarios.
    All,
    /// Run one specific subscriber scenario.
    Run {
        #[arg(value_enum)]
        scenario: Scenario,
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

    /// x-token override. Takes precedence over dotenv and environment variables.
    #[arg(long)]
    x_token: Option<String>,

    /// Dotenv file path override. If set, only this file is loaded.
    #[arg(long, value_name = "PATH")]
    dotenv: Option<PathBuf>,
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

async fn run_scenario(scenario: &Scenario, config: &RunConfig) -> Result<()> {
    match scenario {
        Scenario::SysvarAccount => subscribe_should_only_returns_sysvarclock_account(config).await,
        Scenario::SysvarBlock => {
            subscribe_should_receive_block_where_sysvarclock1111_account_has_been_updated(config)
                .await
        }
        Scenario::FullBlocks => subscribe_should_receive_full_blocks(config).await,
        Scenario::Replay => it_should_support_replay(config).await,
        Scenario::Deshred => test_subscribe_deshred(config).await,
        Scenario::AnyCommitment => {
            any_commitment_level_of_subscription_should_return_all_possible_values(config).await
        }
        Scenario::TokenOwnerBalanceChanged => {
            it_should_subscribe_to_all_transaction_include_token_ata_to_an_owner(config).await
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    init_log();

    if let Commands::List = cli.command {
        let scenarios = [
            Scenario::SysvarAccount,
            Scenario::SysvarBlock,
            Scenario::FullBlocks,
            Scenario::Replay,
            Scenario::Deshred,
            Scenario::AnyCommitment,
            Scenario::TokenOwnerBalanceChanged,
        ];

        for scenario in scenarios {
            println!("{} - {}", scenario.name(), scenario.description());
        }
        return Ok(());
    }

    let dotenv_values = load_dotenv(cli.dotenv.as_ref());

    let endpoint = resolve_endpoint(&cli, &dotenv_values).map_err(|msg| anyhow::anyhow!(msg))?;
    let x_token = resolve_x_token(&cli, &dotenv_values);
    let run_config = RunConfig { endpoint, x_token };

    match &cli.command {
        Commands::List => Ok(()),
        Commands::All => {
            let scenarios = [
                Scenario::SysvarAccount,
                Scenario::SysvarBlock,
                Scenario::FullBlocks,
                Scenario::Replay,
                Scenario::Deshred,
                Scenario::AnyCommitment,
                Scenario::TokenOwnerBalanceChanged,
            ];

            for scenario in scenarios {
                log::info!("running scenario: {}", scenario.name());
                run_scenario(&scenario, &run_config)
                    .await
                    .with_context(|| format!("scenario '{}' failed", scenario.name()))?;
            }
            Ok(())
        }
        Commands::Run { scenario } => run_scenario(scenario, &run_config)
            .await
            .with_context(|| format!("scenario '{}' failed", scenario.name())),
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
