//! E2E test scenario registry — shared types, configuration, and submodule declarations.

use {crate::config::Config, std::sync::Once};

static LOG_INIT: Once = Once::new();

/// Boxed async fn signature for scenario functions.
pub type ScenarioFn = for<'a> fn(
    &'a RunConfig,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>,
>;

pub struct Scenario {
    /// Stable CLI scenario name (for example `sysvar-account`).
    pub name: &'static str,
    /// Human-friendly scenario description shown in `yellowstone-e2e list`.
    pub description: &'static str,
    /// Rust module path where this scenario was defined (from `module_path!()`).
    pub module: &'static str,
    /// Optional classification tags (e.g. `&["accounts", "filters"]`).
    pub tags: &'static [&'static str],
    /// Callable entry point; box-pins the underlying async fn.
    pub run: ScenarioFn,
}

inventory::collect!(Scenario);

#[derive(Debug, Clone)]
pub struct RunConfig {
    /// gRPC endpoint URI used by scenarios.
    pub endpoint: String,
    /// Optional dial target (`host:port`) used for TCP connection override.
    pub dial: Option<String>,
    /// Optional x-token used for authenticated requests.
    pub x_token: Option<String>,
    pub config: Config,
}

/// Initializes logger once for scenario execution.
pub fn init_log() {
    LOG_INIT.call_once(|| {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error"))
            .try_init()
            .ok();
    });
}

pub mod blockmachine;
pub mod default;
pub mod ratelimit;
