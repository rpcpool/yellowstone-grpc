//! Yellowstone gRPC end-to-end test scenarios and shared test harness helpers.
//!
//! The [`scenarios`] module contains individual async scenarios used by the
//! `yellowstone-e2e` CLI tool. Scenario metadata is registered through
//! `#[test_helper(...)]` annotations and exposed for listing in the CLI.

pub mod config;
pub mod grpc;
pub mod scenarios;
pub mod serde;

/// Declares the config type for all scenarios in the calling module.
///
/// Expands to `pub type ModuleConfig = $ty;`, so every `#[test_helper]` in the
/// same file can write `config = ModuleConfig` instead of repeating the full type.
///
/// # Example
/// ```ignore
/// module_config!(RatelimitConfig);
///
/// #[test_helper(name = "get-version", config = ModuleConfig)]
/// pub async fn test_get_version(config: &RunConfig, cfg: &ModuleConfig) -> Result<()> { ... }
/// ```
#[macro_export]
macro_rules! module_config {
    ($ty:ty) => {
        pub type ModuleConfig = $ty;
    };
}
