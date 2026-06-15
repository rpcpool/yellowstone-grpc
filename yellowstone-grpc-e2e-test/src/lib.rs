//! Yellowstone gRPC end-to-end test scenarios and shared test harness helpers.
//!
//! The [`scenarios`] module contains individual async scenarios used by the
//! `yellowstone-e2e` CLI tool. Scenario metadata is registered through
//! `#[test_helper(...)]` annotations and exposed for listing in the CLI.

pub mod scenarios;
