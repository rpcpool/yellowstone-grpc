//! Integration-test harness for the type generator (`typegen.rs`).
//!
//! # Why this file exists
//!
//! `typegen.rs` is compiled as part of the *build script* (`build.rs`), not as
//! part of the regular library crate.  Build-script code lives in its own
//! separate compilation unit and is therefore not reachable from normal
//! `#[test]` functions or from `cargo test` without extra plumbing.
//!
//! Cargo treats every `.rs` file inside the `tests/` directory as a standalone
//! integration-test binary.  By re-declaring the typegen module here with an
//! explicit `#[path]` attribute, this file pulls `typegen.rs` into a test
//! binary where it *can* be compiled and tested like ordinary Rust code.
//!
//! # What this enables
//!
//! * The module compiles successfully under `cargo test` (a basic smoke test
//!   that the generator code is syntactically and semantically valid).
//! * Individual helper functions inside `typegen.rs` can be exercised by adding
//!   `#[test]` functions in this file or in child modules declared here,
//!   without having to run a full build-script invocation.
//! * CI can catch regressions in the generator logic independently of the
//!   generated output (`src/js_types.rs`).

#[path = "../typegen.rs"]
mod typegen;
