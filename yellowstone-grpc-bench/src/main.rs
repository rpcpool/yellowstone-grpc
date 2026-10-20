//! `ygb`: latency benchmark harness for the Yellowstone gRPC geyser plugin.
//! See README.md for the fixture format and the measurement rules.

mod codec;
mod compare;
mod cpu;
mod driver;
mod fixture;
mod fleet;
mod frames;
mod hist;
mod inspect;
mod profiles;
mod record;
mod report;
mod sys;

// Unprefixed jemalloc, as in agave: the dlopen'd plugin's malloc resolves to it too.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[clap(author, version, about = "Yellowstone gRPC latency harness")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Record a node's processed firehose (Subscribe + SubscribeDeshred) into a fixture
    Record(record::RecordArgs),
    /// Check a fixture bank by bank and print a summary
    Inspect(inspect::InspectArgs),
    /// Load a plugin in-process and replay a fixture into it
    Run(driver::RunArgs),
    /// Run subscribers against a plugin and record probe latencies
    Fleet(fleet::FleetArgs),
    /// Summarize probe latencies of a run
    Report(report::ReportArgs),
    /// Generate a production-shaped subscriber profile from a fixture
    Profiles(profiles::GenArgs),
    /// Paired A/B of two runs over the same window, by per-slot p99
    Compare(compare::CompareArgs),
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    match Args::parse().command {
        Command::Record(args) => record::run(args),
        Command::Inspect(args) => inspect::run(args),
        Command::Run(args) => driver::run(args),
        Command::Fleet(args) => fleet::run(args),
        Command::Report(args) => report::run(args),
        Command::Profiles(args) => profiles::run(args),
        Command::Compare(args) => compare::run(args),
    }
}
