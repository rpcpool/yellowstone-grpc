#![deny(clippy::clone_on_ref_ptr)]
#![deny(clippy::missing_const_for_fn)]
#![deny(clippy::trivially_copy_pass_by_ref)]

pub mod config;
#[cfg(feature = "google-pubsub")]
pub mod google_pubsub;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod prom;
#[cfg(feature = "scylla")]
pub mod scylladb;
pub mod version;

use {
    futures::future::{BoxFuture, FutureExt},
    tokio::signal::unix::{signal, SignalKind},
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
};

pub fn setup_tracing() -> anyhow::Result<()> {
    let is_atty = atty::is(atty::Stream::Stdout) && atty::is(atty::Stream::Stderr);
    let io_layer = tracing_subscriber::fmt::layer().with_ansi(is_atty);
    let level_layer = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(io_layer)
        .with(level_layer)
        .try_init()?;
    Ok(())
}

pub fn create_shutdown() -> anyhow::Result<BoxFuture<'static, ()>> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {}
        };
    }
    .boxed())
}
