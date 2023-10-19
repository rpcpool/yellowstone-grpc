#![deny(clippy::clone_on_ref_ptr)]
#![deny(clippy::missing_const_for_fn)]
#![deny(clippy::trivially_copy_pass_by_ref)]

pub mod config;
pub mod google_pubsub;
pub mod kafka;
pub mod prom;
pub mod version;

use {
    futures::future::{BoxFuture, FutureExt},
    tokio::signal::unix::{signal, SignalKind},
};

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
