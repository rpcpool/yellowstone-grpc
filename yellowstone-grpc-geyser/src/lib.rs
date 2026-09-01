pub mod auth;
pub mod billing;
// pub mod block_reconstruction;
pub mod cache_ext;
pub mod config;
pub mod contact_info;
pub mod file_watcher;
pub mod grpc;
pub mod metered;
pub mod metrics;
pub mod plugin;
pub mod ratelimit;
pub mod stream;
pub mod util;
pub mod version;
pub use agave_geyser_plugin_interface as plugin_interface;
mod block_reconstruction_v2;

pub fn get_thread_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static ATOMIC_ID: AtomicU64 = AtomicU64::new(0);
    let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
    format!("solGeyserGrpc{id:02}")
}
