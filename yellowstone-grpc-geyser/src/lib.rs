pub(crate) mod auth;
pub(crate) mod billing;
mod block_reconstruction;
mod cache_ext;
pub mod config;
pub(crate) mod file_watcher;
pub mod grpc;
pub mod metered;
pub mod metrics;
pub mod plugin;
pub(crate) mod ratelimit;
pub mod stream;
pub(crate) mod util;
pub mod version;

pub fn get_thread_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static ATOMIC_ID: AtomicU64 = AtomicU64::new(0);
    let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
    format!("solGeyserGrpc{id:02}")
}

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;
