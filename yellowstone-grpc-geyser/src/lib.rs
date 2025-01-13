pub mod config;
pub mod grpc;
pub mod metrics;
pub mod plugin;
pub mod version;

pub fn get_thread_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static ATOMIC_ID: AtomicU64 = AtomicU64::new(0);
    let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
    format!("solGeyserGrpc{id:02}")
}
