#![deny(clippy::clone_on_ref_ptr)]
#![deny(clippy::missing_const_for_fn)]
#![deny(clippy::trivially_copy_pass_by_ref)]

pub mod config;
pub mod filters;
pub mod grpc;
pub mod plugin;
pub mod prom;
pub mod version;

pub fn get_thread_name() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
    let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
    format!("solGeyserGrpc{id:02}")
}
