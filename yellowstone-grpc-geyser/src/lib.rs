#![deny(clippy::clone_on_ref_ptr)]
#![deny(clippy::missing_const_for_fn)]
#![deny(clippy::trivially_copy_pass_by_ref)]

pub mod config;
pub mod filters;
pub mod grpc;
pub mod plugin;
pub mod prom;
pub mod proto;
pub mod version;
