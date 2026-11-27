#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
//! The Subscribe service, its stream queues and its response bodies.

pub mod entry;
pub mod group;
pub mod head;
pub mod key;
#[cfg(test)]
pub mod queue_item_decoding;
pub mod response_body;
pub mod sender;
pub mod service;
pub mod staging;
