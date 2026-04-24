//! Compressed account filtering via cuckoo filters.
//!
//! This module provides a probabilistic account filter for subscribe requests,
//! aimed at clients tracking large pubkey sets (e.g., hundreds of thousands to
//! millions). Instead of uploading an explicit pubkey list every few seconds,
//! the client uploads a compact cuckoo filter typically 3 bytes per pubkey
//! at 95% load and the server matches accounts against it.
//!
//! # Primary API
//!
//! - [`CuckooMap`] — safe, tracked wrapper. Use this.
//! - [`CuckooFilter`] — raw filter. Has a `remove` footgun; prefer [`CuckooMap`].
//!
//! # Example — cuckoo account filter alongside transaction and block filters
//!
//! ```no_run
//! use {
//!     yellowstone_grpc_proto::{
//!         cuckoo::CuckooMap,
//!         geyser::{
//!             SubscribeRequest,
//!             SubscribeRequestFilterTransactions,
//!             SubscribeRequestFilterBlocksMeta,
//!         },
//!     },
//!     std::collections::HashMap,
//! };
//!
//! // 1. Build the cuckoo map for the ~2M accounts we track
//! let mut accounts = CuckooMap::<[u8; 32]>::with_capacity(2_000_000).unwrap();
//! for pk in my_tracked_pubkeys() {
//!     accounts.insert(pk).unwrap();
//! }
//!
//! // 2. Construct the subscribe request; cuckoo is one of several filters
//! let mut req = SubscribeRequest::default();
//!
//! //    cuckoo filter under a name of our choosing
//! accounts.insert_into_subscribe_request(&mut req, "tracked_accounts");
//!
//! //    explicit transaction filter for specific signatures
//! req.transactions.insert(
//!     "txs".to_string(),
//!     SubscribeRequestFilterTransactions {
//!         vote: Some(false),
//!         failed: Some(false),
//!         ..Default::default()
//!     },
//! );
//!
//! //    block metadata for all slots
//! req.blocks_meta.insert(
//!     "meta".to_string(),
//!     SubscribeRequestFilterBlocksMeta::default(),
//! );
//!
//! //    send `req` to the server...
//!
//! // 3. Update the filter as tracked set changes
//! accounts.insert([7u8; 32]).unwrap();
//! accounts.remove(&[3u8; 32]);
//!
//! // 4. Only re-send when something changed
//! if accounts.take_dirty() {
//!     accounts.insert_into_subscribe_request(&mut req, "tracked_accounts");
//!     // re-send `req` on the existing stream sink
//! }
//!
//! # fn my_tracked_pubkeys() -> Vec<[u8; 32]> { vec![] }
//! ```
//!
//! # Handling updates
//!
//! Account updates flowing in from the server may include false positives
//! (bounded at <1% at full load). Filter locally with [`CuckooMap::contains`]
//! for an exact check:
//!
//! ```no_run
//! # use yellowstone_grpc_proto::cuckoo::CuckooMap;
//! # let accounts: CuckooMap<[u8; 32]> = CuckooMap::with_capacity(100).unwrap();
//! # let incoming_pubkey = [0u8; 32];
//! if accounts.contains(&incoming_pubkey) {
//!     // definitely a tracked account
//! } else {
//!     // false positive from the server-side cuckoo check — ignore
//! }
//! ```

mod constants;
mod error;
mod filter;
mod hasher;
mod map;

pub use {
    constants::DEFAULT_HASH_SEED,
    error::{CuckooBuildError, TableFullError},
    filter::CuckooFilter,
    hasher::YellowstoneHasherBuilder,
    map::CuckooMap,
};
