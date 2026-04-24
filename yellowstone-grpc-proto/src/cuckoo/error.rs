//! Error types for the `cuckoo` module.
//!
//! Errors are scoped to the method that produces them — see [`CuckooBuildError`]
//! for build-time failures and [`TableFullError`] for `insert` failures.
//! Callers only pattern-match on variants the method can actually produce,
//! avoiding leaky union error types.

use {super::constants::MAX_KICKS, thiserror::Error};

/// Build-time error for [`CuckooFilter`] and [`CuckooMap`] construction.
///
/// Returned by [`CuckooFilter::with_capacity`], [`CuckooFilter::with_capacity_and_hasher`],
/// and [`CuckooMap::with_capacity`].
///
/// [`CuckooFilter`]: super::filter::CuckooFilter
/// [`CuckooFilter::with_capacity`]: super::filter::CuckooFilter::with_capacity
/// [`CuckooFilter::with_capacity_and_hasher`]: super::filter::CuckooFilter::with_capacity_and_hasher
/// [`CuckooMap`]: super::map::CuckooMap
/// [`CuckooMap::with_capacity`]: super::map::CuckooMap::with_capacity
#[derive(Debug, Error)]
pub enum CuckooBuildError {
    /// The requested capacity could not be allocated.
    ///
    /// Either the next power-of-two bucket count would exceed `usize::MAX`,
    /// or the system rejected the allocation (e.g., not enough memory).
    #[error("capacity overflow: requested capacity exceeds maximum")]
    CapacityOverflow,
}

/// Error returned when [`CuckooFilter::insert`] or [`CuckooMap::insert`] cannot
/// accommodate a new item.
///
/// Indicates the filter reached its load limit and could not relocate an
/// existing fingerprint after `MAX_KICKS` attempts. Typically means the filter
/// was under-sized for the workload; rebuild with a larger `max_capacity`.
///
/// On error, the inserting type's state is unchanged.
///
/// [`CuckooFilter::insert`]: super::filter::CuckooFilter::insert
/// [`CuckooMap::insert`]: super::map::CuckooMap::insert
#[derive(Debug, Error)]
#[error("cuckoo table full after {} kicks", MAX_KICKS)]
pub struct TableFullError;
