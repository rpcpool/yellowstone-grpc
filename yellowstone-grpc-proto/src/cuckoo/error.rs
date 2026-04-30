//! Error types for the `cuckoo` module.
//!
//! Errors are scoped to the method that produces them — see [`CuckooBuildError`]
//! for build-time failures and [`TableFullError`] for `insert` failures.
//! Callers only pattern-match on variants the method can actually produce,
//! avoiding leaky union error types.

use {super::constants::MAX_KICKS, thiserror::Error};

/// Build-time error for [`CuckooFilter`] and [`CompressedAccountFilterSet`] construction.
///
/// Returned by [`CuckooFilter::with_capacity`], [`CuckooFilter::with_capacity_and_hasher`],
/// and [`CompressedAccountFilterSet::with_capacity`].
///
/// [`CuckooFilter`]: super::filter::CuckooFilter
/// [`CuckooFilter::with_capacity`]: super::filter::CuckooFilter::with_capacity
/// [`CuckooFilter::with_capacity_and_hasher`]: super::filter::CuckooFilter::with_capacity_and_hasher
/// [`CompressedAccountFilterSet`]: super::map::CompressedAccountFilterSet
/// [`CompressedAccountFilterSet::with_capacity`]: super::map::CompressedAccountFilterSet::with_capacity
#[derive(Debug, Error)]
pub enum CuckooBuildError {
    /// The requested capacity could not be allocated.
    ///
    /// Either the next power-of-two bucket count would exceed `usize::MAX`,
    /// or the system rejected the allocation (e.g., not enough memory).
    #[error("capacity overflow: requested capacity exceeds maximum")]
    CapacityOverflow,
}

/// Error returned when [`CuckooFilter::insert`] or [`CompressedAccountFilterSet::insert`] cannot
/// accommodate a new item.
///
/// Indicates the filter reached its load limit and could not relocate an
/// existing fingerprint after `MAX_KICKS` attempts. Typically means the filter
/// was under-sized for the workload; rebuild with a larger `max_capacity`.
///
/// On error, the inserting type's state is unchanged.
///
/// [`CuckooFilter::insert`]: super::filter::CuckooFilter::insert
/// [`CompressedAccountFilterSet::insert`]: super::map::CompressedAccountFilterSet::insert
#[derive(Debug, Error)]
#[error("cuckoo table full after {} kicks", MAX_KICKS)]
pub struct TableFullError;
