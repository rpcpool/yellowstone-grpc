//! Safe, tracked wrapper around [`CuckooFilter`] for client-side filter construction.
//!
//! [`CuckooMap`] is the primary user-facing type for building cuckoo filters to
//! send in subscribe requests. It maintains an exact-membership [`HashSet`]
//! alongside the probabilistic filter, which:
//!
//! - **Makes `remove` safe**: the filter is only mutated for items actually
//!   present, avoiding the footgun where removing an un-inserted item clears
//!   a different item sharing its fingerprint.
//! - **Makes `contains` exact**: client-side membership checks return the
//!   true answer, not a probabilistic one.
//!
//! The probabilistic filter is only used on the wire. Server-side matching
//! accepts false positives (clients filter locally on receipt).
//!
//! [`HashSet`]: std::collections::HashSet

use {
    super::{
        error::{CuckooBuildError, TableFullError},
        filter::CuckooFilter,
    },
    crate::geyser::{
        CuckooFilter as ProtoCuckooFilter, SubscribeRequest, SubscribeRequestFilterAccounts,
    },
    std::{collections::HashSet, hash::Hash},
};

/// A HashMap-like wrapper around [`CuckooFilter`] for safe filter construction.
///
/// Maintains two parallel collections:
/// - A [`HashSet`] as the source of truth for exact membership
/// - A [`CuckooFilter`] as the compact wire representation
///
/// Writes go to both; reads (`contains`, `len`) answer from the [`HashSet`];
/// serialization reads from the [`CuckooFilter`]. This gives users exact local
/// semantics while producing a compact probabilistic filter for the server.
///
/// # When to Use
///
/// Use [`CuckooMap`] whenever you need to build a cuckoo filter to send in a
/// subscribe request. It is strictly safer than constructing a [`CuckooFilter`]
/// directly — the filter's `remove` method has a footgun (it can silently remove
/// the wrong item when fingerprints collide), and [`CuckooMap`] guards against it.
///
/// # Example
///
/// ```
/// use yellowstone_grpc_proto::cuckoo::CuckooMap;
///
/// let mut map = CuckooMap::<u64>::with_capacity(1000).unwrap();
/// map.insert(42).unwrap();
/// map.insert(100).unwrap();
/// assert!(map.contains(&42));
/// assert_eq!(map.len(), 2);
///
/// map.remove(&42);
/// assert!(!map.contains(&42));
/// ```
///
/// [`HashSet`]: std::collections::HashSet
pub struct CuckooMap<T> {
    items: HashSet<T>,
    filter: CuckooFilter<T>,
}

impl<T> CuckooMap<T>
where
    T: Sized + Hash + Eq,
{
    /// Creates an empty map pre-sized for up to `max_capacity` items.
    ///
    /// Both the internal [`HashSet`] and [`CuckooFilter`] are allocated up front
    /// to avoid rehashing or reallocation during inserts.
    ///
    /// # Errors
    ///
    /// Returns [`CuckooBuildError::CapacityOverflow`] if `max_capacity` exceeds
    /// what the system can allocate, or if the underlying cuckoo filter cannot
    /// be built at the requested size.
    ///
    /// # Example
    ///
    /// ```
    /// use yellowstone_grpc_proto::cuckoo::CuckooMap;
    ///
    /// let map = CuckooMap::<u64>::with_capacity(10_000).unwrap();
    /// assert!(map.is_empty());
    /// ```
    ///
    /// [`HashSet`]: std::collections::HashSet
    pub fn with_capacity(max_capacity: usize) -> Result<Self, CuckooBuildError> {
        let filter = CuckooFilter::with_capacity(max_capacity)?;

        let mut items: HashSet<T> = HashSet::new();
        items
            .try_reserve(max_capacity)
            .map_err(|_| CuckooBuildError::CapacityOverflow)?;

        Ok(Self { items, filter })
    }

    /// Inserts an item into the map.
    ///
    /// Returns `Ok(true)` if the item was newly inserted, `Ok(false)` if it was
    /// already present (idempotent).
    ///
    /// # Errors
    ///
    /// Returns [`TableFullError`] if the underlying cuckoo filter is saturated and
    /// cannot accommodate the item. This typically means the map was under-sized
    /// for the workload; rebuild with a larger `max_capacity`.
    ///
    /// On error, the map's state is unchanged — neither the [`HashSet`] nor the
    /// [`CuckooFilter`] is mutated.
    ///
    /// [`HashSet`]: std::collections::HashSet
    pub fn insert(&mut self, v: T) -> Result<bool, TableFullError> {
        if self.items.contains(&v) {
            return Ok(false);
        }
        self.filter.insert(&v)?;
        self.items.insert(v);
        Ok(true)
    }

    /// Removes an item from the map.
    ///
    /// Returns `true` if the item was present and removed, `false` if it was not
    /// in the map.
    ///
    /// # Safety over [`CuckooFilter::remove`]
    ///
    /// The underlying [`CuckooFilter`] has a known footgun: removing an item that
    /// was never inserted can silently remove a different item that shares the
    /// same fingerprint. [`CuckooMap`] prevents this by checking its internal
    /// [`HashSet`] first and only touching the filter when the item is genuinely
    /// present.
    ///
    /// [`HashSet`]: std::collections::HashSet
    pub fn remove(&mut self, v: &T) -> bool {
        if self.items.remove(v) {
            self.filter.remove(v);
            true
        } else {
            false
        }
    }

    /// Checks if an item is in the map.
    ///
    /// Unlike [`CuckooFilter::contains`], this returns an exact answer — the
    /// [`HashSet`] guarantees no false positives.
    ///
    /// [`HashSet`]: std::collections::HashSet
    pub fn contains(&self, v: &T) -> bool {
        self.items.contains(v) // exact, uses HashSet — no error possible
    }

    /// Returns the number of items in the map.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if the map contains no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Serializes the underlying cuckoo filter to its proto wire format.
    ///
    /// The returned proto carries all parameters needed for deserialization on
    /// the server side or in cross-language clients: bucket count, entries per
    /// bucket, fingerprint bits, and the hash seed.
    pub fn to_proto(&self) -> ProtoCuckooFilter {
        ProtoCuckooFilter::from(&self.filter)
    }

    /// Replaces any existing account filters in the request with this map's cuckoo filter.
    ///
    /// Clears `req.accounts` and inserts a single entry named `"default"` carrying
    /// the cuckoo filter in the `cuckoo_accounts_filter` field. Other account filter
    /// fields (explicit `account` list, `owner`, predicate `filters`,
    /// `nonempty_txn_signature`) are left empty.
    ///
    /// # When to Use
    ///
    /// Call this when the cuckoo filter is your *only* account matching strategy.
    /// For setups that combine cuckoo matching with explicit pubkey lists or owner
    /// filters, construct the [`SubscribeRequestFilterAccounts`] directly rather
    /// than using this helper — this method is destructive and discards any
    /// existing filter configuration.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use yellowstone_grpc_proto::{
    ///     cuckoo::CuckooMap,
    ///     geyser::SubscribeRequest,
    /// };
    ///
    /// let mut map = CuckooMap::<[u8; 32]>::with_capacity(1000).unwrap();
    /// map.insert([1u8; 32]).unwrap();
    ///
    /// let mut req = SubscribeRequest::default();
    /// map.override_subscribe_request(&mut req);
    /// // req now has one account filter named "default" with the cuckoo data.
    /// ```
    ///
    /// [`SubscribeRequestFilterAccounts`]: crate::geyser::SubscribeRequestFilterAccounts
    pub fn override_subscribe_request(&self, req: &mut SubscribeRequest) {
        let filter = SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
            cuckoo_accounts_filter: Some(self.to_proto()),
        };

        req.accounts.clear();
        req.accounts.insert("default".to_string(), filter);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_contains() {
        let mut map = CuckooMap::with_capacity(100).unwrap();
        assert!(map.insert("hello").unwrap());
        assert!(map.contains(&"hello"));
        assert!(!map.contains(&"world"));
    }

    #[test]
    fn insert_duplicate_returns_false() {
        let mut map = CuckooMap::with_capacity(100).unwrap();
        assert!(map.insert("hello").unwrap());
        assert!(!map.insert("hello").unwrap()); // already exists
    }

    #[test]
    fn remove_existing() {
        let mut map = CuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();
        assert!(map.remove(&"hello"));
        assert!(!map.contains(&"hello"));
    }

    #[test]
    fn remove_nonexistent_is_safe() {
        // This is the footgun protection test
        let mut map = CuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();

        // Remove something never inserted — should be safe
        assert!(!map.remove(&"world"));

        // Original item still there
        assert!(map.contains(&"hello"));
    }

    #[test]
    fn len_and_is_empty() {
        let mut map = CuckooMap::<&str>::with_capacity(100).unwrap();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        map.insert("a").unwrap();
        map.insert("b").unwrap();
        assert!(!map.is_empty());
        assert_eq!(map.len(), 2);

        map.remove(&"a");
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn into_proto_cuckoo_filter() {
        let mut map = CuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();
        map.insert("world").unwrap();

        let proto = map.to_proto();
        assert!(!proto.data.is_empty());
        assert!(proto.bucket_count > 0);
    }

    #[test]
    fn override_subscribe_request_creates_filter() {
        let mut map = CuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();

        let mut req = SubscribeRequest::default();
        map.override_subscribe_request(&mut req);

        assert!(req.accounts.contains_key("default"));
        let filter = req.accounts.get("default").unwrap();
        assert!(filter.cuckoo_accounts_filter.is_some());
    }

    #[test]
    fn override_subscribe_request_clears_existing() {
        let mut map = CuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();

        let mut req = SubscribeRequest::default();
        req.accounts.insert(
            "old_filter".to_string(),
            SubscribeRequestFilterAccounts::default(),
        );

        map.override_subscribe_request(&mut req);

        assert!(!req.accounts.contains_key("old_filter"));
        assert!(req.accounts.contains_key("default"));
    }

    #[test]
    fn pubkey_like_usage() {
        let mut map = CuckooMap::with_capacity(1000).unwrap();

        // Simulate 32-byte pubkeys
        for i in 0..100u8 {
            let pubkey = [i; 32];
            map.insert(pubkey).unwrap();
        }

        assert_eq!(map.len(), 100);

        for i in 0..100u8 {
            let pubkey = [i; 32];
            assert!(map.contains(&pubkey));
        }

        let missing = [255u8; 32];
        assert!(!map.contains(&missing));
    }

    #[test]
    fn capacity_overflow() {
        let result = CuckooMap::<u64>::with_capacity(usize::MAX);
        assert!(result.is_err());
    }
}
