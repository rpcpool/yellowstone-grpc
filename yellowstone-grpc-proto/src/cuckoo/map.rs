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
    dirty: bool,
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

        Ok(Self {
            items,
            filter,
            dirty: false,
        })
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
        self.dirty = true;
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
            self.dirty = true;
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

    /// Returns the number of items the map can hold without reallocating.
    ///
    /// Typically larger than the `max_capacity` passed to [`with_capacity`],
    /// the underlying [`HashSet`] rounds allocation up to its own sizing
    /// policy. Use this to check remaining headroom before a batch of inserts
    /// or to report occupancy alongside [`len`].
    ///
    /// ```
    /// use yellowstone_grpc_proto::cuckoo::CuckooMap;
    ///
    /// let map = CuckooMap::<u64>::with_capacity(1000).unwrap();
    /// assert!(map.capacity() >= 1000);
    /// ```
    ///
    /// [`with_capacity`]: CuckooMap::with_capacity
    /// [`len`]: CuckooMap::len
    /// [`HashSet`]: std::collections::HashSet
    pub fn capacity(&self) -> usize {
        self.items.capacity()
    }

    /// Returns an iterator over the items in the map in arbitrary order.
    ///
    /// Reads from the internal [`HashSet`], so iteration order matches
    /// `HashSet` semantics; no ordering guarantee, and two calls on the
    /// same map may yield items in different orders.
    ///
    /// ```
    /// use yellowstone_grpc_proto::cuckoo::CuckooMap;
    ///
    /// let mut map = CuckooMap::<u64>::with_capacity(100).unwrap();
    /// map.insert(1).unwrap();
    /// map.insert(2).unwrap();
    /// map.insert(3).unwrap();
    ///
    /// let sum: u64 = map.iter().sum();
    /// assert_eq!(sum, 6);
    /// ```
    ///
    /// [`HashSet`]: std::collections::HashSet
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }

    /// Returns `true` if the map contains no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns `true` if the map has been mutated since the last call to
    /// [`take_dirty`] (or since construction).
    ///
    /// Use this to check whether the filter needs to be re-sent without
    /// clearing the flag.
    pub const fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Returns the dirty flag and clears it.
    ///
    /// Call this when transmitting the filter: if it returns `true`, rebuild
    /// and send; if `false`, skip the send. Clearing means subsequent
    /// mutations will flip the flag back to `true` for the next cycle.
    ///
    /// # Example
    ///
    /// ```
    /// use yellowstone_grpc_proto::cuckoo::CuckooMap;
    ///
    /// let mut map = CuckooMap::<u64>::with_capacity(100).unwrap();
    /// assert!(!map.take_dirty());    // fresh map is clean
    ///
    /// map.insert(42).unwrap();
    /// assert!(map.take_dirty());     // mutation flipped it
    /// assert!(!map.take_dirty());    // and clearing it takes effect
    /// ```
    pub const fn take_dirty(&mut self) -> bool {
        std::mem::replace(&mut self.dirty, false)
    }

    /// Serializes the underlying cuckoo filter to its proto wire format.
    ///
    /// The returned proto carries all parameters needed for deserialization on
    /// the server side or in cross-language clients: bucket count, entries per
    /// bucket, fingerprint bits, and the hash seed.
    pub fn to_proto(&self) -> ProtoCuckooFilter {
        ProtoCuckooFilter::from(&self.filter)
    }

    /// Returns a `SubscribeRequestFilterAccounts` that carries only this cuckoo
    /// filter in essence no explicit account list, no owner, no predicates.
    ///
    /// Use this when you want to add the cuckoo filter to a subscribe request
    /// yourself, under a name of your choosing, alongside other account filters
    /// or other subscription types:
    ///
    /// ```no_run
    /// use yellowstone_grpc_proto::{cuckoo::CuckooMap, geyser::SubscribeRequest};
    ///
    /// let mut map = CuckooMap::<[u8; 32]>::with_capacity(1000).unwrap();
    /// map.insert([1u8; 32]).unwrap();
    ///
    /// let mut req = SubscribeRequest::default();
    /// req.accounts.insert("my_cuckoo".to_string(), map.to_account_filter());
    /// ```
    pub fn to_account_filter(&self) -> SubscribeRequestFilterAccounts {
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
            cuckoo_accounts_filter: Some(self.to_proto()),
        }
    }

    /// Inserts this cuckoo filter into `req.accounts` under the given name.
    ///
    /// Existing entries in `req.accounts` are preserved. If an entry already
    /// exists under `name`, it is replaced. Other fields of `req` (transactions,
    /// blocks, slots, etc.) are untouched.
    ///
    /// Marks the map as clean — subsequent mutations will flip the dirty flag
    /// back to `true` for the next transmission cycle.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use yellowstone_grpc_proto::{cuckoo::CuckooMap, geyser::SubscribeRequest};
    ///
    /// let mut map = CuckooMap::<[u8; 32]>::with_capacity(1000).unwrap();
    /// map.insert([1u8; 32]).unwrap();
    ///
    /// let mut req = SubscribeRequest::default();
    /// map.insert_into_subscribe_request(&mut req, "tracked_accounts");
    /// // req.accounts["tracked_accounts"] now carries the cuckoo filter
    /// ```
    pub fn insert_into_subscribe_request(&mut self, req: &mut SubscribeRequest, name: &str) {
        req.accounts
            .insert(name.to_string(), self.to_account_filter());
        self.dirty = false;
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
    fn to_account_filter_carries_cuckoo_and_no_other_matchers() {
        let mut map = CuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();

        let filter = map.to_account_filter();

        assert!(filter.cuckoo_accounts_filter.is_some());
        assert!(filter.account.is_empty());
        assert!(filter.owner.is_empty());
        assert!(filter.filters.is_empty());
        assert_eq!(filter.nonempty_txn_signature, None);
    }

    #[test]
    fn insert_into_subscribe_request_uses_given_name_and_preserves_other_filters() {
        let mut map = CuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();

        let mut req = SubscribeRequest::default();

        // pre-existing filter under a different name should survive
        req.accounts.insert(
            "pre_existing".to_string(),
            SubscribeRequestFilterAccounts::default(),
        );

        map.insert_into_subscribe_request(&mut req, "tracked_accounts");

        assert!(req.accounts.contains_key("tracked_accounts"));
        assert!(req.accounts.contains_key("pre_existing"));
        assert_eq!(req.accounts.len(), 2);

        let filter = req.accounts.get("tracked_accounts").unwrap();
        assert!(filter.cuckoo_accounts_filter.is_some());
    }

    #[test]
    fn insert_into_subscribe_request_clears_dirty_flag() {
        let mut map = CuckooMap::<&str>::with_capacity(100).unwrap();
        map.insert("hello").unwrap();
        assert!(map.is_dirty());

        let mut req = SubscribeRequest::default();
        map.insert_into_subscribe_request(&mut req, "default");

        assert!(!map.is_dirty());
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

    #[test]
    fn dirty_tracking() {
        let mut map = CuckooMap::<u64>::with_capacity(100).unwrap();
        assert!(!map.is_dirty());

        map.insert(1).unwrap();
        assert!(map.is_dirty());

        assert!(map.take_dirty());
        assert!(!map.is_dirty());

        map.insert(1).unwrap(); // no-op, already present
        assert!(!map.is_dirty());

        map.insert(2).unwrap();
        map.remove(&2);
        assert!(map.is_dirty());
    }
}
