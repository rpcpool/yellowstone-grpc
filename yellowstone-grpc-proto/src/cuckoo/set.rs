//! Safe, tracked wrapper around [`CuckooFilter`] for client-side filter construction.
//!
//! [`CompressedAccountFilterSet`] is the primary user-facing type for building cuckoo filters to
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
        SubscribeRequestFilterBlocks,
    },
    solana_pubkey::Pubkey,
    std::collections::HashSet,
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
/// Use [`CompressedAccountFilterSet`] whenever you need to build a cuckoo filter to send in a
/// subscribe request. It is strictly safer than constructing a [`CuckooFilter`]
/// directly — the filter's `remove` method has a footgun (it can silently remove
/// the wrong item when fingerprints collide), and [`CompressedAccountFilterSet`] guards against it.
///
/// # Example
///
/// ```
/// use {
///     solana_pubkey::Pubkey,
///     yellowstone_grpc_proto::cuckoo::CompressedAccountFilterSet,
/// };
///
/// let mut map = CompressedAccountFilterSet::with_capacity(1000).unwrap();
/// map.insert(Pubkey::new_from_array([42u8; 32])).unwrap();
/// map.insert(Pubkey::new_from_array([100u8; 32])).unwrap();
/// assert!(map.contains(Pubkey::new_from_array([42u8; 32])));
/// assert_eq!(map.len(), 2);
///
/// map.remove(Pubkey::new_from_array([42u8; 32]));
/// assert!(!map.contains(Pubkey::new_from_array([42u8; 32])));
/// ```
/// [`HashSet`]: std::collections::HashSet
pub struct CompressedAccountFilterSet {
    items: HashSet<[u8; 32]>,
    filter: CuckooFilter<[u8; 32]>,
}

impl CompressedAccountFilterSet {
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
    /// use yellowstone_grpc_proto::cuckoo::CompressedAccountFilterSet;
    ///
    /// let map = CompressedAccountFilterSet::with_capacity(10_000).unwrap();
    /// assert!(map.is_empty());
    /// ```
    ///
    /// [`HashSet`]: std::collections::HashSet
    pub fn with_capacity(max_capacity: usize) -> Result<Self, CuckooBuildError> {
        let filter = CuckooFilter::with_capacity(max_capacity)?;

        let mut items: HashSet<[u8; 32]> = HashSet::new();
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
    pub fn insert(&mut self, key: Pubkey) -> Result<bool, TableFullError> {
        let bytes = key.to_bytes();

        if self.items.contains(&bytes) {
            return Ok(false);
        }
        self.filter.insert(&bytes)?;
        self.items.insert(bytes);
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
    /// same fingerprint. [`CompressedAccountFilterSet`] prevents this by checking its internal
    /// [`HashSet`] first and only touching the filter when the item is genuinely
    /// present.
    ///
    /// [`HashSet`]: std::collections::HashSet
    pub fn remove(&mut self, key: Pubkey) -> bool {
        let bytes = key.to_bytes();

        if self.items.remove(&bytes) {
            self.filter.remove(&bytes);
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
    pub fn contains(&self, key: Pubkey) -> bool {
        self.items.contains(&key.to_bytes())
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
    /// use yellowstone_grpc_proto::cuckoo::CompressedAccountFilterSet;
    ///
    /// let map = CompressedAccountFilterSet::with_capacity(1000).unwrap();
    /// assert!(map.capacity() >= 1000);
    /// ```
    ///
    /// [`with_capacity`]: CompressedAccountFilterSet::with_capacity
    /// [`len`]: CompressedAccountFilterSet::len
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
    /// use {
    ///     solana_pubkey::Pubkey,
    ///     yellowstone_grpc_proto::cuckoo::CompressedAccountFilterSet,
    /// };
    ///
    /// let mut map = CompressedAccountFilterSet::with_capacity(100).unwrap();
    /// map.insert(Pubkey::new_from_array([1u8; 32])).unwrap();
    /// map.insert(Pubkey::new_from_array([2u8; 32])).unwrap();
    /// map.insert(Pubkey::new_from_array([3u8; 32])).unwrap();
    ///
    /// let count = map.iter().count();
    /// assert_eq!(count, 3);
    /// ```
    ///
    /// [`HashSet`]: std::collections::HashSet
    pub fn iter(&self) -> impl Iterator<Item = &[u8; 32]> {
        self.items.iter()
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

    /// Returns a `SubscribeRequestFilterAccounts` that carries only this cuckoo
    /// filter in essence no explicit account list, no owner, no predicates.
    ///
    /// Use this when you want to add the cuckoo filter to a subscribe request
    /// yourself, under a name of your choosing, alongside other account filters
    /// or other subscription types:
    ///
    /// ```no_run
    /// use {
    ///     solana_pubkey::Pubkey,
    ///     yellowstone_grpc_proto::{cuckoo::CompressedAccountFilterSet, geyser::SubscribeRequest},
    /// };
    ///
    /// let mut map = CompressedAccountFilterSet::with_capacity(1000).unwrap();
    /// map.insert(Pubkey::new_from_array([1u8; 32])).unwrap();
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

    /// Returns a [`SubscribeRequestFilterBlocks`] that carries only this cuckoo
    /// filter, no explicit account include list, no transaction or entry options set.
    ///
    /// Use this when you want to subscribe to full blocks but filter the
    /// transactions and accounts inside them by the cuckoo set:
    ///
    /// ```no_run
    /// use {
    ///     solana_pubkey::Pubkey,
    ///     yellowstone_grpc_proto::{cuckoo::CompressedAccountFilterSet, geyser::SubscribeRequest},
    /// };
    ///
    /// let mut map = CompressedAccountFilterSet::with_capacity(1000).unwrap();
    /// map.insert(Pubkey::new_from_array([1u8; 32])).unwrap();
    ///
    /// let mut req = SubscribeRequest::default();
    /// req.blocks.insert("blocks".to_string(), map.to_block_filter());
    /// ```
    pub fn to_block_filter(&self) -> SubscribeRequestFilterBlocks {
        SubscribeRequestFilterBlocks {
            account_include: vec![],
            include_transactions: None,
            include_accounts: None,
            include_entries: None,
            cuckoo_account_include: Some(self.to_proto()),
        }
    }

    /// Inserts this cuckoo filter into `req.accounts` under the given name.
    ///
    /// Existing entries in `req.accounts` are preserved. If an entry already
    /// exists under `name`, it is replaced. Other fields of `req` (transactions,
    /// blocks, slots, etc.) are untouched.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use {
    ///     solana_pubkey::Pubkey,
    ///     yellowstone_grpc_proto::{cuckoo::CompressedAccountFilterSet, geyser::SubscribeRequest},
    /// };
    ///
    /// let mut map = CompressedAccountFilterSet::with_capacity(1000).unwrap();
    /// map.insert(Pubkey::new_from_array([1u8; 32])).unwrap();
    ///
    /// let mut req = SubscribeRequest::default();
    /// map.insert_into_subscribe_request(&mut req, "tracked_accounts");
    /// // req.accounts["tracked_accounts"] now carries the cuckoo filter
    /// ```
    pub fn insert_into_subscribe_request(&self, req: &mut SubscribeRequest, name: &str) {
        req.accounts
            .insert(name.to_string(), self.to_account_filter());
    }

    /// Inserts this cuckoo filter into `req.blocks` under the given name.
    ///
    /// Existing entries in `req.blocks` are preserved. If an entry already
    /// exists under `name`, it is replaced. Other fields of `req` are untouched.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use {
    ///     solana_pubkey::Pubkey,
    ///     yellowstone_grpc_proto::{cuckoo::CompressedAccountFilterSet, geyser::SubscribeRequest},
    /// };
    ///
    /// let mut map = CompressedAccountFilterSet::with_capacity(1000).unwrap();
    /// map.insert(Pubkey::new_from_array([1u8; 32])).unwrap();
    ///
    /// let mut req = SubscribeRequest::default();
    /// map.insert_into_block_subscribe_request(&mut req, "tracked_blocks");
    /// ```
    pub fn insert_into_block_subscribe_request(&self, req: &mut SubscribeRequest, name: &str) {
        req.blocks.insert(name.to_string(), self.to_block_filter());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // helper: a 32-byte key from a single seed byte
    fn key(b: u8) -> Pubkey {
        Pubkey::new_from_array([b; 32])
    }

    #[test]
    fn basic_insert_contains() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        assert!(filter.insert(key(1)).unwrap());
        assert!(filter.contains(key(1)));
        assert!(!filter.contains(key(2)));
    }

    #[test]
    fn insert_duplicate_returns_false() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        assert!(filter.insert(key(1)).unwrap());
        assert!(!filter.insert(key(1)).unwrap());
    }

    #[test]
    fn remove_existing() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        filter.insert(key(1)).unwrap();
        assert!(filter.remove(key(1)));
        assert!(!filter.contains(key(1)));
    }

    #[test]
    fn remove_nonexistent_is_safe() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        filter.insert(key(1)).unwrap();

        assert!(!filter.remove(key(2)));
        assert!(filter.contains(key(1)));
    }

    #[test]
    fn len_and_is_empty() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        assert!(filter.is_empty());
        assert_eq!(filter.len(), 0);

        filter.insert(key(1)).unwrap();
        filter.insert(key(2)).unwrap();
        assert!(!filter.is_empty());
        assert_eq!(filter.len(), 2);

        filter.remove(key(1));
        assert_eq!(filter.len(), 1);
    }

    #[test]
    fn to_proto_round_trip() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        filter.insert(key(1)).unwrap();
        filter.insert(key(2)).unwrap();

        let proto = filter.to_proto();
        assert!(!proto.data.is_empty());
        assert!(proto.bucket_count > 0);
    }

    #[test]
    fn to_account_filter_carries_cuckoo_and_no_other_matchers() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        filter.insert(key(1)).unwrap();

        let f = filter.to_account_filter();

        assert!(f.cuckoo_accounts_filter.is_some());
        assert!(f.account.is_empty());
        assert!(f.owner.is_empty());
        assert!(f.filters.is_empty());
        assert_eq!(f.nonempty_txn_signature, None);
    }

    #[test]
    fn insert_into_subscribe_request_uses_given_name_and_preserves_other_filters() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        filter.insert(key(1)).unwrap();

        let mut req = SubscribeRequest::default();
        req.accounts.insert(
            "pre_existing".to_string(),
            SubscribeRequestFilterAccounts::default(),
        );

        filter.insert_into_subscribe_request(&mut req, "tracked_accounts");

        assert!(req.accounts.contains_key("tracked_accounts"));
        assert!(req.accounts.contains_key("pre_existing"));
        assert_eq!(req.accounts.len(), 2);
        assert!(req
            .accounts
            .get("tracked_accounts")
            .unwrap()
            .cuckoo_accounts_filter
            .is_some());
    }

    #[test]
    fn to_block_filter_carries_cuckoo_and_no_other_matchers() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        filter.insert(key(1)).unwrap();

        let f = filter.to_block_filter();

        assert!(f.cuckoo_account_include.is_some());
        assert!(f.account_include.is_empty());
        assert_eq!(f.include_transactions, None);
        assert_eq!(f.include_accounts, None);
        assert_eq!(f.include_entries, None);
    }

    #[test]
    fn insert_into_block_subscribe_request_uses_given_name_and_preserves_other_filters() {
        let mut filter = CompressedAccountFilterSet::with_capacity(100).unwrap();
        filter.insert(key(1)).unwrap();

        let mut req = SubscribeRequest::default();
        req.blocks.insert(
            "pre_existing".to_string(),
            SubscribeRequestFilterBlocks::default(),
        );

        filter.insert_into_block_subscribe_request(&mut req, "tracked_blocks");

        assert!(req.blocks.contains_key("tracked_blocks"));
        assert!(req.blocks.contains_key("pre_existing"));
        assert_eq!(req.blocks.len(), 2);
        assert!(req
            .blocks
            .get("tracked_blocks")
            .unwrap()
            .cuckoo_account_include
            .is_some());
    }

    #[test]
    fn pubkey_like_usage() {
        let mut filter = CompressedAccountFilterSet::with_capacity(1000).unwrap();

        for i in 0..100u8 {
            filter.insert(key(i)).unwrap();
        }

        assert_eq!(filter.len(), 100);

        for i in 0..100u8 {
            assert!(filter.contains(key(i)));
        }

        assert!(!filter.contains(key(255)));
    }

    #[test]
    fn capacity_overflow() {
        let result = CompressedAccountFilterSet::with_capacity(usize::MAX);
        assert!(matches!(result, Err(CuckooBuildError::CapacityOverflow)));
    }
}
