use std::collections::HashSet;
use std::hash::Hash;
use yellowstone_grpc_proto::cuckoo::{CuckooFilter, CuckooError};
use yellowstone_grpc_proto::geyser::{
    CuckooFilter as ProtoCuckooFilter,
    SubscribeRequest, SubscribeRequestFilterAccounts,
};

/// A HashMap-like wrapper around `CuckooFilter` for safe client-side usage.
///
/// Maintains both a `HashSet` (source of truth) and a `CuckooFilter` (for wire transmission).
/// The `HashSet` ensures exact membership checks and guards against unsafe removes.
pub struct LocalCuckooMap<T> {
    items: HashSet<T>,
    filter: CuckooFilter,
    dirty: bool,
}

impl<T> LocalCuckooMap<T>
where
    T: Sized + Hash + Eq,
{
    /// Creates a new map sized to hold `max_capacity` items.
    ///
    /// # Errors
    ///
    /// Returns `CuckooError::CapacityOverflow` if capacity exceeds system limits.
    pub fn with_capacity(max_capacity: usize) -> Result<Self, CuckooError> {
        let filter = CuckooFilter::with_capacity(max_capacity)?;
        
        let mut items: HashSet<T> = HashSet::new();
        items.try_reserve(max_capacity).map_err(|_| CuckooError::CapacityOverflow)?;
        
        Ok(Self {
            items,
            filter,
            dirty: false,
        })
    }

    /// Inserts an item into the map.
    ///
    /// Returns `Ok(true)` if the item was newly inserted, `Ok(false)` if it already existed.
    ///
    /// # Errors
    ///
    /// - `CuckooError::TableFull`: Filter cannot accommodate more items
    /// - `CuckooError::InvalidState`: Filter is corrupted
    pub fn insert(&mut self, v: T) -> Result<bool, CuckooError> {
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
    /// Returns `Ok(true)` if the item was removed, `Ok(false)` if it didn't exist.
    ///
    /// # Safety Guarantee
    ///
    /// Unlike raw `CuckooFilter::remove`, this method is safe to call with
    /// any item even items never inserted. The internal `HashSet` acts as
    /// a guard: the filter is only modified if the item actually exists.
    ///
    /// This prevents the cuckoo filter footgun where removing a non-existent
    /// item could accidentally remove a different item sharing the same fingerprint.
    ///
    /// # Errors
    ///
    /// Returns `CuckooError::InvalidState` if the filter is corrupted.
    pub fn remove(&mut self, v: &T) -> Result<bool, CuckooError> {
        if self.items.remove(v) {
            self.filter.remove(v)?;
            self.dirty = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Checks if an item exists in the map.
    ///
    /// Uses the internal `HashSet` for exact matching — no false positives.
    /// This differs from `CuckooFilter::contains` which has a small false positive rate.
    pub fn contains(&self, v: &T) -> bool {
        self.items.contains(v)  // exact, uses HashSet — no error possible
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Serializes the filter to proto format for wire transmission.
    ///
    /// The proto contains all parameters needed for cross-language deserialization.
    pub fn into_proto_cuckoo_filter(&self) -> ProtoCuckooFilter {
        ProtoCuckooFilter::from(&self.filter)
    }

    /// Writes the cuckoo filter into the subscribe request.
    ///
    /// Creates a default account filter with the cuckoo filter set.
    /// Clears any existing account filters, the cuckoo filter replaces them.
    pub fn override_subscribe_request(&self, req: &mut SubscribeRequest) {
        let filter = SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
            cuckoo_filter: Some(self.into_proto_cuckoo_filter()),
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
        let mut map = LocalCuckooMap::with_capacity(100).unwrap();
        assert!(map.insert("hello").unwrap());
        assert!(map.contains(&"hello"));
        assert!(!map.contains(&"world"));
    }

    #[test]
    fn insert_duplicate_returns_false() {
        let mut map = LocalCuckooMap::with_capacity(100).unwrap();
        assert!(map.insert("hello").unwrap());
        assert!(!map.insert("hello").unwrap());  // already exists
    }

    #[test]
    fn remove_existing() {
        let mut map = LocalCuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();
        assert!(map.remove(&"hello").unwrap());
        assert!(!map.contains(&"hello"));
    }

    #[test]
    fn remove_nonexistent_is_safe() {
        // This is the footgun protection test
        let mut map = LocalCuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();
        
        // Remove something never inserted — should be safe
        assert!(!map.remove(&"world").unwrap());
        
        // Original item still there
        assert!(map.contains(&"hello"));
    }

    #[test]
    fn len_and_is_empty() {
        let mut map = LocalCuckooMap::<&str>::with_capacity(100).unwrap();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
        
        map.insert("a").unwrap();
        map.insert("b").unwrap();
        assert!(!map.is_empty());
        assert_eq!(map.len(), 2);
        
        map.remove(&"a").unwrap();
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn into_proto_cuckoo_filter() {
        let mut map = LocalCuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();
        map.insert("world").unwrap();
        
        let proto = map.into_proto_cuckoo_filter();
        assert!(!proto.data.is_empty());
        assert!(proto.bucket_count > 0);
    }

    #[test]
    fn override_subscribe_request_creates_filter() {
        let mut map = LocalCuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();
        
        let mut req = SubscribeRequest::default();
        map.override_subscribe_request(&mut req);
        
        assert!(req.accounts.contains_key("default"));
        let filter = req.accounts.get("default").unwrap();
        assert!(filter.cuckoo_filter.is_some());
    }

    #[test]
    fn override_subscribe_request_clears_existing() {
        let mut map = LocalCuckooMap::with_capacity(100).unwrap();
        map.insert("hello").unwrap();
        
        let mut req = SubscribeRequest::default();
        req.accounts.insert("old_filter".to_string(), SubscribeRequestFilterAccounts::default());
        
        map.override_subscribe_request(&mut req);
        
        assert!(!req.accounts.contains_key("old_filter"));
        assert!(req.accounts.contains_key("default"));
    }

    #[test]
    fn pubkey_like_usage() {
        let mut map = LocalCuckooMap::with_capacity(1000).unwrap();
        
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
        let result = LocalCuckooMap::<u64>::with_capacity(usize::MAX);
        assert!(result.is_err());
    }
}