//! Cuckoo filter implementation for probabilistic set membership.
//!
//! A cuckoo filter is a space-efficient data structure that supports:
//! - `insert`: Add an item to the set
//! - `contains`: Check if an item is probably in the set
//! - `remove`: Remove an item from the set
//!
//! Key properties:
//! - **No false negatives**: If `contains` returns `false`, the item is definitely not in the set
//! - **Possible false positives**: If `contains` returns `true`, the item is probably in the set (~0.01% error rate)
//! - **Space efficient**: 2M items compress to ~2-3MB (vs 64MB for explicit list)
//!
//! # Example
//!
//! ```ignore
//! use yellowstone_grpc_proto::cuckoo::CuckooFilter;
//!
//! let mut filter = CuckooFilter::with_capacity(1000)?;
//! filter.insert(&"hello")?;
//! assert!(filter.contains(&"hello")?);
//! assert!(!filter.contains(&"world")?);
//! filter.remove(&"hello")?;
//! assert!(!filter.contains(&"hello")?);
//! ```
//!
//! # Wire Format
//!
//! The filter serializes to a `CuckooFilter` proto message containing:
//! - `data`: Raw bucket bytes
//! - `bucket_count`: Number of buckets (power of 2)
//! - `entries_per_bucket`: Slots per bucket (4)
//! - `fingerprint_bits`: Bits per fingerprint (16)
//! - `hash_seed`: Seed for deterministic hashing
//!
//! This enables cross-language deserialization.

use std::hash::{DefaultHasher, Hash, Hasher};
use crate::geyser::CuckooFilter as ProtoCuckooFilter;

/// Slots per bucket.
const ENTRIES_PER_BUCKET: usize = 4;

/// Target load factor. 95% occupancy is achievable with 4 entries/bucket.
const LOAD_FACTOR: f64 = 0.95;

/// Maximum relocations before declaring table full.
const MAX_KICKS: usize = 500;

/// Fingerprint size in bits. 16 bits gives ~0.0001% false positive rate.
const FINGERPRINT_BITS: u32 = 16;

/// Fingerprint type. Must match FINGERPRINT_BITS.
type Fingerprint = u16; // must match FINGERPRINT_BITS

/// A bucket holds ENTRIES_PER_BUCKET fingerprints. Value 0 means empty slot.
type Bucket = [Fingerprint; ENTRIES_PER_BUCKET];

/// Hash seed for deterministic behavior. ASCII: "yllwstn!"
const HASH_SEED: u64 = 0x_796c_6c77_7374_6e21;

/// A space-efficient probabilistic set membership filter.
///
/// Stores fingerprints (short hashes) of items rather than items themselves.
/// Supports insert, lookup, and delete operations.
#[derive(Debug)]
pub struct CuckooFilter {
    buckets: Vec<Bucket>,
}

#[derive(Debug)]
pub enum CuckooError {
    CapacityOverflow,
    TableFull,
    InvalidState,  // corrupted or empty buckets
}


impl CuckooFilter {
    /// Creates a new filter sized to hold `max_capacity` items.
    ///
    /// The actual bucket count is rounded up to a power of 2 and adjusted
    /// for the target load factor (~95% occupancy).
    ///
    /// # Errors
    ///
    /// Returns `CuckooError::CapacityOverflow` if the requested capacity
    /// exceeds system memory limits.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let filter = CuckooFilter::with_capacity(2_000_000)?;
    /// `
    pub fn with_capacity(max_capacity: usize) -> Result<Self, CuckooError> {
        let buckets_needed = (max_capacity as f64 / (LOAD_FACTOR * ENTRIES_PER_BUCKET as f64)).ceil() as usize;
        
        let bucket_count = buckets_needed
            .checked_next_power_of_two()
            .ok_or(CuckooError::CapacityOverflow)?
            .max(1);

        let mut buckets = Vec::new();
        buckets
            .try_reserve_exact(bucket_count)
            .map_err(|_| CuckooError::CapacityOverflow)?;
        
        buckets.resize(bucket_count, [0; ENTRIES_PER_BUCKET]);

        Ok(Self { buckets })
    }

    /// Inserts an item into the filter.
    ///
    /// Uses partial-key cuckoo hashing: if both candidate buckets are full,
    /// existing fingerprints are relocated to make room.
    ///
    /// # Errors
    ///
    /// - `CuckooError::InvalidState`: Filter has no buckets (corrupted state)
    /// - `CuckooError::TableFull`: Could not insert after MAX_KICKS relocations
    pub fn insert<T: Hash>(&mut self, item: &T) -> Result<(), CuckooError> {
        if self.buckets.is_empty() {
            return Err(CuckooError::InvalidState);
        }

        let fp = Self::fingerprint(item);
        let h = Self::hash(item);
        let i1 = self.index(h);
        let i2 = i1 ^ self.index(Self::hash(&fp));

        if self.try_insert_to_bucket(i1, fp) {
            return Ok(());
        }
        
        if self.try_insert_to_bucket(i2, fp) {
            return Ok(());
        }

        let mut i = i1;
        let mut fp = fp;

        for n in 0..MAX_KICKS {
            let slot = (n + fp as usize) % ENTRIES_PER_BUCKET;
            std::mem::swap(&mut fp, &mut self.buckets[i][slot]);
            
            i = i ^ self.index(Self::hash(&fp));
            
            if self.try_insert_to_bucket(i, fp) {
                return Ok(());
            }
        }

        Err(CuckooError::TableFull)
    }

    /// Checks if an item is probably in the filter.
    ///
    /// - Returns `Ok(false)`: Item is definitely NOT in the set
    /// - Returns `Ok(true)`: Item is probably in the set (small false positive chance)
    ///
    /// # Errors
    ///
    /// Returns `CuckooError::InvalidState` if filter has no buckets.
    pub fn contains<T: Hash>(&self, item: &T) -> Result<bool, CuckooError> {
        if self.buckets.is_empty() {
            return Err(CuckooError::InvalidState);
        }

        let fp = Self::fingerprint(item);
        let h = Self::hash(item);
        let i1 = self.index(h);
        let i2 = i1 ^ self.index(Self::hash(&fp));

        Ok(self.bucket_contains(i1, fp) || self.bucket_contains(i2, fp))
    }

    /// Removes an item from the filter.
    ///
    /// Returns `Ok(true)` if a matching fingerprint was found and removed,
    /// `Ok(false)` if no matching fingerprint existed.
    ///
    /// # Warning
    ///
    /// Only remove items that were previously inserted. Removing a non-inserted
    /// item may accidentally remove a different item with the same fingerprint.
    ///
    /// # Errors
    ///
    /// Returns `CuckooError::InvalidState` if filter has no buckets.
    pub fn remove<T: Hash>(&mut self, item: &T) -> Result<bool, CuckooError> {
        if self.buckets.is_empty() {
            return Err(CuckooError::InvalidState);
        }

        let fp = Self::fingerprint(item);
        let h = Self::hash(item);
        let i1 = self.index(h);
        let i2 = i1 ^ self.index(Self::hash(&fp));

        Ok(self.try_remove_from_bucket(i1, fp) || self.try_remove_from_bucket(i2, fp))
    }

    /// Hashes an item using the seeded hasher.
    fn hash<T: Hash>(item: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        HASH_SEED.hash(&mut hasher);
        item.hash(&mut hasher);
        hasher.finish()
    }

    /// Extracts a fingerprint from an item's hash.
    /// Returns upper 16 bits, ensuring non-zero (0 = empty slot).
    fn fingerprint<T: Hash>(item: &T) -> Fingerprint {
        let h = Self::hash(item);
        let fp = (h >> 32) as u16;
        if fp == 0 { 1 } else { fp }
    }

    /// Maps a hash to a bucket index using bitmask (why bucket_count is power of 2).
    fn index(&self, hash: u64) -> usize {
        hash as usize & (self.buckets.len() - 1)
    }

    /// Tries to insert fingerprint into an empty slot in the bucket.
    fn try_insert_to_bucket(&mut self, index: usize, fp: Fingerprint) -> bool {
        for slot in &mut self.buckets[index] {
            if *slot == 0 {
                *slot = fp;
                return true;
            }
        }
        false
    }

    /// Tries to remove one copy of fingerprint from the bucket.
    fn try_remove_from_bucket(&mut self, index: usize, fp: Fingerprint) -> bool {
        for slot in &mut self.buckets[index] {
            if *slot == fp {
                *slot = 0;
                return true;
            }
        }
        false
    }

    /// Checks if bucket contains the fingerprint.
    fn bucket_contains(&self, index: usize, fp: Fingerprint) -> bool {
        self.buckets[index].iter().any(|&x| x == fp)
    }
}

/// Deserializes from proto wire format.
///
/// Handles malformed input gracefully:
/// - Empty data -> single empty bucket
/// - Odd bytes -> truncated (via chunks_exact)
/// - Misaligned data -> incomplete buckets dropped
impl From<&ProtoCuckooFilter> for CuckooFilter {
    fn from(proto: &ProtoCuckooFilter) -> Self {
        if proto.data.is_empty() {
            return Self { buckets: vec![[0; ENTRIES_PER_BUCKET]; 1] };
        }

        let buckets: Vec<Bucket> = proto.data
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<u16>>()
            .chunks_exact(ENTRIES_PER_BUCKET)
            .map(|chunk| [chunk[0], chunk[1], chunk[2], chunk[3]])
            .collect();

        if buckets.is_empty() {
            return Self { buckets: vec![[0; ENTRIES_PER_BUCKET]; 1] };
        }

        Self { buckets }
    }
}

/// Serializes to proto wire format for cross-language interop.
impl From<&CuckooFilter> for ProtoCuckooFilter {
    fn from(filter: &CuckooFilter) -> Self {
        let data: Vec<u8> = filter.buckets
            .iter()
            .flat_map(|bucket| bucket.iter())
            .flat_map(|fp| fp.to_le_bytes())
            .collect();

        Self {
            data,
            bucket_count: filter.buckets.len() as u32,
            entries_per_bucket: ENTRIES_PER_BUCKET as u32,
            fingerprint_bits: FINGERPRINT_BITS,
            hash_seed: HASH_SEED,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_contains_nothing() {
        let filter = CuckooFilter::with_capacity(100).unwrap();
        assert!(!filter.contains(&"hello").unwrap());
        assert!(!filter.contains(&42u64).unwrap());
    }

    #[test]
    fn insert_then_contains() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        assert!(filter.insert(&"hello").is_ok());
        assert!(filter.contains(&"hello").unwrap());
        assert!(!filter.contains(&"world").unwrap());
    }

    #[test]
    fn remove_then_not_contains() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        filter.insert(&"hello").unwrap();
        assert!(filter.contains(&"hello").unwrap());
        assert!(filter.remove(&"hello").unwrap());
        assert!(!filter.contains(&"hello").unwrap());
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        assert!(!filter.remove(&"hello").unwrap());
    }

    #[test]
    fn capacity_zero() {
        let filter = CuckooFilter::with_capacity(0).unwrap();
        assert!(!filter.buckets.is_empty());
    }

    #[test]
    fn capacity_one() {
        let mut filter = CuckooFilter::with_capacity(1).unwrap();
        assert!(filter.insert(&"only").is_ok());
        assert!(filter.contains(&"only").unwrap());
    }

    #[test]
    fn many_inserts() {
        let mut filter = CuckooFilter::with_capacity(1000).unwrap();
        for i in 0..1000u64 {
            let _ = filter.insert(&i);
        }
        assert!(filter.contains(&0u64).unwrap());
        assert!(filter.contains(&500u64).unwrap());
        assert!(filter.contains(&999u64).unwrap());
        assert!(!filter.contains(&1000u64).unwrap());
    }

    #[test]
    fn insert_returns_error_when_full() {
        let mut filter = CuckooFilter::with_capacity(10).unwrap();
        let mut failures = 0;
        for i in 0..1000u64 {
            if filter.insert(&i).is_err() {
                failures += 1;
            }
        }
        assert!(failures > 0, "filter should reject some inserts when full");
    }

    #[test]
    fn proto_roundtrip() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        filter.insert(&"hello").unwrap();
        filter.insert(&"world").unwrap();
        filter.insert(&42u64).unwrap();

        let proto = ProtoCuckooFilter::from(&filter);
        let restored = CuckooFilter::from(&proto);

        assert!(restored.contains(&"hello").unwrap());
        assert!(restored.contains(&"world").unwrap());
        assert!(restored.contains(&42u64).unwrap());
        assert!(!restored.contains(&"missing").unwrap());
    }

    #[test]
    fn bucket_count_is_power_of_two() {
        for cap in [1, 10, 100, 1000, 1337, 10000] {
            let filter = CuckooFilter::with_capacity(cap).unwrap();
            let len = filter.buckets.len();
            assert!(len.is_power_of_two(), "capacity {} gave {} buckets", cap, len);
        }
    }

    #[test]
    fn capacity_usize_max() {
        let result = CuckooFilter::with_capacity(usize::MAX);
        assert!(result.is_err());
    }

    #[test]
    fn capacity_usize_max_minus_one() {
        let result = CuckooFilter::with_capacity(usize::MAX - 1);
        assert!(result.is_err());
    }

    #[test]
    fn insert_same_item_thousand_times() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        for _ in 0..1000 {
            let _ = filter.insert(&"same");
        }
        assert!(filter.contains(&"same").unwrap());
    }

    #[test]
    fn insert_after_remove_same_item() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        filter.insert(&"hello").unwrap();
        filter.remove(&"hello").unwrap();
        assert!(filter.insert(&"hello").is_ok());
        assert!(filter.contains(&"hello").unwrap());
    }

    #[test]
    fn remove_same_item_twice() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        filter.insert(&"hello").unwrap();
        assert!(filter.remove(&"hello").unwrap());
        assert!(!filter.remove(&"hello").unwrap());
    }

    #[test]
    fn proto_empty_data() {
        let proto = ProtoCuckooFilter {
            data: vec![],
            bucket_count: 0,
            entries_per_bucket: 4,
            fingerprint_bits: 16,
            hash_seed: HASH_SEED,
        };
        let filter = CuckooFilter::from(&proto);
        assert!(!filter.contains(&"anything").unwrap());
    }

    #[test]
    fn proto_odd_bytes() {
        let proto = ProtoCuckooFilter {
            data: vec![1, 2, 3],
            bucket_count: 1,
            entries_per_bucket: 4,
            fingerprint_bits: 16,
            hash_seed: HASH_SEED,
        };
        let filter = CuckooFilter::from(&proto);
        // should not panic, truncates odd byte
        let _ = filter.contains(&"test");
    }

    #[test]
    fn proto_not_aligned_to_bucket() {
        let proto = ProtoCuckooFilter {
            data: vec![1, 0, 2, 0],
            bucket_count: 1,
            entries_per_bucket: 4,
            fingerprint_bits: 16,
            hash_seed: HASH_SEED,
        };
        let filter = CuckooFilter::from(&proto);
        let _ = filter.contains(&"test");
    }

    #[test]
    fn proto_lies_about_bucket_count() {
        let proto = ProtoCuckooFilter {
            data: vec![0; 8],
            bucket_count: 10,
            entries_per_bucket: 4,
            fingerprint_bits: 16,
            hash_seed: HASH_SEED,
        };
        let filter = CuckooFilter::from(&proto);
        let _ = filter.contains(&"test");
    }

    #[test]
    fn contains_on_empty_buckets() {
        let filter = CuckooFilter { buckets: vec![] };
        assert!(filter.contains(&"test").is_err());
    }

    #[test]
    fn insert_on_empty_buckets() {
        let mut filter = CuckooFilter { buckets: vec![] };
        assert!(filter.insert(&"test").is_err());
    }

    #[test]
    fn remove_on_empty_buckets() {
        let mut filter = CuckooFilter { buckets: vec![] };
        assert!(filter.remove(&"test").is_err());
    }

    #[test]
    fn items_with_zero_hash() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        filter.insert(&0u64).unwrap();
        assert!(filter.contains(&0u64).unwrap());
    }

    #[test]
    fn items_with_max_hash() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        filter.insert(&u64::MAX).unwrap();
        assert!(filter.contains(&u64::MAX).unwrap());
    }

    #[test]
    fn empty_string() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        filter.insert(&"").unwrap();
        assert!(filter.contains(&"").unwrap());
    }

    #[test]
    fn very_long_string() {
        let long = "a".repeat(1_000_000);
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        filter.insert(&long).unwrap();
        assert!(filter.contains(&long).unwrap());
    }

    #[test]
    fn different_types_same_value() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        filter.insert(&42u32).unwrap();
        let contains_u64 = filter.contains(&42u64).unwrap();
        println!("42u32 inserted, contains 42u64: {}", contains_u64);
    }

    #[test]
    fn fill_then_remove_then_fill() {
        let mut filter = CuckooFilter::with_capacity(50).unwrap();
        
        for i in 0..50u64 {
            let _ = filter.insert(&i);
        }
        
        for i in 0..50u64 {
            let _ = filter.remove(&i);
        }
        
        for i in 100..150u64 {
            assert!(filter.insert(&i).is_ok(), "failed to insert {} after remove cycle", i);
        }
    }

    #[test]
    fn false_positive_rate() {
        let n = 10_000;
        let mut filter = CuckooFilter::with_capacity(n).unwrap();
        
        for i in 0..n as u64 {
            let _ = filter.insert(&i);
        }
        
        let mut false_positives = 0;
        let test_count = 100_000;
        for i in n as u64..(n as u64 + test_count) {
            if filter.contains(&i).unwrap() {
                false_positives += 1;
            }
        }
        
        let fp_rate = false_positives as f64 / test_count as f64;
        println!("False positive rate: {:.4}%", fp_rate * 100.0);
        
        assert!(fp_rate < 0.01, "false positive rate too high: {:.4}%", fp_rate * 100.0);
    }

    #[test]
    fn pubkey_like_data() {
        // 32-byte arrays like Solana pubkeys
        let mut filter = CuckooFilter::with_capacity(1000).unwrap();
        
        for i in 0..100u8 {
            let pubkey = [i; 32];
            filter.insert(&pubkey).unwrap();
        }
        
        for i in 0..100u8 {
            let pubkey = [i; 32];
            assert!(filter.contains(&pubkey).unwrap());
        }
        
        // not inserted
        let missing = [255u8; 32];
        assert!(!filter.contains(&missing).unwrap());
    }

    #[test]
    fn similar_pubkeys() {
        // pubkeys that differ by one byte
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        
        let mut pk1 = [0u8; 32];
        let mut pk2 = [0u8; 32];
        pk2[31] = 1;  // differ only in last byte
        
        filter.insert(&pk1).unwrap();
        
        assert!(filter.contains(&pk1).unwrap());
        assert!(!filter.contains(&pk2).unwrap());  // should NOT match
    }

    // determinism

    #[test]
    fn deterministic_behavior() {
        // same inputs should produce same filter state
        let mut filter1 = CuckooFilter::with_capacity(100).unwrap();
        let mut filter2 = CuckooFilter::with_capacity(100).unwrap();
        
        for i in 0..50u64 {
            filter1.insert(&i).unwrap();
            filter2.insert(&i).unwrap();
        }
        
        let proto1 = ProtoCuckooFilter::from(&filter1);
        let proto2 = ProtoCuckooFilter::from(&filter2);
        
        assert_eq!(proto1.data, proto2.data);
    }

    #[test]
    fn proto_roundtrip_preserves_state() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        for i in 0..50u64 {
            filter.insert(&i).unwrap();
        }
        
        // roundtrip multiple times
        let proto1 = ProtoCuckooFilter::from(&filter);
        let restored1 = CuckooFilter::from(&proto1);
        let proto2 = ProtoCuckooFilter::from(&restored1);
        let restored2 = CuckooFilter::from(&proto2);
        
        // should be identical
        assert_eq!(proto1.data, proto2.data);
        
        // should still work
        for i in 0..50u64 {
            assert!(restored2.contains(&i).unwrap());
        }
    }

    // at scale

    #[test]
    fn hundred_thousand_items() {
        let n = 100_000;
        let mut filter = CuckooFilter::with_capacity(n).unwrap();
        
        let mut inserted = 0;
        for i in 0..n as u64 {
            if filter.insert(&i).is_ok() {
                inserted += 1;
            }
        }
        
        println!("Inserted {} / {} items", inserted, n);
        assert!(inserted > n * 90 / 100, "should insert at least 90%");
    }

    #[test]
    fn proto_size_at_scale() {
        let n = 100_000;
        let mut filter = CuckooFilter::with_capacity(n).unwrap();
        
        for i in 0..n as u64 {
            let _ = filter.insert(&i);
        }
        
        let proto = ProtoCuckooFilter::from(&filter);
        let size_bytes = proto.data.len();
        let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
        
        println!("Filter size for {} items: {} bytes ({:.2} MB)", n, size_bytes, size_mb);
        
        // should be much smaller than raw pubkeys
        // n pubkeys * 32 bytes = 3.2 MB
        // filter should be < 1 MB
        assert!(size_bytes < 1024 * 1024, "filter too large");
    }

    // interleaved ops

    #[test]
    fn interleaved_insert_remove_contains() {
        let mut filter = CuckooFilter::with_capacity(100).unwrap();
        
        // insert 1-50
        for i in 1..=50u64 {
            filter.insert(&i).unwrap();
        }
        
        // remove even numbers
        for i in (2..=50u64).step_by(2) {
            filter.remove(&i).unwrap();
        }
        
        // insert 51-75
        for i in 51..=75u64 {
            filter.insert(&i).unwrap();
        }
        
        // check odd 1-50 still there
        for i in (1..=50u64).step_by(2) {
            assert!(filter.contains(&i).unwrap(), "{} should exist", i);
        }
        
        // check even 1-50 gone
        for i in (2..=50u64).step_by(2) {
            assert!(!filter.contains(&i).unwrap(), "{} should be gone", i);
        }
        
        // check 51-75 there
        for i in 51..=75u64 {
            assert!(filter.contains(&i).unwrap(), "{} should exist", i);
        }
    }

    // boundary values

    #[test]
    fn insert_at_exact_capacity() {
        let cap = 64;  // small, power of 2
        let mut filter = CuckooFilter::with_capacity(cap).unwrap();
        
        let mut inserted = 0;
        for i in 0..cap as u64 {
            if filter.insert(&i).is_ok() {
                inserted += 1;
            }
        }
        
        println!("Inserted {} at capacity {}", inserted, cap);
        // should get close to capacity
        assert!(inserted >= cap * 80 / 100);
    }

    #[test]
    fn capacity_not_power_of_two() {
        // user asks for non-power-of-2 capacity
        for cap in [7, 13, 99, 1000, 1337, 9999] {
            let filter = CuckooFilter::with_capacity(cap).unwrap();
            assert!(filter.buckets.len().is_power_of_two());
            assert!(filter.buckets.len() * ENTRIES_PER_BUCKET >= cap);
        }
    }

    // error propagation
    #[test]
    fn error_type_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CuckooError>();
    }

    #[test]
    fn filter_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CuckooFilter>();
    }
}
