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

use {
    crate::geyser::CuckooFilter as ProtoCuckooFilter,
    std::{
        hash::{BuildHasher, DefaultHasher, Hash, Hasher},
        marker::PhantomData,
    },
    thiserror::Error,
};

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

/// Deterministic BuildHasher used as the default for `CuckooFilter`.
///
/// Produces hashers pre-seeded with `HASH_SEED`, ensuring wire-compatible
/// hashing across client and server without requiring explicit seed coordination.
#[derive(Debug, Clone, Default)]
pub struct YellowstoneHasherBuilder;

impl BuildHasher for YellowstoneHasherBuilder {
    type Hasher = SeededDefaultHasher;

    fn build_hasher(&self) -> SeededDefaultHasher {
        SeededDefaultHasher::new(HASH_SEED)
    }
}

/// A `DefaultHasher` pre-seeded at construction.
pub struct SeededDefaultHasher {
    inner: DefaultHasher,
}

impl SeededDefaultHasher {
    fn new(seed: u64) -> Self {
        let mut inner = DefaultHasher::new();
        seed.hash(&mut inner);
        Self { inner }
    }
}

impl Hasher for SeededDefaultHasher {
    fn finish(&self) -> u64 {
        self.inner.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.inner.write(bytes)
    }
}

/// A space-efficient probabilistic set membership filter.
///
/// Stores fingerprints (short hashes) of items rather than items themselves.
/// Supports insert, lookup, and delete operations.
#[derive(Debug)]
pub struct CuckooFilter<T, S = YellowstoneHasherBuilder> {
    buckets: Vec<Bucket>,
    hasher_builder: S,
    _phantom: PhantomData<fn() -> T>,
}

#[derive(Debug, Error)]
pub enum CuckooError {
    #[error("capacity overflow: requested capacity exceeds maximum")]
    CapacityOverflow,
    #[error("cuckoo table full after {} kicks", MAX_KICKS)]
    TableFull,
}

impl<T> CuckooFilter<T, YellowstoneHasherBuilder> {
    pub fn with_capacity(max_capacity: usize) -> Result<Self, CuckooError> {
        Self::with_capacity_and_hasher(max_capacity, YellowstoneHasherBuilder)
    }
}

impl<T, S: BuildHasher> CuckooFilter<T, S> {
    /// Creates a new filter with a custom hasher builder.
    ///
    /// # Example
    ///
    /// ```
    /// use yellowstone_grpc_proto::cuckoo::{CuckooFilter, YellowstoneHasherBuilder};
    ///
    /// let filter = CuckooFilter::<u64, YellowstoneHasherBuilder>::with_capacity_and_hasher(
    ///     1000,
    ///     YellowstoneHasherBuilder,
    /// ).unwrap();
    /// assert!(!filter.contains(&42));
    /// ```
    pub fn with_capacity_and_hasher(
        max_capacity: usize,
        hasher_builder: S,
    ) -> Result<Self, CuckooError> {
        let buckets_needed =
            (max_capacity as f64 / (LOAD_FACTOR * ENTRIES_PER_BUCKET as f64)).ceil() as usize;

        let bucket_count = buckets_needed
            .checked_next_power_of_two()
            .ok_or(CuckooError::CapacityOverflow)?
            .max(1);

        let mut buckets = Vec::new();
        buckets
            .try_reserve_exact(bucket_count)
            .map_err(|_| CuckooError::CapacityOverflow)?;

        buckets.resize(bucket_count, [0; ENTRIES_PER_BUCKET]);

        Ok(Self {
            buckets,
            hasher_builder,
            _phantom: PhantomData,
        })
    }

    /// Hashes an item using the seeded hasher.
    fn hash<H: Hash>(&self, item: &H) -> u64 {
        let mut hasher = self.hasher_builder.build_hasher();
        item.hash(&mut hasher);
        hasher.finish()
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

impl<T: Hash, S: BuildHasher> CuckooFilter<T, S> {
    /// Inserts an item into the filter.
    ///
    /// Uses partial-key cuckoo hashing: if both candidate buckets are full,
    /// existing fingerprints are relocated to make room.
    ///
    /// # Errors
    ///
    /// - `CuckooError::InvalidState`: Filter has no buckets (corrupted state)
    /// - `CuckooError::TableFull`: Could not insert after MAX_KICKS relocations
    pub fn insert(&mut self, item: &T) -> Result<(), CuckooError> {
        let fp = self.fingerprint(item);
        let h = self.hash(item);
        let i1 = self.index(h);
        let i2 = i1 ^ self.index(self.hash(&fp));

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

            i ^= self.index(self.hash(&fp));

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
    pub fn contains(&self, item: &T) -> bool {
        let fp = self.fingerprint(item);
        let h = self.hash(item);
        let i1 = self.index(h);
        let i2 = i1 ^ self.index(self.hash(&fp));

        self.bucket_contains(i1, fp) || self.bucket_contains(i2, fp)
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
    pub fn remove(&mut self, item: &T) -> bool {
        let fp = self.fingerprint(item);
        let h = self.hash(item);
        let i1 = self.index(h);
        let i2 = i1 ^ self.index(self.hash(&fp));

        self.try_remove_from_bucket(i1, fp) || self.try_remove_from_bucket(i2, fp)
    }

    /// Extracts a fingerprint from an item's hash.
    /// Returns upper 16 bits, ensuring non-zero (0 = empty slot).
    fn fingerprint(&self, item: &T) -> Fingerprint {
        let h = self.hash(item);
        let fp = (h >> 32) as u16;
        if fp == 0 {
            1
        } else {
            fp
        }
    }
}

/// Deserializes from proto wire format.
///
/// Handles malformed input gracefully:
/// - Empty data -> single empty bucket
/// - Odd bytes -> truncated (via chunks_exact)
/// - Misaligned data -> incomplete buckets dropped
impl<T> From<&ProtoCuckooFilter> for CuckooFilter<T, YellowstoneHasherBuilder> {
    fn from(proto: &ProtoCuckooFilter) -> Self {
        if proto.data.is_empty() {
            return Self {
                buckets: vec![[0; ENTRIES_PER_BUCKET]; 1],
                hasher_builder: YellowstoneHasherBuilder,
                _phantom: PhantomData,
            };
        }

        let buckets: Vec<Bucket> = proto
            .data
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<u16>>()
            .chunks_exact(ENTRIES_PER_BUCKET)
            .map(|chunk| [chunk[0], chunk[1], chunk[2], chunk[3]])
            .collect();

        if buckets.is_empty() {
            return Self {
                buckets: vec![[0; ENTRIES_PER_BUCKET]; 1],
                hasher_builder: YellowstoneHasherBuilder,
                _phantom: PhantomData,
            };
        }

        Self {
            buckets,
            hasher_builder: YellowstoneHasherBuilder,
            _phantom: PhantomData,
        }
    }
}

/// Serializes to proto wire format for cross-language interop.
impl<T, S> From<&CuckooFilter<T, S>> for ProtoCuckooFilter {
    fn from(filter: &CuckooFilter<T, S>) -> Self {
        let data: Vec<u8> = filter
            .buckets
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
        let str_filter = CuckooFilter::<&str>::with_capacity(100).unwrap();
        assert!(!str_filter.contains(&"hello"));

        let int_filter = CuckooFilter::<u64>::with_capacity(100).unwrap();
        assert!(!int_filter.contains(&42u64));
    }

    #[test]
    fn insert_then_contains() {
        let mut filter = CuckooFilter::<&str>::with_capacity(100).unwrap();
        assert!(filter.insert(&"hello").is_ok());
        assert!(filter.contains(&"hello"));
        assert!(!filter.contains(&"world"));
    }

    #[test]
    fn remove_then_not_contains() {
        let mut filter = CuckooFilter::<&str>::with_capacity(100).unwrap();
        filter.insert(&"hello").unwrap();
        assert!(filter.contains(&"hello"));
        assert!(filter.remove(&"hello"));
        assert!(!filter.contains(&"hello"));
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let mut filter = CuckooFilter::<&str>::with_capacity(100).unwrap();
        assert!(!filter.remove(&"hello"));
    }

    #[test]
    fn capacity_zero() {
        let filter = CuckooFilter::<[u8; 32]>::with_capacity(0).unwrap();
        assert!(!filter.buckets.is_empty());
    }

    #[test]
    fn capacity_one() {
        let mut filter = CuckooFilter::<&str>::with_capacity(1).unwrap();
        assert!(filter.insert(&"only").is_ok());
        assert!(filter.contains(&"only"));
    }

    #[test]
    fn many_inserts() {
        let mut filter = CuckooFilter::<u64>::with_capacity(1000).unwrap();
        for i in 0..1000u64 {
            let _ = filter.insert(&i);
        }
        assert!(filter.contains(&0u64));
        assert!(filter.contains(&500u64));
        assert!(filter.contains(&999u64));
        assert!(!filter.contains(&1000u64));
    }

    #[test]
    fn insert_returns_error_when_full() {
        let mut filter = CuckooFilter::<u64>::with_capacity(10).unwrap();
        let mut table_full_seen = false;
        for i in 0..1000u64 {
            match filter.insert(&i) {
                Ok(()) => {}
                Err(CuckooError::TableFull) => {
                    table_full_seen = true;
                    break;
                }
                Err(other) => panic!("unexpected error: {:?}", other),
            }
        }
        assert!(table_full_seen, "expected TableFull error");
    }

    #[test]
    fn proto_roundtrip() {
        let mut filter = CuckooFilter::<u64>::with_capacity(100).unwrap();
        filter.insert(&1).unwrap();
        filter.insert(&2).unwrap();
        filter.insert(&42).unwrap();

        let proto = ProtoCuckooFilter::from(&filter);
        let restored = CuckooFilter::<u64>::from(&proto);

        assert!(restored.contains(&1));
        assert!(restored.contains(&2));
        assert!(restored.contains(&42));
        assert!(!restored.contains(&999));
    }

    #[test]
    fn bucket_count_is_power_of_two() {
        for cap in [1, 10, 100, 1000, 1337, 10000] {
            let filter = CuckooFilter::<u64>::with_capacity(cap).unwrap();
            let len = filter.buckets.len();
            assert!(
                len.is_power_of_two(),
                "capacity {} gave {} buckets",
                cap,
                len
            );
        }
    }

    #[test]
    fn capacity_usize_max() {
        let result = CuckooFilter::<u64>::with_capacity(usize::MAX);
        assert!(matches!(result, Err(CuckooError::CapacityOverflow)));
    }

    #[test]
    fn capacity_usize_max_minus_one() {
        let result = CuckooFilter::<u64>::with_capacity(usize::MAX - 1);
        assert!(matches!(result, Err(CuckooError::CapacityOverflow)));
    }

    #[test]
    fn insert_same_item_thousand_times() {
        let mut filter = CuckooFilter::<&str>::with_capacity(100).unwrap();
        for _ in 0..1000 {
            let _ = filter.insert(&"same");
        }
        assert!(filter.contains(&"same"));
    }

    #[test]
    fn insert_after_remove_same_item() {
        let mut filter = CuckooFilter::<&str>::with_capacity(100).unwrap();
        filter.insert(&"hello").unwrap();
        filter.remove(&"hello");
        assert!(filter.insert(&"hello").is_ok());
        assert!(filter.contains(&"hello"));
    }

    #[test]
    fn remove_same_item_twice() {
        let mut filter = CuckooFilter::<&str>::with_capacity(100).unwrap();
        filter.insert(&"hello").unwrap();
        assert!(filter.remove(&"hello"));
        assert!(!filter.remove(&"hello"));
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
        let filter = CuckooFilter::<&str>::from(&proto);
        assert!(!filter.contains(&"anything"));
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
        let filter = CuckooFilter::<&str>::from(&proto);
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
        let filter = CuckooFilter::<&str>::from(&proto);
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
        let filter = CuckooFilter::<&str>::from(&proto);
        let _ = filter.contains(&"test");
    }

    #[test]
    fn items_with_zero_hash() {
        let mut filter = CuckooFilter::<u64>::with_capacity(100).unwrap();
        filter.insert(&0u64).unwrap();
        assert!(filter.contains(&0u64));
    }

    #[test]
    fn items_with_max_hash() {
        let mut filter = CuckooFilter::<u64>::with_capacity(100).unwrap();
        filter.insert(&u64::MAX).unwrap();
        assert!(filter.contains(&u64::MAX));
    }

    #[test]
    fn empty_string() {
        let mut filter = CuckooFilter::<&str>::with_capacity(100).unwrap();
        filter.insert(&"").unwrap();
        assert!(filter.contains(&""));
    }

    #[test]
    fn very_long_string() {
        let long = "a".repeat(1_000_000);
        let mut filter = CuckooFilter::<String>::with_capacity(100).unwrap();
        filter.insert(&long).unwrap();
        assert!(filter.contains(&long));
    }

    #[test]
    fn fill_then_remove_then_fill() {
        let mut filter = CuckooFilter::<u64>::with_capacity(50).unwrap();

        for i in 0..50u64 {
            let _ = filter.insert(&i);
        }

        for i in 0..50u64 {
            filter.remove(&i);
        }

        for i in 100..150u64 {
            assert!(
                filter.insert(&i).is_ok(),
                "failed to insert {} after remove cycle",
                i
            );
        }
    }

    #[test]
    fn false_positive_rate() {
        let n = 10_000;
        let mut filter = CuckooFilter::<u64>::with_capacity(n).unwrap();

        for i in 0..n as u64 {
            let _ = filter.insert(&i);
        }

        let mut false_positives = 0;
        let test_count = 100_000;
        for i in n as u64..(n as u64 + test_count) {
            if filter.contains(&i) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / test_count as f64;
        println!("False positive rate: {:.4}%", fp_rate * 100.0);

        assert!(
            fp_rate < 0.01,
            "false positive rate too high: {:.4}%",
            fp_rate * 100.0
        );
    }

    #[test]
    fn pubkey_like_data() {
        // 32-byte arrays like Solana pubkeys
        let mut filter = CuckooFilter::<[u8; 32]>::with_capacity(1000).unwrap();

        for i in 0..100u8 {
            let pubkey = [i; 32];
            filter.insert(&pubkey).unwrap();
        }

        for i in 0..100u8 {
            let pubkey = [i; 32];
            assert!(filter.contains(&pubkey));
        }

        // not inserted
        let missing = [255u8; 32];
        assert!(!filter.contains(&missing));
    }

    #[test]
    fn similar_pubkeys() {
        // pubkeys that differ by one byte
        let mut filter = CuckooFilter::<[u8; 32]>::with_capacity(100).unwrap();

        let pk1 = [0u8; 32];
        let mut pk2 = [0u8; 32];
        pk2[31] = 1;

        filter.insert(&pk1).unwrap();

        assert!(filter.contains(&pk1));
        assert!(!filter.contains(&pk2));
    }

    // determinism

    #[test]
    fn deterministic_behavior() {
        let mut filter1 = CuckooFilter::<u64>::with_capacity(100).unwrap();
        let mut filter2 = CuckooFilter::<u64>::with_capacity(100).unwrap();

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
        let mut filter = CuckooFilter::<u64>::with_capacity(100).unwrap();
        for i in 0..50u64 {
            filter.insert(&i).unwrap();
        }

        let proto1 = ProtoCuckooFilter::from(&filter);
        let restored1 = CuckooFilter::<u64>::from(&proto1);
        let proto2 = ProtoCuckooFilter::from(&restored1);
        let restored2 = CuckooFilter::<u64>::from(&proto2);

        assert_eq!(proto1.data, proto2.data);

        for i in 0..50u64 {
            assert!(restored2.contains(&i));
        }
    }

    // at scale

    #[test]
    fn hundred_thousand_items() {
        let n = 100_000;
        let mut filter = CuckooFilter::<u64>::with_capacity(n).unwrap();

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
        let mut filter = CuckooFilter::<u64>::with_capacity(n).unwrap();

        for i in 0..n as u64 {
            let _ = filter.insert(&i);
        }

        let proto = ProtoCuckooFilter::from(&filter);
        let size_bytes = proto.data.len();
        let size_mb = size_bytes as f64 / (1024.0 * 1024.0);

        println!(
            "Filter size for {} items: {} bytes ({:.2} MB)",
            n, size_bytes, size_mb
        );

        // n u64s * 8 bytes = 800KB raw; filter should be smaller
        assert!(size_bytes < 1024 * 1024, "filter too large");
    }

    // interleaved ops

    #[test]
    fn interleaved_insert_remove_contains() {
        let mut filter = CuckooFilter::<u64>::with_capacity(100).unwrap();

        for i in 1..=50u64 {
            filter.insert(&i).unwrap();
        }

        for i in (2..=50u64).step_by(2) {
            filter.remove(&i);
        }

        for i in 51..=75u64 {
            filter.insert(&i).unwrap();
        }

        for i in (1..=50u64).step_by(2) {
            assert!(filter.contains(&i), "{} should exist", i);
        }

        for i in (2..=50u64).step_by(2) {
            assert!(!filter.contains(&i), "{} should be gone", i);
        }

        for i in 51..=75u64 {
            assert!(filter.contains(&i), "{} should exist", i);
        }
    }

    // boundary values

    #[test]
    fn insert_at_exact_capacity() {
        let cap = 64;
        let mut filter = CuckooFilter::<u64>::with_capacity(cap).unwrap();

        let mut inserted = 0;
        for i in 0..cap as u64 {
            if filter.insert(&i).is_ok() {
                inserted += 1;
            }
        }

        println!("Inserted {} at capacity {}", inserted, cap);
        assert!(inserted >= cap * 80 / 100);
    }

    #[test]
    fn capacity_not_power_of_two() {
        for cap in [7, 13, 99, 1000, 1337, 9999] {
            let filter = CuckooFilter::<u64>::with_capacity(cap).unwrap();
            assert!(filter.buckets.len().is_power_of_two());
            assert!(filter.buckets.len() * ENTRIES_PER_BUCKET >= cap);
        }
    }

    // send/sync

    #[test]
    fn error_type_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CuckooError>();
    }

    #[test]
    fn filter_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CuckooFilter<[u8; 32]>>();
    }

    #[test]
    fn false_positive_rate_small() {
        use rand::{rngs::StdRng, Rng, SeedableRng};

        const N: usize = 10_000;
        const PROBES: usize = 100_000;

        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);
        let mut filter: CuckooFilter<[u8; 32]> = CuckooFilter::with_capacity(N).unwrap();

        let mut inserted = Vec::with_capacity(N);
        for _ in 0..N {
            let key: [u8; 32] = rng.gen();
            filter.insert(&key).unwrap();
            inserted.push(key);
        }

        // sanity: all inserted items are found
        for key in &inserted {
            assert!(filter.contains(key));
        }

        // probe with random keys that were NOT inserted
        let inserted_set: std::collections::HashSet<[u8; 32]> = inserted.iter().copied().collect();
        let mut false_positives = 0;
        let mut probed = 0;
        while probed < PROBES {
            let key: [u8; 32] = rng.gen();
            if inserted_set.contains(&key) {
                continue; // skip collisions with real insertions
            }
            if filter.contains(&key) {
                false_positives += 1;
            }
            probed += 1;
        }

        let fp_rate = false_positives as f64 / probed as f64;
        assert!(
            fp_rate < 0.01,
            "false positive rate {:.4} exceeded 1%",
            fp_rate
        );
    }

    #[test]
    #[ignore] // gated due to CI: cargo test --release -- --ignored false_positive_rate_at_scale
    fn false_positive_rate_at_scale() {
        use rand::{rngs::StdRng, Rng, SeedableRng};

        const N: usize = 2_000_000;
        const PROBES: usize = 200_000; // 10% of N, keeps runtime reasonable

        let mut rng = StdRng::seed_from_u64(0xCAFEBABE);
        let mut filter: CuckooFilter<[u8; 32]> = CuckooFilter::with_capacity(N).unwrap();

        // inserting 2M items; skip storing them all, use a Bloom-style second filter
        // for cheap dedup, or just accept tiny probability of probe collision
        for _ in 0..N {
            let key: [u8; 32] = rng.gen();
            filter.insert(&key).unwrap();
        }

        // probes use a different RNG stream so we don't accidentally re-draw insertions
        let mut probe_rng = StdRng::seed_from_u64(0xF00DFACE);
        let mut false_positives = 0;
        for _ in 0..PROBES {
            let key: [u8; 32] = probe_rng.gen();
            if filter.contains(&key) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / PROBES as f64;
        assert!(
            fp_rate < 0.01,
            "false positive rate {:.4} at 2M scale exceeded 1%",
            fp_rate
        );
    }

    #[test]
    fn colliding_items_both_insert_and_contain() {
        // find two distinct [u8; 32] values that hash to the same (primary_bucket, fingerprint).
        // brute force until we find a pair. deterministic seed so test is reproducible.
        use rand::{rngs::StdRng, Rng, SeedableRng};

        const CAPACITY: usize = 1024;
        let mut rng = StdRng::seed_from_u64(42);

        // throwaway filter to access the same index/fingerprint logic
        // (we can compute both externally but calling through the public API would be cleaner;
        // easiest path is to search until two items collide at the API level)
        let mut seen: std::collections::HashMap<(u64, u16), [u8; 32]> = Default::default();
        let (key_a, key_b) = loop {
            let key: [u8; 32] = rng.gen();
            // replicate the exact hash used internally
            let h = seeded_hash_for_test(&key);
            let fp = fingerprint_for_test(h);
            let primary = h & (CAPACITY as u64 - 1);

            if let Some(&existing) = seen.get(&(primary, fp)) {
                if existing != key {
                    break (existing, key);
                }
            }
            seen.insert((primary, fp), key);
        };

        let mut filter: CuckooFilter<[u8; 32]> = CuckooFilter::with_capacity(CAPACITY).unwrap();
        filter.insert(&key_a).unwrap();
        filter.insert(&key_b).unwrap();

        assert!(filter.contains(&key_a));
        assert!(filter.contains(&key_b));
    }

    // helpers that mirror the internal hash
    fn seeded_hash_for_test(item: &[u8; 32]) -> u64 {
        use std::hash::{BuildHasher, Hash, Hasher};
        let builder = YellowstoneHasherBuilder;
        let mut hasher = builder.build_hasher();
        item.hash(&mut hasher);
        hasher.finish()
    }

    fn fingerprint_for_test(h: u64) -> u16 {
        let fp = (h >> 32) as u16;
        if fp == 0 {
            1
        } else {
            fp
        }
    }

    #[test]
    fn custom_hasher_builder() {
        use std::collections::hash_map::RandomState;

        let hasher = RandomState::new();
        let mut filter =
            CuckooFilter::<&str, RandomState>::with_capacity_and_hasher(100, hasher).unwrap();

        filter.insert(&"hello").unwrap();
        assert!(filter.contains(&"hello"));
        assert!(!filter.contains(&"world"));
    }
}
