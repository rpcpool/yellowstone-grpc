/// Slots per bucket.
pub(crate) const ENTRIES_PER_BUCKET: usize = 4;

/// Target load factor. 95% occupancy is achievable with 4 entries/bucket.
pub(crate) const LOAD_FACTOR: f64 = 0.95;

/// Maximum relocations before declaring table full.
pub(crate) const MAX_KICKS: usize = 500;

/// Fingerprint size in bits. 16 bits gives ~0.0001% false positive rate.
pub(crate) const FINGERPRINT_BITS: u32 = 16;

/// Default seed for [`YellowstoneHasherBuilder`]. ASCII: "yllwstn!"
///
/// Serves as the stable default used by filters constructed via
/// [`CuckooFilter::with_capacity`] or [`CuckooMap::with_capacity`]. The seed
/// in use is always serialized into the proto's `hash_seed` field, so filters
/// built with this default remain readable even if the default changes in a
/// future version.
///
/// Exposed publicly so callers who want to construct a builder with the
/// default seed explicitly (rather than via [`YellowstoneHasherBuilder::default`])
/// can reference it by name.
///
/// [`YellowstoneHasherBuilder`]: crate::cuckoo::YellowstoneHasherBuilder
/// [`CuckooFilter::with_capacity`]: crate::cuckoo::CuckooFilter::with_capacity
/// [`CuckooMap::with_capacity`]: crate::cuckoo::CuckooMap::with_capacity
/// [`YellowstoneHasherBuilder::default`]: crate::cuckoo::YellowstoneHasherBuilder::default
pub const DEFAULT_HASH_SEED: u64 = 0x_796c_6c77_7374_6e21;
