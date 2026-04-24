//! Stable, wire-compatible hasher for [`CuckooFilter`].
//!
//! Provides [`YellowstoneHasherBuilder`], a `BuildHasher` backed by SipHash-2-4.
//! The hasher's seed is serialized into the proto wire format so that client
//! and server reproduce identical hashes without coordinating constants
//! out-of-band.
//!
//! # Why Not `std::hash::DefaultHasher`
//!
//! The standard library's `DefaultHasher` is explicitly documented as
//! implementation-defined, its algorithm can change across Rust releases.
//! For a filter whose semantics depend on producing identical hashes in
//! separate processes (possibly built with different Rust versions, possibly
//! in different languages), algorithm stability is mandatory.
//!
//! [`CuckooFilter`]: super::filter::CuckooFilter

use {siphasher::sip::SipHasher24, std::hash::BuildHasher};

/// Deterministic `BuildHasher` backed by SipHash-2-4.
///
/// Uses SipHash rather than [`std::hash::DefaultHasher`] because
/// `DefaultHasher`'s algorithm is an implementation detail of the Rust
/// standard library and can change across releases. SipHash is a stable,
/// specified algorithm with implementations in every major language,
/// making filter bytes interoperable across Rust versions and across
/// client languages.
///
/// The seed is stored on the builder (not hardcoded) and is written into
/// the proto's `hash_seed` field at serialization. Deserialization reads
/// the seed back, ensuring wire-compatible hashing regardless of which
/// default either side was built with.
///
/// This is the default `S` type parameter for [`CuckooFilter`].
///
/// [`CuckooFilter`]: super::filter::CuckooFilter
#[derive(Debug, Clone)]
pub struct YellowstoneHasherBuilder {
    seed: u64,
}

impl YellowstoneHasherBuilder {
    /// Creates a builder with the given seed.
    ///
    /// Most callers should use [`YellowstoneHasherBuilder::default`] for
    /// construction — it uses [`DEFAULT_HASH_SEED`], which is the seed all
    /// standard filters serialize with.
    ///
    /// Use `new` when reconstructing a builder from a seed read off the wire
    /// (e.g. via `From<&ProtoCuckooFilter>`), or when intentionally choosing
    /// a non-default seed for a filter that will interoperate with a peer
    /// using the same seed.
    ///
    /// # Example
    ///
    /// ```
    /// use yellowstone_grpc_proto::cuckoo::YellowstoneHasherBuilder;
    ///
    /// let builder = YellowstoneHasherBuilder::new(0xDEADBEEF);
    /// assert_eq!(builder.seed(), 0xDEADBEEF);
    /// ```
    ///
    /// [`DEFAULT_HASH_SEED`]: super::constants::DEFAULT_HASH_SEED
    pub const fn new(seed: u64) -> Self {
        Self { seed }
    }

    /// Returns the seed this builder hashes with.
    ///
    /// Used by [`CuckooFilter`]'s proto serialization to write `hash_seed` onto
    /// the wire, enabling the receiver to reconstruct a matching hasher.
    ///
    /// [`CuckooFilter`]: super::filter::CuckooFilter
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Derives SipHash's two keys (k0, k1) from a single seed.
    ///
    /// SipHash-2-4 takes two 64-bit keys. We expose one seed on the wire and
    /// derive the second deterministically:
    ///
    /// - `k0 = seed`
    /// - `k1 = seed.rotate_left(32)`
    ///
    /// Cross-language implementations must use the exact same derivation to
    /// produce matching hashes. The rotation direction (left), the rotation
    /// amount (32 bits), and the assignment of `seed` to `k0` (not `k1`) are
    /// all part of the wire contract and cannot be changed without breaking
    /// every existing serialized filter.
    pub(crate) const fn keys_from_seed(seed: u64) -> (u64, u64) {
        (seed, seed.rotate_left(32))
    }
}

impl Default for YellowstoneHasherBuilder {
    fn default() -> Self {
        Self::new(super::constants::DEFAULT_HASH_SEED)
    }
}

impl BuildHasher for YellowstoneHasherBuilder {
    type Hasher = SipHasher24;

    fn build_hasher(&self) -> SipHasher24 {
        let (k0, k1) = Self::keys_from_seed(self.seed);
        SipHasher24::new_with_keys(k0, k1)
    }
}
