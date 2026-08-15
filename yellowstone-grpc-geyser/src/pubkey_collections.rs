//! Hash collections specialized for small, transaction-local [`Pubkey`] sets.
//!
//! [`PubkeyHasherBuilder`] hashes a randomly selected eight-byte window of a
//! pubkey. This is faster than a general-purpose hasher, but is intentionally
//! less collision-resistant. These aliases must not be used for unbounded
//! client-defined filters, where collisions can become a denial-of-service
//! vector.

use {
    solana_pubkey::{Pubkey, PubkeyHasherBuilder},
    std::collections::HashSet,
};

/// A [`HashSet`] using Solana's specialized pubkey hasher.
pub type PubkeyHashSet = HashSet<Pubkey, PubkeyHasherBuilder>;

/// A borrowed-pubkey [`HashSet`] using Solana's specialized pubkey hasher.
pub(crate) type PubkeyRefHashSet<'a> = HashSet<&'a Pubkey, PubkeyHasherBuilder>;
