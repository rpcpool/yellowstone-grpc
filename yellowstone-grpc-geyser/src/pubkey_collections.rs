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

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::hash::{BuildHasher, Hasher},
    };

    const WINDOWS: usize = 25;

    fn hash_with_window(offset: usize, pubkey: &Pubkey) -> u64 {
        let builder = PubkeyHasherBuilder::with_offset(offset);
        let mut hasher = builder.build_hasher();
        hasher.write(pubkey.as_array());
        hasher.finish()
    }

    #[test]
    fn every_reachable_window_is_within_bounds() {
        let pubkey = Pubkey::new_from_array([7; 32]);
        for offset in 0..WINDOWS {
            let _ = hash_with_window(offset, &pubkey);
        }
    }

    #[test]
    fn hashing_a_pubkey_writes_exactly_thirty_two_bytes() {
        struct LengthRecorder(Vec<usize>);
        impl Hasher for LengthRecorder {
            fn finish(&self) -> u64 {
                0
            }
            fn write(&mut self, bytes: &[u8]) {
                self.0.push(bytes.len());
            }
        }

        let pubkey = Pubkey::new_unique();

        let mut recorder = LengthRecorder(Vec::new());
        std::hash::Hash::hash(&pubkey, &mut recorder);
        assert_eq!(recorder.0, vec![32], "owned Pubkey");

        let mut recorder = LengthRecorder(Vec::new());
        std::hash::Hash::hash(&&pubkey, &mut recorder);
        assert_eq!(recorder.0, vec![32], "borrowed &Pubkey");
    }

    #[test]
    fn keys_differing_only_in_byte_zero_collide_under_all_but_one_window() {
        let keys = (0..8u8)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = i;
                Pubkey::new_from_array(bytes)
            })
            .collect::<Vec<_>>();

        let colliding = (0..WINDOWS)
            .filter(|&offset| {
                let first = hash_with_window(offset, &keys[0]);
                keys.iter()
                    .all(|key| hash_with_window(offset, key) == first)
            })
            .count();

        assert_eq!(
            colliding,
            WINDOWS - 1,
            "only the window starting at byte 0 separates these keys"
        );
    }

    #[test]
    fn membership_is_exact_under_every_window_including_degenerate_ones() {
        let members = (0..64u8)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = i;
                Pubkey::new_from_array(bytes)
            })
            .collect::<Vec<_>>();
        let strangers = (64..96u8)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = i;
                Pubkey::new_from_array(bytes)
            })
            .collect::<Vec<_>>();

        for offset in 0..WINDOWS {
            let mut set = PubkeyHashSet::with_hasher(PubkeyHasherBuilder::with_offset(offset));
            set.extend(members.iter().copied());
            set.extend(members.iter().copied());

            assert_eq!(set.len(), members.len(), "offset {offset}: dedup");
            for member in &members {
                assert!(set.contains(member), "offset {offset}: missing member");
            }
            for stranger in &strangers {
                assert!(!set.contains(stranger), "offset {offset}: false membership");
            }

            let borrowed = members.iter().collect::<PubkeyRefHashSet<'_>>();
            assert_eq!(borrowed.len(), members.len(), "offset {offset}: ref dedup");
        }
    }

    #[test]
    fn sysvar_shaped_keys_are_handled_exactly() {
        let natives = [
            "11111111111111111111111111111111",
            "Sysvar1nstructions1111111111111111111111111",
            "SysvarC1ock11111111111111111111111111111111",
            "SysvarRent111111111111111111111111111111111",
            "Stake11111111111111111111111111111111111111",
            "Vote111111111111111111111111111111111111111",
        ]
        .iter()
        .map(|s| s.parse::<Pubkey>().expect("valid native program id"))
        .collect::<Vec<_>>();

        for offset in 0..WINDOWS {
            let set = {
                let mut set = PubkeyHashSet::with_hasher(PubkeyHasherBuilder::with_offset(offset));
                set.extend(natives.iter().copied());
                set
            };
            assert_eq!(set.len(), natives.len(), "offset {offset}");
            for native in &natives {
                assert!(set.contains(native), "offset {offset}");
            }
            assert!(
                !set.contains(&Pubkey::new_from_array([9; 32])),
                "offset {offset}"
            );
        }
    }
}
