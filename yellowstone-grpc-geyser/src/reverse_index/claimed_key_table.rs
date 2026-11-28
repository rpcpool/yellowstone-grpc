//! The claimed key table: one bucket per claimed (32-byte key, claim role) pair, in 4,096
//! open-addressing shards that grow on their own, so a growth moves one shard's pairs only.

use {
    super::key_claim_index::ClaimRef,
    foldhash::fast::RandomState,
    std::{hash::BuildHasher, mem::size_of},
};

/// The shards of a claimed key table.
pub const CLAIMED_KEY_SHARDS: usize = 4096;

/// The buckets a shard allocates for its first pair; it doubles past half full.
pub const FIRST_SHARD_BUCKETS: usize = 8;

/// The tag of an empty bucket. A held bucket's tag has its high bit set.
const EMPTY_BUCKET_TAG: u8 = 0;

/// Which probe of a message finds a claimed key. The role is part of the pair, so one 32-byte
/// value can be claimed as an account and as an owner at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum ClaimRole {
    Account,
    /// An account's owner; its bucket also counts that owner's memcmp gates.
    Owner,
    Transaction,
    MemcmpAt0,
    MemcmpAt32,
}

/// How many claim roles there are.
pub const CLAIM_ROLE_COUNT: usize = 5;

impl ClaimRole {
    /// Every claim role, by its `u8`.
    pub const ALL: [Self; CLAIM_ROLE_COUNT] = [
        Self::Account,
        Self::Owner,
        Self::Transaction,
        Self::MemcmpAt0,
        Self::MemcmpAt32,
    ];
}

/// One held pair: the slots that claim it and, on an owner bucket, the owner's memcmp gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ClaimedKeyBucket {
    pub table_key: [u8; 32],
    pub claim_ref: ClaimRef,
    /// While above 0, account updates of this owner probe their memcmp keys.
    pub memcmp_gate_count: u32,
    pub claim_role: u8,
}

impl ClaimedKeyBucket {
    /// The content of every empty bucket.
    const EMPTY: Self = Self {
        table_key: [0; 32],
        claim_ref: ClaimRef::NONE,
        memcmp_gate_count: 0,
        claim_role: 0,
    };

    /// Whether neither a slot nor a memcmp gate claims the pair, so it can leave the table.
    pub const fn is_unclaimed(&self) -> bool {
        self.claim_ref.is_none() && self.memcmp_gate_count == 0
    }

    /// The bucket's claim role; every insert writes a valid one.
    pub const fn bucket_role(&self) -> ClaimRole {
        ClaimRole::ALL[self.claim_role as usize]
    }
}

/// One shard: a power-of-two count of buckets, probed linearly from a pair's home bucket. A probe
/// reads a bucket's one-byte tag before the bucket.
#[derive(Default)]
pub struct KeyTableShard {
    bucket_tags: Box<[u8]>,
    shard_buckets: Box<[ClaimedKeyBucket]>,
    held_bucket_count: usize,
}

impl KeyTableShard {
    fn with_bucket_count(bucket_count: usize) -> Self {
        debug_assert!(bucket_count.is_power_of_two());
        Self {
            bucket_tags: vec![EMPTY_BUCKET_TAG; bucket_count].into_boxed_slice(),
            shard_buckets: vec![ClaimedKeyBucket::EMPTY; bucket_count].into_boxed_slice(),
            held_bucket_count: 0,
        }
    }

    /// The shard's bucket count, 0 before its first pair.
    pub const fn bucket_count(&self) -> usize {
        self.bucket_tags.len()
    }

    pub const fn held_bucket_count(&self) -> usize {
        self.held_bucket_count
    }

    /// Whether one more pair would take the shard past its load limit of 1/2.
    const fn needs_growth_for_insert(&self) -> bool {
        (self.held_bucket_count + 1) * 2 > self.bucket_count()
    }

    /// The position of the pair, if held. The load limit leaves an empty bucket to end the probe.
    #[inline(always)]
    fn position_of(
        &self,
        pair_hash: u64,
        table_key: &[u8; 32],
        claim_role: ClaimRole,
    ) -> Option<usize> {
        let bucket_mask = self.bucket_count().checked_sub(1)?;
        let pair_tag = bucket_tag_of(pair_hash);
        let mut bucket_position = pair_hash as usize & bucket_mask;
        loop {
            let bucket_tag = self.bucket_tags[bucket_position];
            if bucket_tag == EMPTY_BUCKET_TAG {
                return None;
            }
            if bucket_tag == pair_tag {
                let probed_bucket = &self.shard_buckets[bucket_position];
                if probed_bucket.claim_role == claim_role as u8
                    && probed_bucket.table_key == *table_key
                {
                    return Some(bucket_position);
                }
            }
            bucket_position = (bucket_position + 1) & bucket_mask;
        }
    }

    /// Puts the pair into the first empty bucket from its home on, and returns that position.
    fn place_bucket(&mut self, pair_hash: u64, placed_bucket: ClaimedKeyBucket) -> usize {
        let bucket_mask = self.bucket_count() - 1;
        let mut bucket_position = pair_hash as usize & bucket_mask;
        while self.bucket_tags[bucket_position] != EMPTY_BUCKET_TAG {
            bucket_position = (bucket_position + 1) & bucket_mask;
        }
        self.bucket_tags[bucket_position] = bucket_tag_of(pair_hash);
        self.shard_buckets[bucket_position] = placed_bucket;
        self.held_bucket_count += 1;
        bucket_position
    }
}

/// A pair's tag: the top 7 bits of its hash, with the high bit set.
#[inline(always)]
const fn bucket_tag_of(pair_hash: u64) -> u8 {
    (pair_hash >> 57) as u8 | 0x80
}

/// A pair's shard: hash bits 40 to 51, which no bucket position below 2^40 nor the tag uses.
#[inline(always)]
pub const fn shard_number_of(pair_hash: u64) -> usize {
    (pair_hash >> 40) as usize & (CLAIMED_KEY_SHARDS - 1)
}

#[inline(always)]
const fn folded_multiply(left_word: u64, right_word: u64) -> u64 {
    let full_product = (left_word as u128) * (right_word as u128);
    (full_product as u64) ^ ((full_product >> 64) as u64)
}

/// A bucket's shard and position, valid until the table next changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketPlace {
    pub shard_number: usize,
    pub bucket_position: usize,
}

/// A hash that replaces the seeded one in a test, to make pairs collide.
#[cfg(test)]
pub type ForcedPairHash = fn(&[u8; 32], ClaimRole) -> u64;

/// A level's claimed pairs in [`CLAIMED_KEY_SHARDS`] shards that share one seeded hash. A removal
/// shifts the rest of its probe run back, so no tombstone is left.
pub struct ClaimedKeyTable {
    hash_seeds: [u64; 4],
    /// Empty until the first pair is inserted.
    table_shards: Box<[KeyTableShard]>,
    held_bucket_count: usize,
    #[cfg(test)]
    pub forced_pair_hash: Option<ForcedPairHash>,
}

impl Default for ClaimedKeyTable {
    fn default() -> Self {
        let seed_source = RandomState::default();
        let hash_seeds = std::array::from_fn(|seed_number| seed_source.hash_one(seed_number) | 1);
        Self {
            hash_seeds,
            table_shards: Box::default(),
            held_bucket_count: 0,
            #[cfg(test)]
            forced_pair_hash: None,
        }
    }
}

impl ClaimedKeyTable {
    /// The pair's hash: folded multiplies of the key's four words, the role and the seeds.
    #[inline(always)]
    pub fn pair_hash(&self, table_key: &[u8; 32], claim_role: ClaimRole) -> u64 {
        #[cfg(test)]
        if let Some(forced_pair_hash) = self.forced_pair_hash {
            return forced_pair_hash(table_key, claim_role);
        }
        let (key_words, _) = table_key.as_chunks::<8>();
        let key_word = |word_number: usize| u64::from_le_bytes(key_words[word_number]);
        let [first_seed, second_seed, third_seed, fourth_seed] = self.hash_seeds;
        let role_word = (claim_role as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let low_fold = folded_multiply(key_word(0) ^ first_seed, key_word(1) ^ second_seed);
        let high_fold = folded_multiply(
            key_word(2) ^ third_seed,
            key_word(3) ^ fourth_seed ^ role_word,
        );
        folded_multiply(low_fold ^ fourth_seed, high_fold ^ first_seed)
    }

    /// The probe a message makes: the pair's bucket, if held.
    #[inline(always)]
    pub fn find_bucket(
        &self,
        table_key: &[u8; 32],
        claim_role: ClaimRole,
    ) -> Option<&ClaimedKeyBucket> {
        let pair_hash = self.pair_hash(table_key, claim_role);
        let table_shard = self.table_shards.get(shard_number_of(pair_hash))?;
        let bucket_position = table_shard.position_of(pair_hash, table_key, claim_role)?;
        Some(&table_shard.shard_buckets[bucket_position])
    }

    pub fn locate_bucket(
        &self,
        table_key: &[u8; 32],
        claim_role: ClaimRole,
    ) -> Option<BucketPlace> {
        let pair_hash = self.pair_hash(table_key, claim_role);
        let shard_number = shard_number_of(pair_hash);
        let table_shard = self.table_shards.get(shard_number)?;
        let bucket_position = table_shard.position_of(pair_hash, table_key, claim_role)?;
        Some(BucketPlace {
            shard_number,
            bucket_position,
        })
    }

    pub fn bucket_at(&self, bucket_place: BucketPlace) -> &ClaimedKeyBucket {
        &self.table_shards[bucket_place.shard_number].shard_buckets[bucket_place.bucket_position]
    }

    pub fn bucket_at_mut(&mut self, bucket_place: BucketPlace) -> &mut ClaimedKeyBucket {
        &mut self.table_shards[bucket_place.shard_number].shard_buckets
            [bucket_place.bucket_position]
    }

    /// The pair's bucket, inserted unclaimed if new; `true` when the insert allocated or doubled
    /// its shard.
    pub fn find_or_insert(
        &mut self,
        table_key: &[u8; 32],
        claim_role: ClaimRole,
    ) -> (BucketPlace, bool) {
        if self.table_shards.is_empty() {
            self.table_shards = std::iter::repeat_with(KeyTableShard::default)
                .take(CLAIMED_KEY_SHARDS)
                .collect();
        }
        let pair_hash = self.pair_hash(table_key, claim_role);
        let shard_number = shard_number_of(pair_hash);
        let held_position =
            self.table_shards[shard_number].position_of(pair_hash, table_key, claim_role);
        if let Some(bucket_position) = held_position {
            let bucket_place = BucketPlace {
                shard_number,
                bucket_position,
            };
            return (bucket_place, false);
        }
        let shard_grows = self.table_shards[shard_number].needs_growth_for_insert();
        if shard_grows {
            self.grow_shard(shard_number);
        }
        let inserted_bucket = ClaimedKeyBucket {
            table_key: *table_key,
            claim_role: claim_role as u8,
            ..ClaimedKeyBucket::EMPTY
        };
        let bucket_position =
            self.table_shards[shard_number].place_bucket(pair_hash, inserted_bucket);
        self.held_bucket_count += 1;
        let bucket_place = BucketPlace {
            shard_number,
            bucket_position,
        };
        (bucket_place, shard_grows)
    }

    /// Allocates the shard's first buckets, or doubles them and places its pairs again.
    fn grow_shard(&mut self, shard_number: usize) {
        let old_shard = std::mem::take(&mut self.table_shards[shard_number]);
        let grown_bucket_count = (old_shard.bucket_count() * 2).max(FIRST_SHARD_BUCKETS);
        let mut grown_shard = KeyTableShard::with_bucket_count(grown_bucket_count);
        let held_buckets = old_shard
            .bucket_tags
            .iter()
            .zip(old_shard.shard_buckets.iter());
        for (bucket_tag, held_bucket) in held_buckets {
            if *bucket_tag != EMPTY_BUCKET_TAG {
                let pair_hash = self.pair_hash(&held_bucket.table_key, held_bucket.bucket_role());
                grown_shard.place_bucket(pair_hash, *held_bucket);
            }
        }
        self.table_shards[shard_number] = grown_shard;
    }

    /// Removes the bucket once nothing claims it, and shifts the rest of its probe run back over
    /// the hole; `true` when it was removed.
    pub fn remove_if_unclaimed(&mut self, bucket_place: BucketPlace) -> bool {
        let table_shard = &self.table_shards[bucket_place.shard_number];
        if !table_shard.shard_buckets[bucket_place.bucket_position].is_unclaimed() {
            return false;
        }
        let bucket_mask = table_shard.bucket_count() - 1;
        let mut hole_position = bucket_place.bucket_position;
        let mut next_position = (hole_position + 1) & bucket_mask;
        loop {
            let table_shard = &self.table_shards[bucket_place.shard_number];
            if table_shard.bucket_tags[next_position] == EMPTY_BUCKET_TAG {
                break;
            }
            let next_bucket = table_shard.shard_buckets[next_position];
            let next_hash = self.pair_hash(&next_bucket.table_key, next_bucket.bucket_role());
            let home_position = next_hash as usize & bucket_mask;
            let distance_from_home = next_position.wrapping_sub(home_position) & bucket_mask;
            let distance_from_hole = next_position.wrapping_sub(hole_position) & bucket_mask;
            // A pair whose home is not between the hole and itself moves back into the hole.
            if distance_from_home >= distance_from_hole {
                let table_shard = &mut self.table_shards[bucket_place.shard_number];
                table_shard.bucket_tags[hole_position] = table_shard.bucket_tags[next_position];
                table_shard.shard_buckets[hole_position] = next_bucket;
                hole_position = next_position;
            }
            next_position = (next_position + 1) & bucket_mask;
        }
        let table_shard = &mut self.table_shards[bucket_place.shard_number];
        table_shard.bucket_tags[hole_position] = EMPTY_BUCKET_TAG;
        table_shard.shard_buckets[hole_position] = ClaimedKeyBucket::EMPTY;
        table_shard.held_bucket_count -= 1;
        self.held_bucket_count -= 1;
        true
    }

    pub const fn held_bucket_count(&self) -> usize {
        self.held_bucket_count
    }

    /// The shards by shard number, empty before the first pair.
    pub fn table_shards(&self) -> &[KeyTableShard] {
        &self.table_shards
    }

    pub fn for_each_held_bucket(&self, mut visit_bucket: impl FnMut(&ClaimedKeyBucket)) {
        for table_shard in self.table_shards.iter() {
            let held_buckets = table_shard
                .bucket_tags
                .iter()
                .zip(table_shard.shard_buckets.iter());
            for (bucket_tag, held_bucket) in held_buckets {
                if *bucket_tag != EMPTY_BUCKET_TAG {
                    visit_bucket(held_bucket);
                }
            }
        }
    }

    /// The shard headers' bytes, plus each bucket's tag and bucket.
    pub fn held_bytes(&self) -> usize {
        let bucket_bytes = size_of::<u8>() + size_of::<ClaimedKeyBucket>();
        let bucket_counts = self.table_shards.iter().map(KeyTableShard::bucket_count);
        size_of::<KeyTableShard>() * self.table_shards.len()
            + bucket_counts.sum::<usize>() * bucket_bytes
    }
}

#[cfg(test)]
mod tests {
    use {super::*, foldhash::HashMap};

    fn numbered_table_key(key_number: u64) -> [u8; 32] {
        let mut table_key = [0; 32];
        table_key[..8].copy_from_slice(&key_number.to_le_bytes());
        table_key
    }

    /// Every pair in shard 0, at one of the last four home buckets and with one of two tags, so
    /// probe runs grow long and wrap around.
    fn colliding_pair_hash(table_key: &[u8; 32], claim_role: ClaimRole) -> u64 {
        let spread_bits = u64::from(table_key[0] ^ claim_role as u8);
        let home_bits = ((1_u64 << 40) - 1) - (spread_bits & 3);
        home_bits | (((spread_bits >> 2) & 1) << 57)
    }

    /// The table beside a model of each held pair's claim ref.
    struct ModelledTable {
        claimed_key_table: ClaimedKeyTable,
        modelled_refs: HashMap<([u8; 32], ClaimRole), ClaimRef>,
    }

    impl ModelledTable {
        fn claim_pair(&mut self, table_key: [u8; 32], claim_role: ClaimRole, claim_ref: ClaimRef) {
            let (bucket_place, _) = self
                .claimed_key_table
                .find_or_insert(&table_key, claim_role);
            self.claimed_key_table.bucket_at_mut(bucket_place).claim_ref = claim_ref;
            self.modelled_refs
                .insert((table_key, claim_role), claim_ref);
        }

        fn release_pair(&mut self, table_key: [u8; 32], claim_role: ClaimRole) {
            let bucket_place = self
                .claimed_key_table
                .locate_bucket(&table_key, claim_role)
                .unwrap();
            self.claimed_key_table.bucket_at_mut(bucket_place).claim_ref = ClaimRef::NONE;
            assert!(self.claimed_key_table.remove_if_unclaimed(bucket_place));
            self.modelled_refs.remove(&(table_key, claim_role));
        }

        /// Every probed pair is found with the model's claim ref, or not found, and the counts
        /// agree; every held pair must be among `probed_pairs`.
        fn assert_matches_model(&self, probed_pairs: &[([u8; 32], ClaimRole)]) {
            for (table_key, claim_role) in probed_pairs {
                let found_ref = self
                    .claimed_key_table
                    .find_bucket(table_key, *claim_role)
                    .map(|found_bucket| found_bucket.claim_ref);
                let modelled_ref = self.modelled_refs.get(&(*table_key, *claim_role)).copied();
                assert_eq!(found_ref, modelled_ref, "{table_key:?} {claim_role:?}");
            }
            let held_count = self.modelled_refs.len();
            assert_eq!(self.claimed_key_table.held_bucket_count(), held_count);
            let mut visited_count = 0;
            self.claimed_key_table
                .for_each_held_bucket(|_| visited_count += 1);
            assert_eq!(visited_count, held_count);
            let shard_counts = self.claimed_key_table.table_shards().iter();
            let shard_count_sum = shard_counts
                .map(KeyTableShard::held_bucket_count)
                .sum::<usize>();
            assert_eq!(shard_count_sum, held_count);
        }
    }

    /// Seeded inserts and removals, under the seeded hash and under one where every pair collides,
    /// leave the table holding what a model holds: backward shifts keep every pair reachable.
    #[test]
    fn the_table_matches_a_model_through_random_inserts_and_removals() {
        let forced_hashes: [Option<ForcedPairHash>; 2] = [None, Some(colliding_pair_hash)];
        for forced_pair_hash in forced_hashes {
            for rng_seed in 0..16 {
                let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
                let claimed_key_table = ClaimedKeyTable {
                    forced_pair_hash,
                    ..Default::default()
                };
                let mut modelled_table = ModelledTable {
                    claimed_key_table,
                    modelled_refs: HashMap::default(),
                };
                let key_count = if forced_pair_hash.is_some() { 64 } else { 3000 };
                let pair_pool = (0..key_count)
                    .flat_map(|key_number| {
                        let table_key = numbered_table_key(key_number);
                        ClaimRole::ALL.map(|claim_role| (table_key, claim_role))
                    })
                    .collect::<Vec<_>>();
                for _ in 0..4 * pair_pool.len() {
                    let (table_key, claim_role) = *seeded_rng.choice(&pair_pool).unwrap();
                    if modelled_table
                        .modelled_refs
                        .contains_key(&(table_key, claim_role))
                    {
                        modelled_table.release_pair(table_key, claim_role);
                    } else {
                        let claim_ref = ClaimRef::one_slot(seeded_rng.u32(..1000));
                        modelled_table.claim_pair(table_key, claim_role, claim_ref);
                    }
                }
                modelled_table.assert_matches_model(&pair_pool);
                let held_pairs = modelled_table
                    .modelled_refs
                    .keys()
                    .copied()
                    .collect::<Vec<_>>();
                for (table_key, claim_role) in held_pairs {
                    modelled_table.release_pair(table_key, claim_role);
                }
                modelled_table.assert_matches_model(&pair_pool);
            }
        }
    }

    /// An insert reports a growth exactly when it allocates or doubles its shard, never for a held
    /// pair.
    #[test]
    fn an_insert_reports_a_growth_only_when_it_allocates_or_doubles_its_shard() {
        let mut claimed_key_table = ClaimedKeyTable::default();
        let shard_number = CLAIMED_KEY_SHARDS / 3;
        let shard_keys = (0..)
            .map(numbered_table_key)
            .filter(|table_key| {
                let pair_hash = claimed_key_table.pair_hash(table_key, ClaimRole::Account);
                shard_number_of(pair_hash) == shard_number
            })
            .take(200)
            .collect::<Vec<_>>();
        let mut reported_growths = 0;
        for (held_count, table_key) in shard_keys.iter().enumerate() {
            let buckets_before = claimed_key_table
                .table_shards()
                .get(shard_number)
                .map_or(0, KeyTableShard::bucket_count);
            let (_, shard_grew) = claimed_key_table.find_or_insert(table_key, ClaimRole::Account);
            let buckets_after = claimed_key_table.table_shards()[shard_number].bucket_count();
            assert_eq!(shard_grew, buckets_after != buckets_before);
            assert_eq!(shard_grew, (held_count + 1) * 2 > buckets_before);
            let (_, held_pair_grew) =
                claimed_key_table.find_or_insert(table_key, ClaimRole::Account);
            assert!(!held_pair_grew);
            reported_growths += usize::from(shard_grew);
        }
        // 8, 16, ..., 512 buckets hold 200 pairs at a load limit of 1/2.
        assert_eq!(reported_growths, 7);
        let grown_shard = &claimed_key_table.table_shards()[shard_number];
        assert_eq!(grown_shard.bucket_count(), 512);
        assert_eq!(claimed_key_table.held_bucket_count(), 200);
    }

    /// Over 600,000 pairs, each growth is of a shard with at most twice the mean of 4,096 shards.
    #[test]
    fn a_shard_growth_moves_one_shard_only() {
        const INSERTED_PAIR_COUNT: u64 = 600_000;
        let mut claimed_key_table = ClaimedKeyTable::default();
        // A literal 4,096, so a table with fewer shards fails the limit.
        let grown_shard_limit = 2 * INSERTED_PAIR_COUNT as usize / 4096;
        let mut reported_growth_count = 0;
        for key_number in 0..INSERTED_PAIR_COUNT {
            let table_key = numbered_table_key(key_number);
            let (bucket_place, shard_grew) =
                claimed_key_table.find_or_insert(&table_key, ClaimRole::Transaction);
            if shard_grew {
                let grown_shard = &claimed_key_table.table_shards()[bucket_place.shard_number];
                let grown_shard_count = grown_shard.held_bucket_count();
                assert!(
                    grown_shard_count <= grown_shard_limit,
                    "{grown_shard_count}"
                );
                reported_growth_count += 1;
            }
        }
        assert!(reported_growth_count > CLAIMED_KEY_SHARDS);
        let shard_counts = claimed_key_table.table_shards().iter();
        let largest_shard_count = shard_counts
            .map(KeyTableShard::held_bucket_count)
            .max()
            .unwrap();
        assert!(
            largest_shard_count <= grown_shard_limit,
            "{largest_shard_count}"
        );
        assert_eq!(
            claimed_key_table.held_bucket_count(),
            INSERTED_PAIR_COUNT as usize
        );
    }

    /// The pairs of one shard take many home buckets and tags, and both values of every hash bit
    /// that places or tags a pair.
    #[test]
    fn the_pairs_of_one_shard_spread_over_their_home_buckets_and_tags() {
        let claimed_key_table = ClaimedKeyTable::default();
        let shard_pair_hashes = (0..)
            .flat_map(|key_number| {
                ClaimRole::ALL.map(|claim_role| (numbered_table_key(key_number), claim_role))
            })
            .map(|(table_key, claim_role)| claimed_key_table.pair_hash(&table_key, claim_role))
            .filter(|pair_hash| shard_number_of(*pair_hash) == 0)
            .take(64)
            .collect::<Vec<_>>();
        let home_buckets = shard_pair_hashes
            .iter()
            .map(|pair_hash| pair_hash % 64)
            .collect::<foldhash::HashSet<_>>();
        let pair_tags = shard_pair_hashes
            .iter()
            .map(|pair_hash| bucket_tag_of(*pair_hash))
            .collect::<foldhash::HashSet<_>>();
        assert!(home_buckets.len() >= 24, "{home_buckets:?}");
        assert!(pair_tags.len() >= 24, "{pair_tags:?}");
        let bits_set_in_every_hash = shard_pair_hashes
            .iter()
            .fold(u64::MAX, |common_bits, pair_hash| common_bits & pair_hash);
        let bits_set_in_any_hash = shard_pair_hashes
            .iter()
            .fold(0, |seen_bits, pair_hash| seen_bits | pair_hash);
        let home_and_tag_bits = ((1_u64 << 40) - 1) | (u64::MAX << 57);
        let fixed_bits = (bits_set_in_every_hash | !bits_set_in_any_hash) & home_and_tag_bits;
        assert_eq!(fixed_bits, 0, "{fixed_bits:#018x}");
    }

    /// One 32-byte value under every role is five pairs, each found under its own role only; an
    /// owner bucket with a memcmp gate stays until the gate goes.
    #[test]
    fn one_key_under_each_role_is_a_pair_of_its_own() {
        let mut claimed_key_table = ClaimedKeyTable::default();
        let shared_key = numbered_table_key(7);
        for (role_number, claim_role) in ClaimRole::ALL.into_iter().enumerate() {
            let (bucket_place, _) = claimed_key_table.find_or_insert(&shared_key, claim_role);
            claimed_key_table.bucket_at_mut(bucket_place).claim_ref =
                ClaimRef::one_slot(role_number as u32);
        }
        assert_eq!(claimed_key_table.held_bucket_count(), CLAIM_ROLE_COUNT);
        for (role_number, claim_role) in ClaimRole::ALL.into_iter().enumerate() {
            let found_bucket = claimed_key_table
                .find_bucket(&shared_key, claim_role)
                .unwrap();
            assert_eq!(
                found_bucket.claim_ref,
                ClaimRef::one_slot(role_number as u32)
            );
            assert_eq!(found_bucket.bucket_role(), claim_role);
        }
        let owner_place = claimed_key_table
            .locate_bucket(&shared_key, ClaimRole::Owner)
            .unwrap();
        let owner_bucket = claimed_key_table.bucket_at_mut(owner_place);
        owner_bucket.claim_ref = ClaimRef::NONE;
        owner_bucket.memcmp_gate_count = 1;
        assert!(!claimed_key_table.remove_if_unclaimed(owner_place));
        claimed_key_table
            .bucket_at_mut(owner_place)
            .memcmp_gate_count = 0;
        assert!(claimed_key_table.remove_if_unclaimed(owner_place));
        let owner_probe = claimed_key_table.find_bucket(&shared_key, ClaimRole::Owner);
        assert!(owner_probe.is_none());
        let account_probe = claimed_key_table.find_bucket(&shared_key, ClaimRole::Account);
        assert!(account_probe.is_some());
        assert_eq!(claimed_key_table.held_bucket_count(), CLAIM_ROLE_COUNT - 1);
    }
}
