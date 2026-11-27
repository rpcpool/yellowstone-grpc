//! The claimed key table: one bucket per claimed (32-byte key, claim role) pair, in 4,096
//! open-addressing shards that grow on their own, so a growth moves one shard's pairs only.

use {super::key_claim_index::ClaimRef, foldhash::fast::RandomState, std::hash::BuildHasher};

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

/// A level's claimed pairs in [`CLAIMED_KEY_SHARDS`] shards that share one seeded hash. A removal
/// shifts the rest of its probe run back, so no tombstone is left.
pub struct ClaimedKeyTable {
    hash_seeds: [u64; 4],
    /// Empty until the first pair is inserted.
    table_shards: Box<[KeyTableShard]>,
    held_bucket_count: usize,
}

impl Default for ClaimedKeyTable {
    fn default() -> Self {
        let seed_source = RandomState::default();
        let hash_seeds = std::array::from_fn(|seed_number| seed_source.hash_one(seed_number) | 1);
        Self {
            hash_seeds,
            table_shards: Box::default(),
            held_bucket_count: 0,
        }
    }
}

impl ClaimedKeyTable {
    /// The pair's hash: folded multiplies of the key's four words, the role and the seeds.
    #[inline(always)]
    pub fn pair_hash(&self, table_key: &[u8; 32], claim_role: ClaimRole) -> u64 {
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
}
