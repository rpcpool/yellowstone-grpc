use {
    solana_sdk::{clock::MAX_RECENT_BLOCKHASHES, hash::Hash, timing::timestamp},
    std::collections::HashMap,
};

#[derive(Debug, PartialEq, Eq, Clone)]
struct HashAge {
    hash_index: u64,
    timestamp: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockhashQueue {
    /// index of last hash to be registered
    last_hash_index: u64,

    /// last hash to be registered
    last_hash: Option<Hash>,

    ages: HashMap<Hash, HashAge>,

    /// hashes older than `max_age` will be dropped from the queue
    max_age: usize,
}

impl Default for BlockhashQueue {
    fn default() -> Self {
        Self::new(MAX_RECENT_BLOCKHASHES)
    }
}

impl BlockhashQueue {
    pub fn new(max_age: usize) -> Self {
        Self {
            ages: HashMap::new(),
            last_hash_index: 0,
            last_hash: None,
            max_age,
        }
    }

    pub fn last_hash(&self) -> Hash {
        self.last_hash.expect("no hash has been set")
    }

    /// Check if the age of the hash is within the queue's max age
    pub fn is_hash_valid(&self, hash: &Hash) -> bool {
        self.ages.get(hash).is_some()
    }

    /// Check if the age of the hash is within the specified age
    pub fn is_hash_valid_for_age(&self, hash: &Hash, max_age: usize) -> bool {
        self.ages
            .get(hash)
            .map(|age| Self::is_hash_index_valid(self.last_hash_index, max_age, age.hash_index))
            .unwrap_or(false)
    }

    #[allow(dead_code)]
    pub fn get_hash_age(&self, hash: &Hash) -> Option<u64> {
        self.ages
            .get(hash)
            .map(|age| self.last_hash_index - age.hash_index)
    }

    fn is_hash_index_valid(last_hash_index: u64, max_age: usize, hash_index: u64) -> bool {
        last_hash_index - hash_index <= max_age as u64
    }

    pub fn is_full(&self) -> bool {
        self.ages.len() >= self.max_age
    }

    pub fn register_hash(&mut self, hash: &Hash) {
        self.last_hash_index += 1;
        if self.ages.len() >= self.max_age {
            self.ages.retain(|_, age| {
                Self::is_hash_index_valid(self.last_hash_index, self.max_age, age.hash_index)
            });
        }

        self.ages.insert(
            *hash,
            HashAge {
                hash_index: self.last_hash_index,
                timestamp: timestamp(),
            },
        );

        self.last_hash = Some(*hash);
    }

    #[allow(dead_code)]
    pub(crate) fn get_max_age(&self) -> usize {
        self.max_age
    }
}

#[cfg(test)]
mod tests {
    use {
        super::BlockhashQueue,
        bincode::serialize,
        solana_sdk::{hash::hash, hash::Hash},
    };

    #[test]
    fn test_register_hash() {
        let last_hash = Hash::default();
        let mut hash_queue = BlockhashQueue::new(100);
        assert!(!hash_queue.is_hash_valid(&last_hash));
        hash_queue.register_hash(&last_hash);
        assert!(hash_queue.is_hash_valid(&last_hash));
        assert_eq!(hash_queue.last_hash_index, 1);
    }

    #[test]
    fn test_reject_old_last_hash() {
        let mut hash_queue = BlockhashQueue::new(100);
        let last_hash = hash(&serialize(&0).unwrap());
        for i in 0..102 {
            let last_hash = hash(&serialize(&i).unwrap());
            hash_queue.register_hash(&last_hash);
        }
        // Assert we're no longer able to use the oldest hash.
        assert!(!hash_queue.is_hash_valid(&last_hash));
        assert!(!hash_queue.is_hash_valid_for_age(&last_hash, 0));

        // Assert we are not able to use the oldest remaining hash.
        let last_valid_hash = hash(&serialize(&1).unwrap());
        assert!(hash_queue.is_hash_valid(&last_valid_hash));
        assert!(!hash_queue.is_hash_valid_for_age(&last_valid_hash, 0));
    }

    /// test that when max age is 0, that a valid last_hash still passes the age check
    #[test]
    fn test_queue_init_blockhash() {
        let last_hash = Hash::default();
        let mut hash_queue = BlockhashQueue::new(100);
        hash_queue.register_hash(&last_hash);
        assert_eq!(last_hash, hash_queue.last_hash());
        assert!(hash_queue.is_hash_valid_for_age(&last_hash, 0));
    }

    #[test]
    fn test_len() {
        const MAX_AGE: usize = 10;
        let mut hash_queue = BlockhashQueue::new(MAX_AGE);
        assert_eq!(hash_queue.ages.len(), 0);

        for _ in 0..MAX_AGE {
            hash_queue.register_hash(&Hash::new_unique());
        }
        assert_eq!(hash_queue.ages.len(), MAX_AGE);

        // Show that the queue actually holds one more entry than the max age.
        // This is because the most recent hash is considered to have an age of 0,
        // which is likely the result of an unintentional off-by-one error in the past.
        hash_queue.register_hash(&Hash::new_unique());
        assert_eq!(hash_queue.ages.len(), MAX_AGE + 1);

        // Ensure that no additional entries beyond `MAX_AGE + 1` are added
        hash_queue.register_hash(&Hash::new_unique());
        assert_eq!(hash_queue.ages.len(), MAX_AGE + 1);
    }

    #[test]
    fn test_get_hash_age() {
        const MAX_AGE: usize = 10;
        let mut hash_list: Vec<Hash> = Vec::new();
        hash_list.resize_with(MAX_AGE + 1, Hash::new_unique);

        let mut hash_queue = BlockhashQueue::new(MAX_AGE);
        for hash in &hash_list {
            assert!(hash_queue.get_hash_age(hash).is_none());
        }

        for hash in &hash_list {
            hash_queue.register_hash(hash);
        }

        // Note that the "age" of a hash in the queue is 0-indexed. So when processing
        // transactions in block 50, the hash for block 49 has an age of 0 despite
        // being one block in the past.
        for (age, hash) in hash_list.iter().rev().enumerate() {
            assert_eq!(hash_queue.get_hash_age(hash), Some(age as u64));
        }

        // When the oldest hash is popped, it should no longer return a hash age
        hash_queue.register_hash(&Hash::new_unique());
        assert!(hash_queue.get_hash_age(&hash_list[0]).is_none());
    }

    #[test]
    fn test_is_hash_valid_for_age() {
        const MAX_AGE: usize = 10;
        let mut hash_list: Vec<Hash> = Vec::new();
        hash_list.resize_with(MAX_AGE + 1, Hash::new_unique);

        let mut hash_queue = BlockhashQueue::new(MAX_AGE);
        for hash in &hash_list {
            assert!(!hash_queue.is_hash_valid_for_age(hash, MAX_AGE));
        }

        for hash in &hash_list {
            hash_queue.register_hash(hash);
        }

        // Note that the "age" of a hash in the queue is 0-indexed. So when checking
        // the age of a hash is within max age, the hash from 11 slots ago is considered
        // to be within the max age of 10.
        for hash in &hash_list {
            assert!(hash_queue.is_hash_valid_for_age(hash, MAX_AGE));
        }

        // When max age is 0, only the most recent blockhash is still considered valid
        assert!(hash_queue.is_hash_valid_for_age(&hash_list[MAX_AGE], 0));
        assert!(!hash_queue.is_hash_valid_for_age(&hash_list[MAX_AGE - 1], 0));
    }
}
