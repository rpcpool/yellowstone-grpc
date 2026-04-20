use std::hash::Hash;

const ENTRIES_PER_BUCKET: usize = 4;

type Fingerprint = u16;
type Bucket = [Fingerprint; ENTRIES_PER_BUCKET];

pub struct CuckooFilter {
    buckets: Vec<Bucket>,
    bucket_count: u32,
    entries_per_bucket: u32,
    fingerprint_bits: u32,
    hash_seed: u64,
}

impl CuckooFilter {
    pub fn with_capacity(max_capacity: usize) -> Self {
        todo!()
    }

    pub fn insert<T: Hash>(&mut self, item: &T) -> bool {
        todo!()
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        todo!()
    }

    pub fn remove<T: Hash>(&mut self, item: &T) -> bool {
        todo!()
    }
}