use std::hash::{DefaultHasher, Hash, Hasher};
use crate::geyser::CuckooFilter as ProtoCuckooFilter;


const ENTRIES_PER_BUCKET: usize = 4;
const LOAD_FACTOR: f64 = 0.95;
const MAX_KICKS: usize = 500;


type Fingerprint = u16;
type Bucket = [Fingerprint; ENTRIES_PER_BUCKET];

const HASH_SEED: u64 = 0x_796c_6c77_7374_6e21;

pub struct CuckooFilter {
    buckets: Vec<Bucket>,
}

impl CuckooFilter {
    pub fn with_capacity(max_capacity: usize) -> Self {
        let buckets_needed = (max_capacity as f64 / (LOAD_FACTOR * ENTRIES_PER_BUCKET as f64)).ceil() as usize;
        let bucket_count = buckets_needed.next_power_of_two();

        Self {
            buckets: vec![[0; ENTRIES_PER_BUCKET]; bucket_count],
        }
    }

    pub fn insert<T: Hash>(&mut self, item: &T) -> bool {
        let fp = Self::fingerprint(item);
        let h = Self::hash(item);
        let i1 = self.index(h);
        let i2 = i1 ^ self.index(Self::hash(&fp));

        if self.try_insert_to_bucket(i1, fp) {
            return true;
        }
        
        if self.try_insert_to_bucket(i2, fp) {
            return true;
        }

        let mut i = i1;  // start with bucket i1
        let mut fp = fp;

        for n in 0..MAX_KICKS {
            let slot = (n + fp as usize) % ENTRIES_PER_BUCKET;
            std::mem::swap(&mut fp, &mut self.buckets[i][slot]);
            
            // find alternate bucket for kicked fingerprint
            i = i ^ self.index(Self::hash(&fp));
            
            // try to insert kicked fingerprint
            if self.try_insert_to_bucket(i, fp) {
                return true;
            }
        }

        false  // table full
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let fp = Self::fingerprint(item);
        let h = Self::hash(item);
        let i1 = self.index(h);
        let i2 = i1 ^ self.index(Self::hash(&fp));

        self.bucket_contains(i1, fp) || self.bucket_contains(i2, fp)
    }

    pub fn remove<T: Hash>(&mut self, item: &T) -> bool {
        let fp = Self::fingerprint(item);
        let h = Self::hash(item);
        let i1 = self.index(h);
        let i2 = i1 ^ self.index(Self::hash(&fp));

        self.try_remove_from_bucket(i1, fp) || self.try_remove_from_bucket(i2, fp)
    }



    fn hash<T: Hash>(item: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        HASH_SEED.hash(&mut hasher);
        item.hash(&mut hasher);
        hasher.finish()
    }

    fn fingerprint<T: Hash>(item: &T) -> Fingerprint {
        let h = Self::hash(item);
        let fp = (h >> 32) as u16;
        if fp == 0 { 1 } else { fp }
    }

    fn index(&self, hash: u64) -> usize {
        hash as usize & (self.buckets.len() - 1)
    }

    fn try_insert_to_bucket(&mut self, index: usize, fp: Fingerprint) -> bool {
        for slot in &mut self.buckets[index] {
            if *slot == 0 {
                *slot = fp;
                return true;
            }
        }
        false
    }

    fn try_remove_from_bucket(&mut self, index: usize, fp: Fingerprint) -> bool {
        for slot in &mut self.buckets[index] {
            if *slot == fp {
                *slot = 0;  // mark empty
                return true;
            }
        }
        false
    }


    fn bucket_contains(&self, index: usize, fp: Fingerprint) -> bool {
        self.buckets[index].iter().any(|&x| x == fp)
    }
}

impl From<&ProtoCuckooFilter> for CuckooFilter {
    fn from(proto: &ProtoCuckooFilter) -> Self {
        todo!()
    }
}

impl From<&CuckooFilter> for ProtoCuckooFilter {
    fn from(filter: &CuckooFilter) -> Self {
        todo!()
    }
}
