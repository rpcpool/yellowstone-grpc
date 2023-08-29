use {
    std::{
        collections::{btree_map::Entry, BTreeMap, HashSet},
        sync::Arc,
    },
    tokio::sync::Mutex,
};

#[async_trait::async_trait]
pub trait KafkaDedup: Clone {
    async fn allowed(&self, slot: u64, hash: [u8; 32]) -> bool;
}

#[derive(Debug, Default, Clone)]
pub struct KafkaDedupMemory {
    inner: Arc<Mutex<BTreeMap<u64, HashSet<[u8; 32]>>>>,
}

#[async_trait::async_trait]
impl KafkaDedup for KafkaDedupMemory {
    async fn allowed(&self, slot: u64, hash: [u8; 32]) -> bool {
        let mut map = self.inner.lock().await;

        if let Some(key_slot) = map.keys().next().cloned() {
            if slot < key_slot {
                return false;
            }
        }

        match map.entry(slot) {
            Entry::Vacant(entry) => {
                entry.insert(HashSet::new()).insert(hash);

                // remove old sets, keep ~30sec log
                while let Some(key_slot) = map.keys().next().cloned() {
                    if key_slot < slot - 75 {
                        map.remove(&key_slot);
                    } else {
                        break;
                    }
                }

                true
            }
            Entry::Occupied(entry) => entry.into_mut().insert(hash),
        }
    }
}
