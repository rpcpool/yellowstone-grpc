use std::collections::HashSet;
use std::hash::Hash;
use yellowstone_grpc_proto::cuckoo::CuckooFilter;
use yellowstone_grpc_proto::geyser::{
    CuckooFilter as ProtoCuckooFilter,
    SubscribeRequest,
};

pub struct LocalCuckooMap<T> {
    items: HashSet<T>,
    filter: CuckooFilter,
    dirty: bool,
}

impl<T> LocalCuckooMap<T>
where
    T: Sized + Hash + Eq,
{
    pub fn with_capacity(max_capacity: usize) -> Self {
        todo!()
    }

    pub fn insert(&mut self, v: T) -> bool {
        todo!()
    }

    pub fn remove(&mut self, v: &T) -> bool {
        todo!()
    }

    pub fn contains(&self, v: &T) -> bool {
        todo!()
    }

    pub fn len(&self) -> usize {
        todo!()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_proto_cuckoo_filter(&self) -> ProtoCuckooFilter {
        todo!()
    }

    pub fn override_subscribe_request(&self, req: &mut SubscribeRequest) {
        todo!()
    }
}
