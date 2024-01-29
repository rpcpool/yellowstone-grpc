use std::{
    cmp::Ord,
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
};

pub fn hash_hashmap<H, K, V>(state: &mut H, map: &HashMap<K, V>)
where
    H: Hasher,
    K: Ord + Hash,
    V: Ord + Hash,
{
    let mut vec = map.iter().collect::<Vec<(&K, &V)>>();
    vec.sort();
    vec.hash(state);
}

pub fn hash_hashmap_hashset<H, K, V>(state: &mut H, map: &HashMap<K, HashSet<V>>)
where
    H: Hasher,
    K: Ord + Hash,
    V: Ord + Hash,
{
    let mut vec = map
        .iter()
        .map(|(k, set)| {
            let mut vec = set.iter().collect::<Vec<&V>>();
            vec.sort();
            (k, vec)
        })
        .collect::<Vec<(&K, Vec<&V>)>>();
    vec.sort();
    vec.hash(state);
}

pub fn hash_hashset<H, K>(state: &mut H, set: &HashSet<K>)
where
    H: Hasher,
    K: Ord + Hash,
{
    let mut vec = set.iter().collect::<Vec<&K>>();
    vec.sort();
    vec.hash(state);
}
