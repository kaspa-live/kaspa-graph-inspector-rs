use std::sync::Arc;

use kgi_model::block::{BlockCoordinate, BlockHash, CompactId};
use moka::sync::Cache;

const IDENTITY_CACHE_CAPACITY: u64 = 50_000;
const COORDINATE_CACHE_CAPACITY: u64 = 50_000;
const MERGE_SET_CACHE_CAPACITY: u64 = 50_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CachedIdentity {
    pub(crate) id: CompactId,
    pub(crate) materialized: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CachedMergeSets {
    pub(crate) blue: Arc<[CompactId]>,
    pub(crate) red: Arc<[CompactId]>,
}

pub(crate) struct ProcessingCaches {
    identities: Cache<BlockHash, CachedIdentity>,
    coordinates: Cache<CompactId, BlockCoordinate>,
    merge_sets: Cache<CompactId, CachedMergeSets>,
}

impl ProcessingCaches {
    pub(crate) fn new() -> Self {
        Self::with_capacities(IDENTITY_CACHE_CAPACITY, COORDINATE_CACHE_CAPACITY, MERGE_SET_CACHE_CAPACITY)
    }

    fn with_capacities(identity_capacity: u64, coordinate_capacity: u64, merge_set_capacity: u64) -> Self {
        Self {
            identities: Cache::new(identity_capacity),
            coordinates: Cache::new(coordinate_capacity),
            merge_sets: Cache::new(merge_set_capacity),
        }
    }

    pub(crate) fn identity(&self, hash: BlockHash) -> Option<CachedIdentity> {
        self.identities.get(&hash)
    }

    pub(crate) fn coordinate(&self, id: CompactId) -> Option<BlockCoordinate> {
        self.coordinates.get(&id)
    }

    #[allow(dead_code, reason = "consumed by the atomic VSPC transaction")]
    pub(crate) fn merge_sets(&self, id: CompactId) -> Option<CachedMergeSets> {
        self.merge_sets.get(&id)
    }

    pub(crate) fn publish_identity(&self, hash: BlockHash, identity: CachedIdentity, coordinate: Option<BlockCoordinate>) {
        self.identities.insert(hash, identity);
        if let Some(coordinate) = coordinate {
            self.coordinates.insert(identity.id, coordinate);
        }
    }

    pub(crate) fn publish_merge_sets(&self, id: CompactId, merge_sets: CachedMergeSets) {
        self.merge_sets.insert(id, merge_sets);
    }
}

#[cfg(test)]
mod tests {
    use kgi_model::block::{BlockCoordinate, BlockHash, CompactId};

    use super::{CachedIdentity, CachedMergeSets, ProcessingCaches};

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn id(value: i64) -> CompactId {
        CompactId::new(value).expect("positive compact ID")
    }

    #[test]
    fn default_capacities_limit_each_processing_cache_to_fifty_thousand_entries() {
        let caches = ProcessingCaches::new();

        assert_eq!(caches.identities.policy().max_capacity(), Some(50_000));
        assert_eq!(caches.coordinates.policy().max_capacity(), Some(50_000));
        assert_eq!(caches.merge_sets.policy().max_capacity(), Some(50_000));
    }

    #[test]
    fn caches_have_independent_capacity_policies() {
        let caches = ProcessingCaches::with_capacities(2, 2, 2);
        let coordinate = |level| BlockCoordinate::new(level, 0).expect("positive level");

        caches.publish_identity(hash(1), CachedIdentity { id: id(1), materialized: true }, Some(coordinate(1)));
        caches.publish_identity(hash(2), CachedIdentity { id: id(2), materialized: true }, Some(coordinate(2)));
        caches.publish_merge_sets(id(1), CachedMergeSets { blue: vec![id(4)].into(), red: vec![id(5)].into() });
        caches.publish_merge_sets(id(2), CachedMergeSets { blue: vec![id(6)].into(), red: vec![id(7)].into() });

        assert_eq!(caches.identities.policy().max_capacity(), Some(2));
        assert_eq!(caches.coordinates.policy().max_capacity(), Some(2));
        assert_eq!(caches.merge_sets.policy().max_capacity(), Some(2));
        assert_eq!(caches.identity(hash(1)).map(|identity| identity.id), Some(id(1)));
        assert_eq!(caches.coordinate(id(1)), Some(coordinate(1)));
        assert_eq!(caches.merge_sets(id(1)).map(|sets| sets.blue), Some(vec![id(4)].into()));

        caches.publish_identity(hash(3), CachedIdentity { id: id(3), materialized: true }, Some(coordinate(3)));
        caches.publish_merge_sets(id(3), CachedMergeSets { blue: vec![id(8)].into(), red: vec![id(9)].into() });
        caches.identities.run_pending_tasks();
        caches.coordinates.run_pending_tasks();
        caches.merge_sets.run_pending_tasks();

        assert!(caches.identities.entry_count() <= 2);
        assert!(caches.coordinates.entry_count() <= 2);
        assert!(caches.merge_sets.entry_count() <= 2);
    }
}
