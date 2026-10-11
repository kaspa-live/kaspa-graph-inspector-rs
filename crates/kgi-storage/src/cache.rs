use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};
use kgi_model::block::{BlockCoordinate, BlockHash, CompactId};
use moka::sync::Cache;

const BLOCK_CACHE_CAPACITY: u64 = 50_000;
const VSPC_SINK_CACHE_CAPACITY: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CachedIdentity {
    pub(crate) id: CompactId,
    pub(crate) materialized: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CachedMaterializedBlock {
    pub(crate) coordinate: BlockCoordinate,
    pub(crate) selected_parent_id: CompactId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CachedMergeSets {
    pub(crate) blue: Arc<[CompactId]>,
    pub(crate) red: Arc<[CompactId]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CachedVspcSink {
    pub(crate) id: CompactId,
    pub(crate) hash: BlockHash,
}

pub(crate) struct ProcessingCacheDomains {
    identities: Cache<BlockHash, CachedIdentity>,
    materialized_blocks: Cache<CompactId, CachedMaterializedBlock>,
    merge_sets: Cache<CompactId, CachedMergeSets>,
    vspc_sink: Cache<(), CachedVspcSink>,
}

impl ProcessingCacheDomains {
    pub(crate) fn new() -> Self {
        Self::with_capacities(BLOCK_CACHE_CAPACITY, BLOCK_CACHE_CAPACITY, BLOCK_CACHE_CAPACITY, VSPC_SINK_CACHE_CAPACITY)
    }

    fn with_capacities(
        identity_capacity: u64,
        materialized_block_capacity: u64,
        merge_set_capacity: u64,
        vspc_sink_capacity: u64,
    ) -> Self {
        Self {
            identities: Cache::new(identity_capacity),
            materialized_blocks: Cache::new(materialized_block_capacity),
            merge_sets: Cache::new(merge_set_capacity),
            vspc_sink: Cache::new(vspc_sink_capacity),
        }
    }

    pub(crate) fn identity(&self, hash: BlockHash) -> Option<CachedIdentity> {
        self.identities.get(&hash)
    }

    pub(crate) fn materialized_block(&self, id: CompactId) -> Option<CachedMaterializedBlock> {
        self.materialized_blocks.get(&id)
    }

    pub(crate) fn merge_sets(&self, id: CompactId) -> Option<CachedMergeSets> {
        self.merge_sets.get(&id)
    }

    pub(crate) fn vspc_sink(&self) -> Option<CachedVspcSink> {
        self.vspc_sink.get(&())
    }

    pub(crate) fn publish_identity(&self, hash: BlockHash, identity: CachedIdentity) {
        self.identities.insert(hash, identity);
    }

    pub(crate) fn publish_materialized_block(&self, id: CompactId, block: CachedMaterializedBlock) {
        self.materialized_blocks.insert(id, block);
    }

    pub(crate) fn publish_merge_sets(&self, id: CompactId, merge_sets: CachedMergeSets) {
        self.merge_sets.insert(id, merge_sets);
    }

    pub(crate) fn publish_vspc_sink(&self, sink: CachedVspcSink) {
        self.vspc_sink.insert((), sink);
    }
}

pub(crate) struct ProcessingCaches {
    current: ArcSwap<ProcessingCacheDomains>,
}

impl ProcessingCaches {
    pub(crate) fn new() -> Self {
        Self { current: ArcSwap::from_pointee(ProcessingCacheDomains::new()) }
    }

    pub(crate) fn load(&self) -> Guard<Arc<ProcessingCacheDomains>> {
        self.current.load()
    }

    pub(crate) fn load_full(&self) -> Arc<ProcessingCacheDomains> {
        self.current.load_full()
    }

    pub(crate) fn replace(&self, replacement: Arc<ProcessingCacheDomains>) -> Arc<ProcessingCacheDomains> {
        self.current.swap(replacement)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kgi_model::block::{BlockCoordinate, BlockHash, CompactId};

    use super::{CachedIdentity, CachedMaterializedBlock, CachedMergeSets, CachedVspcSink, ProcessingCacheDomains, ProcessingCaches};

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn id(value: i64) -> CompactId {
        CompactId::new(value).expect("positive compact ID")
    }

    #[test]
    fn default_capacities_match_the_four_processing_cache_domains() {
        let caches = ProcessingCacheDomains::new();

        assert_eq!(caches.identities.policy().max_capacity(), Some(50_000));
        assert_eq!(caches.materialized_blocks.policy().max_capacity(), Some(50_000));
        assert_eq!(caches.merge_sets.policy().max_capacity(), Some(50_000));
        assert_eq!(caches.vspc_sink.policy().max_capacity(), Some(1));
    }

    #[test]
    fn caches_have_independent_capacity_policies() {
        let caches = ProcessingCacheDomains::with_capacities(2, 2, 2, 1);
        let coordinate = |level| BlockCoordinate::new(level, 0).expect("positive level");

        for value in 1..=2 {
            caches.publish_identity(hash(value), CachedIdentity { id: id(i64::from(value)), materialized: true });
            caches.publish_materialized_block(
                id(i64::from(value)),
                CachedMaterializedBlock { coordinate: coordinate(u64::from(value)), selected_parent_id: id(4) },
            );
            caches.publish_merge_sets(id(i64::from(value)), CachedMergeSets { blue: vec![id(5)].into(), red: vec![id(6)].into() });
        }
        caches.publish_vspc_sink(CachedVspcSink { id: id(2), hash: hash(2) });

        assert_eq!(caches.identities.policy().max_capacity(), Some(2));
        assert_eq!(caches.materialized_blocks.policy().max_capacity(), Some(2));
        assert_eq!(caches.merge_sets.policy().max_capacity(), Some(2));
        assert_eq!(caches.vspc_sink.policy().max_capacity(), Some(1));
        assert_eq!(caches.identity(hash(1)).map(|identity| identity.id), Some(id(1)));
        assert_eq!(caches.materialized_block(id(1)).map(|block| block.coordinate), Some(coordinate(1)));
        assert_eq!(caches.merge_sets(id(1)).map(|sets| sets.blue), Some(vec![id(5)].into()));
        assert_eq!(caches.vspc_sink(), Some(CachedVspcSink { id: id(2), hash: hash(2) }));

        caches.publish_identity(hash(3), CachedIdentity { id: id(3), materialized: true });
        caches.publish_materialized_block(id(3), CachedMaterializedBlock { coordinate: coordinate(3), selected_parent_id: id(4) });
        caches.publish_merge_sets(id(3), CachedMergeSets { blue: vec![id(7)].into(), red: vec![id(8)].into() });
        caches.publish_vspc_sink(CachedVspcSink { id: id(3), hash: hash(3) });
        caches.identities.run_pending_tasks();
        caches.materialized_blocks.run_pending_tasks();
        caches.merge_sets.run_pending_tasks();
        caches.vspc_sink.run_pending_tasks();

        assert!(caches.identities.entry_count() <= 2);
        assert!(caches.materialized_blocks.entry_count() <= 2);
        assert!(caches.merge_sets.entry_count() <= 2);
        assert!(caches.vspc_sink.entry_count() <= 1);
    }

    #[test]
    fn vspc_sink_cache_replaces_and_recalls_the_latest_value() {
        let caches = ProcessingCacheDomains::new();
        let first = CachedVspcSink { id: id(1), hash: hash(1) };
        let second = CachedVspcSink { id: id(2), hash: hash(2) };

        caches.publish_vspc_sink(first);
        assert_eq!(caches.vspc_sink(), Some(first));

        caches.publish_vspc_sink(second);
        caches.vspc_sink.run_pending_tasks();

        assert_eq!(caches.vspc_sink(), Some(second));
        assert_eq!(caches.vspc_sink.entry_count(), 1);
    }

    #[test]
    fn processing_cache_domains_are_replaced_as_one_snapshot() {
        let caches = ProcessingCaches::new();
        let predecessor = caches.load_full();
        predecessor.publish_identity(hash(1), CachedIdentity { id: id(1), materialized: false });

        let replacement = Arc::new(ProcessingCacheDomains::new());
        replacement.publish_identity(hash(2), CachedIdentity { id: id(2), materialized: false });
        let displaced = caches.replace(replacement.clone());

        assert!(Arc::ptr_eq(&predecessor, &displaced));
        assert!(Arc::ptr_eq(&replacement, &caches.load_full()));
        assert_eq!(caches.load().identity(hash(1)), None);
        assert_eq!(caches.load().identity(hash(2)).map(|identity| identity.id), Some(id(2)));
        assert_eq!(predecessor.identity(hash(1)).map(|identity| identity.id), Some(id(1)));
        assert_eq!(predecessor.identity(hash(2)), None);
    }
}
