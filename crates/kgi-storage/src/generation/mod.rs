use std::{
    future::Future,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use kaspa_consensus_core::network::NetworkId;
use kgi_core::timing::Timing;
use kgi_model::{
    block::{BlockHash, BlockPresence, CompactId},
    lifecycle::PersistenceFault,
};
use sqlx::PgPool;

use tokio::sync::{Mutex, oneshot};

use crate::{
    cache::{CachedVspcSink, ProcessingCaches},
    error::StorageError,
    identity,
    operation::ResolveMaterializedIdsError,
    runtime::{RetirementRequest, RetirementSender, RetirementTarget},
    state::ProcessingStateInspection,
};

#[cfg(test)]
use crate::cache::CachedMaterializedBlock;

mod materialization;
mod vspc;

/// Immutable network binding of one database generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DatabaseBinding {
    network_id: NetworkId,
    genesis_hash: BlockHash,
}

impl DatabaseBinding {
    pub(crate) const fn new(network_id: NetworkId, genesis_hash: BlockHash) -> Self {
        Self { network_id, genesis_hash }
    }

    /// Returns the exact network type and suffix bound to the database.
    #[must_use]
    pub const fn network_id(&self) -> NetworkId {
        self.network_id
    }

    /// Returns the immutable Genesis hash bound to the database.
    #[must_use]
    pub const fn genesis_hash(&self) -> BlockHash {
        self.genesis_hash
    }
}

/// Local processing state loaded once while preparing a recovery session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoredSessionState {
    Empty,
    Inconsistent,
    Initialized(StoredSessionSnapshot),
}

/// Persisted values required to prepare Resync against one coherent database snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredSessionSnapshot {
    pub db_pp_hash: BlockHash,
    pub db_pp_blue_score: u64,
    pub committed_vspc_sink: StoredVspcSink,
}

/// Persisted committed VSPC sink fields enriched by NodeService during Resync preparation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredVspcSink {
    pub hash: BlockHash,
    pub id: CompactId,
    pub selected_parent: BlockHash,
    pub daa_score: u64,
}

trait GenerationKind: Sized {
    fn retirement_target(generation: Weak<Self>) -> RetirementTarget;
}

struct GenerationRuntime<T> {
    pool: PgPool,
    valid: AtomicBool,
    retirement_tx: RetirementSender,
    self_weak: Weak<T>,
}

impl<T: GenerationKind> GenerationRuntime<T> {
    fn new(pool: PgPool, retirement_tx: RetirementSender, self_weak: Weak<T>) -> Self {
        Self { pool, valid: AtomicBool::new(true), retirement_tx, self_weak }
    }

    const fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    fn retire(&self) -> bool {
        self.valid.swap(false, Ordering::AcqRel)
    }

    fn begin_close(&self) {
        // SQLx marks the pool closed before returning the future that drains it.
        drop(self.pool.close());
    }

    async fn close(&self) {
        self.pool.close().await;
    }

    async fn request_retirement(&self) -> Result<(), StorageError> {
        if !self.is_valid() {
            return Err(StorageError::GenerationLost);
        }
        let (completion, acknowledgement) = oneshot::channel();
        let target = T::retirement_target(self.self_weak.clone());
        self.retirement_tx.send(RetirementRequest::new(target, completion)).map_err(|_| StorageError::ControlUnavailable)?;
        acknowledgement.await.map_err(|_| StorageError::ControlUnavailable)?
    }
}

/// Processing database capability bound to one validated pool generation.
pub struct ValidatedDbClient {
    runtime: GenerationRuntime<Self>,
    binding: DatabaseBinding,
    caches: ProcessingCaches,
    materialization_lane: Mutex<()>,
    vspc_lane: Mutex<()>,
    transaction_timing: Timing,
    #[cfg(test)]
    materialization_commit_behavior: std::sync::Mutex<Option<materialization::CommitBehavior>>,
    #[cfg(test)]
    vspc_commit_behavior: std::sync::Mutex<Option<vspc::CommitBehavior>>,
}

impl GenerationKind for ValidatedDbClient {
    fn retirement_target(generation: Weak<Self>) -> RetirementTarget {
        RetirementTarget::Processing(generation)
    }
}

impl ValidatedDbClient {
    pub(crate) fn new(pool: PgPool, binding: DatabaseBinding, retirement_tx: RetirementSender) -> Arc<Self> {
        Self::new_with_transaction_timing(pool, binding, retirement_tx, Timing::production())
    }

    fn new_with_transaction_timing(
        pool: PgPool,
        binding: DatabaseBinding,
        retirement_tx: RetirementSender,
        transaction_timing: Timing,
    ) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            runtime: GenerationRuntime::new(pool, retirement_tx, self_weak.clone()),
            binding,
            caches: ProcessingCaches::new(),
            materialization_lane: Mutex::new(()),
            vspc_lane: Mutex::new(()),
            transaction_timing,
            #[cfg(test)]
            materialization_commit_behavior: std::sync::Mutex::new(None),
            #[cfg(test)]
            vspc_commit_behavior: std::sync::Mutex::new(None),
        })
    }

    /// Returns this generation's validated immutable network binding.
    #[must_use]
    pub const fn binding(&self) -> DatabaseBinding {
        self.binding
    }

    /// Loads fresh local processing state for one recovery-session attempt.
    pub async fn load_session_state(&self) -> Result<StoredSessionState, StorageError> {
        let state = self.run_operation(|| ProcessingStateInspection::load(self.runtime.pool(), self.binding)).await?;
        if let StoredSessionState::Initialized(snapshot) = state {
            self.caches
                .publish_vspc_sink(CachedVspcSink { id: snapshot.committed_vspc_sink.id, hash: snapshot.committed_vspc_sink.hash });
        }
        Ok(state)
    }

    /// Resolves one hash without creating or promoting a persistent identity.
    pub async fn block_presence(&self, hash: BlockHash) -> Result<BlockPresence, StorageError> {
        self.run_operation(|| identity::block_presence(self.runtime.pool(), &self.caches, hash)).await
    }

    /// Resolves an ordered hash batch only when every position is materialized.
    pub async fn resolve_materialized_ids(&self, hashes: &[BlockHash]) -> Result<Box<[CompactId]>, ResolveMaterializedIdsError> {
        match self.run_operation(|| identity::resolve_materialized_ids(self.runtime.pool(), &self.caches, hashes)).await? {
            identity::MaterializedIdResolution::Resolved(ids) => Ok(ids),
            identity::MaterializedIdResolution::NonMaterialized { missing, identity_only } => {
                Err(ResolveMaterializedIdsError::NonMaterialized { missing, identity_only })
            }
        }
    }

    #[allow(dead_code, reason = "used by processing operations in the persistence increment")]
    pub(crate) const fn pool(&self) -> &PgPool {
        self.runtime.pool()
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.runtime.is_valid()
    }

    #[allow(dead_code, reason = "used by the permanent service lifecycle")]
    pub(crate) fn retire(&self) -> bool {
        self.runtime.retire()
    }

    pub(crate) fn begin_close(&self) {
        self.runtime.begin_close();
    }

    pub(crate) async fn close(&self) {
        self.runtime.close().await;
    }

    pub(crate) async fn request_retirement(&self) -> Result<(), StorageError> {
        self.runtime.request_retirement().await
    }

    #[cfg(test)]
    pub(crate) fn has_cached_identity(&self, hash: BlockHash) -> bool {
        self.caches.identity(hash).is_some()
    }

    #[cfg(test)]
    pub(crate) fn has_cached_merge_sets(&self, id: CompactId) -> bool {
        self.caches.merge_sets(id).is_some()
    }

    #[cfg(test)]
    pub(crate) fn cached_hash(&self, id: CompactId) -> Option<BlockHash> {
        self.caches.hash(id)
    }

    #[cfg(test)]
    pub(crate) fn cached_materialized_block(&self, id: CompactId) -> Option<CachedMaterializedBlock> {
        self.caches.materialized_block(id)
    }

    #[cfg(test)]
    pub(crate) fn cached_vspc_sink(&self) -> Option<CachedVspcSink> {
        self.caches.vspc_sink()
    }

    async fn run_operation<T, F, Fut>(&self, operation: F) -> Result<T, StorageError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, StorageError>>,
    {
        if !self.is_valid() {
            return Err(StorageError::GenerationLost);
        }
        match operation().await {
            Err(error @ StorageError::Persistence(PersistenceFault::AmbiguousCommit)) => {
                self.request_retirement().await?;
                Err(error)
            }
            Err(error) if error.is_connection_lost() => {
                self.request_retirement().await?;
                Err(StorageError::GenerationLost)
            }
            result => result,
        }
    }
}

impl std::fmt::Debug for ValidatedDbClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ValidatedDbClient")
            .field("binding", &self.binding)
            .field("valid", &self.is_valid())
            .finish_non_exhaustive()
    }
}

/// Read-only API database capability bound to one validated pool generation.
pub struct ValidatedApiDbClient {
    runtime: GenerationRuntime<Self>,
}

impl GenerationKind for ValidatedApiDbClient {
    fn retirement_target(generation: Weak<Self>) -> RetirementTarget {
        RetirementTarget::Api(generation)
    }
}

impl ValidatedApiDbClient {
    pub(crate) fn new(pool: PgPool, retirement_tx: RetirementSender) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self { runtime: GenerationRuntime::new(pool, retirement_tx, self_weak.clone()) })
    }

    /// Reports whether StorageService still considers this exact generation usable.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.runtime.is_valid()
    }

    #[allow(dead_code, reason = "used by API projection operations in the persistence increment")]
    pub(crate) const fn pool(&self) -> &PgPool {
        self.runtime.pool()
    }

    #[allow(dead_code, reason = "used by the permanent service lifecycle")]
    pub(crate) fn retire(&self) -> bool {
        self.runtime.retire()
    }

    pub(crate) fn begin_close(&self) {
        self.runtime.begin_close();
    }

    pub(crate) async fn close(&self) {
        self.runtime.close().await;
    }

    #[allow(dead_code, reason = "used by API projection operations in the persistence increment")]
    pub(crate) async fn request_retirement(&self) -> Result<(), StorageError> {
        self.runtime.request_retirement().await
    }
}

impl std::fmt::Debug for ValidatedApiDbClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ValidatedApiDbClient").field("valid", &self.is_valid()).finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_model::{
        block::{BlockCoordinate, BlockHash, CompactId},
        lifecycle::PersistenceFault,
    };
    use sqlx::postgres::PgPoolOptions;

    use super::{DatabaseBinding, ValidatedDbClient};
    use crate::{
        cache::{CachedIdentity, CachedMaterializedBlock, CachedMergeSets, CachedVspcSink},
        error::StorageError,
        runtime::{RetirementTarget, retirement_channel},
    };

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    #[tokio::test]
    async fn replacement_processing_generation_cannot_observe_predecessor_caches() {
        let pool =
            PgPoolOptions::new().connect_lazy("postgresql://postgres@localhost/kgi").expect("lazy PostgreSQL pool configuration");
        let binding = DatabaseBinding::new(NetworkId::new(NetworkType::Mainnet), hash(0));
        let (retirement_tx, _retirements) = retirement_channel();
        let predecessor = ValidatedDbClient::new(pool.clone(), binding, retirement_tx.clone());
        let id = CompactId::new(1).expect("positive compact ID");
        let coordinate = BlockCoordinate::new(1, 0).expect("positive level");

        predecessor.caches.publish_identity(hash(1), CachedIdentity { id, materialized: true });
        predecessor.caches.publish_materialized_block(
            id,
            CachedMaterializedBlock { coordinate, selected_parent_id: CompactId::new(4).expect("positive ID") },
        );
        predecessor.caches.publish_merge_sets(
            id,
            CachedMergeSets {
                blue: vec![CompactId::new(2).expect("positive ID")].into(),
                red: vec![CompactId::new(3).expect("positive ID")].into(),
            },
        );
        predecessor.caches.publish_vspc_sink(CachedVspcSink { id, hash: hash(1) });
        assert_eq!(predecessor.caches.identity(hash(1)).map(|identity| identity.id), Some(id));
        assert_eq!(predecessor.caches.hash(id), Some(hash(1)));
        assert_eq!(predecessor.caches.materialized_block(id).map(|block| block.coordinate), Some(coordinate));
        assert_eq!(
            predecessor.caches.merge_sets(id).map(|sets| sets.blue),
            Some(vec![CompactId::new(2).expect("positive ID")].into())
        );
        assert_eq!(predecessor.caches.vspc_sink(), Some(CachedVspcSink { id, hash: hash(1) }));

        let replacement = ValidatedDbClient::new(pool, binding, retirement_tx);

        assert_eq!(replacement.caches.identity(hash(1)), None);
        assert_eq!(replacement.caches.hash(id), None);
        assert_eq!(replacement.caches.materialized_block(id), None);
        assert_eq!(replacement.caches.merge_sets(id), None);
        assert_eq!(replacement.caches.vspc_sink(), None);
    }

    #[tokio::test]
    async fn invalid_generation_does_not_start_an_operation() {
        let pool =
            PgPoolOptions::new().connect_lazy("postgresql://postgres@localhost/kgi").expect("lazy PostgreSQL pool configuration");
        let binding = DatabaseBinding::new(NetworkId::new(NetworkType::Mainnet), hash(0));
        let (retirement_tx, _retirements) = retirement_channel();
        let client = ValidatedDbClient::new(pool, binding, retirement_tx);
        let started = Arc::new(AtomicBool::new(false));
        let operation_started = Arc::clone(&started);
        assert!(client.retire());

        let result = client
            .run_operation(|| {
                operation_started.store(true, Ordering::Release);
                async { Ok::<_, StorageError>(()) }
            })
            .await;

        assert_eq!(result, Err(StorageError::GenerationLost));
        assert!(!started.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn ambiguous_operation_retires_exact_generation_before_returning() {
        let pool =
            PgPoolOptions::new().connect_lazy("postgresql://postgres@localhost/kgi").expect("lazy PostgreSQL pool configuration");
        let binding = DatabaseBinding::new(NetworkId::new(NetworkType::Mainnet), hash(0));
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedDbClient::new(pool, binding, retirement_tx);
        let operation_client = Arc::clone(&client);
        let operation = tokio::spawn(async move {
            operation_client
                .run_operation(|| async { Err::<(), _>(StorageError::Persistence(PersistenceFault::AmbiguousCommit)) })
                .await
        });

        let request = retirements.recv().await.expect("ambiguous operation retirement request");
        let retired = match request.target() {
            RetirementTarget::Processing(generation) => generation.upgrade().expect("processing generation remains alive"),
            RetirementTarget::Api(_) => panic!("processing operation requested API retirement"),
        };
        assert!(Arc::ptr_eq(&retired, &client));
        assert!(retired.retire());
        request.complete(Ok(()));

        assert_eq!(operation.await.expect("operation task"), Err(StorageError::Persistence(PersistenceFault::AmbiguousCommit)));
        assert!(!client.is_valid());
    }
}
