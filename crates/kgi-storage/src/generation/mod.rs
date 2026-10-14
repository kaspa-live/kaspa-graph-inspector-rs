use std::{
    future::Future,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use kaspa_consensus_core::network::NetworkId;
use kgi_core::timing::Timing;
use kgi_model::{
    block::{BlockHash, BlockPresence, CompactId, MaterializedSyncAnchor, ValidatedNodeBlock},
    lifecycle::PersistenceFault,
};
use sqlx::PgPool;

use tokio::sync::{Mutex, RwLock, oneshot, watch};

use crate::{
    cache::{CachedVspcSink, ProcessingCaches},
    error::{ProcessingDbError, StorageError},
    identity,
    operation::{RebuildFromPruningPointError, ResolveMaterializedIdsError},
    replacement::{ApiDbReadLease, ApiReplacementGate},
    runtime::{RebuildRequest, RebuildSender, RetirementRequest, RetirementSender, RetirementTarget},
    state::ProcessingStateInspection,
};

#[cfg(test)]
use crate::cache::CachedMaterializedBlock;

mod block_rows;
mod interning;
mod materialization;
pub(crate) mod rebuild;
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
        self.request_operation_retirement().await
    }

    async fn request_operation_retirement(&self) -> Result<(), StorageError> {
        // Always join the service barrier: another failure may have selected this
        // generation's retirement after the operation itself started.
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
    mutation_guard: RwLock<()>,
    materialization_lane: Mutex<()>,
    vspc_lane: Mutex<()>,
    transaction_timing: Timing,
    rebuild_tx: RebuildSender,
    #[cfg(test)]
    materialization_commit_behavior: std::sync::Mutex<Option<materialization::CommitBehavior>>,
    #[cfg(test)]
    vspc_commit_behavior: std::sync::Mutex<Option<vspc::CommitBehavior>>,
    #[cfg(test)]
    rebuild_commit_behavior: std::sync::Mutex<Option<rebuild::CommitBehavior>>,
}

impl GenerationKind for ValidatedDbClient {
    fn retirement_target(generation: Weak<Self>) -> RetirementTarget {
        RetirementTarget::Processing(generation)
    }
}

impl ValidatedDbClient {
    #[cfg(test)]
    pub(crate) fn new(pool: PgPool, binding: DatabaseBinding, retirement_tx: RetirementSender) -> Arc<Self> {
        let (rebuild_tx, _rebuild_rx) = crate::runtime::rebuild_channel();
        Self::new_managed(pool, binding, retirement_tx, rebuild_tx)
    }

    pub(crate) fn new_managed(
        pool: PgPool,
        binding: DatabaseBinding,
        retirement_tx: RetirementSender,
        rebuild_tx: RebuildSender,
    ) -> Arc<Self> {
        Self::new_with_transaction_timing(pool, binding, retirement_tx, rebuild_tx, Timing::production())
    }

    fn new_with_transaction_timing(
        pool: PgPool,
        binding: DatabaseBinding,
        retirement_tx: RetirementSender,
        rebuild_tx: RebuildSender,
        transaction_timing: Timing,
    ) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            runtime: GenerationRuntime::new(pool, retirement_tx, self_weak.clone()),
            binding,
            caches: ProcessingCaches::new(),
            mutation_guard: RwLock::new(()),
            materialization_lane: Mutex::new(()),
            vspc_lane: Mutex::new(()),
            transaction_timing,
            rebuild_tx,
            #[cfg(test)]
            materialization_commit_behavior: std::sync::Mutex::new(None),
            #[cfg(test)]
            vspc_commit_behavior: std::sync::Mutex::new(None),
            #[cfg(test)]
            rebuild_commit_behavior: std::sync::Mutex::new(None),
        })
    }

    /// Returns this generation's validated immutable network binding.
    #[must_use]
    pub const fn binding(&self) -> DatabaseBinding {
        self.binding
    }

    /// Loads fresh local processing state for one recovery-session attempt.
    pub async fn load_session_state(&self) -> Result<StoredSessionState, ProcessingDbError> {
        let state = self
            .run_operation(|| ProcessingStateInspection::load(self.runtime.pool(), self.binding))
            .await
            .map_err(ProcessingDbError::query)?;
        if let StoredSessionState::Initialized(snapshot) = state {
            self.caches
                .load()
                .publish_vspc_sink(CachedVspcSink { id: snapshot.committed_vspc_sink.id, hash: snapshot.committed_vspc_sink.hash });
        }
        Ok(state)
    }

    /// Resolves one hash without creating or promoting a persistent identity.
    pub async fn block_presence(&self, hash: BlockHash) -> Result<BlockPresence, ProcessingDbError> {
        let caches = self.caches.load_full();
        self.run_operation(|| identity::block_presence(self.runtime.pool(), &caches, hash)).await.map_err(ProcessingDbError::query)
    }

    /// Resolves an ordered hash batch only when every position is materialized.
    pub async fn resolve_materialized_ids(&self, hashes: &[BlockHash]) -> Result<Box<[CompactId]>, ResolveMaterializedIdsError> {
        let caches = self.caches.load_full();
        match self
            .run_operation(|| identity::resolve_materialized_ids(self.runtime.pool(), &caches, hashes))
            .await
            .map_err(ProcessingDbError::query)?
        {
            identity::MaterializedIdResolution::Resolved(ids) => Ok(ids),
            identity::MaterializedIdResolution::NonMaterialized { missing, identity_only } => {
                Err(ResolveMaterializedIdsError::NonMaterialized { missing, identity_only })
            }
        }
    }

    /// Atomically replaces all processing data with one validated pruning-point anchor.
    pub async fn rebuild_from_pruning_point(
        &self,
        pruning_point: ValidatedNodeBlock,
    ) -> Result<MaterializedSyncAnchor, RebuildFromPruningPointError> {
        rebuild::validate_input(&pruning_point)
            .map_err(ProcessingDbError::mutation)
            .map_err(RebuildFromPruningPointError::Database)?;
        if !self.is_valid() {
            return Err(RebuildFromPruningPointError::Database(ProcessingDbError::GenerationLost));
        }
        let (completion, acknowledgement) = oneshot::channel();
        self.rebuild_tx
            .send(RebuildRequest::new(self.runtime.self_weak.clone(), pruning_point, completion))
            .map_err(|_| rebuild_error(StorageError::ControlUnavailable))?;
        acknowledgement.await.map_err(|_| rebuild_error(StorageError::ControlUnavailable))?.map_err(rebuild_error)
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

    #[cfg(test)]
    pub(crate) async fn request_retirement(&self) -> Result<(), StorageError> {
        self.runtime.request_retirement().await
    }

    #[cfg(test)]
    pub(crate) fn has_cached_identity(&self, hash: BlockHash) -> bool {
        self.caches.load().identity(hash).is_some()
    }

    #[cfg(test)]
    pub(crate) fn has_cached_merge_sets(&self, id: CompactId) -> bool {
        self.caches.load().merge_sets(id).is_some()
    }

    #[cfg(test)]
    pub(crate) fn cached_materialized_block(&self, id: CompactId) -> Option<CachedMaterializedBlock> {
        self.caches.load().materialized_block(id)
    }

    #[cfg(test)]
    pub(crate) fn cached_vspc_sink(&self) -> Option<CachedVspcSink> {
        self.caches.load().vspc_sink()
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
                self.runtime.request_operation_retirement().await?;
                Err(error)
            }
            Err(error) if error.is_connection_lost() => {
                self.runtime.request_operation_retirement().await?;
                Err(StorageError::GenerationLost)
            }
            result => result,
        }
    }

    #[cfg(test)]
    pub(crate) async fn run_gated_test_operation(
        &self,
        started: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
        result: Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        self.run_operation(|| async move {
            let _ = started.send(());
            let _ = release.await;
            result
        })
        .await
    }
}

fn rebuild_error(error: StorageError) -> RebuildFromPruningPointError {
    match error {
        StorageError::RebuildSetupFailed(reason) => RebuildFromPruningPointError::Setup(reason),
        StorageError::ReplacementControlUnavailable { diagnostic } => {
            RebuildFromPruningPointError::ReplacementControlUnavailable { diagnostic }
        }
        error => RebuildFromPruningPointError::Database(ProcessingDbError::mutation(error)),
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
    replacement_gate: Arc<ApiReplacementGate>,
    admission: StdMutex<()>,
    validity: watch::Sender<bool>,
}

impl GenerationKind for ValidatedApiDbClient {
    fn retirement_target(generation: Weak<Self>) -> RetirementTarget {
        RetirementTarget::Api(generation)
    }
}

impl ValidatedApiDbClient {
    pub(crate) fn new(pool: PgPool, retirement_tx: RetirementSender, replacement_gate: Arc<ApiReplacementGate>) -> Arc<Self> {
        let (validity, _) = watch::channel(true);
        Arc::new_cyclic(|self_weak| Self {
            runtime: GenerationRuntime::new(pool, retirement_tx, self_weak.clone()),
            replacement_gate,
            admission: StdMutex::new(()),
            validity,
        })
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
        let _admission = self.admission.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let retired = self.runtime.retire();
        if retired {
            self.validity.send_replace(false);
        }
        retired
    }

    #[allow(dead_code, reason = "used by API projection operations in the persistence increment")]
    pub(crate) async fn acquire_read_lease(&self) -> Result<ApiDbReadLease, StorageError> {
        let mut gate_changes = self.replacement_gate.subscribe_changes();
        let mut validity = self.validity.subscribe();
        loop {
            {
                let _admission = self.admission.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if !self.runtime.is_valid() {
                    return Err(StorageError::GenerationLost);
                }
                if let Some(lease) = self.replacement_gate.try_acquire_lease()? {
                    return Ok(lease);
                }
            }
            tokio::select! {
                result = gate_changes.changed() => {
                    result.map_err(|_| StorageError::replacement_control("API replacement notification path closed"))?;
                }
                result = validity.changed() => {
                    result.map_err(|_| StorageError::replacement_control("API generation validity path closed"))?;
                    if !*validity.borrow() {
                        return Err(StorageError::GenerationLost);
                    }
                }
            }
        }
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
    use kgi_core::timing::Timing;
    use kgi_model::{
        block::{BlockCoordinate, BlockHash, CompactId},
        lifecycle::PersistenceFault,
    };
    use sqlx::postgres::PgPoolOptions;

    use super::{DatabaseBinding, ValidatedApiDbClient, ValidatedDbClient};
    use crate::{
        cache::{CachedIdentity, CachedMaterializedBlock, CachedMergeSets, CachedVspcSink},
        error::StorageError,
        replacement::ApiReplacementGate,
        runtime::{RetirementTarget, retirement_channel},
    };

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    #[tokio::test]
    async fn api_read_lease_waits_behind_replacement_and_retirement_wakes_the_waiter() {
        let pool =
            PgPoolOptions::new().connect_lazy("postgresql://postgres@localhost/kgi").expect("lazy PostgreSQL pool configuration");
        let (retirement_tx, _retirements) = retirement_channel();
        let gate = ApiReplacementGate::new();
        let client = ValidatedApiDbClient::new(pool, retirement_tx, gate.clone());
        let attempt = gate.begin_replacement().expect("replacement closes admission");
        let exclusive =
            attempt.acquire_exclusive(&Timing::production()).await.expect("empty active set establishes replacement exclusion");

        let waiting_client = client.clone();
        let mut waiting = tokio::spawn(async move { waiting_client.acquire_read_lease().await });
        assert!(tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiting).await.is_err());
        exclusive.open().expect("replacement completion opens admission");
        assert!(waiting.await.expect("admission task completes").is_ok());

        let attempt = gate.begin_replacement().expect("next replacement closes admission");
        let exclusive =
            attempt.acquire_exclusive(&Timing::production()).await.expect("released lease establishes replacement exclusion");
        let waiting_client = client.clone();
        let waiting = tokio::spawn(async move { waiting_client.acquire_read_lease().await });
        tokio::task::yield_now().await;
        assert!(client.retire());
        assert!(matches!(waiting.await.expect("retired waiter completes"), Err(StorageError::GenerationLost)));
        exclusive.open().expect("test restores gate admission");
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

        let predecessor_caches = predecessor.caches.load();
        predecessor_caches.publish_identity(hash(1), CachedIdentity { id, materialized: true });
        predecessor_caches.publish_materialized_block(
            id,
            CachedMaterializedBlock { coordinate, selected_parent_id: CompactId::new(4).expect("positive ID") },
        );
        predecessor_caches.publish_merge_sets(
            id,
            CachedMergeSets {
                blue: vec![CompactId::new(2).expect("positive ID")].into(),
                red: vec![CompactId::new(3).expect("positive ID")].into(),
            },
        );
        predecessor_caches.publish_vspc_sink(CachedVspcSink { id, hash: hash(1) });
        assert_eq!(predecessor_caches.identity(hash(1)).map(|identity| identity.id), Some(id));
        assert_eq!(predecessor_caches.materialized_block(id).map(|block| block.coordinate), Some(coordinate));
        assert_eq!(
            predecessor_caches.merge_sets(id).map(|sets| sets.blue),
            Some(vec![CompactId::new(2).expect("positive ID")].into())
        );
        assert_eq!(predecessor_caches.vspc_sink(), Some(CachedVspcSink { id, hash: hash(1) }));

        let replacement = ValidatedDbClient::new(pool, binding, retirement_tx);
        let replacement_caches = replacement.caches.load();

        assert_eq!(replacement_caches.identity(hash(1)), None);
        assert_eq!(replacement_caches.materialized_block(id), None);
        assert_eq!(replacement_caches.merge_sets(id), None);
        assert_eq!(replacement_caches.vspc_sink(), None);
    }

    #[tokio::test]
    async fn processing_mutations_share_one_nonwaiting_exclusive_guard() {
        let pool =
            PgPoolOptions::new().connect_lazy("postgresql://postgres@localhost/kgi").expect("lazy PostgreSQL pool configuration");
        let binding = DatabaseBinding::new(NetworkId::new(NetworkType::Mainnet), hash(0));
        let (retirement_tx, _retirements) = retirement_channel();
        let client = ValidatedDbClient::new(pool, binding, retirement_tx);

        let first_mutation = client.mutation_guard.read().await;
        let second_mutation = client.mutation_guard.try_read().expect("ordinary mutation permits overlap");
        assert!(client.mutation_guard.try_write().is_err());

        drop(first_mutation);
        drop(second_mutation);
        let _replacement = client.mutation_guard.try_write().expect("exclusive replacement does not wait after mutations finish");
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
