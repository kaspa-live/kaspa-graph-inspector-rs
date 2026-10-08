use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};

use kaspa_consensus_core::network::NetworkId;
use kgi_model::block::{BlockHash, CompactId};
use sqlx::PgPool;

use tokio::sync::oneshot;

use crate::{
    error::StorageError,
    runtime::{RetirementRequest, RetirementSender, RetirementTarget},
    state::ProcessingStateInspection,
};

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
}

impl GenerationKind for ValidatedDbClient {
    fn retirement_target(generation: Weak<Self>) -> RetirementTarget {
        RetirementTarget::Processing(generation)
    }
}

impl ValidatedDbClient {
    pub(crate) fn new(pool: PgPool, binding: DatabaseBinding, retirement_tx: RetirementSender) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self { runtime: GenerationRuntime::new(pool, retirement_tx, self_weak.clone()), binding })
    }

    /// Returns this generation's validated immutable network binding.
    #[must_use]
    pub const fn binding(&self) -> DatabaseBinding {
        self.binding
    }

    /// Loads fresh local processing state for one recovery-session attempt.
    pub async fn load_session_state(&self) -> Result<StoredSessionState, StorageError> {
        if !self.is_valid() {
            return Err(StorageError::GenerationLost);
        }
        match ProcessingStateInspection::load(self.runtime.pool(), self.binding).await {
            Err(error) if error.is_connection_lost() => {
                self.request_retirement().await?;
                Err(StorageError::GenerationLost)
            }
            result => result,
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
