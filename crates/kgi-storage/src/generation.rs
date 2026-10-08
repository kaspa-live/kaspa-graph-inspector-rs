use std::sync::atomic::{AtomicBool, Ordering};

use kaspa_consensus_core::network::NetworkId;
use kgi_model::block::{BlockHash, CompactId};
use sqlx::PgPool;

use crate::{error::StorageError, state::ProcessingStateInspection};

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

/// Processing database capability bound to one validated pool generation.
pub struct ValidatedDbClient {
    #[allow(dead_code, reason = "used by processing operations in the persistence increment")]
    pool: PgPool,
    binding: DatabaseBinding,
    valid: AtomicBool,
}

impl ValidatedDbClient {
    pub(crate) fn new(pool: PgPool, binding: DatabaseBinding) -> Self {
        Self { pool, binding, valid: AtomicBool::new(true) }
    }

    /// Returns this generation's validated immutable network binding.
    #[must_use]
    pub const fn binding(&self) -> DatabaseBinding {
        self.binding
    }

    /// Loads fresh local processing state for one recovery-session attempt.
    pub async fn load_session_state(&self) -> Result<StoredSessionState, StorageError> {
        ProcessingStateInspection::load(&self.pool, self.binding).await
    }

    #[allow(dead_code, reason = "used by processing operations in the persistence increment")]
    pub(crate) const fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    #[allow(dead_code, reason = "used by the permanent service lifecycle")]
    pub(crate) fn retire(&self) -> bool {
        self.valid.swap(false, Ordering::AcqRel)
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
    #[allow(dead_code, reason = "used by API projection operations in the persistence increment")]
    pool: PgPool,
    valid: AtomicBool,
}

impl ValidatedApiDbClient {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool, valid: AtomicBool::new(true) }
    }

    /// Reports whether StorageService still considers this exact generation usable.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    #[allow(dead_code, reason = "used by API projection operations in the persistence increment")]
    pub(crate) const fn pool(&self) -> &PgPool {
        &self.pool
    }

    #[allow(dead_code, reason = "used by the permanent service lifecycle")]
    pub(crate) fn retire(&self) -> bool {
        self.valid.swap(false, Ordering::AcqRel)
    }
}

impl std::fmt::Debug for ValidatedApiDbClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ValidatedApiDbClient").field("valid", &self.is_valid()).finish_non_exhaustive()
    }
}
