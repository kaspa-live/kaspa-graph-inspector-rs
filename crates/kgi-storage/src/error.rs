use std::sync::Arc;

use kaspa_consensus_core::network::NetworkId;
use kgi_model::{block::BlockHash, lifecycle::ScoreRangeFault};
use thiserror::Error;

/// Permanent reason a PostgreSQL database cannot be used by KGI.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StorageRejection {
    /// Another KGI process owns the database advisory lock.
    #[error("database is already in use by another KGI process")]
    DatabaseAlreadyInUse,

    /// The immutable database binding does not match the validated node.
    #[error(
        "database network binding mismatch: expected {expected_network_id}/{expected_genesis_hash}, observed {observed_network_id}/{observed_genesis_hash}"
    )]
    NetworkMismatch {
        expected_network_id: NetworkId,
        expected_genesis_hash: BlockHash,
        observed_network_id: NetworkId,
        observed_genesis_hash: BlockHash,
    },

    /// The database was migrated by a newer KGI binary.
    #[error("database schema version {observed} is newer than supported version {supported}")]
    SchemaTooNew { observed: i64, supported: i64 },

    /// The database contains an unsupported, partial, or unknown schema.
    #[error("database schema is unsupported: {diagnostic}")]
    UnsupportedSchema { diagnostic: Arc<str> },
}

/// Failure while preparing or inspecting KGI storage.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StorageError {
    /// PostgreSQL could not complete the requested operation.
    #[error("PostgreSQL {operation} failed: {diagnostic}")]
    Database { operation: &'static str, diagnostic: Arc<str> },

    /// An embedded migration could not be applied or validated.
    #[error("database migration failed: {diagnostic}")]
    Migration { diagnostic: Arc<str> },

    /// Persisted metadata cannot be represented by the KGI domain model.
    #[error("invalid persisted node metadata: {diagnostic}")]
    InvalidMetadata { diagnostic: Arc<str> },

    /// A caller supplied a score outside KGI's persistent range.
    #[error("score is outside KGI's persistent range: {0:?}")]
    ScoreOutOfRange(ScoreRangeFault),

    /// The database is permanently incompatible with this process.
    #[error(transparent)]
    Rejected(#[from] StorageRejection),
}

impl StorageError {
    #[allow(dead_code, reason = "used by the private bootstrap lifecycle")]
    pub(crate) fn database(operation: &'static str, error: impl std::fmt::Display) -> Self {
        Self::Database { operation, diagnostic: Arc::from(error.to_string()) }
    }

    #[allow(dead_code, reason = "used by the private bootstrap lifecycle")]
    pub(crate) fn migration(error: impl std::fmt::Display) -> Self {
        Self::Migration { diagnostic: Arc::from(error.to_string()) }
    }

    #[allow(dead_code, reason = "used by the private bootstrap lifecycle")]
    pub(crate) fn invalid_metadata(diagnostic: impl Into<Arc<str>>) -> Self {
        Self::InvalidMetadata { diagnostic: diagnostic.into() }
    }
}
