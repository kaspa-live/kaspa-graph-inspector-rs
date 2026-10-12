use std::sync::Arc;

use kaspa_consensus_core::network::NetworkId;
use kgi_model::{
    block::{BlockHash, CompactId},
    lifecycle::{PersistenceFault, ScoreRangeFault},
};
use thiserror::Error;

use crate::operation::VspcPathConflict;

/// Failure while establishing the database-replacement preconditions.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RebuildSetupFault {
    /// The fresh read-only API generation could not be prepared.
    #[error("fresh API database generation preparation failed")]
    ApiGenerationPreparation,

    /// Existing API database phases could not be cancelled and drained.
    #[error("API database phases could not be excluded")]
    ApiReadExclusion,
}

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

    /// The current database and binary cannot complete schema migration.
    #[error("database migration failed: {diagnostic}")]
    MigrationFailed { diagnostic: Arc<str> },
}

/// Failure while preparing or inspecting KGI storage.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StorageError {
    /// The exact validated database generation was retired.
    #[error("validated database generation was lost")]
    GenerationLost,

    /// PostgreSQL could not complete the requested operation.
    #[error("PostgreSQL {operation} failed: {diagnostic}")]
    Database { operation: &'static str, diagnostic: Arc<str> },

    /// Communication with the current PostgreSQL generation was lost.
    #[error("PostgreSQL {operation} lost its connection: {diagnostic}")]
    ConnectionLost { operation: &'static str, diagnostic: Arc<str> },

    /// A persistent mutation did not produce a definite outcome.
    #[error("database persistence fault: {0:?}")]
    Persistence(PersistenceFault),

    /// Persisted metadata cannot be represented by the KGI domain model.
    #[error("invalid persisted database metadata: {diagnostic}")]
    InvalidMetadata { diagnostic: Arc<str> },

    /// Storage reached an internal or non-persisted representation failure.
    #[error("storage cannot safely continue: {diagnostic}")]
    InternalFailure { diagnostic: Arc<str> },

    /// A caller supplied a score outside KGI's persistent range.
    #[error("score is outside KGI's persistent range: {0:?}")]
    ScoreOutOfRange(ScoreRangeFault),

    /// The supplied VSPC source is not the database's currently committed sink.
    #[error("VSPC source does not equal the currently committed sink")]
    VspcSourceDiscontinuity,

    /// A directly named VSPC chain member is not materialized.
    #[error("VSPC chain member is not materialized: {id}")]
    VspcMemberNotMaterialized {
        /// Database-local identity of the nonmaterialized chain member.
        id: CompactId,
    },

    /// Persisted selected-parent evidence does not follow the supplied VSPC path.
    #[error("VSPC selected-parent path is discontinuous at {0:?}")]
    VspcPathDiscontinuity(VspcPathConflict),

    /// Rebuild could not establish its API database replacement preconditions.
    #[error("database rebuild setup failed: {0}")]
    RebuildSetupFailed(RebuildSetupFault),

    /// StorageService's replacement-control primitive became unusable.
    #[error("database replacement control is unavailable: {diagnostic}")]
    ReplacementControlUnavailable { diagnostic: Arc<str> },

    /// The database is permanently incompatible with this process.
    #[error(transparent)]
    Rejected(#[from] StorageRejection),

    /// The reliable lifecycle-event receiver was dropped.
    #[error("storage lifecycle event path closed")]
    EventPathClosed,

    /// The permanent service control path is unavailable.
    #[error("storage service control path is unavailable")]
    ControlUnavailable,

    /// The permanent service worker terminated unexpectedly.
    #[error("storage service worker failed: {diagnostic}")]
    WorkerFailed { diagnostic: Arc<str> },
}

/// Closed error domain shared by validated processing-database operations.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ProcessingDbError {
    /// The exact validated database generation was retired.
    #[error("validated database generation was lost")]
    GenerationLost,

    /// A nonmutating query failed while the generation remained valid.
    #[error("processing database query failed: {diagnostic}")]
    QueryFailed { diagnostic: Arc<str> },

    /// A bounded read found invalid persisted processing state.
    #[error("processing database contains invalid stored state: {diagnostic}")]
    InvalidStoredState { diagnostic: Arc<str> },

    /// A persistent mutation produced a classified outcome.
    #[error("database persistence fault: {0:?}")]
    Persistence(PersistenceFault),

    /// A processing score cannot be represented by the persistent model.
    #[error("score is outside KGI's persistent range: {0:?}")]
    ScoreOutOfRange(ScoreRangeFault),

    /// Storage cannot safely continue serving this validated generation.
    #[error("storage service is unavailable: {diagnostic}")]
    ServiceUnavailable { diagnostic: Arc<str> },
}

impl ProcessingDbError {
    pub(crate) fn query(error: StorageError) -> Self {
        match error {
            StorageError::GenerationLost => Self::GenerationLost,
            StorageError::Database { operation, diagnostic } => {
                Self::QueryFailed { diagnostic: Arc::from(format!("{operation}: {diagnostic}")) }
            }
            StorageError::InvalidMetadata { diagnostic } => Self::InvalidStoredState { diagnostic },
            StorageError::Persistence(reason) => Self::Persistence(reason),
            StorageError::ScoreOutOfRange(reason) => Self::ScoreOutOfRange(reason),
            error => Self::service_unavailable(error),
        }
    }

    pub(crate) fn mutation(error: StorageError) -> Self {
        match error {
            StorageError::GenerationLost => Self::GenerationLost,
            StorageError::InvalidMetadata { diagnostic } => Self::InvalidStoredState { diagnostic },
            StorageError::Persistence(reason) => Self::Persistence(reason),
            StorageError::ScoreOutOfRange(reason) => Self::ScoreOutOfRange(reason),
            error => Self::service_unavailable(error),
        }
    }

    fn service_unavailable(error: StorageError) -> Self {
        Self::ServiceUnavailable { diagnostic: Arc::from(error.to_string()) }
    }
}

impl StorageError {
    #[allow(dead_code, reason = "used by the private bootstrap lifecycle")]
    pub(crate) fn database(operation: &'static str, error: sqlx::Error) -> Self {
        let connection_lost = sqlx_connection_lost(&error);
        let diagnostic = Arc::from(error.to_string());
        if connection_lost { Self::ConnectionLost { operation, diagnostic } } else { Self::Database { operation, diagnostic } }
    }

    pub(crate) fn mutation_commit(operation: &'static str, error: sqlx::Error) -> Self {
        if sqlx_connection_lost(&error) {
            Self::Persistence(PersistenceFault::AmbiguousCommit)
        } else {
            Self::database(operation, error)
        }
    }

    #[allow(dead_code, reason = "used by the private bootstrap lifecycle")]
    pub(crate) fn invalid_metadata(diagnostic: impl Into<Arc<str>>) -> Self {
        Self::InvalidMetadata { diagnostic: diagnostic.into() }
    }

    pub(crate) fn internal_failure(diagnostic: impl Into<Arc<str>>) -> Self {
        Self::InternalFailure { diagnostic: diagnostic.into() }
    }

    pub(crate) fn replacement_control(diagnostic: impl Into<Arc<str>>) -> Self {
        Self::ReplacementControlUnavailable { diagnostic: diagnostic.into() }
    }

    pub(crate) const fn is_connection_lost(&self) -> bool {
        matches!(self, Self::ConnectionLost { .. })
    }
}

pub(crate) fn sqlx_connection_lost(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed
            | sqlx::Error::BeginFailed
    ) || error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| code.starts_with("08") || matches!(code.as_ref(), "57P01" | "57P02" | "57P03"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kgi_model::lifecycle::{PersistenceFault, ScoreRangeFault};

    use super::{ProcessingDbError, StorageError};

    #[test]
    fn query_errors_preserve_the_processing_boundary_classification() {
        assert_eq!(
            ProcessingDbError::query(StorageError::Database { operation: "test query", diagnostic: Arc::from("failed") }),
            ProcessingDbError::QueryFailed { diagnostic: Arc::from("test query: failed") }
        );
        assert_eq!(
            ProcessingDbError::query(StorageError::InvalidMetadata { diagnostic: Arc::from("invalid row") }),
            ProcessingDbError::InvalidStoredState { diagnostic: Arc::from("invalid row") }
        );
        assert_eq!(
            ProcessingDbError::query(StorageError::InternalFailure { diagnostic: Arc::from("impossible state") }),
            ProcessingDbError::ServiceUnavailable { diagnostic: Arc::from("storage cannot safely continue: impossible state") }
        );
    }

    #[test]
    fn common_processing_errors_are_preserved_for_mutations() {
        assert_eq!(ProcessingDbError::mutation(StorageError::GenerationLost), ProcessingDbError::GenerationLost);
        assert_eq!(
            ProcessingDbError::mutation(StorageError::Persistence(PersistenceFault::AmbiguousCommit)),
            ProcessingDbError::Persistence(PersistenceFault::AmbiguousCommit)
        );
        assert_eq!(
            ProcessingDbError::mutation(StorageError::ScoreOutOfRange(ScoreRangeFault::BlueScore)),
            ProcessingDbError::ScoreOutOfRange(ScoreRangeFault::BlueScore)
        );
    }
}
