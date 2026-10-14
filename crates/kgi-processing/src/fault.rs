use std::sync::Arc;

use kgi_model::lifecycle::{
    Component, ComponentFault, FaultDisposition, FaultKind, OwnershipFault, PersistenceFault, RecoveryMode, ServiceKind,
};
use kgi_storage::error::ProcessingDbError;

pub(crate) fn active_or_live(is_live: bool, active: FaultDisposition) -> FaultDisposition {
    if is_live { FaultDisposition::Require(RecoveryMode::Resync) } else { active }
}

pub(crate) fn storage_fault(error: ProcessingDbError, is_live: bool) -> ComponentFault {
    let diagnostic: Arc<str> = Arc::from(error.to_string());
    let (disposition, kind) = match error {
        ProcessingDbError::GenerationLost => {
            (active_or_live(is_live, FaultDisposition::Retry), FaultKind::ServiceGenerationLost(ServiceKind::Storage))
        }
        ProcessingDbError::QueryFailed { .. } => (active_or_live(is_live, FaultDisposition::Retry), FaultKind::StorageQueryFailed),
        ProcessingDbError::InvalidStoredState { .. } => {
            (FaultDisposition::Require(RecoveryMode::Rebuild), FaultKind::StorageInvariantViolation)
        }
        ProcessingDbError::Persistence(reason) => {
            let disposition = match reason {
                PersistenceFault::DefiniteFailure => FaultDisposition::Fatal,
                PersistenceFault::RetryExhausted | PersistenceFault::AmbiguousCommit => {
                    active_or_live(is_live, FaultDisposition::Retry)
                }
            };
            (disposition, FaultKind::Persistence(reason))
        }
        ProcessingDbError::ScoreOutOfRange(reason) => (FaultDisposition::Fatal, FaultKind::ScoreOutOfRange(reason)),
        ProcessingDbError::ServiceUnavailable { .. } => {
            (FaultDisposition::Fatal, FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable))
        }
    };
    ComponentFault { source: Component::StorageService, disposition, kind, diagnostic }
}
