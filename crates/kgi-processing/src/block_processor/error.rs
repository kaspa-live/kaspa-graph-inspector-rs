use std::sync::Arc;

use kgi_model::lifecycle::OwnershipFault;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ChildControlError {
    #[error("child command path is closed")]
    CommandPathClosed,

    #[error("child command acknowledgement was lost")]
    AcknowledgementLost,

    #[error("child worker task could not be joined")]
    WorkerJoinFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SessionDataPath {
    OrphanInput,
    ResolverWork,
    Resolution,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ChildFault {
    #[error("orphan capacity is exhausted")]
    OrphanCapacityExhausted,

    #[error("session data path is full: {0:?}")]
    SessionDataFull(SessionDataPath),

    #[error("session data path is unavailable: {0:?}")]
    SessionDataEndpointLost(SessionDataPath),

    #[error("child worker ownership fault: {0:?}")]
    Ownership(OwnershipFault),

    #[error("invalid child protocol: {0}")]
    InvalidProtocol(Arc<str>),
}

impl From<ChildControlError> for ChildFault {
    fn from(error: ChildControlError) -> Self {
        match error {
            ChildControlError::CommandPathClosed | ChildControlError::AcknowledgementLost => {
                Self::Ownership(OwnershipFault::InternalControlPathLost)
            }
            ChildControlError::WorkerJoinFailed => Self::Ownership(OwnershipFault::UnexpectedWorkerTermination),
        }
    }
}
