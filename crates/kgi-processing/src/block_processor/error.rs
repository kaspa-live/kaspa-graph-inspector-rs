use std::sync::Arc;

use kgi_model::lifecycle::{Component, OwnershipFault};
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChildFault {
    pub source: Component,
    pub kind: ChildFaultKind,
}

impl ChildFault {
    pub(crate) const fn new(source: Component, kind: ChildFaultKind) -> Self {
        Self { source, kind }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ChildFaultKind {
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
        let kind = match error {
            ChildControlError::CommandPathClosed | ChildControlError::AcknowledgementLost => {
                ChildFaultKind::Ownership(OwnershipFault::InternalControlPathLost)
            }
            ChildControlError::WorkerJoinFailed => ChildFaultKind::Ownership(OwnershipFault::UnexpectedWorkerTermination),
        };
        Self::new(Component::DependencyResolver, kind)
    }
}
