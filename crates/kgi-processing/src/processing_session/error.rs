use std::sync::Arc;

use kgi_model::lifecycle::{Component, ComponentFault, FaultDisposition, FaultKind, OwnershipFault};

use crate::{block_processor::BlockProcessorControlError, vspc_processor::VspcProcessorControlError};

pub(super) fn invalid_lifecycle(diagnostic: impl Into<Arc<str>>) -> ComponentFault {
    ownership_fault(OwnershipFault::InvalidLifecycleControl, diagnostic)
}

pub(super) fn block_control(error: BlockProcessorControlError) -> ComponentFault {
    match error {
        BlockProcessorControlError::Rejected(fault) => fault,
        BlockProcessorControlError::ControlUnavailable | BlockProcessorControlError::AcknowledgementLost => {
            ownership_fault(OwnershipFault::InternalControlPathLost, error.to_string())
        }
        BlockProcessorControlError::WorkerJoinFailed => {
            ownership_fault(OwnershipFault::UnexpectedWorkerTermination, error.to_string())
        }
    }
}

pub(super) fn vspc_control(error: VspcProcessorControlError) -> ComponentFault {
    match error {
        VspcProcessorControlError::Rejected(fault) => fault,
        VspcProcessorControlError::ControlUnavailable | VspcProcessorControlError::AcknowledgementLost => {
            ownership_fault(OwnershipFault::InternalControlPathLost, error.to_string())
        }
        VspcProcessorControlError::WorkerJoinFailed => ownership_fault(OwnershipFault::UnexpectedWorkerTermination, error.to_string()),
    }
}

pub(super) fn managed_component_unavailable(component: Component) -> ComponentFault {
    ComponentFault {
        source: component,
        disposition: FaultDisposition::Fatal,
        kind: FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable),
        diagnostic: Arc::from("component event path closed while the processing session was active"),
    }
}

fn ownership_fault(kind: OwnershipFault, diagnostic: impl Into<Arc<str>>) -> ComponentFault {
    ComponentFault {
        source: Component::ProcessingSession,
        disposition: FaultDisposition::Fatal,
        kind: FaultKind::Ownership(kind),
        diagnostic: diagnostic.into(),
    }
}
