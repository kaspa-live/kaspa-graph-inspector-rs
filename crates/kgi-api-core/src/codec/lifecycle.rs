use kgi_model::lifecycle::{NodeServiceStatusState, ProcessingState, RecoveryMode, StorageServiceStatusState, SupervisorLifecycle};
use serde::Serialize;

use crate::publication::GraphPublicationState;

/// Public spelling of the Supervisor lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum SupervisorLifecycleDto {
    Running,
    Fatal,
    ShuttingDown,
    Stopped,
}

impl From<SupervisorLifecycle> for SupervisorLifecycleDto {
    fn from(value: SupervisorLifecycle) -> Self {
        match value {
            SupervisorLifecycle::Running => Self::Running,
            SupervisorLifecycle::Fatal => Self::Fatal,
            SupervisorLifecycle::ShuttingDown => Self::ShuttingDown,
            SupervisorLifecycle::Stopped => Self::Stopped,
        }
    }
}

/// Public spelling of one recovery mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum RecoveryModeDto {
    Resync,
    Rebuild,
}

impl From<RecoveryMode> for RecoveryModeDto {
    fn from(value: RecoveryMode) -> Self {
        match value {
            RecoveryMode::Resync => Self::Resync,
            RecoveryMode::Rebuild => Self::Rebuild,
        }
    }
}

/// Public spelling of the NodeService lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum NodeServiceStatusStateDto {
    Connecting,
    Ready,
    Unavailable,
    Rejected,
    Stopped,
}

impl From<NodeServiceStatusState> for NodeServiceStatusStateDto {
    fn from(value: NodeServiceStatusState) -> Self {
        match value {
            NodeServiceStatusState::Connecting => Self::Connecting,
            NodeServiceStatusState::Ready => Self::Ready,
            NodeServiceStatusState::Unavailable => Self::Unavailable,
            NodeServiceStatusState::Rejected => Self::Rejected,
            NodeServiceStatusState::Stopped => Self::Stopped,
        }
    }
}

/// Public spelling of the StorageService lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum StorageServiceStatusStateDto {
    Connecting,
    AwaitingInitialization,
    Ready,
    Unavailable,
    Rejected,
    Stopped,
}

impl From<StorageServiceStatusState> for StorageServiceStatusStateDto {
    fn from(value: StorageServiceStatusState) -> Self {
        match value {
            StorageServiceStatusState::Connecting => Self::Connecting,
            StorageServiceStatusState::AwaitingInitialization => Self::AwaitingInitialization,
            StorageServiceStatusState::Ready => Self::Ready,
            StorageServiceStatusState::Unavailable => Self::Unavailable,
            StorageServiceStatusState::Rejected => Self::Rejected,
            StorageServiceStatusState::Stopped => Self::Stopped,
        }
    }
}

/// Public processing state name, independent of its optional recovery mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ProcessingStateNameDto {
    Idle,
    Reconciling,
    RebuildingDatabase,
    ResyncingDag,
    CatchingUp,
    Live,
    Stopping,
    Stopped,
}

impl From<ProcessingState> for ProcessingStateNameDto {
    fn from(value: ProcessingState) -> Self {
        match value {
            ProcessingState::Idle => Self::Idle,
            ProcessingState::Reconciling { .. } => Self::Reconciling,
            ProcessingState::RebuildingDatabase => Self::RebuildingDatabase,
            ProcessingState::ResyncingDag => Self::ResyncingDag,
            ProcessingState::CatchingUp => Self::CatchingUp,
            ProcessingState::Live => Self::Live,
            ProcessingState::Deactivating => Self::Stopping,
            ProcessingState::Stopped => Self::Stopped,
        }
    }
}

/// Public graph-publication lifecycle spelling.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum GraphPublicationStateDto {
    Synchronizing,
    Live,
    Stale,
}

impl From<GraphPublicationState> for GraphPublicationStateDto {
    fn from(value: GraphPublicationState) -> Self {
        match value {
            GraphPublicationState::Synchronizing => Self::Synchronizing,
            GraphPublicationState::Live => Self::Live,
            GraphPublicationState::Stale => Self::Stale,
        }
    }
}

#[cfg(test)]
mod tests {
    use kgi_model::lifecycle::{
        NodeServiceStatusState, ProcessingState, RecoveryMode, StorageServiceStatusState, SupervisorLifecycle,
    };
    use serde::Serialize;

    use crate::publication::GraphPublicationState;

    use super::{
        GraphPublicationStateDto, NodeServiceStatusStateDto, ProcessingStateNameDto, RecoveryModeDto, StorageServiceStatusStateDto,
        SupervisorLifecycleDto,
    };

    fn json<T: Serialize>(value: T) -> String {
        serde_json::to_string(&value).expect("lifecycle JSON")
    }

    #[test]
    fn lifecycle_values_use_exact_public_spellings() {
        let supervisor = [
            (SupervisorLifecycle::Running, "\"running\""),
            (SupervisorLifecycle::Fatal, "\"fatal\""),
            (SupervisorLifecycle::ShuttingDown, "\"shutting-down\""),
            (SupervisorLifecycle::Stopped, "\"stopped\""),
        ];
        for (value, expected) in supervisor {
            assert_eq!(json(SupervisorLifecycleDto::from(value)), expected);
        }

        assert_eq!(json(RecoveryModeDto::from(RecoveryMode::Resync)), "\"resync\"");
        assert_eq!(json(RecoveryModeDto::from(RecoveryMode::Rebuild)), "\"rebuild\"");

        let node = [
            (NodeServiceStatusState::Connecting, "\"connecting\""),
            (NodeServiceStatusState::Ready, "\"ready\""),
            (NodeServiceStatusState::Unavailable, "\"unavailable\""),
            (NodeServiceStatusState::Rejected, "\"rejected\""),
            (NodeServiceStatusState::Stopped, "\"stopped\""),
        ];
        for (value, expected) in node {
            assert_eq!(json(NodeServiceStatusStateDto::from(value)), expected);
        }

        let storage = [
            (StorageServiceStatusState::Connecting, "\"connecting\""),
            (StorageServiceStatusState::AwaitingInitialization, "\"awaiting-initialization\""),
            (StorageServiceStatusState::Ready, "\"ready\""),
            (StorageServiceStatusState::Unavailable, "\"unavailable\""),
            (StorageServiceStatusState::Rejected, "\"rejected\""),
            (StorageServiceStatusState::Stopped, "\"stopped\""),
        ];
        for (value, expected) in storage {
            assert_eq!(json(StorageServiceStatusStateDto::from(value)), expected);
        }

        let processing = [
            (ProcessingState::Idle, "\"idle\""),
            (ProcessingState::Reconciling { mode: RecoveryMode::Resync }, "\"reconciling\""),
            (ProcessingState::RebuildingDatabase, "\"rebuilding-database\""),
            (ProcessingState::ResyncingDag, "\"resyncing-dag\""),
            (ProcessingState::CatchingUp, "\"catching-up\""),
            (ProcessingState::Live, "\"live\""),
            (ProcessingState::Deactivating, "\"stopping\""),
            (ProcessingState::Stopped, "\"stopped\""),
        ];
        for (value, expected) in processing {
            assert_eq!(json(ProcessingStateNameDto::from(value)), expected);
        }

        let publication = [
            (GraphPublicationState::Synchronizing, "\"synchronizing\""),
            (GraphPublicationState::Live, "\"live\""),
            (GraphPublicationState::Stale, "\"stale\""),
        ];
        for (value, expected) in publication {
            assert_eq!(json(GraphPublicationStateDto::from(value)), expected);
        }
    }
}
