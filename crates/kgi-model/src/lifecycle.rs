use std::sync::Arc;

/// Recovery strength. Ordering is not lifecycle or execution order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RecoveryMode {
    Resync,
    Rebuild,
}

/// Required owner action for a component fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultDisposition {
    Retry,
    Require(RecoveryMode),
    Fatal,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Component {
    Supervisor,
    NodeService,
    StorageService,
    ResyncEngine,
    BlockProcessor,
    OrphanManager,
    DependencyResolver,
    VspcProcessor,
    ApiService,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ServiceKind {
    Node,
    Storage,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MalformedVspcNotificationReason {
    RemovedChainWithoutAddedPath,
    DuplicateChainMember,
    RemovedAddedIntersection,
    ResolvedSourceDiscontinuity,
    SelectedParentPathDiscontinuity,
    DuplicatePendingTransition,
    ContradictoryDestination,
    CompetingNextMove,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NotificationInputKind {
    MalformedBlockAdded,
    MalformedVspcChange(MalformedVspcNotificationReason),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MalformedVspcResponseReason {
    RemovedChainWithoutAddedPath,
    NonAdvancingAddedCursor,
    DuplicateChainMember,
    RemovedAddedIntersection,
    LowHashPathMismatch,
    ResolvedSourceDiscontinuity,
    SelectedParentPathDiscontinuity,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecoveryInputKind {
    MalformedPruningPointResponse,
    MalformedCatchupSinkResponse,
    MalformedGetBlock,
    MalformedGetBlocks,
    MalformedVspcResponse(MalformedVspcResponseReason),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PersistenceFault {
    DefiniteFailure,
    RetryExhausted,
    AmbiguousCommit,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ScoreRangeFault {
    DaaScore,
    BlueScore,
    BoundarySealThreshold,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BoundedProcessingState {
    Orphans,
    VspcPending,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OwnershipFault {
    SessionDataEndpointLost,
    ManagedComponentUnavailable,
    InternalControlPathLost,
    UnexpectedWorkerTermination,
    InvalidLifecycleControl,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FaultKind {
    ServiceGenerationLost(ServiceKind),
    SessionContinuityLost,
    NotificationInputInvalid(NotificationInputKind),
    RecoveryInputInvalid(RecoveryInputKind),
    ReconciliationFailed,
    MaterialityViolation,
    DependencyUnavailable,
    BoundedStateExhausted(BoundedProcessingState),
    ScoreOutOfRange(ScoreRangeFault),
    Persistence(PersistenceFault),
    Ownership(OwnershipFault),
}

/// Typed fault envelope crossing a worker ownership boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentFault {
    pub source: Component,
    pub disposition: FaultDisposition,
    pub kind: FaultKind,
    pub diagnostic: Arc<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupervisorLifecycle {
    Running,
    Fatal,
    ShuttingDown,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SupervisorStatus {
    pub lifecycle: SupervisorLifecycle,
    pub desired_recovery: Option<RecoveryMode>,
    pub active_recovery: Option<RecoveryMode>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessingState {
    Idle,
    Reconciling { mode: RecoveryMode },
    RebuildingDatabase,
    ResyncingDag,
    CatchingUp,
    Live,
    Deactivating,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessingStatus {
    pub state: ProcessingState,
}

#[cfg(test)]
mod tests {
    use super::RecoveryMode;

    #[test]
    fn recovery_mode_order_represents_strength() {
        assert!(RecoveryMode::Resync < RecoveryMode::Rebuild);
        assert_eq!(RecoveryMode::Resync.max(RecoveryMode::Rebuild), RecoveryMode::Rebuild);
    }
}
