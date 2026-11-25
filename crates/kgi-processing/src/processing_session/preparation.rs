use std::{future::Future, sync::Arc};

use kgi_model::{
    block::{
        BlockHash, ConsensusOrder, MAX_BLUE_SCORE, MaterializedSyncAnchor, ValidatedNodeBlock, ValidatedRecoveryHeader, VspcPoint,
    },
    lifecycle::{Component, ComponentFault, FaultDisposition, FaultKind, OwnershipFault, RecoveryMode, ScoreRangeFault, ServiceKind},
};
use kgi_node::{error::NodeError, rpc::ValidatedRpcClient};
use kgi_storage::{
    error::ProcessingDbError,
    generation::{StoredSessionState, ValidatedDbClient},
    operation::{RebuildFromPruningPointError, RebuildFromPruningPointOutcome},
};
use tokio_util::sync::CancellationToken;

use crate::fault::storage_fault;

/// Complete common input prepared for both processing workers.
pub(super) struct PreparedSync<R: PreparationRpc, D: PreparationDb> {
    pub(super) rpc: Arc<R>,
    pub(super) db: Arc<D>,
    pub(super) anchor: MaterializedSyncAnchor,
    pub(super) boundary_seal_blue_score: u64,
}

/// Terminal result of one session preparation attempt.
pub(super) enum PreparationOutcome<R: PreparationRpc, D: PreparationDb> {
    Prepared(PreparedSync<R, D>),
    RequireRebuild,
    Failed(ComponentFault),
    Cancelled,
}

/// Cloneable request to stop one in-progress session startup.
#[derive(Clone)]
pub(crate) struct StartupCancellation {
    token: CancellationToken,
}

impl StartupCancellation {
    pub(crate) fn new() -> Self {
        Self { token: CancellationToken::new() }
    }

    pub(crate) fn cancel(&self) {
        self.token.cancel();
    }

    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundarySealThresholdError {
    Overflow,
    AboveMaximum,
}

fn construct_boundary_seal_blue_score(
    boundary_hash: BlockHash,
    boundary_blue_score: u64,
    genesis_hash: BlockHash,
    anticone_finalization_depth: u64,
) -> Result<u64, BoundarySealThresholdError> {
    if boundary_hash == genesis_hash {
        return Ok(0);
    }
    let threshold = boundary_blue_score.checked_add(anticone_finalization_depth).ok_or(BoundarySealThresholdError::Overflow)?;
    if threshold > MAX_BLUE_SCORE { Err(BoundarySealThresholdError::AboveMaximum) } else { Ok(threshold) }
}

/// Node-generation capabilities consumed by session preparation.
pub(crate) trait PreparationRpc: Send + Sync + 'static {
    /// Returns the immutable validation result for this generation.
    fn genesis_hash(&self) -> BlockHash;

    /// Returns the anticone finalization depth retained for this generation.
    fn anticone_finalization_depth(&self) -> u64;

    /// Obtains the normalized header-only fields needed by Resync preparation.
    fn recovery_header(&self, hash: BlockHash) -> impl Future<Output = Result<ValidatedRecoveryHeader, NodeError>> + Send;

    /// Obtains and normalizes the current pruning-point block.
    fn current_pruning_point_block(&self) -> impl Future<Output = Result<ValidatedNodeBlock, NodeError>> + Send;
}

impl PreparationRpc for ValidatedRpcClient {
    fn genesis_hash(&self) -> BlockHash {
        self.node_info().genesis_hash
    }

    fn anticone_finalization_depth(&self) -> u64 {
        self.node_info().consensus.anticone_finalization_depth()
    }

    async fn recovery_header(&self, hash: BlockHash) -> Result<ValidatedRecoveryHeader, NodeError> {
        self.recovery_header(hash).await
    }

    async fn current_pruning_point_block(&self) -> Result<ValidatedNodeBlock, NodeError> {
        self.current_pruning_point_block().await
    }
}

/// Database-generation capabilities consumed by session preparation.
pub(crate) trait PreparationDb: Send + Sync + 'static {
    /// Loads fresh local processing state for one recovery-session attempt.
    fn load_session_state(&self) -> impl Future<Output = Result<StoredSessionState, ProcessingDbError>> + Send;

    /// Replaces processing data while forwarding startup cancellation to the database generation.
    fn rebuild_from_pruning_point(
        &self,
        pruning_point: ValidatedNodeBlock,
        cancellation: CancellationToken,
    ) -> impl Future<Output = Result<RebuildFromPruningPointOutcome, RebuildFromPruningPointError>> + Send;
}

impl PreparationDb for ValidatedDbClient {
    async fn load_session_state(&self) -> Result<StoredSessionState, ProcessingDbError> {
        self.load_session_state().await
    }

    async fn rebuild_from_pruning_point(
        &self,
        pruning_point: ValidatedNodeBlock,
        cancellation: CancellationToken,
    ) -> Result<RebuildFromPruningPointOutcome, RebuildFromPruningPointError> {
        self.rebuild_from_pruning_point(pruning_point, cancellation).await
    }
}

pub(super) async fn prepare<R: PreparationRpc, D: PreparationDb>(
    mode: RecoveryMode,
    rpc: Arc<R>,
    db: Arc<D>,
    cancellation: &StartupCancellation,
) -> PreparationOutcome<R, D> {
    match mode {
        RecoveryMode::Resync => prepare_resync(rpc, db, cancellation).await,
        RecoveryMode::Rebuild => prepare_rebuild(rpc, db, cancellation).await,
    }
}

async fn prepare_resync<R: PreparationRpc, D: PreparationDb>(
    rpc: Arc<R>,
    db: Arc<D>,
    cancellation: &StartupCancellation,
) -> PreparationOutcome<R, D> {
    let state = tokio::select! {
        biased;
        () = cancellation.cancelled() => return PreparationOutcome::Cancelled,
        result = db.load_session_state() => match result {
            Ok(state) => state,
            Err(error) => return PreparationOutcome::Failed(storage_fault(error, false)),
        },
    };
    let snapshot = match state {
        StoredSessionState::Empty | StoredSessionState::Inconsistent => return PreparationOutcome::RequireRebuild,
        StoredSessionState::Initialized(snapshot) => snapshot,
    };
    let threshold = match construct_boundary_seal_blue_score(
        snapshot.db_pp_hash,
        snapshot.db_pp_blue_score,
        rpc.genesis_hash(),
        rpc.anticone_finalization_depth(),
    ) {
        Ok(threshold) => threshold,
        Err(error) => return PreparationOutcome::Failed(threshold_fault(error)),
    };
    let sink = snapshot.committed_vspc_sink;
    let header = tokio::select! {
        biased;
        () = cancellation.cancelled() => return PreparationOutcome::Cancelled,
        result = rpc.recovery_header(sink.hash) => match result {
            Ok(header) => header,
            Err(NodeError::BlockNotFound { .. }) => return PreparationOutcome::RequireRebuild,
            Err(error) => return PreparationOutcome::Failed(node_fault(error)),
        },
    };
    if header.daa_score != sink.daa_score || header.blue_score < threshold {
        return PreparationOutcome::RequireRebuild;
    }
    let anchor = MaterializedSyncAnchor {
        point: VspcPoint::new(ConsensusOrder::new(header.blue_work, header.hash), sink.id),
        selected_parent: sink.selected_parent,
        blue_score: header.blue_score,
    };
    if cancellation.is_cancelled() {
        return PreparationOutcome::Cancelled;
    }
    PreparationOutcome::Prepared(PreparedSync { rpc, db, anchor, boundary_seal_blue_score: threshold })
}

async fn prepare_rebuild<R: PreparationRpc, D: PreparationDb>(
    rpc: Arc<R>,
    db: Arc<D>,
    cancellation: &StartupCancellation,
) -> PreparationOutcome<R, D> {
    let pruning_point = tokio::select! {
        biased;
        () = cancellation.cancelled() => return PreparationOutcome::Cancelled,
        result = rpc.current_pruning_point_block() => match result {
            Ok(pruning_point) => pruning_point,
            Err(error) => return PreparationOutcome::Failed(node_fault(error)),
        },
    };
    let threshold = match construct_boundary_seal_blue_score(
        pruning_point.hash,
        pruning_point.blue_score,
        rpc.genesis_hash(),
        rpc.anticone_finalization_depth(),
    ) {
        Ok(threshold) => threshold,
        Err(error) => return PreparationOutcome::Failed(threshold_fault(error)),
    };

    match db.rebuild_from_pruning_point(pruning_point, cancellation.token()).await {
        Err(error) => PreparationOutcome::Failed(rebuild_fault(error)),
        Ok(RebuildFromPruningPointOutcome::Cancelled) => PreparationOutcome::Cancelled,
        Ok(RebuildFromPruningPointOutcome::Rebuilt(_)) if cancellation.is_cancelled() => PreparationOutcome::Cancelled,
        Ok(RebuildFromPruningPointOutcome::Rebuilt(anchor)) => {
            PreparationOutcome::Prepared(PreparedSync { rpc, db, anchor, boundary_seal_blue_score: threshold })
        }
    }
}

fn threshold_fault(error: BoundarySealThresholdError) -> ComponentFault {
    ComponentFault {
        source: Component::ProcessingSession,
        disposition: FaultDisposition::Fatal,
        kind: FaultKind::ScoreOutOfRange(ScoreRangeFault::BoundarySealThreshold),
        diagnostic: Arc::from(format!("boundary seal threshold construction failed: {error:?}")),
    }
}

fn node_fault(error: NodeError) -> ComponentFault {
    let diagnostic = Arc::from(error.to_string());
    let (disposition, kind) = match error {
        NodeError::GenerationLost => (FaultDisposition::Retry, FaultKind::ServiceGenerationLost(ServiceKind::Node)),
        NodeError::RpcRequestFailed { .. } => (FaultDisposition::Retry, FaultKind::RpcRequestFailed),
        NodeError::SubscriptionControlFailed { .. } => (FaultDisposition::Retry, FaultKind::SubscriptionControlFailed),
        NodeError::RecoveryInputInvalid(kind) => (FaultDisposition::Retry, FaultKind::RecoveryInputInvalid(kind)),
        NodeError::ScoreOutOfRange(reason) => (FaultDisposition::Fatal, FaultKind::ScoreOutOfRange(reason)),
        NodeError::InvalidSubscriptionState | NodeError::BlockNotFound { .. } => {
            (FaultDisposition::Fatal, FaultKind::Ownership(OwnershipFault::InvalidLifecycleControl))
        }
        NodeError::RetirementControlUnavailable | NodeError::Cancelled => {
            (FaultDisposition::Fatal, FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable))
        }
    };
    ComponentFault { source: Component::NodeService, disposition, kind, diagnostic }
}

fn rebuild_fault(error: RebuildFromPruningPointError) -> ComponentFault {
    match error {
        RebuildFromPruningPointError::Database(error) => storage_fault(error, false),
        RebuildFromPruningPointError::Setup(reason) => ComponentFault {
            source: Component::StorageService,
            disposition: FaultDisposition::Retry,
            kind: FaultKind::RebuildSetupFailed(reason),
            diagnostic: Arc::from(format!("database rebuild setup failed: {reason}")),
        },
        RebuildFromPruningPointError::ReplacementControlUnavailable { diagnostic } => ComponentFault {
            source: Component::StorageService,
            disposition: FaultDisposition::Fatal,
            kind: FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable),
            diagnostic,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use kgi_model::{
        block::{BlueWork, CompactId},
        lifecycle::{PersistenceFault, RebuildSetupFault, RecoveryInputKind},
    };
    use kgi_storage::generation::{StoredSessionSnapshot, StoredVspcSink};
    use tokio::sync::{Semaphore, mpsc};

    use super::*;

    #[derive(Clone)]
    struct Gate {
        reached: mpsc::UnboundedSender<()>,
        release: Arc<Semaphore>,
    }

    impl Gate {
        async fn wait(&self) {
            self.reached.send(()).expect("gate observer remains available");
            self.release.acquire().await.expect("gate remains open").forget();
        }
    }

    struct MockRpc {
        genesis_hash: BlockHash,
        anticone_finalization_depth: u64,
        recovery_result: Mutex<Option<Result<ValidatedRecoveryHeader, NodeError>>>,
        pruning_point_result: Mutex<Option<Result<ValidatedNodeBlock, NodeError>>>,
        recovery_gate: Option<Gate>,
        recovery_calls: AtomicUsize,
        pruning_point_calls: AtomicUsize,
    }

    impl MockRpc {
        fn new(
            recovery_result: Result<ValidatedRecoveryHeader, NodeError>,
            pruning_point_result: Result<ValidatedNodeBlock, NodeError>,
        ) -> Self {
            Self {
                genesis_hash: hash(1),
                anticone_finalization_depth: 10,
                recovery_result: Mutex::new(Some(recovery_result)),
                pruning_point_result: Mutex::new(Some(pruning_point_result)),
                recovery_gate: None,
                recovery_calls: AtomicUsize::new(0),
                pruning_point_calls: AtomicUsize::new(0),
            }
        }

        fn with_recovery_gate(mut self, gate: Gate) -> Self {
            self.recovery_gate = Some(gate);
            self
        }
    }

    impl PreparationRpc for MockRpc {
        fn genesis_hash(&self) -> BlockHash {
            self.genesis_hash
        }

        fn anticone_finalization_depth(&self) -> u64 {
            self.anticone_finalization_depth
        }

        async fn recovery_header(&self, _hash: BlockHash) -> Result<ValidatedRecoveryHeader, NodeError> {
            self.recovery_calls.fetch_add(1, Ordering::Relaxed);
            if let Some(gate) = &self.recovery_gate {
                gate.wait().await;
            }
            self.recovery_result.lock().expect("recovery-result mutex").take().expect("one recovery result")
        }

        async fn current_pruning_point_block(&self) -> Result<ValidatedNodeBlock, NodeError> {
            self.pruning_point_calls.fetch_add(1, Ordering::Relaxed);
            self.pruning_point_result.lock().expect("pruning-point-result mutex").take().expect("one pruning-point result")
        }
    }

    struct MockDb {
        state_result: Mutex<Option<Result<StoredSessionState, ProcessingDbError>>>,
        rebuild_result: Mutex<Option<Result<RebuildFromPruningPointOutcome, RebuildFromPruningPointError>>>,
        rebuild_gate: Option<Gate>,
        load_calls: AtomicUsize,
        rebuild_inputs: Mutex<Vec<ValidatedNodeBlock>>,
    }

    impl MockDb {
        fn new(
            state_result: Result<StoredSessionState, ProcessingDbError>,
            rebuild_result: Result<MaterializedSyncAnchor, RebuildFromPruningPointError>,
        ) -> Self {
            Self {
                state_result: Mutex::new(Some(state_result)),
                rebuild_result: Mutex::new(Some(rebuild_result.map(RebuildFromPruningPointOutcome::Rebuilt))),
                rebuild_gate: None,
                load_calls: AtomicUsize::new(0),
                rebuild_inputs: Mutex::new(Vec::new()),
            }
        }

        fn with_rebuild_gate(mut self, gate: Gate) -> Self {
            self.rebuild_gate = Some(gate);
            self
        }
    }

    impl PreparationDb for MockDb {
        async fn load_session_state(&self) -> Result<StoredSessionState, ProcessingDbError> {
            self.load_calls.fetch_add(1, Ordering::Relaxed);
            self.state_result.lock().expect("state-result mutex").take().expect("one state result")
        }

        async fn rebuild_from_pruning_point(
            &self,
            pruning_point: ValidatedNodeBlock,
            _cancellation: CancellationToken,
        ) -> Result<RebuildFromPruningPointOutcome, RebuildFromPruningPointError> {
            self.rebuild_inputs.lock().expect("rebuild-input mutex").push(pruning_point);
            if let Some(gate) = &self.rebuild_gate {
                gate.wait().await;
            }
            self.rebuild_result.lock().expect("rebuild-result mutex").take().expect("one rebuild result")
        }
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn header() -> ValidatedRecoveryHeader {
        ValidatedRecoveryHeader { hash: hash(4), daa_score: 50, blue_work: BlueWork::from_u64(70), blue_score: 60 }
    }

    fn pruning_point() -> ValidatedNodeBlock {
        ValidatedNodeBlock {
            hash: hash(2),
            selected_parent: hash(3),
            direct_parents: vec![hash(3)],
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: 40,
            daa_score: 50,
            blue_score: 60,
            blue_work: BlueWork::from_u64(70),
        }
    }

    fn anchor() -> MaterializedSyncAnchor {
        MaterializedSyncAnchor {
            point: VspcPoint::new(ConsensusOrder::new(BlueWork::from_u64(70), hash(4)), CompactId::new(9).expect("positive ID")),
            selected_parent: hash(5),
            blue_score: 60,
        }
    }

    fn session_state() -> StoredSessionState {
        StoredSessionState::Initialized(StoredSessionSnapshot {
            db_pp_hash: hash(2),
            db_pp_blue_score: 40,
            committed_vspc_sink: StoredVspcSink {
                hash: hash(4),
                id: CompactId::new(9).expect("positive ID"),
                selected_parent: hash(5),
                daa_score: 50,
            },
        })
    }

    fn mocks() -> (Arc<MockRpc>, Arc<MockDb>) {
        (Arc::new(MockRpc::new(Ok(header()), Ok(pruning_point()))), Arc::new(MockDb::new(Ok(session_state()), Ok(anchor()))))
    }

    #[test]
    fn boundary_threshold_covers_genesis_maximum_and_failures() {
        assert_eq!(construct_boundary_seal_blue_score(hash(1), u64::MAX, hash(1), u64::MAX), Ok(0));
        assert_eq!(construct_boundary_seal_blue_score(hash(2), MAX_BLUE_SCORE - 10, hash(1), 10), Ok(MAX_BLUE_SCORE));
        assert_eq!(construct_boundary_seal_blue_score(hash(2), u64::MAX, hash(1), 1), Err(BoundarySealThresholdError::Overflow));
        assert_eq!(
            construct_boundary_seal_blue_score(hash(2), MAX_BLUE_SCORE, hash(1), 1),
            Err(BoundarySealThresholdError::AboveMaximum)
        );
    }

    #[tokio::test]
    async fn resync_prepares_one_common_value_from_stored_and_normalized_evidence() {
        let (rpc, db) = mocks();
        let cancellation = StartupCancellation::new();

        let outcome = prepare(RecoveryMode::Resync, rpc.clone(), db.clone(), &cancellation).await;
        let PreparationOutcome::Prepared(prepared) = outcome else {
            panic!("coherent Resync must prepare");
        };
        assert!(Arc::ptr_eq(&prepared.rpc, &rpc));
        assert!(Arc::ptr_eq(&prepared.db, &db));
        assert_eq!(prepared.anchor, anchor());
        assert_eq!(prepared.boundary_seal_blue_score, 50);
        assert_eq!(rpc.recovery_calls.load(Ordering::Relaxed), 1);
        assert_eq!(rpc.pruning_point_calls.load(Ordering::Relaxed), 0);
        assert!(db.rebuild_inputs.lock().expect("rebuild-input mutex").is_empty());
    }

    #[tokio::test]
    async fn empty_and_inconsistent_state_require_a_distinct_rebuild_session() {
        for state in [StoredSessionState::Empty, StoredSessionState::Inconsistent] {
            let rpc = Arc::new(MockRpc::new(Ok(header()), Ok(pruning_point())));
            let db = Arc::new(MockDb::new(Ok(state), Ok(anchor())));
            let outcome = prepare(RecoveryMode::Resync, rpc.clone(), db, &StartupCancellation::new()).await;

            assert!(matches!(outcome, PreparationOutcome::RequireRebuild));
            assert_eq!(rpc.recovery_calls.load(Ordering::Relaxed), 0);
            assert_eq!(rpc.pruning_point_calls.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn absent_or_incoherent_resync_sink_requires_rebuild() {
        let absent_rpc = Arc::new(MockRpc::new(Err(NodeError::BlockNotFound { hash: hash(4) }), Ok(pruning_point())));
        let absent_db = Arc::new(MockDb::new(Ok(session_state()), Ok(anchor())));
        assert!(matches!(
            prepare(RecoveryMode::Resync, absent_rpc, absent_db, &StartupCancellation::new()).await,
            PreparationOutcome::RequireRebuild
        ));

        for header in [ValidatedRecoveryHeader { daa_score: 51, ..header() }, ValidatedRecoveryHeader { blue_score: 49, ..header() }] {
            let rpc = Arc::new(MockRpc::new(Ok(header), Ok(pruning_point())));
            let db = Arc::new(MockDb::new(Ok(session_state()), Ok(anchor())));
            assert!(matches!(
                prepare(RecoveryMode::Resync, rpc, db, &StartupCancellation::new()).await,
                PreparationOutcome::RequireRebuild
            ));
        }
    }

    #[tokio::test]
    async fn rebuild_uses_one_pruning_point_for_threshold_and_storage() {
        let (rpc, db) = mocks();
        let outcome = prepare(RecoveryMode::Rebuild, rpc.clone(), db.clone(), &StartupCancellation::new()).await;
        let PreparationOutcome::Prepared(prepared) = outcome else {
            panic!("coherent Rebuild must prepare");
        };

        assert_eq!(prepared.anchor, anchor());
        assert_eq!(prepared.boundary_seal_blue_score, 70);
        assert_eq!(rpc.recovery_calls.load(Ordering::Relaxed), 0);
        assert_eq!(rpc.pruning_point_calls.load(Ordering::Relaxed), 1);
        assert_eq!(&*db.rebuild_inputs.lock().expect("rebuild-input mutex"), &[pruning_point()]);
    }

    #[tokio::test]
    async fn both_modes_use_the_zero_genesis_threshold() {
        let genesis_header = ValidatedRecoveryHeader { hash: hash(1), daa_score: 0, blue_work: BlueWork::from_u64(0), blue_score: 0 };
        let genesis_anchor = MaterializedSyncAnchor {
            point: VspcPoint::new(ConsensusOrder::new(BlueWork::from_u64(0), hash(1)), CompactId::new(1).expect("positive ID")),
            selected_parent: hash(1),
            blue_score: 0,
        };
        let genesis_state = StoredSessionState::Initialized(StoredSessionSnapshot {
            db_pp_hash: hash(1),
            db_pp_blue_score: 0,
            committed_vspc_sink: StoredVspcSink {
                hash: hash(1),
                id: CompactId::new(1).expect("positive ID"),
                selected_parent: hash(1),
                daa_score: 0,
            },
        });
        let mut genesis_block = pruning_point();
        genesis_block.hash = hash(1);
        genesis_block.selected_parent = hash(1);
        genesis_block.direct_parents.clear();
        genesis_block.blue_score = 0;
        genesis_block.blue_work = BlueWork::from_u64(0);
        genesis_block.daa_score = 0;

        let rpc = Arc::new(MockRpc::new(Ok(genesis_header), Ok(genesis_block.clone())));
        let db = Arc::new(MockDb::new(Ok(genesis_state), Ok(genesis_anchor)));
        let PreparationOutcome::Prepared(prepared) = prepare(RecoveryMode::Resync, rpc, db, &StartupCancellation::new()).await else {
            panic!("Genesis Resync must prepare");
        };
        assert_eq!(prepared.boundary_seal_blue_score, 0);

        let rpc = Arc::new(MockRpc::new(Ok(genesis_header), Ok(genesis_block.clone())));
        let db = Arc::new(MockDb::new(Ok(genesis_state), Ok(genesis_anchor)));
        let PreparationOutcome::Prepared(prepared) =
            prepare(RecoveryMode::Rebuild, rpc, db.clone(), &StartupCancellation::new()).await
        else {
            panic!("Genesis Rebuild must prepare");
        };
        assert_eq!(prepared.boundary_seal_blue_score, 0);
        assert_eq!(&*db.rebuild_inputs.lock().expect("rebuild-input mutex"), &[genesis_block]);
    }

    #[tokio::test]
    async fn threshold_failure_prevents_rebuild_replacement() {
        let mut block = pruning_point();
        block.blue_score = MAX_BLUE_SCORE;
        let rpc = Arc::new(MockRpc::new(Ok(header()), Ok(block)));
        let db = Arc::new(MockDb::new(Ok(session_state()), Ok(anchor())));
        let outcome = prepare(RecoveryMode::Rebuild, rpc, db.clone(), &StartupCancellation::new()).await;
        let PreparationOutcome::Failed(fault) = outcome else {
            panic!("unrepresentable threshold must fail");
        };

        assert_eq!(fault.source, Component::ProcessingSession);
        assert_eq!(fault.disposition, FaultDisposition::Fatal);
        assert_eq!(fault.kind, FaultKind::ScoreOutOfRange(ScoreRangeFault::BoundarySealThreshold));
        assert!(db.rebuild_inputs.lock().expect("rebuild-input mutex").is_empty());
    }

    #[test]
    fn node_failures_retain_the_settled_typed_dispositions() {
        let cases = [
            (NodeError::GenerationLost, FaultDisposition::Retry, FaultKind::ServiceGenerationLost(ServiceKind::Node)),
            (NodeError::RpcRequestFailed { diagnostic: Arc::from("rpc") }, FaultDisposition::Retry, FaultKind::RpcRequestFailed),
            (
                NodeError::SubscriptionControlFailed { diagnostic: Arc::from("subscription") },
                FaultDisposition::Retry,
                FaultKind::SubscriptionControlFailed,
            ),
            (
                NodeError::RecoveryInputInvalid(RecoveryInputKind::MalformedGetBlock),
                FaultDisposition::Retry,
                FaultKind::RecoveryInputInvalid(RecoveryInputKind::MalformedGetBlock),
            ),
            (
                NodeError::ScoreOutOfRange(ScoreRangeFault::BlueScore),
                FaultDisposition::Fatal,
                FaultKind::ScoreOutOfRange(ScoreRangeFault::BlueScore),
            ),
            (
                NodeError::InvalidSubscriptionState,
                FaultDisposition::Fatal,
                FaultKind::Ownership(OwnershipFault::InvalidLifecycleControl),
            ),
            (
                NodeError::RetirementControlUnavailable,
                FaultDisposition::Fatal,
                FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable),
            ),
            (NodeError::Cancelled, FaultDisposition::Fatal, FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable)),
        ];

        for (error, disposition, kind) in cases {
            let fault = node_fault(error);
            assert_eq!(fault.source, Component::NodeService);
            assert_eq!(fault.disposition, disposition);
            assert_eq!(fault.kind, kind);
        }
    }

    #[test]
    fn rebuild_failures_retain_storage_classification() {
        for reason in [RebuildSetupFault::ApiGenerationPreparation, RebuildSetupFault::ApiReadExclusion] {
            let fault = rebuild_fault(RebuildFromPruningPointError::Setup(reason));
            assert_eq!(fault.source, Component::StorageService);
            assert_eq!(fault.disposition, FaultDisposition::Retry);
            assert_eq!(fault.kind, FaultKind::RebuildSetupFailed(reason));
        }

        let persistence =
            rebuild_fault(RebuildFromPruningPointError::Database(ProcessingDbError::Persistence(PersistenceFault::AmbiguousCommit)));
        assert_eq!(persistence.kind, FaultKind::Persistence(PersistenceFault::AmbiguousCommit));
        assert_eq!(persistence.disposition, FaultDisposition::Retry);

        let control = rebuild_fault(RebuildFromPruningPointError::ReplacementControlUnavailable { diagnostic: Arc::from("control") });
        assert_eq!(control.source, Component::StorageService);
        assert_eq!(control.disposition, FaultDisposition::Fatal);
        assert_eq!(control.kind, FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable));
    }

    #[tokio::test]
    async fn local_cancellation_wins_a_concurrent_rpc_cancellation() {
        let (reached_tx, mut reached_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let rpc = Arc::new(
            MockRpc::new(Err(NodeError::Cancelled), Ok(pruning_point()))
                .with_recovery_gate(Gate { reached: reached_tx, release: release.clone() }),
        );
        let db = Arc::new(MockDb::new(Ok(session_state()), Ok(anchor())));
        let cancellation = StartupCancellation::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move { prepare(RecoveryMode::Resync, rpc, db, &task_cancellation).await });

        reached_rx.recv().await.expect("recovery RPC reached gate");
        cancellation.cancel();
        release.add_permits(1);
        assert!(matches!(task.await.expect("preparation task"), PreparationOutcome::Cancelled));
    }

    #[tokio::test]
    async fn rebuild_cancellation_awaits_storage_and_preserves_failure() {
        let (reached_tx, mut reached_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let rpc = Arc::new(MockRpc::new(Ok(header()), Ok(pruning_point())));
        let db = Arc::new(
            MockDb::new(Ok(session_state()), Ok(anchor())).with_rebuild_gate(Gate { reached: reached_tx, release: release.clone() }),
        );
        let cancellation = StartupCancellation::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move { prepare(RecoveryMode::Rebuild, rpc, db, &task_cancellation).await });

        reached_rx.recv().await.expect("rebuild reached gate");
        cancellation.cancel();
        tokio::task::yield_now().await;
        assert!(!task.is_finished(), "cancellation must retain the rebuild future");
        release.add_permits(1);
        assert!(matches!(task.await.expect("preparation task"), PreparationOutcome::Cancelled));

        let (reached_tx, mut reached_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let rpc = Arc::new(MockRpc::new(Ok(header()), Ok(pruning_point())));
        let db = Arc::new(
            MockDb::new(
                Ok(session_state()),
                Err(RebuildFromPruningPointError::Database(ProcessingDbError::Persistence(PersistenceFault::AmbiguousCommit))),
            )
            .with_rebuild_gate(Gate { reached: reached_tx, release: release.clone() }),
        );
        let cancellation = StartupCancellation::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move { prepare(RecoveryMode::Rebuild, rpc, db, &task_cancellation).await });

        reached_rx.recv().await.expect("failed rebuild reached gate");
        cancellation.cancel();
        release.add_permits(1);
        let PreparationOutcome::Failed(fault) = task.await.expect("preparation task") else {
            panic!("typed rebuild failure must survive cancellation");
        };
        assert_eq!(fault.kind, FaultKind::Persistence(PersistenceFault::AmbiguousCommit));
    }

    #[tokio::test]
    async fn storage_setup_cancellation_is_the_session_cancelled_outcome() {
        let rpc = Arc::new(MockRpc::new(Ok(header()), Ok(pruning_point())));
        let db = Arc::new(MockDb::new(Ok(session_state()), Ok(anchor())));
        *db.rebuild_result.lock().expect("rebuild-result mutex") = Some(Ok(RebuildFromPruningPointOutcome::Cancelled));

        let outcome = prepare(RecoveryMode::Rebuild, rpc, db.clone(), &StartupCancellation::new()).await;

        assert!(matches!(outcome, PreparationOutcome::Cancelled));
        assert_eq!(&*db.rebuild_inputs.lock().expect("rebuild-input mutex"), &[pruning_point()]);
    }
}
