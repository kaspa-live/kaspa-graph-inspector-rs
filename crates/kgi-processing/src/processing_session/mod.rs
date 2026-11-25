mod error;
mod marker;
mod preparation;

use std::{future::Future, sync::Arc};

use kgi_api_ingress::channel::GraphUpdateProducer;
use kgi_core::channel::bounded_channel;
use kgi_model::{
    block::PersistedBlock,
    lifecycle::{Component, ComponentFault, RecoveryMode},
};
use kgi_node::{error::NodeError, rpc::ValidatedRpcClient};
use kgi_storage::generation::ValidatedDbClient;
use tokio::sync::mpsc;

use self::{
    error::{block_control, invalid_lifecycle, managed_component_unavailable, vspc_control},
    marker::LifecycleMarker,
    preparation::{PreparationDb, PreparationOutcome, PreparationRpc, StartupCancellation, prepare},
};
use crate::{
    PROCESSING_DATA_CHANNEL_CAPACITY,
    block_processor::{BlockDatabase, BlockProcessor, BlockProcessorEvent, BlockRpc},
    vspc_processor::{VspcDatabase, VspcProcessor, VspcProcessorEvent, VspcRpc},
};

/// Complete node-generation capability retained by one processing session.
pub(crate) trait SessionRpc: PreparationRpc + BlockRpc + VspcRpc {
    /// Disables routing and both remote notification subscriptions.
    fn disable_notifications(&self) -> impl Future<Output = Result<(), NodeError>> + Send;
}

impl SessionRpc for ValidatedRpcClient {
    async fn disable_notifications(&self) -> Result<(), NodeError> {
        self.disable_notifications().await
    }
}

/// Complete processing-database capability retained by one processing session.
pub(crate) trait SessionDb: PreparationDb + BlockDatabase + VspcDatabase {}

impl SessionDb for ValidatedDbClient {}

/// Reliable upward observation produced by an active processing session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProcessingSessionEvent {
    /// The rebuilding BlockProcessor durably sealed its pruning-point boundary.
    PpBoundarySealed,
    /// A session-owned component produced a typed lifecycle fault.
    Fault(ComponentFault),
}

/// Terminal outcome of the exact-once asynchronous startup barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProcessingSessionStartOutcome {
    /// The complete initialized actor tree is running.
    Started,
    /// Resync evidence requires a fresh Rebuild session.
    RequireRebuild,
    /// Startup concluded with the supplied typed fault.
    Failed(ComponentFault),
    /// The session owner cancelled startup before activation completed.
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessingSessionState {
    Inert,
    Started,
    Concluded,
}

/// Non-reusable owner of one complete processing-session actor tree.
pub(crate) struct ProcessingSession<R: SessionRpc = ValidatedRpcClient, D: SessionDb = ValidatedDbClient> {
    mode: RecoveryMode,
    rpc: Arc<R>,
    db: Arc<D>,
    block: BlockProcessor<D, R>,
    block_events: mpsc::UnboundedReceiver<BlockProcessorEvent>,
    vspc: VspcProcessor<D, R>,
    vspc_events: mpsc::UnboundedReceiver<VspcProcessorEvent>,
    marker: LifecycleMarker,
    startup_cancellation: StartupCancellation,
    state: ProcessingSessionState,
    pp_boundary_sealed: bool,
    pending_event: Option<ProcessingSessionEvent>,
}

impl<R: SessionRpc, D: SessionDb> ProcessingSession<R, D> {
    /// Constructs the complete session graph without launching work.
    pub(crate) fn new(mode: RecoveryMode, rpc: Arc<R>, db: Arc<D>, graph_updates: GraphUpdateProducer) -> Self {
        let (persisted_tx, persisted_rx) = bounded_channel::<PersistedBlock>(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (block, block_events) = BlockProcessor::new(mode, rpc.clone(), db.clone(), graph_updates.clone(), persisted_tx);
        let (vspc, vspc_events) = VspcProcessor::new(mode, rpc.clone(), db.clone(), graph_updates.clone(), persisted_rx);
        Self {
            mode,
            rpc,
            db,
            block,
            block_events,
            vspc,
            vspc_events,
            marker: LifecycleMarker::new(graph_updates),
            startup_cancellation: StartupCancellation::new(),
            state: ProcessingSessionState::Inert,
            pp_boundary_sealed: false,
            pending_event: None,
        }
    }

    /// Returns the idempotent cancellation capability captured before startup is polled.
    pub(crate) fn startup_cancellation(&self) -> StartupCancellation {
        self.startup_cancellation.clone()
    }

    /// Prepares, initializes, and launches the complete actor tree exactly once.
    pub(crate) async fn start(&mut self) -> ProcessingSessionStartOutcome {
        if self.state != ProcessingSessionState::Inert {
            return ProcessingSessionStartOutcome::Failed(invalid_lifecycle("ProcessingSession starts exactly once"));
        }
        self.state = ProcessingSessionState::Concluded;

        let prepared = match prepare(self.mode, self.rpc.clone(), self.db.clone(), &self.startup_cancellation).await {
            PreparationOutcome::Prepared(prepared) => prepared,
            PreparationOutcome::RequireRebuild if self.mode == RecoveryMode::Resync => {
                return ProcessingSessionStartOutcome::RequireRebuild;
            }
            PreparationOutcome::RequireRebuild => {
                return ProcessingSessionStartOutcome::Failed(invalid_lifecycle("Rebuild preparation requested another Rebuild"));
            }
            PreparationOutcome::Failed(fault) => return ProcessingSessionStartOutcome::Failed(fault),
            PreparationOutcome::Cancelled => return ProcessingSessionStartOutcome::Cancelled,
        };

        assert!(Arc::ptr_eq(&prepared.rpc, &self.rpc), "preparation retains the session RPC generation");
        assert!(Arc::ptr_eq(&prepared.db, &self.db), "preparation retains the session database generation");
        let genesis_rebuild =
            self.mode == RecoveryMode::Rebuild && prepared.anchor.point.hash() == PreparationRpc::genesis_hash(prepared.rpc.as_ref());
        if let Err(error) = self.block.initialize(prepared.anchor, prepared.boundary_seal_blue_score) {
            return ProcessingSessionStartOutcome::Failed(block_control(error));
        }
        if let Err(error) = self.vspc.initialize(prepared.anchor) {
            return ProcessingSessionStartOutcome::Failed(vspc_control(error));
        }
        if let Err(error) = self.block.start() {
            return ProcessingSessionStartOutcome::Failed(block_control(error));
        }
        if let Err(error) = self.vspc.start() {
            return ProcessingSessionStartOutcome::Failed(vspc_control(error));
        }
        self.marker.start();

        if genesis_rebuild {
            let event = self.block_events.try_recv();
            match event {
                Ok(BlockProcessorEvent::PpBoundarySealed) => {
                    self.pp_boundary_sealed = true;
                    self.pending_event = Some(ProcessingSessionEvent::PpBoundarySealed);
                }
                Ok(BlockProcessorEvent::Fault(fault)) => return ProcessingSessionStartOutcome::Failed(fault),
                Err(mpsc::error::TryRecvError::Empty) => {
                    return ProcessingSessionStartOutcome::Failed(invalid_lifecycle(
                        "Genesis Rebuild did not produce its startup PP-boundary milestone",
                    ));
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return ProcessingSessionStartOutcome::Failed(managed_component_unavailable(Component::BlockProcessor));
                }
            }
        }

        self.state = ProcessingSessionState::Started;
        ProcessingSessionStartOutcome::Started
    }

    /// Waits for the next reliable component fault or lifecycle milestone.
    pub(crate) async fn next_event(&mut self) -> Option<ProcessingSessionEvent> {
        if let Some(event) = self.pending_event.take() {
            return Some(event);
        }
        if self.state != ProcessingSessionState::Started {
            return None;
        }
        tokio::select! {
            event = self.block_events.recv() => self.handle_block_event(event),
            event = self.vspc_events.recv() => self.handle_vspc_event(event),
        }
    }

    fn handle_block_event(&mut self, event: Option<BlockProcessorEvent>) -> Option<ProcessingSessionEvent> {
        match event {
            Some(BlockProcessorEvent::PpBoundarySealed) if self.mode == RecoveryMode::Rebuild && !self.pp_boundary_sealed => {
                self.pp_boundary_sealed = true;
                Some(ProcessingSessionEvent::PpBoundarySealed)
            }
            Some(BlockProcessorEvent::PpBoundarySealed) => Some(ProcessingSessionEvent::Fault(invalid_lifecycle(
                "ProcessingSession received a duplicate or mode-invalid PP-boundary milestone",
            ))),
            Some(BlockProcessorEvent::Fault(fault)) => Some(ProcessingSessionEvent::Fault(fault)),
            None => Some(ProcessingSessionEvent::Fault(managed_component_unavailable(Component::BlockProcessor))),
        }
    }

    fn handle_vspc_event(&mut self, event: Option<VspcProcessorEvent>) -> Option<ProcessingSessionEvent> {
        match event {
            Some(VspcProcessorEvent::Fault(fault)) => Some(ProcessingSessionEvent::Fault(fault)),
            None => Some(ProcessingSessionEvent::Fault(managed_component_unavailable(Component::VspcProcessor))),
        }
    }

    /// Consumes the session and joins every launched descendant before returning.
    pub(crate) async fn shutdown(self) {
        let Self {
            rpc,
            db,
            block,
            block_events,
            vspc,
            vspc_events,
            marker,
            startup_cancellation,
            state,
            pp_boundary_sealed,
            pending_event,
            ..
        } = self;
        let _ = rpc.disable_notifications().await;
        let (_block_result, _vspc_result) = tokio::join!(block.shutdown(), vspc.shutdown());
        let _ = marker.shutdown().await;
        drop((rpc, db));
        drop((block_events, vspc_events, startup_cancellation, state, pp_boundary_sealed, pending_event));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_api_ingress::channel::graph_update_channel;
    use kgi_model::{
        block::{
            BlockHash, BlockPresence, BlueWork, CompactId, ConsensusOrder, MaterializedSyncAnchor, ValidatedNodeBlock,
            ValidatedRecoveryHeader, VspcPoint,
        },
        lifecycle::{FaultDisposition, FaultKind, OwnershipFault},
        vspc::ReadyVspcChange,
    };
    use kgi_storage::{
        error::ProcessingDbError,
        generation::{StoredSessionSnapshot, StoredSessionState, StoredVspcSink},
        operation::{
            ApplyVspcChangeError, MaterializeBlockError, MaterializeBlockOutcome, RebuildFromPruningPointError,
            RebuildFromPruningPointOutcome, ReferencePolicy, ResolveMaterializedIdsError, VspcCommitOutcome,
        },
    };
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::block_processor::DependencyRpc;

    struct MockRpc {
        network_id: NetworkId,
        genesis_hash: BlockHash,
        recovery: Mutex<Option<Result<ValidatedRecoveryHeader, NodeError>>>,
        pruning_point: Mutex<Option<Result<ValidatedNodeBlock, NodeError>>>,
        disable_calls: AtomicUsize,
    }

    impl MockRpc {
        fn new(genesis_hash: BlockHash, recovery: ValidatedRecoveryHeader, pruning_point: ValidatedNodeBlock) -> Arc<Self> {
            Arc::new(Self {
                network_id: NetworkId::new(NetworkType::Mainnet),
                genesis_hash,
                recovery: Mutex::new(Some(Ok(recovery))),
                pruning_point: Mutex::new(Some(Ok(pruning_point))),
                disable_calls: AtomicUsize::new(0),
            })
        }
    }

    impl PreparationRpc for MockRpc {
        fn genesis_hash(&self) -> BlockHash {
            self.genesis_hash
        }

        fn anticone_finalization_depth(&self) -> u64 {
            10
        }

        async fn recovery_header(&self, _hash: BlockHash) -> Result<ValidatedRecoveryHeader, NodeError> {
            self.recovery.lock().expect("recovery mutex").take().expect("one recovery result")
        }

        async fn current_pruning_point_block(&self) -> Result<ValidatedNodeBlock, NodeError> {
            self.pruning_point.lock().expect("pruning-point mutex").take().expect("one pruning-point result")
        }
    }

    impl DependencyRpc for MockRpc {
        async fn full_block(&self, hash: BlockHash) -> Result<ValidatedNodeBlock, NodeError> {
            Err(NodeError::BlockNotFound { hash })
        }
    }

    impl BlockRpc for MockRpc {
        fn network_id(&self) -> NetworkId {
            self.network_id
        }

        fn genesis_hash(&self) -> BlockHash {
            self.genesis_hash
        }
    }

    impl VspcRpc for MockRpc {
        async fn full_block(&self, hash: BlockHash) -> Result<ValidatedNodeBlock, NodeError> {
            Err(NodeError::BlockNotFound { hash })
        }
    }

    impl SessionRpc for MockRpc {
        async fn disable_notifications(&self) -> Result<(), NodeError> {
            self.disable_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct MockDb {
        network_id: NetworkId,
        genesis_hash: BlockHash,
        state: Mutex<Option<Result<StoredSessionState, ProcessingDbError>>>,
        rebuild: Mutex<Option<Result<RebuildFromPruningPointOutcome, RebuildFromPruningPointError>>>,
    }

    impl MockDb {
        fn new(
            genesis_hash: BlockHash,
            state: Result<StoredSessionState, ProcessingDbError>,
            rebuild: Result<MaterializedSyncAnchor, RebuildFromPruningPointError>,
        ) -> Arc<Self> {
            Arc::new(Self {
                network_id: NetworkId::new(NetworkType::Mainnet),
                genesis_hash,
                state: Mutex::new(Some(state)),
                rebuild: Mutex::new(Some(rebuild.map(RebuildFromPruningPointOutcome::Rebuilt))),
            })
        }
    }

    impl PreparationDb for MockDb {
        async fn load_session_state(&self) -> Result<StoredSessionState, ProcessingDbError> {
            self.state.lock().expect("state mutex").take().expect("one state result")
        }

        async fn rebuild_from_pruning_point(
            &self,
            _pruning_point: ValidatedNodeBlock,
            _cancellation: CancellationToken,
        ) -> Result<RebuildFromPruningPointOutcome, RebuildFromPruningPointError> {
            self.rebuild.lock().expect("rebuild mutex").take().expect("one rebuild result")
        }
    }

    impl BlockDatabase for MockDb {
        fn network_id(&self) -> NetworkId {
            self.network_id
        }

        fn genesis_hash(&self) -> BlockHash {
            self.genesis_hash
        }

        async fn block_presence(&self, _hash: BlockHash) -> Result<BlockPresence, ProcessingDbError> {
            panic!("an inert session performs no block lookup")
        }

        async fn materialize_block(
            &self,
            _block: ValidatedNodeBlock,
            _policy: ReferencePolicy,
        ) -> Result<MaterializeBlockOutcome, MaterializeBlockError> {
            panic!("an inert session performs no materialization")
        }
    }

    impl VspcDatabase for MockDb {
        async fn resolve_materialized_ids(&self, _hashes: &[BlockHash]) -> Result<Box<[CompactId]>, ResolveMaterializedIdsError> {
            panic!("an inert session performs no VSPC lookup")
        }

        async fn apply_vspc_change(&self, _change: ReadyVspcChange) -> Result<VspcCommitOutcome, ApplyVspcChangeError> {
            panic!("an inert session performs no VSPC mutation")
        }
    }

    impl SessionDb for MockDb {}

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn non_genesis_fixture() -> (Arc<MockRpc>, Arc<MockDb>) {
        let genesis = hash(1);
        let sink = hash(4);
        let anchor = MaterializedSyncAnchor {
            point: VspcPoint::new(ConsensusOrder::new(BlueWork::from_u64(70), sink), CompactId::new(9).expect("positive ID")),
            selected_parent: hash(5),
            blue_score: 60,
        };
        let recovery = ValidatedRecoveryHeader { hash: sink, daa_score: 50, blue_work: BlueWork::from_u64(70), blue_score: 60 };
        let pruning_point = ValidatedNodeBlock {
            hash: hash(2),
            selected_parent: hash(3),
            direct_parents: vec![hash(3)],
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: 40,
            daa_score: 50,
            blue_score: 60,
            blue_work: BlueWork::from_u64(70),
        };
        let state = StoredSessionState::Initialized(StoredSessionSnapshot {
            db_pp_hash: hash(2),
            db_pp_blue_score: 40,
            committed_vspc_sink: StoredVspcSink {
                hash: sink,
                id: CompactId::new(9).expect("positive ID"),
                selected_parent: hash(5),
                daa_score: 50,
            },
        });
        (MockRpc::new(genesis, recovery, pruning_point), MockDb::new(genesis, Ok(state), Ok(anchor)))
    }

    fn genesis_fixture() -> (Arc<MockRpc>, Arc<MockDb>) {
        let genesis = hash(1);
        let anchor = MaterializedSyncAnchor {
            point: VspcPoint::new(ConsensusOrder::new(BlueWork::from_u64(0), genesis), CompactId::new(1).expect("positive ID")),
            selected_parent: genesis,
            blue_score: 0,
        };
        let recovery = ValidatedRecoveryHeader { hash: genesis, daa_score: 0, blue_work: BlueWork::from_u64(0), blue_score: 0 };
        let pruning_point = ValidatedNodeBlock {
            hash: genesis,
            selected_parent: genesis,
            direct_parents: Vec::new(),
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: 0,
            daa_score: 0,
            blue_score: 0,
            blue_work: BlueWork::from_u64(0),
        };
        (MockRpc::new(genesis, recovery, pruning_point), MockDb::new(genesis, Ok(StoredSessionState::Empty), Ok(anchor)))
    }

    #[tokio::test]
    async fn resync_starts_the_complete_tree_without_publishing_a_marker() {
        let (rpc, db) = non_genesis_fixture();
        let (producer, mut graph_updates) = graph_update_channel();
        let mut session = ProcessingSession::new(RecoveryMode::Resync, rpc.clone(), db.clone(), producer);

        assert_eq!(session.start().await, ProcessingSessionStartOutcome::Started);
        assert_eq!(graph_updates.try_recv_update(), Err(mpsc::error::TryRecvError::Empty));
        assert_eq!(
            session.start().await,
            ProcessingSessionStartOutcome::Failed(invalid_lifecycle("ProcessingSession starts exactly once"))
        );

        session.shutdown().await;
        assert_eq!(rpc.disable_calls.load(Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&rpc), 1);
        assert_eq!(Arc::strong_count(&db), 1);
        assert_eq!(graph_updates.recv_update().await, None);
    }

    #[tokio::test]
    async fn genesis_rebuild_forwards_the_startup_boundary_milestone() {
        let (rpc, db) = genesis_fixture();
        let (producer, mut graph_updates) = graph_update_channel();
        let mut session = ProcessingSession::new(RecoveryMode::Rebuild, rpc, db, producer);

        assert_eq!(session.start().await, ProcessingSessionStartOutcome::Started);
        assert_eq!(session.next_event().await, Some(ProcessingSessionEvent::PpBoundarySealed));
        assert_eq!(graph_updates.try_recv_update(), Err(mpsc::error::TryRecvError::Empty));

        session.shutdown().await;
        assert_eq!(graph_updates.recv_update().await, None);
    }

    #[tokio::test]
    async fn all_nonstarted_outcomes_leave_an_inert_or_joinable_tree() {
        let (rpc, _) = non_genesis_fixture();
        let db = MockDb::new(
            rpc.genesis_hash,
            Ok(StoredSessionState::Empty),
            Err(RebuildFromPruningPointError::Database(ProcessingDbError::GenerationLost)),
        );
        let (producer, mut graph_updates) = graph_update_channel();
        let mut session = ProcessingSession::new(RecoveryMode::Resync, rpc, db, producer);
        assert_eq!(session.start().await, ProcessingSessionStartOutcome::RequireRebuild);
        session.shutdown().await;
        assert_eq!(graph_updates.recv_update().await, None);

        let (rpc, db) = non_genesis_fixture();
        let (producer, _graph_updates) = graph_update_channel();
        let mut session = ProcessingSession::new(RecoveryMode::Resync, rpc, db, producer);
        session.startup_cancellation().cancel();
        assert_eq!(session.start().await, ProcessingSessionStartOutcome::Cancelled);
        session.shutdown().await;

        let (rpc, mut db) = non_genesis_fixture();
        Arc::get_mut(&mut db).expect("fixture DB is uniquely owned").network_id = NetworkId::with_suffix(NetworkType::Testnet, 10);
        let (producer, _graph_updates) = graph_update_channel();
        let mut session = ProcessingSession::new(RecoveryMode::Resync, rpc, db, producer);
        let ProcessingSessionStartOutcome::Failed(fault) = session.start().await else {
            panic!("mismatched generations must fail startup");
        };
        assert_eq!(fault.disposition, FaultDisposition::Fatal);
        assert_eq!(fault.kind, FaultKind::Ownership(OwnershipFault::InvalidLifecycleControl));
        session.shutdown().await;
    }

    #[tokio::test]
    async fn partial_launch_failure_is_joined_by_consuming_shutdown() {
        let (rpc, db) = genesis_fixture();
        let (producer, mut graph_updates) = graph_update_channel();
        let mut session = ProcessingSession::new(RecoveryMode::Rebuild, rpc.clone(), db.clone(), producer);
        session.block_events.close();

        let ProcessingSessionStartOutcome::Failed(fault) = session.start().await else {
            panic!("closed milestone path must fail startup");
        };
        assert_eq!(fault.kind, FaultKind::Ownership(OwnershipFault::InternalControlPathLost));
        session.shutdown().await;

        assert_eq!(Arc::strong_count(&rpc), 1);
        assert_eq!(Arc::strong_count(&db), 1);
        assert_eq!(graph_updates.recv_update().await, None);
    }
}
