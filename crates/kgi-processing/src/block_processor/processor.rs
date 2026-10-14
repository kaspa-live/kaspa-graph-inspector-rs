use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use kgi_api_ingress::channel::{GraphUpdateOfferOutcome, GraphUpdateProducer, GraphUpdateProducerError};
use kgi_model::{
    block::{BlockHash, BlockPresence, CompactId, ConsensusOrder, PersistedBlock, ValidatedNodeBlock, VspcPoint},
    lifecycle::{
        BoundedProcessingState, Component, ComponentFault, FaultDisposition, FaultKind, OwnershipFault, PersistenceFault,
        RecoveryMode, ServiceKind,
    },
};
use kgi_node::error::NodeError;
use kgi_storage::{
    error::ProcessingDbError,
    operation::{MaterializeBlockError, MaterializeBlockOutcome, ReferencePolicy},
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use super::{
    BlockDatabase, BlockProcessorInitialization, BlockResolutionEvent, BlockRpc, PROCESSING_DATA_CHANNEL_CAPACITY,
    error::{ChildControlError, ChildFault, ChildFaultKind},
    marker::{LifecycleMarkerWorker, MarkerHandoffError},
    orphan::OrphanManager,
};

#[derive(Clone, Debug)]
pub(crate) struct BlockOverlap(Arc<AtomicBool>);

impl BlockOverlap {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    #[must_use]
    pub(crate) fn observed(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn reset(&self) {
        self.0.store(false, Ordering::Release);
    }

    fn observe(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BlockProcessorEvent {
    PpBoundarySealed,
    Fault(ComponentFault),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum BlockProcessorControlError {
    #[error("BlockProcessor command path is unavailable")]
    ControlUnavailable,

    #[error("BlockProcessor command acknowledgement was lost")]
    AcknowledgementLost,

    #[error("BlockProcessor rejected the command: {0:?}")]
    Rejected(ComponentFault),

    #[error("BlockProcessor worker could not be joined")]
    WorkerJoinFailed,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("BlockProcessor synthetic input endpoint is closed")]
pub(crate) struct BlockSubmissionError;

enum BlockProcessorCommand {
    Catchup { known_id_lower_bound: CompactId },
    Live,
    Shutdown(oneshot::Sender<Result<(), ComponentFault>>),
}

pub(crate) struct BlockProcessor<
    D: BlockDatabase = kgi_storage::generation::ValidatedDbClient,
    R: BlockRpc = kgi_node::rpc::ValidatedRpcClient,
> {
    commands: mpsc::UnboundedSender<BlockProcessorCommand>,
    notifications: mpsc::Sender<ValidatedNodeBlock>,
    synthetic: mpsc::Sender<ValidatedNodeBlock>,
    overlap: BlockOverlap,
    worker: Option<BlockProcessorWorker<D, R>>,
    join: Option<JoinHandle<()>>,
}

impl<D: BlockDatabase, R: BlockRpc> BlockProcessor<D, R> {
    pub(crate) fn new(
        mode: RecoveryMode,
        rpc: Arc<R>,
        db: Arc<D>,
        graph_updates: GraphUpdateProducer,
        persisted: mpsc::Sender<PersistedBlock>,
    ) -> (Self, mpsc::UnboundedReceiver<BlockProcessorEvent>) {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (notification_tx, notification_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (synthetic_tx, synthetic_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (resolution_tx, resolution_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (child_fault_tx, child_fault_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let overlap = BlockOverlap::new();
        let orphan = OrphanManager::new(rpc.clone(), resolution_tx, child_fault_tx);
        let worker = BlockProcessorWorker {
            commands: command_rx,
            notifications: notification_rx,
            synthetic: synthetic_rx,
            resolution: resolution_rx,
            child_faults: child_fault_rx,
            events: event_tx,
            persisted,
            orphan: Some(orphan),
            overlap: overlap.clone(),
            rpc,
            db,
            graph_updates: graph_updates.clone(),
            marker: Some(LifecycleMarkerWorker::new(graph_updates)),
            run: BlockProcessorRun::Inert { mode },
            faulted: false,
        };
        let processor = Self {
            commands: command_tx,
            notifications: notification_tx,
            synthetic: synthetic_tx,
            overlap,
            worker: Some(worker),
            join: None,
        };
        (processor, event_rx)
    }

    pub(crate) fn initialize(&mut self, initialization: BlockProcessorInitialization) -> Result<(), BlockProcessorControlError> {
        self.worker
            .as_mut()
            .expect("BlockProcessor initialization precedes its single start")
            .initialize(initialization)
            .map_err(BlockProcessorControlError::Rejected)
    }

    pub(crate) fn start(&mut self) -> Result<(), BlockProcessorControlError> {
        let mut worker = self.worker.take().expect("BlockProcessor starts exactly once");
        let result = worker.startup().map_err(BlockProcessorControlError::Rejected);
        self.join = Some(tokio::spawn(worker.run()));
        result
    }

    pub(crate) fn block_notification_sender(&self) -> mpsc::Sender<ValidatedNodeBlock> {
        self.notifications.clone()
    }

    pub(crate) async fn submit_synthetic(&self, block: ValidatedNodeBlock) -> Result<(), BlockSubmissionError> {
        self.synthetic.send(block).await.map_err(|_| BlockSubmissionError)
    }

    pub(crate) fn overlap(&self) -> BlockOverlap {
        self.overlap.clone()
    }

    pub(crate) fn catchup(&self, known_id_lower_bound: CompactId) -> Result<(), BlockProcessorControlError> {
        self.commands
            .send(BlockProcessorCommand::Catchup { known_id_lower_bound })
            .map_err(|_| BlockProcessorControlError::ControlUnavailable)
    }

    pub(crate) fn live(&self) -> Result<(), BlockProcessorControlError> {
        self.commands.send(BlockProcessorCommand::Live).map_err(|_| BlockProcessorControlError::ControlUnavailable)
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), BlockProcessorControlError> {
        if self.worker.take().is_some() {
            return Ok(());
        }
        let (completion, acknowledgement) = oneshot::channel();
        let result = match self.commands.send(BlockProcessorCommand::Shutdown(completion)) {
            Ok(()) => match acknowledgement.await {
                Ok(result) => result.map_err(BlockProcessorControlError::Rejected),
                Err(_) => Err(BlockProcessorControlError::AcknowledgementLost),
            },
            Err(_) => Err(BlockProcessorControlError::ControlUnavailable),
        };
        let joined = match self.join.take() {
            Some(join) => join.await.map_err(|_| BlockProcessorControlError::WorkerJoinFailed),
            None => Ok(()),
        };
        joined?;
        result
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SealPhase {
    PreSeal { seal_blue_score: u64 },
    PostSeal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunStage {
    Synchronizing(SealPhase),
    Catchup { known_id_lower_bound: CompactId },
    Live,
}

impl RunStage {
    const fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }

    const fn accepts_notifications(self) -> bool {
        matches!(self, Self::Catchup { .. } | Self::Live)
    }

    const fn permits_orphans(self) -> bool {
        matches!(self, Self::Catchup { .. } | Self::Live)
    }

    const fn seal_phase(self) -> SealPhase {
        match self {
            Self::Synchronizing(phase) => phase,
            Self::Catchup { .. } | Self::Live => SealPhase::PostSeal,
        }
    }
}

enum BlockProcessorRun {
    Inert { mode: RecoveryMode },
    Active { stage: RunStage, sources: HashMap<BlockHash, SourceBits>, emit_boundary_sealed_on_start: bool },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlockSource {
    Resolution,
    Notification,
    Synthetic,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SourceBits(u8);

impl SourceBits {
    const NOTIFICATION: u8 = 1;
    const SYNTHETIC: u8 = 2;

    fn contains(self, source: BlockSource) -> bool {
        let source = Self::from(source);
        source.0 != 0 && self.0 & source.0 != 0
    }

    fn insert(&mut self, source: BlockSource) {
        self.0 |= Self::from(source).0;
    }

    const fn overlaps(self) -> bool {
        self.0 == Self::SYNTHETIC | Self::NOTIFICATION
    }
}

impl From<BlockSource> for SourceBits {
    fn from(source: BlockSource) -> Self {
        Self(match source {
            BlockSource::Resolution => 0,
            BlockSource::Notification => Self::NOTIFICATION,
            BlockSource::Synthetic => Self::SYNTHETIC,
        })
    }
}

struct BlockProcessorWorker<D: BlockDatabase, R: BlockRpc> {
    commands: mpsc::UnboundedReceiver<BlockProcessorCommand>,
    notifications: mpsc::Receiver<ValidatedNodeBlock>,
    synthetic: mpsc::Receiver<ValidatedNodeBlock>,
    resolution: mpsc::Receiver<BlockResolutionEvent>,
    child_faults: mpsc::UnboundedReceiver<ChildFault>,
    events: mpsc::UnboundedSender<BlockProcessorEvent>,
    persisted: mpsc::Sender<PersistedBlock>,
    orphan: Option<OrphanManager<R>>,
    overlap: BlockOverlap,
    rpc: Arc<R>,
    db: Arc<D>,
    graph_updates: GraphUpdateProducer,
    marker: Option<LifecycleMarkerWorker>,
    run: BlockProcessorRun,
    faulted: bool,
}

impl<D: BlockDatabase, R: BlockRpc> BlockProcessorWorker<D, R> {
    fn initialize(&mut self, initialization: BlockProcessorInitialization) -> Result<(), ComponentFault> {
        let BlockProcessorRun::Inert { mode } = self.run else {
            return Err(invalid_lifecycle("BlockProcessor initialization requires an inert worker"));
        };
        if self.faulted {
            return Err(invalid_lifecycle("BlockProcessor initialization requires an inert worker"));
        }
        if self.rpc.network_id() != self.db.network_id()
            || self.rpc.genesis_hash() != self.db.genesis_hash()
            || initialization.anchor.blue_score > kgi_model::block::MAX_BLUE_SCORE
        {
            return Err(invalid_lifecycle("BlockProcessor generations or anchor are inconsistent"));
        }
        if mode == RecoveryMode::Resync && initialization.anchor.blue_score < initialization.boundary_seal_blue_score {
            return Err(invalid_lifecycle("Resync anchor is below the sealed boundary"));
        }
        let genesis = initialization.anchor.point.hash() == self.db.genesis_hash();
        if mode == RecoveryMode::Rebuild
            && genesis
            && (initialization.anchor.blue_score != 0 || initialization.boundary_seal_blue_score != 0)
        {
            return Err(invalid_lifecycle("Genesis Rebuild requires zero blue scores"));
        }

        let stage = if mode == RecoveryMode::Rebuild && !genesis {
            RunStage::Synchronizing(SealPhase::PreSeal { seal_blue_score: initialization.boundary_seal_blue_score })
        } else {
            RunStage::Synchronizing(SealPhase::PostSeal)
        };
        self.run = BlockProcessorRun::Active {
            stage,
            sources: HashMap::new(),
            emit_boundary_sealed_on_start: mode == RecoveryMode::Rebuild && genesis,
        };
        Ok(())
    }

    fn startup(&mut self) -> Result<(), ComponentFault> {
        let BlockProcessorRun::Active { stage, emit_boundary_sealed_on_start, .. } = self.run else {
            return Err(invalid_lifecycle("BlockProcessor start requires initialization"));
        };
        self.orphan.as_mut().expect("initialized BlockProcessor owns OrphanManager").start();
        let marker = self.marker.as_mut().expect("initialized BlockProcessor owns lifecycle-marker worker");
        marker.start();

        if !matches!(stage, RunStage::Synchronizing(SealPhase::PreSeal { .. }))
            && matches!(marker.publish_post_seal(), Err(MarkerHandoffError::CommandPathClosed))
        {
            self.faulted = true;
            return Err(ownership_fault(
                Component::BlockProcessor,
                OwnershipFault::InternalControlPathLost,
                "lifecycle-marker PostSeal handoff failed",
            ));
        }
        if emit_boundary_sealed_on_start && self.events.send(BlockProcessorEvent::PpBoundarySealed).is_err() {
            self.faulted = true;
            return Err(ownership_fault(
                Component::BlockProcessor,
                OwnershipFault::InternalControlPathLost,
                "BlockProcessor milestone path closed",
            ));
        }
        Ok(())
    }

    async fn run(mut self) {
        loop {
            while let Ok(command) = self.commands.try_recv() {
                if self.handle_command(command).await {
                    return;
                }
            }

            if self.faulted || !matches!(self.run, BlockProcessorRun::Active { .. }) {
                tokio::select! {
                    biased;
                    command = self.commands.recv() => match command {
                        Some(command) => if self.handle_command(command).await { return; },
                        None => {
                            let _ = self.shutdown_worker().await;
                            return;
                        }
                    },
                    _ = self.notifications.recv() => {}
                    _ = self.synthetic.recv() => {}
                    _ = self.resolution.recv() => {}
                    _ = self.child_faults.recv() => {}
                }
                continue;
            }

            if let Ok(fault) = self.child_faults.try_recv() {
                self.handle_child_fault(fault).await;
                continue;
            }
            if let Ok(event) = self.resolution.try_recv() {
                self.handle_resolution(event).await;
                continue;
            }
            if let Ok(block) = self.notifications.try_recv() {
                self.handle_block(block, BlockSource::Notification).await;
                continue;
            }
            if let Ok(block) = self.synthetic.try_recv() {
                self.handle_block(block, BlockSource::Synthetic).await;
                continue;
            }

            tokio::select! {
                biased;
                command = self.commands.recv() => match command {
                    Some(command) => if self.handle_command(command).await { return; },
                    None => {
                        let _ = self.shutdown_worker().await;
                        return;
                    }
                },
                fault = self.child_faults.recv() => match fault {
                    Some(fault) => self.handle_child_fault(fault).await,
                    None => self.enter_fault(ownership_fault(
                        Component::OrphanManager,
                        OwnershipFault::UnexpectedWorkerTermination,
                        "OrphanManager fault path closed",
                    )).await,
                },
                event = self.resolution.recv() => match event {
                    Some(event) => self.handle_resolution(event).await,
                    None => self.enter_fault(session_endpoint_fault(
                        Component::OrphanManager,
                        self.is_live(),
                        "block-resolution endpoint closed",
                    )).await,
                },
                block = self.notifications.recv() => if let Some(block) = block {
                    self.handle_block(block, BlockSource::Notification).await;
                },
                block = self.synthetic.recv() => if let Some(block) = block {
                    self.handle_block(block, BlockSource::Synthetic).await;
                },
            }
        }
    }

    /// Returns `true` when the command completed the worker lifecycle and the
    /// run loop must terminate; returns `false` when the worker remains active.
    async fn handle_command(&mut self, command: BlockProcessorCommand) -> bool {
        match command {
            BlockProcessorCommand::Catchup { known_id_lower_bound } => {
                if let BlockProcessorRun::Active { stage, sources, .. } = &mut self.run
                    && *stage == RunStage::Synchronizing(SealPhase::PostSeal)
                {
                    *stage = RunStage::Catchup { known_id_lower_bound };
                    sources.clear();
                    self.overlap.reset();
                } else {
                    self.enter_fault(invalid_lifecycle("Catchup requires active PostSeal synchronization")).await;
                }
                false
            }
            BlockProcessorCommand::Live => {
                if !matches!(self.run, BlockProcessorRun::Active { stage: RunStage::Catchup { .. }, .. }) {
                    self.enter_fault(invalid_lifecycle("Live requires active Catchup")).await;
                } else {
                    match self.marker.as_ref().expect("active BlockProcessor owns marker worker").publish_live() {
                        Ok(()) | Err(MarkerHandoffError::ReceiverClosed) => {
                            let BlockProcessorRun::Active { stage, .. } = &mut self.run else {
                                unreachable!("validated active BlockProcessor run remains active");
                            };
                            *stage = RunStage::Live;
                        }
                        Err(MarkerHandoffError::CommandPathClosed) => {
                            self.enter_fault(ownership_fault(
                                Component::BlockProcessor,
                                OwnershipFault::InternalControlPathLost,
                                "lifecycle-marker Live handoff failed",
                            ))
                            .await;
                        }
                    }
                }
                false
            }
            BlockProcessorCommand::Shutdown(completion) => {
                let result = self.shutdown_worker().await;
                let _ = completion.send(result);
                true
            }
        }
    }

    async fn shutdown_worker(&mut self) -> Result<(), ComponentFault> {
        self.drain_session_data();
        self.overlap.reset();
        let mut first_fault = None;
        if let Err(error) = self.shutdown_orphan().await {
            first_fault = Some(child_control_fault(Component::OrphanManager, error, "OrphanManager Shutdown barrier"));
        }
        if let Some(marker) = self.marker.take()
            && let Err(error) = marker.shutdown().await
        {
            first_fault.get_or_insert_with(|| child_control_fault(Component::BlockProcessor, error, "lifecycle-marker worker join"));
        }
        first_fault.map_or(Ok(()), Err)
    }

    async fn shutdown_orphan(&mut self) -> Result<(), ChildControlError> {
        let Some(orphan) = self.orphan.take() else {
            return Ok(());
        };
        let shutdown = orphan.shutdown();
        tokio::pin!(shutdown);
        let mut resolution_open = true;
        let mut child_faults_open = true;
        loop {
            tokio::select! {
                result = &mut shutdown => return result,
                event = self.resolution.recv(), if resolution_open => {
                    resolution_open = event.is_some();
                }
                fault = self.child_faults.recv(), if child_faults_open => {
                    child_faults_open = fault.is_some();
                }
            }
        }
    }

    fn drain_session_data(&mut self) {
        while self.notifications.try_recv().is_ok() {}
        while self.synthetic.try_recv().is_ok() {}
        while self.resolution.try_recv().is_ok() {}
        while self.child_faults.try_recv().is_ok() {}
    }

    async fn handle_resolution(&mut self, event: BlockResolutionEvent) {
        match event {
            BlockResolutionEvent::OrphanReady(block) => self.handle_block(block, BlockSource::Resolution).await,
            BlockResolutionEvent::DependencyResolved { requested_hash, result: Ok(block) } => {
                if block.hash != requested_hash {
                    self.enter_fault(ownership_fault(
                        Component::DependencyResolver,
                        OwnershipFault::InvalidLifecycleControl,
                        "DependencyResolver returned a different block hash",
                    ))
                    .await;
                } else {
                    self.handle_block(block, BlockSource::Resolution).await;
                }
            }
            BlockResolutionEvent::DependencyResolved { requested_hash, result: Err(error) } => {
                if let Some(fault) = node_fault(error, self.is_live(), requested_hash) {
                    self.enter_fault(fault).await;
                }
            }
        }
    }

    async fn handle_block(&mut self, block: ValidatedNodeBlock, source: BlockSource) {
        let BlockProcessorRun::Active { stage, .. } = &self.run else {
            return;
        };
        if source == BlockSource::Notification && !stage.accepts_notifications() {
            return;
        }

        let presence = match self.db.block_presence(block.hash).await {
            Ok(presence) => presence,
            Err(error) => {
                self.enter_fault(storage_fault(error, self.is_live())).await;
                return;
            }
        };

        let BlockProcessorRun::Active { stage, sources, .. } = &self.run else {
            unreachable!("run remains active after block-presence query");
        };
        if source == BlockSource::Notification {
            if let RunStage::Catchup { known_id_lower_bound } = stage {
                let known_id = match presence {
                    BlockPresence::Absent => None,
                    BlockPresence::BoundaryIdentity { id } | BlockPresence::Materialized { id, .. } => Some(id),
                };
                if known_id.is_some_and(|id| id < *known_id_lower_bound) {
                    return;
                }
            }
            if sources.get(&block.hash).copied().unwrap_or_default().contains(source) {
                return;
            }
        }

        let result = match presence {
            BlockPresence::BoundaryIdentity { .. } => Err(materiality_fault(block.hash, "incoming block is a boundary identity")),
            BlockPresence::Materialized { id, .. } => self.deliver_persisted(&block, id).map(|()| BlockDisposition::Persisted),
            BlockPresence::Absent => self.materialize(block.clone()).await,
        };

        match result {
            Ok(BlockDisposition::Persisted | BlockDisposition::Orphaned) => self.record_source(block.hash, source),
            Err(fault) => self.enter_fault(fault).await,
        }
    }

    async fn materialize(&mut self, block: ValidatedNodeBlock) -> Result<BlockDisposition, ComponentFault> {
        let BlockProcessorRun::Active { stage, .. } = &self.run else {
            unreachable!("materialization requires an active run");
        };
        let sealing = matches!(
            stage,
            RunStage::Synchronizing(SealPhase::PreSeal { seal_blue_score }) if block.blue_score >= *seal_blue_score
        );
        let policy = if matches!(stage.seal_phase(), SealPhase::PreSeal { .. }) && !sealing {
            ReferencePolicy::AllowBoundaryIdentities
        } else {
            ReferencePolicy::RequireMaterialized
        };
        let outcome = self.db.materialize_block(block.clone(), policy).await;
        match outcome {
            Ok(MaterializeBlockOutcome::AlreadyMaterialized { id, .. }) => {
                self.deliver_persisted(&block, id)?;
                Ok(BlockDisposition::Persisted)
            }
            Ok(MaterializeBlockOutcome::Inserted { committed }) => {
                let offer = self.graph_updates.offer_block_committed(committed.clone());
                if matches!(stage.seal_phase(), SealPhase::PreSeal { .. })
                    && !matches!(offer, Ok(GraphUpdateOfferOutcome::SuppressedPreSeal) | Err(GraphUpdateProducerError::ReceiverClosed))
                {
                    return Err(ownership_fault(
                        Component::BlockProcessor,
                        OwnershipFault::InvalidLifecycleControl,
                        "PreSeal graph update was not suppressed",
                    ));
                }

                let delivery_fault = self.deliver_persisted(&block, committed.id).err();
                if sealing {
                    if matches!(
                        self.marker.as_ref().expect("active BlockProcessor owns marker worker").publish_post_seal(),
                        Err(MarkerHandoffError::CommandPathClosed)
                    ) {
                        return Err(ownership_fault(
                            Component::BlockProcessor,
                            OwnershipFault::InternalControlPathLost,
                            "lifecycle-marker PostSeal handoff failed",
                        ));
                    }
                    let BlockProcessorRun::Active { stage, .. } = &mut self.run else {
                        unreachable!("sealing run remains active");
                    };
                    *stage = RunStage::Synchronizing(SealPhase::PostSeal);
                    self.events.send(BlockProcessorEvent::PpBoundarySealed).map_err(|_| {
                        ownership_fault(
                            Component::BlockProcessor,
                            OwnershipFault::InternalControlPathLost,
                            "BlockProcessor milestone path closed",
                        )
                    })?;
                }
                if let Some(fault) = delivery_fault {
                    return Err(fault);
                }
                Ok(BlockDisposition::Persisted)
            }
            Err(MaterializeBlockError::IncomingBoundaryIdentity { hash }) => {
                Err(materiality_fault(hash, "incoming block is a boundary identity"))
            }
            Err(MaterializeBlockError::NonMaterializedReferences { missing, identity_only }) if !identity_only.is_empty() => {
                Err(materiality_fault(
                    block.hash,
                    format!(
                        "block references include {} boundary identities and {} absent hashes",
                        identity_only.len(),
                        missing.len()
                    ),
                ))
            }
            Err(MaterializeBlockError::NonMaterializedReferences { missing, identity_only: _ }) => {
                let BlockProcessorRun::Active { stage, .. } = &self.run else {
                    unreachable!("run remains active");
                };
                if stage.permits_orphans() {
                    self.orphan.as_ref().expect("active BlockProcessor owns OrphanManager").add_orphan(block, missing).map_err(
                        |error| bounded_send_fault(Component::BlockProcessor, self.is_live(), "OrphanManager input", error),
                    )?;
                    Ok(BlockDisposition::Orphaned)
                } else {
                    Err(ComponentFault {
                        source: Component::BlockProcessor,
                        disposition: FaultDisposition::Require(RecoveryMode::Resync),
                        kind: FaultKind::ReconciliationFailed,
                        diagnostic: Arc::from("strict pre-Catchup block has absent references"),
                    })
                }
            }
            Err(MaterializeBlockError::Database(error)) => Err(storage_fault(error, self.is_live())),
        }
    }

    fn deliver_persisted(&self, block: &ValidatedNodeBlock, id: CompactId) -> Result<(), ComponentFault> {
        let persisted = PersistedBlock {
            point: VspcPoint::new(ConsensusOrder::new(block.blue_work, block.hash), id),
            selected_parent: block.selected_parent,
        };
        let mut first_fault = None;
        if let Err(error) = self.persisted.try_send(persisted) {
            first_fault =
                Some(bounded_send_fault(Component::BlockProcessor, self.is_live(), "VspcProcessor persisted-block input", error));
        }
        if let Err(error) = self.orphan.as_ref().expect("active BlockProcessor owns OrphanManager").block_persisted(block.hash) {
            first_fault
                .get_or_insert_with(|| bounded_send_fault(Component::BlockProcessor, self.is_live(), "OrphanManager input", error));
        }
        first_fault.map_or(Ok(()), Err)
    }

    fn record_source(&mut self, hash: BlockHash, source: BlockSource) {
        let BlockProcessorRun::Active { stage, sources, .. } = &mut self.run else {
            return;
        };
        if !matches!(stage, RunStage::Catchup { .. }) || source == BlockSource::Resolution {
            return;
        }
        let bits = sources.entry(hash).or_default();
        bits.insert(source);
        if bits.overlaps() {
            self.overlap.observe();
        }
    }

    async fn handle_child_fault(&mut self, fault: ChildFault) {
        let mapped = match fault.kind {
            ChildFaultKind::OrphanCapacityExhausted => ComponentFault {
                source: fault.source,
                disposition: FaultDisposition::Require(RecoveryMode::Resync),
                kind: FaultKind::BoundedStateExhausted(BoundedProcessingState::Orphans),
                diagnostic: Arc::from("orphan capacity is exhausted"),
            },
            ChildFaultKind::SessionDataFull(path) => ComponentFault {
                source: fault.source,
                disposition: FaultDisposition::Require(RecoveryMode::Resync),
                kind: FaultKind::SessionContinuityLost,
                diagnostic: Arc::from(format!("session data path is full: {path:?}")),
            },
            ChildFaultKind::SessionDataEndpointLost(path) => {
                session_endpoint_fault(fault.source, self.is_live(), format!("session data endpoint is unavailable: {path:?}"))
            }
            ChildFaultKind::Ownership(kind) => ownership_fault(fault.source, kind, "child worker ownership failure"),
            ChildFaultKind::InvalidProtocol(diagnostic) => {
                ownership_fault(fault.source, OwnershipFault::InvalidLifecycleControl, diagnostic)
            }
        };
        self.enter_fault(mapped).await;
    }

    async fn enter_fault(&mut self, fault: ComponentFault) {
        if self.faulted {
            return;
        }
        self.faulted = true;
        let _ = self.events.send(BlockProcessorEvent::Fault(fault));
        if let Err(error) = self.shutdown_orphan().await {
            let secondary = child_control_fault(Component::OrphanManager, error, "OrphanManager fault barrier");
            let _ = self.events.send(BlockProcessorEvent::Fault(secondary));
        }
    }

    fn is_live(&self) -> bool {
        matches!(&self.run, BlockProcessorRun::Active { stage, .. } if stage.is_live())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlockDisposition {
    Persisted,
    Orphaned,
}

fn active_or_live(is_live: bool, active: FaultDisposition) -> FaultDisposition {
    if is_live { FaultDisposition::Require(RecoveryMode::Resync) } else { active }
}

fn storage_fault(error: ProcessingDbError, is_live: bool) -> ComponentFault {
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

fn node_fault(error: NodeError, is_live: bool, requested_hash: BlockHash) -> Option<ComponentFault> {
    let diagnostic: Arc<str> = Arc::from(error.to_string());
    let (source, disposition, kind) = match error {
        NodeError::GenerationLost => (
            Component::NodeService,
            active_or_live(is_live, FaultDisposition::Retry),
            FaultKind::ServiceGenerationLost(ServiceKind::Node),
        ),
        NodeError::Cancelled => return None,
        NodeError::BlockNotFound { .. } => {
            (Component::DependencyResolver, FaultDisposition::Require(RecoveryMode::Rebuild), FaultKind::DependencyUnavailable)
        }
        NodeError::RpcRequestFailed { .. } => {
            (Component::NodeService, active_or_live(is_live, FaultDisposition::Retry), FaultKind::RpcRequestFailed)
        }
        NodeError::SubscriptionControlFailed { .. } => {
            (Component::NodeService, active_or_live(is_live, FaultDisposition::Retry), FaultKind::SubscriptionControlFailed)
        }
        NodeError::InvalidSubscriptionState => {
            (Component::NodeService, FaultDisposition::Fatal, FaultKind::Ownership(OwnershipFault::InvalidLifecycleControl))
        }
        NodeError::RecoveryInputInvalid(kind) => {
            (Component::NodeService, active_or_live(is_live, FaultDisposition::Retry), FaultKind::RecoveryInputInvalid(kind))
        }
        NodeError::ScoreOutOfRange(reason) => (Component::NodeService, FaultDisposition::Fatal, FaultKind::ScoreOutOfRange(reason)),
        NodeError::RetirementControlUnavailable => {
            (Component::NodeService, FaultDisposition::Fatal, FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable))
        }
    };
    Some(ComponentFault { source, disposition, kind, diagnostic: Arc::from(format!("dependency {requested_hash}: {diagnostic}")) })
}

fn materiality_fault(hash: BlockHash, diagnostic: impl std::fmt::Display) -> ComponentFault {
    ComponentFault {
        source: Component::BlockProcessor,
        disposition: FaultDisposition::Require(RecoveryMode::Rebuild),
        kind: FaultKind::MaterialityViolation,
        diagnostic: Arc::from(format!("block {hash}: {diagnostic}")),
    }
}

fn invalid_lifecycle(diagnostic: impl Into<Arc<str>>) -> ComponentFault {
    ownership_fault(Component::BlockProcessor, OwnershipFault::InvalidLifecycleControl, diagnostic)
}

fn ownership_fault(source: Component, kind: OwnershipFault, diagnostic: impl Into<Arc<str>>) -> ComponentFault {
    ComponentFault { source, disposition: FaultDisposition::Fatal, kind: FaultKind::Ownership(kind), diagnostic: diagnostic.into() }
}

fn session_endpoint_fault(source: Component, is_live: bool, diagnostic: impl Into<Arc<str>>) -> ComponentFault {
    ComponentFault {
        source,
        disposition: active_or_live(is_live, FaultDisposition::Retry),
        kind: FaultKind::Ownership(OwnershipFault::SessionDataEndpointLost),
        diagnostic: diagnostic.into(),
    }
}

fn bounded_send_fault<T>(source: Component, is_live: bool, path: &'static str, error: mpsc::error::TrySendError<T>) -> ComponentFault {
    match error {
        mpsc::error::TrySendError::Full(_) => ComponentFault {
            source,
            disposition: FaultDisposition::Require(RecoveryMode::Resync),
            kind: FaultKind::SessionContinuityLost,
            diagnostic: Arc::from(format!("{path} is full")),
        },
        mpsc::error::TrySendError::Closed(_) => session_endpoint_fault(source, is_live, format!("{path} is closed")),
    }
}

fn child_control_fault(source: Component, error: ChildControlError, diagnostic: &'static str) -> ComponentFault {
    let kind = match error {
        ChildControlError::CommandPathClosed | ChildControlError::AcknowledgementLost => OwnershipFault::InternalControlPathLost,
        ChildControlError::WorkerJoinFailed => OwnershipFault::UnexpectedWorkerTermination,
    };
    ownership_fault(source, kind, format!("{diagnostic}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_api_ingress::channel::{
        GRAPH_UPDATE_CHANNEL_CAPACITY, GraphUpdateOfferOutcome, GraphUpdateReceiveEvent, graph_update_channel,
    };
    use kgi_model::{
        block::{
            BlockColor, BlockCoordinate, BlockHash, BlockPresence, BlueWork, ConsensusOrder, MaterializedSyncAnchor, PersistedBlock,
            ValidatedNodeBlock, VspcPoint,
        },
        graph_update::{BlockCommitted, GraphUpdate},
        lifecycle::{Component, FaultDisposition, FaultKind, OwnershipFault, PersistenceFault, RecoveryMode},
    };
    use kgi_node::error::NodeError;
    use kgi_storage::{
        error::ProcessingDbError,
        operation::{MaterializeBlockError, MaterializeBlockOutcome, ReferencePolicy},
    };
    use tokio::sync::{Semaphore, mpsc};

    use super::*;
    use crate::block_processor::{BlockDatabase, BlockProcessorInitialization, BlockRpc, dependency::DependencyRpc};

    #[derive(Default)]
    struct MockDbState {
        presences: VecDeque<Result<BlockPresence, ProcessingDbError>>,
        materializations: VecDeque<Result<MaterializeBlockOutcome, MaterializeBlockError>>,
        presence_hashes: Vec<BlockHash>,
        policies: Vec<ReferencePolicy>,
    }

    struct MockDb {
        network_id: NetworkId,
        genesis_hash: BlockHash,
        state: Mutex<MockDbState>,
        presence_gate: Mutex<Option<(mpsc::UnboundedSender<()>, Arc<Semaphore>)>>,
    }

    impl MockDb {
        fn new(genesis_hash: BlockHash) -> Arc<Self> {
            Arc::new(Self {
                network_id: NetworkId::new(NetworkType::Mainnet),
                genesis_hash,
                state: Mutex::new(MockDbState::default()),
                presence_gate: Mutex::new(None),
            })
        }

        fn push_presence(&self, result: Result<BlockPresence, ProcessingDbError>) {
            self.state.lock().expect("mock DB mutex").presences.push_back(result);
        }

        fn push_materialization(&self, result: Result<MaterializeBlockOutcome, MaterializeBlockError>) {
            self.state.lock().expect("mock DB mutex").materializations.push_back(result);
        }

        fn gate_next_presence(&self) -> (mpsc::UnboundedReceiver<()>, Arc<Semaphore>) {
            let (started_tx, started_rx) = mpsc::unbounded_channel();
            let releases = Arc::new(Semaphore::new(0));
            *self.presence_gate.lock().expect("mock DB gate mutex") = Some((started_tx, releases.clone()));
            (started_rx, releases)
        }
    }

    impl BlockDatabase for MockDb {
        fn network_id(&self) -> NetworkId {
            self.network_id
        }

        fn genesis_hash(&self) -> BlockHash {
            self.genesis_hash
        }

        async fn block_presence(&self, hash: BlockHash) -> Result<BlockPresence, ProcessingDbError> {
            let result = {
                let mut state = self.state.lock().expect("mock DB mutex");
                state.presence_hashes.push(hash);
                state.presences.pop_front().expect("scripted block presence")
            };
            let gate = self.presence_gate.lock().expect("mock DB gate mutex").take();
            if let Some((started, releases)) = gate {
                let _ = started.send(());
                releases.acquire().await.expect("mock DB release semaphore remains open").forget();
            }
            result
        }

        async fn materialize_block(
            &self,
            _block: ValidatedNodeBlock,
            policy: ReferencePolicy,
        ) -> Result<MaterializeBlockOutcome, MaterializeBlockError> {
            let mut state = self.state.lock().expect("mock DB mutex");
            state.policies.push(policy);
            state.materializations.pop_front().expect("scripted materialization")
        }
    }

    struct MockRpc {
        network_id: NetworkId,
        genesis_hash: BlockHash,
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

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn id(value: i64) -> CompactId {
        CompactId::new(value).expect("positive compact ID")
    }

    fn block(block_hash: BlockHash, blue_score: u64) -> ValidatedNodeBlock {
        ValidatedNodeBlock {
            hash: block_hash,
            selected_parent: hash(2),
            direct_parents: vec![hash(2)],
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: 1,
            daa_score: 1,
            blue_score,
            blue_work: BlueWork::from_u64(blue_score),
        }
    }

    fn anchor(block_hash: BlockHash, blue_score: u64) -> MaterializedSyncAnchor {
        MaterializedSyncAnchor {
            point: VspcPoint::new(ConsensusOrder::new(BlueWork::from_u64(blue_score), block_hash), id(1)),
            selected_parent: hash(2),
            blue_score,
        }
    }

    fn committed(block: &ValidatedNodeBlock, id: CompactId) -> BlockCommitted {
        BlockCommitted {
            id,
            hash: block.hash,
            coordinate: BlockCoordinate::new(1, 0).expect("coordinate"),
            timestamp: block.timestamp,
            daa_score: block.daa_score,
            selected_parent_index: Some(0),
            direct_parents: Arc::from([]),
            blue_merge_set: Arc::from([]),
            red_merge_set: Arc::from([]),
            color: BlockColor::Gray,
            is_in_vspc: false,
            level_snapshots: Arc::from([]),
        }
    }

    struct ProcessorFixture {
        processor: BlockProcessor<MockDb, MockRpc>,
        events: mpsc::UnboundedReceiver<BlockProcessorEvent>,
        persisted: mpsc::Receiver<PersistedBlock>,
        db: Arc<MockDb>,
        rpc: Arc<MockRpc>,
    }

    fn fixture(
        genesis_hash: BlockHash,
        mode: RecoveryMode,
        sync_anchor: MaterializedSyncAnchor,
        boundary_seal_blue_score: u64,
        graph_updates: GraphUpdateProducer,
    ) -> ProcessorFixture {
        let db = MockDb::new(genesis_hash);
        let rpc = Arc::new(MockRpc { network_id: db.network_id, genesis_hash: db.genesis_hash });
        let (persisted_tx, persisted_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (mut processor, events) = BlockProcessor::new(mode, rpc.clone(), db.clone(), graph_updates, persisted_tx);
        processor
            .initialize(BlockProcessorInitialization { anchor: sync_anchor, boundary_seal_blue_score })
            .expect("BlockProcessor initialization");
        processor.start().expect("BlockProcessor start");
        ProcessorFixture { processor, events, persisted: persisted_rx, db, rpc }
    }

    #[tokio::test]
    async fn construction_and_initialization_are_inert_until_single_start() {
        let genesis = hash(1);
        let db = MockDb::new(genesis);
        let rpc = Arc::new(MockRpc { network_id: db.network_id, genesis_hash: db.genesis_hash });
        let (graph, mut graph_rx) = graph_update_channel();
        let (persisted, _persisted_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (mut processor, mut events) = BlockProcessor::new(RecoveryMode::Resync, rpc, db, graph, persisted);

        assert!(graph_rx.try_recv_update().is_err());
        assert!(events.try_recv().is_err());
        processor
            .initialize(BlockProcessorInitialization { anchor: anchor(genesis, 20), boundary_seal_blue_score: 10 })
            .expect("BlockProcessor initialization");
        assert!(graph_rx.try_recv_update().is_err());
        assert!(events.try_recv().is_err());

        processor.start().expect("BlockProcessor start");
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn final_startup_handoff_failure_remains_joinable() {
        let genesis = hash(1);
        let db = MockDb::new(genesis);
        let rpc = Arc::new(MockRpc { network_id: db.network_id, genesis_hash: db.genesis_hash });
        let weak_db = Arc::downgrade(&db);
        let weak_rpc = Arc::downgrade(&rpc);
        let (graph, _graph_rx) = graph_update_channel();
        let (persisted, _persisted_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (mut processor, events) = BlockProcessor::new(RecoveryMode::Rebuild, rpc, db, graph, persisted);
        processor
            .initialize(BlockProcessorInitialization { anchor: anchor(genesis, 0), boundary_seal_blue_score: 0 })
            .expect("BlockProcessor initialization");
        drop(events);

        assert!(matches!(
            processor.start(),
            Err(BlockProcessorControlError::Rejected(ComponentFault {
                kind: FaultKind::Ownership(OwnershipFault::InternalControlPathLost),
                ..
            }))
        ));
        processor.shutdown().await.expect("shutdown after failed final handoff");
        assert!(weak_db.upgrade().is_none());
        assert!(weak_rpc.upgrade().is_none());
    }

    #[tokio::test]
    async fn genesis_rebuild_publishes_post_seal_and_milestone_during_start() {
        let genesis = hash(1);
        let (graph, mut graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, mut events, mut persisted, .. } =
            fixture(genesis, RecoveryMode::Rebuild, anchor(genesis, 0), 0, graph);

        assert_eq!(events.recv().await, Some(BlockProcessorEvent::PpBoundarySealed));
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        assert!(persisted.try_recv().is_err(), "Genesis never enters ordinary persisted-block delivery");

        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn non_genesis_rebuild_seals_only_after_strict_commit() {
        let genesis = hash(1);
        let pruning_point = hash(3);
        let below = block(hash(4), 9);
        let sealing = block(hash(5), 10);
        let (graph, mut graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, mut events, mut persisted, db, .. } =
            fixture(genesis, RecoveryMode::Rebuild, anchor(pruning_point, 5), 10, graph);
        db.push_presence(Ok(BlockPresence::Absent));
        db.push_materialization(Ok(MaterializeBlockOutcome::Inserted { committed: committed(&below, id(4)) }));
        db.push_presence(Ok(BlockPresence::Absent));
        db.push_materialization(Ok(MaterializeBlockOutcome::Inserted { committed: committed(&sealing, id(5)) }));
        processor.submit_synthetic(below.clone()).await.expect("below-threshold block");
        assert_eq!(persisted.recv().await.map(|block| block.point.hash()), Some(below.hash));
        assert!(graph_rx.try_recv_update().is_err());
        assert!(events.try_recv().is_err());

        processor.submit_synthetic(sealing.clone()).await.expect("sealing block");
        assert_eq!(persisted.recv().await.map(|block| block.point.hash()), Some(sealing.hash));
        assert_eq!(events.recv().await, Some(BlockProcessorEvent::PpBoundarySealed));
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        assert_eq!(
            db.state.lock().expect("mock DB mutex").policies,
            [ReferencePolicy::AllowBoundaryIdentities, ReferencePolicy::RequireMaterialized]
        );

        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn catchup_filters_late_notifications_and_proves_overlap_once() {
        let genesis = hash(1);
        let shared = block(hash(20), 20);
        let late = block(hash(5), 5);
        let (graph, mut graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, mut events, mut persisted, db, .. } =
            fixture(genesis, RecoveryMode::Resync, anchor(genesis, 20), 10, graph);
        db.push_presence(Ok(BlockPresence::Materialized { id: id(5), coordinate: BlockCoordinate::new(1, 0).expect("coordinate") }));
        for _ in 0..3 {
            db.push_presence(Ok(BlockPresence::Materialized {
                id: id(20),
                coordinate: BlockCoordinate::new(1, 0).expect("coordinate"),
            }));
        }
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        processor.catchup(id(10)).expect("Catchup command");
        tokio::task::yield_now().await;

        processor.block_notification_sender().send(late).await.expect("late notification");
        processor.submit_synthetic(shared.clone()).await.expect("synthetic block");
        processor.block_notification_sender().send(shared.clone()).await.expect("overlap notification");
        processor.block_notification_sender().send(shared.clone()).await.expect("duplicate notification");

        assert_eq!(persisted.recv().await.map(|block| block.point.hash()), Some(shared.hash));
        assert_eq!(persisted.recv().await.map(|block| block.point.hash()), Some(shared.hash));
        for _ in 0..20 {
            if processor.overlap().observed() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(processor.overlap().observed());
        assert!(persisted.try_recv().is_err(), "late and repeated notifications are discarded");
        assert!(events.try_recv().is_err());
        assert_eq!(db.state.lock().expect("mock DB mutex").presence_hashes.len(), 4);

        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn closed_notification_gate_discards_without_database_admission() {
        let genesis = hash(1);
        let (graph, mut graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, events: _events, persisted: _persisted, db, .. } =
            fixture(genesis, RecoveryMode::Resync, anchor(genesis, 20), 10, graph);
        let notification = processor.block_notification_sender();
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        notification.send(block(hash(9), 9)).await.expect("pre-Catchup notification");
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }

        assert!(db.state.lock().expect("mock DB mutex").presence_hashes.is_empty());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn identity_only_reference_takes_rebuild_precedence_during_catchup() {
        let genesis = hash(1);
        let incoming = block(hash(4), 4);
        let (graph, mut graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, mut events, mut persisted, db, .. } =
            fixture(genesis, RecoveryMode::Resync, anchor(genesis, 20), 10, graph);
        db.push_presence(Ok(BlockPresence::Absent));
        db.push_materialization(Err(MaterializeBlockError::NonMaterializedReferences {
            missing: Arc::from([hash(7)]),
            identity_only: Arc::from([hash(8)]),
        }));
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        processor.catchup(id(1)).expect("Catchup command");
        processor.submit_synthetic(incoming).await.expect("synthetic block");

        assert!(matches!(
            events.recv().await,
            Some(BlockProcessorEvent::Fault(ComponentFault {
                source: Component::BlockProcessor,
                disposition: FaultDisposition::Require(RecoveryMode::Rebuild),
                kind: FaultKind::MaterialityViolation,
                ..
            }))
        ));
        assert!(persisted.try_recv().is_err());

        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn api_receiver_closure_does_not_fault_committed_processing() {
        let genesis = hash(1);
        let incoming = block(hash(4), 4);
        let (graph, graph_rx) = graph_update_channel();
        drop(graph_rx);
        let ProcessorFixture { processor, mut events, mut persisted, db, .. } =
            fixture(genesis, RecoveryMode::Resync, anchor(genesis, 20), 10, graph);
        db.push_presence(Ok(BlockPresence::Absent));
        db.push_materialization(Ok(MaterializeBlockOutcome::Inserted { committed: committed(&incoming, id(4)) }));
        processor.submit_synthetic(incoming.clone()).await.expect("synthetic block");

        assert_eq!(persisted.recv().await.map(|block| block.point.hash()), Some(incoming.hash));
        assert!(events.try_recv().is_err());
        processor.catchup(id(1)).expect("Catchup command");
        processor.live().expect("Live command");
        tokio::task::yield_now().await;
        assert!(events.try_recv().is_err());

        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn strict_pre_catchup_omission_requires_resync_without_orphan_admission() {
        let genesis = hash(1);
        let incoming = block(hash(4), 4);
        let (graph, mut graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, mut events, mut persisted, db, .. } =
            fixture(genesis, RecoveryMode::Resync, anchor(genesis, 20), 10, graph);
        db.push_presence(Ok(BlockPresence::Absent));
        db.push_materialization(Err(MaterializeBlockError::NonMaterializedReferences {
            missing: Arc::from([hash(7)]),
            identity_only: Arc::from([]),
        }));
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));

        processor.submit_synthetic(incoming).await.expect("synthetic block");

        assert!(matches!(
            events.recv().await,
            Some(BlockProcessorEvent::Fault(ComponentFault {
                source: Component::BlockProcessor,
                disposition: FaultDisposition::Require(RecoveryMode::Resync),
                kind: FaultKind::ReconciliationFailed,
                ..
            }))
        ));
        assert!(persisted.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn full_graph_feed_reports_gap_without_suppressing_persisted_delivery() {
        let genesis = hash(1);
        let incoming = block(hash(4), 4);
        let (graph, mut graph_rx) = graph_update_channel();
        let filler = graph.clone();
        let ProcessorFixture { processor, mut events, mut persisted, db, .. } =
            fixture(genesis, RecoveryMode::Resync, anchor(genesis, 20), 10, graph);
        db.push_presence(Ok(BlockPresence::Absent));
        db.push_materialization(Ok(MaterializeBlockOutcome::Inserted { committed: committed(&incoming, id(4)) }));
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        let filler_update = committed(&block(hash(99), 99), id(99));
        for _ in 0..GRAPH_UPDATE_CHANNEL_CAPACITY {
            assert_eq!(filler.offer_block_committed(filler_update.clone()), Ok(GraphUpdateOfferOutcome::Enqueued));
        }

        processor.submit_synthetic(incoming.clone()).await.expect("synthetic block");

        assert_eq!(persisted.recv().await.map(|block| block.point.hash()), Some(incoming.hash));
        assert_eq!(graph_rx.gap_generation(), 1);
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn live_command_delivers_marker_without_clearing_overlap() {
        let genesis = hash(1);
        let shared = block(hash(20), 20);
        let (graph, mut graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, mut events, mut persisted, db, .. } =
            fixture(genesis, RecoveryMode::Resync, anchor(genesis, 20), 10, graph);
        for _ in 0..2 {
            db.push_presence(Ok(BlockPresence::Materialized {
                id: id(20),
                coordinate: BlockCoordinate::new(1, 0).expect("coordinate"),
            }));
        }
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        processor.catchup(id(1)).expect("Catchup command");
        processor.submit_synthetic(shared.clone()).await.expect("synthetic block");
        processor.block_notification_sender().send(shared).await.expect("notification block");
        persisted.recv().await.expect("synthetic persisted block");
        persisted.recv().await.expect("notification persisted block");
        assert!(processor.overlap().observed());

        processor.live().expect("Live command");

        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::Live));
        assert!(processor.overlap().observed());
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn ambiguous_sealing_commit_publishes_no_marker_or_milestone() {
        let genesis = hash(1);
        let pruning_point = hash(3);
        let sealing = block(hash(5), 10);
        let (graph, mut graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, mut events, mut persisted, db, .. } =
            fixture(genesis, RecoveryMode::Rebuild, anchor(pruning_point, 5), 10, graph);
        db.push_presence(Ok(BlockPresence::Absent));
        db.push_materialization(Err(MaterializeBlockError::Database(ProcessingDbError::Persistence(
            PersistenceFault::AmbiguousCommit,
        ))));
        processor.submit_synthetic(sealing).await.expect("sealing block");

        assert!(matches!(
            events.recv().await,
            Some(BlockProcessorEvent::Fault(ComponentFault {
                source: Component::StorageService,
                disposition: FaultDisposition::Retry,
                kind: FaultKind::Persistence(PersistenceFault::AmbiguousCommit),
                ..
            }))
        ));
        assert!(events.try_recv().is_err(), "no PP-boundary milestone follows ambiguity");
        assert!(graph_rx.try_recv_update().is_err(), "no PostSeal marker follows ambiguity");
        assert!(persisted.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn queued_shutdown_preempts_remaining_block_inputs() {
        let genesis = hash(1);
        let first = block(hash(4), 4);
        let (graph, mut graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, events: _events, mut persisted, db, .. } =
            fixture(genesis, RecoveryMode::Resync, anchor(genesis, 20), 10, graph);
        db.push_presence(Ok(BlockPresence::Materialized { id: id(4), coordinate: BlockCoordinate::new(1, 0).expect("coordinate") }));
        let (mut started, releases) = db.gate_next_presence();
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        processor.submit_synthetic(first.clone()).await.expect("first input");
        started.recv().await.expect("first database operation started");
        for value in 5..25 {
            processor.submit_synthetic(block(hash(value), u64::from(value))).await.expect("queued input");
        }
        let shutting_down = tokio::spawn(processor.shutdown());
        tokio::task::yield_now().await;
        releases.add_permits(1);

        shutting_down.await.expect("shutdown task").expect("shutdown barrier");
        assert_eq!(persisted.recv().await.map(|block| block.point.hash()), Some(first.hash));
        assert_eq!(db.state.lock().expect("mock DB mutex").presence_hashes, [first.hash]);
    }

    #[tokio::test]
    async fn failed_initialization_leaves_an_inert_tree_for_consuming_shutdown() {
        let genesis = hash(1);
        let db = MockDb::new(genesis);
        let wrong_rpc = Arc::new(MockRpc { network_id: db.network_id, genesis_hash: hash(99) });
        let (graph, _graph_rx) = graph_update_channel();
        let (persisted, _persisted_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (mut processor, _events) = BlockProcessor::new(RecoveryMode::Resync, wrong_rpc, db, graph, persisted);

        assert!(matches!(
            processor.initialize(BlockProcessorInitialization { anchor: anchor(genesis, 20), boundary_seal_blue_score: 10 }),
            Err(BlockProcessorControlError::Rejected(ComponentFault {
                kind: FaultKind::Ownership(OwnershipFault::InvalidLifecycleControl),
                ..
            }))
        ));
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn consuming_shutdown_releases_exact_session_generations() {
        let genesis = hash(1);
        let (graph, _graph_rx) = graph_update_channel();
        let ProcessorFixture { processor, events: _events, persisted: _persisted, db, rpc } =
            fixture(genesis, RecoveryMode::Resync, anchor(genesis, 20), 10, graph);
        let weak_db = Arc::downgrade(&db);
        let weak_rpc = Arc::downgrade(&rpc);
        drop(db);
        drop(rpc);

        processor.shutdown().await.expect("shutdown");
        assert!(weak_db.upgrade().is_none());
        assert!(weak_rpc.upgrade().is_none());
    }

    #[tokio::test]
    async fn postcommit_delivery_failure_still_offers_graph_update_and_reports_continuity_loss() {
        let genesis = hash(1);
        let inserted = block(hash(4), 4);
        let db = MockDb::new(genesis);
        let rpc = Arc::new(MockRpc { network_id: db.network_id, genesis_hash: db.genesis_hash });
        db.push_presence(Ok(BlockPresence::Absent));
        db.push_materialization(Ok(MaterializeBlockOutcome::Inserted { committed: committed(&inserted, id(4)) }));
        let (persisted_tx, mut persisted_rx) = mpsc::channel(1);
        persisted_tx
            .send(PersistedBlock {
                point: VspcPoint::new(ConsensusOrder::new(BlueWork::from_u64(1), hash(9)), id(9)),
                selected_parent: hash(8),
            })
            .await
            .expect("fill persisted channel");
        let (graph, mut graph_rx) = graph_update_channel();
        let (mut processor, mut events) = BlockProcessor::new(RecoveryMode::Resync, rpc, db, graph, persisted_tx);
        processor
            .initialize(BlockProcessorInitialization { anchor: anchor(genesis, 4), boundary_seal_blue_score: 0 })
            .expect("BlockProcessor initialization");
        processor.start().expect("BlockProcessor start");
        assert_eq!(graph_rx.recv_update().await, Some(GraphUpdate::PublishPostSeal));

        processor.submit_synthetic(inserted.clone()).await.expect("inserted block");

        assert!(matches!(
            graph_rx.next_event().await,
            Some(GraphUpdateReceiveEvent::Update(GraphUpdate::BlockCommitted(update))) if update.hash == inserted.hash
        ));
        let fault = events.recv().await.expect("delivery fault");
        assert!(matches!(
            fault,
            BlockProcessorEvent::Fault(ComponentFault {
                source: Component::BlockProcessor,
                disposition: FaultDisposition::Require(RecoveryMode::Resync),
                kind: FaultKind::SessionContinuityLost,
                ..
            })
        ));
        assert_eq!(persisted_rx.recv().await.map(|block| block.point.hash()), Some(hash(9)));

        processor.shutdown().await.expect("shutdown");
    }

    #[test]
    fn storage_error_mapping_preserves_typed_lifecycle_policy() {
        let query = storage_fault(ProcessingDbError::QueryFailed { diagnostic: Arc::from("query") }, false);
        assert_eq!(query.source, Component::StorageService);
        assert_eq!(query.disposition, FaultDisposition::Retry);
        assert_eq!(query.kind, FaultKind::StorageQueryFailed);

        let invariant = storage_fault(ProcessingDbError::InvalidStoredState { diagnostic: Arc::from("row") }, true);
        assert_eq!(invariant.disposition, FaultDisposition::Require(RecoveryMode::Rebuild));
        assert_eq!(invariant.kind, FaultKind::StorageInvariantViolation);

        let unavailable = storage_fault(ProcessingDbError::ServiceUnavailable { diagnostic: Arc::from("control") }, false);
        assert_eq!(unavailable.disposition, FaultDisposition::Fatal);
        assert_eq!(unavailable.kind, FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable));
    }
}
