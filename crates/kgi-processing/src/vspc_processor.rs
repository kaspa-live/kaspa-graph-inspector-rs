use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use kgi_api_ingress::channel::GraphUpdateProducer;
use kgi_model::{
    block::{BlockHash, CompactId, ConsensusOrder, MaterializedSyncAnchor, PersistedBlock, VspcPoint},
    graph_update::VspcCommitted,
    lifecycle::{
        BoundedProcessingState, Component, ComponentFault, FaultDisposition, FaultKind, MalformedVspcNotificationReason,
        MalformedVspcResponseReason, NotificationInputKind, OwnershipFault, RecoveryInputKind, RecoveryMode, ServiceKind,
    },
    vspc::{ReadyVspcChange, VspcChange},
};
use kgi_node::{error::NodeError, rpc::ValidatedRpcClient};
use kgi_storage::{
    generation::ValidatedDbClient,
    operation::{ApplyVspcChangeError, ResolveMaterializedIdsError, VspcCommitOutcome, VspcPathConflict},
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    PROCESSING_DATA_CHANNEL_CAPACITY,
    fault::{active_or_live, storage_fault},
};

const MAX_PENDING_VSPC_CHANGES: usize = 1024;

#[derive(Clone, Debug)]
pub(crate) struct VspcOverlap(Arc<AtomicBool>);

impl VspcOverlap {
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct VspcProcessorMetricsSnapshot {
    pub pending_occupancy: usize,
    pub pending_high_water_mark: usize,
}

#[derive(Clone, Debug, Default)]
struct VspcProcessorMetrics(Arc<Mutex<VspcProcessorMetricsSnapshot>>);

impl VspcProcessorMetrics {
    fn update(&self, occupancy: usize) {
        let mut metrics = self.0.lock().expect("VSPC metrics mutex poisoned");
        metrics.pending_occupancy = occupancy;
        metrics.pending_high_water_mark = metrics.pending_high_water_mark.max(occupancy);
    }

    fn snapshot(&self) -> VspcProcessorMetricsSnapshot {
        *self.0.lock().expect("VSPC metrics mutex poisoned")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VspcProcessorEvent {
    Fault(ComponentFault),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum VspcProcessorControlError {
    #[error("VspcProcessor command path is unavailable")]
    ControlUnavailable,

    #[error("VspcProcessor command acknowledgement was lost")]
    AcknowledgementLost,

    #[error("VspcProcessor rejected the command: {0:?}")]
    Rejected(ComponentFault),

    #[error("VspcProcessor worker could not be joined")]
    WorkerJoinFailed,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("VspcProcessor synthetic input endpoint is closed")]
pub(crate) struct VspcSubmissionError;

enum VspcProcessorCommand {
    Catchup { synthetic_sink_lower_bound: VspcPoint },
    Live,
    Shutdown(oneshot::Sender<Result<(), ComponentFault>>),
}

pub(crate) trait VspcDatabase: Send + Sync + 'static {
    fn resolve_materialized_ids(
        &self,
        hashes: &[BlockHash],
    ) -> impl Future<Output = Result<Box<[CompactId]>, ResolveMaterializedIdsError>> + Send;

    fn apply_vspc_change(
        &self,
        change: ReadyVspcChange,
    ) -> impl Future<Output = Result<VspcCommitOutcome, ApplyVspcChangeError>> + Send;
}

pub(crate) trait VspcRpc: Send + Sync + 'static {
    fn full_block(&self, hash: BlockHash) -> impl Future<Output = Result<kgi_model::block::ValidatedNodeBlock, NodeError>> + Send;
}

impl VspcRpc for ValidatedRpcClient {
    async fn full_block(&self, hash: BlockHash) -> Result<kgi_model::block::ValidatedNodeBlock, NodeError> {
        ValidatedRpcClient::full_block(self, hash).await
    }
}

impl VspcDatabase for ValidatedDbClient {
    async fn resolve_materialized_ids(&self, hashes: &[BlockHash]) -> Result<Box<[CompactId]>, ResolveMaterializedIdsError> {
        self.resolve_materialized_ids(hashes).await
    }

    async fn apply_vspc_change(&self, change: ReadyVspcChange) -> Result<VspcCommitOutcome, ApplyVspcChangeError> {
        self.apply_vspc_change(change).await
    }
}

pub(crate) struct VspcProcessor<D: VspcDatabase = ValidatedDbClient, R: VspcRpc = ValidatedRpcClient> {
    commands: mpsc::UnboundedSender<VspcProcessorCommand>,
    notifications: mpsc::Sender<VspcChange>,
    synthetic: mpsc::Sender<VspcChange>,
    overlap: VspcOverlap,
    metrics: VspcProcessorMetrics,
    worker: Option<VspcProcessorWorker<D, R>>,
    join: Option<JoinHandle<()>>,
}

impl<D: VspcDatabase, R: VspcRpc> VspcProcessor<D, R> {
    pub(crate) fn new(
        mode: RecoveryMode,
        rpc: Arc<R>,
        db: Arc<D>,
        graph_updates: GraphUpdateProducer,
        persisted: mpsc::Receiver<PersistedBlock>,
    ) -> (Self, mpsc::UnboundedReceiver<VspcProcessorEvent>) {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (notification_tx, notification_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (synthetic_tx, synthetic_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let overlap = VspcOverlap::new();
        let metrics = VspcProcessorMetrics::default();
        let worker = VspcProcessorWorker {
            commands: command_rx,
            notifications: notification_rx,
            synthetic: synthetic_rx,
            persisted,
            events: event_tx,
            rpc,
            db,
            graph_updates,
            overlap: overlap.clone(),
            metrics: metrics.clone(),
            run: VspcProcessorRun::Inert { mode },
            faulted: false,
        };
        (
            Self {
                commands: command_tx,
                notifications: notification_tx,
                synthetic: synthetic_tx,
                overlap,
                metrics,
                worker: Some(worker),
                join: None,
            },
            event_rx,
        )
    }

    pub(crate) fn initialize(&mut self, anchor: MaterializedSyncAnchor) -> Result<(), VspcProcessorControlError> {
        self.worker
            .as_mut()
            .expect("VspcProcessor initialization precedes its single start")
            .initialize(anchor)
            .map_err(VspcProcessorControlError::Rejected)
    }

    pub(crate) fn start(&mut self) -> Result<(), VspcProcessorControlError> {
        let mut worker = self.worker.take().expect("VspcProcessor starts exactly once");
        let result = worker.startup().map_err(VspcProcessorControlError::Rejected);
        self.join = Some(tokio::spawn(worker.run()));
        result
    }

    pub(crate) fn notification_sender(&self) -> mpsc::Sender<VspcChange> {
        self.notifications.clone()
    }

    pub(crate) async fn submit_synthetic(&self, change: VspcChange) -> Result<(), VspcSubmissionError> {
        self.synthetic.send(change).await.map_err(|_| VspcSubmissionError)
    }

    pub(crate) fn overlap(&self) -> VspcOverlap {
        self.overlap.clone()
    }

    pub(crate) fn metrics(&self) -> VspcProcessorMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub(crate) fn catchup(&self, synthetic_sink_lower_bound: VspcPoint) -> Result<(), VspcProcessorControlError> {
        self.commands
            .send(VspcProcessorCommand::Catchup { synthetic_sink_lower_bound })
            .map_err(|_| VspcProcessorControlError::ControlUnavailable)
    }

    pub(crate) fn live(&self) -> Result<(), VspcProcessorControlError> {
        self.commands.send(VspcProcessorCommand::Live).map_err(|_| VspcProcessorControlError::ControlUnavailable)
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), VspcProcessorControlError> {
        if self.worker.take().is_some() {
            return Ok(());
        }
        let (completion, acknowledgement) = oneshot::channel();
        let result = match self.commands.send(VspcProcessorCommand::Shutdown(completion)) {
            Ok(()) => match acknowledgement.await {
                Ok(result) => result.map_err(VspcProcessorControlError::Rejected),
                Err(_) => Err(VspcProcessorControlError::AcknowledgementLost),
            },
            Err(_) => Err(VspcProcessorControlError::ControlUnavailable),
        };
        let joined = match self.join.take() {
            Some(join) => join.await.map_err(|_| VspcProcessorControlError::WorkerJoinFailed),
            None => Ok(()),
        };
        joined?;
        result
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VspcPhase {
    Synchronizing,
    Catchup { synthetic_sink_lower_bound: VspcPoint },
    Live,
}

impl VspcPhase {
    const fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }
}

enum VspcProcessorRun {
    Inert { mode: RecoveryMode },
    Active(Box<VspcState>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CandidateSource {
    Synthetic,
    Notification,
}

type PendingId = u64;

struct PendingCandidate {
    source: CandidateSource,
    change: VspcChange,
    destination: BlockHash,
    missing: HashSet<BlockHash>,
    resolved_destination_order: Option<ConsensusOrder>,
    overlap_credited: bool,
}

struct MaterializedHistory {
    by_hash: HashMap<BlockHash, PersistedBlock>,
    by_order: BTreeMap<ConsensusOrder, BlockHash>,
}

impl MaterializedHistory {
    fn new(anchor: MaterializedSyncAnchor) -> Self {
        let persisted = PersistedBlock { point: anchor.point, selected_parent: anchor.selected_parent };
        let mut history = Self { by_hash: HashMap::new(), by_order: BTreeMap::new() };
        history.insert(persisted);
        history
    }

    fn insert(&mut self, persisted: PersistedBlock) -> bool {
        let hash = persisted.point.hash();
        if self.by_hash.contains_key(&hash) {
            return false;
        }
        self.by_order.insert(*persisted.point.order(), hash);
        self.by_hash.insert(hash, persisted);
        true
    }

    fn prune_below(&mut self, sink: VspcPoint) {
        let stale_orders: Vec<_> = self.by_order.range(..*sink.order()).map(|(order, _)| *order).collect();
        for order in stale_orders {
            if let Some(hash) = self.by_order.remove(&order) {
                self.by_hash.remove(&hash);
            }
        }
    }
}

struct VspcState {
    phase: VspcPhase,
    committed_sink: VspcPoint,
    history: MaterializedHistory,
    next_pending_id: PendingId,
    pending: HashMap<PendingId, PendingCandidate>,
    missing_to_pending: HashMap<BlockHash, HashSet<PendingId>>,
    notification_by_destination: HashMap<BlockHash, PendingId>,
    resolved_notifications: BTreeMap<ConsensusOrder, PendingId>,
    synthetic_fifo: VecDeque<PendingId>,
}

impl VspcState {
    fn new(anchor: MaterializedSyncAnchor) -> Self {
        Self {
            phase: VspcPhase::Synchronizing,
            committed_sink: anchor.point,
            history: MaterializedHistory::new(anchor),
            next_pending_id: 0,
            pending: HashMap::new(),
            missing_to_pending: HashMap::new(),
            notification_by_destination: HashMap::new(),
            resolved_notifications: BTreeMap::new(),
            synthetic_fifo: VecDeque::new(),
        }
    }

    fn pending_len(&self) -> usize {
        self.pending.len()
    }

    fn allocate_pending_id(&mut self) -> PendingId {
        let id = self.next_pending_id;
        self.next_pending_id = self.next_pending_id.checked_add(1).expect("VSPC pending ID overflow");
        id
    }

    fn remove_candidate(&mut self, id: PendingId) -> Option<PendingCandidate> {
        let candidate = self.pending.remove(&id)?;
        for hash in &candidate.missing {
            if let Some(waiters) = self.missing_to_pending.get_mut(hash) {
                waiters.remove(&id);
                if waiters.is_empty() {
                    self.missing_to_pending.remove(hash);
                }
            }
        }
        if candidate.source == CandidateSource::Notification {
            self.notification_by_destination.remove(&candidate.destination);
            if let Some(order) = candidate.resolved_destination_order {
                self.resolved_notifications.remove(&order);
            }
        } else {
            self.synthetic_fifo.retain(|pending| *pending != id);
        }
        Some(candidate)
    }

    fn clear_synthetic(&mut self) {
        let ids: Vec<_> = self.synthetic_fifo.iter().copied().collect();
        for id in ids {
            self.remove_candidate(id);
        }
    }

    fn prune_history(&mut self) {
        self.history.prune_below(self.committed_sink);
        let ids: Vec<_> = self.pending.keys().copied().collect();
        for id in ids {
            self.refresh_candidate(id);
        }
    }

    fn refresh_candidate(&mut self, id: PendingId) {
        let Some(mut candidate) = self.pending.remove(&id) else {
            return;
        };
        for hash in &candidate.missing {
            if let Some(waiters) = self.missing_to_pending.get_mut(hash) {
                waiters.remove(&id);
                if waiters.is_empty() {
                    self.missing_to_pending.remove(hash);
                }
            }
        }
        if candidate.source == CandidateSource::Notification
            && let Some(order) = candidate.resolved_destination_order.take()
        {
            self.resolved_notifications.remove(&order);
        }

        candidate.missing = self.required_history(&candidate.change);
        candidate.resolved_destination_order = self.history.by_hash.get(&candidate.destination).map(|block| *block.point.order());
        for hash in &candidate.missing {
            self.missing_to_pending.entry(*hash).or_default().insert(id);
        }
        if candidate.source == CandidateSource::Notification
            && let Some(order) = candidate.resolved_destination_order
        {
            self.resolved_notifications.insert(order, id);
        }
        self.pending.insert(id, candidate);
    }

    fn required_history(&self, change: &VspcChange) -> HashSet<BlockHash> {
        let mut required = HashSet::new();
        let destination = *change.added.last().expect("admitted VSPC change has a destination");
        required.insert(destination);
        if let Some(source) = change.removed.first() {
            required.insert(*source);
        } else {
            for hash in change.added.iter() {
                required.insert(*hash);
            }
            let first = change.added.first().expect("admitted VSPC change has an added path");
            if let Some(first) = self.history.by_hash.get(first) {
                required.insert(first.selected_parent);
            }
        }
        required.retain(|hash| !self.history.by_hash.contains_key(hash));
        required
    }

    fn endpoints(&self, change: &VspcChange) -> Option<(VspcPoint, VspcPoint)> {
        let destination_hash = *change.added.last()?;
        let destination = self.history.by_hash.get(&destination_hash)?.point;
        let source_hash = match change.removed.first() {
            Some(source) => *source,
            None => {
                let first = self.history.by_hash.get(change.added.first()?)?;
                first.selected_parent
            }
        };
        let source = self.history.by_hash.get(&source_hash)?.point;
        Some((source, destination))
    }
}

struct VspcProcessorWorker<D: VspcDatabase, R: VspcRpc> {
    commands: mpsc::UnboundedReceiver<VspcProcessorCommand>,
    notifications: mpsc::Receiver<VspcChange>,
    synthetic: mpsc::Receiver<VspcChange>,
    persisted: mpsc::Receiver<PersistedBlock>,
    events: mpsc::UnboundedSender<VspcProcessorEvent>,
    rpc: Arc<R>,
    db: Arc<D>,
    graph_updates: GraphUpdateProducer,
    overlap: VspcOverlap,
    metrics: VspcProcessorMetrics,
    run: VspcProcessorRun,
    faulted: bool,
}

impl<D: VspcDatabase, R: VspcRpc> VspcProcessorWorker<D, R> {
    fn initialize(&mut self, anchor: MaterializedSyncAnchor) -> Result<(), ComponentFault> {
        let VspcProcessorRun::Inert { mode } = self.run else {
            return Err(invalid_lifecycle("VspcProcessor initialization requires an inert worker"));
        };
        let _ = mode;
        if self.faulted {
            return Err(invalid_lifecycle("VspcProcessor initialization requires an inert worker"));
        }
        self.run = VspcProcessorRun::Active(Box::new(VspcState::new(anchor)));
        self.metrics.update(0);
        self.overlap.reset();
        Ok(())
    }

    fn startup(&mut self) -> Result<(), ComponentFault> {
        if matches!(self.run, VspcProcessorRun::Active(_)) {
            Ok(())
        } else {
            Err(invalid_lifecycle("VspcProcessor start requires initialization"))
        }
    }

    async fn run(mut self) {
        loop {
            while let Ok(command) = self.commands.try_recv() {
                if self.handle_command(command).await {
                    return;
                }
            }
            if self.faulted || !matches!(self.run, VspcProcessorRun::Active(_)) {
                tokio::select! {
                    biased;
                    command = self.commands.recv() => match command {
                        Some(command) => if self.handle_command(command).await { return; },
                        None => {
                            self.report_command_path_loss();
                            return;
                        }
                    },
                    _ = self.persisted.recv() => {}
                    _ = self.synthetic.recv() => {}
                    _ = self.notifications.recv() => {}
                }
                continue;
            }

            if self.drive_once().await {
                continue;
            }

            if let Ok(persisted) = self.persisted.try_recv() {
                self.handle_persisted(persisted).await;
                continue;
            }
            if let Ok(change) = self.synthetic.try_recv() {
                self.handle_change(change, CandidateSource::Synthetic).await;
                continue;
            }
            if let Ok(change) = self.notifications.try_recv() {
                self.handle_change(change, CandidateSource::Notification).await;
                continue;
            }

            tokio::select! {
                biased;
                command = self.commands.recv() => match command {
                    Some(command) => if self.handle_command(command).await { return; },
                    None => {
                        self.report_command_path_loss();
                        return;
                    }
                },
                persisted = self.persisted.recv() => match persisted {
                    Some(persisted) => self.handle_persisted(persisted).await,
                    None => self.enter_fault(session_endpoint_fault(self.is_live(), "persisted-block input is closed")),
                },
                change = self.synthetic.recv() => if let Some(change) = change {
                    self.handle_change(change, CandidateSource::Synthetic).await;
                },
                change = self.notifications.recv() => if let Some(change) = change {
                    self.handle_change(change, CandidateSource::Notification).await;
                },
            }
        }
    }

    /// Returns `true` when the command completes the worker lifecycle.
    async fn handle_command(&mut self, command: VspcProcessorCommand) -> bool {
        match command {
            VspcProcessorCommand::Catchup { synthetic_sink_lower_bound } => {
                if let VspcProcessorRun::Active(state) = &mut self.run
                    && state.phase == VspcPhase::Synchronizing
                {
                    state.phase = VspcPhase::Catchup { synthetic_sink_lower_bound };
                    self.overlap.reset();
                } else {
                    self.enter_fault(invalid_lifecycle("Catchup requires active synchronization"));
                }
                false
            }
            VspcProcessorCommand::Live => {
                if let VspcProcessorRun::Active(state) = &mut self.run
                    && matches!(state.phase, VspcPhase::Catchup { .. })
                {
                    state.clear_synthetic();
                    while self.synthetic.try_recv().is_ok() {}
                    state.phase = VspcPhase::Live;
                    state.prune_history();
                    self.metrics.update(state.pending_len());
                } else {
                    self.enter_fault(invalid_lifecycle("Live requires active Catchup"));
                }
                false
            }
            VspcProcessorCommand::Shutdown(completion) => {
                self.clear_state();
                let _ = completion.send(Ok(()));
                true
            }
        }
    }

    async fn handle_persisted(&mut self, persisted: PersistedBlock) {
        let VspcProcessorRun::Active(state) = &mut self.run else {
            return;
        };
        if !state.history.insert(persisted) {
            return;
        }
        let waiting: Vec<_> =
            state.missing_to_pending.get(&persisted.point.hash()).map_or_else(Vec::new, |waiting| waiting.iter().copied().collect());
        for id in waiting {
            state.refresh_candidate(id);
        }
    }

    async fn handle_change(&mut self, change: VspcChange, source: CandidateSource) {
        let Some(destination) = change.added.last().copied() else {
            self.enter_fault(malformed_change(source, MalformedVspcNotificationReason::RemovedChainWithoutAddedPath));
            return;
        };
        let VspcProcessorRun::Active(state) = &mut self.run else {
            return;
        };
        if source == CandidateSource::Notification {
            if !matches!(state.phase, VspcPhase::Catchup { .. } | VspcPhase::Live) {
                return;
            }
            if let Some(existing_id) = state.notification_by_destination.get(&destination).copied() {
                let existing = &state.pending[&existing_id].change;
                let reason = if existing == &change {
                    MalformedVspcNotificationReason::DuplicatePendingTransition
                } else {
                    MalformedVspcNotificationReason::ContradictoryDestination
                };
                self.enter_fault(malformed_notification(reason, RecoveryMode::Resync));
                return;
            }
        } else if matches!(state.phase, VspcPhase::Live) {
            return;
        }
        if state.pending_len() == MAX_PENDING_VSPC_CHANGES {
            self.enter_fault(ComponentFault {
                source: Component::VspcProcessor,
                disposition: FaultDisposition::Require(RecoveryMode::Resync),
                kind: FaultKind::BoundedStateExhausted(BoundedProcessingState::VspcPending),
                diagnostic: Arc::from("VSPC pending capacity is exhausted"),
            });
            return;
        }
        let id = state.allocate_pending_id();
        state.pending.insert(
            id,
            PendingCandidate {
                source,
                change,
                destination,
                missing: HashSet::new(),
                resolved_destination_order: None,
                overlap_credited: false,
            },
        );
        if source == CandidateSource::Synthetic {
            state.synthetic_fifo.push_back(id);
        } else {
            state.notification_by_destination.insert(destination, id);
        }
        state.refresh_candidate(id);
        self.metrics.update(state.pending_len());
    }

    /// Advances at most one ready state-machine action so the owner loop can
    /// recheck its prioritized lifecycle-command mailbox between actions.
    async fn drive_once(&mut self) -> bool {
        if self.faulted {
            return false;
        }
        let Some(action) = self.next_action() else {
            return false;
        };
        match action {
            DriveAction::Discard(id, overlap) => {
                let VspcProcessorRun::Active(state) = &mut self.run else {
                    return false;
                };
                state.remove_candidate(id);
                self.metrics.update(state.pending_len());
                if overlap {
                    self.overlap.observe();
                }
            }
            DriveAction::NormalizeCrossing(id, suffix) => {
                let VspcProcessorRun::Active(state) = &mut self.run else {
                    return false;
                };
                if let Some(candidate) = state.pending.get_mut(&id) {
                    candidate.change = VspcChange { removed: Arc::from([]), added: suffix };
                    candidate.overlap_credited = true;
                }
                state.refresh_candidate(id);
                self.overlap.observe();
            }
            DriveAction::MarkOverlap(id) => {
                let VspcProcessorRun::Active(state) = &mut self.run else {
                    return false;
                };
                if let Some(candidate) = state.pending.get_mut(&id) {
                    candidate.overlap_credited = true;
                }
                self.overlap.observe();
            }
            DriveAction::Commit(id) => {
                if !self.commit_candidate(id).await {
                    return false;
                }
            }
            DriveAction::CompetingNotifications => {
                self.enter_fault(malformed_notification(MalformedVspcNotificationReason::CompetingNextMove, RecoveryMode::Resync));
            }
            DriveAction::SyntheticSourceMismatch => {
                self.enter_fault(malformed_synthetic(MalformedVspcResponseReason::ResolvedSourceDiscontinuity));
            }
        }
        true
    }

    fn next_action(&self) -> Option<DriveAction> {
        let VspcProcessorRun::Active(state) = &self.run else {
            return None;
        };
        if let Some(id) = state.synthetic_fifo.front().copied() {
            let candidate = &state.pending[&id];
            if candidate.missing.is_empty() {
                let (source, _) = state.endpoints(&candidate.change)?;
                return Some(if source == state.committed_sink {
                    DriveAction::Commit(id)
                } else {
                    DriveAction::SyntheticSourceMismatch
                });
            }
        }

        match state.phase {
            VspcPhase::Synchronizing => None,
            VspcPhase::Catchup { synthetic_sink_lower_bound } => {
                for (order, id) in &state.resolved_notifications {
                    let candidate = &state.pending[id];
                    if order < synthetic_sink_lower_bound.order() {
                        return Some(DriveAction::Discard(*id, false));
                    }
                    if order <= state.committed_sink.order() {
                        return Some(DriveAction::Discard(*id, true));
                    }
                    if let Some(position) = candidate.change.added.iter().position(|hash| *hash == state.committed_sink.hash()) {
                        let suffix: Arc<[BlockHash]> = candidate.change.added[position + 1..].to_vec().into();
                        if !suffix.is_empty() {
                            return Some(DriveAction::NormalizeCrossing(*id, suffix));
                        }
                    }
                    if !candidate.overlap_credited
                        && let Some((source, _)) = state.endpoints(&candidate.change)
                        && source == state.committed_sink
                    {
                        return Some(DriveAction::MarkOverlap(*id));
                    }
                }
                None
            }
            VspcPhase::Live => {
                if let Some((_, id)) = state.resolved_notifications.range(..=*state.committed_sink.order()).next() {
                    return Some(DriveAction::Discard(*id, false));
                }
                let actionable: Vec<_> = state
                    .resolved_notifications
                    .values()
                    .copied()
                    .filter(|id| {
                        let candidate = &state.pending[id];
                        state.endpoints(&candidate.change).is_some_and(|(source, _)| source == state.committed_sink)
                    })
                    .collect();
                match actionable.as_slice() {
                    [] => None,
                    [id] => Some(DriveAction::Commit(*id)),
                    _ => Some(DriveAction::CompetingNotifications),
                }
            }
        }
    }

    async fn commit_candidate(&mut self, id: PendingId) -> bool {
        let prepared = match self.prepare_candidate(id).await {
            Ok(prepared) => prepared,
            Err(fault) => {
                self.enter_fault(fault);
                return true;
            }
        };
        let result = self.db.apply_vspc_change(prepared.ready.clone()).await;
        match result {
            Ok(outcome) => self.finish_commit(id, prepared, outcome),
            Err(ApplyVspcChangeError::Database(error)) => self.enter_fault(storage_fault(error, self.is_live())),
            Err(ApplyVspcChangeError::VspcSourceDiscontinuity) => {
                self.enter_fault(source_discontinuity(prepared.source));
            }
            Err(ApplyVspcChangeError::VspcMemberNotMaterialized { id }) => {
                self.enter_fault(ComponentFault {
                    source: Component::VspcProcessor,
                    disposition: FaultDisposition::Require(RecoveryMode::Rebuild),
                    kind: FaultKind::MaterialityViolation,
                    diagnostic: Arc::from(format!("VSPC member {id} is not materialized")),
                });
            }
            Err(ApplyVspcChangeError::VspcPathDiscontinuity(conflict)) => {
                let fault = self.attribute_path(prepared.source, conflict).await;
                if let Some(fault) = fault {
                    self.enter_fault(fault);
                } else {
                    return false;
                }
            }
        }
        true
    }

    async fn prepare_candidate(&self, id: PendingId) -> Result<PreparedCandidate, ComponentFault> {
        let VspcProcessorRun::Active(state) = &self.run else {
            return Err(invalid_lifecycle("VSPC preparation requires an active run"));
        };
        let candidate = &state.pending[&id];
        let (source, destination) = state
            .endpoints(&candidate.change)
            .ok_or_else(|| invalid_lifecycle("VSPC candidate became actionable without endpoint history"))?;
        let (removed, added) = if candidate.change.removed.is_empty() {
            let added = candidate
                .change
                .added
                .iter()
                .map(|hash| state.history.by_hash.get(hash).map(|block| block.point.id()))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| invalid_lifecycle("added-only VSPC readiness lacks member history"))?;
            (Arc::from([]), added.into())
        } else {
            let mut distinct = Vec::new();
            let mut seen = HashSet::new();
            for hash in candidate.change.removed.iter().chain(candidate.change.added.iter()) {
                if seen.insert(*hash) {
                    distinct.push(*hash);
                }
            }
            let ids = self.db.resolve_materialized_ids(&distinct).await.map_err(|error| match error {
                ResolveMaterializedIdsError::Database(error) => storage_fault(error, self.is_live()),
                ResolveMaterializedIdsError::NonMaterialized { missing, identity_only } => ComponentFault {
                    source: Component::VspcProcessor,
                    disposition: FaultDisposition::Require(RecoveryMode::Rebuild),
                    kind: FaultKind::MaterialityViolation,
                    diagnostic: Arc::from(format!(
                        "VSPC chain contains {} absent and {} boundary-only members",
                        missing.len(),
                        identity_only.len()
                    )),
                },
            })?;
            let by_hash: HashMap<_, _> = distinct.into_iter().zip(ids).collect();
            let removed = candidate.change.removed.iter().map(|hash| by_hash[hash]).collect::<Vec<_>>().into();
            let added = candidate.change.added.iter().map(|hash| by_hash[hash]).collect::<Vec<_>>().into();
            (removed, added)
        };
        Ok(PreparedCandidate {
            source: candidate.source,
            hashes: candidate.change.clone(),
            ready: ReadyVspcChange { source, destination, removed, added },
        })
    }

    fn finish_commit(&mut self, id: PendingId, prepared: PreparedCandidate, outcome: VspcCommitOutcome) {
        let VspcProcessorRun::Active(state) = &mut self.run else {
            return;
        };
        state.committed_sink = outcome.destination;
        state.remove_candidate(id);
        if !matches!(state.phase, VspcPhase::Catchup { .. }) {
            state.prune_history();
        }
        self.metrics.update(state.pending_len());
        let update = VspcCommitted {
            source: prepared.ready.source.hash(),
            destination: outcome.destination.hash(),
            removed: prepared.hashes.removed,
            added: prepared.hashes.added,
            level_snapshots: outcome.level_snapshots,
        };
        let _ = self.graph_updates.offer_vspc_committed(update);
    }

    async fn attribute_path(&self, source: CandidateSource, conflict: VspcPathConflict) -> Option<ComponentFault> {
        match self.rpc.full_block(conflict.child).await {
            Ok(block) => {
                let attribution = if block.selected_parent == conflict.expected_parent {
                    VspcPathAttribution::StoredParentConflict
                } else if block.selected_parent == conflict.stored_parent {
                    VspcPathAttribution::CandidatePathConflict
                } else {
                    VspcPathAttribution::StoredAndCandidateConflict
                };
                Some(attribution_fault(source, attribution))
            }
            Err(NodeError::BlockNotFound { .. }) => Some(attribution_fault(source, VspcPathAttribution::AttributionBlockUnavailable)),
            Err(NodeError::Cancelled) => None,
            Err(error) => Some(node_operation_fault(error, self.is_live(), conflict.child)),
        }
    }

    fn clear_state(&mut self) {
        while self.notifications.try_recv().is_ok() {}
        while self.synthetic.try_recv().is_ok() {}
        while self.persisted.try_recv().is_ok() {}
        self.metrics.update(0);
    }

    fn enter_fault(&mut self, fault: ComponentFault) {
        if self.faulted {
            return;
        }
        self.faulted = true;
        let _ = self.events.send(VspcProcessorEvent::Fault(fault));
    }

    fn report_command_path_loss(&mut self) {
        if self.faulted || !matches!(self.run, VspcProcessorRun::Active(_)) {
            return;
        }
        self.enter_fault(ComponentFault {
            source: Component::VspcProcessor,
            disposition: FaultDisposition::Fatal,
            kind: FaultKind::Ownership(OwnershipFault::InternalControlPathLost),
            diagnostic: Arc::from("VspcProcessor command path closed while the worker was active"),
        });
    }

    fn is_live(&self) -> bool {
        matches!(&self.run, VspcProcessorRun::Active(state) if state.phase.is_live())
    }
}

struct PreparedCandidate {
    source: CandidateSource,
    hashes: VspcChange,
    ready: ReadyVspcChange,
}

enum DriveAction {
    Discard(PendingId, bool),
    NormalizeCrossing(PendingId, Arc<[BlockHash]>),
    MarkOverlap(PendingId),
    Commit(PendingId),
    CompetingNotifications,
    SyntheticSourceMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VspcPathAttribution {
    StoredParentConflict,
    CandidatePathConflict,
    StoredAndCandidateConflict,
    AttributionBlockUnavailable,
}

fn invalid_lifecycle(diagnostic: impl Into<Arc<str>>) -> ComponentFault {
    ComponentFault {
        source: Component::VspcProcessor,
        disposition: FaultDisposition::Fatal,
        kind: FaultKind::Ownership(OwnershipFault::InvalidLifecycleControl),
        diagnostic: diagnostic.into(),
    }
}

fn session_endpoint_fault(is_live: bool, diagnostic: impl Into<Arc<str>>) -> ComponentFault {
    ComponentFault {
        source: Component::VspcProcessor,
        disposition: if is_live { FaultDisposition::Require(RecoveryMode::Resync) } else { FaultDisposition::Retry },
        kind: FaultKind::Ownership(OwnershipFault::SessionDataEndpointLost),
        diagnostic: diagnostic.into(),
    }
}

fn malformed_change(source: CandidateSource, reason: MalformedVspcNotificationReason) -> ComponentFault {
    match source {
        CandidateSource::Synthetic => malformed_synthetic(MalformedVspcResponseReason::RemovedChainWithoutAddedPath),
        CandidateSource::Notification => malformed_notification(reason, RecoveryMode::Resync),
    }
}

fn malformed_notification(reason: MalformedVspcNotificationReason, mode: RecoveryMode) -> ComponentFault {
    ComponentFault {
        source: Component::VspcProcessor,
        disposition: FaultDisposition::Require(mode),
        kind: FaultKind::NotificationInputInvalid(NotificationInputKind::MalformedVspcChange(reason)),
        diagnostic: Arc::from(format!("malformed VSPC notification: {reason:?}")),
    }
}

fn malformed_synthetic(reason: MalformedVspcResponseReason) -> ComponentFault {
    ComponentFault {
        source: Component::VspcProcessor,
        disposition: FaultDisposition::Retry,
        kind: FaultKind::RecoveryInputInvalid(RecoveryInputKind::MalformedVspcResponse(reason)),
        diagnostic: Arc::from(format!("malformed synthetic VSPC response: {reason:?}")),
    }
}

fn source_discontinuity(source: CandidateSource) -> ComponentFault {
    match source {
        CandidateSource::Synthetic => malformed_synthetic(MalformedVspcResponseReason::ResolvedSourceDiscontinuity),
        CandidateSource::Notification => {
            malformed_notification(MalformedVspcNotificationReason::ResolvedSourceDiscontinuity, RecoveryMode::Resync)
        }
    }
}

fn attribution_fault(source: CandidateSource, attribution: VspcPathAttribution) -> ComponentFault {
    match (source, attribution) {
        (_, VspcPathAttribution::StoredParentConflict) => ComponentFault {
            source: Component::VspcProcessor,
            disposition: FaultDisposition::Require(RecoveryMode::Rebuild),
            kind: FaultKind::ReconciliationFailed,
            diagnostic: Arc::from("stored VSPC selected parent conflicts with the current node"),
        },
        (CandidateSource::Synthetic, VspcPathAttribution::CandidatePathConflict) => {
            malformed_synthetic(MalformedVspcResponseReason::SelectedParentPathDiscontinuity)
        }
        (CandidateSource::Synthetic, VspcPathAttribution::StoredAndCandidateConflict) => {
            let mut fault = malformed_synthetic(MalformedVspcResponseReason::SelectedParentPathDiscontinuity);
            fault.disposition = FaultDisposition::Require(RecoveryMode::Rebuild);
            fault
        }
        (CandidateSource::Synthetic, VspcPathAttribution::AttributionBlockUnavailable) => {
            malformed_synthetic(MalformedVspcResponseReason::SelectedParentPathDiscontinuity)
        }
        (
            CandidateSource::Notification,
            VspcPathAttribution::CandidatePathConflict | VspcPathAttribution::AttributionBlockUnavailable,
        ) => malformed_notification(MalformedVspcNotificationReason::SelectedParentPathDiscontinuity, RecoveryMode::Resync),
        (CandidateSource::Notification, VspcPathAttribution::StoredAndCandidateConflict) => {
            malformed_notification(MalformedVspcNotificationReason::SelectedParentPathDiscontinuity, RecoveryMode::Rebuild)
        }
    }
}

fn node_operation_fault(error: NodeError, is_live: bool, hash: BlockHash) -> ComponentFault {
    let diagnostic = Arc::from(format!("VSPC attribution block {hash}: {error}"));
    let (disposition, kind) = match error {
        NodeError::GenerationLost => {
            (active_or_live(is_live, FaultDisposition::Retry), FaultKind::ServiceGenerationLost(ServiceKind::Node))
        }
        NodeError::RpcRequestFailed { .. } => (active_or_live(is_live, FaultDisposition::Retry), FaultKind::RpcRequestFailed),
        NodeError::RecoveryInputInvalid(kind) => {
            (active_or_live(is_live, FaultDisposition::Retry), FaultKind::RecoveryInputInvalid(kind))
        }
        NodeError::ScoreOutOfRange(reason) => (FaultDisposition::Fatal, FaultKind::ScoreOutOfRange(reason)),
        NodeError::RetirementControlUnavailable => {
            (FaultDisposition::Fatal, FaultKind::Ownership(OwnershipFault::ManagedComponentUnavailable))
        }
        NodeError::SubscriptionControlFailed { .. } => {
            (active_or_live(is_live, FaultDisposition::Retry), FaultKind::SubscriptionControlFailed)
        }
        NodeError::InvalidSubscriptionState => {
            (FaultDisposition::Fatal, FaultKind::Ownership(OwnershipFault::InvalidLifecycleControl))
        }
        NodeError::Cancelled => return session_endpoint_fault(is_live, diagnostic),
        NodeError::BlockNotFound { .. } => unreachable!("definitive absence is handled as path attribution"),
    };
    ComponentFault { source: Component::NodeService, disposition, kind, diagnostic }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Weak};

    use kgi_api_ingress::channel::{GraphUpdateReceiveEvent, graph_update_channel};
    use kgi_model::{
        block::{BlueWork, ConsensusOrder},
        graph_update::GraphUpdate,
    };
    use tokio::sync::{Semaphore, mpsc};

    use super::*;
    #[derive(Default)]
    struct MockDbState {
        resolved_batches: Vec<Vec<BlockHash>>,
        applied: Vec<ReadyVspcChange>,
        resolution_results: VecDeque<Result<Box<[CompactId]>, ResolveMaterializedIdsError>>,
        apply_results: VecDeque<Result<VspcCommitOutcome, ApplyVspcChangeError>>,
    }

    #[derive(Default)]
    struct MockDb {
        state: Mutex<MockDbState>,
        apply_gate: Mutex<Option<(mpsc::UnboundedSender<()>, Arc<Semaphore>)>>,
    }

    impl MockDb {
        fn push_resolution(&self, result: Result<Box<[CompactId]>, ResolveMaterializedIdsError>) {
            self.state.lock().expect("mock DB mutex").resolution_results.push_back(result);
        }

        fn push_apply(&self, result: Result<VspcCommitOutcome, ApplyVspcChangeError>) {
            self.state.lock().expect("mock DB mutex").apply_results.push_back(result);
        }

        fn gate_next_apply(&self) -> (mpsc::UnboundedReceiver<()>, Arc<Semaphore>) {
            let (started_tx, started_rx) = mpsc::unbounded_channel();
            let releases = Arc::new(Semaphore::new(0));
            *self.apply_gate.lock().expect("mock DB gate mutex") = Some((started_tx, releases.clone()));
            (started_rx, releases)
        }
    }

    impl VspcDatabase for MockDb {
        async fn resolve_materialized_ids(&self, hashes: &[BlockHash]) -> Result<Box<[CompactId]>, ResolveMaterializedIdsError> {
            let mut state = self.state.lock().expect("mock DB mutex");
            state.resolved_batches.push(hashes.to_vec());
            state.resolution_results.pop_front().expect("scripted materialized-ID resolution")
        }

        async fn apply_vspc_change(&self, change: ReadyVspcChange) -> Result<VspcCommitOutcome, ApplyVspcChangeError> {
            let result = {
                let mut state = self.state.lock().expect("mock DB mutex");
                state.applied.push(change.clone());
                state
                    .apply_results
                    .pop_front()
                    .unwrap_or_else(|| Ok(VspcCommitOutcome { destination: change.destination, level_snapshots: Arc::from([]) }))
            };
            let gate = self.apply_gate.lock().expect("mock DB gate mutex").take();
            if let Some((started, releases)) = gate {
                let _ = started.send(());
                releases.acquire().await.expect("mock DB release semaphore remains open").forget();
            }
            result
        }
    }

    struct MockRpc {
        results: Mutex<VecDeque<Result<kgi_model::block::ValidatedNodeBlock, NodeError>>>,
    }

    impl MockRpc {
        fn new() -> Arc<Self> {
            Arc::new(Self { results: Mutex::new(VecDeque::new()) })
        }

        fn push_result(&self, result: Result<kgi_model::block::ValidatedNodeBlock, NodeError>) {
            self.results.lock().expect("mock RPC mutex").push_back(result);
        }
    }

    impl VspcRpc for MockRpc {
        async fn full_block(&self, hash: BlockHash) -> Result<kgi_model::block::ValidatedNodeBlock, NodeError> {
            self.results.lock().expect("mock RPC mutex").pop_front().unwrap_or(Err(NodeError::BlockNotFound { hash }))
        }
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn indexed_hash(value: usize) -> BlockHash {
        let bytes = value.to_le_bytes();
        let mut hash = [0_u8; 32];
        hash[..bytes.len()].copy_from_slice(&bytes);
        BlockHash::from_bytes(hash)
    }

    fn id(value: i64) -> CompactId {
        CompactId::new(value).expect("positive compact ID")
    }

    fn point(byte: u8) -> VspcPoint {
        VspcPoint::new(ConsensusOrder::new(BlueWork::from_u64(u64::from(byte)), hash(byte)), id(i64::from(byte)))
    }

    fn anchor(byte: u8) -> MaterializedSyncAnchor {
        MaterializedSyncAnchor { point: point(byte), selected_parent: hash(byte.saturating_sub(1)), blue_score: u64::from(byte) }
    }

    fn persisted_block(byte: u8, selected_parent: u8) -> PersistedBlock {
        PersistedBlock { point: point(byte), selected_parent: hash(selected_parent) }
    }

    fn added(bytes: &[u8]) -> VspcChange {
        VspcChange { removed: Arc::from([]), added: bytes.iter().copied().map(hash).collect::<Vec<_>>().into() }
    }

    fn reorg(removed: &[u8], added: &[u8]) -> VspcChange {
        VspcChange {
            removed: removed.iter().copied().map(hash).collect::<Vec<_>>().into(),
            added: added.iter().copied().map(hash).collect::<Vec<_>>().into(),
        }
    }

    fn node_block(block_hash: BlockHash, selected_parent: BlockHash) -> kgi_model::block::ValidatedNodeBlock {
        kgi_model::block::ValidatedNodeBlock {
            hash: block_hash,
            selected_parent,
            direct_parents: vec![selected_parent],
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: 1,
            daa_score: 1,
            blue_score: 1,
            blue_work: BlueWork::from_u64(1),
        }
    }

    struct Fixture {
        processor: VspcProcessor<MockDb, MockRpc>,
        events: mpsc::UnboundedReceiver<VspcProcessorEvent>,
        persisted: mpsc::Sender<PersistedBlock>,
        graph: kgi_api_ingress::channel::GraphUpdateReceiver,
        db: Arc<MockDb>,
        rpc: Arc<MockRpc>,
    }

    fn fixture(anchor: MaterializedSyncAnchor) -> Fixture {
        let db = Arc::new(MockDb::default());
        let rpc = MockRpc::new();
        let (persisted_tx, persisted_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (graph_tx, mut graph_rx) = graph_update_channel();
        graph_tx.publish_post_seal().expect("open graph producer gate");
        assert_eq!(graph_rx.try_recv_update(), Ok(GraphUpdate::PublishPostSeal));
        let (mut processor, events) = VspcProcessor::new(RecoveryMode::Resync, rpc.clone(), db.clone(), graph_tx, persisted_rx);
        processor.initialize(anchor).expect("VspcProcessor initialization");
        processor.start().expect("VspcProcessor start");
        Fixture { processor, events, persisted: persisted_tx, graph: graph_rx, db, rpc }
    }

    #[tokio::test]
    async fn construction_and_initialization_are_inert_until_start() {
        let db = Arc::new(MockDb::default());
        let rpc = MockRpc::new();
        let weak_db: Weak<MockDb> = Arc::downgrade(&db);
        let weak_rpc: Weak<MockRpc> = Arc::downgrade(&rpc);
        let (_persisted_tx, persisted_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (graph, _graph_rx) = graph_update_channel();
        let (mut processor, mut events) = VspcProcessor::new(RecoveryMode::Resync, rpc.clone(), db.clone(), graph, persisted_rx);
        processor.initialize(anchor(1)).expect("initialization");
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());
        assert!(events.try_recv().is_err());
        drop(db);
        drop(rpc);
        processor.shutdown().await.expect("inert shutdown");
        assert!(weak_db.upgrade().is_none());
        assert!(weak_rpc.upgrade().is_none());
    }

    #[tokio::test]
    async fn consuming_shutdown_releases_exact_session_generations() {
        let db = Arc::new(MockDb::default());
        let rpc = MockRpc::new();
        let weak_db = Arc::downgrade(&db);
        let weak_rpc = Arc::downgrade(&rpc);
        let (_persisted_tx, persisted_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (graph, _graph_rx) = graph_update_channel();
        let (mut processor, _events) = VspcProcessor::new(RecoveryMode::Resync, rpc.clone(), db.clone(), graph, persisted_rx);
        processor.initialize(anchor(1)).expect("initialization");
        processor.start().expect("start");
        drop(db);
        drop(rpc);
        assert!(weak_db.upgrade().is_some());
        assert!(weak_rpc.upgrade().is_some());
        processor.shutdown().await.expect("shutdown");
        assert!(weak_db.upgrade().is_none());
        assert!(weak_rpc.upgrade().is_none());
    }

    #[tokio::test]
    async fn dropping_an_active_owner_reports_command_path_loss() {
        let Fixture { processor, mut events, .. } = fixture(anchor(1));

        drop(processor);

        assert!(matches!(
            events.recv().await,
            Some(VspcProcessorEvent::Fault(ComponentFault {
                source: Component::VspcProcessor,
                disposition: FaultDisposition::Fatal,
                kind: FaultKind::Ownership(OwnershipFault::InternalControlPathLost),
                ..
            }))
        ));
        assert!(events.recv().await.is_none());
    }

    #[tokio::test]
    async fn failed_start_before_initialization_remains_joinable() {
        let db = Arc::new(MockDb::default());
        let rpc = MockRpc::new();
        let (_persisted_tx, persisted_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (graph, _graph_rx) = graph_update_channel();
        let (mut processor, _events) = VspcProcessor::new(RecoveryMode::Resync, rpc, db, graph, persisted_rx);

        assert!(matches!(
            processor.start(),
            Err(VspcProcessorControlError::Rejected(ComponentFault {
                kind: FaultKind::Ownership(OwnershipFault::InvalidLifecycleControl),
                ..
            }))
        ));
        processor.shutdown().await.expect("failed startup remains joinable");
    }

    #[tokio::test]
    async fn event_before_block_becomes_ready_from_persisted_history() {
        let Fixture { processor, mut events, persisted, mut graph, db, .. } = fixture(anchor(1));
        processor.submit_synthetic(added(&[2])).await.expect("synthetic change");
        tokio::task::yield_now().await;
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());

        persisted.send(persisted_block(2, 1)).await.expect("persisted destination");
        assert!(matches!(
            graph.next_event().await,
            Some(GraphUpdateReceiveEvent::Update(GraphUpdate::VspcCommitted(VspcCommitted { source, destination, .. })))
                if source == hash(1) && destination == hash(2)
        ));
        let applied = db.state.lock().expect("mock DB mutex").applied.clone();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].added.as_ref(), &[id(2)]);
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn live_preempts_the_ready_synthetic_suffix_after_an_in_flight_commit() {
        let Fixture { processor, mut events, persisted, mut graph, db, .. } = fixture(anchor(1));
        processor.catchup(point(1)).expect("Catchup");

        // Preload block 3, then admit both synthetic candidates while block 2
        // is still missing. Persisting block 2 makes the complete suffix ready
        // in one worker transition.
        persisted.send(persisted_block(3, 2)).await.expect("later destination");
        processor.submit_synthetic(added(&[2])).await.expect("first synthetic change");
        for _ in 0..20 {
            if processor.metrics().pending_occupancy == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.metrics().pending_occupancy, 1);
        processor.submit_synthetic(added(&[3])).await.expect("second synthetic change");
        for _ in 0..20 {
            if processor.metrics().pending_occupancy == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.metrics().pending_occupancy, 2);

        let (mut apply_started, apply_releases) = db.gate_next_apply();
        persisted.send(persisted_block(2, 1)).await.expect("shared missing history");
        apply_started.recv().await.expect("first synthetic commit started");
        processor.live().expect("Live while first commit is in flight");
        apply_releases.add_permits(1);

        assert!(matches!(
            graph.next_event().await,
            Some(GraphUpdateReceiveEvent::Update(GraphUpdate::VspcCommitted(VspcCommitted { destination, .. })))
                if destination == hash(2)
        ));
        for _ in 0..20 {
            if processor.metrics().pending_occupancy == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.metrics().pending_occupancy, 0);
        assert_eq!(db.state.lock().expect("mock DB mutex").applied.len(), 1);
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn catchup_overlap_retains_notification_until_live() {
        let Fixture { processor, mut events, persisted, mut graph, db, .. } = fixture(anchor(1));
        persisted.send(persisted_block(2, 1)).await.expect("persisted destination");
        processor.catchup(point(1)).expect("Catchup");
        processor.notification_sender().send(added(&[2])).await.expect("notification");
        for _ in 0..20 {
            if processor.overlap().observed() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(processor.overlap().observed());
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());

        processor.live().expect("Live");
        assert!(matches!(
            graph.next_event().await,
            Some(GraphUpdateReceiveEvent::Update(GraphUpdate::VspcCommitted(VspcCommitted { destination, .. })))
                if destination == hash(2)
        ));
        assert_eq!(db.state.lock().expect("mock DB mutex").applied.len(), 1);
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn exact_replay_after_commit_is_an_obsolete_live_discard() {
        let Fixture { processor, mut events, persisted, mut graph, db, .. } = fixture(anchor(1));
        persisted.send(persisted_block(2, 1)).await.expect("persisted destination");
        processor.catchup(point(1)).expect("Catchup");
        let notifications = processor.notification_sender();
        notifications.send(added(&[2])).await.expect("notification");
        for _ in 0..20 {
            if processor.overlap().observed() {
                break;
            }
            tokio::task::yield_now().await;
        }
        processor.live().expect("Live");
        assert!(matches!(graph.next_event().await, Some(GraphUpdateReceiveEvent::Update(GraphUpdate::VspcCommitted(_)))));

        notifications.send(added(&[2])).await.expect("exact replay");
        for _ in 0..20 {
            if processor.metrics().pending_occupancy == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.metrics().pending_occupancy, 0);
        assert_eq!(db.state.lock().expect("mock DB mutex").applied.len(), 1);
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn unresolved_older_notification_does_not_block_a_later_actionable_one() {
        let Fixture { processor, mut events, persisted, mut graph, db, .. } = fixture(anchor(1));
        processor.catchup(point(1)).expect("Catchup");
        processor.live().expect("Live");
        let notifications = processor.notification_sender();
        notifications.send(added(&[9])).await.expect("unresolved notification");
        persisted.send(persisted_block(2, 1)).await.expect("later destination");
        notifications.send(added(&[2])).await.expect("actionable notification");

        assert!(matches!(
            graph.next_event().await,
            Some(GraphUpdateReceiveEvent::Update(GraphUpdate::VspcCommitted(VspcCommitted { destination, .. })))
                if destination == hash(2)
        ));
        assert_eq!(db.state.lock().expect("mock DB mutex").applied.len(), 1);
        assert_eq!(processor.metrics().pending_occupancy, 1);
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn notification_collision_precedes_endpoint_readiness() {
        let Fixture { processor, mut events, persisted: _persisted, graph: _graph, db, .. } = fixture(anchor(1));
        processor.catchup(point(1)).expect("Catchup");
        let notification = processor.notification_sender();
        notification.send(added(&[2])).await.expect("first notification");
        notification.send(added(&[2])).await.expect("duplicate notification");

        assert!(matches!(
            events.recv().await,
            Some(VspcProcessorEvent::Fault(ComponentFault {
                disposition: FaultDisposition::Require(RecoveryMode::Resync),
                kind: FaultKind::NotificationInputInvalid(NotificationInputKind::MalformedVspcChange(
                    MalformedVspcNotificationReason::DuplicatePendingTransition
                )),
                ..
            }))
        ));
        assert_eq!(processor.metrics().pending_occupancy, 1);
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn contradictory_destination_faults_before_endpoint_readiness() {
        let Fixture { processor, mut events, persisted: _persisted, graph: _graph, db, .. } = fixture(anchor(1));
        processor.catchup(point(1)).expect("Catchup");
        let notifications = processor.notification_sender();
        notifications.send(added(&[2])).await.expect("first notification");
        notifications.send(reorg(&[9], &[2])).await.expect("contradictory notification");

        assert!(matches!(
            events.recv().await,
            Some(VspcProcessorEvent::Fault(ComponentFault {
                kind: FaultKind::NotificationInputInvalid(NotificationInputKind::MalformedVspcChange(
                    MalformedVspcNotificationReason::ContradictoryDestination
                )),
                ..
            }))
        ));
        assert_eq!(processor.metrics().pending_occupancy, 1);
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn destination_below_catchup_lower_bound_is_discarded_without_overlap() {
        let Fixture { processor, mut events, persisted, graph: _graph, db, .. } = fixture(anchor(5));
        persisted.send(persisted_block(3, 2)).await.expect("old destination");
        processor.catchup(point(4)).expect("Catchup");
        processor.notification_sender().send(added(&[3])).await.expect("late notification");
        for _ in 0..20 {
            if processor.metrics().pending_occupancy == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.metrics().pending_occupancy, 0);
        assert!(!processor.overlap().observed());
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn crossing_notification_keeps_added_only_suffix_for_live() {
        let Fixture { processor, mut events, persisted, mut graph, db, .. } = fixture(anchor(3));
        persisted.send(persisted_block(4, 3)).await.expect("crossing child");
        persisted.send(persisted_block(5, 4)).await.expect("destination");
        processor.catchup(point(1)).expect("Catchup");
        processor.notification_sender().send(reorg(&[2], &[2, 3, 4, 5])).await.expect("crossing notification");
        for _ in 0..20 {
            if processor.overlap().observed() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(processor.overlap().observed());
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());
        processor.live().expect("Live");

        assert!(matches!(
            graph.next_event().await,
            Some(GraphUpdateReceiveEvent::Update(GraphUpdate::VspcCommitted(VspcCommitted { removed, added, .. })))
                if removed.is_empty() && added.as_ref() == [hash(4), hash(5)]
        ));
        let applied = db.state.lock().expect("mock DB mutex").applied.clone();
        assert_eq!(applied[0].source, point(3));
        assert_eq!(applied[0].added.as_ref(), &[id(4), id(5)]);
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn competing_live_notifications_fault_before_commit() {
        let Fixture { processor, mut events, persisted, graph: _graph, db, .. } = fixture(anchor(1));
        persisted.send(persisted_block(2, 1)).await.expect("first destination");
        persisted.send(persisted_block(3, 1)).await.expect("second destination");
        processor.catchup(point(1)).expect("Catchup");
        let notifications = processor.notification_sender();
        notifications.send(added(&[2])).await.expect("first notification");
        notifications.send(added(&[3])).await.expect("second notification");
        for _ in 0..20 {
            if processor.metrics().pending_occupancy == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        processor.live().expect("Live");

        assert!(matches!(
            events.recv().await,
            Some(VspcProcessorEvent::Fault(ComponentFault {
                kind: FaultKind::NotificationInputInvalid(NotificationInputKind::MalformedVspcChange(
                    MalformedVspcNotificationReason::CompetingNextMove
                )),
                ..
            }))
        ));
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn reorg_resolves_one_distinct_member_batch_and_preserves_positions() {
        let Fixture { processor, mut events, persisted, mut graph, db, .. } = fixture(anchor(3));
        persisted.send(persisted_block(5, 4)).await.expect("destination");
        db.push_resolution(Ok(vec![id(3), id(2), id(4), id(5)].into_boxed_slice()));
        processor.submit_synthetic(reorg(&[3, 2, 3], &[4, 5])).await.expect("synthetic reorg");

        assert!(matches!(
            graph.next_event().await,
            Some(GraphUpdateReceiveEvent::Update(GraphUpdate::VspcCommitted(VspcCommitted { destination, .. })))
                if destination == hash(5)
        ));
        {
            let state = db.state.lock().expect("mock DB mutex");
            assert_eq!(state.resolved_batches, [vec![hash(3), hash(2), hash(4), hash(5)]]);
            assert_eq!(state.applied[0].removed.as_ref(), &[id(3), id(2), id(3)]);
            assert_eq!(state.applied[0].added.as_ref(), &[id(4), id(5)]);
        }
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn nonmaterialized_direct_member_requires_rebuild_before_apply() {
        let Fixture { processor, mut events, persisted, graph: _graph, db, .. } = fixture(anchor(3));
        persisted.send(persisted_block(5, 4)).await.expect("destination");
        db.push_resolution(Err(ResolveMaterializedIdsError::NonMaterialized {
            missing: Arc::from([hash(4)]),
            identity_only: Arc::from([hash(2)]),
        }));
        processor.submit_synthetic(reorg(&[3, 2], &[4, 5])).await.expect("synthetic reorg");

        assert!(matches!(
            events.recv().await,
            Some(VspcProcessorEvent::Fault(ComponentFault {
                disposition: FaultDisposition::Require(RecoveryMode::Rebuild),
                kind: FaultKind::MaterialityViolation,
                ..
            }))
        ));
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn resolved_synthetic_source_mismatch_is_recovery_input_invalid() {
        let Fixture { processor, mut events, persisted, graph: _graph, db, .. } = fixture(anchor(1));
        persisted.send(persisted_block(2, 1)).await.expect("source history");
        persisted.send(persisted_block(3, 2)).await.expect("destination history");
        processor.submit_synthetic(added(&[3])).await.expect("discontinuous synthetic change");

        assert!(matches!(
            events.recv().await,
            Some(VspcProcessorEvent::Fault(ComponentFault {
                disposition: FaultDisposition::Retry,
                kind: FaultKind::RecoveryInputInvalid(RecoveryInputKind::MalformedVspcResponse(
                    MalformedVspcResponseReason::ResolvedSourceDiscontinuity
                )),
                ..
            }))
        ));
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn path_probe_attributes_current_parent_to_stored_state() {
        let Fixture { processor, mut events, persisted, graph: _graph, db, rpc } = fixture(anchor(1));
        persisted.send(persisted_block(2, 1)).await.expect("destination");
        db.push_apply(Err(ApplyVspcChangeError::VspcPathDiscontinuity(VspcPathConflict {
            child: hash(2),
            expected_parent: hash(1),
            stored_parent: hash(9),
        })));
        rpc.push_result(Ok(node_block(hash(2), hash(1))));
        processor.submit_synthetic(added(&[2])).await.expect("synthetic change");

        assert!(matches!(
            events.recv().await,
            Some(VspcProcessorEvent::Fault(ComponentFault {
                disposition: FaultDisposition::Require(RecoveryMode::Rebuild),
                kind: FaultKind::ReconciliationFailed,
                ..
            }))
        ));
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn pending_capacity_rejects_without_partial_admission() {
        let Fixture { processor, mut events, persisted: _persisted, graph: _graph, db, .. } = fixture(anchor(1));
        processor.catchup(point(1)).expect("Catchup");
        let notifications = processor.notification_sender();
        for value in 1..=MAX_PENDING_VSPC_CHANGES {
            notifications
                .send(VspcChange { removed: Arc::from([]), added: Arc::from([indexed_hash(value + 10_000)]) })
                .await
                .expect("pending notification");
        }
        for _ in 0..100 {
            if processor.metrics().pending_occupancy == MAX_PENDING_VSPC_CHANGES {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.metrics().pending_occupancy, MAX_PENDING_VSPC_CHANGES);
        notifications
            .send(VspcChange { removed: Arc::from([]), added: Arc::from([indexed_hash(20_000)]) })
            .await
            .expect("capacity-triggering notification");

        assert!(matches!(
            events.recv().await,
            Some(VspcProcessorEvent::Fault(ComponentFault {
                kind: FaultKind::BoundedStateExhausted(BoundedProcessingState::VspcPending),
                ..
            }))
        ));
        assert_eq!(processor.metrics().pending_occupancy, MAX_PENDING_VSPC_CHANGES);
        assert_eq!(processor.metrics().pending_high_water_mark, MAX_PENDING_VSPC_CHANGES);
        assert!(db.state.lock().expect("mock DB mutex").applied.is_empty());
        processor.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn graph_receiver_closure_does_not_roll_back_committed_sink() {
        let Fixture { processor, mut events, persisted, graph, db, .. } = fixture(anchor(1));
        drop(graph);
        persisted.send(persisted_block(2, 1)).await.expect("destination");
        processor.submit_synthetic(added(&[2])).await.expect("synthetic change");
        for _ in 0..20 {
            if db.state.lock().expect("mock DB mutex").applied.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(db.state.lock().expect("mock DB mutex").applied.len(), 1);
        assert!(events.try_recv().is_err());
        processor.shutdown().await.expect("shutdown");
    }

    #[test]
    fn attribution_policy_preserves_candidate_source_and_rebuild_strength() {
        let stored = attribution_fault(CandidateSource::Synthetic, VspcPathAttribution::StoredParentConflict);
        assert_eq!(stored.disposition, FaultDisposition::Require(RecoveryMode::Rebuild));
        assert_eq!(stored.kind, FaultKind::ReconciliationFailed);

        let candidate = attribution_fault(CandidateSource::Synthetic, VspcPathAttribution::CandidatePathConflict);
        assert_eq!(candidate.disposition, FaultDisposition::Retry);
        assert_eq!(
            candidate.kind,
            FaultKind::RecoveryInputInvalid(RecoveryInputKind::MalformedVspcResponse(
                MalformedVspcResponseReason::SelectedParentPathDiscontinuity
            ))
        );

        let both = attribution_fault(CandidateSource::Notification, VspcPathAttribution::StoredAndCandidateConflict);
        assert_eq!(both.disposition, FaultDisposition::Require(RecoveryMode::Rebuild));
        assert!(matches!(both.kind, FaultKind::NotificationInputInvalid(_)));
    }

    #[test]
    fn duplicate_persisted_hash_keeps_first_history_record() {
        let mut history = MaterializedHistory::new(anchor(1));
        assert!(history.insert(persisted_block(2, 1)));
        assert!(!history.insert(persisted_block(2, 9)));
        assert_eq!(history.by_hash[&hash(2)].selected_parent, hash(1));
        assert_eq!(history.by_order.len(), 2);
    }

    #[test]
    fn history_pruning_is_strictly_below_the_sink() {
        let mut history = MaterializedHistory::new(anchor(1));
        assert!(history.insert(persisted_block(2, 1)));
        assert!(history.insert(persisted_block(3, 2)));

        history.prune_below(point(2));

        assert!(!history.by_hash.contains_key(&hash(1)));
        assert!(history.by_hash.contains_key(&hash(2)));
        assert!(history.by_hash.contains_key(&hash(3)));
        assert_eq!(history.by_order.len(), 2);
    }
}
