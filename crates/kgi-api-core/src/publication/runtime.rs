use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use kgi_api_ingress::channel::{GraphUpdateObservation, GraphUpdateReceiver};
use kgi_api_model::{
    graph::{GraphView, LevelExtent, TrackingPolicy},
    history::GraphHistory,
    projection::{GraphViewSeed, GraphViewSeedOutcome, GraphViewSeedRequest},
};
use kgi_model::{
    block::{BlockHash, CompactId},
    graph_update::{BlockCommitted, GraphUpdate, VspcCommitted},
};
use kgi_storage::{error::ApiReadError, generation::ValidatedApiDbClient};
use thiserror::Error;
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc::error::TryRecvError, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::{AppliedLineageUpdate, GraphPublication, GraphPublicationState, OriginalUpdate, revision_timestamp_ms_at};
use crate::api_db::{ApiDbGeneration, ApiDbState};

/// Maximum number of ordinary committed updates retained during construction.
pub(crate) const API_STAGING_UPDATE_CAPACITY: usize = 4096;

/// Point-in-time operational measurements for one publication runtime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PublicationRuntimeMetricsSnapshot {
    /// Number of ordinary committed updates currently staged.
    pub staging_occupancy: usize,
    /// Greatest staging occupancy observed during this runtime.
    pub staging_high_water_mark: usize,
    /// Number of updates rejected at the staging-capacity boundary.
    pub staging_overflow_count: u64,
    /// Number of intra-session reconstruction cuts observed by this runtime.
    pub reconstruction_count: u64,
}

/// Terminal failure of the private publication-runtime worker.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum PublicationRuntimeError {
    /// The session stream violated its lifecycle-marker order.
    #[error("graph-update stream violated publication marker order")]
    InvalidMarkerOrder,
    /// The runtime worker panicked or its Tokio task was otherwise lost.
    #[error("publication-runtime worker could not be joined")]
    WorkerJoinFailed,
}

/// One session-bound graph-publication worker and its completed shutdown barrier.
pub(crate) struct PublicationRuntime {
    cancellation: CancellationToken,
    completion: PublicationRuntimeCompletion,
    shared: Arc<RuntimeSharedState>,
}

impl PublicationRuntime {
    /// Starts the single worker for one fresh processing-session ingress.
    pub(crate) fn new<G, I>(graph_updates: GraphUpdateReceiver, api_db: Arc<ApiDbState<G>>, installer: I) -> Self
    where
        G: PublicationSeedDatabase,
        I: PublicationInstaller,
    {
        let cancellation = CancellationToken::new();
        let shared = Arc::new(RuntimeSharedState::new());
        let worker = PublicationRuntimeWorker {
            graph_updates,
            api_db,
            installer,
            staging: StagingBuffer::new(shared.clone()),
            work: PublicationWork::None,
            construction_gap_generation: 0,
            shared: shared.clone(),
        };
        let join = tokio::spawn(worker.run(cancellation.clone()));
        Self { cancellation, completion: PublicationRuntimeCompletion::new(join), shared }
    }

    /// Cancels and joins the runtime worker, returning its retained result.
    pub(crate) async fn shutdown(&self) -> Result<(), PublicationRuntimeError> {
        self.cancellation.cancel();
        self.completion.wait().await
    }

    /// Returns this runtime's current operational measurements.
    #[must_use]
    pub(crate) fn metrics(&self) -> PublicationRuntimeMetricsSnapshot {
        self.shared.lock().metrics
    }

    #[cfg(test)]
    fn snapshot(&self) -> RuntimeSnapshot {
        *self.shared.lock()
    }
}

impl Drop for PublicationRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// Database capability required by one publication construction attempt.
pub(crate) trait PublicationSeedDatabase: ApiDbGeneration {
    /// Loads the complete detached Head seed from this exact generation.
    fn load_head_seed(&self) -> impl Future<Output = Result<GraphViewSeed, ApiReadError>> + Send;
}

impl PublicationSeedDatabase for ValidatedApiDbClient {
    async fn load_head_seed(&self) -> Result<GraphViewSeed, ApiReadError> {
        let outcome = self.load_graph_view_seed(GraphViewSeedRequest::HeadPublication).await?;
        let GraphViewSeedOutcome::Loaded(seed) = outcome else { unreachable!("HeadPublication has no anchor-unavailable outcome") };
        Ok(seed)
    }
}

/// Result of submitting an aligned publication to the owning ApiService boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationInstallOutcome {
    /// The publication became the current publication for this runtime generation.
    Installed,
    /// A newer runtime generation already owns installation authority.
    Superseded,
}

/// Synchronous generation-fenced publication-installation capability.
pub(crate) trait PublicationInstaller: Send + Sync + 'static {
    /// Attempts to install one complete aligned publication.
    fn install(&self, publication: Arc<GraphPublication>) -> PublicationInstallOutcome;
}

#[derive(Clone)]
struct PublicationRuntimeCompletion {
    inner: Arc<PublicationRuntimeCompletionInner>,
}

struct PublicationRuntimeCompletionInner {
    join: AsyncMutex<Option<JoinHandle<Result<(), PublicationRuntimeError>>>>,
    result: watch::Sender<Option<Result<(), PublicationRuntimeError>>>,
}

impl PublicationRuntimeCompletion {
    fn new(join: JoinHandle<Result<(), PublicationRuntimeError>>) -> Self {
        let (result, _) = watch::channel(None);
        Self { inner: Arc::new(PublicationRuntimeCompletionInner { join: AsyncMutex::new(Some(join)), result }) }
    }

    async fn wait(&self) -> Result<(), PublicationRuntimeError> {
        if let Some(result) = *self.inner.result.borrow() {
            return result;
        }

        let mut join = self.inner.join.lock().await;
        if let Some(worker) = join.as_mut() {
            let result = worker.await.unwrap_or(Err(PublicationRuntimeError::WorkerJoinFailed));
            *join = None;
            self.inner.result.send_replace(Some(result));
            return result;
        }

        self.inner.result.borrow().as_ref().copied().expect("a consumed publication-runtime join stores its result")
    }
}

struct PublicationRuntimeWorker<G: PublicationSeedDatabase, I: PublicationInstaller> {
    graph_updates: GraphUpdateReceiver,
    api_db: Arc<ApiDbState<G>>,
    installer: I,
    staging: StagingBuffer,
    work: PublicationWork,
    construction_gap_generation: u64,
    shared: Arc<RuntimeSharedState>,
}

impl<G: PublicationSeedDatabase, I: PublicationInstaller> PublicationRuntimeWorker<G, I> {
    async fn run(mut self, cancellation: CancellationToken) -> Result<(), PublicationRuntimeError> {
        loop {
            let phase = self.shared.lock().phase;
            let action = match phase {
                PublicationRuntimePhase::AwaitingCatchup | PublicationRuntimePhase::Active => {
                    self.await_observation(&cancellation).await?
                }
                PublicationRuntimePhase::Constructing => match self.construct(&cancellation).await? {
                    ConstructionOutcome::Seeded => {
                        self.shared.set_phase(PublicationRuntimePhase::Aligning);
                        LoopAction::Continue
                    }
                    ConstructionOutcome::Restart => LoopAction::RestartConstruction,
                    ConstructionOutcome::Quiescent => LoopAction::Quiescent,
                    ConstructionOutcome::Cancelled => LoopAction::Cancelled,
                },
                PublicationRuntimePhase::Aligning => match self.advance_alignment() {
                    AlignmentOutcome::Waiting => self.await_observation(&cancellation).await?,
                    AlignmentOutcome::Installed => {
                        self.shared.set_phase(PublicationRuntimePhase::Active);
                        LoopAction::Continue
                    }
                    AlignmentOutcome::Superseded => LoopAction::Quiescent,
                    AlignmentOutcome::Restart => LoopAction::RestartConstruction,
                },
                PublicationRuntimePhase::Quiescent => LoopAction::Quiescent,
                PublicationRuntimePhase::Stopped => unreachable!("a running publication worker is not stopped"),
            };

            let action = if action == LoopAction::RestartConstruction { self.restart_construction()? } else { action };

            match action {
                LoopAction::Continue => {}
                LoopAction::RestartConstruction => unreachable!("reconstruction resolves to a runnable or quiescent state"),
                LoopAction::Quiescent => return self.quiesce(&cancellation).await,
                LoopAction::Cancelled => {
                    self.finish().await;
                    return Ok(());
                }
            }
        }
    }

    async fn await_observation(&mut self, cancellation: &CancellationToken) -> Result<LoopAction, PublicationRuntimeError> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(LoopAction::Cancelled),
            observation = self.graph_updates.next_observation() => match observation {
                Some(observation) => match self.handle_observation(observation).await? {
                    WorkerAction::Continue => Ok(LoopAction::Continue),
                    WorkerAction::RestartConstruction => Ok(LoopAction::RestartConstruction),
                },
                None => Ok(LoopAction::Quiescent),
            }
        }
    }

    async fn construct(&mut self, cancellation: &CancellationToken) -> Result<ConstructionOutcome, PublicationRuntimeError> {
        let origin = Instant::now();
        let api_db = self.api_db.clone();
        let current = api_db.current();
        tokio::pin!(current);
        let client = loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(ConstructionOutcome::Cancelled),
                client = &mut current => match client {
                    Some(client) => break client,
                    None => return Ok(ConstructionOutcome::Quiescent),
                },
                observation = self.graph_updates.next_observation() => match observation {
                    Some(observation) => match self.handle_observation(observation).await? {
                        WorkerAction::Continue => {}
                        WorkerAction::RestartConstruction => return Ok(ConstructionOutcome::Restart),
                    },
                    None => return Ok(ConstructionOutcome::Quiescent),
                }
            }
        };

        let seed = client.load_head_seed();
        tokio::pin!(seed);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(ConstructionOutcome::Cancelled),
                result = &mut seed => return match result {
                    Ok(seed) => {
                        self.work = PublicationWork::Aligning(Box::new(ConstructionCandidate::from_seed(seed, origin)));
                        Ok(ConstructionOutcome::Seeded)
                    }
                    Err(ApiReadError::QueryFailed | ApiReadError::GenerationLost | ApiReadError::InconsistentProjection) =>
                        Ok(ConstructionOutcome::Restart),
                },
                observation = self.graph_updates.next_observation() => match observation {
                    Some(observation) => match self.handle_observation(observation).await? {
                        WorkerAction::Continue => {}
                        WorkerAction::RestartConstruction => return Ok(ConstructionOutcome::Restart),
                    },
                    None => return Ok(ConstructionOutcome::Quiescent),
                }
            }
        }
    }

    async fn handle_observation(&mut self, observation: GraphUpdateObservation) -> Result<WorkerAction, PublicationRuntimeError> {
        let phase = self.shared.lock().phase;
        match (phase, observation) {
            (PublicationRuntimePhase::AwaitingCatchup, GraphUpdateObservation::Update(GraphUpdate::Catchup)) => {
                self.construction_gap_generation = self.graph_updates.gap_generation();
                self.shared.set_phase(PublicationRuntimePhase::Constructing);
                Ok(WorkerAction::Continue)
            }
            (PublicationRuntimePhase::AwaitingCatchup, _) => Err(PublicationRuntimeError::InvalidMarkerOrder),
            (
                PublicationRuntimePhase::Constructing | PublicationRuntimePhase::Aligning,
                GraphUpdateObservation::Update(GraphUpdate::BlockCommitted(update)),
            ) => Ok(self.stage(CommittedGraphUpdate::Block(update))),
            (
                PublicationRuntimePhase::Constructing | PublicationRuntimePhase::Aligning,
                GraphUpdateObservation::Update(GraphUpdate::VspcCommitted(update)),
            ) => Ok(self.stage(CommittedGraphUpdate::Vspc(update))),
            (
                PublicationRuntimePhase::Constructing | PublicationRuntimePhase::Aligning,
                GraphUpdateObservation::Update(GraphUpdate::Live),
            ) => {
                self.shared.set_target_live();
                Ok(WorkerAction::Continue)
            }
            (PublicationRuntimePhase::Constructing | PublicationRuntimePhase::Aligning, GraphUpdateObservation::Gap(_)) => {
                Ok(WorkerAction::RestartConstruction)
            }
            (
                PublicationRuntimePhase::Constructing | PublicationRuntimePhase::Aligning,
                GraphUpdateObservation::Update(GraphUpdate::Catchup),
            ) => Err(PublicationRuntimeError::InvalidMarkerOrder),
            (PublicationRuntimePhase::Active, GraphUpdateObservation::Update(GraphUpdate::BlockCommitted(update))) => {
                Ok(self.apply_active_update(CommittedGraphUpdate::Block(update)).await)
            }
            (PublicationRuntimePhase::Active, GraphUpdateObservation::Update(GraphUpdate::VspcCommitted(update))) => {
                Ok(self.apply_active_update(CommittedGraphUpdate::Vspc(update)).await)
            }
            (PublicationRuntimePhase::Active, GraphUpdateObservation::Update(GraphUpdate::Live)) => {
                if self.shared.set_target_live() {
                    self.active_publication().transition_live().await;
                }
                Ok(WorkerAction::Continue)
            }
            (PublicationRuntimePhase::Active, GraphUpdateObservation::Gap(_)) => {
                self.retire_active().await;
                Ok(WorkerAction::RestartConstruction)
            }
            (PublicationRuntimePhase::Active, GraphUpdateObservation::Update(GraphUpdate::Catchup)) => {
                Err(PublicationRuntimeError::InvalidMarkerOrder)
            }
            (PublicationRuntimePhase::Quiescent | PublicationRuntimePhase::Stopped, _) => {
                unreachable!("a quiescent or stopped publication runtime does not consume graph updates")
            }
        }
    }

    fn stage(&mut self, update: CommittedGraphUpdate) -> WorkerAction {
        if self.staging.push(update).is_err() { WorkerAction::RestartConstruction } else { WorkerAction::Continue }
    }

    fn advance_alignment(&mut self) -> AlignmentOutcome {
        let frontier = self.staging.len();
        for _ in 0..frontier {
            let update = self.staging.pop_front().expect("captured staging frontier remains available");
            let result = self.alignment_candidate_mut().apply_alignment_update(&update);
            if result.is_err() {
                return AlignmentOutcome::Restart;
            }
        }

        if !self.alignment_candidate().is_aligned() {
            return AlignmentOutcome::Waiting;
        }
        if self.graph_updates.gap_generation() != self.construction_gap_generation {
            return AlignmentOutcome::Restart;
        }

        let initial_state = self.shared.target_publication_state();
        let PublicationWork::Aligning(candidate) = std::mem::replace(&mut self.work, PublicationWork::None) else {
            unreachable!("Aligning owns one construction candidate")
        };
        match (*candidate).into_publication(initial_state) {
            Ok(publication) => {
                let publication = Arc::new(publication);
                match self.installer.install(publication.clone()) {
                    PublicationInstallOutcome::Installed => {
                        self.work = PublicationWork::Active(publication);
                        AlignmentOutcome::Installed
                    }
                    PublicationInstallOutcome::Superseded => AlignmentOutcome::Superseded,
                }
            }
            Err(_) => AlignmentOutcome::Restart,
        }
    }

    async fn apply_active_update(&mut self, update: CommittedGraphUpdate) -> WorkerAction {
        let publication = self.active_publication().clone();
        let result = match &update {
            CommittedGraphUpdate::Block(committed) => publication.apply_block_committed(committed).await,
            CommittedGraphUpdate::Vspc(committed) => publication.apply_vspc_committed(committed).await,
        };
        if result.is_err() {
            self.retire_active().await;
            WorkerAction::RestartConstruction
        } else {
            WorkerAction::Continue
        }
    }

    fn alignment_candidate(&self) -> &ConstructionCandidate {
        let PublicationWork::Aligning(candidate) = &self.work else { unreachable!("Aligning owns one construction candidate") };
        candidate
    }

    fn alignment_candidate_mut(&mut self) -> &mut ConstructionCandidate {
        let PublicationWork::Aligning(candidate) = &mut self.work else { unreachable!("Aligning owns one construction candidate") };
        candidate
    }

    fn active_publication(&self) -> &Arc<GraphPublication> {
        let PublicationWork::Active(publication) = &self.work else { unreachable!("Active owns one installed publication") };
        publication
    }

    async fn retire_active(&mut self) {
        if let PublicationWork::Active(publication) = &self.work {
            publication.transition_stale().await;
        }
        self.work = PublicationWork::None;
    }

    fn restart_construction(&mut self) -> Result<LoopAction, PublicationRuntimeError> {
        self.work = PublicationWork::None;
        self.staging.clear();
        self.shared.record_reconstruction();

        loop {
            match self.graph_updates.try_recv_update() {
                Ok(GraphUpdate::BlockCommitted(_) | GraphUpdate::VspcCommitted(_)) => {}
                Ok(GraphUpdate::Live) => {
                    self.shared.set_target_live();
                }
                Ok(GraphUpdate::Catchup) => return Err(PublicationRuntimeError::InvalidMarkerOrder),
                Err(TryRecvError::Empty) => {
                    self.construction_gap_generation = self.graph_updates.gap_generation();
                    self.shared.set_phase(PublicationRuntimePhase::Constructing);
                    return Ok(LoopAction::Continue);
                }
                Err(TryRecvError::Disconnected) => return Ok(LoopAction::Quiescent),
            }
        }
    }

    async fn finish(&mut self) {
        self.retire_active().await;
        self.staging.clear();
        self.shared.set_phase(PublicationRuntimePhase::Stopped);
    }

    async fn quiesce(mut self, cancellation: &CancellationToken) -> Result<(), PublicationRuntimeError> {
        self.shared.set_phase(PublicationRuntimePhase::Quiescent);
        cancellation.cancelled().await;
        self.finish().await;
        Ok(())
    }
}

enum PublicationWork {
    None,
    Aligning(Box<ConstructionCandidate>),
    Active(Arc<GraphPublication>),
}

enum ConstructionOutcome {
    Seeded,
    Restart,
    Quiescent,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoopAction {
    Continue,
    RestartConstruction,
    Quiescent,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AlignmentOutcome {
    Waiting,
    Installed,
    Superseded,
    Restart,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerAction {
    Continue,
    RestartConstruction,
}

struct ConstructionCandidate {
    revision_clock_origin: Instant,
    view: GraphView,
    history: GraphHistory,
    snapshot_max_materialized_id: CompactId,
    snapshot_vspc_sink: BlockHash,
    block_cut_crossed: bool,
    vspc_cut_crossed: bool,
}

impl ConstructionCandidate {
    fn from_seed(seed: GraphViewSeed, revision_clock_origin: Instant) -> Self {
        let extent = LevelExtent::new(seed.resolution.effective_start_level, seed.resolution.effective_end_level)
            .expect("a storage-certified Head projection has a valid extent");
        let view =
            GraphView::from_database_projection(seed.max_depth, extent, TrackingPolicy::Head, seed.levels, seed.blocks, seed.edges);
        Self {
            revision_clock_origin,
            view,
            history: GraphHistory::new(),
            snapshot_max_materialized_id: seed.snapshot_max_materialized_id,
            snapshot_vspc_sink: seed.snapshot_vspc_sink,
            block_cut_crossed: false,
            vspc_cut_crossed: false,
        }
    }

    fn apply_alignment_update(&mut self, update: &CommittedGraphUpdate) -> Result<(), super::PublicationProjectionError> {
        match update {
            CommittedGraphUpdate::Block(committed) if !self.block_cut_crossed => {
                if committed.id <= self.snapshot_max_materialized_id {
                    return Ok(());
                }
                self.apply_committed_update(update)?;
                self.block_cut_crossed = true;
            }
            CommittedGraphUpdate::Vspc(committed) if !self.vspc_cut_crossed => {
                if self.snapshot_vspc_sink != committed.source {
                    return Ok(());
                }
                self.apply_committed_update(update)?;
                self.vspc_cut_crossed = true;
            }
            CommittedGraphUpdate::Block(_) | CommittedGraphUpdate::Vspc(_) => self.apply_committed_update(update)?,
        }
        Ok(())
    }

    fn apply_committed_update(&mut self, update: &CommittedGraphUpdate) -> Result<(), super::PublicationProjectionError> {
        let original = match update {
            CommittedGraphUpdate::Block(committed) => OriginalUpdate::Block(committed),
            CommittedGraphUpdate::Vspc(committed) => OriginalUpdate::Vspc(committed),
        };
        let timestamp = revision_timestamp_ms_at(self.revision_clock_origin, Instant::now());
        AppliedLineageUpdate::apply(&mut self.view, &mut self.history, original, timestamp, || {})?;
        Ok(())
    }

    fn is_aligned(&self) -> bool {
        self.block_cut_crossed && self.vspc_cut_crossed
    }

    fn into_publication(self, initial_state: GraphPublicationState) -> Result<GraphPublication, super::PublicationProjectionError> {
        GraphPublication::new(initial_state, self.revision_clock_origin, self.view, self.history)
    }
}

enum CommittedGraphUpdate {
    Block(BlockCommitted),
    Vspc(VspcCommitted),
}

struct StagingBuffer {
    entries: VecDeque<CommittedGraphUpdate>,
    shared: Arc<RuntimeSharedState>,
}

impl StagingBuffer {
    fn new(shared: Arc<RuntimeSharedState>) -> Self {
        Self { entries: VecDeque::with_capacity(API_STAGING_UPDATE_CAPACITY), shared }
    }

    fn push(&mut self, update: CommittedGraphUpdate) -> Result<(), StagingOverflow> {
        if self.entries.len() == API_STAGING_UPDATE_CAPACITY {
            self.shared.record_staging_overflow();
            return Err(StagingOverflow);
        }
        self.entries.push_back(update);
        self.shared.record_staging_occupancy(self.entries.len());
        Ok(())
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn pop_front(&mut self) -> Option<CommittedGraphUpdate> {
        let update = self.entries.pop_front()?;
        self.shared.record_staging_occupancy(self.entries.len());
        Some(update)
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.shared.record_staging_occupancy(0);
    }
}

impl Drop for StagingBuffer {
    fn drop(&mut self) {
        self.shared.record_staging_occupancy(0);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StagingOverflow;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationRuntimePhase {
    AwaitingCatchup,
    Constructing,
    Aligning,
    Active,
    Quiescent,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeSnapshot {
    phase: PublicationRuntimePhase,
    target_live: bool,
    metrics: PublicationRuntimeMetricsSnapshot,
}

struct RuntimeSharedState {
    inner: Mutex<RuntimeSnapshot>,
}

impl RuntimeSharedState {
    fn new() -> Self {
        Self {
            inner: Mutex::new(RuntimeSnapshot {
                phase: PublicationRuntimePhase::AwaitingCatchup,
                target_live: false,
                metrics: PublicationRuntimeMetricsSnapshot::default(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, RuntimeSnapshot> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn set_phase(&self, phase: PublicationRuntimePhase) {
        self.lock().phase = phase;
    }

    fn set_target_live(&self) -> bool {
        let mut state = self.lock();
        let changed = !state.target_live;
        state.target_live = true;
        changed
    }

    fn target_publication_state(&self) -> GraphPublicationState {
        if self.lock().target_live { GraphPublicationState::Live } else { GraphPublicationState::Synchronizing }
    }

    fn record_staging_occupancy(&self, occupancy: usize) {
        let mut state = self.lock();
        state.metrics.staging_occupancy = occupancy;
        state.metrics.staging_high_water_mark = state.metrics.staging_high_water_mark.max(occupancy);
    }

    fn record_staging_overflow(&self) {
        let mut state = self.lock();
        state.metrics.staging_overflow_count =
            state.metrics.staging_overflow_count.checked_add(1).expect("publication-runtime staging-overflow count fits u64");
    }

    fn record_reconstruction(&self) {
        let mut state = self.lock();
        state.metrics.reconstruction_count =
            state.metrics.reconstruction_count.checked_add(1).expect("publication-runtime reconstruction count fits u64");
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap, VecDeque},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use kgi_api_ingress::channel::{
        GRAPH_UPDATE_CHANNEL_CAPACITY, GraphUpdateOfferOutcome, GraphUpdateProducer, GraphUpdateProducerError, graph_update_channel,
    };
    use kgi_api_model::{
        graph::Level,
        projection::{GraphViewSeed, GraphWindowResolution},
    };
    use kgi_model::{
        block::{BlockColor, BlockCoordinate, BlockHash, CompactId},
        graph_update::{BlockCommitted, GraphUpdate, LevelCommitted, ParentCommitted, VspcCommitted},
    };
    use tokio::sync::Semaphore;

    use super::*;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn vspc_update(byte: u8) -> VspcCommitted {
        VspcCommitted {
            source: hash(byte),
            destination: hash(byte.wrapping_add(1)),
            removed: Arc::from([]),
            added: Arc::from([hash(byte.wrapping_add(1))]),
            level_snapshots: Arc::from([]),
        }
    }

    fn seed(byte: u8) -> GraphViewSeed {
        GraphViewSeed {
            resolution: GraphWindowResolution { resolved_level: 1, effective_start_level: 1, effective_end_level: 1 },
            max_depth: 1,
            levels: BTreeMap::from([(1, Level { size: 0, daa_score: None })]),
            blocks: HashMap::new(),
            edges: HashMap::new(),
            snapshot_max_materialized_id: CompactId::new(i64::from(byte.max(1))).expect("positive compact ID"),
            snapshot_vspc_sink: hash(byte),
        }
    }

    fn alignment_seed() -> GraphViewSeed {
        GraphViewSeed {
            resolution: GraphWindowResolution { resolved_level: 10, effective_start_level: 8, effective_end_level: 10 },
            max_depth: 3,
            levels: BTreeMap::from([
                (8, Level { size: 0, daa_score: Some(8) }),
                (9, Level { size: 0, daa_score: Some(9) }),
                (10, Level { size: 0, daa_score: Some(10) }),
            ]),
            blocks: HashMap::new(),
            edges: HashMap::new(),
            snapshot_max_materialized_id: CompactId::new(5).expect("positive compact ID"),
            snapshot_vspc_sink: hash(90),
        }
    }

    fn block_update(id: u8, level: u32, parent_level: u32) -> BlockCommitted {
        BlockCommitted {
            id: CompactId::new(i64::from(id)).expect("positive compact ID"),
            hash: hash(id),
            coordinate: BlockCoordinate::new(level, 0).expect("positive block coordinate"),
            timestamp: u64::from(id),
            daa_score: u64::from(id),
            selected_parent_index: Some(0),
            direct_parents: Arc::from([ParentCommitted {
                hash: hash(id.wrapping_add(100)),
                coordinate: Some(BlockCoordinate::new(parent_level, 0).expect("positive parent coordinate")),
            }]),
            blue_merge_set: Arc::from([]),
            red_merge_set: Arc::from([]),
            color: BlockColor::Gray,
            is_in_vspc: false,
            level_snapshots: Arc::from([
                LevelCommitted { level: parent_level, size: 1, daa_score: Some(u64::from(parent_level)) },
                LevelCommitted { level, size: 1, daa_score: Some(u64::from(level)) },
            ]),
        }
    }

    fn vspc_update_at(source: u8, daa_score: u64) -> VspcCommitted {
        VspcCommitted {
            source: hash(source),
            destination: hash(source.wrapping_add(1)),
            removed: Arc::from([]),
            added: Arc::from([]),
            level_snapshots: Arc::from([LevelCommitted { level: 10, size: 0, daa_score: Some(daa_score) }]),
        }
    }

    struct MockSeedDb {
        valid: AtomicBool,
        calls: AtomicUsize,
        active_attempts: AtomicUsize,
        results: Mutex<VecDeque<Result<GraphViewSeed, ApiReadError>>>,
        started: Arc<Semaphore>,
        release: Option<Arc<Semaphore>>,
    }

    impl MockSeedDb {
        fn immediate(result: Result<GraphViewSeed, ApiReadError>) -> Arc<Self> {
            Self::immediate_sequence([result])
        }

        fn immediate_sequence(results: impl IntoIterator<Item = Result<GraphViewSeed, ApiReadError>>) -> Arc<Self> {
            Arc::new(Self {
                valid: AtomicBool::new(true),
                calls: AtomicUsize::new(0),
                active_attempts: AtomicUsize::new(0),
                results: Mutex::new(results.into_iter().collect()),
                started: Arc::new(Semaphore::new(0)),
                release: None,
            })
        }

        fn gated(result: Result<GraphViewSeed, ApiReadError>) -> (Arc<Self>, Arc<Semaphore>, Arc<Semaphore>) {
            let started = Arc::new(Semaphore::new(0));
            let release = Arc::new(Semaphore::new(0));
            let database = Arc::new(Self {
                valid: AtomicBool::new(true),
                calls: AtomicUsize::new(0),
                active_attempts: AtomicUsize::new(0),
                results: Mutex::new(VecDeque::from([result])),
                started: started.clone(),
                release: Some(release.clone()),
            });
            (database, started, release)
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn active_attempts(&self) -> usize {
            self.active_attempts.load(Ordering::SeqCst)
        }
    }

    struct SeedAttemptGuard<'a>(&'a AtomicUsize);

    impl Drop for SeedAttemptGuard<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl ApiDbGeneration for MockSeedDb {
        fn is_valid(&self) -> bool {
            self.valid.load(Ordering::SeqCst)
        }
    }

    impl PublicationSeedDatabase for MockSeedDb {
        async fn load_head_seed(&self) -> Result<GraphViewSeed, ApiReadError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.active_attempts.fetch_add(1, Ordering::SeqCst);
            let _attempt = SeedAttemptGuard(&self.active_attempts);
            self.started.add_permits(1);
            if let Some(release) = &self.release {
                release.acquire().await.expect("seed gate remains open").forget();
            }
            let result = self.results.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).pop_front().expect("mock seed result");
            if result == Err(ApiReadError::GenerationLost) {
                self.valid.store(false, Ordering::SeqCst);
            }
            result
        }
    }

    #[derive(Clone)]
    struct MockInstaller {
        outcome: PublicationInstallOutcome,
        installed: Arc<Mutex<Vec<Arc<GraphPublication>>>>,
    }

    impl MockInstaller {
        fn installed() -> Self {
            Self { outcome: PublicationInstallOutcome::Installed, installed: Arc::new(Mutex::new(Vec::new())) }
        }

        fn superseded() -> Self {
            Self { outcome: PublicationInstallOutcome::Superseded, installed: Arc::new(Mutex::new(Vec::new())) }
        }

        fn latest(&self) -> Option<Arc<GraphPublication>> {
            self.installed.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).last().cloned()
        }
    }

    impl PublicationInstaller for MockInstaller {
        fn install(&self, publication: Arc<GraphPublication>) -> PublicationInstallOutcome {
            if self.outcome == PublicationInstallOutcome::Installed {
                self.installed.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(publication);
            }
            self.outcome
        }
    }

    fn empty_api_db() -> Arc<ApiDbState<MockSeedDb>> {
        Arc::new(ApiDbState::new())
    }

    fn aligning_worker() -> PublicationRuntimeWorker<MockSeedDb, MockInstaller> {
        aligning_worker_with_producer().0
    }

    fn aligning_worker_with_producer() -> (PublicationRuntimeWorker<MockSeedDb, MockInstaller>, GraphUpdateProducer) {
        let (producer, graph_updates) = graph_update_channel();
        let construction_gap_generation = graph_updates.gap_generation();
        let shared = Arc::new(RuntimeSharedState::new());
        shared.set_phase(PublicationRuntimePhase::Aligning);
        (
            PublicationRuntimeWorker {
                graph_updates,
                api_db: empty_api_db(),
                installer: MockInstaller::installed(),
                staging: StagingBuffer::new(shared.clone()),
                work: PublicationWork::Aligning(Box::new(ConstructionCandidate::from_seed(alignment_seed(), Instant::now()))),
                construction_gap_generation,
                shared,
            },
            producer,
        )
    }

    async fn wait_for(runtime: &PublicationRuntime, predicate: impl Fn(RuntimeSnapshot) -> bool) -> RuntimeSnapshot {
        for _ in 0..100 {
            let snapshot = runtime.snapshot();
            if predicate(snapshot) {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
        panic!("publication runtime did not reach the expected state")
    }

    async fn start_active_runtime() -> (PublicationRuntime, GraphUpdateProducer, Arc<GraphPublication>) {
        let database = MockSeedDb::immediate(Ok(alignment_seed()));
        let api_db = Arc::new(ApiDbState::new());
        api_db.update(crate::api_db::ApiDbGenerationEvent::Published(database)).expect("publish database generation");
        let installer = MockInstaller::installed();
        let retained_installer = installer.clone();
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, api_db, installer);

        producer.publish_catchup().expect("runtime receiver is open");
        assert_eq!(producer.offer_block_committed(block_update(6, 11, 10)), Ok(GraphUpdateOfferOutcome::Enqueued));
        assert_eq!(producer.offer_vspc_committed(vspc_update_at(90, 90)), Ok(GraphUpdateOfferOutcome::Enqueued));
        wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Active).await;
        let publication = retained_installer.latest().expect("active publication was installed");
        (runtime, producer, publication)
    }

    #[tokio::test]
    async fn catchup_starts_staging_and_live_is_sticky_without_consuming_capacity() {
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, empty_api_db(), MockInstaller::installed());

        assert_eq!(producer.offer_vspc_committed(vspc_update(1)), Ok(GraphUpdateOfferOutcome::SuppressedAwaitingCatchup));
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::AwaitingCatchup);
        assert_eq!(runtime.metrics().staging_occupancy, 0);

        producer.publish_catchup().expect("runtime receiver is open");
        wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Constructing).await;
        assert_eq!(producer.offer_vspc_committed(vspc_update(2)), Ok(GraphUpdateOfferOutcome::Enqueued));
        let staged = wait_for(&runtime, |snapshot| snapshot.metrics.staging_occupancy == 1).await;
        assert_eq!(staged.metrics.staging_high_water_mark, 1);

        producer.publish_live().await.expect("runtime receiver is open");
        let live = wait_for(&runtime, |snapshot| snapshot.target_live).await;
        assert_eq!(live.metrics.staging_occupancy, 1);

        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
        assert_eq!(runtime.metrics().staging_occupancy, 0);
    }

    #[tokio::test]
    async fn shutdown_from_awaiting_catchup_joins_the_worker_and_releases_the_receiver() {
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, empty_api_db(), MockInstaller::installed());

        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::AwaitingCatchup);
        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
        assert_eq!(producer.offer_vspc_committed(vspc_update(1)), Err(GraphUpdateProducerError::ReceiverClosed));
        assert_eq!(producer.metrics().receiver_closure_count, 1);
    }

    #[tokio::test]
    async fn shutdown_while_waiting_for_a_database_generation_completes_construction() {
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, empty_api_db(), MockInstaller::installed());
        producer.publish_catchup().expect("runtime receiver is open");
        wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Constructing).await;

        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
        assert_eq!(producer.offer_vspc_committed(vspc_update(2)), Err(GraphUpdateProducerError::ReceiverClosed));
    }

    #[tokio::test]
    async fn shutdown_drops_an_in_progress_seed_attempt_without_a_child_task() {
        let (database, started, _release) = MockSeedDb::gated(Ok(seed(3)));
        let api_db = Arc::new(ApiDbState::new());
        api_db.update(crate::api_db::ApiDbGenerationEvent::Published(database.clone())).expect("publish database generation");
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, api_db, MockInstaller::installed());
        producer.publish_catchup().expect("runtime receiver is open");
        started.acquire().await.expect("seed attempt starts").forget();
        assert_eq!(database.active_attempts(), 1);

        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(database.active_attempts(), 0, "the joined worker dropped its inline seed future");
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
    }

    #[tokio::test]
    async fn shutdown_abandons_an_unpublished_alignment_candidate() {
        let database = MockSeedDb::immediate(Ok(alignment_seed()));
        let api_db = Arc::new(ApiDbState::new());
        api_db.update(crate::api_db::ApiDbGenerationEvent::Published(database)).expect("publish database generation");
        let installer = MockInstaller::installed();
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, api_db, installer.clone());
        producer.publish_catchup().expect("runtime receiver is open");
        wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Aligning).await;

        assert_eq!(runtime.shutdown().await, Ok(()));
        assert!(installer.latest().is_none());
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
    }

    #[tokio::test]
    async fn receiver_closure_is_quiescent_until_explicit_shutdown() {
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, empty_api_db(), MockInstaller::installed());
        producer.publish_catchup().expect("runtime receiver is open");
        assert_eq!(producer.offer_vspc_committed(vspc_update(3)), Ok(GraphUpdateOfferOutcome::Enqueued));
        wait_for(&runtime, |snapshot| snapshot.metrics.staging_occupancy == 1).await;

        drop(producer);
        let quiescent = wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Quiescent).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(runtime.snapshot(), quiescent);

        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
        assert_eq!(runtime.metrics().staging_occupancy, 0);
    }

    #[tokio::test]
    async fn active_receiver_closure_preserves_the_publication_until_shutdown_marks_it_stale() {
        let (runtime, producer, publication) = start_active_runtime().await;
        let state_before_closure = publication.capture_delta_target().await.state();

        drop(producer);
        wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Quiescent).await;
        assert_eq!(publication.capture_delta_target().await.state(), state_before_closure);

        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(publication.capture_delta_target().await.state(), GraphPublicationState::Stale);
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
    }

    #[tokio::test]
    async fn shutdown_from_active_marks_stale_before_releasing_the_receiver() {
        let (runtime, producer, publication) = start_active_runtime().await;
        assert_ne!(publication.capture_delta_target().await.state(), GraphPublicationState::Stale);

        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(publication.capture_delta_target().await.state(), GraphPublicationState::Stale);
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
        assert_eq!(producer.offer_vspc_committed(vspc_update(4)), Err(GraphUpdateProducerError::ReceiverClosed));
        assert_eq!(runtime.shutdown().await, Ok(()), "repeated shutdown observes the retained completion result");
    }

    #[tokio::test]
    async fn superseded_installation_becomes_quiescent_until_shutdown() {
        let database = MockSeedDb::immediate(Ok(alignment_seed()));
        let api_db = Arc::new(ApiDbState::new());
        api_db.update(crate::api_db::ApiDbGenerationEvent::Published(database)).expect("publish database generation");
        let installer = MockInstaller::superseded();
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, api_db, installer.clone());
        producer.publish_catchup().expect("runtime receiver is open");
        assert_eq!(producer.offer_block_committed(block_update(6, 11, 10)), Ok(GraphUpdateOfferOutcome::Enqueued));
        assert_eq!(producer.offer_vspc_committed(vspc_update_at(90, 90)), Ok(GraphUpdateOfferOutcome::Enqueued));

        wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Quiescent).await;
        assert!(installer.latest().is_none());
        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
    }

    #[test]
    fn staging_capacity_is_exact_and_overflow_preserves_sticky_measurements() {
        let shared = Arc::new(RuntimeSharedState::new());
        let mut staging = StagingBuffer::new(shared.clone());
        for byte in 0..API_STAGING_UPDATE_CAPACITY {
            staging.push(CommittedGraphUpdate::Vspc(vspc_update(byte as u8))).expect("entry is within staging capacity");
        }
        assert_eq!(staging.push(CommittedGraphUpdate::Vspc(vspc_update(0))), Err(StagingOverflow));
        staging.clear();

        assert_eq!(
            shared.lock().metrics,
            PublicationRuntimeMetricsSnapshot {
                staging_occupancy: 0,
                staging_high_water_mark: API_STAGING_UPDATE_CAPACITY,
                staging_overflow_count: 1,
                reconstruction_count: 0,
            }
        );
    }

    #[test]
    fn staging_overflow_restarts_without_retaining_a_partial_prefix() {
        let (mut worker, _producer) = aligning_worker_with_producer();
        for byte in 0..API_STAGING_UPDATE_CAPACITY {
            assert_eq!(worker.stage(CommittedGraphUpdate::Vspc(vspc_update(byte as u8))), WorkerAction::Continue);
        }

        assert_eq!(worker.stage(CommittedGraphUpdate::Vspc(vspc_update(0))), WorkerAction::RestartConstruction);
        assert_eq!(worker.staging.len(), API_STAGING_UPDATE_CAPACITY, "the overflowing update is not partially admitted");
        assert_eq!(worker.restart_construction(), Ok(LoopAction::Continue));
        assert_eq!(worker.staging.len(), 0, "reconstruction discards the complete staged prefix");
        assert_eq!(
            worker.shared.lock().metrics,
            PublicationRuntimeMetricsSnapshot {
                staging_occupancy: 0,
                staging_high_water_mark: API_STAGING_UPDATE_CAPACITY,
                staging_overflow_count: 1,
                reconstruction_count: 1,
            }
        );
    }

    #[tokio::test]
    async fn completion_join_ownership_survives_a_cancelled_waiter() {
        let release = Arc::new(Semaphore::new(0));
        let worker_release = release.clone();
        let join = tokio::spawn(async move {
            worker_release.acquire().await.expect("test semaphore remains open").forget();
            Ok(())
        });
        let completion = PublicationRuntimeCompletion::new(join);

        let first_completion = completion.clone();
        let first = tokio::spawn(async move { first_completion.wait().await });
        tokio::task::yield_now().await;
        first.abort();
        assert!(first.await.expect_err("first waiter is cancelled").is_cancelled());

        let second_completion = completion.clone();
        let second = tokio::spawn(async move { second_completion.wait().await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        release.add_permits(1);
        assert_eq!(second.await.expect("second waiter joins"), Ok(()));
        assert_eq!(completion.wait().await, Ok(()));
    }

    #[tokio::test]
    async fn unexpected_worker_loss_is_mapped_and_retained_for_every_waiter() {
        let join = tokio::spawn(async {
            panic!("simulated publication-runtime worker loss");
            #[allow(unreachable_code)]
            Ok(())
        });
        let completion = PublicationRuntimeCompletion::new(join);

        assert_eq!(completion.wait().await, Err(PublicationRuntimeError::WorkerJoinFailed));
        assert_eq!(completion.wait().await, Err(PublicationRuntimeError::WorkerJoinFailed));
    }

    #[tokio::test]
    async fn invalid_first_marker_is_a_typed_worker_failure() {
        let (_producer, receiver) = graph_update_channel();
        let shared = Arc::new(RuntimeSharedState::new());
        let mut worker = PublicationRuntimeWorker {
            graph_updates: receiver,
            api_db: empty_api_db(),
            installer: MockInstaller::installed(),
            staging: StagingBuffer::new(shared.clone()),
            work: PublicationWork::None,
            construction_gap_generation: 0,
            shared,
        };

        assert_eq!(
            worker.handle_observation(GraphUpdateObservation::Update(GraphUpdate::Live)).await,
            Err(PublicationRuntimeError::InvalidMarkerOrder)
        );
    }

    #[tokio::test]
    async fn catchup_loads_one_exact_generation_while_updates_continue_staging() {
        let (first, started, release) = MockSeedDb::gated(Ok(seed(7)));
        let second = MockSeedDb::immediate(Ok(seed(8)));
        let api_db = Arc::new(ApiDbState::new());
        api_db.update(crate::api_db::ApiDbGenerationEvent::Published(first.clone())).expect("publish first generation");
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, api_db.clone(), MockInstaller::installed());

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(first.calls(), 0, "construction does not start before Catchup");

        producer.publish_catchup().expect("runtime receiver is open");
        started.acquire().await.expect("seed attempt starts").forget();
        assert_eq!(producer.offer_vspc_committed(vspc_update(9)), Ok(GraphUpdateOfferOutcome::Enqueued));
        producer.publish_live().await.expect("runtime receiver is open");
        api_db.update(crate::api_db::ApiDbGenerationEvent::Published(second.clone())).expect("publish replacement generation");
        release.add_permits(1);

        let aligned = wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Aligning && snapshot.target_live).await;
        assert_eq!(aligned.metrics.staging_occupancy, 0, "alignment consumes the staged update after seeding");
        assert_eq!(aligned.metrics.staging_high_water_mark, 1);
        assert_eq!(first.calls(), 1);
        assert_eq!(second.calls(), 0, "an in-flight attempt never switches generations");

        assert_eq!(runtime.shutdown().await, Ok(()));
    }

    #[tokio::test]
    async fn construction_waits_for_a_database_generation_without_stopping_stream_staging() {
        let api_db = Arc::new(ApiDbState::new());
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, api_db.clone(), MockInstaller::installed());

        producer.publish_catchup().expect("runtime receiver is open");
        assert_eq!(producer.offer_vspc_committed(vspc_update(12)), Ok(GraphUpdateOfferOutcome::Enqueued));
        producer.publish_live().await.expect("runtime receiver is open");
        let waiting = wait_for(&runtime, |snapshot| {
            snapshot.phase == PublicationRuntimePhase::Constructing && snapshot.target_live && snapshot.metrics.staging_occupancy == 1
        })
        .await;
        assert_eq!(waiting.metrics.staging_high_water_mark, 1);

        let database = MockSeedDb::immediate(Ok(seed(13)));
        api_db.update(crate::api_db::ApiDbGenerationEvent::Published(database.clone())).expect("publish database generation");
        wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Aligning).await;
        assert_eq!(database.calls(), 1);

        assert_eq!(runtime.shutdown().await, Ok(()));
    }

    #[tokio::test]
    async fn retained_generation_seed_failures_reconstruct_through_the_same_current_client() {
        for failure in [ApiReadError::QueryFailed, ApiReadError::InconsistentProjection] {
            let database = MockSeedDb::immediate_sequence([Err(failure), Ok(seed(14))]);
            let api_db = Arc::new(ApiDbState::new());
            api_db.update(crate::api_db::ApiDbGenerationEvent::Published(database.clone())).expect("publish database generation");
            let (producer, receiver) = graph_update_channel();
            let runtime = PublicationRuntime::new(receiver, api_db, MockInstaller::installed());

            producer.publish_catchup().expect("runtime receiver is open");
            let reconstructed = wait_for(&runtime, |snapshot| {
                snapshot.phase == PublicationRuntimePhase::Aligning && snapshot.metrics.reconstruction_count == 1
            })
            .await;

            assert_eq!(reconstructed.metrics.staging_occupancy, 0);
            assert_eq!(database.calls(), 2, "the same current generation serves the newer seed attempt");
            assert_eq!(runtime.shutdown().await, Ok(()));
        }
    }

    #[tokio::test]
    async fn generation_lost_reconstructs_and_waits_for_the_forwarded_replacement() {
        let lost = MockSeedDb::immediate(Err(ApiReadError::GenerationLost));
        let replacement = MockSeedDb::immediate(Ok(seed(15)));
        let api_db = Arc::new(ApiDbState::new());
        api_db.update(crate::api_db::ApiDbGenerationEvent::Published(lost.clone())).expect("publish database generation");
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, api_db.clone(), MockInstaller::installed());

        producer.publish_catchup().expect("runtime receiver is open");
        wait_for(&runtime, |snapshot| {
            snapshot.phase == PublicationRuntimePhase::Constructing && snapshot.metrics.reconstruction_count == 1
        })
        .await;
        assert_eq!(lost.calls(), 1);
        assert_eq!(replacement.calls(), 0);

        api_db.update(crate::api_db::ApiDbGenerationEvent::Retired(lost)).expect("retire lost database generation");
        api_db
            .update(crate::api_db::ApiDbGenerationEvent::Published(replacement.clone()))
            .expect("publish replacement database generation");
        wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Aligning).await;
        assert_eq!(replacement.calls(), 1);

        assert_eq!(runtime.shutdown().await, Ok(()));
    }

    #[test]
    fn loaded_head_seed_becomes_a_revision_zero_candidate_with_its_alignment_cuts() {
        let origin = Instant::now();
        let candidate = ConstructionCandidate::from_seed(seed(11), origin);

        assert_eq!(candidate.revision_clock_origin, origin);
        assert_eq!(candidate.view.tracking_policy(), TrackingPolicy::Head);
        assert_eq!(candidate.view.current_revision_id(), 0);
        assert_eq!(candidate.view.current_revision_timestamp_ms(), 0);
        assert_eq!(candidate.history.current_revision_id(), 0);
        assert_eq!(candidate.snapshot_max_materialized_id, CompactId::new(11).expect("positive compact ID"));
        assert_eq!(candidate.snapshot_vspc_sink, hash(11));
        assert!(!candidate.block_cut_crossed);
        assert!(!candidate.vspc_cut_crossed);
    }

    #[tokio::test]
    async fn block_first_alignment_crosses_on_a_no_effect_commit_and_replays_the_captured_frontier() {
        let mut worker = aligning_worker();
        worker.staging.push(CommittedGraphUpdate::Block(block_update(5, 7, 6))).expect("covered block fits");
        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update_at(89, 89))).expect("covered VSPC fits");
        worker.staging.push(CommittedGraphUpdate::Block(block_update(6, 7, 6))).expect("crossing block fits");
        worker.staging.push(CommittedGraphUpdate::Block(block_update(7, 11, 10))).expect("post-cut block fits");
        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update_at(90, 90))).expect("crossing VSPC fits");
        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update_at(91, 91))).expect("post-cut VSPC fits");

        assert_eq!(worker.advance_alignment(), AlignmentOutcome::Installed);
        assert_eq!(worker.staging.len(), 0);
        let publication = worker.active_publication();
        let target = publication.capture_delta_target().await;
        let view = publication.capture_view().await.into_view();

        assert_eq!(target.state(), GraphPublicationState::Synchronizing);
        assert_eq!(target.revision(), 3, "no-effect crossing and covered values create no revisions");
        assert!(view.block(&hash(5)).is_none(), "the snapshot-covered block is skipped");
        assert!(view.block(&hash(6)).is_none(), "the below-window crossing block has no retained effect");
        assert!(view.block(&hash(7)).is_some(), "later block updates apply after the block cut crosses");
        assert_eq!(view.level(10), Some(&Level { size: 1, daa_score: Some(91) }));
    }

    #[tokio::test]
    async fn vspc_first_alignment_keeps_applying_vspc_and_waits_indefinitely_for_the_block_cut() {
        let mut worker = aligning_worker();
        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update_at(90, 90))).expect("crossing VSPC fits");
        assert_eq!(worker.advance_alignment(), AlignmentOutcome::Waiting);
        assert_eq!(worker.alignment_candidate().view.current_revision_id(), 1);
        assert_eq!(worker.advance_alignment(), AlignmentOutcome::Waiting, "an idle source has no synthetic fence or timeout");

        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update_at(91, 91))).expect("post-cut VSPC fits");
        worker.staging.push(CommittedGraphUpdate::Block(block_update(5, 7, 6))).expect("covered block fits");
        assert_eq!(worker.advance_alignment(), AlignmentOutcome::Waiting);
        assert_eq!(worker.alignment_candidate().view.current_revision_id(), 2);

        worker.shared.set_target_live();
        worker.staging.push(CommittedGraphUpdate::Block(block_update(6, 11, 10))).expect("crossing block fits");
        assert_eq!(worker.advance_alignment(), AlignmentOutcome::Installed);
        let target = worker.active_publication().capture_delta_target().await;
        assert_eq!(target.state(), GraphPublicationState::Live);
        assert_eq!(target.revision(), 3);
    }

    #[tokio::test]
    async fn live_after_installation_changes_only_the_active_lifecycle_state() {
        let mut worker = aligning_worker();
        worker.staging.push(CommittedGraphUpdate::Block(block_update(6, 11, 10))).expect("crossing block fits");
        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update_at(90, 90))).expect("crossing VSPC fits");
        assert_eq!(worker.advance_alignment(), AlignmentOutcome::Installed);
        worker.shared.set_phase(PublicationRuntimePhase::Active);
        let publication = worker.active_publication().clone();
        let before = publication.capture_delta_target().await;

        assert_eq!(worker.handle_observation(GraphUpdateObservation::Update(GraphUpdate::Live)).await, Ok(WorkerAction::Continue));
        let after = publication.capture_delta_target().await;
        assert_eq!(before.state(), GraphPublicationState::Synchronizing);
        assert_eq!(after.state(), GraphPublicationState::Live);
        assert_eq!(after.revision(), before.revision());
        assert_eq!(after.revision_timestamp_ms(), before.revision_timestamp_ms());

        assert_eq!(
            worker.handle_observation(GraphUpdateObservation::Update(GraphUpdate::VspcCommitted(vspc_update_at(91, 91)))).await,
            Ok(WorkerAction::Continue)
        );
        let advanced = publication.capture_delta_target().await;
        assert_eq!(advanced.state(), GraphPublicationState::Live);
        assert_eq!(advanced.revision(), after.revision() + 1);
        assert_eq!(worker.staging.len(), 0, "Active applies new arrivals without a hidden backlog");
    }

    #[test]
    fn superseded_installation_abandons_the_aligned_publication() {
        let mut worker = aligning_worker();
        worker.installer = MockInstaller::superseded();
        worker.staging.push(CommittedGraphUpdate::Block(block_update(6, 11, 10))).expect("crossing block fits");
        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update_at(90, 90))).expect("crossing VSPC fits");

        assert_eq!(worker.advance_alignment(), AlignmentOutcome::Superseded);
        assert!(matches!(worker.work, PublicationWork::None));
        assert!(worker.installer.latest().is_none());
    }

    #[test]
    fn changed_gap_generation_prevents_installation_at_the_alignment_boundary() {
        let (mut worker, producer) = aligning_worker_with_producer();
        producer.publish_catchup().expect("alignment test receiver is open");
        assert_eq!(worker.graph_updates.try_recv_update(), Ok(GraphUpdate::Catchup));
        for byte in 0..GRAPH_UPDATE_CHANNEL_CAPACITY {
            assert_eq!(
                producer.offer_vspc_committed(vspc_update(byte as u8)),
                Ok(GraphUpdateOfferOutcome::Enqueued),
                "the bounded ingress fills before reporting a gap"
            );
        }
        assert_eq!(producer.offer_vspc_committed(vspc_update(0)), Ok(GraphUpdateOfferOutcome::GapReported));
        worker.staging.push(CommittedGraphUpdate::Block(block_update(6, 11, 10))).expect("crossing block fits");
        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update_at(90, 90))).expect("crossing VSPC fits");

        assert_eq!(worker.advance_alignment(), AlignmentOutcome::Restart);
        assert!(matches!(worker.work, PublicationWork::Aligning(_)), "the run loop still owns abandonment");
        assert!(worker.installer.latest().is_none());
        assert_eq!(worker.restart_construction(), Ok(LoopAction::Continue));
        assert!(matches!(worker.work, PublicationWork::None));
        assert_eq!(worker.shared.lock().phase, PublicationRuntimePhase::Constructing);
        assert_eq!(worker.construction_gap_generation, 1);
    }

    #[tokio::test]
    async fn reconstruction_drains_to_empty_discards_staged_work_and_preserves_live() {
        let (mut worker, producer) = aligning_worker_with_producer();
        producer.publish_catchup().expect("alignment test receiver is open");
        assert_eq!(worker.graph_updates.try_recv_update(), Ok(GraphUpdate::Catchup));
        assert_eq!(producer.offer_vspc_committed(vspc_update(1)), Ok(GraphUpdateOfferOutcome::Enqueued));
        producer.publish_live().await.expect("alignment test receiver is open");
        assert_eq!(producer.offer_vspc_committed(vspc_update(2)), Ok(GraphUpdateOfferOutcome::Enqueued));
        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update(3))).expect("staged value fits");

        assert_eq!(worker.restart_construction(), Ok(LoopAction::Continue));
        assert!(matches!(worker.work, PublicationWork::None));
        assert_eq!(worker.staging.len(), 0);
        assert!(worker.shared.lock().target_live);
        assert_eq!(worker.shared.lock().phase, PublicationRuntimePhase::Constructing);
        assert_eq!(worker.shared.lock().metrics.reconstruction_count, 1);
        assert_eq!(producer.metrics().current_occupancy, 0);

        assert_eq!(producer.offer_vspc_committed(vspc_update(4)), Ok(GraphUpdateOfferOutcome::Enqueued));
        assert_eq!(producer.metrics().current_occupancy, 1, "work after the empty frontier remains for the new attempt");
    }

    #[tokio::test]
    async fn active_gap_retires_the_installed_publication_before_reconstruction() {
        let (mut worker, _producer) = aligning_worker_with_producer();
        worker.staging.push(CommittedGraphUpdate::Block(block_update(6, 11, 10))).expect("crossing block fits");
        worker.staging.push(CommittedGraphUpdate::Vspc(vspc_update_at(90, 90))).expect("crossing VSPC fits");
        assert_eq!(worker.advance_alignment(), AlignmentOutcome::Installed);
        worker.shared.set_phase(PublicationRuntimePhase::Active);
        let publication = worker.active_publication().clone();

        assert_eq!(worker.handle_observation(GraphUpdateObservation::Gap(1)).await, Ok(WorkerAction::RestartConstruction));
        assert_eq!(publication.capture_delta_target().await.state(), GraphPublicationState::Stale);
        assert!(matches!(worker.work, PublicationWork::None));
        assert_eq!(worker.restart_construction(), Ok(LoopAction::Continue));
        assert_eq!(worker.shared.lock().phase, PublicationRuntimePhase::Constructing);
    }
}
