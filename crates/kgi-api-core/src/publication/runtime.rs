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
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

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
    pub(crate) fn new<G: PublicationSeedDatabase>(graph_updates: GraphUpdateReceiver, api_db: Arc<ApiDbState<G>>) -> Self {
        let cancellation = CancellationToken::new();
        let shared = Arc::new(RuntimeSharedState::new());
        let worker = PublicationRuntimeWorker {
            graph_updates,
            api_db,
            staging: StagingBuffer::new(shared.clone()),
            candidate: None,
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

struct PublicationRuntimeWorker<G: PublicationSeedDatabase> {
    graph_updates: GraphUpdateReceiver,
    api_db: Arc<ApiDbState<G>>,
    staging: StagingBuffer,
    candidate: Option<ConstructionCandidate>,
    shared: Arc<RuntimeSharedState>,
}

impl<G: PublicationSeedDatabase> PublicationRuntimeWorker<G> {
    async fn run(mut self, cancellation: CancellationToken) -> Result<(), PublicationRuntimeError> {
        loop {
            let phase = self.shared.lock().phase;
            match phase {
                PublicationRuntimePhase::AwaitingCatchup | PublicationRuntimePhase::Aligning => {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => {
                            self.finish();
                            return Ok(());
                        }
                        observation = self.graph_updates.next_observation() => match observation {
                            Some(observation) => match self.handle_observation(observation)? {
                                WorkerAction::Continue => {}
                                WorkerAction::RestartConstruction => self.begin_reconstruction(),
                            },
                            None => return self.quiesce(&cancellation).await,
                        }
                    }
                }
                PublicationRuntimePhase::Constructing => match self.construct(&cancellation).await? {
                    ConstructionOutcome::Seeded => {
                        self.shared.set_phase(PublicationRuntimePhase::Aligning);
                    }
                    ConstructionOutcome::Restart => self.begin_reconstruction(),
                    ConstructionOutcome::Quiescent => return self.quiesce(&cancellation).await,
                    ConstructionOutcome::Cancelled => {
                        self.finish();
                        return Ok(());
                    }
                },
                PublicationRuntimePhase::Quiescent => return self.quiesce(&cancellation).await,
                PublicationRuntimePhase::Stopped => unreachable!("a running publication worker is not stopped"),
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
                    Some(observation) => match self.handle_observation(observation)? {
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
                        self.candidate = Some(ConstructionCandidate::from_seed(seed, origin));
                        Ok(ConstructionOutcome::Seeded)
                    }
                    Err(ApiReadError::QueryFailed | ApiReadError::GenerationLost | ApiReadError::InconsistentProjection) => {
                        self.restart_staging();
                        Ok(ConstructionOutcome::Restart)
                    }
                },
                observation = self.graph_updates.next_observation() => match observation {
                    Some(observation) => match self.handle_observation(observation)? {
                        WorkerAction::Continue => {}
                        WorkerAction::RestartConstruction => return Ok(ConstructionOutcome::Restart),
                    },
                    None => return Ok(ConstructionOutcome::Quiescent),
                }
            }
        }
    }

    fn handle_observation(&mut self, observation: GraphUpdateObservation) -> Result<WorkerAction, PublicationRuntimeError> {
        let phase = self.shared.lock().phase;
        match (phase, observation) {
            (PublicationRuntimePhase::AwaitingCatchup, GraphUpdateObservation::Update(GraphUpdate::Catchup)) => {
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
                self.restart_staging();
                Ok(WorkerAction::RestartConstruction)
            }
            (
                PublicationRuntimePhase::Constructing | PublicationRuntimePhase::Aligning,
                GraphUpdateObservation::Update(GraphUpdate::Catchup),
            ) => Err(PublicationRuntimeError::InvalidMarkerOrder),
            (PublicationRuntimePhase::Quiescent | PublicationRuntimePhase::Stopped, _) => {
                unreachable!("a quiescent or stopped publication runtime does not consume graph updates")
            }
        }
    }

    fn stage(&mut self, update: CommittedGraphUpdate) -> WorkerAction {
        if self.staging.push(update).is_err() {
            self.restart_staging();
            WorkerAction::RestartConstruction
        } else {
            WorkerAction::Continue
        }
    }

    fn begin_reconstruction(&mut self) {
        self.candidate = None;
        self.shared.set_phase(PublicationRuntimePhase::Constructing);
    }

    fn restart_staging(&mut self) {
        self.staging.clear();
        self.shared.record_reconstruction();
    }

    fn finish(&mut self) {
        self.candidate = None;
        self.staging.clear();
        self.shared.set_phase(PublicationRuntimePhase::Stopped);
    }

    async fn quiesce(mut self, cancellation: &CancellationToken) -> Result<(), PublicationRuntimeError> {
        self.shared.set_phase(PublicationRuntimePhase::Quiescent);
        cancellation.cancelled().await;
        self.finish();
        Ok(())
    }
}

enum ConstructionOutcome {
    Seeded,
    Restart,
    Quiescent,
    Cancelled,
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
        }
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

    fn set_target_live(&self) {
        self.lock().target_live = true;
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

    use kgi_api_ingress::channel::{GraphUpdateOfferOutcome, graph_update_channel};
    use kgi_api_model::{
        graph::Level,
        projection::{GraphViewSeed, GraphWindowResolution},
    };
    use kgi_model::{
        block::{BlockHash, CompactId},
        graph_update::{GraphUpdate, VspcCommitted},
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

    struct MockSeedDb {
        valid: AtomicBool,
        calls: AtomicUsize,
        results: Mutex<VecDeque<Result<GraphViewSeed, ApiReadError>>>,
        started: Arc<Semaphore>,
        release: Option<Arc<Semaphore>>,
    }

    impl MockSeedDb {
        fn immediate(result: Result<GraphViewSeed, ApiReadError>) -> Arc<Self> {
            Arc::new(Self {
                valid: AtomicBool::new(true),
                calls: AtomicUsize::new(0),
                results: Mutex::new(VecDeque::from([result])),
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
                results: Mutex::new(VecDeque::from([result])),
                started: started.clone(),
                release: Some(release.clone()),
            });
            (database, started, release)
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
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
            self.started.add_permits(1);
            if let Some(release) = &self.release {
                release.acquire().await.expect("seed gate remains open").forget();
            }
            self.results.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).pop_front().expect("mock seed result")
        }
    }

    fn empty_api_db() -> Arc<ApiDbState<MockSeedDb>> {
        Arc::new(ApiDbState::new())
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

    #[tokio::test]
    async fn catchup_starts_staging_and_live_is_sticky_without_consuming_capacity() {
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, empty_api_db());

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
    async fn receiver_closure_is_quiescent_until_explicit_shutdown() {
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, empty_api_db());
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
    async fn invalid_first_marker_is_a_typed_worker_failure() {
        let (_producer, receiver) = graph_update_channel();
        let shared = Arc::new(RuntimeSharedState::new());
        let mut worker = PublicationRuntimeWorker {
            graph_updates: receiver,
            api_db: empty_api_db(),
            staging: StagingBuffer::new(shared.clone()),
            candidate: None,
            shared,
        };

        assert_eq!(
            worker.handle_observation(GraphUpdateObservation::Update(GraphUpdate::Live)),
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
        let runtime = PublicationRuntime::new(receiver, api_db.clone());

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

        let aligned = wait_for(&runtime, |snapshot| {
            snapshot.phase == PublicationRuntimePhase::Aligning && snapshot.target_live && snapshot.metrics.staging_occupancy == 1
        })
        .await;
        assert_eq!(aligned.metrics.staging_high_water_mark, 1);
        assert_eq!(first.calls(), 1);
        assert_eq!(second.calls(), 0, "an in-flight attempt never switches generations");

        assert_eq!(runtime.shutdown().await, Ok(()));
    }

    #[tokio::test]
    async fn construction_waits_for_a_database_generation_without_stopping_stream_staging() {
        let api_db = Arc::new(ApiDbState::new());
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver, api_db.clone());

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
    }
}
