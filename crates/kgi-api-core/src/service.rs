use std::{
    fmt::Display,
    sync::{Arc, Mutex},
};

use kgi_api_ingress::channel::GraphUpdateReceiver;
use kgi_api_model::{
    projection::MAX_WINDOW_DEPTH,
    protocol::{CACHED_HEAD_WINDOW_DEPTHS, FreshViewReason, HeadLevelLookupOutcome, SystemStatus},
};
use kgi_model::lifecycle::{NodeServiceStatus, ProcessingStatus, RecoveryMode, StorageServiceStatus, SupervisorStatus};
use kgi_storage::generation::ValidatedApiDbClient;
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    api_db::{ApiDbGeneration, ApiDbGenerationUpdate, ApiDbState},
    cache::{CacheJobTracker, CacheWaitError, CachedHeadSnapshot},
    codec::{HeadEtag, graph::HeadLevelLookupResponseDto},
    encoding::{
        EncodedGraphBody,
        scheduler::{EncodingScheduler, EncodingSchedulerError},
    },
    error::ApiServiceError,
    publication::{
        GraphPublication, HeadTierSelection, ReplayFloorAdmission,
        runtime::{PublicationInstallOutcome, PublicationInstaller, PublicationRuntime},
    },
};

/// Parsed conditional state for one logical Head snapshot operation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[allow(dead_code, reason = "HTTP conditional parsing is introduced by the transport increment")]
pub(crate) enum HeadSnapshotCondition {
    /// The caller supplied no current validator.
    #[default]
    Unconditional,
    /// Any current representation satisfies the caller's condition.
    Any,
    /// One of these opaque validators must match the current representation.
    Tags(Arc<[HeadEtag]>),
}

impl HeadSnapshotCondition {
    fn matches(&self, etag: &HeadEtag) -> bool {
        match self {
            Self::Unconditional => false,
            Self::Any => true,
            Self::Tags(tags) => tags.iter().any(|candidate| candidate == etag),
        }
    }
}

/// Logical result of one in-memory Head snapshot operation.
#[allow(dead_code, reason = "HTTP response delivery is introduced by the transport increment")]
pub(crate) enum HeadSnapshotOutcome {
    /// No coherent publication is currently addressable.
    Unavailable,
    /// The selected immutable gzip response must be delivered.
    Complete {
        /// Exact publication retained for response finalization and delivery.
        publication: Arc<GraphPublication>,
        /// Exact eligible cached representation selected by the operation.
        snapshot: Arc<CachedHeadSnapshot>,
    },
    /// The selected representation matches the caller's conditional state.
    NotModified {
        /// Exact publication retained for response finalization.
        publication: Arc<GraphPublication>,
        /// Exact admitted weak validator matched by the caller's condition.
        etag: HeadEtag,
    },
}

/// Logical result of one retained Head-level lookup operation.
#[allow(dead_code, reason = "HTTP response delivery is introduced by the transport increment")]
pub(crate) enum HeadLevelLookupResponse {
    /// Every requested level was captured and encoded for delivery.
    Complete {
        /// Exact publication retained for response finalization and delivery.
        publication: Arc<GraphPublication>,
        /// Uncached, size-checked gzip representation owned by this request.
        body: EncodedGraphBody,
    },
    /// One requested level was unavailable in the captured Head image.
    LevelUnavailable {
        /// Exact publication retained for response finalization.
        publication: Arc<GraphPublication>,
        /// Unavailable level that prevents an all-or-nothing response.
        requested_level: u32,
    },
    /// The supplied publication identity no longer addresses the current publication.
    FreshViewRequired {
        /// Current coherent publication, when one was available for selection.
        publication: Option<Arc<GraphPublication>>,
        /// Reason the caller must abandon its existing graph lineage.
        reason: FreshViewReason,
    },
}

/// Reliable lifecycle event produced by ApiService.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiServiceEvent {
    /// A permanent ApiService worker failed unexpectedly.
    Failed(ApiServiceError),
}

/// Receiver for the reliable ApiService lifecycle-event stream.
pub type ApiServiceEventReceiver = mpsc::UnboundedReceiver<ApiServiceEvent>;

/// One ordered API database-generation message delivered to ApiService.
#[derive(Clone, Debug)]
pub enum ApiDbGenerationMessage {
    /// Removes the exact generation when it remains current.
    Retired(Arc<ValidatedApiDbClient>),
    /// Makes one validated generation current.
    Published(Arc<ValidatedApiDbClient>),
}

enum ApiRuntimeSlot {
    AwaitReset,
    Running(OwnedPublicationRuntime),
    Stopping(Option<OwnedPublicationRuntime>),
    Stopped,
}

struct OwnedPublicationRuntime {
    runtime: PublicationRuntime,
    retiring_predecessor: Mutex<Option<Arc<OwnedPublicationRuntime>>>,
    completion_observer: AsyncMutex<Option<JoinHandle<()>>>,
    events: mpsc::UnboundedSender<ApiServiceEvent>,
}

impl OwnedPublicationRuntime {
    fn new(
        graph_updates: GraphUpdateReceiver,
        api_db: Arc<ApiDbState>,
        installer: RuntimePublicationInstaller,
        events: mpsc::UnboundedSender<ApiServiceEvent>,
    ) -> Self {
        let runtime = PublicationRuntime::new(graph_updates, api_db, installer);
        Self::observe(runtime, events)
    }

    fn observe(runtime: PublicationRuntime, events: mpsc::UnboundedSender<ApiServiceEvent>) -> Self {
        let completion = runtime.completion();
        let observer_events = events.clone();
        let completion_observer = tokio::spawn(async move {
            if let Err(error) = completion.await {
                let _ = observer_events.send(ApiServiceEvent::Failed(Self::failure(error)));
            }
        });
        Self {
            runtime,
            retiring_predecessor: Mutex::new(None),
            completion_observer: AsyncMutex::new(Some(completion_observer)),
            events,
        }
    }

    async fn shutdown(&self) -> Result<(), ApiServiceError> {
        let predecessor_result = self.join_retiring_predecessor().await;
        let result = self.shutdown_self().await;
        predecessor_result.and(result)
    }

    fn retain_predecessor(&self, predecessor: OwnedPublicationRuntime) {
        let mut retained = self.retiring_predecessor.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(retained.is_none(), "a successor retains at most one retiring predecessor");
        *retained = Some(Arc::new(predecessor));
    }

    async fn join_retiring_predecessor(&self) -> Result<(), ApiServiceError> {
        let predecessor = self.retiring_predecessor.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        let Some(predecessor) = predecessor else { return Ok(()) };
        let result = predecessor.shutdown_self().await;
        let mut retained = self.retiring_predecessor.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if retained.as_ref().is_some_and(|retained| Arc::ptr_eq(retained, &predecessor)) {
            *retained = None;
        }
        result
    }

    async fn shutdown_self(&self) -> Result<(), ApiServiceError> {
        let result = self.runtime.shutdown().await.map_err(Self::failure);
        let observer_result = {
            let mut observer = self.completion_observer.lock().await;
            let observer_result = match observer.as_mut() {
                Some(observer) => Some(observer.await),
                None => None,
            };
            *observer = None;
            observer_result
        };
        if let Some(Err(error)) = observer_result
            && result.is_ok()
        {
            let failure = Self::failure(error);
            let _ = self.events.send(ApiServiceEvent::Failed(failure.clone()));
            return Err(failure);
        }
        result
    }

    fn failure(error: impl Display) -> ApiServiceError {
        ApiServiceError::PublicationRuntimeFailed { diagnostic: error.to_string().into() }
    }
}

struct OwnedEncodingScheduler {
    scheduler: EncodingScheduler,
    completion_observer: AsyncMutex<Option<JoinHandle<()>>>,
    events: mpsc::UnboundedSender<ApiServiceEvent>,
}

impl OwnedEncodingScheduler {
    fn new(events: mpsc::UnboundedSender<ApiServiceEvent>) -> Self {
        let scheduler = EncodingScheduler::production();
        let completion = scheduler.completion();
        let observer_events = events.clone();
        let completion_observer = tokio::spawn(async move {
            if let Err(error) = completion.await {
                let _ = observer_events.send(ApiServiceEvent::Failed(Self::failure(error)));
            }
        });
        Self { scheduler, completion_observer: AsyncMutex::new(Some(completion_observer)), events }
    }

    #[allow(dead_code, reason = "terminal shutdown consumes this barrier in increment-14 point 6")]
    async fn shutdown(&self) -> Result<(), ApiServiceError> {
        let result = self.scheduler.shutdown().await.map_err(Self::failure);
        let observer_result = {
            let mut observer = self.completion_observer.lock().await;
            let observer_result = match observer.as_mut() {
                Some(observer) => Some(observer.await),
                None => None,
            };
            *observer = None;
            observer_result
        };
        if let Some(Err(error)) = observer_result
            && result.is_ok()
        {
            let failure = Self::failure(error);
            let _ = self.events.send(ApiServiceEvent::Failed(failure.clone()));
            return Err(failure);
        }
        result
    }

    fn failure(error: impl Display) -> ApiServiceError {
        ApiServiceError::EncodingSchedulerFailed { diagnostic: error.to_string().into() }
    }
}

struct PublicationInstallState {
    runtime_generation: u64,
}

impl PublicationInstallState {
    fn advance_runtime_generation(&mut self) -> u64 {
        let generation = self.runtime_generation.checked_add(1).expect("publication runtime generation fits u64");
        self.runtime_generation = generation;
        generation
    }
}

struct RuntimePublicationInstaller<G = ValidatedApiDbClient> {
    publication_install: Arc<Mutex<PublicationInstallState>>,
    current_publication: watch::Sender<Option<Arc<GraphPublication>>>,
    api_db: Arc<ApiDbState<G>>,
    runtime_generation: u64,
    recovery_mode: RecoveryMode,
}

impl<G: ApiDbGeneration> PublicationInstaller for RuntimePublicationInstaller<G> {
    fn install(&self, publication: Arc<GraphPublication>) -> PublicationInstallOutcome {
        let state = self.publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.runtime_generation != self.runtime_generation {
            return PublicationInstallOutcome::Superseded;
        }
        self.current_publication.send_replace(Some(publication));
        if self.recovery_mode == RecoveryMode::Rebuild {
            self.api_db.set_public_reads_enabled(true);
        }
        PublicationInstallOutcome::Installed
    }
}

#[allow(dead_code, reason = "the cancellation roots are consumed by later increment-14 lifecycle points")]
struct ApiCancellation {
    http: CancellationToken,
    sse: CancellationToken,
    cache_jobs: CancellationToken,
}

#[allow(dead_code, reason = "status aggregation is implemented in increment-14 point 3")]
struct ApiStatusInputs {
    kgi_version: &'static str,
    supervisor: watch::Receiver<SupervisorStatus>,
    node: watch::Receiver<NodeServiceStatus>,
    storage: watch::Receiver<StorageServiceStatus>,
    processing: watch::Receiver<ProcessingStatus>,
}

impl ApiStatusInputs {
    fn current(&self) -> SystemStatus {
        SystemStatus {
            kgi_version: self.kgi_version.to_owned(),
            supervisor: *self.supervisor.borrow(),
            node: self.node.borrow().clone(),
            storage: *self.storage.borrow(),
            processing: *self.processing.borrow(),
        }
    }
}

/// Permanent owner of API publication, cache, encoding, and transport lifecycles.
#[allow(dead_code, reason = "the initialized resources are consumed by later increment-14 control points")]
pub struct ApiService {
    events: mpsc::UnboundedSender<ApiServiceEvent>,
    runtime: AsyncMutex<ApiRuntimeSlot>,
    publication_install: Arc<Mutex<PublicationInstallState>>,
    api_db: Arc<ApiDbState>,
    current_publication: watch::Sender<Option<Arc<GraphPublication>>>,
    cancellation: ApiCancellation,
    cache_jobs: CacheJobTracker,
    encoding: OwnedEncodingScheduler,
    status: ApiStatusInputs,
}

impl ApiService {
    /// Constructs ApiService in `AwaitReset` with its reliable event path installed.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    #[must_use]
    pub fn new(
        supervisor_status: watch::Receiver<SupervisorStatus>,
        node_status: watch::Receiver<NodeServiceStatus>,
        storage_status: watch::Receiver<StorageServiceStatus>,
        processing_status: watch::Receiver<ProcessingStatus>,
    ) -> (Arc<Self>, ApiServiceEventReceiver) {
        let (events, event_rx) = mpsc::unbounded_channel();
        let cache_jobs_cancellation = CancellationToken::new();
        let api_db = Arc::new(ApiDbState::new());
        let publication_install = Arc::new(Mutex::new(PublicationInstallState { runtime_generation: 0 }));
        let (current_publication, _) = watch::channel(None);
        let encoding = OwnedEncodingScheduler::new(events.clone());
        let service = Arc::new(Self {
            events,
            runtime: AsyncMutex::new(ApiRuntimeSlot::AwaitReset),
            publication_install,
            api_db,
            current_publication,
            cancellation: ApiCancellation {
                http: CancellationToken::new(),
                sse: CancellationToken::new(),
                cache_jobs: cache_jobs_cancellation.clone(),
            },
            cache_jobs: CacheJobTracker::with_cancellation(cache_jobs_cancellation),
            encoding,
            status: ApiStatusInputs {
                kgi_version: env!("CARGO_PKG_VERSION"),
                supervisor: supervisor_status,
                node: node_status,
                storage: storage_status,
                processing: processing_status,
            },
        });
        (service, event_rx)
    }

    /// Applies one ordered API database-generation message before returning.
    pub async fn update_api_db_generation(&self, message: ApiDbGenerationMessage) -> Result<(), ApiServiceError> {
        let update = match message {
            ApiDbGenerationMessage::Retired(client) => ApiDbGenerationUpdate::Retired(client),
            ApiDbGenerationMessage::Published(client) => ApiDbGenerationUpdate::Published(client),
        };
        self.api_db.update(update).map_err(|_| ApiServiceError::ControlUnavailable)
    }

    /// Replaces the session-bound publication runtime and joins its predecessor.
    pub async fn reset(&self, graph_updates: GraphUpdateReceiver, recovery_mode: RecoveryMode) -> Result<(), ApiServiceError> {
        let mut runtime = self.runtime.lock().await;
        if matches!(*runtime, ApiRuntimeSlot::Stopping(_) | ApiRuntimeSlot::Stopped) {
            return Err(ApiServiceError::ControlUnavailable);
        }
        if let ApiRuntimeSlot::Running(installed) = &*runtime {
            installed.join_retiring_predecessor().await?;
        }

        let runtime_generation = {
            let mut installation = self.publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let generation = installation.advance_runtime_generation();
            if recovery_mode == RecoveryMode::Rebuild {
                self.api_db.begin_rebuild();
            }
            generation
        };
        let installer = RuntimePublicationInstaller {
            publication_install: self.publication_install.clone(),
            current_publication: self.current_publication.clone(),
            api_db: self.api_db.clone(),
            runtime_generation,
            recovery_mode,
        };
        let successor = OwnedPublicationRuntime::new(graph_updates, self.api_db.clone(), installer, self.events.clone());
        let predecessor = std::mem::replace(&mut *runtime, ApiRuntimeSlot::Running(successor));

        if let ApiRuntimeSlot::Running(predecessor) = predecessor {
            let ApiRuntimeSlot::Running(successor) = &*runtime else {
                unreachable!("reset installs its successor before retaining the predecessor")
            };
            successor.retain_predecessor(predecessor);
            successor.join_retiring_predecessor().await?;
        }
        Ok(())
    }

    /// Completes the idempotent terminal barrier for every ApiService-owned resource.
    pub async fn shutdown(&self) -> Result<(), ApiServiceError> {
        let mut runtime = self.runtime.lock().await;
        if matches!(*runtime, ApiRuntimeSlot::Stopped) {
            return Ok(());
        }

        if matches!(*runtime, ApiRuntimeSlot::AwaitReset | ApiRuntimeSlot::Running(_)) {
            let previous = std::mem::replace(&mut *runtime, ApiRuntimeSlot::Stopping(None));
            if let ApiRuntimeSlot::Running(installed) = previous {
                *runtime = ApiRuntimeSlot::Stopping(Some(installed));
            }
        }

        self.api_db.stop();

        let mut result = Ok(());
        if let ApiRuntimeSlot::Stopping(Some(installed)) = &*runtime {
            if let Err(error) = installed.shutdown().await {
                result = Err(error);
            }
            let ApiRuntimeSlot::Stopping(installed) = &mut *runtime else {
                unreachable!("shutdown retains the terminal runtime slot")
            };
            *installed = None;
        }

        self.cancellation.sse.cancel();
        self.cancellation.http.cancel();

        let ((), encoding_result) = tokio::join!(biased; self.cache_jobs.shutdown(), self.encoding.shutdown());
        if result.is_ok()
            && let Err(error) = encoding_result
        {
            result = Err(error);
        }

        self.current_publication.send_replace(None);
        *runtime = ApiRuntimeSlot::Stopped;
        result
    }

    /// Captures the latest independent component observations without waiting for a common revision.
    #[allow(dead_code, reason = "the public status operation is implemented in increment 16")]
    pub(crate) fn system_status(&self) -> SystemStatus {
        self.status.current()
    }

    /// Selects one coherent in-memory Head snapshot or its conditional result.
    #[allow(dead_code, reason = "the logical operation is consumed by the later HTTP transport increment")]
    pub(crate) async fn head_snapshot(
        &self,
        target_depth: u32,
        condition: &HeadSnapshotCondition,
        cancellation: CancellationToken,
    ) -> Result<HeadSnapshotOutcome, CacheWaitError> {
        let depth = selected_head_depth(target_depth);
        let Some(publication) = self.current_publication() else {
            return Ok(HeadSnapshotOutcome::Unavailable);
        };

        loop {
            if cancellation.is_cancelled() {
                return Err(CacheWaitError::Cancelled);
            }
            let access =
                match publication.head_tier(&self.cache_jobs, &self.encoding.scheduler, depth, |etag| condition.matches(etag)).await {
                    HeadTierSelection::Access(access) => access,
                    HeadTierSelection::ConditionMatched(etag) => {
                        return Ok(HeadSnapshotOutcome::NotModified { publication, etag });
                    }
                };
            let candidate = match access.resolve(cancellation.clone()).await {
                Ok(candidate) => candidate,
                Err(CacheWaitError::InvalidatedByReplayFloor { .. }) => continue,
                Err(error) => return Err(error),
            };
            let candidate = match publication.admit_cached_head_replay_floor(candidate).await {
                ReplayFloorAdmission::Admitted(candidate) => candidate,
                ReplayFloorAdmission::Rejected { .. } => continue,
            };
            if condition.matches(candidate.etag()) {
                return Ok(HeadSnapshotOutcome::NotModified { publication, etag: candidate.etag().clone() });
            }
            return Ok(HeadSnapshotOutcome::Complete { publication, snapshot: candidate });
        }
    }

    /// Captures and encodes complete retained levels from one exact publication.
    #[allow(dead_code, reason = "the logical operation is consumed by the later HTTP transport increment")]
    pub(crate) async fn head_levels(
        &self,
        requested_publication_id: u64,
        requested_levels: &[u32],
        cancellation: CancellationToken,
    ) -> Result<HeadLevelLookupResponse, EncodingSchedulerError> {
        if cancellation.is_cancelled() {
            return Err(EncodingSchedulerError::Cancelled);
        }
        let Some(publication) = self.current_publication() else {
            return Ok(HeadLevelLookupResponse::FreshViewRequired { publication: None, reason: FreshViewReason::PublicationMismatch });
        };
        if publication.publication_id() != requested_publication_id {
            return Ok(HeadLevelLookupResponse::FreshViewRequired {
                publication: Some(publication),
                reason: FreshViewReason::PublicationMismatch,
            });
        }

        let (publication_id, revision, levels) = match publication.capture_levels(requested_levels).await {
            HeadLevelLookupOutcome::Complete { publication_id, revision, levels } => (publication_id, revision, levels),
            HeadLevelLookupOutcome::LevelUnavailable { requested_level } => {
                return Ok(HeadLevelLookupResponse::LevelUnavailable { publication, requested_level });
            }
            HeadLevelLookupOutcome::FreshViewRequired(_) => {
                unreachable!("an exact publication capture cannot produce a publication mismatch")
            }
        };
        let response = HeadLevelLookupResponseDto::from_complete(publication_id, revision, &levels);
        let body = self.encoding.scheduler.encode_priority(response, cancellation).await?;
        Ok(HeadLevelLookupResponse::Complete { publication, body })
    }

    /// Returns the complete publication current at this instant, when present.
    #[allow(dead_code, reason = "public graph operations are implemented in later API increments")]
    pub(crate) fn current_publication(&self) -> Option<Arc<GraphPublication>> {
        self.current_publication.borrow().clone()
    }
}

#[allow(dead_code, reason = "called by the logical Head operation before its HTTP consumer exists")]
fn selected_head_depth(target_depth: u32) -> u32 {
    assert!((1..=MAX_WINDOW_DEPTH).contains(&target_depth), "Head target depth is validated before logical selection");
    CACHED_HEAD_WINDOW_DEPTHS
        .into_iter()
        .find(|&depth| depth >= target_depth)
        .expect("the configured Head tiers cover every valid target depth")
}

#[cfg(test)]
mod tests {
    use flate2::read::GzDecoder;
    use std::{
        collections::{BTreeMap, HashMap},
        io::Read,
        sync::{
            Arc, Barrier as StdBarrier, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    };

    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_api_ingress::channel::{GraphUpdateOfferOutcome, GraphUpdateProducerError, GraphUpdateReceiver, graph_update_channel};
    use kgi_api_model::{
        graph::{GraphView, Level, LevelExtent, TrackingPolicy},
        history::GraphHistory,
        projection::{GraphViewSeed, GraphWindowResolution},
    };
    use kgi_model::{
        block::{BlockColor, BlockCoordinate, BlockHash, CompactId},
        graph_update::{BlockCommitted, LevelCommitted, ParentCommitted, VspcCommitted},
        lifecycle::{
            NodeServiceStatus, NodeServiceStatusState, ProcessingState, ProcessingStatus, RecoveryMode, StorageServiceStatus,
            StorageServiceStatusState, SupervisorLifecycle, SupervisorStatus,
        },
    };
    use kgi_storage::service::{StorageService, StorageServiceEvent};
    use serde::{Serialize, Serializer};
    use serde_json::json;
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ImageExt, runners::AsyncRunner},
    };
    use tokio::{
        sync::{Semaphore, mpsc::error::TryRecvError, watch},
        time::timeout,
    };
    use tokio_util::sync::CancellationToken;
    use url::Url;

    use super::{
        ApiDbGenerationMessage, ApiRuntimeSlot, ApiService, ApiServiceEvent, ApiStatusInputs, FreshViewReason,
        HeadLevelLookupResponse, HeadSnapshotCondition, HeadSnapshotOutcome, OwnedPublicationRuntime, PublicationInstallState,
        RuntimePublicationInstaller, selected_head_depth,
    };
    use crate::{
        api_db::{ApiDbGeneration, ApiDbGenerationUpdate, ApiDbState},
        client::{SseMailbox, SseMessage},
        codec::{HeadEtagInput, sse::PublicationStateDto},
        error::ApiServiceError,
        publication::{
            GraphPublication, GraphPublicationState,
            runtime::{
                PublicationInstallOutcome, PublicationInstaller, PublicationRuntime, PublicationRuntimePhase, PublicationSeedDatabase,
            },
        },
    };
    use kgi_storage::error::ApiReadError;

    const POSTGRES_PORT: u16 = 5432;
    const TEST_TIMEOUT: Duration = Duration::from_secs(15);

    struct TestGeneration {
        valid: AtomicBool,
    }

    impl TestGeneration {
        fn valid() -> Arc<Self> {
            Arc::new(Self { valid: AtomicBool::new(true) })
        }
    }

    impl ApiDbGeneration for TestGeneration {
        fn is_valid(&self) -> bool {
            self.valid.load(Ordering::Acquire)
        }
    }

    impl PublicationSeedDatabase for TestGeneration {
        async fn load_head_seed(&self) -> Result<GraphViewSeed, ApiReadError> {
            panic!("simulated publication-runtime worker loss")
        }
    }

    struct ControlledSeedGeneration {
        valid: AtomicBool,
        seed: Mutex<Option<GraphViewSeed>>,
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }

    impl ControlledSeedGeneration {
        fn gated(seed: GraphViewSeed) -> (Arc<Self>, Arc<Semaphore>, Arc<Semaphore>) {
            let started = Arc::new(Semaphore::new(0));
            let release = Arc::new(Semaphore::new(0));
            (
                Arc::new(Self {
                    valid: AtomicBool::new(true),
                    seed: Mutex::new(Some(seed)),
                    started: started.clone(),
                    release: release.clone(),
                }),
                started,
                release,
            )
        }

        fn immediate(seed: GraphViewSeed) -> Arc<Self> {
            let (generation, _, release) = Self::gated(seed);
            release.add_permits(1);
            generation
        }
    }

    impl ApiDbGeneration for ControlledSeedGeneration {
        fn is_valid(&self) -> bool {
            self.valid.load(Ordering::Acquire)
        }
    }

    impl PublicationSeedDatabase for ControlledSeedGeneration {
        async fn load_head_seed(&self) -> Result<GraphViewSeed, ApiReadError> {
            self.started.add_permits(1);
            self.release.acquire().await.expect("seed release path remains open").forget();
            Ok(self.seed.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take().expect("one seed is loaded once"))
        }
    }

    struct BlockingEncoding {
        started: Arc<Semaphore>,
        release: Arc<StdBarrier>,
    }

    impl Serialize for BlockingEncoding {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.started.add_permits(1);
            self.release.wait();
            serializer.serialize_unit()
        }
    }

    fn publication() -> Arc<GraphPublication> {
        let view = GraphView::from_database_projection(
            1,
            LevelExtent::new(1, 1).expect("valid extent"),
            TrackingPolicy::Head,
            BTreeMap::from([(1, Level { size: 1, daa_score: None })]),
            HashMap::new(),
            HashMap::new(),
        );
        Arc::new(
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), view, GraphHistory::new())
                .expect("coherent publication"),
        )
    }

    fn decode_gzip(body: &bytes::Bytes) -> serde_json::Value {
        let mut decoded = Vec::new();
        GzDecoder::new(body.as_ref()).read_to_end(&mut decoded).expect("gzip response decodes");
        serde_json::from_slice(&decoded).expect("response contains JSON")
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
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

    fn vspc_update(source: u8) -> VspcCommitted {
        VspcCommitted {
            source: hash(source),
            destination: hash(source.wrapping_add(1)),
            removed: Arc::from([]),
            added: Arc::from([]),
            level_snapshots: Arc::from([LevelCommitted { level: 10, size: 0, daa_score: Some(u64::from(source)) }]),
        }
    }

    async fn install_controlled_runtime(
        service: &Arc<ApiService>,
        graph_updates: GraphUpdateReceiver,
        generation: Arc<ControlledSeedGeneration>,
    ) {
        let api_db = Arc::new(ApiDbState::new());
        api_db.update(ApiDbGenerationUpdate::Published(generation)).expect("test seed generation is published");
        let runtime_generation =
            service.publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).advance_runtime_generation();
        let installer = RuntimePublicationInstaller {
            publication_install: service.publication_install.clone(),
            current_publication: service.current_publication.clone(),
            api_db: api_db.clone(),
            runtime_generation,
            recovery_mode: RecoveryMode::Resync,
        };
        let runtime = PublicationRuntime::new(graph_updates, api_db, installer);
        let mut slot = service.runtime.lock().await;
        assert!(matches!(*slot, ApiRuntimeSlot::AwaitReset));
        *slot = ApiRuntimeSlot::Running(OwnedPublicationRuntime::observe(runtime, service.events.clone()));
    }

    async fn wait_for_runtime_phase(service: &ApiService, expected: PublicationRuntimePhase) {
        for _ in 0..100 {
            let phase = {
                let slot = service.runtime.lock().await;
                let ApiRuntimeSlot::Running(runtime) = &*slot else { panic!("test runtime remains installed") };
                runtime.runtime.phase()
            };
            if phase == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("publication runtime did not reach {expected:?}");
    }

    async fn postgres_fixture() -> (testcontainers_modules::testcontainers::ContainerAsync<Postgres>, Url) {
        let container = Postgres::default().with_tag("17-alpine").start().await.expect("PostgreSQL fixture must start");
        let host = container.get_host().await.expect("fixture host must resolve");
        let port = container.get_host_port_ipv4(POSTGRES_PORT).await.expect("fixture PostgreSQL port must resolve");
        let database_url =
            Url::parse(&format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable")).expect("fixture URL");
        (container, database_url)
    }

    async fn api_generation_fixture() -> (
        testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
        Arc<StorageService>,
        kgi_storage::service::StorageServiceEventReceiver,
        Arc<kgi_storage::generation::ValidatedApiDbClient>,
    ) {
        let (container, database_url) = postgres_fixture().await;
        let (storage, mut events) = StorageService::start(database_url);
        let mut status = storage.subscribe_status();
        timeout(TEST_TIMEOUT, async {
            loop {
                if status.borrow().state == StorageServiceStatusState::AwaitingInitialization {
                    return;
                }
                status.changed().await.expect("StorageService remains observable");
            }
        })
        .await
        .expect("StorageService reaches initialization admission");
        storage
            .initialize_if_uninitialized(NetworkId::new(NetworkType::Mainnet), hash(1))
            .await
            .expect("database initialization succeeds");
        match timeout(TEST_TIMEOUT, events.recv()).await.expect("processing publication arrives") {
            Some(StorageServiceEvent::ProcessingDbPublished(_)) => {}
            event => panic!("expected processing publication, got {event:?}"),
        }
        let api = match timeout(TEST_TIMEOUT, events.recv()).await.expect("API publication arrives") {
            Some(StorageServiceEvent::ApiDbPublished(api)) => api,
            event => panic!("expected API publication, got {event:?}"),
        };
        (container, storage, events, api)
    }

    fn service() -> (std::sync::Arc<ApiService>, super::ApiServiceEventReceiver) {
        let (_, supervisor) = watch::channel(SupervisorStatus {
            lifecycle: SupervisorLifecycle::Running,
            desired_recovery: None,
            active_recovery: None,
        });
        let (_, node) = watch::channel(NodeServiceStatus { state: NodeServiceStatusState::Connecting, last_validated: None });
        let (_, storage) = watch::channel(StorageServiceStatus { state: StorageServiceStatusState::Connecting });
        let (_, processing) = watch::channel(ProcessingStatus { state: ProcessingState::Idle });
        ApiService::new(supervisor, node, storage, processing)
    }

    #[test]
    fn head_target_depth_selects_the_smallest_covering_tier() {
        for (target_depth, expected_tier) in [
            (1, 20),
            (19, 20),
            (20, 20),
            (21, 50),
            (49, 50),
            (50, 50),
            (51, 100),
            (99, 100),
            (100, 100),
            (101, 150),
            (149, 150),
            (150, 150),
            (151, 200),
            (199, 200),
            (200, 200),
            (201, 250),
            (249, 250),
            (250, 250),
        ] {
            assert_eq!(selected_head_depth(target_depth), expected_tier);
        }
    }

    #[tokio::test]
    async fn head_snapshot_reuses_one_exact_tier_and_ignores_state_for_validation() {
        let (service, _events) = service();
        assert!(matches!(
            service
                .head_snapshot(1, &HeadSnapshotCondition::Unconditional, CancellationToken::new())
                .await
                .expect("an absent publication is an ordinary logical outcome"),
            HeadSnapshotOutcome::Unavailable
        ));

        let publication = publication();
        service.current_publication.send_replace(Some(publication.clone()));
        let prospective_etag =
            HeadEtagInput::new(publication.publication_id(), 0, LevelExtent::new(1, 1).expect("valid current-tier extent")).etag();
        let prospective_condition = HeadSnapshotCondition::Tags(Arc::from([prospective_etag.clone()]));
        let prospective_match = service
            .head_snapshot(1, &prospective_condition, CancellationToken::new())
            .await
            .expect("a prospective current-tier identity is admitted");
        let HeadSnapshotOutcome::NotModified { publication: retained, etag } = prospective_match else {
            panic!("a matching prospective identity avoids construction")
        };
        assert!(Arc::ptr_eq(&retained, &publication));
        assert_eq!(etag, prospective_etag);

        let first = service
            .head_snapshot(1, &HeadSnapshotCondition::Unconditional, CancellationToken::new())
            .await
            .expect("the first tier is constructed");
        let HeadSnapshotOutcome::Complete { publication: retained, snapshot: first } = first else {
            panic!("an unconditional request receives the complete snapshot")
        };
        assert!(Arc::ptr_eq(&retained, &publication));
        assert_eq!(first.depth(), 20);
        assert_eq!((first.low_level(), first.high_level()), (1, 1));

        let repeated = service
            .head_snapshot(20, &HeadSnapshotCondition::Unconditional, CancellationToken::new())
            .await
            .expect("the exact completed tier is reusable");
        let HeadSnapshotOutcome::Complete { snapshot: repeated, .. } = repeated else {
            panic!("an unconditional request receives the complete snapshot")
        };
        assert!(Arc::ptr_eq(&repeated, &first));

        publication.transition_live().await;
        let condition = HeadSnapshotCondition::Tags(Arc::from([first.etag().clone()]));
        let matched = service
            .head_snapshot(1, &condition, CancellationToken::new())
            .await
            .expect("a state-only transition preserves snapshot identity");
        let HeadSnapshotOutcome::NotModified { publication: retained, etag: matched } = matched else {
            panic!("the unchanged semantic validator matches")
        };
        assert!(Arc::ptr_eq(&retained, &publication));
        assert_eq!(matched, *first.etag());

        let nonmatching = HeadSnapshotCondition::Tags(Arc::from([HeadEtagInput::new(
            publication.publication_id(),
            first.revision(),
            LevelExtent::new(1, 2).expect("valid extent"),
        )
        .etag()]));
        assert!(matches!(
            service.head_snapshot(1, &nonmatching, CancellationToken::new()).await.expect("a nonmatch is successful"),
            HeadSnapshotOutcome::Complete { .. }
        ));

        service.shutdown().await.expect("service resources shut down");
    }

    #[tokio::test]
    async fn head_level_lookup_requires_the_exact_current_publication() {
        let (service, _events) = service();

        let absent =
            service.head_levels(7, &[1], CancellationToken::new()).await.expect("an absent publication is a semantic outcome");
        let HeadLevelLookupResponse::FreshViewRequired { publication: None, reason } = absent else {
            panic!("an absent publication requires a fresh view without response context")
        };
        assert_eq!(reason, FreshViewReason::PublicationMismatch);

        let publication = publication();
        service.current_publication.send_replace(Some(publication.clone()));
        let mismatched_publication_id = if publication.publication_id() == 1 { 2 } else { 1 };
        let mismatch = service
            .head_levels(mismatched_publication_id, &[1], CancellationToken::new())
            .await
            .expect("a publication mismatch is a semantic outcome");
        let HeadLevelLookupResponse::FreshViewRequired { publication: Some(retained), reason } = mismatch else {
            panic!("the current publication is retained while reporting a mismatch")
        };
        assert!(Arc::ptr_eq(&retained, &publication));
        assert_eq!(reason, FreshViewReason::PublicationMismatch);

        service.shutdown().await.expect("service resources shut down");
    }

    #[tokio::test]
    async fn head_level_lookup_is_all_or_nothing_and_encodes_one_capture() {
        let (service, _events) = service();
        let publication = publication();
        service.current_publication.send_replace(Some(publication.clone()));

        let missing = service
            .head_levels(publication.publication_id(), &[1, 2], CancellationToken::new())
            .await
            .expect("an unavailable level is a semantic outcome");
        let HeadLevelLookupResponse::LevelUnavailable { publication: retained, requested_level } = missing else {
            panic!("one unavailable level prevents a partial response")
        };
        assert!(Arc::ptr_eq(&retained, &publication));
        assert_eq!(requested_level, 2);

        let complete = service
            .head_levels(publication.publication_id(), &[1], CancellationToken::new())
            .await
            .expect("retained levels encode successfully");
        let HeadLevelLookupResponse::Complete { publication: retained, body } = complete else {
            panic!("every retained level produces one complete response")
        };
        assert!(Arc::ptr_eq(&retained, &publication));
        assert_eq!(
            decode_gzip(body.gzip_body()),
            json!({
                "publication_id": publication.publication_id().to_string(),
                "revision": "0",
                "levels": [{ "level": 1, "size": 1, "daa_score": null }]
            })
        );

        service.shutdown().await.expect("service resources shut down");
    }

    #[tokio::test]
    async fn construction_installs_the_event_path_and_initializes_every_lifecycle_domain() {
        let (service, mut events) = service();

        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::AwaitReset));
        assert_eq!(service.publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).runtime_generation, 0);
        assert!(service.api_db.public_read_client().is_none());
        assert!(service.current_publication().is_none());
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
        assert!(!events.is_closed());

        assert_eq!(service.status.kgi_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(service.status.supervisor.borrow().lifecycle, SupervisorLifecycle::Running);
        assert_eq!(service.status.node.borrow().state, NodeServiceStatusState::Connecting);
        assert_eq!(service.status.storage.borrow().state, StorageServiceStatusState::Connecting);
        assert_eq!(service.status.processing.borrow().state, ProcessingState::Idle);

        service.encoding.shutdown().await.expect("encoding scheduler shuts down");
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn cancellation_domains_are_independent_roots() {
        let (service, _events) = service();

        service.cancellation.sse.cancel();
        assert!(service.cancellation.sse.is_cancelled());
        assert!(!service.cancellation.http.is_cancelled());
        assert!(!service.cancellation.cache_jobs.is_cancelled());
        service.encoding.shutdown().await.expect("encoding scheduler shuts down");
    }

    #[test]
    fn current_generation_installation_replaces_the_complete_publication_arc() {
        let api_db = Arc::new(ApiDbState::<TestGeneration>::new());
        let publication_install = Arc::new(Mutex::new(PublicationInstallState { runtime_generation: 0 }));
        let (current_publication, _) = watch::channel(None);
        let generation = publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).advance_runtime_generation();
        let installer = RuntimePublicationInstaller {
            publication_install: publication_install.clone(),
            current_publication: current_publication.clone(),
            api_db,
            runtime_generation: generation,
            recovery_mode: RecoveryMode::Resync,
        };
        let first = publication();
        let replacement = publication();

        assert_eq!(installer.install(first), PublicationInstallOutcome::Installed);
        assert_eq!(installer.install(replacement.clone()), PublicationInstallOutcome::Installed);
        assert!(current_publication.borrow().as_ref().is_some_and(|current| Arc::ptr_eq(current, &replacement)));

        publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).advance_runtime_generation();
        assert_eq!(installer.install(publication()), PublicationInstallOutcome::Superseded);
        assert!(current_publication.borrow().as_ref().is_some_and(|current| Arc::ptr_eq(current, &replacement)));
    }

    #[test]
    fn only_current_rebuild_installation_reopens_public_database_reads() {
        let api_db = Arc::new(ApiDbState::<TestGeneration>::new());
        let previous = TestGeneration::valid();
        api_db.update(ApiDbGenerationUpdate::Published(previous.clone())).expect("initial generation is published");
        assert!(api_db.public_read_client().is_some_and(|current| Arc::ptr_eq(&current, &previous)));

        let publication_install = Arc::new(Mutex::new(PublicationInstallState { runtime_generation: 0 }));
        let (current_publication, _) = watch::channel(None);
        let stale_generation =
            publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).advance_runtime_generation();
        let rebuild_generation = {
            let mut state = publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let generation = state.advance_runtime_generation();
            api_db.begin_rebuild();
            generation
        };
        assert!(api_db.public_read_client().is_none());
        assert!(previous.is_valid());

        let replacement = TestGeneration::valid();
        api_db.update(ApiDbGenerationUpdate::Published(replacement.clone())).expect("replacement is bound");
        assert!(api_db.public_read_client().is_none());

        let stale = RuntimePublicationInstaller {
            publication_install: publication_install.clone(),
            current_publication: current_publication.clone(),
            api_db: api_db.clone(),
            runtime_generation: stale_generation,
            recovery_mode: RecoveryMode::Rebuild,
        };
        assert_eq!(stale.install(publication()), PublicationInstallOutcome::Superseded);
        assert!(api_db.public_read_client().is_none());

        let current = RuntimePublicationInstaller {
            publication_install,
            current_publication: current_publication.clone(),
            api_db: api_db.clone(),
            runtime_generation: rebuild_generation,
            recovery_mode: RecoveryMode::Rebuild,
        };
        let installed = publication();
        assert_eq!(current.install(installed.clone()), PublicationInstallOutcome::Installed);
        assert!(current_publication.borrow().as_ref().is_some_and(|current| Arc::ptr_eq(current, &installed)));
        assert!(api_db.public_read_client().is_some_and(|current| Arc::ptr_eq(&current, &replacement)));
    }

    #[test]
    fn runtime_generation_accepts_u64_max_then_panics_before_rebuild_effects() {
        let api_db = Arc::new(ApiDbState::<TestGeneration>::new());
        let generation = TestGeneration::valid();
        api_db.update(ApiDbGenerationUpdate::Published(generation.clone())).expect("generation is published");
        let publication_install = Mutex::new(PublicationInstallState { runtime_generation: u64::MAX - 1 });

        assert_eq!(publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).advance_runtime_generation(), u64::MAX);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut state = publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            state.advance_runtime_generation();
            api_db.begin_rebuild();
        }));

        assert!(panic.is_err());
        assert_eq!(publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).runtime_generation, u64::MAX);
        assert!(api_db.public_read_client().is_some_and(|current| Arc::ptr_eq(&current, &generation)));
    }

    #[test]
    fn status_composition_reads_each_latest_value_without_cross_component_coordination() {
        let (supervisor_tx, supervisor) = watch::channel(SupervisorStatus {
            lifecycle: SupervisorLifecycle::Running,
            desired_recovery: None,
            active_recovery: None,
        });
        let (node_tx, node) = watch::channel(NodeServiceStatus { state: NodeServiceStatusState::Connecting, last_validated: None });
        let (storage_tx, storage) = watch::channel(StorageServiceStatus { state: StorageServiceStatusState::Connecting });
        let (processing_tx, processing) = watch::channel(ProcessingStatus { state: ProcessingState::Idle });
        let inputs = ApiStatusInputs { kgi_version: "test-version", supervisor, node, storage, processing };

        supervisor_tx.send_modify(|status| status.desired_recovery = Some(RecoveryMode::Rebuild));
        node_tx.send_modify(|status| status.state = NodeServiceStatusState::Unavailable);
        storage_tx.send_replace(StorageServiceStatus { state: StorageServiceStatusState::Ready });
        processing_tx.send_replace(ProcessingStatus { state: ProcessingState::Reconciling { mode: RecoveryMode::Resync } });

        let status = inputs.current();
        assert_eq!(status.kgi_version, "test-version");
        assert_eq!(status.supervisor.desired_recovery, Some(RecoveryMode::Rebuild));
        assert_eq!(status.node.state, NodeServiceStatusState::Unavailable);
        assert_eq!(status.node.last_validated, None);
        assert_eq!(status.storage.state, StorageServiceStatusState::Ready);
        assert_eq!(status.processing.state, ProcessingState::Reconciling { mode: RecoveryMode::Resync });
    }

    #[tokio::test]
    async fn reset_starts_the_successor_and_joins_the_predecessor_receiver() {
        let (service, _) = service();
        let (predecessor_producer, predecessor_receiver) = graph_update_channel();

        service.reset(predecessor_receiver, RecoveryMode::Resync).await.expect("first reset succeeds");
        assert_eq!(service.publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).runtime_generation, 1);
        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Running(_)));

        let (successor_producer, successor_receiver) = graph_update_channel();
        service.reset(successor_receiver, RecoveryMode::Rebuild).await.expect("replacement reset succeeds");

        assert_eq!(service.publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).runtime_generation, 2);
        assert_eq!(predecessor_producer.publish_catchup(), Err(GraphUpdateProducerError::ReceiverClosed));
        successor_producer.publish_catchup().expect("successor receiver is active when reset returns");

        {
            let slot = service.runtime.lock().await;
            let ApiRuntimeSlot::Running(runtime) = &*slot else { panic!("successor remains installed") };
            runtime.shutdown().await.expect("successor shuts down");
        }
        service.encoding.shutdown().await.expect("encoding scheduler shuts down");
    }

    #[tokio::test]
    async fn reset_after_stop_is_rejected_and_releases_the_supplied_receiver() {
        let (service, _) = service();
        *service.runtime.lock().await = ApiRuntimeSlot::Stopped;
        let (producer, receiver) = graph_update_channel();

        assert_eq!(service.reset(receiver, RecoveryMode::Resync).await, Err(crate::error::ApiServiceError::ControlUnavailable));
        assert_eq!(producer.publish_catchup(), Err(GraphUpdateProducerError::ReceiverClosed));
        assert_eq!(service.publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).runtime_generation, 0);
        service.encoding.shutdown().await.expect("encoding scheduler shuts down");
    }

    #[tokio::test]
    async fn shutdown_from_await_reset_is_terminal_silent_and_idempotent() {
        let (service, mut events) = service();
        service.current_publication.send_replace(Some(publication()));

        service.shutdown().await.expect("shutdown succeeds");

        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Stopped));
        assert!(service.cancellation.sse.is_cancelled());
        assert!(service.cancellation.http.is_cancelled());
        assert!(service.cancellation.cache_jobs.is_cancelled());
        assert!(service.current_publication().is_none());
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
        service.shutdown().await.expect("repeated shutdown succeeds");
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn shutdown_joins_the_installed_runtime_and_releases_its_receiver() {
        let (service, mut events) = service();
        let (producer, receiver) = graph_update_channel();
        service.reset(receiver, RecoveryMode::Resync).await.expect("runtime starts");

        service.shutdown().await.expect("shutdown succeeds");

        assert_eq!(producer.publish_catchup(), Err(GraphUpdateProducerError::ReceiverClosed));
        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Stopped));
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn shutdown_cancels_a_constructing_runtime_and_its_seed_attempt() {
        let (service, mut events) = service();
        let (generation, started, _release) = ControlledSeedGeneration::gated(alignment_seed());
        let (producer, receiver) = graph_update_channel();
        install_controlled_runtime(&service, receiver, generation).await;

        producer.publish_catchup().expect("runtime receiver is open");
        started.acquire().await.expect("seed start path remains open").forget();
        wait_for_runtime_phase(&service, PublicationRuntimePhase::Constructing).await;

        service.shutdown().await.expect("shutdown cancels construction");

        assert_eq!(producer.publish_live().await, Err(GraphUpdateProducerError::ReceiverClosed));
        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Stopped));
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn shutdown_abandons_an_aligning_runtime_without_installing_its_candidate() {
        let (service, mut events) = service();
        let generation = ControlledSeedGeneration::immediate(alignment_seed());
        let (producer, receiver) = graph_update_channel();
        install_controlled_runtime(&service, receiver, generation).await;

        producer.publish_catchup().expect("runtime receiver is open");
        wait_for_runtime_phase(&service, PublicationRuntimePhase::Aligning).await;
        assert!(service.current_publication().is_none());

        service.shutdown().await.expect("shutdown abandons alignment");

        assert_eq!(producer.publish_live().await, Err(GraphUpdateProducerError::ReceiverClosed));
        assert!(service.current_publication().is_none());
        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Stopped));
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn shutdown_publishes_final_stale_before_releasing_an_active_runtime() {
        let (service, mut events) = service();
        let generation = ControlledSeedGeneration::immediate(alignment_seed());
        let (producer, receiver) = graph_update_channel();
        install_controlled_runtime(&service, receiver, generation).await;

        producer.publish_catchup().expect("runtime receiver is open");
        assert_eq!(producer.offer_block_committed(block_update(6, 11, 10)), Ok(GraphUpdateOfferOutcome::Enqueued));
        assert_eq!(producer.offer_vspc_committed(vspc_update(90)), Ok(GraphUpdateOfferOutcome::Enqueued));
        producer.publish_live().await.expect("runtime receiver is open");
        wait_for_runtime_phase(&service, PublicationRuntimePhase::Active).await;
        let publication = service.current_publication().expect("active runtime installs its publication");
        let target = publication.capture_delta_target().await;
        assert_eq!(target.state(), GraphPublicationState::Live);

        let mailbox = Arc::new(SseMailbox::new());
        let identifier = publication
            .client_registry()
            .register(target.revision(), &mailbox, CancellationToken::new())
            .expect("active publication accepts an SSE registration");
        for expected in [
            SseMessage::ClientRegistration(identifier.registration()),
            SseMessage::PublicationState(PublicationStateDto::new(
                publication.publication_id(),
                target.revision(),
                GraphPublicationState::Live,
            )),
            SseMessage::PublicationWakeup(crate::codec::sse::PublicationWakeupDto::new(
                publication.publication_id(),
                target.revision(),
            )),
        ] {
            assert_eq!(mailbox.recv().await, Some(expected));
        }

        service.shutdown().await.expect("shutdown retires the active publication");

        assert_eq!(publication.capture_delta_target().await.state(), GraphPublicationState::Stale);
        assert_eq!(
            mailbox.recv().await,
            Some(SseMessage::PublicationState(PublicationStateDto::new(
                publication.publication_id(),
                target.revision(),
                GraphPublicationState::Stale,
            )))
        );
        assert_eq!(producer.publish_live().await, Err(GraphUpdateProducerError::ReceiverClosed));
        assert!(service.current_publication().is_none());
        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Stopped));
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn unexpected_runtime_failure_emits_exactly_one_reliable_event() {
        let (producer, receiver) = graph_update_channel();
        let api_db = Arc::new(ApiDbState::<TestGeneration>::new());
        api_db.update(ApiDbGenerationUpdate::Published(TestGeneration::valid())).expect("test database generation is published");
        let (current_publication, _) = watch::channel(None);
        let runtime = PublicationRuntime::new(
            receiver,
            api_db.clone(),
            RuntimePublicationInstaller {
                publication_install: Arc::new(Mutex::new(PublicationInstallState { runtime_generation: 0 })),
                current_publication,
                api_db,
                runtime_generation: 0,
                recovery_mode: RecoveryMode::Resync,
            },
        );
        let (event_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
        let owned = OwnedPublicationRuntime::observe(runtime, event_tx);

        producer.publish_catchup().expect("runtime receiver is open");
        let event = events.recv().await.expect("runtime failure is reported");
        assert!(matches!(event, ApiServiceEvent::Failed(crate::error::ApiServiceError::PublicationRuntimeFailed { .. })));
        assert!(owned.shutdown().await.is_err());
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn encoding_scheduler_failure_emits_exactly_one_reliable_event() {
        let (service, mut events) = service();

        service.encoding.scheduler.fail_worker();

        assert!(matches!(events.recv().await, Some(ApiServiceEvent::Failed(ApiServiceError::EncodingSchedulerFailed { .. }))));
        assert!(matches!(service.shutdown().await, Err(ApiServiceError::EncodingSchedulerFailed { .. })));
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn api_database_update_linearizes_on_both_sides_of_service_shutdown() {
        let (_container, storage, _storage_events, api) = api_generation_fixture().await;
        let (service, mut events) = service();
        service
            .update_api_db_generation(ApiDbGenerationMessage::Published(api.clone()))
            .await
            .expect("a generation update before shutdown is accepted");
        let start = Arc::new(tokio::sync::Barrier::new(3));

        let update_service = service.clone();
        let update_start = start.clone();
        let retired = api.clone();
        let update = tokio::spawn(async move {
            update_start.wait().await;
            update_service.update_api_db_generation(ApiDbGenerationMessage::Retired(retired)).await
        });
        let shutdown_service = service.clone();
        let shutdown_start = start.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_start.wait().await;
            shutdown_service.shutdown().await
        });
        start.wait().await;

        let update_result = update.await.expect("generation update task completes");
        assert!(update_result.is_ok() || update_result == Err(ApiServiceError::ControlUnavailable));
        assert_eq!(shutdown.await.expect("service shutdown task completes"), Ok(()));
        assert_eq!(
            service.update_api_db_generation(ApiDbGenerationMessage::Published(api)).await,
            Err(ApiServiceError::ControlUnavailable)
        );
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));

        storage.shutdown().await.expect("StorageService fixture shuts down");
    }

    #[tokio::test]
    async fn reset_and_shutdown_serialize_to_one_terminal_runtime_state() {
        let (service, mut events) = service();
        let (producer, receiver) = graph_update_channel();
        let start = Arc::new(tokio::sync::Barrier::new(3));

        let reset_service = service.clone();
        let reset_start = start.clone();
        let reset = tokio::spawn(async move {
            reset_start.wait().await;
            reset_service.reset(receiver, RecoveryMode::Resync).await
        });
        let shutdown_service = service.clone();
        let shutdown_start = start.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_start.wait().await;
            shutdown_service.shutdown().await
        });
        start.wait().await;

        let reset_result = reset.await.expect("reset task completes");
        assert!(reset_result.is_ok() || reset_result == Err(crate::error::ApiServiceError::ControlUnavailable));
        assert_eq!(shutdown.await.expect("shutdown task completes"), Ok(()));
        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Stopped));
        assert_eq!(producer.publish_catchup(), Err(GraphUpdateProducerError::ReceiverClosed));
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn cancelled_shutdown_keeps_reset_rejected_and_later_shutdown_resumes() {
        let (service, mut events) = service();
        let (installed_producer, installed_receiver) = graph_update_channel();
        service.reset(installed_receiver, RecoveryMode::Resync).await.expect("runtime starts");
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(StdBarrier::new(2));
        let encoding = service.encoding.scheduler.clone();
        let encoding_started = started.clone();
        let encoding_release = release.clone();
        let encode = tokio::spawn(async move {
            encoding
                .encode_priority(BlockingEncoding { started: encoding_started, release: encoding_release }, CancellationToken::new())
                .await
        });
        started.acquire().await.expect("encoding start signal remains open").forget();

        let shutdown_service = service.clone();
        let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
        while !service.cancellation.cache_jobs.is_cancelled() {
            tokio::task::yield_now().await;
        }
        shutdown.abort();
        assert!(shutdown.await.expect_err("shutdown waiter is cancelled").is_cancelled());
        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Stopping(None)));
        assert_eq!(installed_producer.publish_catchup(), Err(GraphUpdateProducerError::ReceiverClosed));

        let (producer, receiver) = graph_update_channel();
        assert_eq!(service.reset(receiver, RecoveryMode::Resync).await, Err(crate::error::ApiServiceError::ControlUnavailable));
        assert_eq!(producer.publish_catchup(), Err(GraphUpdateProducerError::ReceiverClosed));
        assert_eq!(service.publication_install.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).runtime_generation, 1);

        let release_wait = release.clone();
        tokio::task::spawn_blocking(move || release_wait.wait()).await.expect("encoding release task completes");
        assert!(encode.await.expect("encoding owner completes").is_err());
        service.shutdown().await.expect("later shutdown resumes the retained barrier");

        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Stopped));
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn cancelled_reset_retains_the_predecessor_until_terminal_shutdown_joins_it() {
        let (service, mut events) = service();
        let (predecessor_producer, predecessor_receiver) = graph_update_channel();
        service.reset(predecessor_receiver, RecoveryMode::Resync).await.expect("predecessor starts");

        let observer_release = Arc::new(Semaphore::new(0));
        let gated_release = observer_release.clone();
        let gated_observer = tokio::spawn(async move {
            gated_release.acquire().await.expect("observer release signal remains open").forget();
        });
        let original_observer = {
            let slot = service.runtime.lock().await;
            let ApiRuntimeSlot::Running(predecessor) = &*slot else { panic!("predecessor remains current") };
            predecessor.completion_observer.lock().await.replace(gated_observer)
        };
        let original_observer = original_observer.expect("predecessor owns its completion observer");
        original_observer.abort();
        assert!(original_observer.await.expect_err("original observer is replaced for the test").is_cancelled());

        let (successor_producer, successor_receiver) = graph_update_channel();
        let reset_service = service.clone();
        let reset = tokio::spawn(async move { reset_service.reset(successor_receiver, RecoveryMode::Resync).await });
        for _ in 0..100 {
            if predecessor_producer.metrics().receiver_closure_count == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(predecessor_producer.metrics().receiver_closure_count, 1);
        reset.abort();
        assert!(reset.await.expect_err("replacement reset waiter is cancelled").is_cancelled());

        {
            let slot = service.runtime.lock().await;
            let ApiRuntimeSlot::Running(successor) = &*slot else { panic!("successor remains current") };
            assert!(successor.retiring_predecessor.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).is_some());
        }

        let shutdown_service = service.clone();
        let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!shutdown.is_finished(), "terminal shutdown joins the retained predecessor observer");

        observer_release.add_permits(1);
        assert_eq!(shutdown.await.expect("terminal shutdown task completes"), Ok(()));
        assert_eq!(successor_producer.publish_catchup(), Err(GraphUpdateProducerError::ReceiverClosed));
        assert!(matches!(*service.runtime.lock().await, ApiRuntimeSlot::Stopped));
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn completed_shutdown_releases_the_reliable_event_path_with_the_service() {
        let (service, mut events) = service();

        service.shutdown().await.expect("shutdown succeeds");
        drop(service);

        assert_eq!(events.recv().await, None);
    }
}
