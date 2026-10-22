use std::{
    fmt::Display,
    sync::{Arc, Mutex},
};

use kgi_api_ingress::channel::GraphUpdateReceiver;
use kgi_api_model::protocol::SystemStatus;
use kgi_model::lifecycle::{NodeServiceStatus, ProcessingStatus, RecoveryMode, StorageServiceStatus, SupervisorStatus};
use kgi_storage::generation::ValidatedApiDbClient;
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    api_db::{ApiDbGeneration, ApiDbGenerationUpdate, ApiDbState},
    cache::CacheJobTracker,
    encoding::scheduler::EncodingScheduler,
    error::ApiServiceError,
    publication::{
        GraphPublication,
        runtime::{PublicationInstallOutcome, PublicationInstaller, PublicationRuntime},
    },
};

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
    Stopped,
}

struct OwnedPublicationRuntime {
    runtime: PublicationRuntime,
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
        Self { runtime, completion_observer: AsyncMutex::new(Some(completion_observer)), events }
    }

    async fn shutdown(&self) -> Result<(), ApiServiceError> {
        let result = self.runtime.shutdown().await.map_err(Self::failure);
        let observer = self.completion_observer.lock().await.take();
        if let Some(observer) = observer
            && let Err(error) = observer.await
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
        let observer = self.completion_observer.lock().await.take();
        if let Some(observer) = observer
            && let Err(error) = observer.await
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
        if matches!(*runtime, ApiRuntimeSlot::Stopped) {
            return Err(ApiServiceError::ControlUnavailable);
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
            predecessor.shutdown().await?;
        }
        Ok(())
    }

    /// Completes the idempotent terminal barrier for every ApiService-owned resource.
    pub async fn shutdown(&self) -> Result<(), ApiServiceError> {
        let mut runtime = self.runtime.lock().await;
        if matches!(*runtime, ApiRuntimeSlot::Stopped) {
            return Ok(());
        }

        self.api_db.stop();

        let mut result = Ok(());
        if let ApiRuntimeSlot::Running(installed) = &*runtime
            && let Err(error) = installed.shutdown().await
        {
            result = Err(error);
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

    /// Returns the complete publication current at this instant, when present.
    #[allow(dead_code, reason = "public graph operations are implemented in later API increments")]
    pub(crate) fn current_publication(&self) -> Option<Arc<GraphPublication>> {
        self.current_publication.borrow().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Instant,
    };

    use kgi_api_ingress::channel::{GraphUpdateProducerError, graph_update_channel};
    use kgi_api_model::{
        graph::{GraphView, Level, LevelExtent, TrackingPolicy},
        history::GraphHistory,
        projection::GraphViewSeed,
    };
    use kgi_model::lifecycle::{
        NodeServiceStatus, NodeServiceStatusState, ProcessingState, ProcessingStatus, RecoveryMode, StorageServiceStatus,
        StorageServiceStatusState, SupervisorLifecycle, SupervisorStatus,
    };
    use tokio::sync::{mpsc::error::TryRecvError, watch};

    use super::{
        ApiRuntimeSlot, ApiService, ApiServiceEvent, ApiStatusInputs, OwnedPublicationRuntime, PublicationInstallState,
        RuntimePublicationInstaller,
    };
    use crate::{
        api_db::{ApiDbGeneration, ApiDbGenerationUpdate, ApiDbState},
        publication::{
            GraphPublication, GraphPublicationState,
            runtime::{PublicationInstallOutcome, PublicationInstaller, PublicationRuntime, PublicationSeedDatabase},
        },
    };
    use kgi_storage::error::ApiReadError;

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
    async fn completed_shutdown_releases_the_reliable_event_path_with_the_service() {
        let (service, mut events) = service();

        service.shutdown().await.expect("shutdown succeeds");
        drop(service);

        assert_eq!(events.recv().await, None);
    }
}
