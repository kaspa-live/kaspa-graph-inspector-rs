use std::sync::{Arc, Mutex};

use kgi_api_model::protocol::SystemStatus;
use kgi_model::lifecycle::{NodeServiceStatus, ProcessingStatus, RecoveryMode, StorageServiceStatus, SupervisorStatus};
use kgi_storage::generation::ValidatedApiDbClient;
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
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

#[allow(dead_code, reason = "the runtime slot is advanced by the next increment-14 control point")]
enum ApiRuntimeSlot {
    AwaitReset,
    Running(PublicationRuntime),
    Stopped,
}

#[allow(dead_code, reason = "the installation boundary is implemented in increment-14 point 2")]
struct PublicationInstallState {
    runtime_generation: u64,
}

impl PublicationInstallState {
    #[allow(dead_code, reason = "ApiService reset advances this generation in increment-14 point 4")]
    fn advance_runtime_generation(&mut self) -> u64 {
        let generation = self.runtime_generation.checked_add(1).expect("publication runtime generation fits u64");
        self.runtime_generation = generation;
        generation
    }
}

#[allow(dead_code, reason = "ApiService reset constructs this capability in increment-14 point 4")]
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
    encoding: EncodingScheduler,
    status: ApiStatusInputs,
}

impl ApiService {
    /// Constructs ApiService in `AwaitReset` with its reliable event path installed.
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
            encoding: EncodingScheduler::production(),
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

    use kgi_api_model::{
        graph::{GraphView, Level, LevelExtent, TrackingPolicy},
        history::GraphHistory,
    };
    use kgi_model::lifecycle::{
        NodeServiceStatus, NodeServiceStatusState, ProcessingState, ProcessingStatus, RecoveryMode, StorageServiceStatus,
        StorageServiceStatusState, SupervisorLifecycle, SupervisorStatus,
    };
    use tokio::sync::{mpsc::error::TryRecvError, watch};

    use super::{ApiRuntimeSlot, ApiService, ApiStatusInputs, PublicationInstallState, RuntimePublicationInstaller};
    use crate::{
        api_db::{ApiDbGeneration, ApiDbGenerationUpdate, ApiDbState},
        publication::{
            GraphPublication, GraphPublicationState,
            runtime::{PublicationInstallOutcome, PublicationInstaller},
        },
    };

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
    }

    #[test]
    fn cancellation_domains_are_independent_roots() {
        let (service, _events) = service();

        service.cancellation.sse.cancel();
        assert!(service.cancellation.sse.is_cancelled());
        assert!(!service.cancellation.http.is_cancelled());
        assert!(!service.cancellation.cache_jobs.is_cancelled());
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
}
