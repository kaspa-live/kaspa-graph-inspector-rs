use std::sync::{Arc, Mutex};

use kgi_model::lifecycle::{NodeServiceStatus, ProcessingStatus, StorageServiceStatus, SupervisorStatus};
use kgi_storage::generation::ValidatedApiDbClient;
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    api_db::ApiDbState,
    cache::CacheJobTracker,
    encoding::scheduler::EncodingScheduler,
    error::ApiServiceError,
    publication::{GraphPublication, runtime::PublicationRuntime},
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
        let (current_publication, _) = watch::channel(None);
        let service = Arc::new(Self {
            events,
            runtime: AsyncMutex::new(ApiRuntimeSlot::AwaitReset),
            publication_install: Arc::new(Mutex::new(PublicationInstallState { runtime_generation: 0 })),
            api_db: Arc::new(ApiDbState::new()),
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
}

#[cfg(test)]
mod tests {
    use kgi_model::lifecycle::{
        NodeServiceStatus, NodeServiceStatusState, ProcessingState, ProcessingStatus, StorageServiceStatus, StorageServiceStatusState,
        SupervisorLifecycle, SupervisorStatus,
    };
    use tokio::sync::{mpsc::error::TryRecvError, watch};

    use super::{ApiRuntimeSlot, ApiService};

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
        assert!(service.current_publication.borrow().is_none());
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
}
