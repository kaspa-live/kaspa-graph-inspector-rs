use std::{collections::VecDeque, future::Future, sync::Arc, time::Duration};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use kaspa_consensus_core::network::NetworkId;
use kgi_core::timing::Timing;
use kgi_model::{
    block::{BlockHash, MaterializedSyncAnchor},
    lifecycle::{PersistenceFault, StorageServiceStatus, StorageServiceStatusState},
};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};
use url::Url;

use crate::{
    database::{
        DatabaseConnector, PreparedDatabase, SqlxDatabaseConnector, ValidatedGenerations, open_api_generation,
        open_processing_generation_managed, open_validated_generations_managed,
    },
    error::{StorageError, StorageRejection},
    generation::{DatabaseBinding, ValidatedApiDbClient, ValidatedDbClient},
    replacement::ApiReplacementGate,
    runtime::{
        RebuildReceiver, RebuildRequest, RebuildSender, RetirementReceiver, RetirementRequest, RetirementSender, RetirementTarget,
        rebuild_channel, retirement_channel,
    },
    state::DatabaseState,
};

const LOCK_HEALTH_INTERVAL: Duration = Duration::from_secs(1);
const READY_BACKOFF_RESET: Duration = Duration::from_secs(60);
const RETRY_DELAYS: [Duration; 6] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
    Duration::from_secs(30),
];

/// Reliable ordered lifecycle event produced by StorageService.
#[derive(Clone, Debug)]
pub enum StorageServiceEvent {
    /// A processing database generation ceased accepting work.
    ProcessingDbRetired(Arc<ValidatedDbClient>),
    /// A processing database generation became usable.
    ProcessingDbPublished(Arc<ValidatedDbClient>),
    /// An API database generation ceased accepting work.
    ApiDbRetired(Arc<ValidatedApiDbClient>),
    /// An API database generation became usable.
    ApiDbPublished(Arc<ValidatedApiDbClient>),
    /// The configured database was permanently rejected.
    Rejected(StorageRejection),
}

/// Receiver for the reliable ordered StorageService event stream.
pub type StorageServiceEventReceiver = mpsc::UnboundedReceiver<StorageServiceEvent>;

/// Permanent owner of PostgreSQL connectivity and validated database generations.
pub struct StorageService {
    commands: mpsc::UnboundedSender<ServiceCommand>,
    status: watch::Receiver<StorageServiceStatus>,
    completion: watch::Receiver<Option<Result<(), StorageError>>>,
    join: Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    retained_cleanup_tasks: Arc<AtomicUsize>,
    #[cfg(test)]
    panic_next_cleanup: Arc<AtomicBool>,
    #[cfg(test)]
    replacement_gate: Arc<ApiReplacementGate>,
}

impl StorageService {
    /// Starts the permanent database lifecycle worker with its event path installed.
    #[must_use]
    pub fn start(database_url: Url) -> (Arc<Self>, StorageServiceEventReceiver) {
        Self::start_with_dependencies(database_url, Arc::new(SqlxDatabaseConnector), Timing::production())
    }

    fn start_with_dependencies(
        database_url: Url,
        connector: Arc<dyn DatabaseConnector>,
        timing: Timing,
    ) -> (Arc<Self>, StorageServiceEventReceiver) {
        Self::start_with_generation_opener(database_url, connector, timing, Arc::new(SqlxGenerationOpener))
    }

    fn start_with_generation_opener(
        database_url: Url,
        connector: Arc<dyn DatabaseConnector>,
        timing: Timing,
        generation_opener: Arc<dyn GenerationOpener>,
    ) -> (Arc<Self>, StorageServiceEventReceiver) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (retirement_tx, retirement_rx) = retirement_channel();
        let (rebuild_tx, rebuild_rx) = rebuild_channel();
        let (status_tx, status_rx) = watch::channel(StorageServiceStatus { state: StorageServiceStatusState::Connecting });
        let (completion_tx, completion_rx) = watch::channel(None);
        let replacement_gate = ApiReplacementGate::new();
        #[cfg(test)]
        let retained_cleanup_tasks = Arc::new(AtomicUsize::new(0));
        #[cfg(test)]
        let panic_next_cleanup = Arc::new(AtomicBool::new(false));
        let worker = StorageServiceWorker {
            database_url,
            connector,
            timing,
            events: event_tx,
            commands: command_rx,
            retirements: retirement_rx,
            retirement_tx,
            rebuilds: rebuild_rx,
            rebuild_tx,
            status: status_tx,
            generation_opener,
            replacement_gate: replacement_gate.clone(),
            cleanup: JoinSet::new(),
            #[cfg(test)]
            retained_cleanup_tasks: retained_cleanup_tasks.clone(),
            #[cfg(test)]
            panic_next_cleanup: panic_next_cleanup.clone(),
        };
        let join = tokio::spawn(async move {
            let result = worker.run().await;
            completion_tx.send_replace(Some(result));
        });
        let service = Arc::new(Self {
            commands: command_tx,
            status: status_rx,
            completion: completion_rx,
            join: Mutex::new(Some(join)),
            #[cfg(test)]
            retained_cleanup_tasks,
            #[cfg(test)]
            panic_next_cleanup,
            #[cfg(test)]
            replacement_gate,
        });
        (service, event_rx)
    }

    /// Returns the latest lossy status observation.
    #[must_use]
    pub fn status(&self) -> StorageServiceStatus {
        *self.status.borrow()
    }

    /// Subscribes to latest-value status changes.
    #[must_use]
    pub fn subscribe_status(&self) -> watch::Receiver<StorageServiceStatus> {
        self.status.clone()
    }

    /// Atomically binds a never-initialized database, or verifies its existing binding.
    pub async fn initialize_if_uninitialized(&self, network_id: NetworkId, genesis_hash: BlockHash) -> Result<(), StorageError> {
        let (completion, acknowledgement) = oneshot::channel();
        self.commands
            .send(ServiceCommand::Initialize { network_id, genesis_hash, completion })
            .map_err(|_| StorageError::ControlUnavailable)?;
        acknowledgement.await.map_err(|_| StorageError::ControlUnavailable)?
    }

    /// Terminates the service and releases its pools and advisory-lock connection.
    pub async fn shutdown(&self) -> Result<(), StorageError> {
        if let Some(result) = self.completion.borrow().clone() {
            self.join_worker().await?;
            return result;
        }
        let (completion, acknowledgement) = oneshot::channel();
        if self.commands.send(ServiceCommand::Shutdown(completion)).is_ok() {
            let _ = acknowledgement.await;
        }
        let mut completion = self.completion.clone();
        loop {
            if let Some(result) = completion.borrow().clone() {
                self.join_worker().await?;
                return result;
            }
            if completion.changed().await.is_err() {
                self.join_worker().await?;
                return Err(StorageError::ControlUnavailable);
            }
        }
    }

    async fn join_worker(&self) -> Result<(), StorageError> {
        let Some(join) = self.join.lock().await.take() else {
            return Ok(());
        };
        join.await.map_err(|error| StorageError::WorkerFailed { diagnostic: Arc::from(error.to_string()) })
    }

    #[cfg(test)]
    fn retained_cleanup_tasks(&self) -> usize {
        self.retained_cleanup_tasks.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn panic_next_cleanup(&self) {
        self.panic_next_cleanup.store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn break_replacement_control(&self) {
        self.replacement_gate.break_control();
    }
}

enum ServiceCommand {
    Initialize { network_id: NetworkId, genesis_hash: BlockHash, completion: oneshot::Sender<Result<(), StorageError>> },
    Shutdown(oneshot::Sender<()>),
}

struct InitializationRequest {
    network_id: NetworkId,
    genesis_hash: BlockHash,
    completion: oneshot::Sender<Result<(), StorageError>>,
}

struct ActiveGenerations {
    binding: DatabaseBinding,
    processing: Option<Arc<ValidatedDbClient>>,
    api: Option<Arc<ValidatedApiDbClient>>,
    api_required: bool,
}

#[async_trait]
trait GenerationOpener: Send + Sync {
    async fn open_initial(
        &self,
        database_url: String,
        state: DatabaseState,
        retirement_tx: RetirementSender,
        rebuild_tx: RebuildSender,
        replacement_gate: Arc<ApiReplacementGate>,
    ) -> Result<Option<ValidatedGenerations>, StorageError> {
        open_validated_generations_managed(database_url, state, retirement_tx, rebuild_tx, replacement_gate).await
    }

    async fn open_processing(
        &self,
        database_url: String,
        binding: DatabaseBinding,
        retirement_tx: RetirementSender,
        rebuild_tx: RebuildSender,
    ) -> Result<Arc<ValidatedDbClient>, StorageError> {
        open_processing_generation_managed(database_url, binding, retirement_tx, rebuild_tx).await
    }

    async fn open_api(
        &self,
        database_url: String,
        binding: DatabaseBinding,
        retirement_tx: RetirementSender,
        replacement_gate: Arc<ApiReplacementGate>,
    ) -> Result<Arc<ValidatedApiDbClient>, StorageError> {
        open_api_generation(database_url, binding, retirement_tx, replacement_gate).await
    }
}

struct SqlxGenerationOpener;

#[async_trait]
impl GenerationOpener for SqlxGenerationOpener {}

struct StorageServiceWorker {
    database_url: Url,
    connector: Arc<dyn DatabaseConnector>,
    timing: Timing,
    events: mpsc::UnboundedSender<StorageServiceEvent>,
    commands: mpsc::UnboundedReceiver<ServiceCommand>,
    retirements: RetirementReceiver,
    retirement_tx: RetirementSender,
    rebuilds: RebuildReceiver,
    rebuild_tx: RebuildSender,
    status: watch::Sender<StorageServiceStatus>,
    generation_opener: Arc<dyn GenerationOpener>,
    replacement_gate: Arc<ApiReplacementGate>,
    cleanup: JoinSet<()>,
    #[cfg(test)]
    retained_cleanup_tasks: Arc<AtomicUsize>,
    #[cfg(test)]
    panic_next_cleanup: Arc<AtomicBool>,
}

impl StorageServiceWorker {
    async fn run(mut self) -> Result<(), StorageError> {
        let lifecycle_result = self.run_lifecycle().await;
        let cleanup_result = self.join_cleanup().await;
        lifecycle_result.and(cleanup_result)
    }

    async fn run_lifecycle(&mut self) -> Result<(), StorageError> {
        let mut retry_index = 0;
        let mut pending_initialization = VecDeque::new();
        'lifecycle: loop {
            self.reap_completed_cleanup()?;
            self.publish_status(StorageServiceStatusState::Connecting);
            let connector = self.connector.clone();
            let database_url = self.database_url.as_str().to_owned();
            let opening = async move {
                let locked = connector.connect(&database_url).await?;
                locked.prepare().await
            };
            tokio::pin!(opening);
            let (mut database, mut state) = loop {
                tokio::select! {
                    cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                    command = self.commands.recv() => match queue_or_shutdown(command, &mut pending_initialization) {
                        CommandDisposition::Continue => {}
                        CommandDisposition::Shutdown => return self.finish_without_generations().await,
                    },
                    request = self.retirements.recv() => complete_stale_retirement(request)?,
                    request = self.rebuilds.recv() => complete_stale_rebuild(request)?,
                    result = &mut opening => match result {
                        Ok(opened) => break opened,
                        Err(StorageError::Rejected(rejection)) => {
                            return self.reject_without_generations(rejection, &mut pending_initialization).await;
                        }
                        Err(_error) => {
                            self.publish_status(StorageServiceStatusState::Unavailable);
                            if self.wait_retry(&mut retry_index, &mut pending_initialization).await? {
                                continue 'lifecycle;
                            }
                            return Ok(());
                        }
                    }
                }
            };

            if matches!(state, DatabaseState::Uninitialized) {
                self.publish_status(StorageServiceStatusState::AwaitingInitialization);
                let request = loop {
                    if let Some(request) = pending_initialization.pop_front() {
                        break request;
                    }
                    tokio::select! {
                        cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                        command = self.commands.recv() => match command {
                            Some(ServiceCommand::Initialize { network_id, genesis_hash, completion }) => {
                                break InitializationRequest { network_id, genesis_hash, completion };
                            }
                            Some(ServiceCommand::Shutdown(completion)) => {
                                let _ = completion.send(());
                                return self.finish_without_generations().await;
                            }
                            None => return self.finish_without_generations().await,
                        },
                        request = self.retirements.recv() => complete_stale_retirement(request)?,
                        request = self.rebuilds.recv() => complete_stale_rebuild(request)?,
                    }
                };
                let result = database.initialize_if_uninitialized(request.network_id, request.genesis_hash, None).await;
                match result {
                    Ok(initialized) => {
                        state = initialized;
                        let _ = request.completion.send(Ok(()));
                    }
                    Err(StorageError::Rejected(rejection)) => {
                        let error = StorageError::Rejected(rejection.clone());
                        let _ = request.completion.send(Err(error));
                        return self.reject_without_generations(rejection, &mut pending_initialization).await;
                    }
                    Err(error) => {
                        let _ = request.completion.send(Err(error));
                        self.publish_status(StorageServiceStatusState::Unavailable);
                        if self.wait_retry(&mut retry_index, &mut pending_initialization).await? {
                            continue;
                        }
                        return Ok(());
                    }
                }
            }

            match self.verify_pending_initializations(&mut database, &mut pending_initialization).await {
                Ok(()) => {}
                Err(StorageError::Rejected(rejection)) => {
                    return self.reject_without_generations(rejection, &mut pending_initialization).await;
                }
                Err(_) => {
                    self.publish_status(StorageServiceStatusState::Unavailable);
                    if self.wait_retry(&mut retry_index, &mut pending_initialization).await? {
                        continue;
                    }
                    return Ok(());
                }
            }

            let generations = match self
                .generation_opener
                .open_initial(
                    self.database_url.as_str().to_owned(),
                    state,
                    self.retirement_tx.clone(),
                    self.rebuild_tx.clone(),
                    self.replacement_gate.clone(),
                )
                .await
            {
                Ok(Some(generations)) => generations,
                Ok(None) => continue,
                Err(StorageError::Rejected(rejection)) => {
                    return self.reject_without_generations(rejection, &mut pending_initialization).await;
                }
                Err(_error) => {
                    self.publish_status(StorageServiceStatusState::Unavailable);
                    if self.wait_retry(&mut retry_index, &mut pending_initialization).await? {
                        continue;
                    }
                    return Ok(());
                }
            };
            let (processing, api) = generations.into_parts();
            if database.ping().await.is_err() {
                self.discard_unpublished(processing, api);
                self.publish_status(StorageServiceStatusState::Unavailable);
                if self.wait_retry(&mut retry_index, &mut pending_initialization).await? {
                    continue;
                }
                return Ok(());
            }
            let mut active =
                ActiveGenerations { binding: processing.binding(), api_required: api.is_some(), processing: Some(processing), api };
            if let Err(error) = self.publish_initial(&active).await {
                let _ = self.retire_all(&mut active).await;
                return Err(error);
            }
            self.publish_status(StorageServiceStatusState::Ready);

            let active_exit = match self.run_active(&mut database, &mut active, &mut retry_index).await {
                Ok(exit) => exit,
                Err(error) => {
                    let _ = self.retire_all(&mut active).await;
                    return Err(error);
                }
            };
            match active_exit {
                ActiveExit::Reconnect => {
                    self.publish_status(StorageServiceStatusState::Unavailable);
                    if self.wait_retry(&mut retry_index, &mut pending_initialization).await? {
                        continue;
                    }
                    return Ok(());
                }
                ActiveExit::Rejected(rejection) => return self.reject_active(&mut active, rejection).await,
                ActiveExit::Stopped => return Ok(()),
            }
        }
    }

    async fn run_active(
        &mut self,
        database: &mut PreparedDatabase,
        active: &mut ActiveGenerations,
        retry_index: &mut usize,
    ) -> Result<ActiveExit, StorageError> {
        loop {
            if active.processing.is_none() {
                self.publish_status(StorageServiceStatusState::Unavailable);
                let database_url = self.database_url.as_str().to_owned();
                let generation_opener = self.generation_opener.clone();
                let retirement_tx = self.retirement_tx.clone();
                let rebuild_tx = self.rebuild_tx.clone();
                let binding = active.binding;
                let opening = async move { generation_opener.open_processing(database_url, binding, retirement_tx, rebuild_tx).await };
                tokio::pin!(opening);
                let health = wait_for_lock_health(self.timing.clone());
                tokio::pin!(health);
                loop {
                    tokio::select! {
                        cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                        result = &mut opening => match result {
                            Ok(client) => {
                                if database.ping().await.is_err() {
                                    client.retire();
                                    self.track_processing_cleanup(client);
                                    self.handle_lock_loss(active).await?;
                                    return Ok(ActiveExit::Reconnect);
                                }
                                self.send_event(StorageServiceEvent::ProcessingDbPublished(client.clone()))?;
                                active.processing = Some(client);
                                break;
                            }
                            Err(_) => {
                                match self.wait_active_retry(database, active, retry_index).await? {
                                    ActiveRetryExit::Retry => break,
                                    ActiveRetryExit::Exit(exit) => return Ok(exit),
                                }
                            }
                        },
                        () = &mut health => {
                            if database.ping().await.is_err() {
                                self.handle_lock_loss(active).await?;
                                return Ok(ActiveExit::Reconnect);
                            }
                            health.set(wait_for_lock_health(self.timing.clone()));
                        },
                        command = self.commands.recv() => {
                            if let Some(exit) = self.handle_active_command(command, database, active).await? {
                                return Ok(exit);
                            }
                            health.set(wait_for_lock_health(self.timing.clone()));
                        },
                        request = self.retirements.recv() => {
                            self.handle_retirement(request, active).await?;
                            health.set(wait_for_lock_health(self.timing.clone()));
                        },
                        request = self.rebuilds.recv() => complete_stale_rebuild(request)?,
                    }
                }
                continue;
            }

            if active.api_required && active.api.is_none() {
                self.publish_status(StorageServiceStatusState::Unavailable);
                let database_url = self.database_url.as_str().to_owned();
                let generation_opener = self.generation_opener.clone();
                let binding = active.binding;
                let retirement_tx = self.retirement_tx.clone();
                let replacement_gate = self.replacement_gate.clone();
                let opening = async move { generation_opener.open_api(database_url, binding, retirement_tx, replacement_gate).await };
                tokio::pin!(opening);
                let health = wait_for_lock_health(self.timing.clone());
                tokio::pin!(health);
                loop {
                    tokio::select! {
                        cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                        result = &mut opening => match result {
                            Ok(client) => {
                                if database.ping().await.is_err() {
                                    client.retire();
                                    self.track_api_cleanup(client);
                                    self.handle_lock_loss(active).await?;
                                    return Ok(ActiveExit::Reconnect);
                                }
                                self.send_event(StorageServiceEvent::ApiDbPublished(client.clone()))?;
                                active.api = Some(client);
                                break;
                            }
                            Err(_) => {
                                match self.wait_active_retry(database, active, retry_index).await? {
                                    ActiveRetryExit::Retry => break,
                                    ActiveRetryExit::Exit(exit) => return Ok(exit),
                                }
                            }
                        },
                        () = &mut health => {
                            if database.ping().await.is_err() {
                                self.handle_lock_loss(active).await?;
                                return Ok(ActiveExit::Reconnect);
                            }
                            health.set(wait_for_lock_health(self.timing.clone()));
                        },
                        command = self.commands.recv() => {
                            if let Some(exit) = self.handle_active_command(command, database, active).await? {
                                return Ok(exit);
                            }
                            health.set(wait_for_lock_health(self.timing.clone()));
                        },
                        request = self.retirements.recv() => {
                            self.handle_retirement(request, active).await?;
                            health.set(wait_for_lock_health(self.timing.clone()));
                        },
                        request = self.rebuilds.recv() => {
                            if let Some(exit) = self.handle_rebuild(request, database, active).await? {
                                return Ok(exit);
                            }
                            break;
                        },
                    }
                }
                continue;
            }

            self.publish_status(StorageServiceStatusState::Ready);
            let ready_timing = self.timing.clone();
            let ready_reset = async move { ready_timing.sleep(READY_BACKOFF_RESET).await };
            tokio::pin!(ready_reset);
            let health = wait_for_lock_health(self.timing.clone());
            tokio::pin!(health);
            let mut reset_complete = false;
            loop {
                tokio::select! {
                    cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                    () = &mut ready_reset, if !reset_complete => {
                        *retry_index = 0;
                        reset_complete = true;
                        health.set(wait_for_lock_health(self.timing.clone()));
                    }
                    () = &mut health => {
                        if database.ping().await.is_err() {
                            self.handle_lock_loss(active).await?;
                            return Ok(ActiveExit::Reconnect);
                        }
                        health.set(wait_for_lock_health(self.timing.clone()));
                    }
                    command = self.commands.recv() => {
                        if let Some(exit) = self.handle_active_command(command, database, active).await? {
                            return Ok(exit);
                        }
                        health.set(wait_for_lock_health(self.timing.clone()));
                    },
                    request = self.retirements.recv() => {
                        self.handle_retirement(request, active).await?;
                        if active.processing.is_none() || (active.api_required && active.api.is_none()) {
                            break;
                        }
                        health.set(wait_for_lock_health(self.timing.clone()));
                    }
                    request = self.rebuilds.recv() => {
                        if let Some(exit) = self.handle_rebuild(request, database, active).await? {
                            return Ok(exit);
                        }
                        if active.processing.is_none() || (active.api_required && active.api.is_none()) {
                            break;
                        }
                        health.set(wait_for_lock_health(self.timing.clone()));
                    }
                }
            }
        }
    }

    async fn handle_rebuild(
        &mut self,
        request: Option<RebuildRequest>,
        database: &mut PreparedDatabase,
        active: &mut ActiveGenerations,
    ) -> Result<Option<ActiveExit>, StorageError> {
        let request = request.ok_or(StorageError::ControlUnavailable)?;
        let Some(processing) = active.processing.as_ref().filter(|processing| request.targets(processing)).cloned() else {
            request.complete(Err(StorageError::GenerationLost));
            return Ok(None);
        };
        let (pruning_point, mut completion) = request.into_parts();
        if completion.is_closed() {
            return Ok(None);
        }

        let replacement = match self.replacement_gate.begin_replacement() {
            Ok(replacement) => replacement,
            Err(error) => {
                self.retire_api(active).await?;
                let _ = completion.send(Err(error));
                return Ok(None);
            }
        };
        active.api_required = false;
        self.retire_api(active).await?;

        let replacement_timing = self.timing.clone();
        let exclusive_wait = replacement.acquire_exclusive(&replacement_timing);
        tokio::pin!(exclusive_wait);
        let mut exclusive_result = None;

        let database_url = self.database_url.as_str().to_owned();
        let generation_opener = self.generation_opener.clone();
        let binding = active.binding;
        let retirement_tx = self.retirement_tx.clone();
        let replacement_gate = self.replacement_gate.clone();
        let opening = async move { generation_opener.open_api(database_url, binding, retirement_tx, replacement_gate).await };
        tokio::pin!(opening);
        let health = wait_for_lock_health(self.timing.clone());
        tokio::pin!(health);
        let fresh = loop {
            tokio::select! {
                cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                result = &mut opening => match result {
                    Ok(client) => break client,
                    Err(_) => {
                        let _ = completion.send(Err(StorageError::RebuildSetupFailed(
                            crate::error::RebuildSetupFault::ApiGenerationPreparation,
                        )));
                        return Ok(None);
                    }
                },
                result = &mut exclusive_wait, if exclusive_result.is_none() => match result {
                    Err(error @ StorageError::ReplacementControlUnavailable { .. }) => {
                        let _ = completion.send(Err(error));
                        return Ok(None);
                    }
                    result => exclusive_result = Some(result),
                },
                () = &mut health => {
                    if database.ping().await.is_err() {
                        self.handle_lock_loss(active).await?;
                        let _ = completion.send(Err(StorageError::GenerationLost));
                        return Ok(Some(ActiveExit::Reconnect));
                    }
                    health.set(wait_for_lock_health(self.timing.clone()));
                },
                command = self.commands.recv() => {
                    if let Some(exit) = self.handle_active_command(command, database, active).await? {
                        let _ = completion.send(Err(StorageError::GenerationLost));
                        return Ok(Some(exit));
                    }
                    health.set(wait_for_lock_health(self.timing.clone()));
                },
                retirement = self.retirements.recv() => {
                    self.handle_retirement(retirement, active).await?;
                    if !active.processing.as_ref().is_some_and(|current| Arc::ptr_eq(current, &processing)) {
                        let _ = completion.send(Err(StorageError::GenerationLost));
                        return Ok(None);
                    }
                    health.set(wait_for_lock_health(self.timing.clone()));
                },
                () = completion.closed() => return Ok(None),
            }
        };
        if database.ping().await.is_err() {
            fresh.retire();
            self.track_api_cleanup(fresh);
            self.handle_lock_loss(active).await?;
            let _ = completion.send(Err(StorageError::GenerationLost));
            return Ok(Some(ActiveExit::Reconnect));
        }
        if let Err(error) = replacement.ensure_current() {
            fresh.retire();
            self.track_api_cleanup(fresh);
            let _ = completion.send(Err(error));
            return Ok(None);
        }
        if let Err(error) = self.send_event(StorageServiceEvent::ApiDbPublished(fresh.clone())) {
            fresh.retire();
            self.track_api_cleanup(fresh);
            let _ = completion.send(Err(error.clone()));
            return Err(error);
        }
        active.api = Some(fresh.clone());

        let exclusive = match exclusive_result {
            Some(Ok(exclusive)) => exclusive,
            Some(Err(error @ StorageError::RebuildSetupFailed(_))) => {
                self.retire_api(active).await?;
                let _ = completion.send(Err(error));
                return Ok(None);
            }
            Some(Err(error)) => {
                self.retire_api(active).await?;
                let _ = completion.send(Err(error));
                return Ok(None);
            }
            None => {
                let health = wait_for_lock_health(self.timing.clone());
                tokio::pin!(health);
                loop {
                    tokio::select! {
                        cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                        result = &mut exclusive_wait => match result {
                            Ok(exclusive) => break exclusive,
                            Err(error @ StorageError::RebuildSetupFailed(_)) => {
                                self.retire_api(active).await?;
                                let _ = completion.send(Err(error));
                                return Ok(None);
                            }
                            Err(error) => {
                                self.retire_api(active).await?;
                                let _ = completion.send(Err(error));
                                return Ok(None);
                            }
                        },
                        () = &mut health => {
                            if database.ping().await.is_err() {
                                self.handle_lock_loss(active).await?;
                                let _ = completion.send(Err(StorageError::GenerationLost));
                                return Ok(Some(ActiveExit::Reconnect));
                            }
                            health.set(wait_for_lock_health(self.timing.clone()));
                        },
                        command = self.commands.recv() => {
                            if let Some(exit) = self.handle_active_command(command, database, active).await? {
                                let _ = completion.send(Err(StorageError::GenerationLost));
                                return Ok(Some(exit));
                            }
                            health.set(wait_for_lock_health(self.timing.clone()));
                        },
                        retirement = self.retirements.recv() => {
                            self.handle_retirement(retirement, active).await?;
                            let processing_current = active.processing.as_ref().is_some_and(|current| Arc::ptr_eq(current, &processing));
                            let api_current = active.api.as_ref().is_some_and(|current| Arc::ptr_eq(current, &fresh));
                            if !processing_current || !api_current {
                                self.retire_api(active).await?;
                                let _ = completion.send(Err(StorageError::GenerationLost));
                                return Ok(None);
                            }
                            health.set(wait_for_lock_health(self.timing.clone()));
                        },
                        () = completion.closed() => {
                            self.retire_api(active).await?;
                            return Ok(None);
                        },
                    }
                }
            }
        };

        if completion.is_closed() {
            self.retire_api(active).await?;
            return Ok(None);
        }
        if database.ping().await.is_err() {
            self.handle_lock_loss(active).await?;
            let _ = completion.send(Err(StorageError::GenerationLost));
            return Ok(Some(ActiveExit::Reconnect));
        }
        if let Err(error) = exclusive.ensure_current() {
            self.retire_api(active).await?;
            let _ = completion.send(Err(error));
            return Ok(None);
        }

        let rebuild_client = processing.clone();
        let rebuild = async move { rebuild_client.perform_rebuild(pruning_point).await };
        tokio::pin!(rebuild);
        let health = wait_for_lock_health(self.timing.clone());
        tokio::pin!(health);
        let mut ownership_lost = false;
        let mut generation_changed = false;
        let mut forced_exit = None;
        let result = loop {
            tokio::select! {
                cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                result = &mut rebuild => break result,
                () = &mut health, if !ownership_lost => {
                    if database.ping().await.is_err() {
                        ownership_lost = true;
                        self.handle_lock_loss(active).await?;
                    } else {
                        health.set(wait_for_lock_health(self.timing.clone()));
                    }
                },
                command = self.commands.recv(), if forced_exit.is_none() => {
                    if let Some(exit) = self.handle_active_command(command, database, active).await? {
                        forced_exit = Some(exit);
                        ownership_lost = true;
                    } else {
                        health.set(wait_for_lock_health(self.timing.clone()));
                    }
                },
                retirement = self.retirements.recv(), if forced_exit.is_none() => {
                    self.handle_retirement(retirement, active).await?;
                    let processing_current = active.processing.as_ref().is_some_and(|current| Arc::ptr_eq(current, &processing));
                    let api_current = active.api.as_ref().is_some_and(|current| Arc::ptr_eq(current, &fresh));
                    if !processing_current || !api_current {
                        generation_changed = true;
                        self.retire_api(active).await?;
                    }
                    health.set(wait_for_lock_health(self.timing.clone()));
                },
            }
        };
        if forced_exit.is_none() && !ownership_lost && database.ping().await.is_err() {
            ownership_lost = true;
            self.handle_lock_loss(active).await?;
        }

        if let Some(exit) = forced_exit {
            let reported = rebuild_result_after_retirement(result);
            let _ = completion.send(reported);
            return Ok(Some(exit));
        }
        if ownership_lost {
            let reported = rebuild_result_after_retirement(result);
            let _ = completion.send(reported);
            return Ok(Some(ActiveExit::Reconnect));
        }
        if generation_changed {
            let reported = rebuild_result_after_retirement(result);
            let _ = completion.send(reported);
            return Ok(None);
        }

        match result {
            Ok(anchor) => {
                if let Err(error) = exclusive.open() {
                    self.retire_api(active).await?;
                    let _ = completion.send(Err(error));
                    return Ok(None);
                }
                active.api_required = true;
                let _ = completion.send(Ok(anchor));
                Ok(None)
            }
            Err(error @ StorageError::Persistence(PersistenceFault::AmbiguousCommit)) => {
                self.retire_api(active).await?;
                self.retire_processing(active).await?;
                let _ = completion.send(Err(error));
                Ok(Some(ActiveExit::Reconnect))
            }
            Err(error) if error.is_connection_lost() => {
                self.retire_api(active).await?;
                self.retire_processing(active).await?;
                let _ = completion.send(Err(StorageError::GenerationLost));
                Ok(Some(ActiveExit::Reconnect))
            }
            Err(error) => {
                self.retire_api(active).await?;
                let _ = completion.send(Err(error));
                Ok(None)
            }
        }
    }

    async fn handle_active_command(
        &mut self,
        command: Option<ServiceCommand>,
        database: &mut PreparedDatabase,
        active: &mut ActiveGenerations,
    ) -> Result<Option<ActiveExit>, StorageError> {
        match command {
            Some(ServiceCommand::Initialize { network_id, genesis_hash, completion }) => {
                let result = database.initialize_if_uninitialized(network_id, genesis_hash, None).await.map(|_| ());
                let rejection = match &result {
                    Err(StorageError::Rejected(rejection)) => Some(rejection.clone()),
                    _ => None,
                };
                let reconnect = result.as_ref().is_err_and(|error| error.is_connection_lost());
                let _ = completion.send(result);
                if let Some(rejection) = rejection {
                    return Ok(Some(ActiveExit::Rejected(rejection)));
                }
                if reconnect {
                    self.retire_all(active).await?;
                    return Ok(Some(ActiveExit::Reconnect));
                }
                Ok(None)
            }
            Some(ServiceCommand::Shutdown(completion)) => {
                self.retire_all(active).await?;
                self.publish_status(StorageServiceStatusState::Stopped);
                let _ = completion.send(());
                Ok(Some(ActiveExit::Stopped))
            }
            None => {
                self.retire_all(active).await?;
                self.publish_status(StorageServiceStatusState::Stopped);
                Ok(Some(ActiveExit::Stopped))
            }
        }
    }

    async fn handle_retirement(
        &mut self,
        request: Option<RetirementRequest>,
        active: &mut ActiveGenerations,
    ) -> Result<(), StorageError> {
        let request = request.ok_or(StorageError::ControlUnavailable)?;
        match request.target() {
            RetirementTarget::Processing(reported) => {
                let targeted = active
                    .processing
                    .as_ref()
                    .is_some_and(|current| reported.upgrade().is_some_and(|reported| Arc::ptr_eq(&reported, current)));
                if targeted {
                    let client = active.processing.take().expect("targeted processing generation exists");
                    if client.retire() {
                        self.track_processing_cleanup(client.clone());
                        let event_result = self.send_event(StorageServiceEvent::ProcessingDbRetired(client.clone()));
                        request.complete(event_result.clone());
                        return event_result;
                    }
                }
            }
            RetirementTarget::Api(reported) => {
                let targeted = active
                    .api
                    .as_ref()
                    .is_some_and(|current| reported.upgrade().is_some_and(|reported| Arc::ptr_eq(&reported, current)));
                if targeted {
                    let client = active.api.take().expect("targeted API generation exists");
                    if client.retire() {
                        self.track_api_cleanup(client.clone());
                        let event_result = self.send_event(StorageServiceEvent::ApiDbRetired(client.clone()));
                        request.complete(event_result.clone());
                        return event_result;
                    }
                }
            }
        }
        request.complete(Ok(()));
        Ok(())
    }

    async fn wait_active_retry(
        &mut self,
        database: &mut PreparedDatabase,
        active: &mut ActiveGenerations,
        retry_index: &mut usize,
    ) -> Result<ActiveRetryExit, StorageError> {
        let nominal = RETRY_DELAYS[(*retry_index).min(RETRY_DELAYS.len() - 1)];
        *retry_index = retry_index.saturating_add(1);
        let timing = self.timing.clone();
        let sleep = async move { timing.sleep_jittered(nominal).await };
        tokio::pin!(sleep);
        let health = wait_for_lock_health(self.timing.clone());
        tokio::pin!(health);
        loop {
            tokio::select! {
                cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                () = &mut sleep => return Ok(ActiveRetryExit::Retry),
                () = &mut health => {
                    if database.ping().await.is_err() {
                        self.handle_lock_loss(active).await?;
                        return Ok(ActiveRetryExit::Exit(ActiveExit::Reconnect));
                    }
                    health.set(wait_for_lock_health(self.timing.clone()));
                },
                command = self.commands.recv() => {
                    if let Some(exit) = self.handle_active_command(command, database, active).await? {
                        return Ok(ActiveRetryExit::Exit(exit));
                    }
                    health.set(wait_for_lock_health(self.timing.clone()));
                },
                request = self.retirements.recv() => {
                    self.handle_retirement(request, active).await?;
                    health.set(wait_for_lock_health(self.timing.clone()));
                },
                request = self.rebuilds.recv() => {
                    return match self.handle_rebuild(request, database, active).await? {
                        Some(exit) => Ok(ActiveRetryExit::Exit(exit)),
                        None => Ok(ActiveRetryExit::Retry),
                    };
                },
            }
        }
    }

    async fn handle_lock_loss(&mut self, active: &mut ActiveGenerations) -> Result<(), StorageError> {
        self.retire_all(active).await?;
        self.publish_status(StorageServiceStatusState::Unavailable);
        Ok(())
    }

    async fn wait_retry(
        &mut self,
        retry_index: &mut usize,
        pending: &mut VecDeque<InitializationRequest>,
    ) -> Result<bool, StorageError> {
        let nominal = RETRY_DELAYS[(*retry_index).min(RETRY_DELAYS.len() - 1)];
        *retry_index = retry_index.saturating_add(1);
        let sleep = self.timing.sleep_jittered(nominal);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                () = &mut sleep => return Ok(true),
                command = self.commands.recv() => match queue_or_shutdown(command, pending) {
                    CommandDisposition::Continue => {}
                    CommandDisposition::Shutdown => {
                        self.publish_status(StorageServiceStatusState::Stopped);
                        return Ok(false);
                    }
                },
                request = self.retirements.recv() => complete_stale_retirement(request)?,
                request = self.rebuilds.recv() => complete_stale_rebuild(request)?,
            }
        }
    }

    async fn verify_pending_initializations(
        &self,
        database: &mut PreparedDatabase,
        pending: &mut VecDeque<InitializationRequest>,
    ) -> Result<(), StorageError> {
        while let Some(request) = pending.pop_front() {
            let result = database.initialize_if_uninitialized(request.network_id, request.genesis_hash, None).await.map(|_| ());
            match result {
                Ok(()) => {
                    let _ = request.completion.send(Ok(()));
                }
                Err(error) => {
                    let _ = request.completion.send(Err(error.clone()));
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    async fn publish_initial(&self, active: &ActiveGenerations) -> Result<(), StorageError> {
        let processing = active.processing.as_ref().expect("initial processing generation").clone();
        self.send_event(StorageServiceEvent::ProcessingDbPublished(processing))?;
        if let Some(api) = &active.api {
            self.send_event(StorageServiceEvent::ApiDbPublished(api.clone()))?;
        }
        Ok(())
    }

    async fn retire_all(&mut self, active: &mut ActiveGenerations) -> Result<(), StorageError> {
        let mut result = Ok(());
        let processing = active.processing.take();
        let api = active.api.take();
        if let Some(processing) = &processing
            && processing.retire()
        {
            self.track_processing_cleanup(processing.clone());
            if let Err(error) = self.send_event(StorageServiceEvent::ProcessingDbRetired(processing.clone())) {
                result = Err(error);
            }
        }
        if let Some(api) = &api
            && api.retire()
        {
            self.track_api_cleanup(api.clone());
            if let Err(error) = self.send_event(StorageServiceEvent::ApiDbRetired(api.clone()))
                && result.is_ok()
            {
                result = Err(error);
            }
        }
        result
    }

    async fn retire_processing(&mut self, active: &mut ActiveGenerations) -> Result<(), StorageError> {
        let Some(processing) = active.processing.take() else {
            return Ok(());
        };
        if processing.retire() {
            self.track_processing_cleanup(processing.clone());
            self.send_event(StorageServiceEvent::ProcessingDbRetired(processing))?;
        }
        Ok(())
    }

    async fn retire_api(&mut self, active: &mut ActiveGenerations) -> Result<(), StorageError> {
        let Some(api) = active.api.take() else {
            return Ok(());
        };
        if api.retire() {
            self.track_api_cleanup(api.clone());
            self.send_event(StorageServiceEvent::ApiDbRetired(api))?;
        }
        Ok(())
    }

    fn track_processing_cleanup(&mut self, client: Arc<ValidatedDbClient>) {
        client.begin_close();
        self.track_cleanup(async move { client.close().await });
    }

    fn track_api_cleanup(&mut self, client: Arc<ValidatedApiDbClient>) {
        client.begin_close();
        self.track_cleanup(async move { client.close().await });
    }

    fn track_cleanup(&mut self, cleanup: impl Future<Output = ()> + Send + 'static) {
        #[cfg(test)]
        self.retained_cleanup_tasks.fetch_add(1, Ordering::AcqRel);
        #[cfg(test)]
        let panic_after_cleanup = self.panic_next_cleanup.swap(false, Ordering::AcqRel);
        self.cleanup.spawn(async move {
            cleanup.await;
            #[cfg(test)]
            assert!(!panic_after_cleanup, "injected cleanup task failure");
        });
    }

    fn discard_unpublished(&mut self, processing: Arc<ValidatedDbClient>, api: Option<Arc<ValidatedApiDbClient>>) {
        processing.retire();
        self.track_processing_cleanup(processing);
        if let Some(api) = api {
            api.retire();
            self.track_api_cleanup(api);
        }
    }

    async fn join_cleanup(&mut self) -> Result<(), StorageError> {
        let mut failure = None;
        while let Some(result) = self.cleanup.join_next().await {
            if let Err(error) = self.finish_cleanup(result)
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn reap_completed_cleanup(&mut self) -> Result<(), StorageError> {
        while let Some(result) = self.cleanup.try_join_next() {
            self.finish_cleanup(result)?;
        }
        Ok(())
    }

    fn complete_cleanup(&self, result: Option<Result<(), tokio::task::JoinError>>) -> Result<(), StorageError> {
        let result =
            result.ok_or_else(|| StorageError::WorkerFailed { diagnostic: Arc::from("cleanup task set closed unexpectedly") })?;
        self.finish_cleanup(result)
    }

    fn finish_cleanup(&self, result: Result<(), tokio::task::JoinError>) -> Result<(), StorageError> {
        #[cfg(test)]
        self.retained_cleanup_tasks.fetch_sub(1, Ordering::AcqRel);
        result.map_err(|error| StorageError::WorkerFailed { diagnostic: Arc::from(error.to_string()) })
    }

    async fn reject_active(&mut self, active: &mut ActiveGenerations, rejection: StorageRejection) -> Result<(), StorageError> {
        self.retire_all(active).await?;
        self.publish_status(StorageServiceStatusState::Rejected);
        self.send_event(StorageServiceEvent::Rejected(rejection.clone()))?;
        self.wait_rejected_shutdown(rejection).await
    }

    async fn reject_without_generations(
        &mut self,
        rejection: StorageRejection,
        pending: &mut VecDeque<InitializationRequest>,
    ) -> Result<(), StorageError> {
        while let Some(request) = pending.pop_front() {
            let _ = request.completion.send(Err(StorageError::Rejected(rejection.clone())));
        }
        self.publish_status(StorageServiceStatusState::Rejected);
        self.send_event(StorageServiceEvent::Rejected(rejection.clone()))?;
        self.wait_rejected_shutdown(rejection).await
    }

    async fn wait_rejected_shutdown(&mut self, rejection: StorageRejection) -> Result<(), StorageError> {
        loop {
            tokio::select! {
                cleanup = self.cleanup.join_next(), if !self.cleanup.is_empty() => self.complete_cleanup(cleanup)?,
                command = self.commands.recv() => match command {
                    Some(ServiceCommand::Shutdown(completion)) => {
                        self.publish_status(StorageServiceStatusState::Stopped);
                        let _ = completion.send(());
                        return Ok(());
                    }
                    Some(ServiceCommand::Initialize { completion, .. }) => {
                        let _ = completion.send(Err(StorageError::Rejected(rejection.clone())));
                    }
                    None => {
                        self.publish_status(StorageServiceStatusState::Stopped);
                        return Ok(());
                    }
                },
                request = self.retirements.recv() => complete_stale_retirement(request)?,
                request = self.rebuilds.recv() => complete_stale_rebuild(request)?,
            }
        }
    }

    async fn finish_without_generations(&mut self) -> Result<(), StorageError> {
        self.publish_status(StorageServiceStatusState::Stopped);
        Ok(())
    }

    fn publish_status(&self, state: StorageServiceStatusState) {
        self.status.send_replace(StorageServiceStatus { state });
    }

    fn send_event(&self, event: StorageServiceEvent) -> Result<(), StorageError> {
        self.events.send(event).map_err(|_| StorageError::EventPathClosed)
    }
}

enum ActiveExit {
    Reconnect,
    Rejected(StorageRejection),
    Stopped,
}

enum ActiveRetryExit {
    Retry,
    Exit(ActiveExit),
}

enum CommandDisposition {
    Continue,
    Shutdown,
}

fn queue_or_shutdown(command: Option<ServiceCommand>, pending: &mut VecDeque<InitializationRequest>) -> CommandDisposition {
    match command {
        Some(ServiceCommand::Initialize { network_id, genesis_hash, completion }) => {
            pending.push_back(InitializationRequest { network_id, genesis_hash, completion });
            CommandDisposition::Continue
        }
        Some(ServiceCommand::Shutdown(completion)) => {
            let _ = completion.send(());
            CommandDisposition::Shutdown
        }
        None => CommandDisposition::Shutdown,
    }
}

fn complete_stale_retirement(request: Option<RetirementRequest>) -> Result<(), StorageError> {
    let request = request.ok_or(StorageError::ControlUnavailable)?;
    request.complete(Ok(()));
    Ok(())
}

fn complete_stale_rebuild(request: Option<RebuildRequest>) -> Result<(), StorageError> {
    let request = request.ok_or(StorageError::ControlUnavailable)?;
    request.complete(Err(StorageError::GenerationLost));
    Ok(())
}

fn rebuild_result_after_retirement(
    result: Result<MaterializedSyncAnchor, StorageError>,
) -> Result<MaterializedSyncAnchor, StorageError> {
    match result {
        Err(error) if error.is_connection_lost() => Err(StorageError::GenerationLost),
        result => result,
    }
}

async fn wait_for_lock_health(timing: Timing) {
    timing.sleep(LOCK_HEALTH_INTERVAL).await;
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use kaspa_consensus_core::{
        blockhash::ORIGIN,
        network::{NetworkId, NetworkType},
    };
    use kgi_core::timing::{Clock, Jitter, Timing};
    use kgi_model::{
        block::{BlockColor, BlockHash, BlockPresence, MAX_DAA_SCORE, ValidatedNodeBlock},
        lifecycle::{PersistenceFault, StorageServiceStatusState},
    };
    use sqlx::{Connection, PgConnection};
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    };
    use tokio::{
        sync::{Semaphore, mpsc},
        time::timeout,
    };
    use url::Url;

    use super::{
        GenerationOpener, LOCK_HEALTH_INTERVAL, READY_BACKOFF_RESET, StorageService, StorageServiceEvent, StorageServiceEventReceiver,
    };
    use crate::{
        database::{
            DatabaseConnector, LockedDatabase, SqlxDatabaseConnector, ValidatedGenerations, open_api_generation,
            open_validated_generations_managed,
        },
        error::{ProcessingDbError, RebuildSetupFault, StorageError, StorageRejection},
        generation::{DatabaseBinding, ValidatedApiDbClient, ValidatedDbClient},
        operation::RebuildFromPruningPointError,
        replacement::ApiReplacementGate,
        runtime::{RebuildSender, RetirementSender},
        state::DatabaseState,
    };

    const POSTGRES_PORT: u16 = 5432;
    const TEST_TIMEOUT: Duration = Duration::from_secs(15);

    async fn fixture() -> (ContainerAsync<Postgres>, Url) {
        let container = Postgres::default().with_tag("17-alpine").start().await.expect("PostgreSQL fixture must start");
        let host = container.get_host().await.expect("fixture host must resolve");
        let port = container.get_host_port_ipv4(POSTGRES_PORT).await.expect("fixture PostgreSQL port must resolve");
        let database_url =
            Url::parse(&format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable")).expect("fixture URL");
        (container, database_url)
    }

    fn mainnet() -> NetworkId {
        NetworkId::new(NetworkType::Mainnet)
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn genesis_pruning_point() -> ValidatedNodeBlock {
        ValidatedNodeBlock {
            hash: hash(1),
            selected_parent: ORIGIN,
            direct_parents: Vec::new(),
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: 0,
            daa_score: 0,
            blue_score: 0,
            blue_work: Default::default(),
        }
    }

    async fn wait_for_status(service: &StorageService, expected: StorageServiceStatusState) {
        let mut status = service.subscribe_status();
        timeout(TEST_TIMEOUT, async {
            loop {
                if status.borrow().state == expected {
                    return;
                }
                status.changed().await.expect("service must remain observable");
            }
        })
        .await
        .expect("status transition must complete");
    }

    async fn next_event(events: &mut StorageServiceEventReceiver) -> StorageServiceEvent {
        timeout(TEST_TIMEOUT, events.recv()).await.expect("storage event must arrive").expect("storage event path must remain open")
    }

    async fn wait_for_cleanup_tasks(service: &StorageService, expected: usize) {
        timeout(TEST_TIMEOUT, async {
            while service.retained_cleanup_tasks() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup task count must converge");
    }

    async fn initialize_service(
        database_url: Url,
    ) -> (Arc<StorageService>, StorageServiceEventReceiver, Arc<ValidatedDbClient>, Arc<ValidatedApiDbClient>) {
        let (service, mut events) = StorageService::start(database_url);
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        service.initialize_if_uninitialized(mainnet(), hash(1)).await.expect("database initialization");

        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected initial processing publication, got {event:?}"),
        };
        let api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected initial API publication, got {event:?}"),
        };
        wait_for_status(&service, StorageServiceStatusState::Ready).await;
        (service, events, processing, api)
    }

    #[tokio::test]
    async fn initialization_publishes_ordered_generations_and_shutdown_retires_exact_arcs() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;

        assert_eq!(processing.binding().network_id(), mainnet());
        assert_eq!(processing.binding().genesis_hash(), hash(1));
        assert!(processing.is_valid());
        assert!(api.is_valid());

        service.shutdown().await.expect("service shutdown");
        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected processing retirement, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
            event => panic!("expected API retirement, got {event:?}"),
        }
        assert!(!processing.is_valid());
        assert!(!api.is_valid());
        assert_eq!(service.status().state, StorageServiceStatusState::Stopped);
        service.shutdown().await.expect("repeated shutdown");
    }

    #[tokio::test]
    async fn rebuild_replaces_processing_state_and_publishes_one_gated_api_generation() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;
        let pruning_point = ValidatedNodeBlock {
            hash: hash(9),
            selected_parent: hash(5),
            direct_parents: vec![hash(4), hash(5)],
            blue_merge_set: vec![hash(3), hash(5)],
            red_merge_set: vec![hash(2), hash(3)],
            timestamp: u64::MAX,
            daa_score: 42,
            blue_score: 43,
            blue_work: Default::default(),
        };

        let rebuild_client = processing.clone();
        let rebuild_block = pruning_point.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(rebuild_block).await });

        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
            event => panic!("expected old API retirement, got {event:?}"),
        }
        let replacement_api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected gated API publication, got {event:?}"),
        };
        let anchor =
            timeout(TEST_TIMEOUT, rebuild).await.expect("Rebuild must complete").expect("Rebuild task").expect("Rebuild transaction");

        assert!(processing.is_valid(), "successful Rebuild keeps its processing generation");
        assert_eq!(anchor.point.hash(), pruning_point.hash);
        assert_eq!(anchor.selected_parent, pruning_point.selected_parent);
        assert_eq!(anchor.blue_score, pruning_point.blue_score);
        assert!(replacement_api.acquire_read_lease().await.is_ok());
        assert!(events.try_recv().is_err(), "successful Rebuild emits no second API publication");

        let rows: Vec<(i64, Vec<u8>)> = sqlx::query_as("SELECT id, hash FROM block_identifiers ORDER BY id")
            .fetch_all(processing.pool())
            .await
            .expect("rebuilt identities");
        assert_eq!(
            rows,
            vec![
                (1, hash(2).as_bytes().to_vec()),
                (2, hash(3).as_bytes().to_vec()),
                (3, hash(5).as_bytes().to_vec()),
                (4, hash(4).as_bytes().to_vec()),
                (5, hash(9).as_bytes().to_vec()),
            ]
        );
        assert_eq!(anchor.point.id().get(), 5);

        let block: (i64, i64, i64, i64, i32, i16, bool, Vec<i64>, Vec<i64>) = sqlx::query_as(
            "SELECT id, timestamp, daa_score, level, slot, color, is_in_vspc, blue_merge_set, red_merge_set
             FROM blocks",
        )
        .fetch_one(processing.pool())
        .await
        .expect("rebuilt pruning-point row");
        assert_eq!(block, (5, -1, 42, 1, 0, BlockColor::Gray as i16, true, vec![2, 3], vec![1, 2]));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT selected_parent_id FROM blocks")
                .fetch_one(processing.pool())
                .await
                .expect("rebuilt selected-parent ID"),
            3
        );
        let parents: Vec<(i64, i64, i64, i32)> =
            sqlx::query_as("SELECT parent_id, child_level, parent_level, parent_slot FROM parents ORDER BY parent_id")
                .fetch_all(processing.pool())
                .await
                .expect("rebuilt parent rows");
        assert_eq!(parents, vec![(3, 1, 0, 0), (4, 1, 0, 0)]);
        assert_eq!(
            sqlx::query_as::<_, (i64, i32, i64)>("SELECT level, size, daa_score FROM levels")
                .fetch_one(processing.pool())
                .await
                .expect("rebuilt level"),
            (1, 1, 42)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT db_pp_blue_score FROM processing_metadata WHERE singleton")
                .fetch_one(processing.pool())
                .await
                .expect("rebuilt processing metadata"),
            43
        );
        assert_eq!(
            processing.block_presence(hash(2)).await.expect("boundary identity"),
            BlockPresence::BoundaryIdentity { id: kgi_model::block::CompactId::new(1).expect("positive ID") }
        );
        assert!(processing.has_cached_identity(hash(2)));
        assert!(processing.cached_materialized_block(anchor.point.id()).is_some());
        assert!(processing.has_cached_merge_sets(anchor.point.id()));
        assert_eq!(
            processing.cached_vspc_sink(),
            Some(crate::cache::CachedVspcSink { id: anchor.point.id(), hash: pruning_point.hash })
        );

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn inconsistent_database_rebuild_publishes_api_without_placeholder_retirement() {
        let (_container, database_url) = fixture().await;
        let (mut database, state) =
            LockedDatabase::connect(database_url.as_str()).await.expect("database lock").prepare().await.expect("schema preparation");
        assert_eq!(state, DatabaseState::Uninitialized);
        database.initialize_if_uninitialized(mainnet(), hash(1), None).await.expect("database initialization");
        sqlx::query("INSERT INTO processing_metadata (singleton, db_pp_blue_score) VALUES (TRUE, 0)")
            .execute(database.connection_mut())
            .await
            .expect("inconsistent metadata fixture");
        drop(database);

        let (service, mut events) = StorageService::start(database_url);
        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing-only publication, got {event:?}"),
        };
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err());
        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(pruning_point).await });

        let api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected sole gated API publication, got {event:?}"),
        };
        rebuild.await.expect("Rebuild task").expect("Rebuild transaction");
        assert!(processing.is_valid());
        assert!(api.acquire_read_lease().await.is_ok());
        assert!(events.try_recv().is_err());

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn rebuild_rejects_out_of_range_input_before_api_replacement() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;
        let invalid = ValidatedNodeBlock { daa_score: MAX_DAA_SCORE + 1, ..genesis_pruning_point() };

        assert!(matches!(
            processing.rebuild_from_pruning_point(invalid).await,
            Err(RebuildFromPruningPointError::Database(ProcessingDbError::ScoreOutOfRange(
                kgi_model::lifecycle::ScoreRangeFault::DaaScore
            )))
        ));
        assert!(processing.is_valid());
        assert!(api.is_valid());
        assert!(api.acquire_read_lease().await.is_ok());
        assert!(events.try_recv().is_err());

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn rebuild_drains_retired_generation_leases_before_mutation() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;
        let lease = api.acquire_read_lease().await.expect("old API lease");
        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let mut rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(pruning_point).await });

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        let fresh = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected gated API publication, got {event:?}"),
        };
        assert!(lease.is_cancelled());
        assert!(timeout(Duration::from_millis(100), &mut rebuild).await.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM block_identifiers")
                .fetch_one(processing.pool())
                .await
                .expect("pre-Rebuild identity count"),
            0
        );

        drop(lease);
        timeout(TEST_TIMEOUT, rebuild)
            .await
            .expect("released lease permits Rebuild")
            .expect("Rebuild task")
            .expect("Rebuild transaction");
        assert!(fresh.acquire_read_lease().await.is_ok());
        assert_eq!(
            sqlx::query_as::<_, (i64, i64)>("SELECT id, selected_parent_id FROM blocks")
                .fetch_one(processing.pool())
                .await
                .expect("Genesis block row"),
            (2, 1)
        );
        assert_eq!(
            sqlx::query_scalar::<_, Vec<u8>>("SELECT hash FROM block_identifiers WHERE id = 1")
                .fetch_one(processing.pool())
                .await
                .expect("Genesis selected-parent identity"),
            ORIGIN.as_bytes().to_vec()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM parents")
                .fetch_one(processing.pool())
                .await
                .expect("Genesis parent count"),
            0
        );

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn rebuild_drains_lease_admitted_by_already_retired_api_generation() {
        let (_container, database_url) = fixture().await;
        let (api_open_tx, mut api_open_rx) = mpsc::unbounded_channel();
        let generation_opener =
            Arc::new(GatedApiGenerationOpener { api_open_started: api_open_tx, api_open_permit: Semaphore::new(0) });
        let (service, mut events) = StorageService::start_with_generation_opener(
            database_url,
            Arc::new(SqlxDatabaseConnector),
            Timing::production(),
            generation_opener.clone(),
        );
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        service.initialize_if_uninitialized(mainnet(), hash(1)).await.expect("database initialization");
        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing publication, got {event:?}"),
        };
        let api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected API publication, got {event:?}"),
        };
        let lease = api.acquire_read_lease().await.expect("old API lease");

        api.request_retirement().await.expect("old API retirement barrier");
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        timeout(TEST_TIMEOUT, api_open_rx.recv()).await.expect("autonomous API replacement starts").expect("API-open observer");

        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let mut rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(pruning_point).await });
        timeout(TEST_TIMEOUT, api_open_rx.recv()).await.expect("Rebuild API preparation starts").expect("API-open observer");
        // The superseded autonomous opener remains suspended on this stack while
        // the worker handles Rebuild, so release both registered waiters.
        generation_opener.api_open_permit.add_permits(2);

        let fresh = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected sole gated API publication, got {event:?}"),
        };
        assert!(lease.is_cancelled());
        assert!(timeout(Duration::from_millis(100), &mut rebuild).await.is_err());
        assert!(events.try_recv().is_err(), "Rebuild must not emit a second retirement for the absent old generation");

        drop(lease);
        rebuild.await.expect("Rebuild task").expect("Rebuild transaction");
        assert!(fresh.acquire_read_lease().await.is_ok());

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn definite_rebuild_failure_rolls_back_and_retires_the_gated_api_only() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;
        processing.inject_rebuild_commit_behavior(crate::generation::rebuild::CommitBehavior::RollbackThenFail);
        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let rebuild_block = pruning_point.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(rebuild_block).await });

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        let fresh = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected gated API publication, got {event:?}"),
        };
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &fresh)));
        assert_eq!(
            rebuild.await.expect("Rebuild task"),
            Err(RebuildFromPruningPointError::Database(ProcessingDbError::Persistence(PersistenceFault::DefiniteFailure)))
        );
        assert!(processing.is_valid());
        assert!(!fresh.is_valid());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM block_identifiers")
                .fetch_one(processing.pool())
                .await
                .expect("rolled-back identity count"),
            0
        );
        assert!(!processing.has_cached_identity(pruning_point.hash));
        assert!(events.try_recv().is_err(), "failed Rebuild does not autonomously publish another API generation");

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn precommit_rebuild_connection_loss_retires_processing_and_derives_rolled_back_truth() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url.clone()).await;
        processing.inject_rebuild_commit_behavior(crate::generation::rebuild::CommitBehavior::RollbackThenLoseConnection);
        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(pruning_point).await });

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        let fresh = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected gated API publication, got {event:?}"),
        };
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &fresh)));
        assert!(
            matches!(next_event(&mut events).await, StorageServiceEvent::ProcessingDbRetired(retired) if Arc::ptr_eq(&retired, &processing))
        );
        assert_eq!(
            rebuild.await.expect("Rebuild task"),
            Err(RebuildFromPruningPointError::Database(ProcessingDbError::GenerationLost))
        );

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM block_identifiers")
            .fetch_one(&mut PgConnection::connect(database_url.as_str()).await.expect("truth-inspection connection"))
            .await
            .expect("rolled-back Rebuild truth");
        assert_eq!(count, 0);
        let replacement_processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing generation derived from rolled-back truth, got {event:?}"),
        };
        assert!(!Arc::ptr_eq(&replacement_processing, &processing));
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbPublished(_)));

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn rebuild_api_preparation_failure_does_not_publish_or_mutate() {
        let (_container, database_url) = fixture().await;
        let (service, mut events) = StorageService::start_with_generation_opener(
            database_url,
            Arc::new(SqlxDatabaseConnector),
            Timing::production(),
            Arc::new(FailingRebuildApiGenerationOpener),
        );
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        service.initialize_if_uninitialized(mainnet(), hash(1)).await.expect("database initialization");
        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing publication, got {event:?}"),
        };
        let api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected API publication, got {event:?}"),
        };
        let pruning_point = genesis_pruning_point();

        let rebuild_client = processing.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(pruning_point).await });
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        assert_eq!(
            rebuild.await.expect("Rebuild task"),
            Err(RebuildFromPruningPointError::Setup(RebuildSetupFault::ApiGenerationPreparation))
        );
        assert!(processing.is_valid());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM block_identifiers")
                .fetch_one(processing.pool())
                .await
                .expect("identity count"),
            0
        );
        assert!(events.try_recv().is_err(), "unvalidated API generation must not be published");

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn rebuild_drain_timeout_retires_fresh_api_without_mutation() {
        let (_container, database_url) = fixture().await;
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let timing = Timing::new(Arc::new(ManualClock { sleeps: sleep_tx }), Arc::new(IdentityJitter));
        let (api_open_tx, mut api_open_rx) = mpsc::unbounded_channel();
        let generation_opener =
            Arc::new(GatedApiGenerationOpener { api_open_started: api_open_tx, api_open_permit: Semaphore::new(0) });
        let (service, mut events) = StorageService::start_with_generation_opener(
            database_url,
            Arc::new(SqlxDatabaseConnector),
            timing,
            generation_opener.clone(),
        );
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        service.initialize_if_uninitialized(mainnet(), hash(1)).await.expect("database initialization");
        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing publication, got {event:?}"),
        };
        let api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected API publication, got {event:?}"),
        };
        wait_for_status(&service, StorageServiceStatusState::Ready).await;
        let (ready_health, ready_reset) = ready_timers(&mut sleeps).await;
        let lease = api.acquire_read_lease().await.expect("old API lease");
        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let mut rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(pruning_point).await });

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        timeout(TEST_TIMEOUT, api_open_rx.recv()).await.expect("fresh API preparation starts").expect("API-open observer");
        drop(ready_health);
        drop(ready_reset);
        let timeout_sleep = loop {
            let sleep = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("replacement timer").expect("clock path");
            if sleep.duration == crate::replacement::API_REPLACEMENT_DRAIN_TIMEOUT {
                break sleep;
            }
        };
        timeout_sleep.completion.send(()).expect("expire replacement drain");
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err(), "fresh API preparation remains blocked");
        assert!(timeout(Duration::from_millis(100), &mut rebuild).await.is_err());

        generation_opener.api_open_permit.add_permits(1);
        let fresh = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected gated API publication, got {event:?}"),
        };

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &fresh)));
        assert_eq!(
            rebuild.await.expect("Rebuild task"),
            Err(RebuildFromPruningPointError::Setup(RebuildSetupFault::ApiReadExclusion))
        );
        assert!(lease.is_cancelled());
        assert!(processing.is_valid());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM block_identifiers")
                .fetch_one(processing.pool())
                .await
                .expect("identity count"),
            0
        );
        drop(lease);

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn rebuild_setup_cancellation_retires_fresh_api_without_mutation() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;
        let lease = api.acquire_read_lease().await.expect("old API lease");
        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(pruning_point).await });

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        let fresh = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected gated API publication, got {event:?}"),
        };
        rebuild.abort();
        assert!(rebuild.await.expect_err("cancelled Rebuild caller").is_cancelled());
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &fresh)));
        assert!(processing.is_valid());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM block_identifiers")
                .fetch_one(processing.pool())
                .await
                .expect("identity count"),
            0
        );
        drop(lease);

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn replacement_control_failure_before_fresh_publication_is_fatal_without_mutation() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;
        service.break_replacement_control();
        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(pruning_point).await });

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        assert!(matches!(
            rebuild.await.expect("Rebuild task"),
            Err(RebuildFromPruningPointError::ReplacementControlUnavailable { .. })
        ));
        assert!(processing.is_valid());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM block_identifiers")
                .fetch_one(processing.pool())
                .await
                .expect("identity count"),
            0
        );
        assert!(events.try_recv().is_err(), "replacement-control failure must not publish a fresh API generation");
        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn replacement_control_failure_after_fresh_publication_retires_it_without_mutation() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;
        let lease = api.acquire_read_lease().await.expect("old API lease");
        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(pruning_point).await });

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        let fresh = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected gated API publication, got {event:?}"),
        };
        service.break_replacement_control();

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &fresh)));
        assert!(matches!(
            rebuild.await.expect("Rebuild task"),
            Err(RebuildFromPruningPointError::ReplacementControlUnavailable { .. })
        ));
        assert!(lease.is_cancelled());
        assert!(processing.is_valid());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM block_identifiers")
                .fetch_one(processing.pool())
                .await
                .expect("identity count"),
            0
        );
        drop(lease);
        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn ambiguous_rebuild_commit_retires_both_generations_without_cache_publication() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url.clone()).await;
        processing.inject_rebuild_commit_behavior(crate::generation::rebuild::CommitBehavior::CommitThenLoseAcknowledgement);
        let pruning_point = genesis_pruning_point();
        let rebuild_client = processing.clone();
        let rebuild_block = pruning_point.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(rebuild_block).await });

        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        let fresh = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected gated API publication, got {event:?}"),
        };
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &fresh)));
        assert!(
            matches!(next_event(&mut events).await, StorageServiceEvent::ProcessingDbRetired(retired) if Arc::ptr_eq(&retired, &processing))
        );
        assert_eq!(
            rebuild.await.expect("Rebuild task"),
            Err(RebuildFromPruningPointError::Database(ProcessingDbError::Persistence(PersistenceFault::AmbiguousCommit)))
        );
        assert!(!processing.has_cached_identity(pruning_point.hash));

        let committed: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM block_identifiers identity
                 JOIN blocks block USING (id)
                 WHERE identity.hash = $1 AND block.level = 1 AND block.slot = 0
             )",
        )
        .bind(pruning_point.hash.as_bytes().as_slice())
        .fetch_one(&mut PgConnection::connect(database_url.as_str()).await.expect("truth-inspection connection"))
        .await
        .expect("committed Rebuild truth");
        assert!(committed);

        let replacement_processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing generation derived from database truth, got {event:?}"),
        };
        assert!(!Arc::ptr_eq(&replacement_processing, &processing));
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbPublished(_)));

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn operation_retirement_is_ordered_exact_and_independent() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;

        processing.close().await;
        assert_eq!(processing.load_session_state().await, Err(ProcessingDbError::GenerationLost));
        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected processing retirement, got {event:?}"),
        }
        let replacement_processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected replacement processing publication, got {event:?}"),
        };
        assert!(!Arc::ptr_eq(&replacement_processing, &processing));
        assert!(api.is_valid());
        assert_eq!(processing.request_retirement().await, Err(StorageError::GenerationLost));

        api.request_retirement().await.expect("API retirement barrier");
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
            event => panic!("expected API retirement without duplicate processing retirement, got {event:?}"),
        }
        let replacement_api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected replacement API publication, got {event:?}"),
        };
        assert!(!Arc::ptr_eq(&replacement_api, &api));
        assert!(replacement_processing.is_valid());

        assert!(matches!(
            service.initialize_if_uninitialized(mainnet(), hash(9)).await,
            Err(StorageError::Rejected(StorageRejection::NetworkMismatch {
                expected_network_id,
                expected_genesis_hash,
                observed_network_id,
                observed_genesis_hash,
            })) if expected_network_id == mainnet()
                && expected_genesis_hash == hash(9)
                && observed_network_id == mainnet()
                && observed_genesis_hash == hash(1)
        ));
        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &replacement_processing)),
            event => panic!("expected replacement processing retirement, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &replacement_api)),
            event => panic!("expected replacement API retirement, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::Rejected(StorageRejection::NetworkMismatch { .. }) => {}
            event => panic!("expected rejection after exact retirements, got {event:?}"),
        }
        service.shutdown().await.expect("rejected service shutdown");
    }

    #[tokio::test]
    async fn ambiguous_operation_joins_advisory_lock_loss_retirement_barrier() {
        let (_container, database_url) = fixture().await;
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) = StorageService::start_with_dependencies(
            database_url.clone(),
            Arc::new(SqlxDatabaseConnector),
            Timing::new(clock, Arc::new(IdentityJitter)),
        );
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        service.initialize_if_uninitialized(mainnet(), hash(1)).await.expect("database initialization");
        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected initial processing publication, got {event:?}"),
        };
        let api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected initial API publication, got {event:?}"),
        };
        wait_for_status(&service, StorageServiceStatusState::Ready).await;
        let (health, reset) = ready_timers(&mut sleeps).await;

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let operation_client = processing.clone();
        let operation = tokio::spawn(async move {
            operation_client
                .run_gated_test_operation(started_tx, release_rx, Err(StorageError::Persistence(PersistenceFault::AmbiguousCommit)))
                .await
        });
        started_rx.await.expect("ambiguous operation must start while the generation is valid");

        let mut observer = PgConnection::connect(database_url.as_str()).await.expect("lock observer");
        terminate_lock_owner(&mut observer).await;
        health.completion.send(()).expect("run active lock-health check");
        drop(reset);

        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected processing retirement after lock loss, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
            event => panic!("expected API retirement after lock loss, got {event:?}"),
        }

        release_tx.send(()).expect("release ambiguous operation after retirement publication");
        assert_eq!(
            timeout(TEST_TIMEOUT, operation).await.expect("retirement barrier must complete").expect("operation task"),
            Err(StorageError::Persistence(PersistenceFault::AmbiguousCommit))
        );
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err(), "old generation must retire exactly once");

        service.shutdown().await.expect("shutdown during reconnect wait");
    }

    #[tokio::test]
    async fn ambiguous_operation_joins_competing_operation_retirement_barrier() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;

        let (ambiguous_started_tx, ambiguous_started_rx) = tokio::sync::oneshot::channel();
        let (ambiguous_release_tx, ambiguous_release_rx) = tokio::sync::oneshot::channel();
        let ambiguous_client = processing.clone();
        let ambiguous = tokio::spawn(async move {
            ambiguous_client
                .run_gated_test_operation(
                    ambiguous_started_tx,
                    ambiguous_release_rx,
                    Err(StorageError::Persistence(PersistenceFault::AmbiguousCommit)),
                )
                .await
        });
        ambiguous_started_rx.await.expect("ambiguous operation must start");

        let (failed_started_tx, failed_started_rx) = tokio::sync::oneshot::channel();
        let (failed_release_tx, failed_release_rx) = tokio::sync::oneshot::channel();
        let failed_client = processing.clone();
        let failed = tokio::spawn(async move {
            failed_client
                .run_gated_test_operation(
                    failed_started_tx,
                    failed_release_rx,
                    Err(StorageError::ConnectionLost {
                        operation: "injected competing operation",
                        diagnostic: Arc::from("connection lost"),
                    }),
                )
                .await
        });
        failed_started_rx.await.expect("competing operation must start");
        failed_release_tx.send(()).expect("release competing operation first");

        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected exact processing retirement, got {event:?}"),
        }
        assert_eq!(failed.await.expect("competing operation task"), Err(StorageError::GenerationLost));

        ambiguous_release_tx.send(()).expect("release ambiguity after competing retirement publication");
        assert_eq!(
            timeout(TEST_TIMEOUT, ambiguous).await.expect("stale retirement barrier must complete").expect("ambiguous operation task"),
            Err(StorageError::Persistence(PersistenceFault::AmbiguousCommit))
        );
        let replacement = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            StorageServiceEvent::ProcessingDbRetired(retired) if Arc::ptr_eq(&retired, &processing) => {
                panic!("old processing generation retired more than once")
            }
            event => panic!("expected processing replacement after one retirement, got {event:?}"),
        };
        assert!(!Arc::ptr_eq(&replacement, &processing));
        assert!(api.is_valid(), "processing retirement remains independent of the API generation");

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn api_retirement_replaces_before_checked_out_connection_returns() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;
        let held_connection = api.pool().acquire().await.expect("checked-out API connection");

        timeout(TEST_TIMEOUT, api.request_retirement())
            .await
            .expect("retirement must not wait for the checked-out connection")
            .expect("API retirement barrier");
        assert!(api.pool().is_closed());
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
            event => panic!("expected API retirement, got {event:?}"),
        }
        let replacement_api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected API replacement before releasing the old connection, got {event:?}"),
        };

        let shutdown = service.shutdown();
        tokio::pin!(shutdown);
        assert!(timeout(Duration::from_millis(100), &mut shutdown).await.is_err());
        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected processing retirement during shutdown, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &replacement_api)),
            event => panic!("expected replacement API retirement during shutdown, got {event:?}"),
        }

        drop(held_connection);
        timeout(TEST_TIMEOUT, shutdown)
            .await
            .expect("shutdown must finish after the old connection returns")
            .expect("service shutdown");
    }

    #[tokio::test]
    async fn completed_cleanup_tasks_are_reaped_during_repeated_replacement() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, _processing, mut api) = initialize_service(database_url).await;

        for _ in 0..8 {
            api.request_retirement().await.expect("API retirement barrier");
            match next_event(&mut events).await {
                StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
                event => panic!("expected API retirement, got {event:?}"),
            }
            api = match next_event(&mut events).await {
                StorageServiceEvent::ApiDbPublished(client) => client,
                event => panic!("expected replacement API publication, got {event:?}"),
            };
            wait_for_cleanup_tasks(&service, 0).await;
        }

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn cleanup_task_failure_terminates_the_active_service() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, _processing, api) = initialize_service(database_url).await;
        service.panic_next_cleanup();

        api.request_retirement().await.expect("API retirement barrier");
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
            event => panic!("expected API retirement, got {event:?}"),
        }

        let mut completion = service.completion.clone();
        let result = timeout(TEST_TIMEOUT, async {
            loop {
                if let Some(result) = completion.borrow().clone() {
                    return result;
                }
                completion.changed().await.expect("service completion must remain observable");
            }
        })
        .await
        .expect("cleanup failure must terminate the active service");
        assert!(matches!(
            result,
            Err(StorageError::WorkerFailed { diagnostic }) if diagnostic.contains("injected cleanup task failure")
        ));
        assert_eq!(service.retained_cleanup_tasks(), 0);
        assert!(matches!(service.shutdown().await, Err(StorageError::WorkerFailed { .. })));
    }

    #[tokio::test]
    async fn advisory_lock_contention_is_terminal_rejection() {
        let (_container, database_url) = fixture().await;
        let _owner = LockedDatabase::connect(database_url.as_str()).await.expect("first lock owner");
        let (service, mut events) = StorageService::start(database_url);

        match next_event(&mut events).await {
            StorageServiceEvent::Rejected(rejection) => assert_eq!(rejection, StorageRejection::DatabaseAlreadyInUse),
            event => panic!("expected terminal rejection, got {event:?}"),
        }
        wait_for_status(&service, StorageServiceStatusState::Rejected).await;
        assert_eq!(
            service.initialize_if_uninitialized(mainnet(), hash(1)).await,
            Err(StorageError::Rejected(StorageRejection::DatabaseAlreadyInUse))
        );
        service.shutdown().await.expect("rejected service shutdown");
        assert_eq!(service.status().state, StorageServiceStatusState::Stopped);
    }

    #[tokio::test]
    async fn dirty_migration_is_terminal_and_completes_pending_initialization() {
        let (_container, database_url) = fixture().await;
        let mut connection = PgConnection::connect(database_url.as_str()).await.expect("fixture connection");
        crate::migration::MIGRATOR.run_to(1, &mut connection).await.expect("first migration only");
        sqlx::query("UPDATE _sqlx_migrations SET success = FALSE WHERE version = 1")
            .execute(&mut connection)
            .await
            .expect("dirty migration marker");
        connection.close().await.expect("close fixture connection");

        assert_terminal_migration_rejection(database_url, "partially applied").await;
    }

    #[tokio::test]
    async fn checksum_mismatch_is_terminal_and_completes_pending_initialization() {
        let (_container, database_url) = fixture().await;
        let mut connection = PgConnection::connect(database_url.as_str()).await.expect("fixture connection");
        crate::migration::MIGRATOR.run(&mut connection).await.expect("current migrations");
        sqlx::query("UPDATE _sqlx_migrations SET checksum = decode('00', 'hex') WHERE version = 1")
            .execute(&mut connection)
            .await
            .expect("migration checksum mismatch");
        connection.close().await.expect("close fixture connection");

        assert_terminal_migration_rejection(database_url, "modified").await;
    }

    #[tokio::test]
    async fn malformed_migration_history_is_terminal_without_retry_or_reconnect() {
        let (_container, database_url) = fixture().await;
        let mut connection = PgConnection::connect(database_url.as_str()).await.expect("fixture connection");
        sqlx::query("CREATE TABLE _sqlx_migrations (not_version BIGINT)")
            .execute(&mut connection)
            .await
            .expect("malformed migration history");
        connection.close().await.expect("close fixture connection");

        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let connector =
            Arc::new(GatedStartupConnector { attempts: AtomicUsize::new(0), started: started_tx, permit: Semaphore::new(0) });
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) =
            StorageService::start_with_dependencies(database_url, connector.clone(), Timing::new(clock, Arc::new(IdentityJitter)));

        timeout(TEST_TIMEOUT, started_rx.recv()).await.expect("startup must begin").expect("startup observer");
        connector.permit.add_permits(1);
        match next_event(&mut events).await {
            StorageServiceEvent::Rejected(StorageRejection::UnsupportedSchema { diagnostic }) => {
                assert!(diagnostic.contains("migration history"));
            }
            event => panic!("expected malformed migration-history rejection, got {event:?}"),
        }
        assert_eq!(service.status().state, StorageServiceStatusState::Rejected);
        assert_eq!(connector.attempts.load(Ordering::Relaxed), 1);
        assert!(sleeps.try_recv().is_err(), "malformed migration history must not schedule retry");
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err());

        service.shutdown().await.expect("rejected service shutdown");
        assert_eq!(service.status().state, StorageServiceStatusState::Stopped);
    }

    #[tokio::test]
    async fn migration_connection_loss_retries_complete_startup_and_shutdown_cancels_backoff() {
        let (_container, database_url) = fixture().await;
        let connector = Arc::new(MigrationConnectionLossConnector { attempts: AtomicUsize::new(0), injected_failures: 2 });
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) =
            StorageService::start_with_dependencies(database_url, connector.clone(), Timing::new(clock, Arc::new(IdentityJitter)));

        let first_retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("first migration retry").expect("clock path");
        assert_eq!(first_retry.duration, Duration::from_secs(1));
        assert_eq!(service.status().state, StorageServiceStatusState::Unavailable);
        assert!(events.try_recv().is_err());
        first_retry.completion.send(()).expect("advance first migration retry");

        let second_retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("second migration retry").expect("clock path");
        assert_eq!(second_retry.duration, Duration::from_secs(2));
        assert_eq!(connector.attempts.load(Ordering::Relaxed), 2);
        assert_eq!(service.status().state, StorageServiceStatusState::Unavailable);
        assert!(events.try_recv().is_err());

        service.shutdown().await.expect("shutdown must cancel migration retry wait");
        assert_eq!(service.status().state, StorageServiceStatusState::Stopped);
    }

    #[tokio::test]
    async fn advisory_lock_loss_retires_both_generations_before_republication() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url.clone()).await;
        let mut observer = PgConnection::connect(database_url.as_str()).await.expect("lock observer");
        terminate_lock_owner(&mut observer).await;

        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected processing retirement after lock loss, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
            event => panic!("expected API retirement after lock loss, got {event:?}"),
        }
        let replacement_processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing republication, got {event:?}"),
        };
        let replacement_api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected API republication, got {event:?}"),
        };
        assert!(!Arc::ptr_eq(&replacement_processing, &processing));
        assert!(!Arc::ptr_eq(&replacement_api, &api));

        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn advisory_lock_loss_without_api_generation_retires_only_processing() {
        let (_container, database_url) = fixture().await;
        let (mut database, state) =
            LockedDatabase::connect(database_url.as_str()).await.expect("database lock").prepare().await.expect("schema preparation");
        assert!(matches!(state, DatabaseState::Uninitialized));
        database.initialize_if_uninitialized(mainnet(), hash(1), None).await.expect("database initialization");
        sqlx::query("INSERT INTO processing_metadata (singleton, db_pp_blue_score) VALUES (TRUE, 0)")
            .execute(database.connection_mut())
            .await
            .expect("inconsistent processing metadata");
        drop(database);

        let (service, mut events) = StorageService::start(database_url.clone());
        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing-only publication, got {event:?}"),
        };
        wait_for_status(&service, StorageServiceStatusState::Ready).await;

        let mut observer = PgConnection::connect(database_url.as_str()).await.expect("lock observer");
        terminate_lock_owner(&mut observer).await;

        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected processing retirement after lock loss, got {event:?}"),
        }
        let replacement = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing-only republication, got {event:?}"),
        };
        assert!(!Arc::ptr_eq(&replacement, &processing));

        service.shutdown().await.expect("service shutdown");
        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &replacement)),
            event => panic!("expected only replacement processing retirement, got {event:?}"),
        }
    }

    #[tokio::test]
    async fn active_lock_loss_waits_before_reconnecting() {
        exercise_active_lock_loss(ActiveLockLossDisposition::Retry).await;
    }

    #[tokio::test]
    async fn shutdown_cancels_active_lock_loss_backoff() {
        exercise_active_lock_loss(ActiveLockLossDisposition::Shutdown).await;
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum ActiveLockLossDisposition {
        Retry,
        Shutdown,
    }

    async fn exercise_active_lock_loss(disposition: ActiveLockLossDisposition) {
        let (_container, database_url) = fixture().await;
        let (mut database, state) =
            LockedDatabase::connect(database_url.as_str()).await.expect("database lock").prepare().await.expect("schema preparation");
        assert_eq!(state, DatabaseState::Uninitialized);
        database.initialize_if_uninitialized(mainnet(), hash(7), None).await.expect("database initialization");
        drop(database);

        let (reconnect_tx, mut reconnect_rx) = mpsc::unbounded_channel();
        let connector = Arc::new(ReconnectGateConnector {
            attempts: AtomicUsize::new(0),
            reconnect_started: reconnect_tx,
            reconnect_permit: Semaphore::new(0),
        });
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) = StorageService::start_with_dependencies(
            database_url.clone(),
            connector.clone(),
            Timing::new(clock, Arc::new(IdentityJitter)),
        );

        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected initial processing publication, got {event:?}"),
        };
        let api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected initial API publication, got {event:?}"),
        };
        wait_for_status(&service, StorageServiceStatusState::Ready).await;
        let (health, reset) = ready_timers(&mut sleeps).await;

        let mut observer = PgConnection::connect(database_url.as_str()).await.expect("lock observer");
        terminate_lock_owner(&mut observer).await;
        health.completion.send(()).expect("run active lock-health check");
        drop(reset);

        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected processing retirement after lock loss, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
            event => panic!("expected API retirement after lock loss, got {event:?}"),
        }

        let retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("active lock-loss retry").expect("clock path");
        assert_eq!(retry.duration, Duration::from_secs(1));
        assert_eq!(service.status().state, StorageServiceStatusState::Unavailable);
        assert_eq!(connector.attempts.load(Ordering::Relaxed), 1);
        assert!(timeout(Duration::from_millis(100), reconnect_rx.recv()).await.is_err());
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err());

        if disposition == ActiveLockLossDisposition::Shutdown {
            service.shutdown().await.expect("shutdown must cancel active lock-loss retry");
            assert_eq!(connector.attempts.load(Ordering::Relaxed), 1);
            assert_eq!(service.status().state, StorageServiceStatusState::Stopped);
            return;
        }

        retry.completion.send(()).expect("advance active lock-loss retry");
        timeout(TEST_TIMEOUT, reconnect_rx.recv()).await.expect("reconnect attempt").expect("reconnect observer");
        assert_eq!(connector.attempts.load(Ordering::Relaxed), 2);
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err());

        connector.reconnect_permit.add_permits(1);
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ProcessingDbPublished(_)));
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbPublished(_)));
        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn initial_publication_recheck_waits_before_reconnecting() {
        exercise_failed_initial_publication_recheck(InitialRecheckDisposition::Retry).await;
    }

    #[tokio::test]
    async fn shutdown_cancels_initial_publication_recheck_backoff() {
        exercise_failed_initial_publication_recheck(InitialRecheckDisposition::Shutdown).await;
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum InitialRecheckDisposition {
        Retry,
        Shutdown,
    }

    async fn exercise_failed_initial_publication_recheck(disposition: InitialRecheckDisposition) {
        let (_container, database_url) = fixture().await;
        let (mut database, state) =
            LockedDatabase::connect(database_url.as_str()).await.expect("database lock").prepare().await.expect("schema preparation");
        assert_eq!(state, DatabaseState::Uninitialized);
        database.initialize_if_uninitialized(mainnet(), hash(6), None).await.expect("database initialization");
        drop(database);

        let (reconnect_tx, mut reconnect_rx) = mpsc::unbounded_channel();
        let connector = Arc::new(ReconnectGateConnector {
            attempts: AtomicUsize::new(0),
            reconnect_started: reconnect_tx,
            reconnect_permit: Semaphore::new(0),
        });
        let (initial_open_tx, mut initial_open_rx) = mpsc::unbounded_channel();
        let generation_opener = Arc::new(GatedInitialGenerationOpener {
            attempts: AtomicUsize::new(0),
            initial_open_started: initial_open_tx,
            initial_open_permit: Semaphore::new(0),
        });
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) = StorageService::start_with_generation_opener(
            database_url.clone(),
            connector.clone(),
            Timing::new(clock, Arc::new(IdentityJitter)),
            generation_opener.clone(),
        );

        timeout(TEST_TIMEOUT, initial_open_rx.recv()).await.expect("initial generation opening must start").expect("open observer");
        let mut observer = PgConnection::connect(database_url.as_str()).await.expect("lock observer");
        terminate_lock_owner(&mut observer).await;
        generation_opener.initial_open_permit.add_permits(1);

        let retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("ownership recheck retry").expect("clock path");
        assert_eq!(retry.duration, Duration::from_secs(1));
        assert_eq!(service.status().state, StorageServiceStatusState::Unavailable);
        assert!(timeout(Duration::from_millis(100), reconnect_rx.recv()).await.is_err());
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err());

        if disposition == InitialRecheckDisposition::Shutdown {
            service.shutdown().await.expect("shutdown must cancel ownership-recheck retry");
            assert_eq!(connector.attempts.load(Ordering::Relaxed), 1);
            assert_eq!(generation_opener.attempts.load(Ordering::Relaxed), 1);
            assert_eq!(service.status().state, StorageServiceStatusState::Stopped);
            return;
        }

        retry.completion.send(()).expect("advance ownership-recheck retry");
        timeout(TEST_TIMEOUT, reconnect_rx.recv()).await.expect("reconnect attempt").expect("reconnect observer");
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err());

        connector.reconnect_permit.add_permits(1);
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ProcessingDbPublished(_)));
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbPublished(_)));
        service.shutdown().await.expect("service shutdown");
    }

    #[derive(Clone, Copy)]
    enum ReplacementLockLossTrigger {
        HealthPoll,
        PrePublicationRecheck,
    }

    async fn exercise_api_replacement_lock_loss(trigger: ReplacementLockLossTrigger) {
        let (_container, database_url) = fixture().await;
        let (reconnect_tx, mut reconnect_rx) = mpsc::unbounded_channel();
        let connector = Arc::new(ReconnectGateConnector {
            attempts: AtomicUsize::new(0),
            reconnect_started: reconnect_tx,
            reconnect_permit: Semaphore::new(0),
        });
        let (api_open_tx, mut api_open_rx) = mpsc::unbounded_channel();
        let generation_opener =
            Arc::new(GatedApiGenerationOpener { api_open_started: api_open_tx, api_open_permit: Semaphore::new(0) });
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) = StorageService::start_with_generation_opener(
            database_url.clone(),
            connector.clone(),
            Timing::new(clock, Arc::new(IdentityJitter)),
            generation_opener.clone(),
        );
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        let genesis = match trigger {
            ReplacementLockLossTrigger::HealthPoll => hash(4),
            ReplacementLockLossTrigger::PrePublicationRecheck => hash(5),
        };
        service.initialize_if_uninitialized(mainnet(), genesis).await.expect("database initialization");
        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected initial processing publication, got {event:?}"),
        };
        let api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected initial API publication, got {event:?}"),
        };
        wait_for_status(&service, StorageServiceStatusState::Ready).await;
        let (ready_health, ready_reset) = ready_timers(&mut sleeps).await;

        api.request_retirement().await.expect("API retirement barrier");
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &api)),
            event => panic!("expected API retirement, got {event:?}"),
        }
        drop(ready_health);
        drop(ready_reset);
        timeout(TEST_TIMEOUT, api_open_rx.recv()).await.expect("API replacement must start").expect("API-open observer");
        let replacement_health = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("replacement health timer").expect("clock path");
        assert_eq!(replacement_health.duration, LOCK_HEALTH_INTERVAL);

        let mut observer = PgConnection::connect(database_url.as_str()).await.expect("lock observer");
        terminate_lock_owner(&mut observer).await;
        match trigger {
            ReplacementLockLossTrigger::HealthPoll => {
                replacement_health.completion.send(()).expect("run replacement health check");
            }
            ReplacementLockLossTrigger::PrePublicationRecheck => {
                drop(replacement_health);
                generation_opener.api_open_permit.add_permits(1);
            }
        }

        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected processing retirement before any replacement publication, got {event:?}"),
        }
        let reconnect_delay = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("lock-loss retry").expect("clock path");
        assert_eq!(reconnect_delay.duration, Duration::from_secs(1));
        assert!(timeout(Duration::from_millis(100), reconnect_rx.recv()).await.is_err());
        reconnect_delay.completion.send(()).expect("advance lock-loss retry");
        timeout(TEST_TIMEOUT, reconnect_rx.recv()).await.expect("reconnect attempt").expect("reconnect observer");
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err());

        connector.reconnect_permit.add_permits(1);
        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(_) => {}
            event => panic!("expected processing publication after reacquisition, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(_) => {}
            event => panic!("expected API publication after reacquisition, got {event:?}"),
        }
        service.shutdown().await.expect("service shutdown");
    }

    #[tokio::test]
    async fn advisory_lock_loss_is_observed_while_api_replacement_is_blocked() {
        exercise_api_replacement_lock_loss(ReplacementLockLossTrigger::HealthPoll).await;
    }

    #[tokio::test]
    async fn completed_api_replacement_rechecks_lock_before_publication() {
        exercise_api_replacement_lock_loss(ReplacementLockLossTrigger::PrePublicationRecheck).await;
    }

    #[tokio::test]
    async fn closed_event_path_is_fatal_and_releases_database_ownership() {
        let (_container, database_url) = fixture().await;
        let (service, events) = StorageService::start(database_url.clone());
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        drop(events);
        service.initialize_if_uninitialized(mainnet(), hash(2)).await.expect("database initialization");

        assert_eq!(service.shutdown().await, Err(StorageError::EventPathClosed));
        let _new_owner = LockedDatabase::connect(database_url.as_str()).await.expect("released lock must be reacquirable");
    }

    #[tokio::test]
    async fn malformed_immutable_network_identifier_is_terminal_rejection() {
        let (_container, database_url) = fixture().await;
        let (mut database, state) =
            LockedDatabase::connect(database_url.as_str()).await.expect("database lock").prepare().await.expect("schema preparation");
        assert_eq!(state, DatabaseState::Uninitialized);
        sqlx::query(
            "INSERT INTO network_metadata (singleton, network_id, genesis_hash)
             VALUES (TRUE, $1, $2)",
        )
        .bind("not-a-kaspa-network")
        .bind(hash(4).as_bytes().as_slice())
        .execute(database.connection_mut())
        .await
        .expect("malformed immutable binding fixture");
        drop(database);

        let (service, mut events) = StorageService::start(database_url);
        match next_event(&mut events).await {
            StorageServiceEvent::Rejected(StorageRejection::UnsupportedSchema { diagnostic }) => {
                assert!(diagnostic.contains("invalid immutable network binding"));
                assert!(diagnostic.contains("invalid network ID"));
            }
            event => panic!("expected terminal malformed-binding rejection, got {event:?}"),
        }
        assert_eq!(service.status().state, StorageServiceStatusState::Rejected);
        service.shutdown().await.expect("rejected service shutdown");
    }

    #[tokio::test]
    async fn operation_retirement_reports_event_path_failure_instead_of_generation_loss() {
        let (_container, database_url) = fixture().await;
        let (service, events, processing, _api) = initialize_service(database_url).await;
        drop(events);

        processing.close().await;
        let result = timeout(TEST_TIMEOUT, processing.load_session_state()).await.expect("operation retirement barrier must complete");
        assert!(matches!(result, Err(ProcessingDbError::ServiceUnavailable { .. })));
        assert_eq!(service.shutdown().await, Err(StorageError::EventPathClosed));
    }

    struct FailingConnector {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl DatabaseConnector for FailingConnector {
        async fn connect(&self, _database_url: &str) -> Result<LockedDatabase, StorageError> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            Err(StorageError::Database { operation: "scripted connection", diagnostic: Arc::from("unavailable") })
        }
    }

    struct GatedStartupConnector {
        attempts: AtomicUsize,
        started: mpsc::UnboundedSender<()>,
        permit: Semaphore,
    }

    #[async_trait]
    impl DatabaseConnector for GatedStartupConnector {
        async fn connect(&self, database_url: &str) -> Result<LockedDatabase, StorageError> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            let database = LockedDatabase::connect(database_url).await?;
            self.started.send(()).expect("startup observer");
            self.permit.acquire().await.expect("startup gate").forget();
            Ok(database)
        }
    }

    struct MigrationConnectionLossConnector {
        attempts: AtomicUsize,
        injected_failures: usize,
    }

    #[async_trait]
    impl DatabaseConnector for MigrationConnectionLossConnector {
        async fn connect(&self, database_url: &str) -> Result<LockedDatabase, StorageError> {
            let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
            let mut database = LockedDatabase::connect(database_url).await?;
            if attempt < self.injected_failures {
                database.inject_migration_connection_loss();
            }
            Ok(database)
        }
    }

    struct ReconnectGateConnector {
        attempts: AtomicUsize,
        reconnect_started: mpsc::UnboundedSender<()>,
        reconnect_permit: Semaphore,
    }

    #[async_trait]
    impl DatabaseConnector for ReconnectGateConnector {
        async fn connect(&self, database_url: &str) -> Result<LockedDatabase, StorageError> {
            if self.attempts.fetch_add(1, Ordering::Relaxed) > 0 {
                self.reconnect_started.send(()).expect("reconnect observer");
                self.reconnect_permit.acquire().await.expect("reconnect gate").forget();
            }
            LockedDatabase::connect(database_url).await
        }
    }

    struct GatedApiGenerationOpener {
        api_open_started: mpsc::UnboundedSender<()>,
        api_open_permit: Semaphore,
    }

    struct FailingRebuildApiGenerationOpener;

    struct GatedInitialGenerationOpener {
        attempts: AtomicUsize,
        initial_open_started: mpsc::UnboundedSender<()>,
        initial_open_permit: Semaphore,
    }

    #[async_trait]
    impl GenerationOpener for GatedInitialGenerationOpener {
        async fn open_initial(
            &self,
            database_url: String,
            state: DatabaseState,
            retirement_tx: RetirementSender,
            rebuild_tx: RebuildSender,
            replacement_gate: Arc<ApiReplacementGate>,
        ) -> Result<Option<ValidatedGenerations>, StorageError> {
            if self.attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                self.initial_open_started.send(()).expect("initial-open observer");
                self.initial_open_permit.acquire().await.expect("initial-open gate").forget();
            }
            open_validated_generations_managed(database_url, state, retirement_tx, rebuild_tx, replacement_gate).await
        }
    }

    #[async_trait]
    impl GenerationOpener for GatedApiGenerationOpener {
        async fn open_api(
            &self,
            database_url: String,
            binding: DatabaseBinding,
            retirement_tx: RetirementSender,
            replacement_gate: Arc<ApiReplacementGate>,
        ) -> Result<Arc<ValidatedApiDbClient>, StorageError> {
            self.api_open_started.send(()).expect("API-open observer");
            self.api_open_permit.acquire().await.expect("API-open gate").forget();
            open_api_generation(database_url, binding, retirement_tx, replacement_gate).await
        }
    }

    #[async_trait]
    impl GenerationOpener for FailingRebuildApiGenerationOpener {
        async fn open_api(
            &self,
            _database_url: String,
            _binding: DatabaseBinding,
            _retirement_tx: RetirementSender,
            _replacement_gate: Arc<ApiReplacementGate>,
        ) -> Result<Arc<ValidatedApiDbClient>, StorageError> {
            Err(StorageError::Database { operation: "scripted API preparation", diagnostic: Arc::from("unavailable") })
        }
    }

    struct SleepRequest {
        duration: Duration,
        completion: tokio::sync::oneshot::Sender<()>,
    }

    struct ManualClock {
        sleeps: mpsc::UnboundedSender<SleepRequest>,
    }

    #[async_trait]
    impl Clock for ManualClock {
        async fn sleep(&self, duration: Duration) {
            let (completion, acknowledgement) = tokio::sync::oneshot::channel();
            self.sleeps.send(SleepRequest { duration, completion }).expect("manual clock receiver");
            if acknowledgement.await.is_err() {
                std::future::pending().await
            }
        }
    }

    struct IdentityJitter;

    impl Jitter for IdentityJitter {
        fn apply(&self, nominal: Duration) -> Duration {
            nominal
        }
    }

    struct IntermittentConnector {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl DatabaseConnector for IntermittentConnector {
        async fn connect(&self, database_url: &str) -> Result<LockedDatabase, StorageError> {
            let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
            if matches!(attempt, 0 | 2 | 4) {
                return Err(StorageError::Database { operation: "scripted connection", diagnostic: Arc::from("unavailable") });
            }
            LockedDatabase::connect(database_url).await
        }
    }

    async fn ready_timers(sleeps: &mut mpsc::UnboundedReceiver<SleepRequest>) -> (SleepRequest, SleepRequest) {
        let first = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("first Ready timer").expect("clock path");
        let second = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("second Ready timer").expect("clock path");
        match (first.duration, second.duration) {
            (LOCK_HEALTH_INTERVAL, READY_BACKOFF_RESET) => (first, second),
            (READY_BACKOFF_RESET, LOCK_HEALTH_INTERVAL) => (second, first),
            durations => panic!("unexpected Ready timers: {durations:?}"),
        }
    }

    async fn latest_live_sleep_after_quiescence(sleeps: &mut mpsc::UnboundedReceiver<SleepRequest>) -> SleepRequest {
        let mut latest = None;
        loop {
            match timeout(Duration::from_millis(50), sleeps.recv()).await {
                Ok(Some(request)) => {
                    if !request.completion.is_closed() {
                        latest = Some(request);
                    }
                }
                Ok(None) => panic!("manual clock path closed"),
                Err(_) => {
                    if let Some(request) = latest.take()
                        && !request.completion.is_closed()
                    {
                        return request;
                    }
                }
            }
        }
    }

    async fn assert_terminal_migration_rejection(database_url: Url, expected_diagnostic: &str) {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let connector =
            Arc::new(GatedStartupConnector { attempts: AtomicUsize::new(0), started: started_tx, permit: Semaphore::new(0) });
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) =
            StorageService::start_with_dependencies(database_url, connector.clone(), Timing::new(clock, Arc::new(IdentityJitter)));

        timeout(TEST_TIMEOUT, started_rx.recv()).await.expect("startup must begin").expect("startup observer");
        let initialization_service = service.clone();
        let initialization =
            tokio::spawn(async move { initialization_service.initialize_if_uninitialized(mainnet(), hash(12)).await });
        tokio::task::yield_now().await;
        connector.permit.add_permits(1);

        let rejection = match next_event(&mut events).await {
            StorageServiceEvent::Rejected(StorageRejection::MigrationFailed { diagnostic }) => {
                assert!(diagnostic.contains(expected_diagnostic), "unexpected migration diagnostic: {diagnostic}");
                StorageRejection::MigrationFailed { diagnostic }
            }
            event => panic!("expected terminal migration rejection, got {event:?}"),
        };
        assert_eq!(
            timeout(TEST_TIMEOUT, initialization).await.expect("pending initialization must complete").expect("initialization task"),
            Err(StorageError::Rejected(rejection))
        );
        assert_eq!(service.status().state, StorageServiceStatusState::Rejected);
        assert_eq!(connector.attempts.load(Ordering::Relaxed), 1);
        assert!(sleeps.try_recv().is_err(), "terminal migration rejection must not schedule retry");
        assert!(timeout(Duration::from_millis(100), events.recv()).await.is_err());

        service.shutdown().await.expect("rejected service shutdown");
        assert_eq!(service.status().state, StorageServiceStatusState::Stopped);
    }

    async fn terminate_lock_owner(observer: &mut PgConnection) {
        let lock_owner_pid: i32 = sqlx::query_scalar(
            "SELECT pid
             FROM pg_locks
             WHERE locktype = 'advisory'
               AND granted
               AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
             LIMIT 1",
        )
        .fetch_one(&mut *observer)
        .await
        .expect("advisory-lock owner PID");
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(lock_owner_pid)
            .fetch_one(&mut *observer)
            .await
            .expect("terminate lock owner");
        assert!(terminated);
        timeout(TEST_TIMEOUT, async {
            loop {
                let owner_exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
                    .bind(lock_owner_pid)
                    .fetch_one(&mut *observer)
                    .await
                    .expect("terminated owner observation");
                if !owner_exists {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("lock owner must terminate");
    }

    #[tokio::test]
    async fn retry_sequence_is_deterministic_and_shutdown_cancels_the_wait() {
        let connector = Arc::new(FailingConnector { attempts: AtomicUsize::new(0) });
        let (sleep_tx, mut sleep_rx) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let database_url = Url::parse("postgresql://localhost/kgi").expect("test URL");
        let (service, _events) =
            StorageService::start_with_dependencies(database_url, connector.clone(), Timing::new(clock, Arc::new(IdentityJitter)));

        for expected in [Duration::from_secs(1), Duration::from_secs(2)] {
            let sleep = timeout(TEST_TIMEOUT, sleep_rx.recv())
                .await
                .expect("retry sleep must be requested")
                .expect("clock path must remain open");
            assert_eq!(sleep.duration, expected);
            sleep.completion.send(()).expect("advance retry clock");
        }
        let pending = timeout(TEST_TIMEOUT, sleep_rx.recv())
            .await
            .expect("third retry sleep must be requested")
            .expect("clock path must remain open");
        assert_eq!(pending.duration, Duration::from_secs(4));
        assert_eq!(connector.attempts.load(Ordering::Relaxed), 3);

        service.shutdown().await.expect("shutdown must cancel retry wait");
        assert_eq!(service.status().state, StorageServiceStatusState::Stopped);
    }

    #[tokio::test]
    async fn successful_rebuild_preserves_ready_retry_reset_deadline() {
        exercise_rebuild_ready_retry_reset(RebuildReadyOutcome::Success).await;
    }

    #[tokio::test]
    async fn definite_rebuild_failure_preserves_ready_retry_reset_deadline() {
        exercise_rebuild_ready_retry_reset(RebuildReadyOutcome::DefiniteFailure).await;
    }

    #[derive(Clone, Copy)]
    enum RebuildReadyOutcome {
        Success,
        DefiniteFailure,
    }

    async fn exercise_rebuild_ready_retry_reset(outcome: RebuildReadyOutcome) {
        let (_container, database_url) = fixture().await;
        let connector = Arc::new(IntermittentConnector { attempts: AtomicUsize::new(0) });
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) =
            StorageService::start_with_dependencies(database_url.clone(), connector, Timing::new(clock, Arc::new(IdentityJitter)));

        let first_retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("first retry timer").expect("clock path");
        assert_eq!(first_retry.duration, Duration::from_secs(1));
        first_retry.completion.send(()).expect("advance first retry");
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        service.initialize_if_uninitialized(mainnet(), hash(1)).await.expect("database initialization");
        let processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing publication, got {event:?}"),
        };
        let api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected API publication, got {event:?}"),
        };
        wait_for_status(&service, StorageServiceStatusState::Ready).await;
        let (ready_health, ready_reset) = ready_timers(&mut sleeps).await;

        let lease = api.acquire_read_lease().await.expect("old API lease");
        if matches!(outcome, RebuildReadyOutcome::DefiniteFailure) {
            processing.inject_rebuild_commit_behavior(crate::generation::rebuild::CommitBehavior::RollbackThenFail);
        }
        let rebuild_client = processing.clone();
        let rebuild = tokio::spawn(async move { rebuild_client.rebuild_from_pruning_point(genesis_pruning_point()).await });
        assert!(matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &api)));
        let fresh = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected gated API publication, got {event:?}"),
        };

        ready_reset.completion.send(()).expect("cross Ready-reset boundary during Rebuild");
        drop(ready_health);
        drop(lease);
        match outcome {
            RebuildReadyOutcome::Success => {
                rebuild.await.expect("Rebuild task").expect("successful Rebuild");
            }
            RebuildReadyOutcome::DefiniteFailure => {
                assert!(
                    matches!(next_event(&mut events).await, StorageServiceEvent::ApiDbRetired(retired) if Arc::ptr_eq(&retired, &fresh))
                );
                assert_eq!(
                    rebuild.await.expect("Rebuild task"),
                    Err(RebuildFromPruningPointError::Database(ProcessingDbError::Persistence(PersistenceFault::DefiniteFailure)))
                );
            }
        }
        assert_eq!(service.status().state, StorageServiceStatusState::Ready);

        let health = latest_live_sleep_after_quiescence(&mut sleeps).await;
        assert_eq!(health.duration, LOCK_HEALTH_INTERVAL);
        let mut observer = PgConnection::connect(database_url.as_str()).await.expect("lock observer");
        terminate_lock_owner(&mut observer).await;
        health.completion.send(()).expect("run post-Rebuild lock-health check");
        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &processing)),
            event => panic!("expected processing retirement after lock loss, got {event:?}"),
        }
        if matches!(outcome, RebuildReadyOutcome::Success) {
            match next_event(&mut events).await {
                StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &fresh)),
                event => panic!("expected rebuilt API retirement after lock loss, got {event:?}"),
            }
        }
        let retry = latest_live_sleep_after_quiescence(&mut sleeps).await;
        assert_eq!(retry.duration, Duration::from_secs(1), "continuous Ready interval must reset retry progression");

        service.shutdown().await.expect("shutdown must cancel post-Rebuild retry");
    }

    #[tokio::test]
    async fn retry_progress_resets_only_after_sixty_continuous_ready_seconds() {
        let (_container, database_url) = fixture().await;
        let connector = Arc::new(IntermittentConnector { attempts: AtomicUsize::new(0) });
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) =
            StorageService::start_with_dependencies(database_url.clone(), connector, Timing::new(clock, Arc::new(IdentityJitter)));

        let first_retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("first retry timer").expect("clock path");
        assert_eq!(first_retry.duration, Duration::from_secs(1));
        first_retry.completion.send(()).expect("advance first retry");
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        service.initialize_if_uninitialized(mainnet(), hash(3)).await.expect("database initialization");
        let first_processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected processing publication, got {event:?}"),
        };
        let first_api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected API publication, got {event:?}"),
        };
        let (short_health, short_reset) = ready_timers(&mut sleeps).await;
        let mut observer = PgConnection::connect(database_url.as_str()).await.expect("lock observer");
        terminate_lock_owner(&mut observer).await;
        short_health.completion.send(()).expect("run short-Ready health check");
        drop(short_reset);
        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &first_processing)),
            event => panic!("expected processing retirement, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &first_api)),
            event => panic!("expected API retirement, got {event:?}"),
        }
        let lock_loss_retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("lock-loss retry timer").expect("clock path");
        assert_eq!(lock_loss_retry.duration, Duration::from_secs(2));
        lock_loss_retry.completion.send(()).expect("advance lock-loss retry");
        let connection_retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("connection retry timer").expect("clock path");
        assert_eq!(connection_retry.duration, Duration::from_secs(4));
        connection_retry.completion.send(()).expect("advance connection retry");
        let second_processing = match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbPublished(client) => client,
            event => panic!("expected replacement processing publication, got {event:?}"),
        };
        let second_api = match next_event(&mut events).await {
            StorageServiceEvent::ApiDbPublished(client) => client,
            event => panic!("expected replacement API publication, got {event:?}"),
        };

        let (stale_health, reset) = ready_timers(&mut sleeps).await;
        reset.completion.send(()).expect("complete Ready reset timer");
        drop(stale_health);
        let health_after_reset = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("post-reset health timer").expect("clock path");
        assert_eq!(health_after_reset.duration, LOCK_HEALTH_INTERVAL);
        terminate_lock_owner(&mut observer).await;
        health_after_reset.completion.send(()).expect("run post-reset health check");
        match next_event(&mut events).await {
            StorageServiceEvent::ProcessingDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &second_processing)),
            event => panic!("expected processing retirement, got {event:?}"),
        }
        match next_event(&mut events).await {
            StorageServiceEvent::ApiDbRetired(retired) => assert!(Arc::ptr_eq(&retired, &second_api)),
            event => panic!("expected API retirement, got {event:?}"),
        }
        let reset_retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("reset retry timer").expect("clock path");
        assert_eq!(reset_retry.duration, Duration::from_secs(1));

        service.shutdown().await.expect("shutdown must cancel reset retry wait");
    }
}
