use std::{collections::VecDeque, sync::Arc, time::Duration};

use kaspa_consensus_core::network::NetworkId;
use kgi_core::timing::{Clock, EqualJitter, Jitter, TokioClock};
use kgi_model::{
    block::BlockHash,
    lifecycle::{StorageServiceStatus, StorageServiceStatusState},
};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};
use url::Url;

use crate::{
    database::{
        DatabaseConnector, PreparedDatabase, SqlxDatabaseConnector, open_api_generation, open_processing_generation,
        open_validated_generations,
    },
    error::{StorageError, StorageRejection},
    generation::{DatabaseBinding, ValidatedApiDbClient, ValidatedDbClient},
    runtime::{RetirementReceiver, RetirementRequest, RetirementSender, RetirementTarget, retirement_channel},
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
}

impl StorageService {
    /// Starts the permanent database lifecycle worker with its event path installed.
    #[must_use]
    pub fn start(database_url: Url) -> (Arc<Self>, StorageServiceEventReceiver) {
        Self::start_with_dependencies(
            database_url,
            Arc::new(SqlxDatabaseConnector),
            Arc::new(TokioClock),
            Arc::new(EqualJitter::from_entropy()),
        )
    }

    fn start_with_dependencies(
        database_url: Url,
        connector: Arc<dyn DatabaseConnector>,
        clock: Arc<dyn Clock>,
        jitter: Arc<dyn Jitter>,
    ) -> (Arc<Self>, StorageServiceEventReceiver) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (retirement_tx, retirement_rx) = retirement_channel();
        let (status_tx, status_rx) = watch::channel(StorageServiceStatus { state: StorageServiceStatusState::Connecting });
        let (completion_tx, completion_rx) = watch::channel(None);
        let worker = StorageServiceWorker {
            database_url,
            connector,
            clock,
            jitter,
            events: event_tx,
            commands: command_rx,
            retirements: retirement_rx,
            retirement_tx,
            status: status_tx,
        };
        let join = tokio::spawn(async move {
            let result = worker.run().await;
            completion_tx.send_replace(Some(result));
        });
        let service =
            Arc::new(Self { commands: command_tx, status: status_rx, completion: completion_rx, join: Mutex::new(Some(join)) });
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

struct StorageServiceWorker {
    database_url: Url,
    connector: Arc<dyn DatabaseConnector>,
    clock: Arc<dyn Clock>,
    jitter: Arc<dyn Jitter>,
    events: mpsc::UnboundedSender<StorageServiceEvent>,
    commands: mpsc::UnboundedReceiver<ServiceCommand>,
    retirements: RetirementReceiver,
    retirement_tx: RetirementSender,
    status: watch::Sender<StorageServiceStatus>,
}

impl StorageServiceWorker {
    async fn run(mut self) -> Result<(), StorageError> {
        let mut retry_index = 0;
        let mut pending_initialization = VecDeque::new();
        'lifecycle: loop {
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
                    command = self.commands.recv() => match queue_or_shutdown(command, &mut pending_initialization) {
                        CommandDisposition::Continue => {}
                        CommandDisposition::Shutdown => return self.finish_without_generations().await,
                    },
                    request = self.retirements.recv() => complete_stale_retirement(request)?,
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

            let generations =
                match open_validated_generations(self.database_url.as_str().to_owned(), state, self.retirement_tx.clone()).await {
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
                ActiveExit::Reconnect => continue,
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
                let opening = open_processing_generation(database_url, active.binding, self.retirement_tx.clone());
                tokio::pin!(opening);
                loop {
                    tokio::select! {
                        result = &mut opening => match result {
                            Ok(client) => {
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
                        command = self.commands.recv() => if let Some(exit) = self.handle_active_command(command, database, active).await? {
                            return Ok(exit);
                        },
                        request = self.retirements.recv() => self.handle_retirement(request, active).await?,
                    }
                }
                continue;
            }

            if active.api_required && active.api.is_none() {
                self.publish_status(StorageServiceStatusState::Unavailable);
                let database_url = self.database_url.as_str().to_owned();
                let opening = open_api_generation(database_url, self.retirement_tx.clone());
                tokio::pin!(opening);
                loop {
                    tokio::select! {
                        result = &mut opening => match result {
                            Ok(client) => {
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
                        command = self.commands.recv() => if let Some(exit) = self.handle_active_command(command, database, active).await? {
                            return Ok(exit);
                        },
                        request = self.retirements.recv() => self.handle_retirement(request, active).await?,
                    }
                }
                continue;
            }

            self.publish_status(StorageServiceStatusState::Ready);
            let ready_clock = self.clock.clone();
            let ready_reset = async move { ready_clock.sleep(READY_BACKOFF_RESET).await };
            tokio::pin!(ready_reset);
            let mut reset_complete = false;
            loop {
                let health_clock = self.clock.clone();
                let health = async move { health_clock.sleep(LOCK_HEALTH_INTERVAL).await };
                tokio::pin!(health);
                tokio::select! {
                    () = &mut ready_reset, if !reset_complete => {
                        *retry_index = 0;
                        reset_complete = true;
                    }
                    () = &mut health => {
                        if database.ping().await.is_err() {
                            self.retire_all(active).await?;
                            self.publish_status(StorageServiceStatusState::Unavailable);
                            return Ok(ActiveExit::Reconnect);
                        }
                    }
                    command = self.commands.recv() => if let Some(exit) = self.handle_active_command(command, database, active).await? {
                        return Ok(exit);
                    },
                    request = self.retirements.recv() => {
                        self.handle_retirement(request, active).await?;
                        if active.processing.is_none() || (active.api_required && active.api.is_none()) {
                            break;
                        }
                    }
                }
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

    async fn handle_retirement(&self, request: Option<RetirementRequest>, active: &mut ActiveGenerations) -> Result<(), StorageError> {
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
                        let event_result = self.send_event(StorageServiceEvent::ProcessingDbRetired(client.clone()));
                        request.complete();
                        client.close().await;
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
                        let event_result = self.send_event(StorageServiceEvent::ApiDbRetired(client.clone()));
                        request.complete();
                        client.close().await;
                        return event_result;
                    }
                }
            }
        }
        request.complete();
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
        let delay = self.jitter.apply(nominal);
        let clock = self.clock.clone();
        let sleep = async move { clock.sleep(delay).await };
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => return Ok(ActiveRetryExit::Retry),
                command = self.commands.recv() => {
                    if let Some(exit) = self.handle_active_command(command, database, active).await? {
                        return Ok(ActiveRetryExit::Exit(exit));
                    }
                },
                request = self.retirements.recv() => self.handle_retirement(request, active).await?,
            }
        }
    }

    async fn wait_retry(
        &mut self,
        retry_index: &mut usize,
        pending: &mut VecDeque<InitializationRequest>,
    ) -> Result<bool, StorageError> {
        let nominal = RETRY_DELAYS[(*retry_index).min(RETRY_DELAYS.len() - 1)];
        *retry_index = retry_index.saturating_add(1);
        let sleep = self.clock.sleep(self.jitter.apply(nominal));
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => return Ok(true),
                command = self.commands.recv() => match queue_or_shutdown(command, pending) {
                    CommandDisposition::Continue => {}
                    CommandDisposition::Shutdown => {
                        self.publish_status(StorageServiceStatusState::Stopped);
                        return Ok(false);
                    }
                },
                request = self.retirements.recv() => complete_stale_retirement(request)?,
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

    async fn retire_all(&self, active: &mut ActiveGenerations) -> Result<(), StorageError> {
        let mut result = Ok(());
        let processing = active.processing.take();
        let api = active.api.take();
        if let Some(processing) = &processing
            && processing.retire()
            && let Err(error) = self.send_event(StorageServiceEvent::ProcessingDbRetired(processing.clone()))
        {
            result = Err(error);
        }
        if let Some(api) = &api
            && api.retire()
            && let Err(error) = self.send_event(StorageServiceEvent::ApiDbRetired(api.clone()))
            && result.is_ok()
        {
            result = Err(error);
        }
        match (processing, api) {
            (Some(processing), Some(api)) => {
                tokio::join!(processing.close(), api.close());
            }
            (Some(processing), None) => processing.close().await,
            (None, Some(api)) => api.close().await,
            (None, None) => {}
        }
        result
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
    request.complete();
    Ok(())
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
    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_core::timing::{Clock, Jitter};
    use kgi_model::{block::BlockHash, lifecycle::StorageServiceStatusState};
    use sqlx::{Connection, PgConnection};
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    };
    use tokio::{sync::mpsc, time::timeout};
    use url::Url;

    use super::{LOCK_HEALTH_INTERVAL, READY_BACKOFF_RESET, StorageService, StorageServiceEvent, StorageServiceEventReceiver};
    use crate::{
        database::{DatabaseConnector, LockedDatabase},
        error::{StorageError, StorageRejection},
        generation::{ValidatedApiDbClient, ValidatedDbClient},
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
    async fn operation_retirement_is_ordered_exact_and_independent() {
        let (_container, database_url) = fixture().await;
        let (service, mut events, processing, api) = initialize_service(database_url).await;

        processing.close().await;
        assert_eq!(processing.load_session_state().await, Err(StorageError::GenerationLost));
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
    async fn closed_event_path_is_fatal_and_releases_database_ownership() {
        let (_container, database_url) = fixture().await;
        let (service, events) = StorageService::start(database_url.clone());
        wait_for_status(&service, StorageServiceStatusState::AwaitingInitialization).await;
        drop(events);
        service.initialize_if_uninitialized(mainnet(), hash(2)).await.expect("database initialization");

        assert_eq!(service.shutdown().await, Err(StorageError::EventPathClosed));
        let _new_owner = LockedDatabase::connect(database_url.as_str()).await.expect("released lock must be reacquirable");
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
            StorageService::start_with_dependencies(database_url, connector.clone(), clock, Arc::new(IdentityJitter));

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
    async fn retry_progress_resets_only_after_sixty_continuous_ready_seconds() {
        let (_container, database_url) = fixture().await;
        let connector = Arc::new(IntermittentConnector { attempts: AtomicUsize::new(0) });
        let (sleep_tx, mut sleeps) = mpsc::unbounded_channel();
        let clock = Arc::new(ManualClock { sleeps: sleep_tx });
        let (service, mut events) =
            StorageService::start_with_dependencies(database_url.clone(), connector, clock, Arc::new(IdentityJitter));

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
        let second_retry = timeout(TEST_TIMEOUT, sleeps.recv()).await.expect("second retry timer").expect("clock path");
        assert_eq!(second_retry.duration, Duration::from_secs(2));
        second_retry.completion.send(()).expect("advance second retry");
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
