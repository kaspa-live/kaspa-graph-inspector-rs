use std::{path::Path, sync::Arc, time::Duration};

use kaspa_consensus_core::network::NetworkId;
use kaspa_grpc_client::GrpcClientNotify;
use kaspa_rpc_core::{
    GetBlocksRequest, GetServerInfoRequest,
    api::ops::{RPC_API_REVISION, RPC_API_VERSION},
};
use kgi_core::timing::{Clock, EqualJitter, Jitter, TokioClock};
use kgi_model::lifecycle::{NodeServiceStatus, NodeServiceStatusState, ValidatedNodeStatus};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};
use url::Url;

use crate::{
    client::{GrpcConnector, RpcConnection, RpcConnector, ServerInfoObservation},
    consensus::KgiConsensusParams,
    error::{ConsensusResolutionError, NodeRejection, NodeServiceError, NodeUnavailableReason},
    rpc::{ValidatedNodeInfo, ValidatedRpcClient},
    runtime::{RetirementReceiver, RetirementRequest, retirement_channel},
};

const IBD_POLL_INTERVAL: Duration = Duration::from_secs(1);
const READY_BACKOFF_RESET: Duration = Duration::from_secs(60);
const RETRY_DELAYS: [Duration; 6] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
    Duration::from_secs(30),
];

/// Reliable ordered lifecycle event produced by NodeService.
#[derive(Clone)]
pub enum NodeServiceEvent {
    /// A validated generation ceased accepting work.
    RpcRetired(Arc<ValidatedRpcClient>),
    /// A newly validated generation became usable.
    RpcPublished(Arc<ValidatedRpcClient>),
    /// The configured node was permanently rejected.
    Rejected(NodeRejection),
}

/// Receiver for the reliable ordered NodeService event stream.
pub type NodeServiceEventReceiver = mpsc::UnboundedReceiver<NodeServiceEvent>;

/// Permanent owner of node connection validation and RPC generations.
pub struct NodeService {
    commands: mpsc::UnboundedSender<ServiceCommand>,
    status: watch::Receiver<NodeServiceStatus>,
    completion: watch::Receiver<Option<Result<(), NodeServiceError>>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl NodeService {
    /// Resolves local node assumptions and starts the permanent lifecycle worker.
    pub fn start(
        endpoint: Url,
        network_id: NetworkId,
        override_params_file: Option<&Path>,
    ) -> Result<(Arc<Self>, NodeServiceEventReceiver), ConsensusResolutionError> {
        let consensus = KgiConsensusParams::resolve(network_id, override_params_file)?;
        Ok(Self::start_with_dependencies(
            endpoint,
            network_id,
            consensus,
            Arc::new(GrpcConnector),
            Arc::new(TokioClock),
            Arc::new(EqualJitter::from_entropy()),
        ))
    }

    fn start_with_dependencies(
        endpoint: Url,
        network_id: NetworkId,
        consensus: KgiConsensusParams,
        connector: Arc<dyn RpcConnector>,
        clock: Arc<dyn Clock>,
        jitter: Arc<dyn Jitter>,
    ) -> (Arc<Self>, NodeServiceEventReceiver) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (retirement_tx, retirement_rx) = retirement_channel();
        let initial_status = NodeServiceStatus { state: NodeServiceStatusState::Connecting, last_validated: None };
        let (status_tx, status_rx) = watch::channel(initial_status);
        let (completion_tx, completion_rx) = watch::channel(None);

        let worker = NodeServiceWorker {
            endpoint,
            network_id,
            consensus,
            connector,
            clock,
            jitter,
            events: event_tx,
            commands: command_rx,
            retirements: retirement_rx,
            retirement_tx,
            status: status_tx,
            last_validated: None,
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
    pub fn status(&self) -> NodeServiceStatus {
        self.status.borrow().clone()
    }

    /// Subscribes to latest-value status changes.
    #[must_use]
    pub fn subscribe_status(&self) -> watch::Receiver<NodeServiceStatus> {
        self.status.clone()
    }

    /// Terminates the service and releases its current physical connection.
    pub async fn shutdown(&self) -> Result<(), NodeServiceError> {
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
            completion.changed().await.map_err(|_| NodeServiceError::ControlUnavailable)?;
        }
    }

    async fn join_worker(&self) -> Result<(), NodeServiceError> {
        let Some(join) = self.join.lock().await.take() else {
            return Ok(());
        };
        join.await.map_err(|error| NodeServiceError::WorkerFailed { diagnostic: Arc::from(error.to_string()) })
    }
}

enum ServiceCommand {
    Shutdown(oneshot::Sender<()>),
}

struct NodeServiceWorker {
    endpoint: Url,
    network_id: NetworkId,
    consensus: KgiConsensusParams,
    connector: Arc<dyn RpcConnector>,
    clock: Arc<dyn Clock>,
    jitter: Arc<dyn Jitter>,
    events: mpsc::UnboundedSender<NodeServiceEvent>,
    commands: mpsc::UnboundedReceiver<ServiceCommand>,
    retirements: RetirementReceiver,
    retirement_tx: crate::runtime::RetirementSender,
    status: watch::Sender<NodeServiceStatus>,
    last_validated: Option<ValidatedNodeStatus>,
}

impl NodeServiceWorker {
    async fn run(mut self) -> Result<(), NodeServiceError> {
        let mut retry_index = 0;
        'lifecycle: loop {
            self.publish_status(NodeServiceStatusState::Connecting);
            let connector = self.connector.clone();
            let endpoint = self.endpoint.clone();
            let connecting = async move { connector.connect(&endpoint).await };
            tokio::pin!(connecting);
            let connection = loop {
                tokio::select! {
                    command = self.commands.recv() => return self.finish_without_generation(command).await,
                    request = self.retirements.recv() => complete_stale_retirement(request)?,
                    result = &mut connecting => match result {
                        Ok(connection) => break connection,
                        Err(diagnostic) => {
                            self.publish_unavailable(NodeUnavailableReason::ConnectionFailed { diagnostic });
                            if self.wait_retry(&mut retry_index).await? {
                                continue 'lifecycle;
                            }
                            return Ok(());
                        }
                    }
                }
            };

            let validation = Self::validate_connection(
                connection.clone(),
                self.network_id,
                self.consensus,
                self.clock.clone(),
                self.status.clone(),
                self.last_validated.clone(),
            );
            tokio::pin!(validation);
            let validation = loop {
                tokio::select! {
                    command = self.commands.recv() => {
                        let _ = connection.disconnect().await;
                        return self.finish_without_generation(command).await;
                    }
                    request = self.retirements.recv() => complete_stale_retirement(request)?,
                    result = &mut validation => break result,
                }
            };
            let node_info = match validation {
                Ok(node_info) => node_info,
                Err(ValidationFailure::Unavailable(reason)) => {
                    let _ = connection.disconnect().await;
                    self.publish_unavailable(reason);
                    if self.wait_retry(&mut retry_index).await? {
                        continue;
                    }
                    return Ok(());
                }
                Err(ValidationFailure::Rejected(rejection)) => {
                    let _ = connection.disconnect().await;
                    self.publish_status(NodeServiceStatusState::Rejected);
                    self.send_event(NodeServiceEvent::Rejected(rejection))?;
                    return self.wait_rejected_shutdown().await;
                }
            };

            let client = ValidatedRpcClient::new(connection.clone(), node_info, self.retirement_tx.clone());
            let notify: GrpcClientNotify = client.notification_router();
            connection.start_collector(notify).await;
            self.last_validated = Some(status_from_info(client.node_info()));
            self.publish_status(NodeServiceStatusState::Ready);
            if let Err(error) = self.send_event(NodeServiceEvent::RpcPublished(client.clone())) {
                client.cancel().await;
                self.publish_status(NodeServiceStatusState::Stopped);
                return Err(error);
            }

            match self.ready(client, connection, &mut retry_index).await? {
                ReadyExit::Reconnect(reason) => {
                    self.publish_unavailable(reason);
                    if self.wait_retry(&mut retry_index).await? {
                        continue;
                    }
                    return Ok(());
                }
                ReadyExit::Stopped => return Ok(()),
            }
        }
    }

    async fn validate_connection(
        connection: Arc<dyn RpcConnection>,
        network_id: NetworkId,
        consensus: KgiConsensusParams,
        clock: Arc<dyn Clock>,
        status: watch::Sender<NodeServiceStatus>,
        last_validated: Option<ValidatedNodeStatus>,
    ) -> Result<ValidatedNodeInfo, ValidationFailure> {
        let mut server_info = Self::server_info(&connection).await?;
        Self::validate_server_info(network_id, &server_info)?;
        let handle_stop_notify = connection.handle_stop_notify();
        let handle_message_id = connection.handle_message_id();
        if !handle_stop_notify || !handle_message_id {
            return Err(ValidationFailure::Rejected(NodeRejection::MissingNotificationCapabilities {
                handle_stop_notify,
                handle_message_id,
            }));
        }

        let genesis = connection
            .get_blocks(GetBlocksRequest::new(None, false, false))
            .await
            .map_err(|error| validation_rpc_failure("Genesis discovery", error))?
            .block_hashes
            .first()
            .copied()
            .ok_or_else(|| {
                ValidationFailure::Unavailable(NodeUnavailableReason::ValidationFailed {
                    diagnostic: Arc::from("Genesis discovery returned an empty hash vector"),
                })
            })?;

        while !server_info.is_synced {
            status.send_replace(NodeServiceStatus {
                state: NodeServiceStatusState::Unavailable,
                last_validated: last_validated.clone(),
            });
            clock.sleep(IBD_POLL_INTERVAL).await;
            server_info = Self::server_info(&connection).await?;
            Self::validate_server_info(network_id, &server_info)?;
        }

        Ok(ValidatedNodeInfo {
            network_id: server_info.network_id,
            genesis_hash: genesis,
            server_version: server_info.server_version,
            rpc_api_version: server_info.rpc_api_version,
            rpc_api_revision: server_info.rpc_api_revision,
            consensus,
        })
    }

    async fn server_info(connection: &Arc<dyn RpcConnection>) -> Result<ServerInfoObservation, ValidationFailure> {
        connection.get_server_info(GetServerInfoRequest {}).await.map_err(|error| validation_rpc_failure("GetServerInfo", error))
    }

    #[allow(
        clippy::absurd_extreme_comparisons,
        reason = "the compiled revision is currently zero, but the architecture requires a revision floor"
    )]
    fn validate_server_info(network_id: NetworkId, server_info: &ServerInfoObservation) -> Result<(), ValidationFailure> {
        if server_info.network_id != network_id {
            return Err(ValidationFailure::Rejected(NodeRejection::NetworkMismatch {
                expected: network_id,
                observed: server_info.network_id,
            }));
        }
        let compatible = matches!(
            (server_info.rpc_api_version, server_info.rpc_api_revision),
            (Some(version), Some(revision)) if version == RPC_API_VERSION && revision >= RPC_API_REVISION
        );
        if !compatible {
            return Err(ValidationFailure::Rejected(NodeRejection::IncompatibleRpcApi {
                required_version: RPC_API_VERSION,
                minimum_revision: RPC_API_REVISION,
                observed_version: server_info.rpc_api_version,
                observed_revision: server_info.rpc_api_revision,
            }));
        }
        Ok(())
    }

    async fn ready(
        &mut self,
        client: Arc<ValidatedRpcClient>,
        connection: Arc<dyn RpcConnection>,
        retry_index: &mut usize,
    ) -> Result<ReadyExit, NodeServiceError> {
        let clock = self.clock.clone();
        let ready_reset = clock.sleep(READY_BACKOFF_RESET);
        tokio::pin!(ready_reset);
        let mut reset_complete = false;
        loop {
            tokio::select! {
                command = self.commands.recv() => {
                    self.stop_generation(&client, true).await?;
                    self.publish_status(NodeServiceStatusState::Stopped);
                    acknowledge(command);
                    return Ok(ReadyExit::Stopped);
                }
                request = self.retirements.recv() => {
                    let Some(request) = request else {
                        self.stop_generation(&client, true).await?;
                        return Err(NodeServiceError::ControlUnavailable);
                    };
                    if request_targets(&request, &client) {
                        let reason = *request.reason();
                        let event_result = self.stop_generation(&client, false).await;
                        request.complete(event_result.clone());
                        event_result?;
                        return Ok(ReadyExit::Reconnect(NodeUnavailableReason::ConnectionLost {
                            diagnostic: Arc::from(format!("generation retired after {reason:?}")),
                        }));
                    }
                    request.complete(Ok(()));
                }
                result = connection.wait_for_disconnect() => {
                    self.stop_generation(&client, false).await?;
                    let diagnostic = match result {
                        Ok(()) => Arc::from("physical connection closed"),
                        Err(diagnostic) => diagnostic,
                    };
                    return Ok(ReadyExit::Reconnect(NodeUnavailableReason::ConnectionLost { diagnostic }));
                }
                () = &mut ready_reset, if !reset_complete => {
                    *retry_index = 0;
                    reset_complete = true;
                }
            }
        }
    }

    async fn stop_generation(&self, client: &Arc<ValidatedRpcClient>, cancelled: bool) -> Result<(), NodeServiceError> {
        let changed = if cancelled { client.cancel().await } else { client.retire().await };
        if changed {
            self.send_event(NodeServiceEvent::RpcRetired(client.clone()))?;
        }
        Ok(())
    }

    async fn wait_retry(&mut self, retry_index: &mut usize) -> Result<bool, NodeServiceError> {
        let nominal = RETRY_DELAYS[(*retry_index).min(RETRY_DELAYS.len() - 1)];
        *retry_index = retry_index.saturating_add(1);
        let delay = self.jitter.apply(nominal);
        let sleep = self.clock.sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                command = self.commands.recv() => {
                    self.publish_status(NodeServiceStatusState::Stopped);
                    acknowledge(command);
                    return Ok(false);
                }
                request = self.retirements.recv() => complete_stale_retirement(request)?,
                () = &mut sleep => return Ok(true),
            }
        }
    }

    async fn wait_rejected_shutdown(&mut self) -> Result<(), NodeServiceError> {
        loop {
            tokio::select! {
                command = self.commands.recv() => {
                    self.publish_status(NodeServiceStatusState::Stopped);
                    acknowledge(command);
                    return Ok(());
                }
                request = self.retirements.recv() => complete_stale_retirement(request)?,
            }
        }
    }

    async fn finish_without_generation(&mut self, command: Option<ServiceCommand>) -> Result<(), NodeServiceError> {
        self.publish_status(NodeServiceStatusState::Stopped);
        acknowledge(command);
        Ok(())
    }

    fn publish_unavailable(&mut self, reason: NodeUnavailableReason) {
        kaspa_core::warn!("NodeService unavailable: {reason}");
        self.publish_status(NodeServiceStatusState::Unavailable);
    }

    fn publish_status(&self, state: NodeServiceStatusState) {
        self.status.send_replace(NodeServiceStatus { state, last_validated: self.last_validated.clone() });
    }

    fn send_event(&self, event: NodeServiceEvent) -> Result<(), NodeServiceError> {
        self.events.send(event).map_err(|_| NodeServiceError::EventPathClosed)
    }
}

enum ValidationFailure {
    Unavailable(NodeUnavailableReason),
    Rejected(NodeRejection),
}

enum ReadyExit {
    Reconnect(NodeUnavailableReason),
    Stopped,
}

fn validation_rpc_failure(operation: &str, error: kaspa_rpc_core::RpcError) -> ValidationFailure {
    ValidationFailure::Unavailable(NodeUnavailableReason::ValidationFailed {
        diagnostic: Arc::from(format!("{operation} failed: {error}")),
    })
}

fn status_from_info(info: &ValidatedNodeInfo) -> ValidatedNodeStatus {
    ValidatedNodeStatus {
        network_id: info.network_id,
        server_version: info.server_version.clone(),
        rpc_api_version: info.rpc_api_version,
        rpc_api_revision: info.rpc_api_revision,
    }
}

fn request_targets(request: &RetirementRequest, client: &Arc<ValidatedRpcClient>) -> bool {
    request.target().upgrade().is_some_and(|reported| Arc::ptr_eq(&reported, client))
}

fn complete_stale_retirement(request: Option<RetirementRequest>) -> Result<(), NodeServiceError> {
    let request = request.ok_or(NodeServiceError::ControlUnavailable)?;
    request.complete(Ok(()));
    Ok(())
}

fn acknowledge(command: Option<ServiceCommand>) {
    if let Some(ServiceCommand::Shutdown(completion)) = command {
        let _ = completion.send(());
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use kaspa_consensus_core::{
        BlueWorkType,
        network::{NetworkId, NetworkType},
    };
    use kaspa_grpc_client::GrpcClientNotify;
    use kaspa_rpc_core::{
        GetBlockDagInfoRequest, GetBlockDagInfoResponse, GetBlockRequest, GetBlockResponse, GetBlocksRequest, GetBlocksResponse,
        GetServerInfoRequest, GetSinkRequest, GetSinkResponse, GetVirtualChainFromBlockV2Request, GetVirtualChainFromBlockV2Response,
        RpcBlock, RpcError, RpcHeader, RpcResult,
        api::ops::{RPC_API_REVISION, RPC_API_VERSION},
    };
    use kgi_core::timing::{Clock, Jitter, TokioClock};
    use kgi_model::{
        block::BlockHash,
        lifecycle::{NodeServiceStatusState, RecoveryInputKind},
    };
    use tokio::sync::{Semaphore, mpsc, oneshot};
    use url::Url;

    use super::{IBD_POLL_INTERVAL, NodeService, NodeServiceEvent, RETRY_DELAYS};
    use crate::{
        client::{RpcConnection, RpcConnector, ServerInfoObservation},
        consensus::KgiConsensusParams,
        error::{NodeError, NodeRejection},
        runtime::{RetirementReason, RetirementRequest},
    };

    type ConnectionOutcome = Result<Arc<dyn RpcConnection>, Arc<str>>;

    struct ScriptedConnector {
        outcomes: tokio::sync::Mutex<VecDeque<ConnectionOutcome>>,
    }

    impl ScriptedConnector {
        fn new(outcomes: impl IntoIterator<Item = ConnectionOutcome>) -> Self {
            Self { outcomes: tokio::sync::Mutex::new(outcomes.into_iter().collect()) }
        }
    }

    #[async_trait]
    impl RpcConnector for ScriptedConnector {
        async fn connect(&self, _endpoint: &Url) -> Result<Arc<dyn RpcConnection>, Arc<str>> {
            self.outcomes.lock().await.pop_front().expect("scripted connection outcome")
        }
    }

    struct ScriptedConnection {
        server_infos: tokio::sync::Mutex<VecDeque<ServerInfoObservation>>,
        genesis_hashes: Vec<BlockHash>,
        genesis_blocks: Vec<RpcBlock>,
        genesis_failure: Option<Arc<str>>,
        malformed_blocks: bool,
        genesis_requests: Mutex<Vec<(Option<BlockHash>, bool, bool)>>,
        collectors: AtomicUsize,
        disconnects: AtomicUsize,
        disconnect_signal: Arc<Semaphore>,
        handle_stop_notify: bool,
        handle_message_id: bool,
    }

    impl ScriptedConnection {
        fn new(server_infos: impl IntoIterator<Item = ServerInfoObservation>, genesis: BlockHash) -> Self {
            Self {
                server_infos: tokio::sync::Mutex::new(server_infos.into_iter().collect()),
                genesis_hashes: vec![genesis],
                genesis_blocks: Vec::new(),
                genesis_failure: None,
                malformed_blocks: false,
                genesis_requests: Mutex::new(Vec::new()),
                collectors: AtomicUsize::new(0),
                disconnects: AtomicUsize::new(0),
                disconnect_signal: Arc::new(Semaphore::new(0)),
                handle_stop_notify: true,
                handle_message_id: true,
            }
        }

        fn with_capabilities(mut self, handle_stop_notify: bool, handle_message_id: bool) -> Self {
            self.handle_stop_notify = handle_stop_notify;
            self.handle_message_id = handle_message_id;
            self
        }

        fn with_malformed_blocks(mut self) -> Self {
            self.malformed_blocks = true;
            self
        }

        fn with_empty_genesis(mut self) -> Self {
            self.genesis_hashes.clear();
            self
        }

        fn with_genesis_response(mut self, hashes: Vec<BlockHash>, blocks: Vec<RpcBlock>) -> Self {
            self.genesis_hashes = hashes;
            self.genesis_blocks = blocks;
            self
        }

        fn with_genesis_failure(mut self, diagnostic: impl Into<Arc<str>>) -> Self {
            self.genesis_failure = Some(diagnostic.into());
            self
        }

        fn signal_disconnect(&self) {
            self.disconnect_signal.add_permits(1);
        }
    }

    #[async_trait]
    impl RpcConnection for ScriptedConnection {
        async fn get_server_info(&self, _request: GetServerInfoRequest) -> RpcResult<ServerInfoObservation> {
            let mut responses = self.server_infos.lock().await;
            if responses.len() > 1 {
                Ok(responses.pop_front().expect("server-info response"))
            } else {
                Ok(responses.front().expect("server-info response").clone())
            }
        }

        async fn get_block(&self, _request: GetBlockRequest) -> RpcResult<GetBlockResponse> {
            if self.malformed_blocks {
                Err(RpcError::MissingRpcFieldError("RpcBlock".to_string(), "header".to_string()))
            } else {
                Err(RpcError::NotImplemented)
            }
        }

        async fn get_blocks(&self, request: GetBlocksRequest) -> RpcResult<GetBlocksResponse> {
            self.genesis_requests.lock().expect("genesis requests").push((
                request.low_hash,
                request.include_blocks,
                request.include_transactions,
            ));
            if let Some(diagnostic) = &self.genesis_failure {
                return Err(RpcError::General(diagnostic.to_string()));
            }
            Ok(GetBlocksResponse::new(self.genesis_hashes.clone(), self.genesis_blocks.clone()))
        }

        async fn get_block_dag_info(&self, _request: GetBlockDagInfoRequest) -> RpcResult<GetBlockDagInfoResponse> {
            Err(RpcError::NotImplemented)
        }

        async fn get_sink(&self, _request: GetSinkRequest) -> RpcResult<GetSinkResponse> {
            Err(RpcError::NotImplemented)
        }

        async fn get_virtual_chain_from_block_v2(
            &self,
            _request: GetVirtualChainFromBlockV2Request,
        ) -> RpcResult<GetVirtualChainFromBlockV2Response> {
            Err(RpcError::NotImplemented)
        }

        fn handle_stop_notify(&self) -> bool {
            self.handle_stop_notify
        }

        fn handle_message_id(&self) -> bool {
            self.handle_message_id
        }

        async fn start_collector(&self, _notify: GrpcClientNotify) {
            self.collectors.fetch_add(1, Ordering::Relaxed);
        }

        async fn wait_for_disconnect(&self) -> Result<(), Arc<str>> {
            self.disconnect_signal.acquire().await.expect("disconnect signal").forget();
            Ok(())
        }

        async fn disconnect(&self) -> Result<(), Arc<str>> {
            self.disconnects.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct ManualClock {
        sleeps: mpsc::UnboundedSender<Duration>,
        permits: Arc<Semaphore>,
    }

    #[async_trait]
    impl Clock for ManualClock {
        async fn sleep(&self, duration: Duration) {
            self.sleeps.send(duration).expect("sleep observer");
            self.permits.acquire().await.expect("sleep permit").forget();
        }
    }

    struct IdentityJitter;

    impl Jitter for IdentityJitter {
        fn apply(&self, nominal: Duration) -> Duration {
            nominal
        }
    }

    #[tokio::test]
    async fn validates_before_publication_and_shutdown_retires_exact_generation() {
        let network_id = mainnet();
        let genesis = hash(7);
        let mut compatible = server_info(network_id, true);
        let newer_revision = RPC_API_REVISION.checked_add(1).expect("compiled API revision has a higher test value");
        compatible.rpc_api_revision = Some(newer_revision);
        let connection = Arc::new(
            ScriptedConnection::new([compatible], genesis)
                .with_genesis_response(vec![genesis, hash(8)], vec![ignored_genesis_block(hash(9))]),
        );
        let connector = connector([Ok(connection.clone() as Arc<dyn RpcConnection>)]);
        let (service, mut events) = start_service(network_id, connector, Arc::new(TokioClock));

        let published = match recv_event(&mut events).await {
            NodeServiceEvent::RpcPublished(client) => client,
            _ => panic!("expected published generation"),
        };
        assert_eq!(published.node_info().genesis_hash, genesis);
        assert_eq!(published.node_info().rpc_api_version, Some(RPC_API_VERSION));
        assert_eq!(published.node_info().rpc_api_revision, Some(newer_revision));
        assert_eq!(service.status().state, NodeServiceStatusState::Ready);
        assert_eq!(connection.genesis_requests.lock().expect("genesis requests").as_slice(), &[(None, false, false)]);
        assert_eq!(connection.collectors.load(Ordering::Relaxed), 1);

        service.shutdown().await.expect("shutdown");
        let retired = match recv_event(&mut events).await {
            NodeServiceEvent::RpcRetired(client) => client,
            _ => panic!("expected retired generation"),
        };
        assert!(Arc::ptr_eq(&published, &retired));
        assert_eq!(service.status().state, NodeServiceStatusState::Stopped);
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
        service.shutdown().await.expect("idempotent shutdown");
    }

    #[tokio::test]
    async fn closed_event_path_is_fatal_and_releases_the_unpublished_generation() {
        let network_id = mainnet();
        let connection = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)));
        let connector = connector([Ok(connection.clone() as Arc<dyn RpcConnection>)]);
        let (service, events) = start_service(network_id, connector, Arc::new(TokioClock));
        drop(events);

        let mut completion = service.completion.clone();
        tokio::time::timeout(Duration::from_secs(1), async {
            while completion.borrow().is_none() {
                completion.changed().await.expect("completion path");
            }
        })
        .await
        .expect("service completion timeout");
        assert_eq!(service.shutdown().await, Err(super::NodeServiceError::EventPathClosed));
        assert_eq!(service.status().state, NodeServiceStatusState::Stopped);
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn incompatible_api_and_missing_capabilities_are_terminal_rejections() {
        let network_id = mainnet();
        let mut incompatible = server_info(network_id, true);
        let higher_version = RPC_API_VERSION.checked_add(1).expect("compiled API version has a higher test value");
        incompatible.rpc_api_version = Some(higher_version);
        let mut cases = vec![
            (
                Arc::new(ScriptedConnection::new([incompatible], hash(1))),
                NodeRejection::IncompatibleRpcApi {
                    required_version: RPC_API_VERSION,
                    minimum_revision: RPC_API_REVISION,
                    observed_version: Some(higher_version),
                    observed_revision: Some(RPC_API_REVISION),
                },
            ),
            (
                Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)).with_capabilities(false, true)),
                NodeRejection::MissingNotificationCapabilities { handle_stop_notify: false, handle_message_id: true },
            ),
        ];
        let observed_network = NetworkId::with_suffix(NetworkType::Testnet, 10);
        cases.push((
            Arc::new(ScriptedConnection::new([server_info(observed_network, true)], hash(1))),
            NodeRejection::NetworkMismatch { expected: network_id, observed: observed_network },
        ));
        if let Some(lower_version) = RPC_API_VERSION.checked_sub(1) {
            let mut observation = server_info(network_id, true);
            observation.rpc_api_version = Some(lower_version);
            cases.push((
                Arc::new(ScriptedConnection::new([observation], hash(1))),
                NodeRejection::IncompatibleRpcApi {
                    required_version: RPC_API_VERSION,
                    minimum_revision: RPC_API_REVISION,
                    observed_version: Some(lower_version),
                    observed_revision: Some(RPC_API_REVISION),
                },
            ));
        }
        if let Some(lower_revision) = RPC_API_REVISION.checked_sub(1) {
            let mut observation = server_info(network_id, true);
            observation.rpc_api_revision = Some(lower_revision);
            cases.push((
                Arc::new(ScriptedConnection::new([observation], hash(1))),
                NodeRejection::IncompatibleRpcApi {
                    required_version: RPC_API_VERSION,
                    minimum_revision: RPC_API_REVISION,
                    observed_version: Some(RPC_API_VERSION),
                    observed_revision: Some(lower_revision),
                },
            ));
        }
        for (missing_version, missing_revision) in [(None, Some(RPC_API_REVISION)), (Some(RPC_API_VERSION), None)] {
            let mut observation = server_info(network_id, true);
            observation.rpc_api_version = missing_version;
            observation.rpc_api_revision = missing_revision;
            cases.push((
                Arc::new(ScriptedConnection::new([observation], hash(1))),
                NodeRejection::IncompatibleRpcApi {
                    required_version: RPC_API_VERSION,
                    minimum_revision: RPC_API_REVISION,
                    observed_version: missing_version,
                    observed_revision: missing_revision,
                },
            ));
        }

        for (connection, expected) in cases {
            let connector = connector([Ok(connection as Arc<dyn RpcConnection>)]);
            let (service, mut events) = start_service(network_id, connector, Arc::new(TokioClock));
            match recv_event(&mut events).await {
                NodeServiceEvent::Rejected(actual) => assert_eq!(actual, expected),
                _ => panic!("expected rejection"),
            }
            assert_eq!(service.status().state, NodeServiceStatusState::Rejected);
            service.shutdown().await.expect("shutdown rejected service");
            assert_eq!(service.status().state, NodeServiceStatusState::Stopped);
        }
    }

    #[tokio::test]
    async fn ibd_blocks_publication_until_a_complete_synced_observation() {
        let network_id = mainnet();
        let connection = Arc::new(ScriptedConnection::new([server_info(network_id, false), server_info(network_id, true)], hash(1)));
        let (clock, mut sleeps, permits) = manual_clock();
        let connector = connector([Ok(connection as Arc<dyn RpcConnection>)]);
        let (service, mut events) = start_service(network_id, connector, clock);

        assert_eq!(sleeps.recv().await.expect("IBD sleep"), IBD_POLL_INTERVAL);
        assert!(matches!(events.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        assert_eq!(service.status().state, NodeServiceStatusState::Unavailable);
        permits.add_permits(1);
        assert!(matches!(recv_event(&mut events).await, NodeServiceEvent::RpcPublished(_)));

        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn malformed_or_failed_genesis_discovery_is_transient_and_never_publishes() {
        let network_id = mainnet();
        let connections = [
            Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)).with_empty_genesis()),
            Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)).with_genesis_failure("Genesis RPC failed")),
        ];

        for connection in connections {
            let (clock, mut sleeps, _permits) = manual_clock();
            let connector = connector([Ok(connection as Arc<dyn RpcConnection>)]);
            let (service, mut events) = start_service(network_id, connector, clock);

            assert_eq!(sleeps.recv().await.expect("validation retry"), Duration::from_secs(1));
            assert!(matches!(events.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
            assert_eq!(service.status().state, NodeServiceStatusState::Unavailable);
            service.shutdown().await.expect("shutdown");
        }
    }

    #[tokio::test]
    async fn transient_connection_failures_follow_the_nominal_retry_sequence() {
        let network_id = mainnet();
        let connection = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)));
        let mut outcomes = RETRY_DELAYS.iter().map(|_| Err(Arc::from("connect failed"))).collect::<Vec<ConnectionOutcome>>();
        outcomes.push(Ok(connection as Arc<dyn RpcConnection>));
        let connector = connector(outcomes);
        let (clock, mut sleeps, permits) = manual_clock();
        let (service, mut events) = start_service(network_id, connector, clock);

        for expected in RETRY_DELAYS {
            assert_eq!(sleeps.recv().await.expect("retry sleep"), expected);
            permits.add_permits(1);
        }
        assert!(matches!(recv_event(&mut events).await, NodeServiceEvent::RpcPublished(_)));
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn short_ready_period_does_not_reset_retry_progress() {
        let network_id = mainnet();
        let first = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)));
        let replacement = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)));
        let connector = connector([
            Err(Arc::from("initial failure")),
            Ok(first.clone() as Arc<dyn RpcConnection>),
            Ok(replacement as Arc<dyn RpcConnection>),
        ]);
        let (clock, mut sleeps, permits) = manual_clock();
        let (service, mut events) = start_service(network_id, connector, clock);

        assert_eq!(sleeps.recv().await.expect("first retry"), Duration::from_secs(1));
        permits.add_permits(1);
        assert!(matches!(recv_event(&mut events).await, NodeServiceEvent::RpcPublished(_)));
        assert_eq!(sleeps.recv().await.expect("Ready reset timer"), Duration::from_secs(60));
        first.signal_disconnect();
        assert!(matches!(recv_event(&mut events).await, NodeServiceEvent::RpcRetired(_)));
        assert_eq!(sleeps.recv().await.expect("second retry"), Duration::from_secs(2));
        permits.add_permits(1);
        assert!(matches!(recv_event(&mut events).await, NodeServiceEvent::RpcPublished(_)));

        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn replacement_uses_a_new_identity_and_ignores_repeated_stale_retirement() {
        let network_id = mainnet();
        let first_connection = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)));
        let second_connection = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(2)));
        let connector = connector([
            Ok(first_connection.clone() as Arc<dyn RpcConnection>),
            Ok(second_connection.clone() as Arc<dyn RpcConnection>),
        ]);
        let (clock, mut sleeps, permits) = manual_clock();
        let (service, mut events) = start_service(network_id, connector, clock);

        let first = match recv_event(&mut events).await {
            NodeServiceEvent::RpcPublished(client) => client,
            _ => panic!("expected first publication"),
        };
        assert_eq!(first.node_info().genesis_hash, hash(1));
        assert_eq!(sleeps.recv().await.expect("Ready reset timer"), Duration::from_secs(60));

        first_connection.signal_disconnect();
        let retired = match recv_event(&mut events).await {
            NodeServiceEvent::RpcRetired(client) => client,
            _ => panic!("expected first retirement"),
        };
        assert!(Arc::ptr_eq(&first, &retired));
        assert_eq!(sleeps.recv().await.expect("replacement retry"), Duration::from_secs(1));
        permits.add_permits(1);

        let second = match recv_event(&mut events).await {
            NodeServiceEvent::RpcPublished(client) => client,
            _ => panic!("expected replacement publication"),
        };
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(second.node_info().genesis_hash, hash(2));

        let retirements = first.retirement_sender();
        for _ in 0..2 {
            let (completion_tx, completion_rx) = oneshot::channel();
            retirements
                .send(RetirementRequest::with_reason(
                    Arc::downgrade(&first),
                    RetirementReason::MalformedRecoveryInput(RecoveryInputKind::MalformedGetBlock),
                    completion_tx,
                ))
                .expect("retirement control path");
            completion_rx.await.expect("stale retirement acknowledgement").expect("stale retirement completion");
        }

        assert!(matches!(events.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        assert_eq!(first.full_block(hash(9)).await, Err(NodeError::GenerationLost));
        assert!(matches!(second.full_block(hash(9)).await, Err(NodeError::RpcRequestFailed { .. })));
        assert_eq!(service.status().state, NodeServiceStatusState::Ready);
        assert_eq!(second_connection.disconnects.load(Ordering::Relaxed), 0);

        service.shutdown().await.expect("shutdown");
        let retired = match recv_event(&mut events).await {
            NodeServiceEvent::RpcRetired(client) => client,
            _ => panic!("expected replacement retirement"),
        };
        assert!(Arc::ptr_eq(&second, &retired));
        assert!(matches!(events.try_recv(), Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)));
    }

    #[tokio::test]
    async fn sixty_continuous_ready_seconds_reset_retry_progress() {
        let network_id = mainnet();
        let first = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)));
        let replacement = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)));
        let connector = connector([
            Err(Arc::from("initial failure")),
            Ok(first.clone() as Arc<dyn RpcConnection>),
            Ok(replacement as Arc<dyn RpcConnection>),
        ]);
        let (clock, mut sleeps, permits) = manual_clock();
        let (service, mut events) = start_service(network_id, connector, clock);

        assert_eq!(sleeps.recv().await.expect("first retry"), Duration::from_secs(1));
        permits.add_permits(1);
        assert!(matches!(recv_event(&mut events).await, NodeServiceEvent::RpcPublished(_)));
        assert_eq!(sleeps.recv().await.expect("Ready reset timer"), Duration::from_secs(60));
        permits.add_permits(1);
        tokio::task::yield_now().await;
        first.signal_disconnect();
        assert!(matches!(recv_event(&mut events).await, NodeServiceEvent::RpcRetired(_)));
        assert_eq!(sleeps.recv().await.expect("reset retry"), Duration::from_secs(1));

        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn malformed_runtime_response_is_retired_before_the_typed_result_returns() {
        let network_id = mainnet();
        let connection = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)).with_malformed_blocks());
        let connector = connector([Ok(connection as Arc<dyn RpcConnection>)]);
        let (service, mut events) = start_service(network_id, connector, Arc::new(TokioClock));
        let published = match recv_event(&mut events).await {
            NodeServiceEvent::RpcPublished(client) => client,
            _ => panic!("expected published generation"),
        };
        let operation = tokio::spawn({
            let published = published.clone();
            async move { published.full_block(hash(9)).await }
        });

        let retired = match recv_event(&mut events).await {
            NodeServiceEvent::RpcRetired(client) => client,
            _ => panic!("expected retired generation"),
        };
        assert!(Arc::ptr_eq(&published, &retired));
        assert_eq!(
            operation.await.expect("operation task"),
            Err(NodeError::RecoveryInputInvalid(kgi_model::lifecycle::RecoveryInputKind::MalformedGetBlock))
        );
        assert_eq!(service.status().state, NodeServiceStatusState::Unavailable);
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn malformed_runtime_response_reports_retirement_event_path_failure() {
        let network_id = mainnet();
        let connection = Arc::new(ScriptedConnection::new([server_info(network_id, true)], hash(1)).with_malformed_blocks());
        let connector = connector([Ok(connection as Arc<dyn RpcConnection>)]);
        let (service, mut events) = start_service(network_id, connector, Arc::new(TokioClock));
        let published = match recv_event(&mut events).await {
            NodeServiceEvent::RpcPublished(client) => client,
            _ => panic!("expected published generation"),
        };
        drop(events);

        assert_eq!(published.full_block(hash(9)).await, Err(NodeError::RetirementControlUnavailable));
        assert_eq!(service.shutdown().await, Err(super::NodeServiceError::EventPathClosed));
    }

    fn start_service(
        network_id: NetworkId,
        connector: Arc<dyn RpcConnector>,
        clock: Arc<dyn Clock>,
    ) -> (Arc<NodeService>, super::NodeServiceEventReceiver) {
        let consensus = KgiConsensusParams::resolve(network_id, None).expect("mainnet consensus");
        NodeService::start_with_dependencies(
            Url::parse("grpc://127.0.0.1:16110").expect("endpoint"),
            network_id,
            consensus,
            connector,
            clock,
            Arc::new(IdentityJitter),
        )
    }

    fn connector(outcomes: impl IntoIterator<Item = ConnectionOutcome>) -> Arc<dyn RpcConnector> {
        Arc::new(ScriptedConnector::new(outcomes))
    }

    fn manual_clock() -> (Arc<dyn Clock>, mpsc::UnboundedReceiver<Duration>, Arc<Semaphore>) {
        let (sleep_tx, sleep_rx) = mpsc::unbounded_channel();
        let permits = Arc::new(Semaphore::new(0));
        (Arc::new(ManualClock { sleeps: sleep_tx, permits: permits.clone() }), sleep_rx, permits)
    }

    async fn recv_event(events: &mut super::NodeServiceEventReceiver) -> NodeServiceEvent {
        tokio::time::timeout(Duration::from_secs(1), events.recv()).await.expect("event timeout").expect("event path closed")
    }

    fn server_info(network_id: NetworkId, is_synced: bool) -> ServerInfoObservation {
        ServerInfoObservation {
            rpc_api_version: Some(RPC_API_VERSION),
            rpc_api_revision: Some(RPC_API_REVISION),
            server_version: "test-node".to_string(),
            network_id,
            is_synced,
        }
    }

    fn mainnet() -> NetworkId {
        NetworkId::new(NetworkType::Mainnet)
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn ignored_genesis_block(hash_value: BlockHash) -> RpcBlock {
        RpcBlock {
            header: RpcHeader {
                hash: hash_value,
                version: 0,
                parents_by_level: Vec::new(),
                hash_merkle_root: hash(10),
                accepted_id_merkle_root: hash(11),
                utxo_commitment: hash(12),
                timestamp: 0,
                bits: 0,
                nonce: 0,
                daa_score: 0,
                blue_work: BlueWorkType::default(),
                blue_score: 0,
                pruning_point: hash(13),
            },
            transactions: Vec::new(),
            verbose_data: None,
        }
    }
}
