use std::{
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};

use kaspa_consensus_core::{blockhash::ORIGIN, errors::consensus::ConsensusError, network::NetworkId};
use kaspa_rpc_core::{
    GetBlockDagInfoRequest, GetBlockRequest, GetBlocksRequest, GetSinkRequest, GetVirtualChainFromBlockV2Request,
    RpcDataVerbosityLevel, RpcError, RpcResult,
};
use kgi_model::{
    block::{BlockHash, ValidatedNodeBlock, ValidatedRecoveryHeader},
    lifecycle::{FaultKind, RecoveryInputKind},
    vspc::VspcChange,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

use crate::{
    client::RpcConnection,
    consensus::KgiConsensusParams,
    error::NodeError,
    normalization::{ResponseNormalizationError, ResponseNormalizer},
    notification::NotificationRouter,
    runtime::{RetirementReason, RetirementRequest, RetirementSender},
};

const MAX_RPC_CONCURRENCY: usize = 32;

const GENERATION_ACTIVE: u8 = 0;
const GENERATION_LOST: u8 = 1;
const GENERATION_CANCELLED: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubscriptionState {
    Disabled,
    Activating,
    Enabled,
    Disabling,
    Retired,
}

/// Identity and assumptions attached to one validated physical RPC connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedNodeInfo {
    pub network_id: NetworkId,
    pub genesis_hash: BlockHash,
    pub server_version: String,
    pub rpc_api_version: Option<u16>,
    pub rpc_api_revision: Option<u16>,
    pub consensus: KgiConsensusParams,
}

/// Minimal normalized marker used by Catchup sink tracking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatchupSinkSample {
    pub hash: BlockHash,
    pub daa_score: u64,
}

/// Current bounded-operation occupancy for one validated generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcOperationCounts {
    pub active: usize,
    pub waiting: usize,
}

/// Typed notification-routing failure reported to the active processing session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotificationFault {
    kind: FaultKind,
    diagnostic: Arc<str>,
}

impl NotificationFault {
    pub(crate) fn new(kind: FaultKind, diagnostic: impl Into<Arc<str>>) -> Self {
        Self { kind, diagnostic: diagnostic.into() }
    }

    /// Returns the semantic fault classification.
    #[must_use]
    pub const fn kind(&self) -> FaultKind {
        self.kind
    }

    /// Returns non-semantic diagnostic context.
    #[must_use]
    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }
}

/// Bounded processor destinations and reliable fault path for one session.
#[derive(Debug)]
pub struct NotificationChannels {
    blocks: mpsc::Sender<ValidatedNodeBlock>,
    vspc: mpsc::Sender<VspcChange>,
    faults: mpsc::UnboundedSender<NotificationFault>,
}

impl NotificationChannels {
    /// Groups the session endpoints installed before remote subscription starts.
    #[must_use]
    pub fn new(
        blocks: mpsc::Sender<ValidatedNodeBlock>,
        vspc: mpsc::Sender<VspcChange>,
        faults: mpsc::UnboundedSender<NotificationFault>,
    ) -> Self {
        Self { blocks, vspc, faults }
    }

    pub(crate) const fn blocks(&self) -> &mpsc::Sender<ValidatedNodeBlock> {
        &self.blocks
    }

    pub(crate) const fn vspc(&self) -> &mpsc::Sender<VspcChange> {
        &self.vspc
    }

    pub(crate) const fn faults(&self) -> &mpsc::UnboundedSender<NotificationFault> {
        &self.faults
    }
}

/// RPC capability bound permanently to one validated physical connection.
pub struct ValidatedRpcClient {
    connection: Arc<dyn RpcConnection>,
    node_info: ValidatedNodeInfo,
    normalizer: Arc<ResponseNormalizer>,
    notification_router: Arc<NotificationRouter>,
    subscription_state: Mutex<SubscriptionState>,
    subscription_transition: Arc<AsyncMutex<()>>,
    #[cfg(test)]
    activation_publication_gate: Mutex<Option<(mpsc::UnboundedSender<()>, Arc<Semaphore>)>>,
    #[cfg(test)]
    operation_completion_gate: Mutex<Option<(mpsc::UnboundedSender<()>, Arc<Semaphore>)>>,
    admission: AtomicU8,
    admission_tx: watch::Sender<u8>,
    permits: Arc<Semaphore>,
    active: AtomicUsize,
    waiting: AtomicUsize,
    retirement_tx: RetirementSender,
    self_weak: Weak<Self>,
}

impl ValidatedRpcClient {
    pub(crate) fn new(connection: Arc<dyn RpcConnection>, node_info: ValidatedNodeInfo, retirement_tx: RetirementSender) -> Arc<Self> {
        let normalizer = Arc::new(ResponseNormalizer::new(node_info.genesis_hash));
        let notification_router = Arc::new(NotificationRouter::new(normalizer.clone()));
        Arc::new_cyclic(|self_weak| {
            let (admission_tx, _) = watch::channel(GENERATION_ACTIVE);
            Self {
                connection,
                normalizer,
                notification_router,
                subscription_state: Mutex::new(SubscriptionState::Disabled),
                subscription_transition: Arc::new(AsyncMutex::new(())),
                #[cfg(test)]
                activation_publication_gate: Mutex::new(None),
                #[cfg(test)]
                operation_completion_gate: Mutex::new(None),
                node_info,
                admission: AtomicU8::new(GENERATION_ACTIVE),
                admission_tx,
                permits: Arc::new(Semaphore::new(MAX_RPC_CONCURRENCY)),
                active: AtomicUsize::new(0),
                waiting: AtomicUsize::new(0),
                retirement_tx,
                self_weak: self_weak.clone(),
            }
        })
    }

    /// Returns the immutable validation result for this generation.
    #[must_use]
    pub const fn node_info(&self) -> &ValidatedNodeInfo {
        &self.node_info
    }

    /// Returns the current active and permit-waiting operation counts.
    #[must_use]
    pub fn operation_counts(&self) -> RpcOperationCounts {
        RpcOperationCounts { active: self.active.load(Ordering::Relaxed), waiting: self.waiting.load(Ordering::Relaxed) }
    }

    pub(crate) fn notification_router(&self) -> Arc<NotificationRouter> {
        self.notification_router.clone()
    }

    #[cfg(test)]
    fn subscription_state(&self) -> SubscriptionState {
        *self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(test)]
    fn set_activation_publication_gate(&self, reached: mpsc::UnboundedSender<()>, gate: Arc<Semaphore>) {
        *self.activation_publication_gate.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((reached, gate));
    }

    #[cfg(test)]
    pub(crate) fn retirement_sender(&self) -> RetirementSender {
        self.retirement_tx.clone()
    }

    #[cfg(test)]
    fn set_operation_completion_gate(&self, reached: mpsc::UnboundedSender<()>, gate: Arc<Semaphore>) {
        *self.operation_completion_gate.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((reached, gate));
    }

    #[cfg(test)]
    async fn wait_for_activation_publication(&self) {
        let publication_gate = self.activation_publication_gate.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        if let Some((reached, gate)) = publication_gate {
            reached.send(()).expect("activation-publication observer");
            gate.acquire().await.expect("activation-publication gate").forget();
        }
    }

    #[cfg(test)]
    async fn wait_for_operation_completion(&self) {
        let completion_gate = self.operation_completion_gate.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take();
        if let Some((reached, gate)) = completion_gate {
            reached.send(()).expect("operation-completion observer");
            gate.acquire().await.expect("operation-completion gate").forget();
        }
    }

    /// Starts both remote notification subscriptions and then enables local routing.
    pub async fn activate_notifications(&self, channels: NotificationChannels) -> Result<(), NodeError> {
        self.require_active()?;
        if *self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) != SubscriptionState::Disabled {
            return Err(NodeError::InvalidSubscriptionState);
        }
        let transition_guard = self.subscription_transition.clone().lock_owned().await;
        self.require_active()?;
        {
            let mut state = self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if *state != SubscriptionState::Disabled {
                return Err(NodeError::InvalidSubscriptionState);
            }
            *state = SubscriptionState::Activating;
        }
        if !self.notification_router.install(channels) {
            self.set_subscription_state_if_not_retired(SubscriptionState::Disabled);
            return Err(NodeError::InvalidSubscriptionState);
        }

        let client = self.self_weak.upgrade().ok_or(NodeError::GenerationLost)?;
        let cancellation = Arc::new(ActivationCancellation::new());
        let mut cancellation_guard = ActivationCancellationGuard::new(cancellation.clone());
        let transition = tokio::spawn(async move { client.complete_activation(cancellation, transition_guard).await });
        let result = transition.await;
        cancellation_guard.disarm();
        self.finish_subscription_transition(result).await
    }

    async fn complete_activation(
        self: Arc<Self>,
        cancellation: Arc<ActivationCancellation>,
        _transition: OwnedMutexGuard<()>,
    ) -> Result<(), NodeError> {
        if self.activation_deactivation_requested()? {
            return self.complete_activation_deactivation(None).await;
        }
        if cancellation.is_abandoned() {
            self.finish_failed_activation();
            return Err(NodeError::Cancelled);
        }

        let block_start = self.subscription_call(self.connection.start_block_added()).await;
        if self.activation_deactivation_requested()? {
            return self.complete_activation_deactivation(None).await;
        }
        if let Err(error) = block_start {
            return self.finish_activation_failure(error).await;
        }
        if cancellation.is_abandoned() {
            return self.cancel_activation_after_block_start().await;
        }

        let vspc_start = self.subscription_call(self.connection.start_virtual_chain_changed()).await;
        if self.activation_deactivation_requested()? {
            return self.complete_activation_deactivation(None).await;
        }
        if let Err(start_error) = vspc_start {
            if is_inactive_error(&start_error) {
                self.finish_failed_activation();
                return Err(start_error);
            }
            let rollback = self.subscription_call(self.connection.stop_block_added()).await;
            if self.activation_deactivation_requested()? {
                return self.complete_activation_deactivation(Some(rollback)).await;
            }
            if let Err(rollback_error) = rollback {
                if is_inactive_error(&rollback_error) {
                    return Err(rollback_error);
                }
                self.request_retirement(RetirementReason::SubscriptionControlFailure).await?;
                return Err(NodeError::GenerationLost);
            }
            return self.finish_activation_failure(start_error).await;
        }

        #[cfg(test)]
        self.wait_for_activation_publication().await;
        self.require_active()?;
        let deactivating = {
            let abandonment = cancellation.lock();
            let mut state = self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            match *state {
                SubscriptionState::Activating if !*abandonment => {
                    if !self.notification_router.enable() {
                        return Err(self.inactive_error());
                    }
                    *state = SubscriptionState::Enabled;
                    return Ok(());
                }
                SubscriptionState::Retired => return Err(self.inactive_error()),
                SubscriptionState::Disabling => true,
                SubscriptionState::Disabled | SubscriptionState::Enabled | SubscriptionState::Activating => false,
            }
        };
        if deactivating { self.complete_activation_deactivation(None).await } else { self.cancel_activation_after_both_starts().await }
    }

    /// Disables local routing immediately and then stops both remote subscriptions.
    pub async fn disable_notifications(&self) -> Result<(), NodeError> {
        let already_disabled = {
            let mut state = self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            match *state {
                SubscriptionState::Disabled => true,
                SubscriptionState::Enabled | SubscriptionState::Activating => {
                    *state = SubscriptionState::Disabling;
                    false
                }
                SubscriptionState::Retired => return Err(self.inactive_error()),
                SubscriptionState::Disabling => return Err(NodeError::InvalidSubscriptionState),
            }
        };
        if already_disabled {
            self.notification_router.clear();
            return Ok(());
        }
        self.notification_router.disable();

        let client = self.self_weak.upgrade().ok_or(NodeError::GenerationLost)?;
        let transition = tokio::spawn(async move { client.complete_disable().await });
        self.finish_subscription_transition(transition.await).await
    }

    async fn complete_disable(self: Arc<Self>) -> Result<(), NodeError> {
        let _transition = self.subscription_transition.lock().await;
        match *self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) {
            SubscriptionState::Disabled => {
                self.notification_router.clear();
                return Ok(());
            }
            SubscriptionState::Retired => return Err(self.inactive_error()),
            SubscriptionState::Disabling => {}
            SubscriptionState::Activating | SubscriptionState::Enabled => return Err(NodeError::InvalidSubscriptionState),
        }

        let block_result = self.subscription_call(self.connection.stop_block_added()).await;
        let vspc_result = self.subscription_call(self.connection.stop_virtual_chain_changed()).await;
        if let Some(error) = [&block_result, &vspc_result].into_iter().find_map(|result| match result {
            Err(error) if is_inactive_error(error) => Some(error.clone()),
            _ => None,
        }) {
            return Err(error);
        }
        if block_result.and(vspc_result).is_err() {
            self.request_retirement(RetirementReason::SubscriptionControlFailure).await?;
            return Err(NodeError::GenerationLost);
        }

        self.require_active()?;
        self.notification_router.clear();
        self.set_subscription_state_if_not_retired(SubscriptionState::Disabled);
        Ok(())
    }

    async fn finish_activation_failure(&self, error: NodeError) -> Result<(), NodeError> {
        let deactivating = {
            let mut state = self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            match *state {
                SubscriptionState::Activating => {
                    *state = SubscriptionState::Disabled;
                    false
                }
                SubscriptionState::Disabling => true,
                SubscriptionState::Retired => return Err(self.inactive_error()),
                SubscriptionState::Disabled | SubscriptionState::Enabled => return Err(NodeError::InvalidSubscriptionState),
            }
        };
        if deactivating {
            return self.complete_activation_deactivation(None).await;
        }
        self.notification_router.disable();
        self.notification_router.clear();
        Err(error)
    }

    async fn complete_activation_deactivation(&self, block_result: Option<Result<(), NodeError>>) -> Result<(), NodeError> {
        self.notification_router.disable();
        let block_result = match block_result {
            Some(result) => result,
            None => self.subscription_call(self.connection.stop_block_added()).await,
        };
        let vspc_result = self.subscription_call(self.connection.stop_virtual_chain_changed()).await;
        if let Some(error) = [&block_result, &vspc_result].into_iter().find_map(|result| match result {
            Err(error) if is_inactive_error(error) => Some(error.clone()),
            _ => None,
        }) {
            return Err(error);
        }
        if block_result.and(vspc_result).is_err() {
            self.request_retirement(RetirementReason::SubscriptionControlFailure).await?;
            return Err(NodeError::GenerationLost);
        }
        self.require_active()?;
        self.notification_router.clear();
        self.set_subscription_state_if_not_retired(SubscriptionState::Disabled);
        Err(NodeError::Cancelled)
    }

    async fn cancel_activation_after_block_start(&self) -> Result<(), NodeError> {
        match self.subscription_call(self.connection.stop_block_added()).await {
            Ok(()) => {
                self.finish_failed_activation();
                Err(NodeError::Cancelled)
            }
            Err(error) if is_inactive_error(&error) => Err(error),
            Err(_) => {
                self.notification_router.disable();
                self.request_retirement(RetirementReason::SubscriptionControlFailure).await?;
                Err(NodeError::GenerationLost)
            }
        }
    }

    async fn cancel_activation_after_both_starts(&self) -> Result<(), NodeError> {
        self.complete_activation_deactivation(None).await
    }

    fn activation_deactivation_requested(&self) -> Result<bool, NodeError> {
        match *self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) {
            SubscriptionState::Activating => Ok(false),
            SubscriptionState::Disabling => Ok(true),
            SubscriptionState::Retired => Err(self.inactive_error()),
            SubscriptionState::Disabled | SubscriptionState::Enabled => Err(NodeError::InvalidSubscriptionState),
        }
    }

    async fn finish_subscription_transition(
        &self,
        result: Result<Result<(), NodeError>, tokio::task::JoinError>,
    ) -> Result<(), NodeError> {
        match result {
            Ok(result) => result,
            Err(error) => {
                self.notification_router.disable();
                kaspa_core::warn!("notification subscription transition task failed: {error}");
                self.request_retirement(RetirementReason::SubscriptionControlFailure).await?;
                Err(NodeError::GenerationLost)
            }
        }
    }

    /// Obtains and normalizes the current pruning-point block.
    pub async fn current_pruning_point_block(&self) -> Result<ValidatedNodeBlock, NodeError> {
        let mut operation = self.begin_operation().await?;
        let dag_info = self
            .call(&mut operation.admission, self.connection.get_block_dag_info(GetBlockDagInfoRequest {}))
            .await
            .map_err(map_opaque_rpc_error)?;
        let pruning_point_hash = dag_info.pruning_point_hash;
        if pruning_point_hash == ORIGIN {
            return self.return_malformed(&operation, RecoveryInputKind::MalformedPruningPointResponse).await;
        }
        let block = match self
            .call(&mut operation.admission, self.connection.get_block(GetBlockRequest::new(pruning_point_hash, false)))
            .await
        {
            Ok(response) => response.block,
            Err(RawCallError::Rpc(error)) => {
                return match classify_get_block_error(pruning_point_hash, error) {
                    GetBlockCallError::NotFound | GetBlockCallError::Malformed => {
                        self.return_malformed(&operation, RecoveryInputKind::MalformedPruningPointResponse).await
                    }
                    GetBlockCallError::Opaque(error) => Err(error),
                };
            }
            Err(RawCallError::Node(error)) => return Err(error),
        };
        self.finish_normalized(&operation, self.normalizer.pruning_point_block(pruning_point_hash, block)).await
    }

    /// Obtains the current Catchup sink and its normalized DAA score.
    pub async fn catchup_sink_sample(&self) -> Result<CatchupSinkSample, NodeError> {
        let mut operation = self.begin_operation().await?;
        let sink =
            self.call(&mut operation.admission, self.connection.get_sink(GetSinkRequest {})).await.map_err(map_opaque_rpc_error)?.sink;
        if sink == ORIGIN {
            return self.return_malformed(&operation, RecoveryInputKind::MalformedCatchupSinkResponse).await;
        }
        let block = match self.call(&mut operation.admission, self.connection.get_block(GetBlockRequest::new(sink, false))).await {
            Ok(response) => response.block,
            Err(RawCallError::Rpc(error)) => {
                return match classify_get_block_error(sink, error) {
                    GetBlockCallError::NotFound | GetBlockCallError::Malformed => {
                        self.return_malformed(&operation, RecoveryInputKind::MalformedCatchupSinkResponse).await
                    }
                    GetBlockCallError::Opaque(error) => Err(error),
                };
            }
            Err(RawCallError::Node(error)) => return Err(error),
        };
        self.finish_normalized(&operation, self.normalizer.catchup_sink_sample(sink, &block)).await
    }

    /// Obtains one inclusive-low-hash GetBlocks page and strips its anchor.
    pub async fn get_blocks(&self, low_hash: BlockHash) -> Result<Vec<ValidatedNodeBlock>, NodeError> {
        let mut operation = self.begin_operation().await?;
        let response = match self
            .call(&mut operation.admission, self.connection.get_blocks(GetBlocksRequest::new(Some(low_hash), true, false)))
            .await
        {
            Ok(response) => response,
            Err(RawCallError::Rpc(RpcError::MissingRpcFieldError(_, _))) => {
                return self.return_malformed(&operation, RecoveryInputKind::MalformedGetBlocks).await;
            }
            Err(error) => return Err(map_opaque_rpc_error(error)),
        };
        self.finish_normalized(&operation, self.normalizer.get_blocks(low_hash, response)).await
    }

    /// Obtains one minimally verbose VSPC V2 response.
    pub async fn virtual_chain_from(&self, low_hash: BlockHash) -> Result<VspcChange, NodeError> {
        let mut operation = self.begin_operation().await?;
        let request = GetVirtualChainFromBlockV2Request::new(low_hash, Some(RpcDataVerbosityLevel::None), None);
        let response = self
            .call(&mut operation.admission, self.connection.get_virtual_chain_from_block_v2(request))
            .await
            .map_err(map_opaque_rpc_error)?;
        self.finish_normalized(&operation, self.normalizer.virtual_chain(low_hash, &response)).await
    }

    /// Obtains the normalized header-only fields needed by Resync preparation.
    pub async fn recovery_header(&self, hash: BlockHash) -> Result<ValidatedRecoveryHeader, NodeError> {
        let operation = self.begin_operation().await?;
        let block = self.get_block(&operation, hash).await?;
        self.finish_normalized(&operation, self.normalizer.recovery_header(hash, &block)).await
    }

    /// Obtains one normalized full block without transactions.
    pub async fn full_block(&self, hash: BlockHash) -> Result<ValidatedNodeBlock, NodeError> {
        let operation = self.begin_operation().await?;
        let block = self.get_block(&operation, hash).await?;
        self.finish_normalized(&operation, self.normalizer.full_block(hash, block)).await
    }

    async fn get_block(&self, operation: &OperationGuard<'_>, hash: BlockHash) -> Result<kaspa_rpc_core::RpcBlock, NodeError> {
        let mut admission = operation.admission.clone();
        match self.call(&mut admission, self.connection.get_block(GetBlockRequest::new(hash, false))).await {
            Ok(response) => Ok(response.block),
            Err(RawCallError::Rpc(error)) => match classify_get_block_error(hash, error) {
                GetBlockCallError::NotFound => Err(NodeError::BlockNotFound { hash }),
                GetBlockCallError::Malformed => self.return_malformed(operation, RecoveryInputKind::MalformedGetBlock).await,
                GetBlockCallError::Opaque(error) => Err(error),
            },
            Err(RawCallError::Node(error)) => Err(error),
        }
    }

    async fn begin_operation(&self) -> Result<OperationGuard<'_>, NodeError> {
        let mut admission = self.admission_tx.subscribe();
        self.require_active()?;
        let waiting = CounterGuard::new(&self.waiting);
        let permit = tokio::select! {
            biased;
            changed = wait_until_inactive(&mut admission) => {
                changed?;
                return Err(self.inactive_error());
            }
            permit = self.permits.clone().acquire_owned() => {
                permit.map_err(|_| self.inactive_error())?
            }
        };
        drop(waiting);
        self.require_active()?;
        Ok(OperationGuard { _permit: permit, _active: CounterGuard::new(&self.active), admission })
    }

    async fn call<T>(
        &self,
        admission: &mut watch::Receiver<u8>,
        future: impl Future<Output = RpcResult<T>>,
    ) -> Result<T, RawCallError> {
        tokio::pin!(future);
        let response = tokio::select! {
            biased;
            changed = wait_until_inactive(admission) => {
                changed.map_err(RawCallError::Node)?;
                return Err(RawCallError::Node(self.inactive_error()));
            }
            response = &mut future => response,
        };
        self.require_active().map_err(RawCallError::Node)?;
        response.map_err(RawCallError::Rpc)
    }

    async fn subscription_call(&self, future: impl Future<Output = RpcResult<()>>) -> Result<(), NodeError> {
        let mut admission = self.admission_tx.subscribe();
        self.require_active()?;
        tokio::pin!(future);
        let response = tokio::select! {
            biased;
            changed = wait_until_inactive(&mut admission) => {
                changed?;
                return Err(self.inactive_error());
            }
            response = &mut future => response,
        };
        self.require_active()?;
        response.map_err(|error| NodeError::SubscriptionControlFailed { diagnostic: Arc::from(error.to_string()) })
    }

    async fn finish_normalized<T>(
        &self,
        operation: &OperationGuard<'_>,
        result: Result<T, ResponseNormalizationError>,
    ) -> Result<T, NodeError> {
        match result {
            Err(ResponseNormalizationError::Malformed(kind)) => self.return_malformed(operation, kind).await,
            Ok(value) => {
                self.require_active()?;
                #[cfg(test)]
                self.wait_for_operation_completion().await;
                Ok(value)
            }
            Err(ResponseNormalizationError::ScoreOutOfRange(fault)) => {
                self.require_active()?;
                Err(NodeError::ScoreOutOfRange(fault))
            }
        }
    }

    async fn return_malformed<T>(&self, _operation: &OperationGuard<'_>, kind: RecoveryInputKind) -> Result<T, NodeError> {
        self.request_retirement(RetirementReason::MalformedRecoveryInput(kind)).await?;
        Err(NodeError::RecoveryInputInvalid(kind))
    }

    async fn request_retirement(&self, reason: RetirementReason) -> Result<(), NodeError> {
        self.require_active()?;
        let (completion_tx, completion_rx) = oneshot::channel();
        self.retirement_tx
            .send(RetirementRequest::with_reason(self.self_weak.clone(), reason, completion_tx))
            .map_err(|_| NodeError::RetirementControlUnavailable)?;
        completion_rx.await.map_err(|_| NodeError::RetirementControlUnavailable)?.map_err(|_| NodeError::RetirementControlUnavailable)
    }

    fn finish_failed_activation(&self) {
        self.notification_router.disable();
        self.notification_router.clear();
        let mut state = self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if *state == SubscriptionState::Activating {
            *state = SubscriptionState::Disabled;
        }
    }

    fn set_subscription_state_if_not_retired(&self, next: SubscriptionState) {
        let mut state = self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if *state != SubscriptionState::Retired {
            *state = next;
        }
    }

    fn require_active(&self) -> Result<(), NodeError> {
        match self.admission.load(Ordering::SeqCst) {
            GENERATION_ACTIVE => Ok(()),
            GENERATION_LOST => Err(NodeError::GenerationLost),
            GENERATION_CANCELLED => Err(NodeError::Cancelled),
            _ => unreachable!("unknown generation admission state"),
        }
    }

    fn inactive_error(&self) -> NodeError {
        self.require_active().expect_err("generation is inactive")
    }

    pub(crate) async fn retire(&self) -> bool {
        self.close(GENERATION_LOST).await
    }

    pub(crate) async fn cancel(&self) -> bool {
        self.close(GENERATION_CANCELLED).await
    }

    async fn close(&self, state: u8) -> bool {
        if self.admission.compare_exchange(GENERATION_ACTIVE, state, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return false;
        }
        self.admission_tx.send_replace(state);
        {
            let mut subscription_state = self.subscription_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            self.notification_router.retire();
            *subscription_state = SubscriptionState::Retired;
        }
        if let Err(error) = self.connection.disconnect().await {
            kaspa_core::warn!("failed to disconnect retired RPC generation: {error}");
        }
        true
    }
}

struct ActivationCancellation {
    abandoned: Mutex<bool>,
}

impl ActivationCancellation {
    const fn new() -> Self {
        Self { abandoned: Mutex::new(false) }
    }

    fn is_abandoned(&self) -> bool {
        *self.lock()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, bool> {
        self.abandoned.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct ActivationCancellationGuard {
    cancellation: Arc<ActivationCancellation>,
    armed: bool,
}

impl ActivationCancellationGuard {
    const fn new(cancellation: Arc<ActivationCancellation>) -> Self {
        Self { cancellation, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ActivationCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            *self.cancellation.lock() = true;
        }
    }
}

struct OperationGuard<'a> {
    _permit: OwnedSemaphorePermit,
    _active: CounterGuard<'a>,
    admission: watch::Receiver<u8>,
}

struct CounterGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> CounterGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for CounterGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
enum RawCallError {
    Node(NodeError),
    Rpc(RpcError),
}

#[derive(Debug, Eq, PartialEq)]
enum GetBlockCallError {
    NotFound,
    Malformed,
    Opaque(NodeError),
}

async fn wait_until_inactive(admission: &mut watch::Receiver<u8>) -> Result<(), NodeError> {
    loop {
        if *admission.borrow_and_update() != GENERATION_ACTIVE {
            return Ok(());
        }
        admission.changed().await.map_err(|_| NodeError::RetirementControlUnavailable)?;
    }
}

fn classify_get_block_error(hash: BlockHash, error: RpcError) -> GetBlockCallError {
    if matches!(error, RpcError::MissingRpcFieldError(_, _)) {
        return GetBlockCallError::Malformed;
    }
    let diagnostic: Arc<str> = Arc::from(error.to_string());
    if let RpcError::General(message) = error {
        let header_not_found = ConsensusError::HeaderNotFound(hash).to_string();
        let block_not_found = ConsensusError::BlockNotFound(hash).to_string();
        if message == header_not_found || message == block_not_found {
            return GetBlockCallError::NotFound;
        }
    }
    GetBlockCallError::Opaque(NodeError::RpcRequestFailed { diagnostic })
}

fn map_opaque_rpc_error(error: RawCallError) -> NodeError {
    match error {
        RawCallError::Node(error) => error,
        RawCallError::Rpc(error) => NodeError::RpcRequestFailed { diagnostic: Arc::from(error.to_string()) },
    }
}

fn is_inactive_error(error: &NodeError) -> bool {
    matches!(error, NodeError::GenerationLost | NodeError::Cancelled)
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
        blockhash::ORIGIN,
        errors::consensus::ConsensusError,
        network::{NetworkId, NetworkType},
    };
    use kaspa_rpc_core::{
        GetBlockDagInfoRequest, GetBlockDagInfoResponse, GetBlockRequest, GetBlockResponse, GetBlocksRequest, GetBlocksResponse,
        GetSinkRequest, GetSinkResponse, GetVirtualChainFromBlockV2Request, GetVirtualChainFromBlockV2Response, RpcBlock,
        RpcBlockVerboseData, RpcError, RpcHeader, RpcResult,
        api::ops::{RPC_API_REVISION, RPC_API_VERSION},
    };
    use kgi_model::{
        block::{BlockHash, MAX_BLUE_SCORE, MAX_DAA_SCORE},
        lifecycle::{RecoveryInputKind, ScoreRangeFault},
    };
    use tokio::{
        sync::{Semaphore, mpsc},
        time::timeout,
    };

    use super::{
        GetBlockCallError, NotificationChannels, SubscriptionState, ValidatedNodeInfo, ValidatedRpcClient, classify_get_block_error,
    };
    use crate::{
        client::RpcConnection,
        consensus::KgiConsensusParams,
        error::NodeError,
        notification::NotificationRouterState,
        runtime::{RetirementReason, RetirementReceiver, retirement_channel},
    };

    enum Script {
        Block(Box<RpcResult<GetBlockResponse>>, Option<Arc<Semaphore>>),
        Blocks(Box<RpcResult<GetBlocksResponse>>),
        Dag(Box<RpcResult<GetBlockDagInfoResponse>>),
        Sink(Box<RpcResult<GetSinkResponse>>),
        Vspc(Box<RpcResult<GetVirtualChainFromBlockV2Response>>),
    }

    impl Script {
        fn block(result: RpcResult<GetBlockResponse>, gate: Option<Arc<Semaphore>>) -> Self {
            Self::Block(Box::new(result), gate)
        }

        fn blocks(result: RpcResult<GetBlocksResponse>) -> Self {
            Self::Blocks(Box::new(result))
        }

        fn dag(result: RpcResult<GetBlockDagInfoResponse>) -> Self {
            Self::Dag(Box::new(result))
        }

        fn sink(result: RpcResult<GetSinkResponse>) -> Self {
            Self::Sink(Box::new(result))
        }

        fn vspc(result: RpcResult<GetVirtualChainFromBlockV2Response>) -> Self {
            Self::Vspc(Box::new(result))
        }
    }

    #[derive(Clone, Copy)]
    enum CompositeOperation {
        PruningPoint,
        CatchupSink,
    }

    impl CompositeOperation {
        async fn execute(self, client: &ValidatedRpcClient) -> Result<(), NodeError> {
            match self {
                Self::PruningPoint => client.current_pruning_point_block().await.map(|_| ()),
                Self::CatchupSink => client.catchup_sink_sample().await.map(|_| ()),
            }
        }

        const fn malformed_kind(self) -> RecoveryInputKind {
            match self {
                Self::PruningPoint => RecoveryInputKind::MalformedPruningPointResponse,
                Self::CatchupSink => RecoveryInputKind::MalformedCatchupSinkResponse,
            }
        }

        fn initial_script(self, advertised_hash: BlockHash) -> Script {
            match self {
                Self::PruningPoint => Script::dag(Ok(dag_info(advertised_hash))),
                Self::CatchupSink => Script::sink(Ok(GetSinkResponse::new(advertised_hash))),
            }
        }
    }

    fn composite_scripts(
        operation: CompositeOperation,
        advertised_hash: BlockHash,
        block: RpcResult<GetBlockResponse>,
        gate: Option<Arc<Semaphore>>,
    ) -> Vec<Script> {
        vec![operation.initial_script(advertised_hash), Script::block(block, gate)]
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum RecordedRequest {
        Block { hash: BlockHash, include_transactions: bool },
        Blocks { low_hash: Option<BlockHash>, include_blocks: bool, include_transactions: bool },
        Dag,
        Sink,
        Vspc { start_hash: BlockHash, verbosity: Option<i32>, min_confirmation_count: Option<u64> },
    }

    struct ScriptedConnection {
        scripts: tokio::sync::Mutex<VecDeque<Script>>,
        requests: Mutex<Vec<RecordedRequest>>,
        subscription_results: Mutex<VecDeque<RpcResult<()>>>,
        subscription_calls: Mutex<Vec<&'static str>>,
        block_start_gate: Option<Arc<Semaphore>>,
        virtual_start_gate: Option<Arc<Semaphore>>,
        block_stop_gate: Option<Arc<Semaphore>>,
        virtual_stop_gate: Option<Arc<Semaphore>>,
        disconnects: AtomicUsize,
    }

    impl ScriptedConnection {
        fn new(scripts: impl IntoIterator<Item = Script>) -> Self {
            Self {
                scripts: tokio::sync::Mutex::new(scripts.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
                subscription_results: Mutex::new(VecDeque::new()),
                subscription_calls: Mutex::new(Vec::new()),
                block_start_gate: None,
                virtual_start_gate: None,
                block_stop_gate: None,
                virtual_stop_gate: None,
                disconnects: AtomicUsize::new(0),
            }
        }

        fn with_subscription_results(self, results: impl IntoIterator<Item = RpcResult<()>>) -> Self {
            *self.subscription_results.lock().expect("subscription results") = results.into_iter().collect();
            self
        }

        fn with_virtual_start_gate(mut self, gate: Arc<Semaphore>) -> Self {
            self.virtual_start_gate = Some(gate);
            self
        }

        fn with_block_start_gate(mut self, gate: Arc<Semaphore>) -> Self {
            self.block_start_gate = Some(gate);
            self
        }

        fn with_block_stop_gate(mut self, gate: Arc<Semaphore>) -> Self {
            self.block_stop_gate = Some(gate);
            self
        }

        fn with_virtual_stop_gate(mut self, gate: Arc<Semaphore>) -> Self {
            self.virtual_stop_gate = Some(gate);
            self
        }

        async fn next(&self) -> Script {
            self.scripts.lock().await.pop_front().expect("scripted RPC response")
        }

        fn record(&self, request: RecordedRequest) {
            self.requests.lock().expect("request log").push(request);
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().expect("request log").clone()
        }

        fn subscription_result(&self) -> RpcResult<()> {
            self.subscription_results.lock().expect("subscription results").pop_front().unwrap_or(Ok(()))
        }
    }

    #[async_trait]
    impl RpcConnection for ScriptedConnection {
        async fn get_block(&self, request: GetBlockRequest) -> RpcResult<GetBlockResponse> {
            self.record(RecordedRequest::Block { hash: request.hash, include_transactions: request.include_transactions });
            match self.next().await {
                Script::Block(result, gate) => {
                    if let Some(gate) = gate {
                        gate.acquire().await.expect("open gate").forget();
                    }
                    *result
                }
                _ => panic!("expected GetBlock script"),
            }
        }

        async fn get_blocks(&self, request: GetBlocksRequest) -> RpcResult<GetBlocksResponse> {
            self.record(RecordedRequest::Blocks {
                low_hash: request.low_hash,
                include_blocks: request.include_blocks,
                include_transactions: request.include_transactions,
            });
            match self.next().await {
                Script::Blocks(result) => *result,
                _ => panic!("expected GetBlocks script"),
            }
        }

        async fn get_block_dag_info(&self, _request: GetBlockDagInfoRequest) -> RpcResult<GetBlockDagInfoResponse> {
            self.record(RecordedRequest::Dag);
            match self.next().await {
                Script::Dag(result) => *result,
                _ => panic!("expected GetBlockDagInfo script"),
            }
        }

        async fn get_sink(&self, _request: GetSinkRequest) -> RpcResult<GetSinkResponse> {
            self.record(RecordedRequest::Sink);
            match self.next().await {
                Script::Sink(result) => *result,
                _ => panic!("expected GetSink script"),
            }
        }

        async fn get_virtual_chain_from_block_v2(
            &self,
            request: GetVirtualChainFromBlockV2Request,
        ) -> RpcResult<GetVirtualChainFromBlockV2Response> {
            self.record(RecordedRequest::Vspc {
                start_hash: request.start_hash,
                verbosity: request.data_verbosity_level.map(|value| value as i32),
                min_confirmation_count: request.min_confirmation_count,
            });
            match self.next().await {
                Script::Vspc(result) => *result,
                _ => panic!("expected VSPC V2 script"),
            }
        }

        async fn start_block_added(&self) -> RpcResult<()> {
            self.subscription_calls.lock().expect("subscription log").push("start BlockAdded");
            if let Some(gate) = &self.block_start_gate {
                gate.acquire().await.expect("block-start gate").forget();
            }
            self.subscription_result()
        }

        async fn start_virtual_chain_changed(&self) -> RpcResult<()> {
            self.subscription_calls.lock().expect("subscription log").push("start VirtualChainChanged");
            if let Some(gate) = &self.virtual_start_gate {
                gate.acquire().await.expect("virtual-start gate").forget();
            }
            self.subscription_result()
        }

        async fn stop_block_added(&self) -> RpcResult<()> {
            self.subscription_calls.lock().expect("subscription log").push("stop BlockAdded");
            if let Some(gate) = &self.block_stop_gate {
                gate.acquire().await.expect("block-stop gate").forget();
            }
            self.subscription_result()
        }

        async fn stop_virtual_chain_changed(&self) -> RpcResult<()> {
            self.subscription_calls.lock().expect("subscription log").push("stop VirtualChainChanged");
            if let Some(gate) = &self.virtual_stop_gate {
                gate.acquire().await.expect("virtual-stop gate").forget();
            }
            self.subscription_result()
        }

        async fn disconnect(&self) -> Result<(), Arc<str>> {
            self.disconnects.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn constructs_runtime_requests_and_normalizes_results() {
        let low = hash(1);
        let block_hash = hash(2);
        let destination = hash(3);
        let scripts = [
            Script::block(Ok(GetBlockResponse { block: rpc_block(block_hash, vec![hash(8)]) }), None),
            Script::blocks(Ok(GetBlocksResponse::new(
                vec![hash(90)],
                vec![rpc_block(low, vec![]), rpc_block(block_hash, vec![hash(8)])],
            ))),
            Script::vspc(Ok(vspc(vec![low], vec![destination]))),
        ];
        let (client, connection, _retirements) = client(scripts);

        assert_eq!(client.full_block(block_hash).await.expect("full block").hash, block_hash);
        assert_eq!(client.get_blocks(low).await.expect("GetBlocks")[0].hash, block_hash);
        assert_eq!(client.virtual_chain_from(low).await.expect("VSPC").added.as_ref(), &[destination]);

        assert_eq!(
            connection.requests(),
            vec![
                RecordedRequest::Block { hash: block_hash, include_transactions: false },
                RecordedRequest::Blocks { low_hash: Some(low), include_blocks: true, include_transactions: false },
                RecordedRequest::Vspc { start_hash: low, verbosity: Some(0), min_confirmation_count: None },
            ]
        );
    }

    #[tokio::test]
    async fn composite_operations_remain_on_one_generation() {
        let pruning_point = hash(4);
        let sink = hash(5);
        let scripts = [
            Script::dag(Ok(dag_info(pruning_point))),
            Script::block(Ok(GetBlockResponse { block: rpc_block(pruning_point, vec![hash(8)]) }), None),
            Script::sink(Ok(GetSinkResponse::new(sink))),
            Script::block(Ok(GetBlockResponse { block: rpc_block(sink, vec![hash(8)]) }), None),
        ];
        let (client, connection, _retirements) = client(scripts);

        assert_eq!(client.current_pruning_point_block().await.expect("pruning point").hash, pruning_point);
        assert_eq!(client.catchup_sink_sample().await.expect("sink").hash, sink);
        assert_eq!(
            connection.requests(),
            vec![
                RecordedRequest::Dag,
                RecordedRequest::Block { hash: pruning_point, include_transactions: false },
                RecordedRequest::Sink,
                RecordedRequest::Block { hash: sink, include_transactions: false },
            ]
        );
    }

    #[tokio::test]
    async fn composite_operations_canonicalize_exact_genesis() {
        let genesis = hash(0);
        let mut genesis_block = rpc_block(genesis, vec![hash(8)]);
        genesis_block.header.daa_score = 17;
        genesis_block.header.blue_score = u64::MAX;
        genesis_block.verbose_data = None;
        let mut dag = dag_info(genesis);
        dag.network = NetworkId::with_suffix(NetworkType::Testnet, 10);
        let scripts = [
            Script::dag(Ok(dag)),
            Script::block(Ok(GetBlockResponse { block: genesis_block.clone() }), None),
            Script::sink(Ok(GetSinkResponse::new(genesis))),
            Script::block(Ok(GetBlockResponse { block: genesis_block }), None),
        ];
        let (client, connection, mut retirements) = client(scripts);

        let pruning_point = client.current_pruning_point_block().await.expect("canonical Genesis pruning point");
        assert_eq!(pruning_point.hash, genesis);
        assert_eq!(pruning_point.selected_parent, ORIGIN);
        assert!(pruning_point.direct_parents.is_empty());
        assert!(pruning_point.blue_merge_set.is_empty());
        assert!(pruning_point.red_merge_set.is_empty());
        assert_eq!(pruning_point.blue_score, 0);
        assert_eq!(pruning_point.daa_score, 17);

        let sink = client.catchup_sink_sample().await.expect("canonical Genesis sink");
        assert_eq!(sink.hash, genesis);
        assert_eq!(sink.daa_score, 17);
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn get_block_not_found_matches_are_exact_and_hash_specific() {
        let requested = hash(1);
        let header_not_found = ConsensusError::HeaderNotFound(requested).to_string();
        let block_not_found = ConsensusError::BlockNotFound(requested).to_string();
        for message in [header_not_found.clone(), block_not_found.clone()] {
            assert_eq!(classify_get_block_error(requested, RpcError::General(message)), GetBlockCallError::NotFound);
        }

        for message in [
            ConsensusError::HeaderNotFound(hash(2)).to_string(),
            ConsensusError::BlockNotFound(hash(2)).to_string(),
            header_not_found.to_uppercase(),
            format!(" {header_not_found}"),
            format!("{block_not_found} "),
            "unrelated RPC failure".to_string(),
        ] {
            assert!(matches!(
                classify_get_block_error(requested, RpcError::General(message)),
                GetBlockCallError::Opaque(NodeError::RpcRequestFailed { .. })
            ));
        }
        assert_eq!(
            classify_get_block_error(requested, RpcError::MissingRpcFieldError("RpcBlock".to_string(), "header".to_string())),
            GetBlockCallError::Malformed
        );
    }

    #[tokio::test]
    async fn opaque_rpc_failure_does_not_retire_generation() {
        let requested = hash(1);
        let scripts = [
            Script::block(Err(RpcError::General("opaque".to_string())), None),
            Script::block(Ok(GetBlockResponse { block: rpc_block(requested, vec![hash(8)]) }), None),
        ];
        let (client, connection, mut retirements) = client(scripts);

        assert_eq!(client.full_block(requested).await, Err(NodeError::RpcRequestFailed { diagnostic: Arc::from("opaque") }));
        assert_eq!(client.full_block(requested).await.expect("generation remains valid").hash, requested);
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn exact_get_block_not_found_forms_remain_distinct_without_retirement() {
        let requested = hash(1);
        let scripts = [
            Script::block(Err(RpcError::General(ConsensusError::HeaderNotFound(requested).to_string())), None),
            Script::block(Err(RpcError::General(ConsensusError::BlockNotFound(requested).to_string())), None),
        ];
        let (client, connection, mut retirements) = client(scripts);

        assert_eq!(client.full_block(requested).await, Err(NodeError::BlockNotFound { hash: requested }));
        assert_eq!(client.full_block(requested).await, Err(NodeError::BlockNotFound { hash: requested }));
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn missing_get_block_field_is_malformed_and_retires() {
        let requested = hash(1);
        let scripts = [Script::block(Err(RpcError::MissingRpcFieldError("RpcBlock".to_string(), "header".to_string())), None)];
        let (client, connection, mut retirements) = client(scripts);
        let operation = tokio::spawn({
            let client = client.clone();
            async move { client.full_block(requested).await }
        });

        let retirement = retirements.recv().await.expect("retirement request");
        assert_eq!(retirement.reason(), &RetirementReason::MalformedRecoveryInput(RecoveryInputKind::MalformedGetBlock));
        assert!(client.retire().await);
        retirement.complete(Ok(()));
        assert_eq!(
            operation.await.expect("operation task"),
            Err(NodeError::RecoveryInputInvalid(RecoveryInputKind::MalformedGetBlock))
        );
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn malformed_composite_responses_retire_the_exact_generation() {
        let pruning_point = hash(4);
        let sink = hash(5);
        let mut missing_verbose = rpc_block(pruning_point, vec![hash(8)]);
        missing_verbose.verbose_data = None;
        let cases = [
            ("ORIGIN pruning point", CompositeOperation::PruningPoint, vec![Script::dag(Ok(dag_info(ORIGIN)))]),
            (
                "wrong pruning-point hash",
                CompositeOperation::PruningPoint,
                composite_scripts(
                    CompositeOperation::PruningPoint,
                    pruning_point,
                    Ok(GetBlockResponse { block: rpc_block(hash(6), vec![hash(8)]) }),
                    None,
                ),
            ),
            (
                "missing pruning-point block",
                CompositeOperation::PruningPoint,
                composite_scripts(
                    CompositeOperation::PruningPoint,
                    pruning_point,
                    Err(RpcError::General(ConsensusError::HeaderNotFound(pruning_point).to_string())),
                    None,
                ),
            ),
            (
                "malformed pruning-point block",
                CompositeOperation::PruningPoint,
                composite_scripts(
                    CompositeOperation::PruningPoint,
                    pruning_point,
                    Ok(GetBlockResponse { block: missing_verbose }),
                    None,
                ),
            ),
            ("ORIGIN sink", CompositeOperation::CatchupSink, vec![Script::sink(Ok(GetSinkResponse::new(ORIGIN)))]),
            (
                "missing sink block",
                CompositeOperation::CatchupSink,
                composite_scripts(
                    CompositeOperation::CatchupSink,
                    sink,
                    Err(RpcError::General(ConsensusError::HeaderNotFound(sink).to_string())),
                    None,
                ),
            ),
            (
                "missing sink header",
                CompositeOperation::CatchupSink,
                composite_scripts(
                    CompositeOperation::CatchupSink,
                    sink,
                    Err(RpcError::MissingRpcFieldError("RpcBlock".to_string(), "header".to_string())),
                    None,
                ),
            ),
            (
                "wrong sink hash",
                CompositeOperation::CatchupSink,
                composite_scripts(
                    CompositeOperation::CatchupSink,
                    sink,
                    Ok(GetBlockResponse { block: rpc_block(hash(7), vec![hash(8)]) }),
                    None,
                ),
            ),
        ];

        for (name, operation, scripts) in cases {
            assert_malformed_composite(name, operation, scripts).await;
        }
    }

    #[tokio::test]
    async fn composite_score_and_opaque_failures_do_not_retire_the_generation() {
        let pruning_point = hash(4);
        let sink = hash(5);
        let mut excessive_pruning_daa = rpc_block(pruning_point, vec![hash(8)]);
        excessive_pruning_daa.header.daa_score = MAX_DAA_SCORE + 1;
        let mut excessive_pruning_blue = rpc_block(pruning_point, vec![hash(8)]);
        excessive_pruning_blue.header.blue_score = MAX_BLUE_SCORE + 1;
        let mut excessive_sink_daa = rpc_block(sink, vec![hash(8)]);
        excessive_sink_daa.header.daa_score = MAX_DAA_SCORE + 1;
        let cases = [
            (
                CompositeOperation::PruningPoint,
                composite_scripts(
                    CompositeOperation::PruningPoint,
                    pruning_point,
                    Ok(GetBlockResponse { block: excessive_pruning_daa }),
                    None,
                ),
                NodeError::ScoreOutOfRange(ScoreRangeFault::DaaScore),
            ),
            (
                CompositeOperation::PruningPoint,
                composite_scripts(
                    CompositeOperation::PruningPoint,
                    pruning_point,
                    Ok(GetBlockResponse { block: excessive_pruning_blue }),
                    None,
                ),
                NodeError::ScoreOutOfRange(ScoreRangeFault::BlueScore),
            ),
            (
                CompositeOperation::CatchupSink,
                composite_scripts(CompositeOperation::CatchupSink, sink, Ok(GetBlockResponse { block: excessive_sink_daa }), None),
                NodeError::ScoreOutOfRange(ScoreRangeFault::DaaScore),
            ),
            (
                CompositeOperation::PruningPoint,
                composite_scripts(
                    CompositeOperation::PruningPoint,
                    pruning_point,
                    Err(RpcError::General("opaque pruning-point failure".to_string())),
                    None,
                ),
                NodeError::RpcRequestFailed { diagnostic: Arc::from("opaque pruning-point failure") },
            ),
            (
                CompositeOperation::CatchupSink,
                composite_scripts(
                    CompositeOperation::CatchupSink,
                    sink,
                    Err(RpcError::General("opaque sink failure".to_string())),
                    None,
                ),
                NodeError::RpcRequestFailed { diagnostic: Arc::from("opaque sink failure") },
            ),
        ];

        for (operation, scripts, expected) in cases {
            let (client, connection, mut retirements) = client(scripts);
            assert_eq!(operation.execute(&client).await, Err(expected));
            assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
            assert_eq!(connection.disconnects.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn composite_cancellation_and_generation_loss_are_not_remapped() {
        for operation in [CompositeOperation::PruningPoint, CompositeOperation::CatchupSink] {
            for cancelled in [true, false] {
                let advertised_hash = hash(4);
                let gate = Arc::new(Semaphore::new(0));
                let scripts = composite_scripts(
                    operation,
                    advertised_hash,
                    Ok(GetBlockResponse { block: rpc_block(advertised_hash, vec![hash(8)]) }),
                    Some(gate),
                );
                let (client, connection, mut retirements) = client(scripts);
                let task = tokio::spawn({
                    let client = client.clone();
                    async move { operation.execute(&client).await }
                });
                wait_for_requests(&connection, 2).await;

                let changed = if cancelled { client.cancel().await } else { client.retire().await };
                assert!(changed);
                let expected = if cancelled { NodeError::Cancelled } else { NodeError::GenerationLost };
                assert_eq!(task.await.expect("composite operation task"), Err(expected));
                assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
                assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
            }
        }
    }

    #[tokio::test]
    async fn missing_get_blocks_field_retires_the_whole_page() {
        let low = hash(1);
        let scripts = [Script::blocks(Err(RpcError::MissingRpcFieldError("RpcBlock".to_string(), "header".to_string())))];
        let (client, _connection, mut retirements) = client(scripts);
        let operation = tokio::spawn({
            let client = client.clone();
            async move { client.get_blocks(low).await }
        });

        let retirement = retirements.recv().await.expect("retirement request");
        assert_eq!(retirement.reason(), &RetirementReason::MalformedRecoveryInput(RecoveryInputKind::MalformedGetBlocks));
        assert!(client.retire().await);
        retirement.complete(Ok(()));
        assert_eq!(
            operation.await.expect("operation task"),
            Err(NodeError::RecoveryInputInvalid(RecoveryInputKind::MalformedGetBlocks))
        );
    }

    #[tokio::test]
    async fn malformed_response_waits_for_exact_generation_retirement() {
        let requested = hash(1);
        let scripts = [Script::block(Ok(GetBlockResponse { block: rpc_block(hash(2), vec![hash(8)]) }), None)];
        let (client, connection, mut retirements) = client(scripts);
        let operation = tokio::spawn({
            let client = client.clone();
            async move { client.full_block(requested).await }
        });

        let retirement = retirements.recv().await.expect("retirement request");
        let generation = retirement.target().upgrade().expect("exact generation");
        assert!(Arc::ptr_eq(&generation, &client));
        assert_eq!(retirement.reason(), &RetirementReason::MalformedRecoveryInput(RecoveryInputKind::MalformedGetBlock));
        assert!(client.retire().await);
        tokio::task::yield_now().await;
        assert!(!operation.is_finished());
        retirement.complete(Ok(()));

        assert_eq!(
            operation.await.expect("operation task"),
            Err(NodeError::RecoveryInputInvalid(RecoveryInputKind::MalformedGetBlock))
        );
        assert_eq!(client.full_block(requested).await, Err(NodeError::GenerationLost));
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn score_range_fault_does_not_retire_generation() {
        let requested = hash(1);
        let mut excessive = rpc_block(requested, vec![hash(8)]);
        excessive.header.daa_score = MAX_DAA_SCORE + 1;
        let scripts = [Script::block(Ok(GetBlockResponse { block: excessive }), None)];
        let (client, connection, mut retirements) = client(scripts);

        assert_eq!(client.full_block(requested).await, Err(NodeError::ScoreOutOfRange(ScoreRangeFault::DaaScore)));
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn rpc_success_linearized_before_retirement_is_preserved() {
        let requested = hash(1);
        let scripts = [Script::block(Ok(GetBlockResponse { block: rpc_block(requested, vec![hash(8)]) }), None)];
        let (client, connection, _retirements) = client(scripts);
        let (reached_tx, mut reached_rx) = mpsc::unbounded_channel();
        let completion_gate = Arc::new(Semaphore::new(0));
        client.set_operation_completion_gate(reached_tx, completion_gate.clone());
        let operation = tokio::spawn({
            let client = client.clone();
            async move { client.full_block(requested).await }
        });

        reached_rx.recv().await.expect("operation must linearize success");
        assert!(client.retire().await);
        completion_gate.add_permits(1);

        assert_eq!(operation.await.expect("operation task").expect("old-generation success").hash, requested);
        assert_eq!(client.full_block(requested).await, Err(NodeError::GenerationLost));
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn retirement_wins_blocked_operation_race() {
        let gate = Arc::new(Semaphore::new(0));
        let requested = hash(1);
        let scripts = [Script::block(Ok(GetBlockResponse { block: rpc_block(requested, vec![hash(8)]) }), Some(gate))];
        let (client, connection, _retirements) = client(scripts);
        let operation = tokio::spawn({
            let client = client.clone();
            async move { client.full_block(requested).await }
        });
        wait_for_counts(&client, 1, 0).await;

        assert!(client.retire().await);
        assert_eq!(operation.await.expect("operation task"), Err(NodeError::GenerationLost));
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cancellation_wins_blocked_operation_race() {
        let gate = Arc::new(Semaphore::new(0));
        let requested = hash(1);
        let scripts = [Script::block(Ok(GetBlockResponse { block: rpc_block(requested, vec![hash(8)]) }), Some(gate))];
        let (client, connection, _retirements) = client(scripts);
        let operation = tokio::spawn({
            let client = client.clone();
            async move { client.full_block(requested).await }
        });
        wait_for_counts(&client, 1, 0).await;

        assert!(client.cancel().await);
        assert_eq!(operation.await.expect("operation task"), Err(NodeError::Cancelled));
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn notification_subscription_transitions_preserve_remote_order() {
        let connection = Arc::new(ScriptedConnection::new([]));
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();

        client.activate_notifications(channels).await.expect("activate notifications");
        assert_eq!(client.notification_router.state(), NotificationRouterState::Enabled);
        client.disable_notifications().await.expect("disable notifications");
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        assert_eq!(
            connection.subscription_calls.lock().expect("subscription log").as_slice(),
            &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded", "stop VirtualChainChanged"]
        );
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn router_remains_disabled_until_both_remote_starts_complete() {
        let gate = Arc::new(Semaphore::new(0));
        let connection = Arc::new(ScriptedConnection::new([]).with_virtual_start_gate(gate.clone()));
        let (retirement_tx, _retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();
        let activation = tokio::spawn({
            let client = client.clone();
            async move { client.activate_notifications(channels).await }
        });

        while connection.subscription_calls.lock().expect("subscription log").len() < 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        gate.add_permits(1);
        activation.await.expect("activation task").expect("activation");
        assert_eq!(client.notification_router.state(), NotificationRouterState::Enabled);
    }

    #[tokio::test]
    async fn abandoned_activation_after_first_start_completes_remote_cleanup() {
        let gate = Arc::new(Semaphore::new(0));
        let connection = Arc::new(ScriptedConnection::new([]).with_virtual_start_gate(gate.clone()));
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();
        let activation = tokio::spawn({
            let client = client.clone();
            async move { client.activate_notifications(channels).await }
        });

        wait_for_subscription_calls(&connection, 2).await;
        activation.abort();
        assert!(activation.await.expect_err("activation caller must be aborted").is_cancelled());
        gate.add_permits(1);

        wait_for_subscription_state(&client, SubscriptionState::Disabled).await;
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        assert_eq!(
            connection.subscription_calls.lock().expect("subscription log").as_slice(),
            &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded", "stop VirtualChainChanged"]
        );
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn failed_cleanup_of_abandoned_activation_retires_exact_generation() {
        let gate = Arc::new(Semaphore::new(0));
        let connection = Arc::new(
            ScriptedConnection::new([])
                .with_subscription_results([Ok(()), Ok(()), Err(RpcError::General("BlockAdded cleanup failed".to_string())), Ok(())])
                .with_virtual_start_gate(gate.clone()),
        );
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();
        let activation = tokio::spawn({
            let client = client.clone();
            async move { client.activate_notifications(channels).await }
        });

        wait_for_subscription_calls(&connection, 2).await;
        activation.abort();
        assert!(activation.await.expect_err("activation caller must be aborted").is_cancelled());
        gate.add_permits(1);

        let retirement = timeout(Duration::from_secs(1), retirements.recv())
            .await
            .expect("failed cleanup must request retirement")
            .expect("retirement path");
        assert_eq!(retirement.reason(), &RetirementReason::SubscriptionControlFailure);
        assert!(retirement.target().upgrade().is_some_and(|generation| Arc::ptr_eq(&generation, &client)));
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        assert!(client.retire().await);
        retirement.complete(Ok(()));
        wait_for_subscription_state(&client, SubscriptionState::Retired).await;
        assert_eq!(
            connection.subscription_calls.lock().expect("subscription log").as_slice(),
            &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded", "stop VirtualChainChanged"]
        );
    }

    #[tokio::test]
    async fn abandoned_activation_during_rollback_finishes_disabled() {
        let gate = Arc::new(Semaphore::new(0));
        let connection = Arc::new(
            ScriptedConnection::new([])
                .with_subscription_results([Ok(()), Err(RpcError::General("VSPC start failed".to_string())), Ok(())])
                .with_block_stop_gate(gate.clone()),
        );
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();
        let activation = tokio::spawn({
            let client = client.clone();
            async move { client.activate_notifications(channels).await }
        });

        wait_for_subscription_calls(&connection, 3).await;
        activation.abort();
        assert!(activation.await.expect_err("activation caller must be aborted").is_cancelled());
        gate.add_permits(1);

        wait_for_subscription_state(&client, SubscriptionState::Disabled).await;
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        assert_eq!(
            connection.subscription_calls.lock().expect("subscription log").as_slice(),
            &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded"]
        );
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn abandoned_deactivation_between_remote_stops_completes_cleanup() {
        let gate = Arc::new(Semaphore::new(0));
        let connection = Arc::new(ScriptedConnection::new([]).with_virtual_stop_gate(gate.clone()));
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();
        client.activate_notifications(channels).await.expect("activation");
        let deactivation = tokio::spawn({
            let client = client.clone();
            async move { client.disable_notifications().await }
        });

        wait_for_subscription_calls(&connection, 4).await;
        deactivation.abort();
        assert!(deactivation.await.expect_err("deactivation caller must be aborted").is_cancelled());
        gate.add_permits(1);

        wait_for_subscription_state(&client, SubscriptionState::Disabled).await;
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        assert_eq!(
            connection.subscription_calls.lock().expect("subscription log").as_slice(),
            &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded", "stop VirtualChainChanged"]
        );
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn deactivation_during_block_start_cancels_activation_after_cleanup() {
        exercise_activation_deactivation_overlap(ActivationOverlap::BlockStart).await;
    }

    #[tokio::test]
    async fn deactivation_during_vspc_start_cancels_activation_after_cleanup() {
        exercise_activation_deactivation_overlap(ActivationOverlap::VspcStart).await;
    }

    #[tokio::test]
    async fn deactivation_during_activation_rollback_cancels_after_cleanup() {
        exercise_activation_deactivation_overlap(ActivationOverlap::Rollback).await;
    }

    #[tokio::test]
    async fn deactivation_before_enabled_publication_cancels_activation_after_cleanup() {
        exercise_activation_deactivation_overlap(ActivationOverlap::Publication).await;
    }

    #[derive(Clone, Copy)]
    enum ActivationOverlap {
        BlockStart,
        VspcStart,
        Rollback,
        Publication,
    }

    async fn exercise_activation_deactivation_overlap(overlap: ActivationOverlap) {
        let gate = Arc::new(Semaphore::new(0));
        let connection = Arc::new(match overlap {
            ActivationOverlap::BlockStart => ScriptedConnection::new([]).with_block_start_gate(gate.clone()),
            ActivationOverlap::VspcStart => ScriptedConnection::new([]).with_virtual_start_gate(gate.clone()),
            ActivationOverlap::Rollback => ScriptedConnection::new([])
                .with_subscription_results([Ok(()), Err(RpcError::General("VSPC start failed".to_string())), Ok(())])
                .with_block_stop_gate(gate.clone()),
            ActivationOverlap::Publication => ScriptedConnection::new([]),
        });
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let mut publication_reached = None;
        if matches!(overlap, ActivationOverlap::Publication) {
            let (reached_tx, reached_rx) = mpsc::unbounded_channel();
            client.set_activation_publication_gate(reached_tx, gate.clone());
            publication_reached = Some(reached_rx);
        }
        let (channels, _receivers) = notification_channels();
        let activation = tokio::spawn({
            let client = client.clone();
            async move { client.activate_notifications(channels).await }
        });

        match overlap {
            ActivationOverlap::BlockStart => wait_for_subscription_calls(&connection, 1).await,
            ActivationOverlap::VspcStart => wait_for_subscription_calls(&connection, 2).await,
            ActivationOverlap::Rollback => wait_for_subscription_calls(&connection, 3).await,
            ActivationOverlap::Publication => {
                timeout(Duration::from_secs(1), publication_reached.as_mut().expect("publication observer").recv())
                    .await
                    .expect("activation must reach publication boundary")
                    .expect("publication observer path");
            }
        }

        let deactivation = tokio::spawn({
            let client = client.clone();
            async move { client.disable_notifications().await }
        });
        wait_for_subscription_state(&client, SubscriptionState::Disabling).await;
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        gate.add_permits(1);

        assert_eq!(activation.await.expect("activation task"), Err(NodeError::Cancelled));
        assert_eq!(deactivation.await.expect("deactivation task"), Ok(()));
        assert_eq!(client.subscription_state(), SubscriptionState::Disabled);
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        let expected_calls: &[&str] = match overlap {
            ActivationOverlap::BlockStart => &["start BlockAdded", "stop BlockAdded", "stop VirtualChainChanged"],
            ActivationOverlap::VspcStart | ActivationOverlap::Rollback | ActivationOverlap::Publication => {
                &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded", "stop VirtualChainChanged"]
            }
        };
        assert_eq!(connection.subscription_calls.lock().expect("subscription log").as_slice(), expected_calls);
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn failed_overlap_cleanup_retires_and_completes_both_operations() {
        let gate = Arc::new(Semaphore::new(0));
        let connection = Arc::new(
            ScriptedConnection::new([])
                .with_subscription_results([Ok(()), Ok(()), Err(RpcError::General("BlockAdded stop failed".to_string())), Ok(())])
                .with_virtual_start_gate(gate.clone()),
        );
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();
        let activation = tokio::spawn({
            let client = client.clone();
            async move { client.activate_notifications(channels).await }
        });
        wait_for_subscription_calls(&connection, 2).await;
        let deactivation = tokio::spawn({
            let client = client.clone();
            async move { client.disable_notifications().await }
        });
        wait_for_subscription_state(&client, SubscriptionState::Disabling).await;
        gate.add_permits(1);

        let retirement = timeout(Duration::from_secs(1), retirements.recv())
            .await
            .expect("failed overlap cleanup must request retirement")
            .expect("retirement path");
        assert_eq!(retirement.reason(), &RetirementReason::SubscriptionControlFailure);
        assert!(retirement.target().upgrade().is_some_and(|generation| Arc::ptr_eq(&generation, &client)));
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        assert_eq!(
            connection.subscription_calls.lock().expect("subscription log").as_slice(),
            &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded", "stop VirtualChainChanged"]
        );
        assert!(client.retire().await);
        retirement.complete(Ok(()));

        assert_eq!(activation.await.expect("activation task"), Err(NodeError::GenerationLost));
        assert_eq!(deactivation.await.expect("deactivation task"), Err(NodeError::GenerationLost));
        assert_eq!(client.subscription_state(), SubscriptionState::Retired);
        assert_eq!(client.notification_router.state(), NotificationRouterState::Retired);
    }

    #[tokio::test]
    async fn first_subscription_failure_preserves_the_generation_and_allows_a_fresh_activation() {
        let connection = Arc::new(ScriptedConnection::new([]).with_subscription_results([
            Err(RpcError::General("BlockAdded start failed".to_string())),
            Ok(()),
            Ok(()),
        ]));
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();

        assert!(matches!(client.activate_notifications(channels).await, Err(NodeError::SubscriptionControlFailed { .. })));
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        assert_eq!(connection.subscription_calls.lock().expect("subscription log").as_slice(), &["start BlockAdded"]);
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        let (channels, _receivers) = notification_channels();
        client.activate_notifications(channels).await.expect("fresh activation after failed first start");
        assert_eq!(client.notification_router.state(), NotificationRouterState::Enabled);
    }

    #[tokio::test]
    async fn second_subscription_failure_rolls_back_and_allows_a_fresh_activation() {
        let connection = Arc::new(ScriptedConnection::new([]).with_subscription_results([
            Ok(()),
            Err(RpcError::General("VSPC start failed".to_string())),
            Ok(()),
        ]));
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();

        assert!(matches!(client.activate_notifications(channels).await, Err(NodeError::SubscriptionControlFailed { .. })));
        assert_eq!(client.notification_router.state(), NotificationRouterState::Disabled);
        assert_eq!(
            connection.subscription_calls.lock().expect("subscription log").as_slice(),
            &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded"]
        );
        assert!(matches!(retirements.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        let (channels, _receivers) = notification_channels();
        client.activate_notifications(channels).await.expect("fresh activation after rollback");
        assert_eq!(client.notification_router.state(), NotificationRouterState::Enabled);
    }

    #[tokio::test]
    async fn failed_subscription_rollback_requests_exact_generation_retirement() {
        let connection = Arc::new(ScriptedConnection::new([]).with_subscription_results([
            Ok(()),
            Err(RpcError::General("VSPC start failed".to_string())),
            Err(RpcError::General("BlockAdded rollback failed".to_string())),
        ]));
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection, node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();
        let activation = tokio::spawn({
            let client = client.clone();
            async move { client.activate_notifications(channels).await }
        });

        let retirement = retirements.recv().await.expect("retirement request");
        assert_eq!(retirement.reason(), &RetirementReason::SubscriptionControlFailure);
        assert!(retirement.target().upgrade().is_some_and(|generation| Arc::ptr_eq(&generation, &client)));
        assert!(client.retire().await);
        retirement.complete(Ok(()));
        assert_eq!(activation.await.expect("activation task"), Err(NodeError::GenerationLost));
        assert_eq!(client.notification_router.state(), NotificationRouterState::Retired);
    }

    #[tokio::test]
    async fn partial_unsubscription_failure_retires_after_attempting_both_stops() {
        let connection = Arc::new(ScriptedConnection::new([]).with_subscription_results([
            Ok(()),
            Ok(()),
            Err(RpcError::General("BlockAdded stop failed".to_string())),
            Ok(()),
        ]));
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();
        client.activate_notifications(channels).await.expect("activation");
        let deactivation = tokio::spawn({
            let client = client.clone();
            async move { client.disable_notifications().await }
        });

        let retirement = retirements.recv().await.expect("retirement request");
        assert_eq!(retirement.reason(), &RetirementReason::SubscriptionControlFailure);
        assert!(client.retire().await);
        retirement.complete(Ok(()));
        assert_eq!(deactivation.await.expect("deactivation task"), Err(NodeError::GenerationLost));
        assert_eq!(client.notification_router.state(), NotificationRouterState::Retired);
        assert_eq!(
            connection.subscription_calls.lock().expect("subscription log").as_slice(),
            &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded", "stop VirtualChainChanged"]
        );
    }

    #[tokio::test]
    async fn second_unsubscription_failure_also_retires_after_attempting_both_stops() {
        let connection = Arc::new(ScriptedConnection::new([]).with_subscription_results([
            Ok(()),
            Ok(()),
            Ok(()),
            Err(RpcError::General("VirtualChainChanged stop failed".to_string())),
        ]));
        let (retirement_tx, mut retirements) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        let (channels, _receivers) = notification_channels();
        client.activate_notifications(channels).await.expect("activation");
        let deactivation = tokio::spawn({
            let client = client.clone();
            async move { client.disable_notifications().await }
        });

        let retirement = retirements.recv().await.expect("retirement request");
        assert_eq!(retirement.reason(), &RetirementReason::SubscriptionControlFailure);
        assert!(client.retire().await);
        retirement.complete(Ok(()));
        assert_eq!(deactivation.await.expect("deactivation task"), Err(NodeError::GenerationLost));
        assert_eq!(client.notification_router.state(), NotificationRouterState::Retired);
        assert_eq!(
            connection.subscription_calls.lock().expect("subscription log").as_slice(),
            &["start BlockAdded", "start VirtualChainChanged", "stop BlockAdded", "stop VirtualChainChanged"]
        );
    }

    #[tokio::test]
    async fn runtime_concurrency_is_bounded_to_thirty_two() {
        let gate = Arc::new(Semaphore::new(0));
        let requested = hash(1);
        let scripts =
            (0..33).map(|_| Script::block(Ok(GetBlockResponse { block: rpc_block(requested, vec![hash(8)]) }), Some(gate.clone())));
        let (client, _connection, _retirements) = client(scripts);
        let tasks = (0..33)
            .map(|_| {
                let client = client.clone();
                tokio::spawn(async move { client.full_block(requested).await })
            })
            .collect::<Vec<_>>();

        wait_for_counts(&client, 32, 1).await;
        gate.add_permits(33);
        for task in tasks {
            assert_eq!(task.await.expect("operation task").expect("full block").hash, requested);
        }
        assert_eq!(client.operation_counts(), super::RpcOperationCounts { active: 0, waiting: 0 });
    }

    fn client(scripts: impl IntoIterator<Item = Script>) -> (Arc<ValidatedRpcClient>, Arc<ScriptedConnection>, RetirementReceiver) {
        let connection = Arc::new(ScriptedConnection::new(scripts));
        let (retirement_tx, retirement_rx) = retirement_channel();
        let client = ValidatedRpcClient::new(connection.clone(), node_info(), retirement_tx);
        (client, connection, retirement_rx)
    }

    async fn assert_malformed_composite(name: &str, operation: CompositeOperation, scripts: Vec<Script>) {
        let expected_kind = operation.malformed_kind();
        let (client, connection, mut retirements) = client(scripts);
        let task = tokio::spawn({
            let client = client.clone();
            async move { operation.execute(&client).await }
        });

        let retirement = retirements.recv().await.expect("malformed composite retirement request");
        assert_eq!(retirement.reason(), &RetirementReason::MalformedRecoveryInput(expected_kind), "{name}");
        assert!(retirement.target().upgrade().is_some_and(|generation| Arc::ptr_eq(&generation, &client)), "{name}");
        assert!(client.retire().await, "{name}");
        tokio::task::yield_now().await;
        assert!(!task.is_finished(), "{name} completed before its exact-generation retirement barrier");
        retirement.complete(Ok(()));
        assert_eq!(task.await.expect("composite operation task"), Err(NodeError::RecoveryInputInvalid(expected_kind)), "{name}");
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1, "{name}");
    }

    type NotificationReceivers = (
        mpsc::Receiver<kgi_model::block::ValidatedNodeBlock>,
        mpsc::Receiver<kgi_model::vspc::VspcChange>,
        mpsc::UnboundedReceiver<super::NotificationFault>,
    );

    fn notification_channels() -> (NotificationChannels, NotificationReceivers) {
        let (block_tx, block_rx) = mpsc::channel(1);
        let (vspc_tx, vspc_rx) = mpsc::channel(1);
        let (fault_tx, fault_rx) = mpsc::unbounded_channel();
        (NotificationChannels::new(block_tx, vspc_tx, fault_tx), (block_rx, vspc_rx, fault_rx))
    }

    fn node_info() -> ValidatedNodeInfo {
        ValidatedNodeInfo {
            network_id: NetworkId::new(NetworkType::Mainnet),
            genesis_hash: hash(0),
            server_version: "test".to_string(),
            rpc_api_version: Some(RPC_API_VERSION),
            rpc_api_revision: Some(RPC_API_REVISION),
            consensus: KgiConsensusParams::resolve(NetworkId::new(NetworkType::Mainnet), None).expect("mainnet parameters"),
        }
    }

    fn dag_info(pruning_point_hash: BlockHash) -> GetBlockDagInfoResponse {
        GetBlockDagInfoResponse::new(
            NetworkId::new(NetworkType::Mainnet),
            0,
            0,
            Vec::new(),
            0.0,
            0,
            Vec::new(),
            pruning_point_hash,
            0,
            hash(9),
        )
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn rpc_block(hash_value: BlockHash, direct_parents: Vec<BlockHash>) -> RpcBlock {
        let selected_parent = direct_parents.first().copied().unwrap_or_else(|| hash(99));
        RpcBlock {
            header: RpcHeader {
                hash: hash_value,
                version: 0,
                parents_by_level: vec![direct_parents],
                hash_merkle_root: hash(10),
                accepted_id_merkle_root: hash(11),
                utxo_commitment: hash(12),
                timestamp: 10,
                bits: 0,
                nonce: 0,
                daa_score: 11,
                blue_work: BlueWorkType::from(13_u64),
                blue_score: 12,
                pruning_point: hash(14),
            },
            transactions: Vec::new(),
            verbose_data: Some(RpcBlockVerboseData {
                hash: hash_value,
                difficulty: 1.0,
                selected_parent_hash: selected_parent,
                transaction_ids: Vec::new(),
                is_header_only: false,
                blue_score: 12,
                children_hashes: Vec::new(),
                merge_set_blues_hashes: Vec::new(),
                merge_set_reds_hashes: Vec::new(),
                is_chain_block: true,
            }),
        }
    }

    fn vspc(removed: Vec<BlockHash>, added: Vec<BlockHash>) -> GetVirtualChainFromBlockV2Response {
        GetVirtualChainFromBlockV2Response {
            removed_chain_block_hashes: Arc::new(removed),
            added_chain_block_hashes: Arc::new(added),
            chain_block_accepted_transactions: Arc::new(Vec::new()),
        }
    }

    async fn wait_for_subscription_calls(connection: &ScriptedConnection, expected: usize) {
        timeout(Duration::from_secs(1), async {
            loop {
                if connection.subscription_calls.lock().expect("subscription log").len() >= expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscription calls must reach the expected count");
    }

    async fn wait_for_requests(connection: &ScriptedConnection, expected: usize) {
        timeout(Duration::from_secs(1), async {
            loop {
                if connection.requests().len() >= expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("RPC requests must reach the expected count");
    }

    async fn wait_for_subscription_state(client: &ValidatedRpcClient, expected: SubscriptionState) {
        timeout(Duration::from_secs(1), async {
            loop {
                if client.subscription_state() == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscription state must converge");
    }

    async fn wait_for_counts(client: &ValidatedRpcClient, active: usize, waiting: usize) {
        for _ in 0..10_000 {
            if client.operation_counts() == (super::RpcOperationCounts { active, waiting }) {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("operation counts did not reach active={active}, waiting={waiting}");
    }
}
