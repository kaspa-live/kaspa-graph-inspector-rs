use std::{
    future::Future,
    sync::{
        Arc, Weak,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};

use kaspa_consensus_core::{errors::consensus::ConsensusError, network::NetworkId};
use kaspa_rpc_core::{
    GetBlockDagInfoRequest, GetBlockRequest, GetBlocksRequest, GetSinkRequest, GetVirtualChainFromBlockV2Request,
    RpcDataVerbosityLevel, RpcError, RpcResult,
};
use kgi_model::{
    block::{BlockHash, ValidatedNodeBlock, ValidatedRecoveryHeader},
    lifecycle::RecoveryInputKind,
    vspc::VspcChange,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};

use crate::{
    client::RpcConnection,
    consensus::KgiConsensusParams,
    error::NodeError,
    normalization::{ResponseNormalizationError, ResponseNormalizer},
    runtime::{RetirementRequest, RetirementSender},
};

#[allow(dead_code, reason = "NodeService constructs validated generations in point 4")]
const MAX_RPC_CONCURRENCY: usize = 32;

const GENERATION_ACTIVE: u8 = 0;
const GENERATION_LOST: u8 = 1;
const GENERATION_CANCELLED: u8 = 2;

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

/// RPC capability bound permanently to one validated physical connection.
pub struct ValidatedRpcClient {
    connection: Arc<dyn RpcConnection>,
    node_info: ValidatedNodeInfo,
    normalizer: Arc<ResponseNormalizer>,
    admission: AtomicU8,
    admission_tx: watch::Sender<u8>,
    permits: Arc<Semaphore>,
    active: AtomicUsize,
    waiting: AtomicUsize,
    retirement_tx: RetirementSender,
    self_weak: Weak<Self>,
}

impl ValidatedRpcClient {
    #[allow(dead_code, reason = "NodeService constructs validated generations in point 4")]
    pub(crate) fn new(connection: Arc<dyn RpcConnection>, node_info: ValidatedNodeInfo, retirement_tx: RetirementSender) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| {
            let (admission_tx, _) = watch::channel(GENERATION_ACTIVE);
            Self {
                connection,
                normalizer: Arc::new(ResponseNormalizer::new(node_info.genesis_hash)),
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

    /// Obtains and normalizes the current pruning-point block.
    pub async fn current_pruning_point_block(&self) -> Result<ValidatedNodeBlock, NodeError> {
        let mut operation = self.begin_operation().await?;
        let dag_info = self
            .call(&mut operation.admission, self.connection.get_block_dag_info(GetBlockDagInfoRequest {}))
            .await
            .map_err(map_opaque_rpc_error)?;
        let pruning_point_hash = dag_info.pruning_point_hash;
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

    async fn finish_normalized<T>(
        &self,
        operation: &OperationGuard<'_>,
        result: Result<T, ResponseNormalizationError>,
    ) -> Result<T, NodeError> {
        match result {
            Err(ResponseNormalizationError::Malformed(kind)) => self.return_malformed(operation, kind).await,
            Ok(value) => {
                self.require_active()?;
                Ok(value)
            }
            Err(ResponseNormalizationError::ScoreOutOfRange(fault)) => {
                self.require_active()?;
                Err(NodeError::ScoreOutOfRange(fault))
            }
        }
    }

    async fn return_malformed<T>(&self, _operation: &OperationGuard<'_>, kind: RecoveryInputKind) -> Result<T, NodeError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.retirement_tx
            .send(RetirementRequest::new(self.self_weak.clone(), kind, completion_tx))
            .map_err(|_| NodeError::RetirementControlUnavailable)?;
        completion_rx.await.map_err(|_| NodeError::RetirementControlUnavailable)?;
        Err(NodeError::RecoveryInputInvalid(kind))
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

    #[allow(dead_code, reason = "NodeService consumes this in point 4")]
    pub(crate) async fn retire(&self) -> bool {
        self.close(GENERATION_LOST).await
    }

    #[allow(dead_code, reason = "NodeService consumes this in point 4")]
    pub(crate) async fn cancel(&self) -> bool {
        self.close(GENERATION_CANCELLED).await
    }

    async fn close(&self, state: u8) -> bool {
        if self.admission.compare_exchange(GENERATION_ACTIVE, state, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return false;
        }
        self.admission_tx.send_replace(state);
        if let Err(error) = self.connection.disconnect().await {
            kaspa_core::warn!("failed to disconnect retired RPC generation: {error}");
        }
        true
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
        let expected = ConsensusError::BlockNotFound(hash).to_string();
        if message == expected {
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

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use kaspa_consensus_core::{
        BlueWorkType,
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
        block::{BlockHash, MAX_DAA_SCORE},
        lifecycle::{RecoveryInputKind, ScoreRangeFault},
    };
    use tokio::sync::{Semaphore, mpsc};

    use super::{GetBlockCallError, ValidatedNodeInfo, ValidatedRpcClient, classify_get_block_error};
    use crate::{
        client::RpcConnection,
        consensus::KgiConsensusParams,
        error::NodeError,
        runtime::{RetirementReceiver, retirement_channel},
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
        disconnects: AtomicUsize,
    }

    impl ScriptedConnection {
        fn new(scripts: impl IntoIterator<Item = Script>) -> Self {
            Self {
                scripts: tokio::sync::Mutex::new(scripts.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
                disconnects: AtomicUsize::new(0),
            }
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

    #[test]
    fn get_block_not_found_match_is_exact_and_hash_specific() {
        let requested = hash(1);
        let exact = ConsensusError::BlockNotFound(requested).to_string();
        assert_eq!(classify_get_block_error(requested, RpcError::General(exact.clone())), GetBlockCallError::NotFound);

        for message in [
            ConsensusError::BlockNotFound(hash(2)).to_string(),
            exact.to_uppercase(),
            format!(" {exact}"),
            format!("{exact} "),
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
    async fn exact_get_block_not_found_remains_distinct_without_retirement() {
        let requested = hash(1);
        let message = ConsensusError::BlockNotFound(requested).to_string();
        let scripts = [Script::block(Err(RpcError::General(message)), None)];
        let (client, connection, mut retirements) = client(scripts);

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
        assert_eq!(retirement.cause(), RecoveryInputKind::MalformedGetBlock);
        assert!(client.retire().await);
        retirement.complete();
        assert_eq!(
            operation.await.expect("operation task"),
            Err(NodeError::RecoveryInputInvalid(RecoveryInputKind::MalformedGetBlock))
        );
        assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn advertised_sink_not_found_uses_sink_malformed_classification() {
        let sink = hash(7);
        let message = ConsensusError::BlockNotFound(sink).to_string();
        let scripts = [Script::sink(Ok(GetSinkResponse::new(sink))), Script::block(Err(RpcError::General(message)), None)];
        let (client, _connection, mut retirements) = client(scripts);
        let operation = tokio::spawn({
            let client = client.clone();
            async move { client.catchup_sink_sample().await }
        });

        let retirement = retirements.recv().await.expect("retirement request");
        assert_eq!(retirement.cause(), RecoveryInputKind::MalformedCatchupSinkResponse);
        assert!(client.retire().await);
        retirement.complete();
        assert_eq!(
            operation.await.expect("operation task"),
            Err(NodeError::RecoveryInputInvalid(RecoveryInputKind::MalformedCatchupSinkResponse))
        );
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
        assert_eq!(retirement.cause(), RecoveryInputKind::MalformedGetBlocks);
        assert!(client.retire().await);
        retirement.complete();
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
        let generation = retirement.generation().upgrade().expect("exact generation");
        assert!(Arc::ptr_eq(&generation, &client));
        assert_eq!(retirement.cause(), RecoveryInputKind::MalformedGetBlock);
        assert!(client.retire().await);
        tokio::task::yield_now().await;
        assert!(!operation.is_finished());
        retirement.complete();

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
    async fn retirement_and_cancellation_win_blocked_operation_races() {
        for cancelled in [false, true] {
            let gate = Arc::new(Semaphore::new(0));
            let requested = hash(1);
            let scripts = [Script::block(Ok(GetBlockResponse { block: rpc_block(requested, vec![hash(8)]) }), Some(gate))];
            let (client, connection, _retirements) = client(scripts);
            let operation = tokio::spawn({
                let client = client.clone();
                async move { client.full_block(requested).await }
            });
            wait_for_counts(&client, 1, 0).await;

            let changed = if cancelled { client.cancel().await } else { client.retire().await };
            assert!(changed);
            let expected = if cancelled { NodeError::Cancelled } else { NodeError::GenerationLost };
            assert_eq!(operation.await.expect("operation task"), Err(expected));
            assert_eq!(connection.disconnects.load(Ordering::Relaxed), 1);
        }
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
