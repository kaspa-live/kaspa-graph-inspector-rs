use std::future::Future;

use kaspa_consensus_core::network::NetworkId;
use kgi_model::block::{BlockHash, BlockPresence, MaterializedSyncAnchor, ValidatedNodeBlock};
use kgi_node::error::NodeError;
use kgi_node::rpc::ValidatedRpcClient;
use kgi_storage::{
    error::ProcessingDbError,
    generation::ValidatedDbClient,
    operation::{MaterializeBlockError, MaterializeBlockOutcome, ReferencePolicy},
};

mod dependency;
mod error;
mod marker;
mod orphan;
mod processor;

pub(crate) use processor::storage_fault;
#[allow(unused_imports, reason = "consumed by the upcoming ProcessingSession integration point")]
pub(crate) use processor::{BlockOverlap, BlockProcessor, BlockProcessorControlError, BlockProcessorEvent, BlockSubmissionError};

pub(crate) const PROCESSING_DATA_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug)]
pub(crate) enum BlockResolutionEvent {
    OrphanReady(ValidatedNodeBlock),
    DependencyResolved { requested_hash: BlockHash, result: Result<ValidatedNodeBlock, NodeError> },
}

pub(crate) struct BlockProcessorInitialization {
    pub anchor: MaterializedSyncAnchor,
    pub boundary_seal_blue_score: u64,
}

pub(crate) trait BlockDatabase: Send + Sync + 'static {
    fn network_id(&self) -> NetworkId;

    fn genesis_hash(&self) -> BlockHash;

    fn block_presence(&self, hash: BlockHash) -> impl Future<Output = Result<BlockPresence, ProcessingDbError>> + Send;

    fn materialize_block(
        &self,
        block: ValidatedNodeBlock,
        policy: ReferencePolicy,
    ) -> impl Future<Output = Result<MaterializeBlockOutcome, MaterializeBlockError>> + Send;
}

impl BlockDatabase for ValidatedDbClient {
    fn network_id(&self) -> NetworkId {
        self.binding().network_id()
    }

    fn genesis_hash(&self) -> BlockHash {
        self.binding().genesis_hash()
    }

    async fn block_presence(&self, hash: BlockHash) -> Result<BlockPresence, ProcessingDbError> {
        self.block_presence(hash).await
    }

    async fn materialize_block(
        &self,
        block: ValidatedNodeBlock,
        policy: ReferencePolicy,
    ) -> Result<MaterializeBlockOutcome, MaterializeBlockError> {
        self.materialize_block(block, policy).await
    }
}

pub(crate) trait BlockRpc: dependency::DependencyRpc {
    fn network_id(&self) -> NetworkId;

    fn genesis_hash(&self) -> BlockHash;
}

impl BlockRpc for ValidatedRpcClient {
    fn network_id(&self) -> NetworkId {
        self.node_info().network_id
    }

    fn genesis_hash(&self) -> BlockHash {
        self.node_info().genesis_hash
    }
}
