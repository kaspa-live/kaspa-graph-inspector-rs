use std::sync::Arc;

use async_trait::async_trait;
use kaspa_grpc_client::GrpcClient;
use kaspa_rpc_core::{
    GetBlockDagInfoRequest, GetBlockDagInfoResponse, GetBlockRequest, GetBlockResponse, GetBlocksRequest, GetBlocksResponse,
    GetSinkRequest, GetSinkResponse, GetVirtualChainFromBlockV2Request, GetVirtualChainFromBlockV2Response, RpcResult,
    api::rpc::RpcApi,
};
use url::Url;

#[async_trait]
pub(crate) trait RpcConnector: Send + Sync {
    async fn connect(&self, endpoint: &Url) -> Result<Arc<dyn RpcConnection>, Arc<str>>;
}

pub(crate) struct GrpcConnector;

#[async_trait]
impl RpcConnector for GrpcConnector {
    async fn connect(&self, endpoint: &Url) -> Result<Arc<dyn RpcConnection>, Arc<str>> {
        // `GrpcClient::connect` selects direct notification mode and fixes its
        // internal automatic reconnect argument to false.
        let client = GrpcClient::connect(endpoint.to_string()).await.map_err(|error| Arc::from(error.to_string()))?;
        Ok(Arc::new(GrpcRpcConnection::new(client)))
    }
}

#[async_trait]
pub(crate) trait RpcConnection: Send + Sync {
    async fn get_block(&self, request: GetBlockRequest) -> RpcResult<GetBlockResponse>;
    async fn get_blocks(&self, request: GetBlocksRequest) -> RpcResult<GetBlocksResponse>;
    async fn get_block_dag_info(&self, request: GetBlockDagInfoRequest) -> RpcResult<GetBlockDagInfoResponse>;
    async fn get_sink(&self, request: GetSinkRequest) -> RpcResult<GetSinkResponse>;
    async fn get_virtual_chain_from_block_v2(
        &self,
        request: GetVirtualChainFromBlockV2Request,
    ) -> RpcResult<GetVirtualChainFromBlockV2Response>;
    async fn disconnect(&self) -> Result<(), Arc<str>>;
}

pub(crate) struct GrpcRpcConnection {
    inner: GrpcClient,
}

impl GrpcRpcConnection {
    pub(crate) fn new(inner: GrpcClient) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl RpcConnection for GrpcRpcConnection {
    async fn get_block(&self, request: GetBlockRequest) -> RpcResult<GetBlockResponse> {
        self.inner.get_block_call(None, request).await
    }

    async fn get_blocks(&self, request: GetBlocksRequest) -> RpcResult<GetBlocksResponse> {
        self.inner.get_blocks_call(None, request).await
    }

    async fn get_block_dag_info(&self, request: GetBlockDagInfoRequest) -> RpcResult<GetBlockDagInfoResponse> {
        self.inner.get_block_dag_info_call(None, request).await
    }

    async fn get_sink(&self, request: GetSinkRequest) -> RpcResult<GetSinkResponse> {
        self.inner.get_sink_call(None, request).await
    }

    async fn get_virtual_chain_from_block_v2(
        &self,
        request: GetVirtualChainFromBlockV2Request,
    ) -> RpcResult<GetVirtualChainFromBlockV2Response> {
        self.inner.get_virtual_chain_from_block_v2_call(None, request).await
    }

    async fn disconnect(&self) -> Result<(), Arc<str>> {
        self.inner.disconnect().await.map_err(|error| Arc::from(error.to_string()))
    }
}
