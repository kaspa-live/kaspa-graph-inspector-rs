use std::sync::Arc;

use async_trait::async_trait;
use kaspa_grpc_client::{GrpcClient, GrpcClientNotify};
use kaspa_notify::scope::{BlockAddedScope, VirtualChainChangedScope};
use kaspa_rpc_core::{
    GetBlockDagInfoRequest, GetBlockDagInfoResponse, GetBlockRequest, GetBlockResponse, GetBlocksRequest, GetBlocksResponse,
    GetServerInfoRequest, GetServerInfoResponse, GetSinkRequest, GetSinkResponse, GetVirtualChainFromBlockV2Request,
    GetVirtualChainFromBlockV2Response, RpcResult, api::rpc::RpcApi,
};
use url::Url;

#[derive(Clone)]
pub(crate) struct ServerInfoObservation {
    pub(crate) rpc_api_version: Option<u16>,
    pub(crate) rpc_api_revision: Option<u16>,
    pub(crate) server_version: String,
    pub(crate) network_id: kaspa_consensus_core::network::NetworkId,
    pub(crate) is_synced: bool,
}

impl From<GetServerInfoResponse> for ServerInfoObservation {
    fn from(response: GetServerInfoResponse) -> Self {
        Self {
            rpc_api_version: Some(response.rpc_api_version),
            rpc_api_revision: Some(response.rpc_api_revision),
            server_version: response.server_version,
            network_id: response.network_id,
            is_synced: response.is_synced,
        }
    }
}

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
    async fn get_server_info(&self, _request: GetServerInfoRequest) -> RpcResult<ServerInfoObservation> {
        Err(kaspa_rpc_core::RpcError::NotImplemented)
    }
    async fn get_block(&self, request: GetBlockRequest) -> RpcResult<GetBlockResponse>;
    async fn get_blocks(&self, request: GetBlocksRequest) -> RpcResult<GetBlocksResponse>;
    async fn get_block_dag_info(&self, request: GetBlockDagInfoRequest) -> RpcResult<GetBlockDagInfoResponse>;
    async fn get_sink(&self, request: GetSinkRequest) -> RpcResult<GetSinkResponse>;
    async fn get_virtual_chain_from_block_v2(
        &self,
        request: GetVirtualChainFromBlockV2Request,
    ) -> RpcResult<GetVirtualChainFromBlockV2Response>;
    fn handle_stop_notify(&self) -> bool {
        false
    }
    fn handle_message_id(&self) -> bool {
        false
    }
    async fn start_collector(&self, _notify: GrpcClientNotify) {}
    async fn start_block_added(&self) -> RpcResult<()> {
        Err(kaspa_rpc_core::RpcError::NotImplemented)
    }
    async fn start_virtual_chain_changed(&self) -> RpcResult<()> {
        Err(kaspa_rpc_core::RpcError::NotImplemented)
    }
    async fn stop_block_added(&self) -> RpcResult<()> {
        Err(kaspa_rpc_core::RpcError::NotImplemented)
    }
    async fn stop_virtual_chain_changed(&self) -> RpcResult<()> {
        Err(kaspa_rpc_core::RpcError::NotImplemented)
    }
    async fn wait_for_disconnect(&self) -> Result<(), Arc<str>> {
        std::future::pending().await
    }
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
    async fn get_server_info(&self, request: GetServerInfoRequest) -> RpcResult<ServerInfoObservation> {
        self.inner.get_server_info_call(None, request).await.map(ServerInfoObservation::from)
    }

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

    fn handle_stop_notify(&self) -> bool {
        self.inner.handle_stop_notify()
    }

    fn handle_message_id(&self) -> bool {
        self.inner.handle_message_id()
    }

    async fn start_collector(&self, notify: GrpcClientNotify) {
        self.inner.start(Some(notify)).await;
    }

    async fn start_block_added(&self) -> RpcResult<()> {
        self.inner.start_notify(GrpcClient::DIRECT_MODE_LISTENER_ID, BlockAddedScope::default().into()).await
    }

    async fn start_virtual_chain_changed(&self) -> RpcResult<()> {
        self.inner.start_notify(GrpcClient::DIRECT_MODE_LISTENER_ID, VirtualChainChangedScope::new(false).into()).await
    }

    async fn stop_block_added(&self) -> RpcResult<()> {
        self.inner.stop_notify(GrpcClient::DIRECT_MODE_LISTENER_ID, BlockAddedScope::default().into()).await
    }

    async fn stop_virtual_chain_changed(&self) -> RpcResult<()> {
        self.inner.stop_notify(GrpcClient::DIRECT_MODE_LISTENER_ID, VirtualChainChangedScope::new(false).into()).await
    }

    async fn wait_for_disconnect(&self) -> Result<(), Arc<str>> {
        self.inner.join().await.map_err(|error| Arc::from(error.to_string()))
    }

    async fn disconnect(&self) -> Result<(), Arc<str>> {
        self.inner.disconnect().await.map_err(|error| Arc::from(error.to_string()))
    }
}
