#![forbid(unsafe_code)]

//! ApiService, graph publication, and HTTP/SSE runtime.

pub(crate) mod admission;
pub(crate) mod api_db;
pub(crate) mod cache;
pub(crate) mod client;
pub(crate) mod codec;
pub(crate) mod delta;
pub(crate) mod encoding;
pub(crate) mod publication;
mod transport;

pub mod error;
pub mod service;

use std::sync::Arc;

use axum::Router;
use service::ApiService;
use tokio::io::{AsyncRead, AsyncWrite};

/// Builds the public API router awaiting its application state.
///
/// The composition crate nests this router at `/api/v1` and supplies the
/// shared [`ApiService`] exactly once after composing the complete HTTP tree.
pub fn router() -> Router<Arc<ApiService>> {
    transport::router()
}

/// Serves one accepted socket through KGI's HTTP/1.1 delivery boundary.
///
/// The supplied application must be the completed router, including the API
/// router state and any outer static-file and tracing composition.
pub async fn serve_http1_connection<I>(io: I, application: Router) -> Result<(), hyper::Error>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    transport::connection::serve(io, application).await
}
