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

/// Builds the public API router awaiting its application state.
///
/// The composition crate nests this router at `/api/v1` and supplies the
/// shared [`ApiService`] exactly once after composing the complete HTTP tree.
pub fn router() -> Router<Arc<ApiService>> {
    transport::router()
}
