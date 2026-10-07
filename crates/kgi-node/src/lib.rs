#![forbid(unsafe_code)]

//! NodeService and validated RPC integration.

pub mod consensus;
pub mod error;
pub mod rpc;

// Point 4 constructs the production adapter and owns its connection lifecycle.
#[allow(dead_code)]
mod client;
mod normalization;
// Point 4 consumes retirement requests in the permanent service loop.
#[allow(dead_code)]
mod runtime;
