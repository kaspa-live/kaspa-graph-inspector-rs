#![forbid(unsafe_code)]

//! NodeService and validated RPC integration.

pub mod consensus;
pub mod error;
pub mod rpc;

// Point 2 wires these private raw-value adapters into `ValidatedRpcClient`.
#[allow(dead_code)]
mod normalization;
