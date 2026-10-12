#![forbid(unsafe_code)]

//! Recovery engine and processing workers.

#[allow(dead_code, reason = "child workers are integrated by the next BlockProcessor point")]
mod block_processor;
