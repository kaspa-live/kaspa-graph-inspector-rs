#![forbid(unsafe_code)]

//! Recovery engine and processing workers.

pub(crate) const PROCESSING_DATA_CHANNEL_CAPACITY: usize = 1024;

#[allow(dead_code, reason = "integrated by the upcoming ProcessingSession point")]
mod block_processor;
mod fault;

#[allow(dead_code, reason = "integrated by the upcoming ProcessingSession point")]
mod vspc_processor;
