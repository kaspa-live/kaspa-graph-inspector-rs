use kgi_model::block::{BlockHash, ValidatedNodeBlock};
use kgi_node::error::NodeError;

mod dependency;
mod error;
mod orphan;

pub(crate) const PROCESSING_DATA_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug)]
pub(crate) enum BlockResolutionEvent {
    OrphanReady(ValidatedNodeBlock),
    DependencyResolved { requested_hash: BlockHash, result: Result<ValidatedNodeBlock, NodeError> },
}
