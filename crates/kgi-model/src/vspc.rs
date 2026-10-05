use std::sync::Arc;

use crate::block::{BlockHash, CompactId, VspcPoint};

/// Normalized hash-level VSPC transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VspcChange {
    pub removed: Arc<[BlockHash]>,
    pub added: Arc<[BlockHash]>,
}

/// Materialized and endpoint-resolved VSPC transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadyVspcChange {
    pub source: VspcPoint,
    pub destination: VspcPoint,
    pub removed: Arc<[CompactId]>,
    pub added: Arc<[CompactId]>,
}
