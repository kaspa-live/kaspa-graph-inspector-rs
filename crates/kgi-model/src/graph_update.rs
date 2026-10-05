use std::sync::Arc;

use crate::block::{BlockColor, BlockCoordinate, BlockHash, CompactId, Timestamp};

/// A committed direct-parent projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentCommitted {
    pub hash: BlockHash,
    pub coordinate: Option<BlockCoordinate>,
}

/// A complete committed snapshot of one affected level.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LevelCommitted {
    pub level: u64,
    pub size: u64,
    pub daa_score: Option<u64>,
}

/// Projection payload produced by a newly inserted materialized block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockCommitted {
    pub id: CompactId,
    pub hash: BlockHash,
    pub coordinate: BlockCoordinate,
    pub timestamp: Timestamp,
    pub daa_score: u64,
    pub selected_parent_index: Option<u32>,
    pub direct_parents: Arc<[ParentCommitted]>,
    pub blue_merge_set: Arc<[BlockHash]>,
    pub red_merge_set: Arc<[BlockHash]>,
    pub color: BlockColor,
    pub is_in_vspc: bool,
    pub level_snapshots: Arc<[LevelCommitted]>,
}

/// Projection payload produced by a committed VSPC transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VspcCommitted {
    pub source: BlockHash,
    pub destination: BlockHash,
    pub removed: Arc<[BlockHash]>,
    pub added: Arc<[BlockHash]>,
    pub level_snapshots: Arc<[LevelCommitted]>,
}

/// Ordered per-session graph publication input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphUpdate {
    PublishPostSeal,
    BlockCommitted(BlockCommitted),
    VspcCommitted(VspcCommitted),
    Live,
}
