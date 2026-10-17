//! Shared request and result values for coherent graph projections.

use std::collections::{BTreeMap, HashMap};

use kgi_model::block::{BlockHash, CompactId};

use crate::graph::{EdgeId, GraphBlock, GraphEdge, Level};

/// Maximum nominal depth accepted for an anchored graph window.
pub const MAX_WINDOW_DEPTH: u32 = 250;

/// Anchor used to resolve one fixed graph window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphWindowAnchor {
    /// Resolve the exact retained level.
    Level(u32),
    /// Resolve the level of one materialized block.
    BlockHash(BlockHash),
    /// Resolve the current-VSPC floor at or below one DAA score.
    DaaScore(u64),
}

/// Normal reason an anchored graph window cannot be resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphWindowAnchorUnavailable {
    /// The requested level is not retained.
    LevelNotRetained {
        /// Exact level requested by the caller.
        requested_level: u32,
    },
    /// The requested hash does not identify a materialized block.
    BlockNotMaterialized {
        /// Exact block hash requested by the caller.
        requested_hash: BlockHash,
    },
    /// No retained current-VSPC DAA score is at or below the requested score.
    NoRetainedDaaMatch {
        /// Exact DAA score requested by the caller.
        requested_score: u64,
    },
}

/// Resolved focus and effective inclusive bounds of one graph window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphWindowResolution {
    /// Level selected by the request anchor.
    pub resolved_level: u32,
    /// Inclusive lower level returned for the window.
    pub effective_start_level: u32,
    /// Inclusive upper level returned for the window.
    pub effective_end_level: u32,
}

/// Request for one coherent database-backed graph projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphViewSeedRequest {
    /// Load the complete retained Head publication seed.
    HeadPublication,
    /// Load one request-local fixed window around a resolved anchor.
    AnchoredWindow {
        /// Anchor resolved inside the projection snapshot.
        anchor: GraphWindowAnchor,
        /// Effective positive nominal depth already capped by the API owner.
        max_depth: u32,
    },
}

/// Complete detached graph projection and publication-alignment metadata.
#[derive(Debug, Eq, PartialEq)]
pub struct GraphViewSeed {
    /// Anchor resolution and effective nominal extent of this projection.
    pub resolution: GraphWindowResolution,
    /// Maximum nominal depth assigned to the resulting graph view.
    pub max_depth: u32,
    /// Complete nominal and retained edge-endpoint levels.
    pub levels: BTreeMap<u32, Level>,
    /// Complete materialized blocks inside the nominal extent.
    pub blocks: HashMap<BlockHash, GraphBlock>,
    /// Complete child-parent edges whose spans intersect the nominal extent.
    pub edges: HashMap<EdgeId, GraphEdge>,
    /// Greatest materialized block ID in the complete snapshot.
    pub snapshot_max_materialized_id: CompactId,
    /// Committed materialized VSPC sink in the complete snapshot.
    pub snapshot_vspc_sink: BlockHash,
}

/// Result of a database-backed graph projection request.
#[derive(Debug, Eq, PartialEq)]
pub enum GraphViewSeedOutcome {
    /// One complete coherent projection was loaded.
    Loaded(GraphViewSeed),
    /// An anchored request had no normal retained match.
    AnchorUnavailable(GraphWindowAnchorUnavailable),
}
