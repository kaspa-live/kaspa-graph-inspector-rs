//! Shared request and result values for coherent graph projections.

use std::collections::{BTreeMap, HashMap};

use kgi_model::block::{BlockHash, CompactId};

use crate::graph::{EdgeId, GraphBlock, GraphEdge, Level, LevelExtent};

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

impl GraphWindowResolution {
    /// Resolves the effective window around one retained anchor.
    ///
    /// Even depths place their additional level below the anchor. Capacity
    /// unavailable at level one or the supplied Head shifts to the opposite
    /// side.
    ///
    /// # Panics
    ///
    /// Panics when `max_depth` is zero or `resolved_level` lies outside
    /// `1..=high_level`.
    #[must_use]
    pub fn around(resolved_level: u32, high_level: u32, max_depth: u32) -> Self {
        assert!(max_depth > 0, "an anchored graph window has positive depth");
        assert!((1..=high_level).contains(&resolved_level), "an anchored graph window resolves inside retained bounds");

        let levels_above = (max_depth - 1) / 2;
        let levels_below = (max_depth - 1) - levels_above;
        let mut low_level = resolved_level.saturating_sub(levels_below).max(1);
        let mut effective_high_level = resolved_level.saturating_add(levels_above).min(high_level);

        let initial_depth = effective_high_level - low_level + 1;
        let unused_depth = max_depth - initial_depth;
        effective_high_level = effective_high_level.saturating_add(unused_depth).min(high_level);

        let shifted_depth = effective_high_level - low_level + 1;
        let remaining_depth = max_depth - shifted_depth;
        low_level = low_level.saturating_sub(remaining_depth).max(1);

        Self { resolved_level, effective_start_level: low_level, effective_end_level: effective_high_level }
    }

    /// Returns the effective inclusive nominal extent.
    #[must_use]
    pub fn extent(self) -> LevelExtent {
        LevelExtent::new(self.effective_start_level, self.effective_end_level)
            .expect("an anchored graph-window resolution has valid effective bounds")
    }
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

#[cfg(test)]
mod tests {
    use crate::{graph::LevelExtent, projection::GraphWindowResolution};

    fn extent(low_level: u32, high_level: u32) -> LevelExtent {
        LevelExtent::new(low_level, high_level).expect("valid retained extent")
    }

    #[test]
    fn anchored_resolution_allocates_even_depth_to_the_historical_side() {
        assert_eq!(
            GraphWindowResolution::around(10, 20, 5),
            GraphWindowResolution { resolved_level: 10, effective_start_level: 8, effective_end_level: 12 }
        );
        assert_eq!(
            GraphWindowResolution::around(10, 20, 4),
            GraphWindowResolution { resolved_level: 10, effective_start_level: 8, effective_end_level: 11 }
        );
    }

    #[test]
    fn anchored_resolution_shifts_at_both_retained_boundaries() {
        assert_eq!(
            GraphWindowResolution::around(1, 10, 5),
            GraphWindowResolution { resolved_level: 1, effective_start_level: 1, effective_end_level: 5 }
        );
        assert_eq!(
            GraphWindowResolution::around(10, 10, 5),
            GraphWindowResolution { resolved_level: 10, effective_start_level: 6, effective_end_level: 10 }
        );
        assert_eq!(
            GraphWindowResolution::around(2, 3, 250),
            GraphWindowResolution { resolved_level: 2, effective_start_level: 1, effective_end_level: 3 }
        );
    }

    #[test]
    fn anchored_resolution_exposes_its_validated_extent() {
        let resolution = GraphWindowResolution::around(7, 10, 4);
        assert_eq!(resolution.extent(), extent(5, 8));
    }
}
