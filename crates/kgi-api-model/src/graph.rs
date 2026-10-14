use std::sync::Arc;

use kgi_model::block::{BlockColor, BlockCoordinate, BlockHash, Timestamp};

/// Complete retained level depth of the canonical Head graph.
pub const MAX_CACHE_DEPTH: u64 = 1_000;

/// Mutation policy associated with one graph view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrackingPolicy {
    /// Follow the advancing retained Head extent.
    Head,
    /// Retain fixed nominal bounds while applying eligible original updates.
    Fixed,
    /// Refuse every graph mutation.
    Frozen,
}

/// Complete public state for one graph level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Level {
    /// Number of allocated slots in the level.
    pub size: u64,
    /// Final committed VSPC DAA score, when the level has one.
    pub daa_score: Option<u64>,
}

/// Immutable identity of one child-parent graph relation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EdgeId {
    /// Parent block hash.
    pub parent: BlockHash,
    /// Child block hash.
    pub child: BlockHash,
}

/// One drawable child-parent relation with complete endpoint coordinates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphEdge {
    /// Immutable relation identity.
    pub id: EdgeId,
    /// Parent display coordinate.
    pub parent_coordinate: BlockCoordinate,
    /// Child display coordinate.
    pub child_coordinate: BlockCoordinate,
}

/// Complete immutable and projected state for one retained block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphBlock {
    /// Canonical block identity.
    pub hash: BlockHash,
    /// Database-local display coordinate.
    pub coordinate: BlockCoordinate,
    /// Header timestamp in whole milliseconds since the Unix epoch.
    pub timestamp: Timestamp,
    /// Block DAA score.
    pub daa_score: u64,
    /// Index of the selected parent in `direct_parents`, or `None` for Genesis.
    pub selected_parent_index: Option<u32>,
    /// Actual direct parents in the projection's local order.
    pub direct_parents: Arc<[BlockHash]>,
    /// Blue merge-set members in canonical first-occurrence order.
    pub blue_merge_set: Arc<[BlockHash]>,
    /// Red merge-set members in canonical first-occurrence order.
    pub red_merge_set: Arc<[BlockHash]>,
    /// Current projected block color.
    pub color: BlockColor,
    /// Whether the block belongs to the current VSPC projection.
    pub is_in_vspc: bool,
}

#[cfg(test)]
mod tests {
    use kgi_model::block::MAX_DAA_SCORE;

    use super::*;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    #[test]
    fn graph_block_retains_ordered_relationships_and_independent_projection_fields() {
        let block = GraphBlock {
            hash: hash(1),
            coordinate: BlockCoordinate::new(5, 2).expect("positive level"),
            timestamp: u64::MAX,
            daa_score: MAX_DAA_SCORE,
            selected_parent_index: Some(1),
            direct_parents: Arc::from([hash(2), hash(3)]),
            blue_merge_set: Arc::from([hash(4), hash(5)]),
            red_merge_set: Arc::from([hash(5), hash(6)]),
            color: BlockColor::Red,
            is_in_vspc: true,
        };

        assert_eq!(&*block.direct_parents, &[hash(2), hash(3)]);
        assert_eq!(&*block.blue_merge_set, &[hash(4), hash(5)]);
        assert_eq!(&*block.red_merge_set, &[hash(5), hash(6)]);
        assert_eq!(block.selected_parent_index, Some(1));
        assert_eq!(block.color, BlockColor::Red);
        assert!(block.is_in_vspc);
    }

    #[test]
    fn edge_identity_is_independent_of_endpoint_coordinates() {
        let id = EdgeId { parent: hash(1), child: hash(2) };
        let first = GraphEdge {
            id,
            parent_coordinate: BlockCoordinate::new(4, 0).expect("positive level"),
            child_coordinate: BlockCoordinate::new(5, 0).expect("positive level"),
        };
        let second = GraphEdge {
            id,
            parent_coordinate: BlockCoordinate::new(14, 3).expect("positive level"),
            child_coordinate: BlockCoordinate::new(15, 7).expect("positive level"),
        };

        assert_eq!(first.id, second.id);
        assert_ne!(first, second);
    }

    #[test]
    fn head_cache_depth_matches_the_settled_complete_level_bound() {
        assert_eq!(MAX_CACHE_DEPTH, 1_000);
    }
}
