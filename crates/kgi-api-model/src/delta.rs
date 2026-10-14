use std::collections::HashMap;

use kgi_model::block::{BlockColor, BlockHash};
use thiserror::Error;

use crate::graph::{EdgeId, GraphBlock, GraphEdge, Level};

/// Complete before-and-after state for one graph level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LevelChange {
    /// Level affected by the change.
    pub level: u64,
    /// State before the delta, or absence.
    pub before: Option<Level>,
    /// State after the delta, or absence.
    pub after: Option<Level>,
}

/// Complete before-and-after state for one projected field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldChange<T> {
    /// Field value before the delta.
    pub before: T,
    /// Field value after the delta.
    pub after: T,
}

/// Failure to construct a canonical graph delta.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphDeltaCreationError {
    /// A delta must advance to a strictly newer revision.
    #[error("graph delta target revision {to_revision_id} does not follow source revision {from_revision_id}")]
    NonForwardRevision { from_revision_id: u64, to_revision_id: u64 },
}

/// One canonical graph-state transition over a forward revision interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphDelta {
    from_revision_id: u64,
    to_revision_id: u64,
    revision_timestamp_us: u64,
    high_level: u64,
    block_changes: HashMap<BlockHash, Option<GraphBlock>>,
    edge_changes: HashMap<EdgeId, Option<GraphEdge>>,
    level_changes: Vec<LevelChange>,
    is_in_vspc_changes: HashMap<BlockHash, FieldChange<bool>>,
    color_changes: HashMap<BlockHash, FieldChange<BlockColor>>,
}

impl GraphDelta {
    /// Constructs a delta whose target revision strictly follows its source.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        from_revision_id: u64,
        to_revision_id: u64,
        revision_timestamp_us: u64,
        high_level: u64,
        block_changes: HashMap<BlockHash, Option<GraphBlock>>,
        edge_changes: HashMap<EdgeId, Option<GraphEdge>>,
        level_changes: Vec<LevelChange>,
        is_in_vspc_changes: HashMap<BlockHash, FieldChange<bool>>,
        color_changes: HashMap<BlockHash, FieldChange<BlockColor>>,
    ) -> Result<Self, GraphDeltaCreationError> {
        if to_revision_id <= from_revision_id {
            return Err(GraphDeltaCreationError::NonForwardRevision { from_revision_id, to_revision_id });
        }

        Ok(Self {
            from_revision_id,
            to_revision_id,
            revision_timestamp_us,
            high_level,
            block_changes,
            edge_changes,
            level_changes,
            is_in_vspc_changes,
            color_changes,
        })
    }

    /// Returns the source revision.
    #[must_use]
    pub const fn from_revision_id(&self) -> u64 {
        self.from_revision_id
    }

    /// Returns the target revision.
    #[must_use]
    pub const fn to_revision_id(&self) -> u64 {
        self.to_revision_id
    }

    /// Returns the publication-local timestamp of the target revision.
    #[must_use]
    pub const fn revision_timestamp_us(&self) -> u64 {
        self.revision_timestamp_us
    }

    /// Returns the target high level of this delta's lineage.
    #[must_use]
    pub const fn high_level(&self) -> u64 {
        self.high_level
    }

    /// Returns the absolute block changes.
    #[must_use]
    pub const fn block_changes(&self) -> &HashMap<BlockHash, Option<GraphBlock>> {
        &self.block_changes
    }

    /// Returns the absolute edge changes.
    #[must_use]
    pub const fn edge_changes(&self) -> &HashMap<EdgeId, Option<GraphEdge>> {
        &self.edge_changes
    }

    /// Returns the ordered level changes.
    #[must_use]
    pub fn level_changes(&self) -> &[LevelChange] {
        &self.level_changes
    }

    /// Returns the VSPC-membership changes.
    #[must_use]
    pub const fn is_in_vspc_changes(&self) -> &HashMap<BlockHash, FieldChange<bool>> {
        &self.is_in_vspc_changes
    }

    /// Returns the color changes.
    #[must_use]
    pub const fn color_changes(&self) -> &HashMap<BlockHash, FieldChange<BlockColor>> {
        &self.color_changes
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kgi_model::block::BlockCoordinate;

    use super::*;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn block(block_hash: BlockHash) -> GraphBlock {
        GraphBlock {
            hash: block_hash,
            coordinate: BlockCoordinate::new(4, 0).expect("positive level"),
            timestamp: 5,
            daa_score: 6,
            selected_parent_index: None,
            direct_parents: Arc::from([]),
            blue_merge_set: Arc::from([]),
            red_merge_set: Arc::from([]),
            color: BlockColor::Gray,
            is_in_vspc: false,
        }
    }

    fn delta(from: u64, to: u64) -> Result<GraphDelta, GraphDeltaCreationError> {
        let block_hash = hash(1);
        let edge_id = EdgeId { parent: hash(2), child: block_hash };
        GraphDelta::new(
            from,
            to,
            77,
            4,
            HashMap::from([(block_hash, Some(block(block_hash)))]),
            HashMap::from([(
                edge_id,
                Some(GraphEdge {
                    id: edge_id,
                    parent_coordinate: BlockCoordinate::new(3, 0).expect("positive level"),
                    child_coordinate: BlockCoordinate::new(4, 0).expect("positive level"),
                }),
            )]),
            vec![LevelChange { level: 4, before: None, after: Some(Level { size: 1, daa_score: None }) }],
            HashMap::from([(block_hash, FieldChange { before: false, after: true })]),
            HashMap::from([(block_hash, FieldChange { before: BlockColor::Gray, after: BlockColor::Blue })]),
        )
    }

    #[test]
    fn constructor_rejects_equal_and_backward_revisions() {
        assert_eq!(delta(8, 8), Err(GraphDeltaCreationError::NonForwardRevision { from_revision_id: 8, to_revision_id: 8 }));
        assert_eq!(delta(8, 7), Err(GraphDeltaCreationError::NonForwardRevision { from_revision_id: 8, to_revision_id: 7 }));
    }

    #[test]
    fn constructor_preserves_the_complete_checked_transition() {
        let delta = delta(8, 9).expect("forward revision");
        let block_hash = hash(1);
        let edge_id = EdgeId { parent: hash(2), child: block_hash };

        assert_eq!(delta.from_revision_id(), 8);
        assert_eq!(delta.to_revision_id(), 9);
        assert_eq!(delta.revision_timestamp_us(), 77);
        assert_eq!(delta.high_level(), 4);
        assert_eq!(delta.block_changes().get(&block_hash), Some(&Some(block(block_hash))));
        assert_eq!(delta.edge_changes().get(&edge_id).and_then(Option::as_ref).map(|edge| edge.id), Some(edge_id));
        assert_eq!(delta.level_changes(), &[LevelChange { level: 4, before: None, after: Some(Level { size: 1, daa_score: None }) }]);
        assert_eq!(delta.is_in_vspc_changes().get(&block_hash), Some(&FieldChange { before: false, after: true }));
        assert_eq!(delta.color_changes().get(&block_hash), Some(&FieldChange { before: BlockColor::Gray, after: BlockColor::Blue }));
    }

    #[test]
    fn empty_mutations_can_still_advance_a_revision_interval() {
        let delta = GraphDelta::new(4, 7, 88, 12, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new())
            .expect("a forward interval does not require a visible mutation");

        assert_eq!(delta.from_revision_id(), 4);
        assert_eq!(delta.to_revision_id(), 7);
        assert_eq!(delta.revision_timestamp_us(), 88);
        assert_eq!(delta.high_level(), 12);
        assert!(delta.block_changes().is_empty());
        assert!(delta.edge_changes().is_empty());
        assert!(delta.level_changes().is_empty());
        assert!(delta.is_in_vspc_changes().is_empty());
        assert!(delta.color_changes().is_empty());
    }
}
