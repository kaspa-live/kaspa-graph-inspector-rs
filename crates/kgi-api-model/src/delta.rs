use std::collections::{BTreeMap, HashMap};

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

    /// A delta lineage cannot have the reserved zero high level.
    #[error("graph delta high level must be positive")]
    ZeroHighLevel,
}

/// Failure to compose two canonical graph-delta intervals.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphDeltaCompositionError {
    /// The right interval does not begin at the left interval's target.
    #[error("graph delta intervals are not gapless: left target {left_to_revision_id}, right source {right_from_revision_id}")]
    RevisionMismatch { left_to_revision_id: u64, right_from_revision_id: u64 },
}

/// One canonical graph-state transition over a forward revision interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphDelta {
    from_revision_id: u64,
    to_revision_id: u64,
    revision_timestamp_ms: u64,
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
        revision_timestamp_ms: u64,
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
        if high_level == 0 {
            return Err(GraphDeltaCreationError::ZeroHighLevel);
        }

        Ok(Self {
            from_revision_id,
            to_revision_id,
            revision_timestamp_ms,
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

    /// Returns the target revision's whole publication-local milliseconds.
    #[must_use]
    pub const fn revision_timestamp_ms(&self) -> u64 {
        self.revision_timestamp_ms
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

    /// Composes this interval with the immediately following interval.
    pub fn compose(&self, next: &Self) -> Result<Self, GraphDeltaCompositionError> {
        if self.to_revision_id != next.from_revision_id {
            return Err(GraphDeltaCompositionError::RevisionMismatch {
                left_to_revision_id: self.to_revision_id,
                right_from_revision_id: next.from_revision_id,
            });
        }

        let mut block_changes = self.block_changes.clone();
        block_changes.extend(next.block_changes.clone());

        let mut edge_changes = self.edge_changes.clone();
        edge_changes.extend(next.edge_changes.clone());

        let mut level_changes: BTreeMap<_, _> =
            self.level_changes.iter().map(|change| (change.level, (change.before, change.after))).collect();
        for change in &next.level_changes {
            level_changes.entry(change.level).and_modify(|(_, after)| *after = change.after).or_insert((change.before, change.after));
        }
        let level_changes = level_changes
            .into_iter()
            .filter_map(|(level, (before, after))| (before != after).then_some(LevelChange { level, before, after }))
            .collect();

        let is_in_vspc_changes = compose_field_changes(&self.is_in_vspc_changes, &next.is_in_vspc_changes);
        let color_changes = compose_field_changes(&self.color_changes, &next.color_changes);

        Ok(Self::new(
            self.from_revision_id,
            next.to_revision_id,
            next.revision_timestamp_ms,
            next.high_level,
            block_changes,
            edge_changes,
            level_changes,
            is_in_vspc_changes,
            color_changes,
        )
        .expect("gapless valid deltas compose into a valid forward interval"))
    }
}

fn compose_field_changes<T: Copy + Eq>(
    left: &HashMap<BlockHash, FieldChange<T>>,
    right: &HashMap<BlockHash, FieldChange<T>>,
) -> HashMap<BlockHash, FieldChange<T>> {
    let mut composed = left.clone();
    for (&hash, &change) in right {
        composed.entry(hash).and_modify(|current| current.after = change.after).or_insert(change);
    }
    composed.retain(|_, change| change.before != change.after);
    composed
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

    fn edge(parent: u8, child: u8, parent_level: u64, child_level: u64) -> GraphEdge {
        let id = EdgeId { parent: hash(parent), child: hash(child) };
        GraphEdge {
            id,
            parent_coordinate: BlockCoordinate::new(parent_level, 0).expect("positive level"),
            child_coordinate: BlockCoordinate::new(child_level, 0).expect("positive level"),
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
    fn constructor_rejects_zero_high_level_after_revision_ordering() {
        let empty = || (HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new());
        let (blocks, edges, levels, membership, colors) = empty();
        assert_eq!(
            GraphDelta::new(8, 9, 77, 0, blocks, edges, levels, membership, colors),
            Err(GraphDeltaCreationError::ZeroHighLevel)
        );

        let (blocks, edges, levels, membership, colors) = empty();
        assert_eq!(
            GraphDelta::new(8, 8, 77, 0, blocks, edges, levels, membership, colors),
            Err(GraphDeltaCreationError::NonForwardRevision { from_revision_id: 8, to_revision_id: 8 })
        );
    }

    #[test]
    fn constructor_preserves_the_complete_checked_transition() {
        let delta = delta(8, 9).expect("forward revision");
        let block_hash = hash(1);
        let edge_id = EdgeId { parent: hash(2), child: block_hash };

        assert_eq!(delta.from_revision_id(), 8);
        assert_eq!(delta.to_revision_id(), 9);
        assert_eq!(delta.revision_timestamp_ms(), 77);
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
        assert_eq!(delta.revision_timestamp_ms(), 88);
        assert_eq!(delta.high_level(), 12);
        assert!(delta.block_changes().is_empty());
        assert!(delta.edge_changes().is_empty());
        assert!(delta.level_changes().is_empty());
        assert!(delta.is_in_vspc_changes().is_empty());
        assert!(delta.color_changes().is_empty());
    }

    #[test]
    fn composition_requires_exactly_gapless_intervals() {
        let left = delta(1, 2).expect("forward delta");
        let right = delta(3, 4).expect("forward delta");

        assert_eq!(
            left.compose(&right),
            Err(GraphDeltaCompositionError::RevisionMismatch { left_to_revision_id: 2, right_from_revision_id: 3 })
        );
    }

    #[test]
    fn composition_is_right_biased_for_absolute_maps_and_folds_relative_changes() {
        let mut left_block_1 = block(hash(1));
        left_block_1.timestamp = 11;
        let mut right_block_1 = block(hash(1));
        right_block_1.timestamp = 12;
        let left_edge_1 = edge(11, 1, 3, 4);
        let right_edge_1 = edge(11, 1, 2, 4);
        let edge_2 = edge(12, 2, 3, 4);
        let edge_3 = edge(13, 3, 3, 4);
        let edge_4 = edge(14, 4, 3, 4);

        let left = GraphDelta::new(
            5,
            6,
            60,
            4,
            HashMap::from([(hash(1), Some(left_block_1)), (hash(2), Some(block(hash(2)))), (hash(3), None), (hash(4), None)]),
            HashMap::from([(left_edge_1.id, Some(left_edge_1)), (edge_2.id, Some(edge_2)), (edge_3.id, None), (edge_4.id, None)]),
            vec![
                LevelChange { level: 1, before: None, after: Some(Level { size: 1, daa_score: None }) },
                LevelChange { level: 2, before: Some(Level { size: 2, daa_score: Some(20) }), after: None },
                LevelChange {
                    level: 3,
                    before: Some(Level { size: 3, daa_score: Some(30) }),
                    after: Some(Level { size: 4, daa_score: Some(30) }),
                },
            ],
            HashMap::from([
                (hash(1), FieldChange { before: false, after: true }),
                (hash(2), FieldChange { before: false, after: true }),
                (hash(4), FieldChange { before: false, after: true }),
            ]),
            HashMap::from([(hash(1), FieldChange { before: BlockColor::Gray, after: BlockColor::Blue })]),
        )
        .expect("forward delta");
        let right = GraphDelta::new(
            6,
            9,
            90,
            7,
            HashMap::from([(hash(1), Some(right_block_1.clone())), (hash(2), None), (hash(3), Some(block(hash(3)))), (hash(4), None)]),
            HashMap::from([(right_edge_1.id, Some(right_edge_1)), (edge_2.id, None), (edge_3.id, Some(edge_3)), (edge_4.id, None)]),
            vec![
                LevelChange {
                    level: 1,
                    before: Some(Level { size: 999, daa_score: Some(999) }),
                    after: Some(Level { size: 2, daa_score: Some(10) }),
                },
                LevelChange { level: 2, before: None, after: Some(Level { size: 2, daa_score: Some(20) }) },
                LevelChange { level: 4, before: None, after: Some(Level { size: 1, daa_score: Some(40) }) },
            ],
            HashMap::from([
                (hash(1), FieldChange { before: true, after: false }),
                (hash(3), FieldChange { before: false, after: true }),
                (hash(4), FieldChange { before: false, after: true }),
            ]),
            HashMap::from([
                (hash(1), FieldChange { before: BlockColor::Blue, after: BlockColor::Red }),
                (hash(3), FieldChange { before: BlockColor::Gray, after: BlockColor::Blue }),
            ]),
        )
        .expect("forward delta");

        let composed = left.compose(&right).expect("gapless intervals");

        assert_eq!(composed.from_revision_id(), 5);
        assert_eq!(composed.to_revision_id(), 9);
        assert_eq!(composed.revision_timestamp_ms(), 90);
        assert_eq!(composed.high_level(), 7);
        assert_eq!(composed.block_changes().get(&hash(1)), Some(&Some(right_block_1)));
        assert_eq!(composed.block_changes().get(&hash(2)), Some(&None));
        assert_eq!(composed.block_changes().get(&hash(3)), Some(&Some(block(hash(3)))));
        assert_eq!(composed.block_changes().get(&hash(4)), Some(&None));
        assert_eq!(composed.edge_changes().get(&right_edge_1.id), Some(&Some(right_edge_1)));
        assert_eq!(composed.edge_changes().get(&edge_2.id), Some(&None));
        assert_eq!(composed.edge_changes().get(&edge_3.id), Some(&Some(edge_3)));
        assert_eq!(composed.edge_changes().get(&edge_4.id), Some(&None));
        assert_eq!(
            composed.level_changes(),
            &[
                LevelChange { level: 1, before: None, after: Some(Level { size: 2, daa_score: Some(10) }) },
                LevelChange {
                    level: 3,
                    before: Some(Level { size: 3, daa_score: Some(30) }),
                    after: Some(Level { size: 4, daa_score: Some(30) }),
                },
                LevelChange { level: 4, before: None, after: Some(Level { size: 1, daa_score: Some(40) }) },
            ]
        );
        assert!(!composed.is_in_vspc_changes().contains_key(&hash(1)));
        assert_eq!(composed.is_in_vspc_changes().get(&hash(2)), Some(&FieldChange { before: false, after: true }));
        assert_eq!(composed.is_in_vspc_changes().get(&hash(3)), Some(&FieldChange { before: false, after: true }));
        assert_eq!(composed.is_in_vspc_changes().get(&hash(4)), Some(&FieldChange { before: false, after: true }));
        assert_eq!(composed.color_changes().get(&hash(1)), Some(&FieldChange { before: BlockColor::Gray, after: BlockColor::Red }));
        assert_eq!(composed.color_changes().get(&hash(3)), Some(&FieldChange { before: BlockColor::Gray, after: BlockColor::Blue }));
    }

    #[test]
    fn empty_composed_mutation_still_retains_the_outer_revision_interval() {
        let left = GraphDelta::new(
            1,
            2,
            20,
            4,
            HashMap::new(),
            HashMap::new(),
            vec![LevelChange { level: 4, before: None, after: Some(Level { size: 1, daa_score: None }) }],
            HashMap::from([(hash(1), FieldChange { before: false, after: true })]),
            HashMap::new(),
        )
        .expect("forward delta");
        let right = GraphDelta::new(
            2,
            3,
            30,
            4,
            HashMap::new(),
            HashMap::new(),
            vec![LevelChange { level: 4, before: Some(Level { size: 1, daa_score: None }), after: None }],
            HashMap::from([(hash(1), FieldChange { before: true, after: false })]),
            HashMap::new(),
        )
        .expect("forward delta");

        let composed = left.compose(&right).expect("gapless intervals");

        assert_eq!(composed.from_revision_id(), 1);
        assert_eq!(composed.to_revision_id(), 3);
        assert_eq!(composed.revision_timestamp_ms(), 30);
        assert!(composed.block_changes().is_empty());
        assert!(composed.edge_changes().is_empty());
        assert!(composed.level_changes().is_empty());
        assert!(composed.is_in_vspc_changes().is_empty());
        assert!(composed.color_changes().is_empty());
    }

    #[test]
    fn composition_is_associative_across_three_gapless_intervals() {
        let make = |from, to, block_change, field_change| {
            GraphDelta::new(
                from,
                to,
                to * 10,
                to + 3,
                HashMap::from([(hash(1), block_change)]),
                HashMap::new(),
                Vec::new(),
                HashMap::from([(hash(1), field_change)]),
                HashMap::new(),
            )
            .expect("forward delta")
        };
        let first = make(1, 2, Some(block(hash(1))), FieldChange { before: false, after: true });
        let second = make(2, 3, None, FieldChange { before: true, after: false });
        let third = make(3, 4, Some(block(hash(1))), FieldChange { before: false, after: true });

        let left_grouped = first.compose(&second).expect("first gap").compose(&third).expect("second gap");
        let right_grouped = first.compose(&second.compose(&third).expect("second gap")).expect("first gap");

        assert_eq!(left_grouped, right_grouped);
    }
}
