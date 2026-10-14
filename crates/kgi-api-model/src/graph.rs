use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use kgi_model::block::{BlockColor, BlockCoordinate, BlockHash, Timestamp};
use thiserror::Error;

/// Complete retained level depth of the canonical Head graph.
pub const MAX_CACHE_DEPTH: u64 = 1_000;

/// Failure to construct a canonical inclusive level extent.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LevelExtentError {
    /// The supplied bounds are zero or not ordered from low to high.
    #[error("invalid level extent bounds {low_level}..={high_level}")]
    InvalidBounds { low_level: u64, high_level: u64 },
}

/// One validated inclusive nominal graph extent.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LevelExtent {
    low_level: u64,
    high_level: u64,
}

impl LevelExtent {
    /// Constructs an inclusive extent from positive ordered bounds.
    pub const fn new(low_level: u64, high_level: u64) -> Result<Self, LevelExtentError> {
        if low_level == 0 || low_level > high_level {
            return Err(LevelExtentError::InvalidBounds { low_level, high_level });
        }
        Ok(Self { low_level, high_level })
    }

    /// Returns the inclusive lower bound.
    #[must_use]
    pub const fn low_level(self) -> u64 {
        self.low_level
    }

    /// Returns the inclusive upper bound.
    #[must_use]
    pub const fn high_level(self) -> u64 {
        self.high_level
    }

    /// Returns the number of levels in the inclusive extent.
    #[must_use]
    pub const fn depth(self) -> u64 {
        self.high_level - self.low_level + 1
    }

    /// Returns whether the extent contains `level`, including its boundaries.
    #[must_use]
    pub const fn contains_level(self, level: u64) -> bool {
        self.low_level <= level && level <= self.high_level
    }

    /// Returns whether this extent fully contains `other`.
    #[must_use]
    pub const fn contains_extent(self, other: Self) -> bool {
        self.low_level <= other.low_level && other.high_level <= self.high_level
    }

    /// Returns whether this extent and `other` share at least one level.
    #[must_use]
    pub const fn overlaps(self, other: Self) -> bool {
        self.low_level <= other.high_level && other.low_level <= self.high_level
    }

    /// Returns whether a child-parent edge span intersects this extent.
    #[must_use]
    pub const fn intersects_span(self, parent_level: u64, child_level: u64) -> bool {
        child_level >= self.low_level && parent_level <= self.high_level
    }
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StoredLevel {
    value: Level,
    usage_count: u64,
}

/// Failure to extract a frozen subview from a graph view.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphViewExtractError {
    /// The requested nominal extent is not contained in the source extent.
    #[error("graph subview extent lies outside the source extent")]
    OutsideSourceExtent,
}

/// One complete canonical graph projection and its revision lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphView {
    max_depth: u64,
    extent: LevelExtent,
    current_revision_id: u64,
    current_revision_timestamp_us: u64,
    tracking_policy: TrackingPolicy,
    levels: BTreeMap<u64, StoredLevel>,
    blocks: HashMap<BlockHash, GraphBlock>,
    edges: HashMap<EdgeId, GraphEdge>,
}

impl GraphView {
    /// Constructs a revision-zero view from one coherent database projection.
    ///
    /// Derived endpoint usage is rebuilt from `edges`; it is never accepted as
    /// seed input.
    ///
    /// # Panics
    ///
    /// Panics when `max_depth` is zero or shallower than `extent`, when the
    /// certified projection omits a parent endpoint level, or when one parent
    /// level is referenced by more retained edges than fit in `u64`.
    #[must_use]
    pub fn from_database_projection(
        max_depth: u64,
        extent: LevelExtent,
        tracking_policy: TrackingPolicy,
        levels: BTreeMap<u64, Level>,
        blocks: HashMap<BlockHash, GraphBlock>,
        edges: HashMap<EdgeId, GraphEdge>,
    ) -> Self {
        assert!(max_depth > 0, "a graph view has positive maximum depth");
        assert!(extent.depth() <= max_depth, "a graph view extent fits its maximum depth");

        let mut levels: BTreeMap<_, _> =
            levels.into_iter().map(|(level, value)| (level, StoredLevel { value, usage_count: 0 })).collect();
        for edge in edges.values() {
            increment_parent_usage(&mut levels, edge.parent_coordinate.level());
        }

        Self { max_depth, extent, current_revision_id: 0, current_revision_timestamp_us: 0, tracking_policy, levels, blocks, edges }
    }

    /// Returns the configured maximum nominal depth.
    #[must_use]
    pub const fn max_depth(&self) -> u64 {
        self.max_depth
    }

    /// Returns the nominal level extent.
    #[must_use]
    pub const fn extent(&self) -> LevelExtent {
        self.extent
    }

    /// Returns the nominal lower level.
    #[must_use]
    pub const fn low_level(&self) -> u64 {
        self.extent.low_level()
    }

    /// Returns the nominal upper level.
    #[must_use]
    pub const fn high_level(&self) -> u64 {
        self.extent.high_level()
    }

    /// Returns the current graph revision.
    #[must_use]
    pub const fn current_revision_id(&self) -> u64 {
        self.current_revision_id
    }

    /// Returns the publication-local timestamp of the current revision.
    #[must_use]
    pub const fn current_revision_timestamp_us(&self) -> u64 {
        self.current_revision_timestamp_us
    }

    /// Returns the view's mutation policy.
    #[must_use]
    pub const fn tracking_policy(&self) -> TrackingPolicy {
        self.tracking_policy
    }

    /// Returns one retained level value.
    #[must_use]
    pub fn level(&self, level: u64) -> Option<&Level> {
        self.levels.get(&level).map(|stored| &stored.value)
    }

    /// Iterates retained level values in ascending level order.
    pub fn levels(&self) -> impl Iterator<Item = (u64, &Level)> {
        self.levels.iter().map(|(&level, stored)| (level, &stored.value))
    }

    /// Returns one retained block value.
    #[must_use]
    pub fn block(&self, hash: &BlockHash) -> Option<&GraphBlock> {
        self.blocks.get(hash)
    }

    /// Iterates retained blocks in unspecified order.
    pub fn blocks(&self) -> impl Iterator<Item = &GraphBlock> {
        self.blocks.values()
    }

    /// Returns one retained edge value.
    #[must_use]
    pub fn edge(&self, id: &EdgeId) -> Option<&GraphEdge> {
        self.edges.get(id)
    }

    /// Iterates retained edges in unspecified order.
    pub fn edges(&self) -> impl Iterator<Item = &GraphEdge> {
        self.edges.values()
    }

    /// Extracts a coherent frozen view over a contained nominal extent.
    pub fn extract_subview(&self, extent: LevelExtent) -> Result<Self, GraphViewExtractError> {
        if !self.extent.contains_extent(extent) {
            return Err(GraphViewExtractError::OutsideSourceExtent);
        }

        let blocks = self
            .blocks
            .iter()
            .filter(|(_, block)| extent.contains_level(block.coordinate.level()))
            .map(|(&hash, block)| (hash, block.clone()))
            .collect();
        let edges: HashMap<_, _> = self
            .edges
            .iter()
            .filter(|(_, edge)| extent.intersects_span(edge.parent_coordinate.level(), edge.child_coordinate.level()))
            .map(|(&id, &edge)| (id, edge))
            .collect();

        let mut selected_levels = BTreeMap::new();
        for level in extent.low_level()..=extent.high_level() {
            insert_source_level(&mut selected_levels, &self.levels, level);
        }
        for edge in edges.values() {
            insert_source_level(&mut selected_levels, &self.levels, edge.parent_coordinate.level());
            insert_source_level(&mut selected_levels, &self.levels, edge.child_coordinate.level());
        }
        for edge in edges.values() {
            increment_parent_usage(&mut selected_levels, edge.parent_coordinate.level());
        }

        Ok(Self {
            max_depth: extent.depth(),
            extent,
            current_revision_id: self.current_revision_id,
            current_revision_timestamp_us: self.current_revision_timestamp_us,
            tracking_policy: TrackingPolicy::Frozen,
            levels: selected_levels,
            blocks,
            edges,
        })
    }
}

fn insert_source_level(target: &mut BTreeMap<u64, StoredLevel>, source: &BTreeMap<u64, StoredLevel>, level: u64) {
    target.entry(level).or_insert_with(|| StoredLevel {
        value: source.get(&level).expect("a coherent graph contains every nominal and edge endpoint level").value,
        usage_count: 0,
    });
}

fn increment_parent_usage(levels: &mut BTreeMap<u64, StoredLevel>, parent_level: u64) {
    let stored = levels.get_mut(&parent_level).expect("a coherent graph contains every edge parent level");
    stored.usage_count = stored.usage_count.checked_add(1).expect("retained edge usage fits u64");
}

#[cfg(test)]
mod tests {
    use kgi_model::block::MAX_DAA_SCORE;

    use super::*;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn coordinate(level: u64, slot: u64) -> BlockCoordinate {
        BlockCoordinate::new(level, slot).expect("positive level")
    }

    fn extent(low_level: u64, high_level: u64) -> LevelExtent {
        LevelExtent::new(low_level, high_level).expect("valid extent")
    }

    fn graph_block(byte: u8, level: u64) -> GraphBlock {
        GraphBlock {
            hash: hash(byte),
            coordinate: coordinate(level, 0),
            timestamp: u64::from(byte),
            daa_score: u64::from(byte),
            selected_parent_index: None,
            direct_parents: Arc::from([]),
            blue_merge_set: Arc::from([]),
            red_merge_set: Arc::from([]),
            color: BlockColor::Gray,
            is_in_vspc: false,
        }
    }

    fn graph_edge(parent_byte: u8, parent_level: u64, child_byte: u8, child_level: u64) -> GraphEdge {
        GraphEdge {
            id: EdgeId { parent: hash(parent_byte), child: hash(child_byte) },
            parent_coordinate: coordinate(parent_level, 0),
            child_coordinate: coordinate(child_level, 0),
        }
    }

    fn view(policy: TrackingPolicy) -> GraphView {
        let levels = (1..=7).map(|level| (level, Level { size: level, daa_score: Some(level * 10) })).collect();
        let blocks = (2..=6).map(|level| (hash(level as u8), graph_block(level as u8, level))).collect();
        let edge_values = [
            graph_edge(1, 1, 2, 2),
            graph_edge(2, 2, 6, 6),
            graph_edge(3, 3, 4, 4),
            graph_edge(4, 4, 5, 5),
            graph_edge(4, 4, 6, 6),
            graph_edge(6, 6, 7, 7),
        ];
        let edges = edge_values.into_iter().map(|edge| (edge.id, edge)).collect();

        GraphView::from_database_projection(5, extent(2, 6), policy, levels, blocks, edges)
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

    #[test]
    fn level_extent_validates_bounds_and_computes_inclusive_depth_without_overflow() {
        let single = extent(7, 7);
        assert_eq!(single.low_level(), 7);
        assert_eq!(single.high_level(), 7);
        assert_eq!(single.depth(), 1);

        let complete = extent(1, u64::MAX);
        assert_eq!(complete.depth(), u64::MAX);

        assert_eq!(LevelExtent::new(0, 0), Err(LevelExtentError::InvalidBounds { low_level: 0, high_level: 0 }));
        assert_eq!(LevelExtent::new(0, 7), Err(LevelExtentError::InvalidBounds { low_level: 0, high_level: 7 }));
        assert_eq!(LevelExtent::new(8, 7), Err(LevelExtentError::InvalidBounds { low_level: 8, high_level: 7 }));
    }

    #[test]
    fn level_extent_operations_include_boundaries() {
        let outer = extent(3, 7);

        assert!(outer.contains_level(3));
        assert!(outer.contains_level(7));
        assert!(!outer.contains_level(2));
        assert!(!outer.contains_level(8));

        assert!(outer.contains_extent(extent(3, 7)));
        assert!(outer.contains_extent(extent(4, 6)));
        assert!(!outer.contains_extent(extent(2, 6)));
        assert!(!outer.contains_extent(extent(4, 8)));

        assert!(outer.overlaps(extent(1, 3)));
        assert!(outer.overlaps(extent(7, 9)));
        assert!(!outer.overlaps(extent(1, 2)));
        assert!(!outer.overlaps(extent(8, 9)));

        assert!(outer.intersects_span(2, 3));
        assert!(outer.intersects_span(7, 8));
        assert!(outer.intersects_span(2, 8));
        assert!(!outer.intersects_span(1, 2));
        assert!(!outer.intersects_span(8, 9));
    }

    #[test]
    fn database_projection_starts_at_revision_zero_and_derives_parent_usage() {
        let view = view(TrackingPolicy::Head);

        assert_eq!(view.max_depth(), 5);
        assert_eq!(view.extent(), extent(2, 6));
        assert_eq!(view.low_level(), 2);
        assert_eq!(view.high_level(), 6);
        assert_eq!(view.current_revision_id(), 0);
        assert_eq!(view.current_revision_timestamp_us(), 0);
        assert_eq!(view.tracking_policy(), TrackingPolicy::Head);
        assert_eq!(view.levels.get(&2).map(|level| level.usage_count), Some(1));
        assert_eq!(view.levels.get(&4).map(|level| level.usage_count), Some(2));
        assert_eq!(view.levels.get(&7).map(|level| level.usage_count), Some(0));
    }

    #[test]
    fn database_projection_rejects_invalid_maximum_depth() {
        let projection = view(TrackingPolicy::Head);

        let zero_depth = std::panic::catch_unwind(|| {
            GraphView::from_database_projection(
                0,
                extent(2, 6),
                TrackingPolicy::Head,
                projection.levels().map(|(level, value)| (level, *value)).collect(),
                projection.blocks.iter().map(|(&hash, block)| (hash, block.clone())).collect(),
                projection.edges.iter().map(|(&id, &edge)| (id, edge)).collect(),
            )
        });
        assert!(zero_depth.is_err());

        let shallow_capacity = std::panic::catch_unwind(|| {
            GraphView::from_database_projection(
                4,
                extent(2, 6),
                TrackingPolicy::Head,
                projection.levels().map(|(level, value)| (level, *value)).collect(),
                projection.blocks.iter().map(|(&hash, block)| (hash, block.clone())).collect(),
                projection.edges.iter().map(|(&id, &edge)| (id, edge)).collect(),
            )
        });
        assert!(shallow_capacity.is_err());
    }

    #[test]
    fn extraction_rejects_uncontained_nominal_extents() {
        let view = view(TrackingPolicy::Head);

        assert_eq!(view.extract_subview(extent(1, 4)), Err(GraphViewExtractError::OutsideSourceExtent));
        assert_eq!(view.extract_subview(extent(4, 7)), Err(GraphViewExtractError::OutsideSourceExtent));
    }

    #[test]
    fn extraction_selects_crossing_edges_and_recomputes_endpoint_usage() {
        let mut source = view(TrackingPolicy::Head);
        source.current_revision_id = 17;
        source.current_revision_timestamp_us = 91;
        let before = source.clone();

        let extracted = source.extract_subview(extent(4, 4)).expect("contained extent");

        assert_eq!(source, before);
        assert_eq!(extracted.max_depth(), 1);
        assert_eq!(extracted.low_level(), 4);
        assert_eq!(extracted.high_level(), 4);
        assert_eq!(extracted.current_revision_id(), 17);
        assert_eq!(extracted.current_revision_timestamp_us(), 91);
        assert_eq!(extracted.tracking_policy(), TrackingPolicy::Frozen);
        assert_eq!(extracted.blocks().map(|block| block.hash).collect::<Vec<_>>(), vec![hash(4)]);

        let expected_edges = [
            EdgeId { parent: hash(2), child: hash(6) },
            EdgeId { parent: hash(3), child: hash(4) },
            EdgeId { parent: hash(4), child: hash(5) },
            EdgeId { parent: hash(4), child: hash(6) },
        ];
        assert_eq!(extracted.edges.len(), expected_edges.len());
        for edge in expected_edges {
            assert!(extracted.edge(&edge).is_some());
        }
        assert_eq!(extracted.levels().map(|(level, _)| level).collect::<Vec<_>>(), vec![2, 3, 4, 5, 6]);
        assert_eq!(extracted.level(4), Some(&Level { size: 4, daa_score: Some(40) }));
        assert_eq!(extracted.levels.get(&2).map(|level| level.usage_count), Some(1));
        assert_eq!(extracted.levels.get(&3).map(|level| level.usage_count), Some(1));
        assert_eq!(extracted.levels.get(&4).map(|level| level.usage_count), Some(2));
        assert_eq!(extracted.levels.get(&5).map(|level| level.usage_count), Some(0));
        assert_eq!(extracted.levels.get(&6).map(|level| level.usage_count), Some(0));

        let two_endpoint_extract = source.extract_subview(extent(4, 5)).expect("contained extent");
        let contained_edge = EdgeId { parent: hash(4), child: hash(5) };
        assert!(two_endpoint_extract.block(&contained_edge.parent).is_some());
        assert!(two_endpoint_extract.block(&contained_edge.child).is_some());
        assert!(two_endpoint_extract.edge(&contained_edge).is_some());
    }

    #[test]
    fn extraction_works_from_every_policy_and_can_be_nested() {
        for policy in [TrackingPolicy::Head, TrackingPolicy::Fixed, TrackingPolicy::Frozen] {
            let source = view(policy);
            let first = source.extract_subview(extent(3, 5)).expect("first contained extent");
            let nested = first.extract_subview(extent(4, 4)).expect("nested contained extent");

            assert_eq!(first.tracking_policy(), TrackingPolicy::Frozen);
            assert_eq!(nested.tracking_policy(), TrackingPolicy::Frozen);
            assert_eq!(nested.blocks().map(|block| block.hash).collect::<Vec<_>>(), vec![hash(4)]);
        }
    }
}
