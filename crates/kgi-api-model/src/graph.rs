use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    sync::Arc,
};

use kgi_model::{
    block::{BlockColor, BlockCoordinate, BlockHash, Timestamp},
    graph_update::{BlockCommitted, LevelCommitted, VspcCommitted},
};
use thiserror::Error;

use crate::delta::{FieldChange, GraphDelta, LevelChange};

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExtractionCandidate {
    Block,
    Edge,
}

/// Failure to extract a frozen subview from a graph view.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphViewExtractError {
    /// The requested nominal extent is not contained in the source extent.
    #[error("graph subview extent lies outside the source extent")]
    OutsideSourceExtent,
}

/// Failure to mutate a canonical graph view.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphViewUpdateError {
    /// Frozen views refuse every mutation.
    #[error("frozen graph views cannot be mutated")]
    Frozen,
}

/// Failure to apply a canonical delta to a graph view.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphDeltaApplyError {
    /// Frozen views refuse every mutation.
    #[error("frozen graph views cannot be mutated")]
    Frozen,

    /// The delta does not begin at the view's current revision.
    #[error("graph view revision {view_revision} does not match delta source {delta_from}")]
    RevisionMismatch { view_revision: u64, delta_from: u64 },
}

/// One complete canonical graph projection and its revision lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphView {
    max_depth: u64,
    extent: LevelExtent,
    current_revision_id: u64,
    current_revision_timestamp_ms: u64,
    tracking_policy: TrackingPolicy,
    levels: BTreeMap<u64, StoredLevel>,
    blocks: HashMap<BlockHash, GraphBlock>,
    edges: HashMap<EdgeId, GraphEdge>,
    block_hashes_by_level: BTreeMap<u64, HashSet<BlockHash>>,
    edge_parents_by_child: HashMap<BlockHash, HashSet<BlockHash>>,
    // Child level is the outer key and parent level the inner key, allowing
    // extraction to apply both span bounds before visiting edge IDs.
    edge_ids_by_span: BTreeMap<u64, BTreeMap<u64, HashSet<EdgeId>>>,
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

        let levels: BTreeMap<_, _> = levels.into_iter().map(|(level, value)| (level, StoredLevel { value, usage_count: 0 })).collect();
        let block_hashes_by_level = index_blocks_by_level(&blocks);
        let edge_parents_by_child = index_edge_parents_by_child(&edges);
        let edge_ids_by_span = index_edges_by_span(&edges);

        let mut view = Self {
            max_depth,
            extent,
            current_revision_id: 0,
            current_revision_timestamp_ms: 0,
            tracking_policy,
            levels,
            blocks,
            edges,
            block_hashes_by_level,
            edge_parents_by_child,
            edge_ids_by_span,
        };
        let parent_levels: Vec<_> = view.edges.values().map(|edge| edge.parent_coordinate.level()).collect();
        for parent_level in parent_levels {
            view.increment_parent_usage(parent_level);
        }
        view
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

    /// Returns the current revision's whole publication-local milliseconds.
    #[must_use]
    pub const fn current_revision_timestamp_ms(&self) -> u64 {
        self.current_revision_timestamp_ms
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

    /// Applies one canonical delta from this view's current revision.
    ///
    /// A Fixed view freezes on a revision mismatch. Frozen views refuse before
    /// inspecting the supplied delta.
    pub fn apply_delta(&mut self, delta: &GraphDelta) -> Result<(), GraphDeltaApplyError> {
        if self.tracking_policy == TrackingPolicy::Frozen {
            return Err(GraphDeltaApplyError::Frozen);
        }
        if self.current_revision_id != delta.from_revision_id() {
            let error = GraphDeltaApplyError::RevisionMismatch {
                view_revision: self.current_revision_id,
                delta_from: delta.from_revision_id(),
            };
            if self.tracking_policy == TrackingPolicy::Fixed {
                self.tracking_policy = TrackingPolicy::Frozen;
            }
            return Err(error);
        }

        for (&hash, change) in delta.block_changes() {
            match change {
                Some(block) => self.apply_block_upsert(hash, block.clone()),
                None => self.apply_block_removal(hash),
            }
        }

        let mut usage_decrements = Vec::new();
        let mut usage_increments = Vec::new();
        for (&id, change) in delta.edge_changes() {
            let previous = match change {
                Some(edge) => self.edges.insert(id, *edge),
                None => self.edges.remove(&id),
            };

            if previous.as_ref() == change.as_ref() {
                continue;
            }
            if let Some(previous) = previous {
                usage_decrements.push(previous.parent_coordinate.level());
                self.unindex_edge(previous);
            }
            if let Some(edge) = change {
                usage_increments.push(edge.parent_coordinate.level());
                self.index_edge(*edge);
            }
        }

        for change in delta.level_changes() {
            match change.after {
                Some(value) => {
                    self.levels
                        .entry(change.level)
                        .and_modify(|stored| stored.value = value)
                        .or_insert(StoredLevel { value, usage_count: 0 });
                }
                None => {
                    self.levels.remove(&change.level);
                }
            }
        }

        for level in usage_decrements {
            if let Some(stored) = self.levels.get_mut(&level) {
                stored.usage_count = stored.usage_count.checked_sub(1).expect("canonical delta edge removal has retained usage");
            }
        }
        for level in usage_increments {
            let stored = self.levels.get_mut(&level).expect("canonical delta edge addition has a retained parent level");
            stored.usage_count = stored.usage_count.checked_add(1).expect("retained edge usage fits u64");
        }

        for (&hash, change) in delta.is_in_vspc_changes() {
            if let Some(block) = self.blocks.get_mut(&hash) {
                block.is_in_vspc = change.after;
            }
        }
        for (&hash, change) in delta.color_changes() {
            if let Some(block) = self.blocks.get_mut(&hash) {
                block.color = change.after;
            }
        }

        if self.tracking_policy == TrackingPolicy::Head {
            let target_low = delta.high_level().saturating_sub(self.max_depth - 1).max(1);
            self.extent = LevelExtent::new(target_low, delta.high_level()).expect("delta lineage high level is positive");
        }
        self.current_revision_id = delta.to_revision_id();
        self.current_revision_timestamp_ms = delta.revision_timestamp_ms();
        Ok(())
    }

    /// Applies one committed block to a head-tracking view.
    ///
    /// Returns the exact visible delta, or `None` when the committed value has
    /// no retained effect. A visible mutation advances the view by one
    /// revision using `revision_timestamp_ms`.
    ///
    /// # Panics
    ///
    /// Panics when called on a Fixed view or when the committed payload
    /// violates Storage's coherent projection invariants.
    pub fn apply_head_block_committed(
        &mut self,
        committed: &BlockCommitted,
        revision_timestamp_ms: u64,
    ) -> Result<Option<GraphDelta>, GraphViewUpdateError> {
        if self.tracking_policy == TrackingPolicy::Frozen {
            return Err(GraphViewUpdateError::Frozen);
        }
        assert_eq!(self.tracking_policy, TrackingPolicy::Head, "head mutation requires a Head graph view");

        let target_high = self.high_level().max(committed.coordinate.level());
        let target_low = target_high.saturating_sub(self.max_depth - 1).max(1);
        let target_extent = LevelExtent::new(target_low, target_high).expect("derived Head extent is positive and ordered");
        let mut mutation = GraphMutation::default();

        self.apply_block_projection(committed, target_extent, &mut mutation);

        let pruned_block_levels: Vec<_> =
            self.block_hashes_by_level.range(..target_extent.low_level()).map(|(&level, _)| level).collect();
        let mut pruned_blocks = Vec::new();
        for level in pruned_block_levels {
            pruned_blocks.extend(self.block_hashes_by_level.remove(&level).expect("a selected block level exists"));
        }
        for &hash in &pruned_blocks {
            let block = self.blocks.remove(&hash).expect("an indexed block exists");
            mutation.capture_removed_block(hash, block);
        }

        let mut pruned_edges = Vec::new();
        for child in pruned_blocks {
            if let Some(parents) = self.edge_parents_by_child.get(&child) {
                pruned_edges.extend(parents.iter().copied().map(|parent| EdgeId { parent, child }));
            }
        }
        for id in pruned_edges {
            let edge = self.edges.remove(&id).expect("a selected retained edge exists");
            mutation.capture_removed_edge(id, edge);
            self.unindex_edge(edge);
            self.decrement_parent_usage(edge.parent_coordinate.level());
        }

        let pruned_levels: Vec<_> = self
            .levels
            .range(..target_extent.low_level())
            .filter(|(_, stored)| stored.usage_count == 0)
            .map(|(&level, _)| level)
            .collect();
        for level in pruned_levels {
            let stored = self.levels.remove(&level).expect("a selected retained level exists");
            mutation.capture_removed_level(level, stored.value);
        }

        self.extent = target_extent;
        Ok(self.finish_mutation(mutation, revision_timestamp_ms))
    }

    /// Applies one committed VSPC transition to a head-tracking view.
    ///
    /// Absent block targets and absent levels are ignored. A visible projected
    /// mutation advances the view by one revision.
    ///
    /// # Panics
    ///
    /// Panics when called on a Fixed view.
    pub fn apply_head_vspc_committed(
        &mut self,
        committed: &VspcCommitted,
        revision_timestamp_ms: u64,
    ) -> Result<Option<GraphDelta>, GraphViewUpdateError> {
        if self.tracking_policy == TrackingPolicy::Frozen {
            return Err(GraphViewUpdateError::Frozen);
        }
        assert_eq!(self.tracking_policy, TrackingPolicy::Head, "head mutation requires a Head graph view");

        let added_metadata: Vec<_> =
            committed.added.iter().filter_map(|hash| self.blocks.get(hash)).map(VspcProjectionMetadata::from).collect();
        Ok(self.apply_vspc_projection(committed, &added_metadata, revision_timestamp_ms))
    }

    /// Applies one original committed block to a fixed view using post-update Head context.
    ///
    /// A disjoint post-update Head extent freezes this view without applying
    /// the value or advancing its revision.
    ///
    /// # Panics
    ///
    /// Panics unless this is a Fixed view and `head` is a Head view, or when
    /// the committed payload violates Storage's coherent projection invariants.
    pub fn apply_fixed_block_committed(
        &mut self,
        committed: &BlockCommitted,
        head: &GraphView,
        revision_timestamp_ms: u64,
    ) -> Result<Option<GraphDelta>, GraphViewUpdateError> {
        if self.tracking_policy == TrackingPolicy::Frozen {
            return Err(GraphViewUpdateError::Frozen);
        }
        assert_eq!(self.tracking_policy, TrackingPolicy::Fixed, "fixed mutation requires a Fixed graph view");
        assert_eq!(head.tracking_policy, TrackingPolicy::Head, "fixed mutation requires post-update Head context");

        if !self.extent.overlaps(head.extent) {
            self.tracking_policy = TrackingPolicy::Frozen;
            return Ok(None);
        }

        let mut mutation = GraphMutation::default();
        self.apply_block_projection(committed, self.extent, &mut mutation);
        Ok(self.finish_mutation(mutation, revision_timestamp_ms))
    }

    /// Applies one original committed VSPC transition to a fixed view.
    ///
    /// Immutable metadata is resolved from this view and the post-update Head
    /// view before mutation. Disjoint extents or missing required metadata
    /// freeze this view without changing its graph contents or revision.
    ///
    /// # Panics
    ///
    /// Panics unless this is a Fixed view and `head` is a Head view.
    pub fn apply_fixed_vspc_committed(
        &mut self,
        committed: &VspcCommitted,
        head: &GraphView,
        revision_timestamp_ms: u64,
    ) -> Result<Option<GraphDelta>, GraphViewUpdateError> {
        if self.tracking_policy == TrackingPolicy::Frozen {
            return Err(GraphViewUpdateError::Frozen);
        }
        assert_eq!(self.tracking_policy, TrackingPolicy::Fixed, "fixed mutation requires a Fixed graph view");
        assert_eq!(head.tracking_policy, TrackingPolicy::Head, "fixed mutation requires post-update Head context");

        if !self.extent.overlaps(head.extent) {
            self.tracking_policy = TrackingPolicy::Frozen;
            return Ok(None);
        }

        let added_metadata: Option<Vec<_>> = committed
            .added
            .iter()
            .map(|hash| self.blocks.get(hash).or_else(|| head.blocks.get(hash)).map(VspcProjectionMetadata::from))
            .collect();
        let Some(added_metadata) = added_metadata else {
            self.tracking_policy = TrackingPolicy::Frozen;
            return Ok(None);
        };

        Ok(self.apply_vspc_projection(committed, &added_metadata, revision_timestamp_ms))
    }

    fn apply_block_projection(&mut self, committed: &BlockCommitted, target_extent: LevelExtent, mutation: &mut GraphMutation) {
        let graph_block = graph_block_from_committed(committed);
        if target_extent.contains_level(graph_block.coordinate.level()) {
            mutation.capture_block(&self.blocks, graph_block.hash);
            self.insert_block(graph_block);
        }

        let retained_edges: Vec<_> = committed
            .direct_parents
            .iter()
            .filter_map(|parent| {
                parent.coordinate.map(|parent_coordinate| GraphEdge {
                    id: EdgeId { parent: parent.hash, child: committed.hash },
                    parent_coordinate,
                    child_coordinate: committed.coordinate,
                })
            })
            .filter(|edge| target_extent.intersects_span(edge.parent_coordinate.level(), edge.child_coordinate.level()))
            .collect();
        let retained_endpoint_levels: HashSet<_> =
            retained_edges.iter().flat_map(|edge| [edge.parent_coordinate.level(), edge.child_coordinate.level()]).collect();

        for snapshot in committed.level_snapshots.iter() {
            let retained_external = self.levels.get(&snapshot.level).is_some_and(|level| level.usage_count > 0);
            if target_extent.contains_level(snapshot.level) || retained_external || retained_endpoint_levels.contains(&snapshot.level)
            {
                mutation.capture_level(&self.levels, snapshot.level);
                self.apply_materialization_snapshot(snapshot);
            }
        }

        for edge in retained_edges {
            mutation.capture_edge(&self.edges, edge.id);
            let inserted = match self.edges.entry(edge.id) {
                Entry::Vacant(entry) => {
                    entry.insert(edge);
                    true
                }
                Entry::Occupied(entry) => {
                    assert_eq!(entry.get(), &edge, "one edge identity has immutable endpoint coordinates");
                    false
                }
            };
            if inserted {
                self.index_edge(edge);
                self.increment_parent_usage(edge.parent_coordinate.level());
            }
        }
    }

    fn apply_vspc_projection(
        &mut self,
        committed: &VspcCommitted,
        added_metadata: &[VspcProjectionMetadata],
        revision_timestamp_ms: u64,
    ) -> Option<GraphDelta> {
        let mut mutation = GraphMutation::default();

        for hash in committed.removed.iter().copied() {
            if let Some(block) = self.blocks.get_mut(&hash) {
                mutation.capture_membership(hash, block.is_in_vspc);
                mutation.capture_color(hash, block.color);
                block.is_in_vspc = false;
                block.color = BlockColor::Gray;
            }
        }

        for hash in committed.added.iter().copied() {
            if let Some(block) = self.blocks.get_mut(&hash) {
                mutation.capture_membership(hash, block.is_in_vspc);
                block.is_in_vspc = true;
            }
        }

        for metadata in added_metadata {
            for hash in metadata.blue_merge_set.iter().copied() {
                if let Some(block) = self.blocks.get_mut(&hash) {
                    mutation.capture_color(hash, block.color);
                    block.color = BlockColor::Blue;
                }
            }
            for hash in metadata.red_merge_set.iter().copied() {
                if let Some(block) = self.blocks.get_mut(&hash) {
                    mutation.capture_color(hash, block.color);
                    block.color = BlockColor::Red;
                }
            }
        }

        for snapshot in committed.level_snapshots.iter() {
            if let Some(stored) = self.levels.get_mut(&snapshot.level) {
                mutation.capture_level_value(snapshot.level, stored.value);
                stored.value.daa_score = snapshot.daa_score;
            }
        }

        self.finish_mutation(mutation, revision_timestamp_ms)
    }

    fn finish_mutation(&mut self, mutation: GraphMutation, revision_timestamp_ms: u64) -> Option<GraphDelta> {
        let changes = mutation.finish(self);
        if changes.is_empty() {
            return None;
        }

        let next_revision = self.current_revision_id.checked_add(1).expect("graph revision fits u64");
        let delta = GraphDelta::new(
            self.current_revision_id,
            next_revision,
            revision_timestamp_ms,
            self.high_level(),
            changes.blocks,
            changes.edges,
            changes.levels,
            changes.is_in_vspc,
            changes.colors,
        )
        .expect("graph mutation constructs a forward delta with a positive high level");
        self.current_revision_id = next_revision;
        self.current_revision_timestamp_ms = revision_timestamp_ms;

        Some(delta)
    }

    fn apply_block_upsert(&mut self, hash: BlockHash, block: GraphBlock) {
        let level = block.coordinate.level();
        if let Some(previous) = self.blocks.insert(hash, block) {
            let previous_level = previous.coordinate.level();
            if previous_level != level {
                let remove_level_entry = self.block_hashes_by_level.get_mut(&previous_level).is_some_and(|hashes| {
                    hashes.remove(&hash);
                    hashes.is_empty()
                });
                if remove_level_entry {
                    self.block_hashes_by_level.remove(&previous_level);
                }
            }
        }
        self.block_hashes_by_level.entry(level).or_default().insert(hash);
    }

    fn apply_block_removal(&mut self, hash: BlockHash) {
        let Some(block) = self.blocks.remove(&hash) else {
            return;
        };
        let level = block.coordinate.level();
        let remove_level_entry = self.block_hashes_by_level.get_mut(&level).is_some_and(|hashes| {
            hashes.remove(&hash);
            hashes.is_empty()
        });
        if remove_level_entry {
            self.block_hashes_by_level.remove(&level);
        }
    }

    fn insert_block(&mut self, block: GraphBlock) {
        let hash = block.hash;
        let level = block.coordinate.level();
        match self.blocks.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(block);
            }
            Entry::Occupied(_) => {
                panic!("BlockCommitted is emitted only for a newly materialized block");
            }
        }
        self.block_hashes_by_level.entry(level).or_default().insert(hash);
    }

    fn insert_source_level(&self, target: &mut BTreeMap<u64, StoredLevel>, level: u64) {
        target.entry(level).or_insert_with(|| StoredLevel {
            value: self.levels.get(&level).expect("a coherent graph contains every nominal and edge endpoint level").value,
            usage_count: 0,
        });
    }

    fn apply_materialization_snapshot(&mut self, snapshot: &LevelCommitted) {
        self.levels
            .entry(snapshot.level)
            .and_modify(|stored| stored.value.size = snapshot.size)
            .or_insert(StoredLevel { value: Level { size: snapshot.size, daa_score: snapshot.daa_score }, usage_count: 0 });
    }

    fn increment_parent_usage(&mut self, parent_level: u64) {
        let stored = self.levels.get_mut(&parent_level).expect("a coherent graph contains every edge parent level");
        stored.usage_count = stored.usage_count.checked_add(1).expect("retained edge usage fits u64");
    }

    fn decrement_parent_usage(&mut self, parent_level: u64) {
        let stored = self.levels.get_mut(&parent_level).expect("a coherent graph contains every edge parent level");
        stored.usage_count = stored.usage_count.checked_sub(1).expect("retained parent usage is positive");
    }

    fn index_edge(&mut self, edge: GraphEdge) {
        self.edge_parents_by_child.entry(edge.id.child).or_default().insert(edge.id.parent);
        self.edge_ids_by_span
            .entry(edge.child_coordinate.level())
            .or_default()
            .entry(edge.parent_coordinate.level())
            .or_default()
            .insert(edge.id);
    }

    fn unindex_edge(&mut self, edge: GraphEdge) {
        let remove_child_entry = self.edge_parents_by_child.get_mut(&edge.id.child).is_some_and(|parents| {
            parents.remove(&edge.id.parent);
            parents.is_empty()
        });
        if remove_child_entry {
            self.edge_parents_by_child.remove(&edge.id.child);
        }

        let child_level = edge.child_coordinate.level();
        let parent_level = edge.parent_coordinate.level();
        let remove_child_level = self.edge_ids_by_span.get_mut(&child_level).is_some_and(|parents| {
            let remove_parent_level = parents.get_mut(&parent_level).is_some_and(|ids| {
                ids.remove(&edge.id);
                ids.is_empty()
            });
            if remove_parent_level {
                parents.remove(&parent_level);
            }
            parents.is_empty()
        });
        if remove_child_level {
            self.edge_ids_by_span.remove(&child_level);
        }
    }

    fn select_subview_graph(
        &self,
        extent: LevelExtent,
        mut inspect: impl FnMut(ExtractionCandidate),
    ) -> (HashMap<BlockHash, GraphBlock>, HashMap<EdgeId, GraphEdge>) {
        let mut blocks = HashMap::new();
        for hashes in self.block_hashes_by_level.range(extent.low_level()..=extent.high_level()).map(|(_, hashes)| hashes) {
            for hash in hashes {
                inspect(ExtractionCandidate::Block);
                blocks.insert(*hash, self.blocks.get(hash).expect("an indexed block exists").clone());
            }
        }

        let mut edges = HashMap::new();
        for parents in self.edge_ids_by_span.range(extent.low_level()..).map(|(_, parents)| parents) {
            for ids in parents.range(..=extent.high_level()).map(|(_, ids)| ids) {
                for id in ids {
                    inspect(ExtractionCandidate::Edge);
                    let edge = *self.edges.get(id).expect("an indexed edge exists");
                    debug_assert!(extent.intersects_span(edge.parent_coordinate.level(), edge.child_coordinate.level()));
                    edges.insert(*id, edge);
                }
            }
        }
        (blocks, edges)
    }

    /// Extracts a coherent frozen view over a contained nominal extent.
    pub fn extract_subview(&self, extent: LevelExtent) -> Result<Self, GraphViewExtractError> {
        if !self.extent.contains_extent(extent) {
            return Err(GraphViewExtractError::OutsideSourceExtent);
        }

        let (blocks, edges) = self.select_subview_graph(extent, |_| {});

        let mut selected_levels = BTreeMap::new();
        for level in extent.low_level()..=extent.high_level() {
            self.insert_source_level(&mut selected_levels, level);
        }
        for edge in edges.values() {
            self.insert_source_level(&mut selected_levels, edge.parent_coordinate.level());
            self.insert_source_level(&mut selected_levels, edge.child_coordinate.level());
        }
        let block_hashes_by_level = index_blocks_by_level(&blocks);
        let edge_parents_by_child = index_edge_parents_by_child(&edges);
        let edge_ids_by_span = index_edges_by_span(&edges);

        let mut extracted = Self {
            max_depth: extent.depth(),
            extent,
            current_revision_id: self.current_revision_id,
            current_revision_timestamp_ms: self.current_revision_timestamp_ms,
            tracking_policy: TrackingPolicy::Frozen,
            levels: selected_levels,
            blocks,
            edges,
            block_hashes_by_level,
            edge_parents_by_child,
            edge_ids_by_span,
        };
        let parent_levels: Vec<_> = extracted.edges.values().map(|edge| edge.parent_coordinate.level()).collect();
        for parent_level in parent_levels {
            extracted.increment_parent_usage(parent_level);
        }
        Ok(extracted)
    }
}

fn index_blocks_by_level(blocks: &HashMap<BlockHash, GraphBlock>) -> BTreeMap<u64, HashSet<BlockHash>> {
    let mut index: BTreeMap<u64, HashSet<BlockHash>> = BTreeMap::new();
    for (&hash, block) in blocks {
        index.entry(block.coordinate.level()).or_default().insert(hash);
    }
    index
}

fn index_edge_parents_by_child(edges: &HashMap<EdgeId, GraphEdge>) -> HashMap<BlockHash, HashSet<BlockHash>> {
    let mut index: HashMap<BlockHash, HashSet<BlockHash>> = HashMap::new();
    for &id in edges.keys() {
        index.entry(id.child).or_default().insert(id.parent);
    }
    index
}

fn index_edges_by_span(edges: &HashMap<EdgeId, GraphEdge>) -> BTreeMap<u64, BTreeMap<u64, HashSet<EdgeId>>> {
    let mut index: BTreeMap<u64, BTreeMap<u64, HashSet<EdgeId>>> = BTreeMap::new();
    for (&id, edge) in edges {
        index.entry(edge.child_coordinate.level()).or_default().entry(edge.parent_coordinate.level()).or_default().insert(id);
    }
    index
}

fn graph_block_from_committed(committed: &BlockCommitted) -> GraphBlock {
    GraphBlock {
        hash: committed.hash,
        coordinate: committed.coordinate,
        timestamp: committed.timestamp,
        daa_score: committed.daa_score,
        selected_parent_index: committed.selected_parent_index,
        direct_parents: committed.direct_parents.iter().map(|parent| parent.hash).collect(),
        blue_merge_set: Arc::clone(&committed.blue_merge_set),
        red_merge_set: Arc::clone(&committed.red_merge_set),
        color: committed.color,
        is_in_vspc: committed.is_in_vspc,
    }
}

struct VspcProjectionMetadata {
    blue_merge_set: Arc<[BlockHash]>,
    red_merge_set: Arc<[BlockHash]>,
}

impl From<&GraphBlock> for VspcProjectionMetadata {
    fn from(block: &GraphBlock) -> Self {
        Self { blue_merge_set: Arc::clone(&block.blue_merge_set), red_merge_set: Arc::clone(&block.red_merge_set) }
    }
}

#[derive(Default)]
struct GraphMutation {
    blocks: HashMap<BlockHash, Option<GraphBlock>>,
    edges: HashMap<EdgeId, Option<GraphEdge>>,
    levels: BTreeMap<u64, Option<Level>>,
    is_in_vspc: HashMap<BlockHash, bool>,
    colors: HashMap<BlockHash, BlockColor>,
}

impl GraphMutation {
    fn capture_block(&mut self, blocks: &HashMap<BlockHash, GraphBlock>, hash: BlockHash) {
        self.blocks.entry(hash).or_insert_with(|| blocks.get(&hash).cloned());
    }

    fn capture_removed_block(&mut self, hash: BlockHash, block: GraphBlock) {
        self.blocks.entry(hash).or_insert(Some(block));
    }

    fn capture_edge(&mut self, edges: &HashMap<EdgeId, GraphEdge>, id: EdgeId) {
        self.edges.entry(id).or_insert_with(|| edges.get(&id).copied());
    }

    fn capture_removed_edge(&mut self, id: EdgeId, edge: GraphEdge) {
        self.edges.entry(id).or_insert(Some(edge));
    }

    fn capture_level(&mut self, levels: &BTreeMap<u64, StoredLevel>, level: u64) {
        self.levels.entry(level).or_insert_with(|| levels.get(&level).map(|stored| stored.value));
    }

    fn capture_removed_level(&mut self, level: u64, value: Level) {
        self.levels.entry(level).or_insert(Some(value));
    }

    fn capture_level_value(&mut self, level: u64, value: Level) {
        self.levels.entry(level).or_insert(Some(value));
    }

    fn capture_membership(&mut self, hash: BlockHash, is_in_vspc: bool) {
        self.is_in_vspc.entry(hash).or_insert(is_in_vspc);
    }

    fn capture_color(&mut self, hash: BlockHash, color: BlockColor) {
        self.colors.entry(hash).or_insert(color);
    }

    fn finish(self, view: &GraphView) -> GraphChanges {
        let blocks = self
            .blocks
            .into_iter()
            .filter_map(|(hash, before)| {
                let after = view.blocks.get(&hash);
                (before.as_ref() != after).then(|| (hash, after.cloned()))
            })
            .collect();
        let edges = self
            .edges
            .into_iter()
            .filter_map(|(id, before)| {
                let after = view.edges.get(&id);
                (before.as_ref() != after).then(|| (id, after.copied()))
            })
            .collect();
        let levels = self
            .levels
            .into_iter()
            .filter_map(|(level, before)| {
                let after = view.levels.get(&level).map(|stored| stored.value);
                (before != after).then_some(LevelChange { level, before, after })
            })
            .collect();
        let is_in_vspc = self
            .is_in_vspc
            .into_iter()
            .filter_map(|(hash, before)| {
                let after = view.blocks.get(&hash)?.is_in_vspc;
                (before != after).then_some((hash, FieldChange { before, after }))
            })
            .collect();
        let colors = self
            .colors
            .into_iter()
            .filter_map(|(hash, before)| {
                let after = view.blocks.get(&hash)?.color;
                (before != after).then_some((hash, FieldChange { before, after }))
            })
            .collect();

        GraphChanges { blocks, edges, levels, is_in_vspc, colors }
    }
}

struct GraphChanges {
    blocks: HashMap<BlockHash, Option<GraphBlock>>,
    edges: HashMap<EdgeId, Option<GraphEdge>>,
    levels: Vec<LevelChange>,
    is_in_vspc: HashMap<BlockHash, FieldChange<bool>>,
    colors: HashMap<BlockHash, FieldChange<BlockColor>>,
}

impl GraphChanges {
    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
            && self.edges.is_empty()
            && self.levels.is_empty()
            && self.is_in_vspc.is_empty()
            && self.colors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use kgi_model::{
        block::{CompactId, MAX_DAA_SCORE},
        graph_update::ParentCommitted,
    };

    use super::*;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn level_hash(level: u64) -> BlockHash {
        let mut bytes = [0; 32];
        bytes[..size_of::<u64>()].copy_from_slice(&level.to_le_bytes());
        BlockHash::from_bytes(bytes)
    }

    fn coordinate(level: u64, slot: u64) -> BlockCoordinate {
        BlockCoordinate::new(level, slot).expect("positive level")
    }

    fn extent(low_level: u64, high_level: u64) -> LevelExtent {
        LevelExtent::new(low_level, high_level).expect("valid extent")
    }

    fn graph_block_at(byte: u8, level: u64, slot: u64) -> GraphBlock {
        GraphBlock {
            hash: hash(byte),
            coordinate: coordinate(level, slot),
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

    fn graph_block(byte: u8, level: u64) -> GraphBlock {
        graph_block_at(byte, level, 0)
    }

    fn graph_edge(parent_byte: u8, parent_level: u64, child_byte: u8, child_level: u64) -> GraphEdge {
        GraphEdge {
            id: EdgeId { parent: hash(parent_byte), child: hash(child_byte) },
            parent_coordinate: coordinate(parent_level, 0),
            child_coordinate: coordinate(child_level, 0),
        }
    }

    fn committed_block(
        byte: u8,
        level: u64,
        slot: u64,
        direct_parents: impl Into<Arc<[ParentCommitted]>>,
        level_snapshots: impl Into<Arc<[LevelCommitted]>>,
    ) -> BlockCommitted {
        let direct_parents = direct_parents.into();
        BlockCommitted {
            id: CompactId::new(i64::from(byte)).expect("positive fixture ID"),
            hash: hash(byte),
            coordinate: coordinate(level, slot),
            timestamp: u64::from(byte) * 10,
            daa_score: u64::from(byte) * 100,
            selected_parent_index: (!direct_parents.is_empty()).then_some(0),
            direct_parents,
            blue_merge_set: Arc::from([hash(20)]),
            red_merge_set: Arc::from([hash(21)]),
            color: BlockColor::Blue,
            is_in_vspc: true,
            level_snapshots: level_snapshots.into(),
        }
    }

    fn vspc_committed(
        removed: impl Into<Arc<[BlockHash]>>,
        added: impl Into<Arc<[BlockHash]>>,
        level_snapshots: impl Into<Arc<[LevelCommitted]>>,
    ) -> VspcCommitted {
        VspcCommitted {
            source: hash(90),
            destination: hash(91),
            removed: removed.into(),
            added: added.into(),
            level_snapshots: level_snapshots.into(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_delta(
        from_revision_id: u64,
        to_revision_id: u64,
        revision_timestamp_ms: u64,
        high_level: u64,
        block_changes: HashMap<BlockHash, Option<GraphBlock>>,
        edge_changes: HashMap<EdgeId, Option<GraphEdge>>,
        level_changes: Vec<LevelChange>,
        is_in_vspc_changes: HashMap<BlockHash, FieldChange<bool>>,
        color_changes: HashMap<BlockHash, FieldChange<BlockColor>>,
    ) -> GraphDelta {
        GraphDelta::new(
            from_revision_id,
            to_revision_id,
            revision_timestamp_ms,
            high_level,
            block_changes,
            edge_changes,
            level_changes,
            is_in_vspc_changes,
            color_changes,
        )
        .expect("valid fixture delta")
    }

    fn pruning_view() -> GraphView {
        let levels = BTreeMap::from([
            (1, Level { size: 1, daa_score: Some(10) }),
            (2, Level { size: 1, daa_score: Some(20) }),
            (3, Level { size: 1, daa_score: Some(30) }),
        ]);
        let blocks = HashMap::from([(hash(2), graph_block(2, 2)), (hash(3), graph_block(3, 3))]);
        let edge_values = [graph_edge(1, 1, 2, 2), graph_edge(2, 2, 3, 3)];
        let edges = edge_values.into_iter().map(|edge| (edge.id, edge)).collect();

        GraphView::from_database_projection(2, extent(2, 3), TrackingPolicy::Head, levels, blocks, edges)
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
        assert_eq!(view.current_revision_timestamp_ms(), 0);
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
        source.current_revision_timestamp_ms = 91;
        let before = source.clone();

        let extracted = source.extract_subview(extent(4, 4)).expect("contained extent");

        assert_eq!(source, before);
        assert_eq!(extracted.max_depth(), 1);
        assert_eq!(extracted.low_level(), 4);
        assert_eq!(extracted.high_level(), 4);
        assert_eq!(extracted.current_revision_id(), 17);
        assert_eq!(extracted.current_revision_timestamp_ms(), 91);
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
    fn narrow_extraction_visits_only_indexed_block_and_edge_candidates() {
        let levels = (1..=MAX_CACHE_DEPTH).map(|level| (level, Level { size: 1, daa_score: Some(level) })).collect();
        let blocks = (1..=MAX_CACHE_DEPTH)
            .map(|level| {
                let hash = level_hash(level);
                (
                    hash,
                    GraphBlock {
                        hash,
                        coordinate: coordinate(level, 0),
                        timestamp: level,
                        daa_score: level,
                        selected_parent_index: None,
                        direct_parents: Arc::from([]),
                        blue_merge_set: Arc::from([]),
                        red_merge_set: Arc::from([]),
                        color: BlockColor::Gray,
                        is_in_vspc: false,
                    },
                )
            })
            .collect();
        let edges = (2..=MAX_CACHE_DEPTH)
            .map(|child_level| {
                let parent_level = child_level - 1;
                let id = EdgeId { parent: level_hash(parent_level), child: level_hash(child_level) };
                (id, GraphEdge { id, parent_coordinate: coordinate(parent_level, 0), child_coordinate: coordinate(child_level, 0) })
            })
            .collect();
        let source = GraphView::from_database_projection(
            MAX_CACHE_DEPTH,
            extent(1, MAX_CACHE_DEPTH),
            TrackingPolicy::Head,
            levels,
            blocks,
            edges,
        );
        let narrow_extent = extent(476, 525);
        let mut visited_blocks = 0;
        let mut visited_edges = 0;

        let (selected_blocks, selected_edges) = source.select_subview_graph(narrow_extent, |candidate| match candidate {
            ExtractionCandidate::Block => visited_blocks += 1,
            ExtractionCandidate::Edge => visited_edges += 1,
        });

        assert_eq!(visited_blocks, 50);
        assert_eq!(visited_edges, 51);
        assert_eq!(selected_blocks.len(), 50);
        assert_eq!(selected_edges.len(), 51);
        assert!(visited_blocks < source.blocks.len());
        assert!(visited_edges < source.edges.len());

        let extracted = source.extract_subview(narrow_extent).expect("narrow extent is contained");
        assert_eq!(extracted.blocks.len(), 50);
        assert_eq!(extracted.edges.len(), 51);
        assert_eq!(extracted.levels.len(), 52, "both crossing endpoint levels remain retained");
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

    #[test]
    fn head_block_mutation_projects_complete_values_edges_and_level_authorities() {
        let levels = BTreeMap::from([(2, Level { size: 1, daa_score: Some(20) }), (3, Level { size: 2, daa_score: Some(30) })]);
        let blocks = HashMap::from([(hash(2), graph_block(2, 2)), (hash(3), graph_block(3, 3)), (hash(13), graph_block_at(13, 3, 1))]);
        let mut view = GraphView::from_database_projection(3, extent(2, 3), TrackingPolicy::Head, levels, blocks, HashMap::new());
        let committed = committed_block(
            4,
            4,
            0,
            Arc::from([
                ParentCommitted { hash: hash(3), coordinate: Some(coordinate(3, 0)) },
                ParentCommitted { hash: hash(9), coordinate: None },
                ParentCommitted { hash: hash(13), coordinate: Some(coordinate(3, 1)) },
            ]),
            Arc::from([
                LevelCommitted { level: 3, size: 3, daa_score: Some(999) },
                LevelCommitted { level: 4, size: 1, daa_score: Some(40) },
            ]),
        );

        let delta = view.apply_head_block_committed(&committed, 700).expect("mutable Head").expect("visible update");

        assert_eq!(view.extent(), extent(2, 4));
        assert_eq!(view.current_revision_id(), 1);
        assert_eq!(view.current_revision_timestamp_ms(), 700);
        let block = view.block(&hash(4)).expect("inserted block retained");
        assert_eq!(block.coordinate, coordinate(4, 0));
        assert_eq!(block.timestamp, 40);
        assert_eq!(block.daa_score, 400);
        assert_eq!(block.selected_parent_index, Some(0));
        assert_eq!(&*block.direct_parents, &[hash(3), hash(9), hash(13)]);
        assert_eq!(&*block.blue_merge_set, &[hash(20)]);
        assert_eq!(&*block.red_merge_set, &[hash(21)]);
        assert_eq!(block.color, BlockColor::Blue);
        assert!(block.is_in_vspc);

        let first_edge = EdgeId { parent: hash(3), child: hash(4) };
        let second_edge = EdgeId { parent: hash(13), child: hash(4) };
        let boundary_edge = EdgeId { parent: hash(9), child: hash(4) };
        assert!(view.edge(&first_edge).is_some());
        assert!(view.edge(&second_edge).is_some());
        assert!(view.edge(&boundary_edge).is_none());
        assert_eq!(view.level(3), Some(&Level { size: 3, daa_score: Some(30) }));
        assert_eq!(view.level(4), Some(&Level { size: 1, daa_score: Some(40) }));
        assert_eq!(view.levels.get(&3).map(|level| level.usage_count), Some(2));

        assert_eq!(delta.from_revision_id(), 0);
        assert_eq!(delta.to_revision_id(), 1);
        assert_eq!(delta.revision_timestamp_ms(), 700);
        assert_eq!(delta.high_level(), 4);
        assert_eq!(delta.block_changes().get(&hash(4)), Some(&Some(block.clone())));
        assert!(delta.edge_changes().get(&first_edge).is_some_and(Option::is_some));
        assert!(delta.edge_changes().get(&second_edge).is_some_and(Option::is_some));
        assert!(!delta.edge_changes().contains_key(&boundary_edge));
        assert_eq!(
            delta.level_changes(),
            &[
                LevelChange {
                    level: 3,
                    before: Some(Level { size: 2, daa_score: Some(30) }),
                    after: Some(Level { size: 3, daa_score: Some(30) }),
                },
                LevelChange { level: 4, before: None, after: Some(Level { size: 1, daa_score: Some(40) }) },
            ]
        );
        assert!(delta.is_in_vspc_changes().is_empty());
        assert!(delta.color_changes().is_empty());
    }

    #[test]
    fn head_pruning_follows_child_owned_edges_and_retains_external_parent_levels() {
        let mut view = pruning_view();
        let committed = committed_block(
            4,
            4,
            0,
            Arc::from([ParentCommitted { hash: hash(3), coordinate: Some(coordinate(3, 0)) }]),
            Arc::from([
                LevelCommitted { level: 3, size: 1, daa_score: Some(300) },
                LevelCommitted { level: 4, size: 1, daa_score: Some(40) },
            ]),
        );

        let delta = view.apply_head_block_committed(&committed, 800).expect("mutable Head").expect("visible update");

        let removed_edge = EdgeId { parent: hash(1), child: hash(2) };
        let parent_only_edge = EdgeId { parent: hash(2), child: hash(3) };
        assert_eq!(view.extent(), extent(3, 4));
        assert!(view.block(&hash(2)).is_none());
        assert!(view.edge(&removed_edge).is_none());
        assert!(view.edge(&parent_only_edge).is_some());
        assert!(view.level(1).is_none());
        assert_eq!(view.level(2), Some(&Level { size: 1, daa_score: Some(20) }));
        assert_eq!(view.levels.get(&2).map(|level| level.usage_count), Some(1));
        assert_eq!(delta.block_changes().get(&hash(2)), Some(&None));
        assert_eq!(delta.edge_changes().get(&removed_edge), Some(&None));
        assert!(!delta.edge_changes().contains_key(&parent_only_edge));
        assert_eq!(
            delta.level_changes(),
            &[
                LevelChange { level: 1, before: Some(Level { size: 1, daa_score: Some(10) }), after: None },
                LevelChange { level: 4, before: None, after: Some(Level { size: 1, daa_score: Some(40) }) },
            ]
        );

        let next = committed_block(
            5,
            5,
            0,
            Arc::from([ParentCommitted { hash: hash(4), coordinate: Some(coordinate(4, 0)) }]),
            Arc::from([
                LevelCommitted { level: 4, size: 1, daa_score: Some(400) },
                LevelCommitted { level: 5, size: 1, daa_score: Some(50) },
            ]),
        );
        view.apply_head_block_committed(&next, 900).expect("mutable Head").expect("visible update");

        assert_eq!(view.extent(), extent(4, 5));
        assert!(view.block(&hash(3)).is_none());
        assert!(view.edge(&parent_only_edge).is_none());
        assert!(view.level(2).is_none());
        assert_eq!(view.level(3), Some(&Level { size: 1, daa_score: Some(30) }));
        assert_eq!(view.levels.get(&3).map(|level| level.usage_count), Some(1));
        view.extract_subview(view.extent()).expect("pruning removes stale span-index entries");
    }

    #[test]
    fn below_range_commit_updates_only_a_retained_external_level_and_can_be_a_noop() {
        let mut view = pruning_view();
        for (byte, timestamp) in [(4, 800), (5, 900)] {
            let committed = committed_block(
                byte,
                u64::from(byte),
                0,
                Arc::from([ParentCommitted { hash: hash(byte - 1), coordinate: Some(coordinate(u64::from(byte - 1), 0)) }]),
                Arc::from([
                    LevelCommitted { level: u64::from(byte - 1), size: 1, daa_score: Some(999) },
                    LevelCommitted { level: u64::from(byte), size: 1, daa_score: Some(u64::from(byte) * 10) },
                ]),
            );
            view.apply_head_block_committed(&committed, timestamp).expect("mutable Head").expect("visible update");
        }
        assert_eq!(view.extent(), extent(4, 5));
        assert_eq!(view.level(3), Some(&Level { size: 1, daa_score: Some(30) }));

        let below_range = committed_block(
            23,
            3,
            1,
            Arc::from([ParentCommitted { hash: hash(2), coordinate: Some(coordinate(2, 0)) }]),
            Arc::from([
                LevelCommitted { level: 2, size: 1, daa_score: Some(222) },
                LevelCommitted { level: 3, size: 9, daa_score: Some(999) },
            ]),
        );
        let delta = view.apply_head_block_committed(&below_range, 1_000).expect("mutable Head").expect("external level changed");

        assert!(view.block(&hash(23)).is_none());
        assert_eq!(view.level(3), Some(&Level { size: 9, daa_score: Some(30) }));
        assert!(view.level(2).is_none());
        assert!(delta.block_changes().is_empty());
        assert!(delta.edge_changes().is_empty());
        assert_eq!(
            delta.level_changes(),
            &[LevelChange {
                level: 3,
                before: Some(Level { size: 1, daa_score: Some(30) }),
                after: Some(Level { size: 9, daa_score: Some(30) }),
            }]
        );
        assert_eq!(delta.high_level(), 5);
        assert_eq!(view.current_revision_id(), 3);
        assert_eq!(view.current_revision_timestamp_ms(), 1_000);

        let unchanged = view.apply_head_block_committed(&below_range, 1_100).expect("mutable Head");
        assert!(unchanged.is_none());
        assert_eq!(view.current_revision_id(), 3);
        assert_eq!(view.current_revision_timestamp_ms(), 1_000);
    }

    #[test]
    fn frozen_head_mutation_refuses_before_touching_the_payload() {
        let mut view = view(TrackingPolicy::Frozen);
        let before = view.clone();
        let incomplete = committed_block(8, 8, 0, Arc::from([]), Arc::from([]));
        let vspc = vspc_committed(Arc::from([]), Arc::from([hash(99)]), Arc::from([]));

        assert_eq!(view.apply_head_block_committed(&incomplete, 1_200), Err(GraphViewUpdateError::Frozen));
        assert_eq!(view, before);
        assert_eq!(view.apply_head_vspc_committed(&vspc, 1_200), Err(GraphViewUpdateError::Frozen));
        assert_eq!(view, before);
    }

    #[test]
    fn head_vspc_projection_records_final_fields_and_retained_level_scores() {
        let levels = BTreeMap::from([
            (1, Level { size: 1, daa_score: Some(10) }),
            (2, Level { size: 1, daa_score: Some(20) }),
            (3, Level { size: 1, daa_score: Some(30) }),
            (4, Level { size: 1, daa_score: Some(40) }),
            (5, Level { size: 1, daa_score: Some(50) }),
        ]);
        let mut removed = graph_block(2, 2);
        removed.is_in_vspc = true;
        removed.color = BlockColor::Red;
        let mut added = graph_block(5, 5);
        added.blue_merge_set = Arc::from([hash(3), hash(4)]);
        added.red_merge_set = Arc::from([hash(4), hash(8)]);
        let blocks = HashMap::from([(hash(2), removed), (hash(3), graph_block(3, 3)), (hash(4), graph_block(4, 4)), (hash(5), added)]);
        let crossing_edge = graph_edge(1, 1, 2, 2);
        let mut view = GraphView::from_database_projection(
            4,
            extent(2, 5),
            TrackingPolicy::Head,
            levels,
            blocks,
            HashMap::from([(crossing_edge.id, crossing_edge)]),
        );
        let committed = vspc_committed(
            Arc::from([hash(2), hash(88)]),
            Arc::from([hash(5), hash(89)]),
            Arc::from([
                LevelCommitted { level: 1, size: 999, daa_score: Some(11) },
                LevelCommitted { level: 4, size: 999, daa_score: None },
                LevelCommitted { level: 99, size: 999, daa_score: Some(99) },
            ]),
        );

        let delta = view.apply_head_vspc_committed(&committed, 1_300).expect("mutable Head").expect("visible projection");

        assert_eq!(view.block(&hash(2)).map(|block| (block.is_in_vspc, block.color)), Some((false, BlockColor::Gray)));
        assert_eq!(view.block(&hash(3)).map(|block| block.color), Some(BlockColor::Blue));
        assert_eq!(view.block(&hash(4)).map(|block| block.color), Some(BlockColor::Red));
        assert_eq!(view.block(&hash(5)).map(|block| block.is_in_vspc), Some(true));
        assert_eq!(view.level(1), Some(&Level { size: 1, daa_score: Some(11) }));
        assert_eq!(view.level(4), Some(&Level { size: 1, daa_score: None }));
        assert!(view.level(99).is_none());
        assert_eq!(view.levels.get(&1).map(|stored| stored.usage_count), Some(1));

        assert_eq!(delta.from_revision_id(), 0);
        assert_eq!(delta.to_revision_id(), 1);
        assert_eq!(delta.high_level(), 5);
        assert!(delta.block_changes().is_empty());
        assert!(delta.edge_changes().is_empty());
        assert_eq!(delta.is_in_vspc_changes().get(&hash(2)), Some(&FieldChange { before: true, after: false }));
        assert_eq!(delta.is_in_vspc_changes().get(&hash(5)), Some(&FieldChange { before: false, after: true }));
        assert_eq!(delta.color_changes().get(&hash(2)), Some(&FieldChange { before: BlockColor::Red, after: BlockColor::Gray }));
        assert_eq!(delta.color_changes().get(&hash(4)), Some(&FieldChange { before: BlockColor::Gray, after: BlockColor::Red }));
        assert_eq!(
            delta.level_changes(),
            &[
                LevelChange {
                    level: 1,
                    before: Some(Level { size: 1, daa_score: Some(10) }),
                    after: Some(Level { size: 1, daa_score: Some(11) }),
                },
                LevelChange {
                    level: 4,
                    before: Some(Level { size: 1, daa_score: Some(40) }),
                    after: Some(Level { size: 1, daa_score: None }),
                },
            ]
        );
    }

    #[test]
    fn head_vspc_noops_and_absent_targets_do_not_advance_revision() {
        let mut view = view(TrackingPolicy::Head);
        let committed = vspc_committed(
            Arc::from([hash(2), hash(88)]),
            Arc::from([hash(89)]),
            Arc::from([
                LevelCommitted { level: 2, size: 999, daa_score: Some(20) },
                LevelCommitted { level: 99, size: 1, daa_score: Some(99) },
            ]),
        );
        let before = view.clone();

        assert!(view.apply_head_vspc_committed(&committed, 1_300).expect("mutable Head").is_none());
        assert_eq!(view, before);
    }

    #[test]
    fn fixed_block_projection_uses_original_event_even_when_head_has_no_effect() {
        let head_levels = (5..=7).map(|level| (level, Level { size: 1, daa_score: Some(level * 10) })).collect();
        let head_blocks = (5..=7).map(|level| (hash(level as u8), graph_block(level as u8, level))).collect();
        let mut head =
            GraphView::from_database_projection(3, extent(5, 7), TrackingPolicy::Head, head_levels, head_blocks, HashMap::new());
        let fixed_levels = (3..=5).map(|level| (level, Level { size: 1, daa_score: Some(level * 10) })).collect();
        let fixed_blocks = (3..=5).map(|level| (hash(level as u8), graph_block(level as u8, level))).collect();
        let mut fixed =
            GraphView::from_database_projection(3, extent(3, 5), TrackingPolicy::Fixed, fixed_levels, fixed_blocks, HashMap::new());
        let below_head = committed_block(
            24,
            4,
            1,
            Arc::from([ParentCommitted { hash: hash(2), coordinate: Some(coordinate(2, 0)) }]),
            Arc::from([
                LevelCommitted { level: 2, size: 1, daa_score: Some(20) },
                LevelCommitted { level: 4, size: 2, daa_score: Some(44) },
            ]),
        );

        assert!(head.apply_head_block_committed(&below_head, 1_400).expect("mutable Head").is_none());
        let delta =
            fixed.apply_fixed_block_committed(&below_head, &head, 1_500).expect("mutable Fixed").expect("visible Fixed update");

        assert_eq!(head.current_revision_id(), 0);
        assert_eq!(fixed.current_revision_id(), 1);
        assert_eq!(fixed.extent(), extent(3, 5));
        assert!(fixed.block(&hash(24)).is_some());
        assert!(fixed.edge(&EdgeId { parent: hash(2), child: hash(24) }).is_some());
        assert_eq!(fixed.level(2), Some(&Level { size: 1, daa_score: Some(20) }));
        assert_eq!(fixed.level(4), Some(&Level { size: 2, daa_score: Some(40) }));
        assert_eq!(delta.high_level(), 5);

        let above_fixed = committed_block(
            26,
            6,
            1,
            Arc::from([ParentCommitted { hash: hash(5), coordinate: Some(coordinate(5, 0)) }]),
            Arc::from([
                LevelCommitted { level: 5, size: 2, daa_score: Some(999) },
                LevelCommitted { level: 6, size: 2, daa_score: Some(60) },
            ]),
        );
        head.apply_head_block_committed(&above_fixed, 1_600).expect("mutable Head").expect("Head retains block");
        let crossing =
            fixed.apply_fixed_block_committed(&above_fixed, &head, 1_700).expect("mutable Fixed").expect("crossing edge retained");

        assert!(fixed.block(&hash(26)).is_none());
        assert!(fixed.edge(&EdgeId { parent: hash(5), child: hash(26) }).is_some());
        assert_eq!(fixed.level(5), Some(&Level { size: 2, daa_score: Some(50) }));
        assert_eq!(fixed.level(6), Some(&Level { size: 2, daa_score: Some(60) }));
        assert!(crossing.block_changes().is_empty());
        assert!(crossing.edge_changes().contains_key(&EdgeId { parent: hash(5), child: hash(26) }));

        let neither_endpoint_retained = committed_block(
            27,
            7,
            1,
            Arc::from([ParentCommitted { hash: hash(2), coordinate: Some(coordinate(2, 0)) }]),
            Arc::from([
                LevelCommitted { level: 2, size: 3, daa_score: Some(20) },
                LevelCommitted { level: 7, size: 4, daa_score: Some(70) },
            ]),
        );
        head.apply_head_block_committed(&neither_endpoint_retained, 1_800).expect("mutable Head").expect("Head retains block");
        fixed
            .apply_fixed_block_committed(&neither_endpoint_retained, &head, 1_900)
            .expect("mutable Fixed")
            .expect("crossing edge with external endpoints retained");

        let parent_external_edge = EdgeId { parent: hash(2), child: hash(27) };
        assert!(fixed.block(&hash(2)).is_none());
        assert!(fixed.block(&hash(27)).is_none());
        assert!(fixed.edge(&parent_external_edge).is_some());
        assert_eq!(fixed.level(2), Some(&Level { size: 3, daa_score: Some(20) }));
        assert_eq!(fixed.level(7), Some(&Level { size: 4, daa_score: Some(70) }));

        let extracted = fixed.extract_subview(extent(3, 5)).expect("every crossing edge has complete endpoint levels");
        assert!(extracted.edge(&EdgeId { parent: hash(5), child: hash(26) }).is_some());
        assert!(extracted.edge(&parent_external_edge).is_some());
        assert_eq!(extracted.level(2), fixed.level(2));
        assert_eq!(extracted.level(6), fixed.level(6));
        assert_eq!(extracted.level(7), fixed.level(7));
    }

    #[test]
    fn fixed_vspc_projection_resolves_added_metadata_from_both_views() {
        let mut fixed_member = graph_block(5, 5);
        fixed_member.blue_merge_set = Arc::from([hash(3)]);
        let mut fixed_target = graph_block(3, 3);
        fixed_target.color = BlockColor::Red;
        let fixed_levels = BTreeMap::from([
            (2, Level { size: 1, daa_score: Some(20) }),
            (3, Level { size: 1, daa_score: Some(30) }),
            (4, Level { size: 1, daa_score: Some(40) }),
            (5, Level { size: 1, daa_score: Some(50) }),
        ]);
        let fixed_edge = graph_edge(2, 2, 3, 3);
        let mut fixed = GraphView::from_database_projection(
            3,
            extent(3, 5),
            TrackingPolicy::Fixed,
            fixed_levels,
            HashMap::from([(hash(3), fixed_target), (hash(4), graph_block(4, 4)), (hash(5), fixed_member)]),
            HashMap::from([(fixed_edge.id, fixed_edge)]),
        );

        let mut head_member = graph_block(7, 7);
        head_member.blue_merge_set = Arc::from([hash(4)]);
        head_member.red_merge_set = Arc::from([hash(3), hash(4)]);
        let head_levels = (5..=7).map(|level| (level, Level { size: 1, daa_score: Some(level * 10) })).collect();
        let mut head = GraphView::from_database_projection(
            3,
            extent(5, 7),
            TrackingPolicy::Head,
            head_levels,
            HashMap::from([(hash(7), head_member)]),
            HashMap::new(),
        );
        let committed = vspc_committed(
            Arc::from([]),
            Arc::from([hash(5), hash(7)]),
            Arc::from([LevelCommitted { level: 2, size: 999, daa_score: Some(22) }]),
        );

        head.apply_head_vspc_committed(&committed, 1_800).expect("mutable Head").expect("Head member enters VSPC");
        let delta = fixed
            .apply_fixed_vspc_committed(&committed, &head, 1_900)
            .expect("mutable Fixed")
            .expect("metadata recolors retained blocks");

        assert_eq!(fixed.block(&hash(5)).map(|block| block.is_in_vspc), Some(true));
        assert_eq!(fixed.block(&hash(3)).map(|block| block.color), Some(BlockColor::Red));
        assert_eq!(fixed.block(&hash(4)).map(|block| block.color), Some(BlockColor::Red));
        assert_eq!(fixed.level(2), Some(&Level { size: 1, daa_score: Some(22) }));
        assert_eq!(fixed.levels.get(&2).map(|stored| stored.usage_count), Some(1));
        assert_eq!(delta.color_changes().get(&hash(4)), Some(&FieldChange { before: BlockColor::Gray, after: BlockColor::Red }));
        assert!(!delta.color_changes().contains_key(&hash(3)), "temporary Blue restores the original Red and is omitted");
    }

    #[test]
    fn fixed_vspc_missing_metadata_freezes_before_any_projection() {
        let head = view(TrackingPolicy::Head);
        let mut fixed = view(TrackingPolicy::Fixed);
        fixed.current_revision_id = 7;
        fixed.current_revision_timestamp_ms = 700;
        let before = fixed.clone();
        let committed = vspc_committed(
            Arc::from([hash(2)]),
            Arc::from([hash(99)]),
            Arc::from([LevelCommitted { level: 4, size: 999, daa_score: None }]),
        );

        assert!(fixed.apply_fixed_vspc_committed(&committed, &head, 2_000).expect("terminal freeze").is_none());
        let mut expected = before;
        expected.tracking_policy = TrackingPolicy::Frozen;
        assert_eq!(fixed, expected);
    }

    #[test]
    fn fixed_disjoint_extent_freezes_and_later_updates_refuse_before_inspection() {
        let head_levels = BTreeMap::from([(4, Level { size: 1, daa_score: Some(40) }), (5, Level { size: 1, daa_score: Some(50) })]);
        let head =
            GraphView::from_database_projection(2, extent(4, 5), TrackingPolicy::Head, head_levels, HashMap::new(), HashMap::new());
        let fixed_levels = BTreeMap::from([(2, Level { size: 1, daa_score: Some(20) }), (3, Level { size: 1, daa_score: Some(30) })]);
        let mut fixed =
            GraphView::from_database_projection(2, extent(2, 3), TrackingPolicy::Fixed, fixed_levels, HashMap::new(), HashMap::new());
        let before = fixed.clone();
        let block = committed_block(8, 8, 0, Arc::from([]), Arc::from([]));

        assert!(fixed.apply_fixed_block_committed(&block, &head, 2_100).expect("terminal freeze").is_none());
        let mut expected = before;
        expected.tracking_policy = TrackingPolicy::Frozen;
        assert_eq!(fixed, expected);

        let invalid_head_context = view(TrackingPolicy::Fixed);
        let vspc = vspc_committed(Arc::from([]), Arc::from([hash(99)]), Arc::from([]));
        assert_eq!(fixed.apply_fixed_vspc_committed(&vspc, &invalid_head_context, 2_200), Err(GraphViewUpdateError::Frozen));
        assert_eq!(fixed.apply_fixed_block_committed(&block, &invalid_head_context, 2_200), Err(GraphViewUpdateError::Frozen));
    }

    #[test]
    fn materialization_and_vspc_delivery_orders_preserve_separate_level_authorities() {
        let make_view = || {
            GraphView::from_database_projection(
                3,
                extent(2, 3),
                TrackingPolicy::Head,
                BTreeMap::from([(2, Level { size: 1, daa_score: Some(20) }), (3, Level { size: 1, daa_score: Some(30) })]),
                HashMap::from([(hash(3), graph_block(3, 3))]),
                HashMap::new(),
            )
        };
        let block = committed_block(
            4,
            4,
            0,
            Arc::from([ParentCommitted { hash: hash(3), coordinate: Some(coordinate(3, 0)) }]),
            Arc::from([
                LevelCommitted { level: 3, size: 2, daa_score: Some(30) },
                LevelCommitted { level: 4, size: 1, daa_score: Some(40) },
            ]),
        );
        let vspc = vspc_committed(
            Arc::from([hash(88)]),
            Arc::from([hash(89)]),
            Arc::from([LevelCommitted { level: 3, size: 999, daa_score: Some(33) }]),
        );

        let mut materialization_first = make_view();
        materialization_first.apply_head_block_committed(&block, 1).expect("mutable Head").expect("visible block");
        let level_only = materialization_first.apply_head_vspc_committed(&vspc, 2).expect("mutable Head").expect("visible score");

        let mut vspc_first = make_view();
        vspc_first.apply_head_vspc_committed(&vspc, 1).expect("mutable Head").expect("visible score");
        vspc_first.apply_head_block_committed(&block, 2).expect("mutable Head").expect("visible block");

        assert_eq!(materialization_first.level(3), Some(&Level { size: 2, daa_score: Some(33) }));
        assert_eq!(vspc_first.level(3), Some(&Level { size: 2, daa_score: Some(33) }));
        assert!(level_only.block_changes().is_empty());
        assert!(level_only.edge_changes().is_empty());
        assert!(level_only.is_in_vspc_changes().is_empty());
        assert!(level_only.color_changes().is_empty());
        assert_eq!(
            level_only.level_changes(),
            &[LevelChange {
                level: 3,
                before: Some(Level { size: 2, daa_score: Some(30) }),
                after: Some(Level { size: 2, daa_score: Some(33) }),
            }]
        );
    }

    #[test]
    fn delta_application_uses_canonical_phase_order_and_maintains_indexes() {
        let levels = BTreeMap::from([(1, Level { size: 1, daa_score: Some(10) }), (2, Level { size: 2, daa_score: Some(20) })]);
        let mut retained = graph_block(1, 2);
        retained.timestamp = 1;
        let removed = graph_block_at(8, 2, 1);
        let old_edge = graph_edge(30, 1, 8, 2);
        let mut view = GraphView::from_database_projection(
            3,
            extent(1, 2),
            TrackingPolicy::Head,
            levels,
            HashMap::from([(hash(1), retained), (hash(8), removed)]),
            HashMap::from([(old_edge.id, old_edge)]),
        );
        view.current_revision_id = 4;
        view.current_revision_timestamp_ms = 40;

        let mut replacement = graph_block(1, 2);
        replacement.timestamp = 99;
        let added = graph_block(3, 3);
        let new_edge = graph_edge(1, 2, 3, 3);
        let delta = graph_delta(
            4,
            7,
            70,
            3,
            HashMap::from([(hash(1), Some(replacement.clone())), (hash(3), Some(added)), (hash(8), None)]),
            HashMap::from([(old_edge.id, None), (new_edge.id, Some(new_edge))]),
            vec![
                LevelChange { level: 1, before: Some(Level { size: 999, daa_score: None }), after: None },
                LevelChange {
                    level: 2,
                    before: Some(Level { size: 999, daa_score: None }),
                    after: Some(Level { size: 3, daa_score: Some(22) }),
                },
                LevelChange { level: 3, before: None, after: Some(Level { size: 1, daa_score: Some(30) }) },
            ],
            HashMap::from([
                (hash(3), FieldChange { before: true, after: true }),
                (hash(8), FieldChange { before: false, after: true }),
                (hash(99), FieldChange { before: false, after: true }),
            ]),
            HashMap::from([
                (hash(3), FieldChange { before: BlockColor::Red, after: BlockColor::Blue }),
                (hash(8), FieldChange { before: BlockColor::Gray, after: BlockColor::Red }),
                (hash(99), FieldChange { before: BlockColor::Gray, after: BlockColor::Red }),
            ]),
        );

        view.apply_delta(&delta).expect("matching Head lineage");

        assert_eq!(view.extent(), extent(1, 3));
        assert_eq!(view.current_revision_id(), 7);
        assert_eq!(view.current_revision_timestamp_ms(), 70);
        assert_eq!(view.block(&hash(1)), Some(&replacement));
        assert!(view.block(&hash(8)).is_none());
        assert!(!view.block_hashes_by_level.get(&2).is_some_and(|hashes| hashes.contains(&hash(8))));
        assert_eq!(view.block(&hash(3)).map(|block| (block.is_in_vspc, block.color)), Some((true, BlockColor::Blue)));
        assert!(view.edge(&old_edge.id).is_none());
        assert_eq!(view.edge(&new_edge.id), Some(&new_edge));
        assert!(!view.edge_parents_by_child.contains_key(&hash(8)));
        assert_eq!(view.edge_parents_by_child.get(&hash(3)), Some(&HashSet::from([hash(1)])));
        assert!(
            !view
                .edge_ids_by_span
                .get(&old_edge.child_coordinate.level())
                .and_then(|parents| parents.get(&old_edge.parent_coordinate.level()))
                .is_some_and(|ids| ids.contains(&old_edge.id))
        );
        assert!(
            view.edge_ids_by_span
                .get(&new_edge.child_coordinate.level())
                .and_then(|parents| parents.get(&new_edge.parent_coordinate.level()))
                .is_some_and(|ids| ids.contains(&new_edge.id))
        );
        assert!(view.level(1).is_none());
        assert_eq!(view.level(2), Some(&Level { size: 3, daa_score: Some(22) }));
        assert_eq!(view.levels.get(&2).map(|stored| stored.usage_count), Some(1));
        assert_eq!(view.level(3), Some(&Level { size: 1, daa_score: Some(30) }));
    }

    #[test]
    fn delta_application_errors_preserve_head_and_freeze_fixed_locally() {
        let delta = graph_delta(1, 2, 20, 9, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new());

        let mut head = view(TrackingPolicy::Head);
        let head_before = head.clone();
        assert_eq!(head.apply_delta(&delta), Err(GraphDeltaApplyError::RevisionMismatch { view_revision: 0, delta_from: 1 }));
        assert_eq!(head, head_before);

        let mut fixed = view(TrackingPolicy::Fixed);
        let fixed_before = fixed.clone();
        assert_eq!(fixed.apply_delta(&delta), Err(GraphDeltaApplyError::RevisionMismatch { view_revision: 0, delta_from: 1 }));
        let mut expected_fixed = fixed_before;
        expected_fixed.tracking_policy = TrackingPolicy::Frozen;
        assert_eq!(fixed, expected_fixed);

        let frozen_before = fixed.clone();
        assert_eq!(fixed.apply_delta(&delta), Err(GraphDeltaApplyError::Frozen));
        assert_eq!(fixed, frozen_before);
    }

    #[test]
    fn matching_fixed_delta_advances_revision_without_changing_bounds() {
        let mut fixed = view(TrackingPolicy::Fixed);
        let original_extent = fixed.extent();
        let delta = graph_delta(0, 3, 30, 99, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new());

        fixed.apply_delta(&delta).expect("matching Fixed lineage");

        assert_eq!(fixed.extent(), original_extent);
        assert_eq!(fixed.current_revision_id(), 3);
        assert_eq!(fixed.current_revision_timestamp_ms(), 30);
        assert_eq!(fixed.tracking_policy(), TrackingPolicy::Fixed);
    }

    #[test]
    fn composed_delta_has_the_same_effect_as_sequential_application() {
        let levels = BTreeMap::from([(1, Level { size: 1, daa_score: Some(10) }), (2, Level { size: 2, daa_score: Some(20) })]);
        let base = GraphView::from_database_projection(
            4,
            extent(1, 2),
            TrackingPolicy::Head,
            levels,
            HashMap::from([(hash(2), graph_block(2, 2)), (hash(8), graph_block_at(8, 2, 1))]),
            HashMap::new(),
        );

        let first_block = graph_block(3, 3);
        let mut touched_then_removed = graph_block_at(8, 2, 1);
        touched_then_removed.timestamp = 88;
        let edge = graph_edge(2, 2, 3, 3);
        let first = graph_delta(
            0,
            1,
            10,
            3,
            HashMap::from([(hash(3), Some(first_block)), (hash(8), Some(touched_then_removed))]),
            HashMap::from([(edge.id, Some(edge))]),
            vec![LevelChange { level: 3, before: None, after: Some(Level { size: 1, daa_score: Some(30) }) }],
            HashMap::from([(hash(3), FieldChange { before: false, after: true })]),
            HashMap::from([(hash(3), FieldChange { before: BlockColor::Gray, after: BlockColor::Blue })]),
        );

        let mut final_block = graph_block(3, 3);
        final_block.timestamp = 33;
        let second = graph_delta(
            1,
            2,
            20,
            4,
            HashMap::from([(hash(3), Some(final_block)), (hash(8), None)]),
            HashMap::new(),
            vec![
                LevelChange {
                    level: 3,
                    before: Some(Level { size: 999, daa_score: None }),
                    after: Some(Level { size: 2, daa_score: Some(33) }),
                },
                LevelChange { level: 4, before: None, after: Some(Level { size: 1, daa_score: Some(40) }) },
            ],
            HashMap::from([(hash(3), FieldChange { before: false, after: true })]),
            HashMap::from([(hash(3), FieldChange { before: BlockColor::Gray, after: BlockColor::Red })]),
        );
        let composed = first.compose(&second).expect("gapless intervals");

        let mut sequential = base.clone();
        sequential.apply_delta(&first).expect("first interval");
        sequential.apply_delta(&second).expect("second interval");
        let mut aggregated = base;
        aggregated.apply_delta(&composed).expect("composed interval");

        assert_eq!(aggregated, sequential);
        assert_eq!(aggregated.block(&hash(3)).map(|block| (block.is_in_vspc, block.color)), Some((true, BlockColor::Red)));
        assert!(aggregated.block(&hash(8)).is_none());
        assert_eq!(aggregated.current_revision_id(), 2);
        assert_eq!(aggregated.current_revision_timestamp_ms(), 20);
        assert_eq!(aggregated.extent(), extent(1, 4));
    }
}
