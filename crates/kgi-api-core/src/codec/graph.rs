use kgi_api_model::{
    graph::{EdgeId, GraphBlock, GraphEdge, GraphView, Level, LevelExtent},
    projection::GraphWindowResolution,
    protocol::HeadLevelValue,
};
use kgi_model::block::{BlockCoordinate, BlockHash};
use serde::Serialize;

use crate::publication::GraphViewCapture;

use super::{
    HeadEtagInput,
    dictionary::HashDictionary,
    scalar::{BlockColorDto, BlockHashDto, HashRef, U64Dto},
    to_json_bytes,
    window::GraphWindowSourceDto,
};

/// Complete public value for one retained level.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct LevelDto {
    level: u32,
    size: u16,
    daa_score: Option<U64Dto>,
}

impl LevelDto {
    pub(super) fn new(level: u32, value: Level) -> Self {
        Self { level, size: value.size, daa_score: value.daa_score.map(Into::into) }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct BlockCoordinateDto {
    level: u32,
    slot: u16,
}

impl From<BlockCoordinate> for BlockCoordinateDto {
    fn from(value: BlockCoordinate) -> Self {
        Self { level: value.level(), slot: value.slot() }
    }
}

/// Complete serialized state for one retained graph block.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct GraphBlockDto {
    hash: HashRef,
    coordinate: BlockCoordinateDto,
    timestamp: U64Dto,
    daa_score: U64Dto,
    selected_parent_index: Option<u32>,
    direct_parents: Vec<HashRef>,
    blue_merge_set: Vec<HashRef>,
    red_merge_set: Vec<HashRef>,
    color: BlockColorDto,
    is_in_vspc: bool,
}

impl GraphBlockDto {
    pub(super) fn from_graph_block(block: &GraphBlock, hashes: &mut HashDictionary) -> Self {
        Self {
            hash: hashes.intern(block.hash),
            coordinate: block.coordinate.into(),
            timestamp: block.timestamp.into(),
            daa_score: block.daa_score.into(),
            selected_parent_index: block.selected_parent_index,
            direct_parents: intern_hashes(&block.direct_parents, hashes),
            blue_merge_set: intern_hashes(&block.blue_merge_set, hashes),
            red_merge_set: intern_hashes(&block.red_merge_set, hashes),
            color: block.color.into(),
            is_in_vspc: block.is_in_vspc,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct EdgeIdDto {
    parent: HashRef,
    child: HashRef,
}

/// Complete serialized state for one retained graph edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct GraphEdgeDto {
    id: EdgeIdDto,
    parent_coordinate: BlockCoordinateDto,
    child_coordinate: BlockCoordinateDto,
}

impl GraphEdgeDto {
    pub(super) fn from_graph_edge(edge: &GraphEdge, hashes: &mut HashDictionary) -> Self {
        Self {
            id: edge_id(edge.id, hashes),
            parent_coordinate: edge.parent_coordinate.into(),
            child_coordinate: edge.child_coordinate.into(),
        }
    }
}

/// Complete graph body with one response-local block-hash dictionary.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct GraphDataDto {
    hashes: Vec<BlockHashDto>,
    levels: Vec<LevelDto>,
    blocks: Vec<GraphBlockDto>,
    edges: Vec<GraphEdgeDto>,
}

impl GraphDataDto {
    /// Converts one canonical captured graph without imposing collection order.
    pub(crate) fn from_view(view: &GraphView) -> Self {
        let levels = view.levels().map(|(level, value)| LevelDto::new(level, *value)).collect();
        let mut hashes = HashDictionary::default();
        let blocks = view.blocks().map(|block| GraphBlockDto::from_graph_block(block, &mut hashes)).collect();
        let edges = view.edges().map(|edge| GraphEdgeDto::from_graph_edge(edge, &mut hashes)).collect();

        Self { hashes: hashes.into_hashes(), levels, blocks, edges }
    }

    /// Serializes this complete graph body as compact JSON.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(self)
    }
}

/// Complete cacheable Head snapshot body.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct HeadSnapshotResponseDto {
    publication_id: U64Dto,
    revision: U64Dto,
    revision_timestamp_ms: U64Dto,
    low_level: u32,
    high_level: u32,
    graph: GraphDataDto,
}

impl HeadSnapshotResponseDto {
    /// Converts one immutable Head capture into a detached response body.
    pub(crate) fn from_capture(capture: &GraphViewCapture) -> Self {
        let view = capture.view();
        Self {
            publication_id: capture.publication_id().into(),
            revision: view.current_revision_id().into(),
            revision_timestamp_ms: view.current_revision_timestamp_ms().into(),
            low_level: view.low_level(),
            high_level: view.high_level(),
            graph: GraphDataDto::from_view(view),
        }
    }

    /// Returns the semantic identity used by the Head response validator.
    pub(crate) fn etag_input(&self) -> HeadEtagInput {
        let extent =
            LevelExtent::new(self.low_level, self.high_level).expect("a serialized Head snapshot retains a valid canonical extent");
        HeadEtagInput::new(self.publication_id.get(), self.revision.get(), extent)
    }

    /// Serializes this Head snapshot as compact JSON.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct GraphWindowResolutionDto {
    resolved_level: u32,
    effective_start_level: u32,
    effective_end_level: u32,
}

impl From<GraphWindowResolution> for GraphWindowResolutionDto {
    fn from(value: GraphWindowResolution) -> Self {
        Self {
            resolved_level: value.resolved_level,
            effective_start_level: value.effective_start_level,
            effective_end_level: value.effective_end_level,
        }
    }
}

/// Complete anchored-window response body.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct GraphWindowResponseDto {
    source: GraphWindowSourceDto,
    resolution: GraphWindowResolutionDto,
    graph: GraphDataDto,
}

impl GraphWindowResponseDto {
    /// Converts one immutable Head-backed window capture.
    pub(crate) fn from_head(capture: &GraphViewCapture, head_high_level: u32, resolution: GraphWindowResolution) -> Self {
        let view = capture.view();
        Self {
            source: GraphWindowSourceDto::head(
                capture.publication_id(),
                view.current_revision_id(),
                view.current_revision_timestamp_ms(),
                head_high_level,
            ),
            resolution: resolution.into(),
            graph: GraphDataDto::from_view(view),
        }
    }

    /// Converts one detached database-backed window without public lineage.
    pub(crate) fn from_database(view: &GraphView, resolution: GraphWindowResolution) -> Self {
        Self { source: GraphWindowSourceDto::database(), resolution: resolution.into(), graph: GraphDataDto::from_view(view) }
    }

    /// Serializes this anchored window as compact JSON.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(self)
    }
}

/// Complete successful retained Head-level lookup body.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct HeadLevelLookupResponseDto {
    publication_id: U64Dto,
    revision: U64Dto,
    levels: Vec<LevelDto>,
}

impl HeadLevelLookupResponseDto {
    /// Converts one successful all-or-nothing retained Head-level lookup.
    pub(crate) fn from_complete(publication_id: u64, revision: u64, levels: &[HeadLevelValue]) -> Self {
        Self {
            publication_id: publication_id.into(),
            revision: revision.into(),
            levels: levels.iter().map(|level| LevelDto::new(level.level, level.value)).collect(),
        }
    }

    /// Serializes this Head-level lookup as compact JSON.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(self)
    }
}

fn intern_hashes(hashes: &[BlockHash], dictionary: &mut HashDictionary) -> Vec<HashRef> {
    hashes.iter().map(|&hash| dictionary.intern(hash)).collect()
}

fn edge_id(id: EdgeId, hashes: &mut HashDictionary) -> EdgeIdDto {
    EdgeIdDto { parent: hashes.intern(id.parent), child: hashes.intern(id.child) }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
        time::Instant,
    };

    use kgi_api_model::{
        graph::{EdgeId, GraphBlock, GraphEdge, GraphView, Level, LevelExtent, TrackingPolicy},
        history::GraphHistory,
        projection::GraphWindowResolution,
        protocol::HeadLevelValue,
    };
    use kgi_model::block::{BlockColor, BlockCoordinate, BlockHash, MAX_DAA_SCORE, MAX_LEVEL_SIZE, MAX_LEVEL_SLOT};
    use serde_json::{Value, json};

    use crate::publication::{GraphPublication, GraphPublicationState};

    use super::{GraphDataDto, GraphWindowResponseDto, HeadLevelLookupResponseDto, HeadSnapshotResponseDto};

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn coordinate(level: u32, slot: u16) -> BlockCoordinate {
        BlockCoordinate::new(level, slot).expect("test coordinate")
    }

    fn graph_view(policy: TrackingPolicy) -> GraphView {
        let child = hash(1);
        let parent = hash(2);
        let blue = hash(3);
        let red = hash(4);
        let child_coordinate = coordinate(2, 0);
        let parent_coordinate = coordinate(1, 7);
        let block = GraphBlock {
            hash: child,
            coordinate: child_coordinate,
            timestamp: u64::MAX,
            daa_score: 19,
            selected_parent_index: Some(0),
            direct_parents: Arc::from([parent]),
            blue_merge_set: Arc::from([parent, blue]),
            red_merge_set: Arc::from([red]),
            color: BlockColor::Blue,
            is_in_vspc: true,
        };
        let edge = GraphEdge { id: EdgeId { parent, child }, parent_coordinate, child_coordinate };

        GraphView::from_database_projection(
            2,
            LevelExtent::new(2, 2).expect("test extent"),
            policy,
            BTreeMap::from([(1, Level { size: 8, daa_score: Some(11) }), (2, Level { size: 1, daa_score: None })]),
            HashMap::from([(child, block)]),
            HashMap::from([(edge.id, edge)]),
        )
    }

    fn encoded_value(bytes: Vec<u8>) -> Value {
        let text = std::str::from_utf8(&bytes).expect("JSON is UTF-8");
        assert!(!text.contains(": "));
        assert!(!text.contains(", "));
        serde_json::from_slice(&bytes).expect("JSON value")
    }

    #[test]
    fn complete_graph_reuses_hashes_and_preserves_relationship_order() {
        let encoded = encoded_value(GraphDataDto::from_view(&graph_view(TrackingPolicy::Fixed)).encode().expect("graph JSON"));

        assert_eq!(encoded["hashes"], json!(["01".repeat(32), "02".repeat(32), "03".repeat(32), "04".repeat(32)]));
        assert_eq!(
            encoded["levels"],
            json!([
                { "level": 1, "size": 8, "daa_score": "11" },
                { "level": 2, "size": 1, "daa_score": null }
            ])
        );
        assert_eq!(encoded["blocks"].as_array().expect("blocks").len(), 1);
        assert_eq!(
            encoded["blocks"][0],
            json!({
                "hash": 0,
                "coordinate": { "level": 2, "slot": 0 },
                "timestamp": u64::MAX.to_string(),
                "daa_score": "19",
                "selected_parent_index": 0,
                "direct_parents": [1],
                "blue_merge_set": [1, 2],
                "red_merge_set": [3],
                "color": 1,
                "is_in_vspc": true
            })
        );
        assert_eq!(
            encoded["edges"][0],
            json!({
                "id": { "parent": 1, "child": 0 },
                "parent_coordinate": { "level": 1, "slot": 7 },
                "child_coordinate": { "level": 2, "slot": 0 }
            })
        );
    }

    #[test]
    fn genesis_selected_parent_is_explicit_null() {
        let genesis = hash(9);
        let block = GraphBlock {
            hash: genesis,
            coordinate: coordinate(1, 0),
            timestamp: 5,
            daa_score: 7,
            selected_parent_index: None,
            direct_parents: Arc::from([]),
            blue_merge_set: Arc::from([]),
            red_merge_set: Arc::from([]),
            color: BlockColor::Gray,
            is_in_vspc: true,
        };
        let view = GraphView::from_database_projection(
            1,
            LevelExtent::new(1, 1).expect("Genesis extent"),
            TrackingPolicy::Fixed,
            BTreeMap::from([(1, Level { size: 1, daa_score: Some(7) })]),
            HashMap::from([(genesis, block)]),
            HashMap::new(),
        );

        let encoded = encoded_value(GraphDataDto::from_view(&view).encode().expect("graph JSON"));
        assert_eq!(encoded["blocks"][0]["selected_parent_index"], Value::Null);
    }

    #[tokio::test]
    async fn head_snapshot_and_window_use_one_immutable_capture() {
        let publication =
            GraphPublication::new(GraphPublicationState::Live, Instant::now(), graph_view(TrackingPolicy::Head), GraphHistory::new())
                .expect("aligned publication");
        let capture = publication.capture_view().await;

        let snapshot = HeadSnapshotResponseDto::from_capture(&capture);
        let identity = snapshot.etag_input();
        assert_eq!(identity.publication_id(), capture.publication_id());
        assert_eq!(identity.revision(), 0);
        assert_eq!(identity.extent(), LevelExtent::new(2, 2).expect("test extent"));
        let encoded = encoded_value(snapshot.encode().expect("snapshot JSON"));
        assert_eq!(encoded["publication_id"], capture.publication_id().to_string());
        assert_eq!(encoded["revision"], "0");
        assert_eq!(encoded["revision_timestamp_ms"], "0");
        assert_eq!(encoded["low_level"], 2);
        assert_eq!(encoded["high_level"], 2);
        assert!(encoded.get("target_depth").is_none());
        assert!(encoded.get("state").is_none());

        let window = GraphWindowResponseDto::from_head(
            &capture,
            9,
            GraphWindowResolution { resolved_level: 2, effective_start_level: 2, effective_end_level: 2 },
        );
        let encoded = encoded_value(window.encode().expect("window JSON"));
        assert_eq!(
            encoded["source"],
            json!({
                "type": "head",
                "publication_id": capture.publication_id().to_string(),
                "revision": "0",
                "revision_timestamp_ms": "0",
                "head_high_level": 9
            })
        );
        assert_eq!(encoded["resolution"], json!({ "resolved_level": 2, "effective_start_level": 2, "effective_end_level": 2 }));
    }

    #[test]
    fn database_window_and_head_levels_have_no_synthetic_lineage() {
        let view = graph_view(TrackingPolicy::Fixed);
        let resolution = GraphWindowResolution { resolved_level: 2, effective_start_level: 2, effective_end_level: 2 };
        let encoded = encoded_value(GraphWindowResponseDto::from_database(&view, resolution).encode().expect("window JSON"));
        assert_eq!(encoded["source"], json!({ "type": "database" }));

        let levels = [
            HeadLevelValue { level: 2, value: Level { size: 1, daa_score: None } },
            HeadLevelValue { level: 1, value: Level { size: 8, daa_score: Some(11) } },
        ];
        let encoded = encoded_value(HeadLevelLookupResponseDto::from_complete(13, 17, &levels).encode().expect("levels JSON"));
        assert_eq!(encoded["publication_id"], "13");
        assert_eq!(encoded["revision"], "17");
        assert_eq!(
            encoded["levels"],
            json!([
                { "level": 2, "size": 1, "daa_score": null },
                { "level": 1, "size": 8, "daa_score": "11" }
            ])
        );
        assert!(encoded.get("hashes").is_none());
    }

    #[test]
    fn bounded_graph_scalars_emit_their_complete_numeric_domains() {
        let block_hash = hash(5);
        let coordinate = coordinate(u32::MAX, MAX_LEVEL_SLOT);
        let block = GraphBlock {
            hash: block_hash,
            coordinate,
            timestamp: u64::MAX,
            daa_score: MAX_DAA_SCORE,
            selected_parent_index: None,
            direct_parents: Arc::from([]),
            blue_merge_set: Arc::from([]),
            red_merge_set: Arc::from([]),
            color: BlockColor::Red,
            is_in_vspc: false,
        };
        let view = GraphView::from_database_projection(
            1,
            LevelExtent::new(u32::MAX, u32::MAX).expect("maximum-level extent"),
            TrackingPolicy::Fixed,
            BTreeMap::from([(u32::MAX, Level { size: MAX_LEVEL_SIZE, daa_score: Some(u64::MAX) })]),
            HashMap::from([(block_hash, block)]),
            HashMap::new(),
        );

        let encoded = encoded_value(GraphDataDto::from_view(&view).encode().expect("maximum graph JSON"));
        assert_eq!(encoded["levels"][0], json!({ "level": u32::MAX, "size": u16::MAX, "daa_score": u64::MAX.to_string() }));
        assert_eq!(encoded["blocks"][0]["coordinate"], json!({ "level": u32::MAX, "slot": MAX_LEVEL_SLOT }));
        assert_eq!(encoded["blocks"][0]["timestamp"], u64::MAX.to_string());
        assert_eq!(encoded["blocks"][0]["daa_score"], MAX_DAA_SCORE.to_string());
    }
}
