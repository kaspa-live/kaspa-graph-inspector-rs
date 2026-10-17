use kgi_api_model::{delta::GraphDelta, graph::Level, history::GraphHistoryEntryList};
use serde::Serialize;

use super::{
    dictionary::HashDictionary,
    graph::{GraphBlockDto, GraphEdgeDto},
    scalar::{BlockColorDto, BlockHashDto, HashRef, U64Dto},
    to_json_bytes,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct LevelValueDto {
    size: u16,
    daa_score: Option<U64Dto>,
}

impl From<Level> for LevelValueDto {
    fn from(value: Level) -> Self {
        Self { size: value.size, daa_score: value.daa_score.map(Into::into) }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct LevelMutationDto {
    level: u32,
    value: Option<LevelValueDto>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct FieldMutationDto<T> {
    hash: HashRef,
    value: T,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct GraphDeltaEntryDto {
    from_revision_id: U64Dto,
    to_revision_id: U64Dto,
    revision_timestamp_ms: U64Dto,
    head_level: u32,
    block_upserts: Vec<GraphBlockDto>,
    edge_upserts: Vec<GraphEdgeDto>,
    level_mutations: Vec<LevelMutationDto>,
    is_in_vspc_mutations: Vec<FieldMutationDto<bool>>,
    color_mutations: Vec<FieldMutationDto<BlockColorDto>>,
}

impl GraphDeltaEntryDto {
    fn from_delta(delta: &GraphDelta, hashes: &mut HashDictionary) -> Self {
        let block_upserts = delta
            .block_changes()
            .values()
            .filter_map(Option::as_ref)
            .map(|block| GraphBlockDto::from_graph_block(block, hashes))
            .collect();
        let edge_upserts =
            delta.edge_changes().values().filter_map(Option::as_ref).map(|edge| GraphEdgeDto::from_graph_edge(edge, hashes)).collect();
        let level_mutations = delta
            .level_changes()
            .iter()
            .map(|change| LevelMutationDto { level: change.level, value: change.after.map(Into::into) })
            .collect();
        let is_in_vspc_mutations = delta
            .is_in_vspc_changes()
            .iter()
            .map(|(&hash, change)| FieldMutationDto { hash: hashes.intern(hash), value: change.after })
            .collect();
        let color_mutations = delta
            .color_changes()
            .iter()
            .map(|(&hash, change)| FieldMutationDto { hash: hashes.intern(hash), value: change.after.into() })
            .collect();

        Self {
            from_revision_id: delta.from_revision_id().into(),
            to_revision_id: delta.to_revision_id().into(),
            revision_timestamp_ms: delta.revision_timestamp_ms().into(),
            head_level: delta.head_level(),
            block_upserts,
            edge_upserts,
            level_mutations,
            is_in_vspc_mutations,
            color_mutations,
        }
    }
}

/// Complete cacheable canonical-delta batch with one shared hash dictionary.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct GraphDeltaBatchDto {
    hashes: Vec<BlockHashDto>,
    deltas: Vec<GraphDeltaEntryDto>,
}

impl GraphDeltaBatchDto {
    /// Projects retained replay units without changing their canonical deltas.
    pub(crate) fn from_entries(entries: &GraphHistoryEntryList) -> Self {
        let mut hashes = HashDictionary::default();
        let deltas = entries.iter().map(|entry| GraphDeltaEntryDto::from_delta(entry.delta(), &mut hashes)).collect();
        Self { hashes: hashes.into_hashes(), deltas }
    }

    /// Serializes this delta batch as compact JSON.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(self)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
    };

    use kgi_api_model::{
        delta::{FieldChange, GraphDelta, LevelChange},
        graph::{EdgeId, GraphBlock, GraphEdge, Level},
        history::{GraphHistory, GraphHistoryRange},
    };
    use kgi_model::block::{BlockColor, BlockCoordinate, BlockHash};
    use serde_json::{Value, json};

    use super::GraphDeltaBatchDto;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn coordinate(level: u32, slot: u16) -> BlockCoordinate {
        BlockCoordinate::new(level, slot).expect("valid test coordinate")
    }

    fn block() -> GraphBlock {
        GraphBlock {
            hash: hash(1),
            coordinate: coordinate(4, 0),
            timestamp: 15,
            daa_score: 16,
            selected_parent_index: Some(0),
            direct_parents: Arc::from([hash(2)]),
            blue_merge_set: Arc::from([hash(2), hash(3)]),
            red_merge_set: Arc::from([hash(4)]),
            color: BlockColor::Gray,
            is_in_vspc: false,
        }
    }

    fn colored_block(byte: u8, color: BlockColor) -> GraphBlock {
        GraphBlock {
            hash: hash(byte),
            coordinate: coordinate(4, u16::from(byte - 1)),
            timestamp: u64::from(byte),
            daa_score: u64::from(byte),
            selected_parent_index: None,
            direct_parents: Arc::from([]),
            blue_merge_set: Arc::from([]),
            red_merge_set: Arc::from([]),
            color,
            is_in_vspc: false,
        }
    }

    fn edge(parent: u8, child: u8) -> GraphEdge {
        GraphEdge {
            id: EdgeId { parent: hash(parent), child: hash(child) },
            parent_coordinate: coordinate(3, 0),
            child_coordinate: coordinate(4, 0),
        }
    }

    fn encoded_value(dto: &GraphDeltaBatchDto) -> Value {
        let bytes = dto.encode().expect("delta JSON");
        let text = std::str::from_utf8(&bytes).expect("JSON is UTF-8");
        assert!(!text.contains(": "));
        assert!(!text.contains(", "));
        serde_json::from_slice(&bytes).expect("delta value")
    }

    fn entries_for(delta: GraphDelta) -> kgi_api_model::history::GraphHistoryEntryList {
        let target = delta.to_revision_id();
        let mut history = GraphHistory::new();
        history.append(delta, 1).expect("history entry");
        match history.range(0, target).expect("retained range") {
            GraphHistoryRange::Deltas { entries, .. } => entries,
            GraphHistoryRange::UpToDate { .. } => panic!("range contains a delta"),
        }
    }

    fn decoded_colors(value: &Value, field: &str) -> BTreeMap<String, u64> {
        let hashes = value["hashes"].as_array().expect("hash dictionary");
        value["deltas"][0][field]
            .as_array()
            .expect("color-bearing values")
            .iter()
            .map(|item| {
                let reference = item["hash"].as_u64().expect("hash reference") as usize;
                let color = item["color"].as_u64().expect("block color");
                (hashes[reference].as_str().expect("hash text").to_owned(), color)
            })
            .collect()
    }

    fn decoded_color_mutations(value: &Value) -> BTreeMap<String, u64> {
        let hashes = value["hashes"].as_array().expect("hash dictionary");
        value["deltas"][0]["color_mutations"]
            .as_array()
            .expect("color mutations")
            .iter()
            .map(|mutation| {
                let reference = mutation["hash"].as_u64().expect("hash reference") as usize;
                let color = mutation["value"].as_u64().expect("color value");
                (hashes[reference].as_str().expect("hash text").to_owned(), color)
            })
            .collect()
    }

    fn assert_no_internal_fields(value: &Value) {
        const FORBIDDEN: &[&str] = &[
            "compact_id",
            "tracking_policy",
            "estimated_raw_bytes",
            "cumulative_estimated_bytes",
            "generation_id",
            "max_depth",
            "target_depth",
            "publication_state",
            "state",
        ];

        match value {
            Value::Array(values) => values.iter().for_each(assert_no_internal_fields),
            Value::Object(fields) => {
                for (name, value) in fields {
                    assert!(!FORBIDDEN.contains(&name.as_str()), "internal field {name} reached delta JSON");
                    assert_no_internal_fields(value);
                }
            }
            _ => {}
        }
    }

    fn retained_entries() -> (GraphHistory, kgi_api_model::history::GraphHistoryEntryList) {
        let removed_block = hash(9);
        let removed_edge = EdgeId { parent: hash(8), child: removed_block };
        let first = GraphDelta::new(
            0,
            1,
            10,
            4,
            HashMap::from([(hash(1), Some(block())), (removed_block, None)]),
            HashMap::from([(edge(2, 1).id, Some(edge(2, 1))), (removed_edge, None)]),
            vec![
                LevelChange { level: 4, before: None, after: Some(Level { size: 1, daa_score: Some(16) }) },
                LevelChange { level: 3, before: Some(Level { size: 2, daa_score: Some(12) }), after: None },
            ],
            HashMap::from([(hash(1), FieldChange { before: false, after: true })]),
            HashMap::from([(hash(1), FieldChange { before: BlockColor::Gray, after: BlockColor::Blue })]),
        )
        .expect("first delta");
        let aggregate = GraphDelta::new(
            1,
            3,
            30,
            5,
            HashMap::new(),
            HashMap::new(),
            Vec::new(),
            HashMap::from([(hash(1), FieldChange { before: true, after: false })]),
            HashMap::from([(hash(3), FieldChange { before: BlockColor::Blue, after: BlockColor::Red })]),
        )
        .expect("aggregate delta");

        let mut history = GraphHistory::new();
        history.append(first, 100).expect("first history entry");
        history.append(aggregate, 200).expect("aggregate history entry");
        let entries = match history.range(0, 3).expect("retained range") {
            GraphHistoryRange::Deltas { entries, .. } => entries,
            GraphHistoryRange::UpToDate { .. } => panic!("range contains deltas"),
        };
        (history, entries)
    }

    #[test]
    fn batch_projects_target_values_and_shares_one_dictionary() {
        let (history, entries) = retained_entries();
        let encoded = encoded_value(&GraphDeltaBatchDto::from_entries(&entries));

        assert_eq!(encoded["hashes"], json!(["01".repeat(32), "02".repeat(32), "03".repeat(32), "04".repeat(32)]));
        assert_eq!(encoded["deltas"].as_array().expect("delta entries").len(), 2);
        assert_eq!(
            encoded["deltas"][0],
            json!({
                "from_revision_id": "0",
                "to_revision_id": "1",
                "revision_timestamp_ms": "10",
                "head_level": 4,
                "block_upserts": [{
                    "hash": 0,
                    "coordinate": { "level": 4, "slot": 0 },
                    "timestamp": "15",
                    "daa_score": "16",
                    "selected_parent_index": 0,
                    "direct_parents": [1],
                    "blue_merge_set": [1, 2],
                    "red_merge_set": [3],
                    "color": 0,
                    "is_in_vspc": false
                }],
                "edge_upserts": [{
                    "id": { "parent": 1, "child": 0 },
                    "parent_coordinate": { "level": 3, "slot": 0 },
                    "child_coordinate": { "level": 4, "slot": 0 }
                }],
                "level_mutations": [
                    { "level": 4, "value": { "size": 1, "daa_score": "16" } },
                    { "level": 3, "value": null }
                ],
                "is_in_vspc_mutations": [{ "hash": 0, "value": true }],
                "color_mutations": [{ "hash": 0, "value": 1 }]
            })
        );
        assert_eq!(
            encoded["deltas"][1],
            json!({
                "from_revision_id": "1",
                "to_revision_id": "3",
                "revision_timestamp_ms": "30",
                "head_level": 5,
                "block_upserts": [],
                "edge_upserts": [],
                "level_mutations": [],
                "is_in_vspc_mutations": [{ "hash": 0, "value": false }],
                "color_mutations": [{ "hash": 2, "value": 2 }]
            })
        );
        assert!(!encoded["hashes"].as_array().expect("hashes").contains(&json!("08".repeat(32))));
        assert!(!encoded["hashes"].as_array().expect("hashes").contains(&json!("09".repeat(32))));

        let first = history.entries().next().expect("first canonical entry").delta();
        assert_eq!(first.block_changes().get(&hash(9)), Some(&None));
        assert_eq!(first.edge_changes().get(&EdgeId { parent: hash(8), child: hash(9) }), Some(&None));
        assert_eq!(first.level_changes()[0].before, None);
        assert_eq!(first.level_changes()[1].before, Some(Level { size: 2, daa_score: Some(12) }));
        assert_eq!(first.is_in_vspc_changes().get(&hash(1)).map(|change| change.before), Some(false));
        assert_eq!(first.color_changes().get(&hash(1)).map(|change| change.before), Some(BlockColor::Gray));
        assert_no_internal_fields(&encoded);
    }

    #[test]
    fn empty_mutation_body_still_serializes_its_complete_interval() {
        let delta =
            GraphDelta::new(7, 9, u64::MAX, u32::MAX, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new())
                .expect("empty forward delta");
        let mut history = GraphHistory::new();
        history
            .append(
                GraphDelta::new(0, 7, 1, 1, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new())
                    .expect("prefix delta"),
                1,
            )
            .expect("prefix entry");
        history.append(delta, 1).expect("empty entry");
        let entries = match history.range(7, 9).expect("empty mutation range") {
            GraphHistoryRange::Deltas { entries, .. } => entries,
            GraphHistoryRange::UpToDate { .. } => panic!("range contains one delta"),
        };

        assert_eq!(
            encoded_value(&GraphDeltaBatchDto::from_entries(&entries)),
            json!({
                "hashes": [],
                "deltas": [{
                    "from_revision_id": "7",
                    "to_revision_id": "9",
                    "revision_timestamp_ms": u64::MAX.to_string(),
                    "head_level": u32::MAX,
                    "block_upserts": [],
                    "edge_upserts": [],
                    "level_mutations": [],
                    "is_in_vspc_mutations": [],
                    "color_mutations": []
                }]
            })
        );
    }

    #[test]
    fn unordered_delta_collections_preserve_decoded_colors_through_both_paths() {
        let encode = |reverse: bool| {
            let mut colors = vec![(1, BlockColor::Gray), (2, BlockColor::Blue), (3, BlockColor::Red)];
            if reverse {
                colors.reverse();
            }
            let block_changes = colors.iter().map(|&(byte, color)| (hash(byte), Some(colored_block(byte, color)))).collect();
            let color_changes =
                colors.into_iter().map(|(byte, color)| (hash(byte), FieldChange { before: BlockColor::Gray, after: color })).collect();
            let delta = GraphDelta::new(0, 1, 1, 4, block_changes, HashMap::new(), Vec::new(), HashMap::new(), color_changes)
                .expect("color delta");
            encoded_value(&GraphDeltaBatchDto::from_entries(&entries_for(delta)))
        };

        let first = encode(false);
        let second = encode(true);
        let expected = BTreeMap::from([("01".repeat(32), 0), ("02".repeat(32), 1), ("03".repeat(32), 2)]);

        assert_eq!(decoded_colors(&first, "block_upserts"), expected);
        assert_eq!(decoded_colors(&second, "block_upserts"), expected);
        assert_eq!(decoded_color_mutations(&first), expected);
        assert_eq!(decoded_color_mutations(&second), expected);
    }
}
