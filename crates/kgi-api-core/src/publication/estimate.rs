use std::collections::HashSet;

use kgi_api_model::delta::GraphDelta;
use kgi_api_model::history::GraphHistoryReplayability;
use kgi_model::block::BlockHash;

/// Soft raw-byte estimate at which an atomic delta becomes a replay barrier.
pub(crate) const SOFT_DELTA_ESTIMATED_BYTES: u64 = 4 * 1024 * 1024;

const ENTRY_AND_BATCH_FRAMING_BYTES: u64 = 230;
const DISTINCT_HASH_BYTES: u64 = 68;
const BLOCK_UPSERT_BYTES: u64 = 236;
const BLOCK_RELATIONSHIP_BYTES: u64 = 6;
const EDGE_UPSERT_BYTES: u64 = 152;
const LEVEL_INSTALLATION_BYTES: u64 = 68;
const LEVEL_REMOVAL_BYTES: u64 = 38;
const MEMBERSHIP_MUTATION_BYTES: u64 = 32;
const COLOR_MUTATION_BYTES: u64 = 25;

pub(super) fn estimate_raw_delta_bytes(delta: &GraphDelta) -> u64 {
    let mut hashes = HashSet::new();
    let mut block_upserts = 0;
    let mut block_relationships = 0;
    for block in delta.block_changes().values().filter_map(Option::as_ref) {
        block_upserts += 1;
        hashes.insert(block.hash);
        collect_hashes(&mut hashes, &block.direct_parents);
        collect_hashes(&mut hashes, &block.blue_merge_set);
        collect_hashes(&mut hashes, &block.red_merge_set);
        block_relationships += block.direct_parents.len() + block.blue_merge_set.len() + block.red_merge_set.len();
    }

    let mut edge_upserts = 0;
    for edge in delta.edge_changes().values().filter_map(Option::as_ref) {
        edge_upserts += 1;
        hashes.insert(edge.id.parent);
        hashes.insert(edge.id.child);
    }
    hashes.extend(delta.is_in_vspc_changes().keys().copied());
    hashes.extend(delta.color_changes().keys().copied());

    let level_installations = delta.level_changes().iter().filter(|change| change.after.is_some()).count();
    let level_removals = delta.level_changes().len() - level_installations;

    let mut estimate = ENTRY_AND_BATCH_FRAMING_BYTES;
    add_weighted(&mut estimate, hashes.len(), DISTINCT_HASH_BYTES);
    add_weighted(&mut estimate, block_upserts, BLOCK_UPSERT_BYTES);
    add_weighted(&mut estimate, block_relationships, BLOCK_RELATIONSHIP_BYTES);
    add_weighted(&mut estimate, edge_upserts, EDGE_UPSERT_BYTES);
    add_weighted(&mut estimate, level_installations, LEVEL_INSTALLATION_BYTES);
    add_weighted(&mut estimate, level_removals, LEVEL_REMOVAL_BYTES);
    add_weighted(&mut estimate, delta.is_in_vspc_changes().len(), MEMBERSHIP_MUTATION_BYTES);
    add_weighted(&mut estimate, delta.color_changes().len(), COLOR_MUTATION_BYTES);
    estimate
}

pub(super) const fn classify_replayability(estimated_raw_bytes: u64) -> GraphHistoryReplayability {
    if estimated_raw_bytes < SOFT_DELTA_ESTIMATED_BYTES {
        GraphHistoryReplayability::Replayable
    } else {
        GraphHistoryReplayability::ReplayBarrier
    }
}

fn collect_hashes(hashes: &mut HashSet<BlockHash>, values: &[BlockHash]) {
    hashes.extend(values.iter().copied());
}

fn add_weighted(estimate: &mut u64, count: usize, weight: u64) {
    let count = u64::try_from(count).expect("delta mutation count fits u64");
    let contribution = count.checked_mul(weight).expect("delta raw-byte estimate fits u64");
    *estimate = estimate.checked_add(contribution).expect("delta raw-byte estimate fits u64");
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::codec::delta::{GraphDeltaBatchDto, maximum_estimator_wire_contribution_bytes};
    use kgi_api_model::{
        delta::{FieldChange, GraphDelta, LevelChange},
        graph::{EdgeId, GraphBlock, GraphEdge, Level},
        history::{GraphHistory, GraphHistoryRange},
    };
    use kgi_model::block::{BlockColor, BlockCoordinate, MAX_DAA_SCORE, MAX_LEVEL_SIZE, MAX_LEVEL_SLOT};

    use super::*;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    #[test]
    fn estimator_deduplicates_emitted_hashes_and_ignores_wire_omissions() {
        let block = GraphBlock {
            hash: hash(1),
            coordinate: BlockCoordinate::new(3, 0).expect("positive coordinate"),
            timestamp: 4,
            daa_score: 5,
            selected_parent_index: Some(0),
            direct_parents: Arc::from([hash(2), hash(3)]),
            blue_merge_set: Arc::from([hash(3), hash(4)]),
            red_merge_set: Arc::from([hash(4), hash(5)]),
            color: BlockColor::Blue,
            is_in_vspc: true,
        };
        let edge = GraphEdge {
            id: EdgeId { parent: hash(2), child: hash(1) },
            parent_coordinate: BlockCoordinate::new(2, 0).expect("positive coordinate"),
            child_coordinate: BlockCoordinate::new(3, 0).expect("positive coordinate"),
        };
        let removed_edge = EdgeId { parent: hash(8), child: hash(9) };
        let delta = GraphDelta::new(
            0,
            1,
            1,
            3,
            HashMap::from([(hash(1), Some(block)), (hash(7), None)]),
            HashMap::from([(edge.id, Some(edge)), (removed_edge, None)]),
            vec![
                LevelChange { level: 2, before: None, after: Some(Level { size: 1, daa_score: None }) },
                LevelChange { level: 1, before: Some(Level { size: 1, daa_score: None }), after: None },
            ],
            HashMap::from([(hash(3), FieldChange { before: false, after: true })]),
            HashMap::from([(hash(6), FieldChange { before: BlockColor::Gray, after: BlockColor::Red })]),
        )
        .expect("valid delta");

        assert_eq!(estimate_raw_delta_bytes(&delta), 1_225);
    }

    #[test]
    fn empty_canonical_delta_still_carries_entry_framing() {
        let delta = GraphDelta::new(2, 3, 4, 5, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new())
            .expect("valid empty delta");

        assert_eq!(estimate_raw_delta_bytes(&delta), ENTRY_AND_BATCH_FRAMING_BYTES);
    }

    #[test]
    fn replayability_boundary_is_exact() {
        assert_eq!(classify_replayability(SOFT_DELTA_ESTIMATED_BYTES - 1), GraphHistoryReplayability::Replayable);
        assert_eq!(classify_replayability(SOFT_DELTA_ESTIMATED_BYTES), GraphHistoryReplayability::ReplayBarrier);
        assert_eq!(classify_replayability(SOFT_DELTA_ESTIMATED_BYTES + 1), GraphHistoryReplayability::ReplayBarrier);
    }

    #[test]
    fn estimator_bound_covers_reachable_maximum_wire_widths() {
        let maximum_distinct_hashes_below_soft_limit =
            (SOFT_DELTA_ESTIMATED_BYTES - 1 - ENTRY_AND_BATCH_FRAMING_BYTES) / DISTINCT_HASH_BYTES;
        assert!(
            maximum_distinct_hashes_below_soft_limit <= 99_999,
            "every admitted dense HashRef fits the representative five-digit width"
        );

        let wire = maximum_estimator_wire_contribution_bytes();
        let assert_contribution = |name: &str, actual: usize, estimated: u64| {
            let bound = usize::try_from(estimated.checked_mul(4).expect("estimator proof bound fits u64"))
                .expect("estimator proof bound fits usize");
            assert!(actual <= bound, "{name} needs {actual} bytes but its factor-four estimator bound is {bound}");
        };
        // The first framing value includes the outer batch. The incremental
        // value proves every later entry, so the same bound composes for any
        // batch length. Global dictionary reuse only reduces bytes relative to
        // the estimator's per-entry distinct-hash charges.
        assert_contribution("first entry and batch framing", wire.entry_and_batch_framing, ENTRY_AND_BATCH_FRAMING_BYTES);
        assert_contribution("additional entry framing", wire.additional_entry_framing, ENTRY_AND_BATCH_FRAMING_BYTES);
        assert_contribution("distinct hash", wire.distinct_hash, DISTINCT_HASH_BYTES);
        assert_contribution("block upsert", wire.block_upsert, BLOCK_UPSERT_BYTES);
        assert_contribution("block relationship", wire.block_relationship, BLOCK_RELATIONSHIP_BYTES);
        assert_contribution("edge upsert", wire.edge_upsert, EDGE_UPSERT_BYTES);
        assert_contribution("level installation", wire.level_installation, LEVEL_INSTALLATION_BYTES);
        assert_contribution("level removal", wire.level_removal, LEVEL_REMOVAL_BYTES);
        assert_contribution("membership mutation", wire.membership_mutation, MEMBERSHIP_MUTATION_BYTES);
        assert_contribution("color mutation", wire.color_mutation, COLOR_MUTATION_BYTES);

        let child = hash(1);
        let parent = hash(2);
        let blue = hash(3);
        let red = hash(4);
        let edge = GraphEdge {
            id: EdgeId { parent, child },
            parent_coordinate: BlockCoordinate::new(u32::MAX - 1, MAX_LEVEL_SLOT).expect("maximum parent coordinate"),
            child_coordinate: BlockCoordinate::new(u32::MAX, MAX_LEVEL_SLOT).expect("maximum child coordinate"),
        };
        let block = GraphBlock {
            hash: child,
            coordinate: edge.child_coordinate,
            timestamp: u64::MAX,
            daa_score: MAX_DAA_SCORE,
            selected_parent_index: Some(0),
            direct_parents: Arc::from([parent]),
            blue_merge_set: Arc::from([blue]),
            red_merge_set: Arc::from([red]),
            color: BlockColor::Red,
            is_in_vspc: true,
        };
        let delta = GraphDelta::new(
            0,
            1,
            u64::MAX,
            u32::MAX,
            HashMap::from([(child, Some(block))]),
            HashMap::from([(edge.id, Some(edge))]),
            vec![
                LevelChange {
                    level: u32::MAX,
                    before: None,
                    after: Some(Level { size: MAX_LEVEL_SIZE, daa_score: Some(MAX_DAA_SCORE) }),
                },
                LevelChange {
                    level: u32::MAX - 1,
                    before: Some(Level { size: MAX_LEVEL_SIZE, daa_score: Some(MAX_DAA_SCORE) }),
                    after: None,
                },
            ],
            HashMap::from([(blue, FieldChange { before: false, after: true })]),
            HashMap::from([(red, FieldChange { before: BlockColor::Blue, after: BlockColor::Red })]),
        )
        .expect("maximum-width wire values form an atomic delta");
        let estimate = estimate_raw_delta_bytes(&delta);
        assert!(estimate < SOFT_DELTA_ESTIMATED_BYTES, "the maximum-width fixture is admitted by the estimator");

        let mut history = GraphHistory::new();
        history.append(delta, estimate, GraphHistoryReplayability::Replayable).expect("maximum-width delta appends to history");
        let entries = match history.range(0, 1).expect("maximum-width entry is retained") {
            GraphHistoryRange::Deltas { entries, .. } => entries,
            GraphHistoryRange::UpToDate { .. } => panic!("the requested range contains one entry"),
        };
        let encoded_bytes = GraphDeltaBatchDto::from_entries(&entries).encode().expect("maximum-width delta encodes").len();
        // History starts at revision zero, so account explicitly for widening
        // both one-digit lineage boundaries to their reachable u64 maximum.
        let maximum_width_encoded_bytes =
            encoded_bytes.checked_add(2 * (u64::MAX.to_string().len() - 1)).expect("test size fits usize");
        let implementation_bound = estimate.checked_mul(4).expect("test estimate fits u64");

        assert!(u64::try_from(maximum_width_encoded_bytes).expect("encoded length fits u64") <= implementation_bound);
    }
}
