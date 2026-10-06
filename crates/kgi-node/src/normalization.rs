use std::sync::Arc;

use kaspa_consensus_core::blockhash::ORIGIN;
use kaspa_rpc_core::{GetBlocksResponse, GetVirtualChainFromBlockV2Response, RpcBlock};
use kgi_model::{
    block::{BlockHash, MAX_BLUE_SCORE, MAX_DAA_SCORE, ValidatedNodeBlock, ValidatedRecoveryHeader},
    lifecycle::{MalformedVspcResponseReason, RecoveryInputKind, ScoreRangeFault},
    vspc::VspcChange,
};

use crate::rpc::CatchupSinkSample;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseNormalizationError {
    ScoreOutOfRange(ScoreRangeFault),
    Malformed(RecoveryInputKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlockNormalizationError {
    ScoreOutOfRange(ScoreRangeFault),
    MissingVerboseData,
    SelectedParentNotDirect,
}

pub(crate) fn discover_genesis(response: &GetBlocksResponse) -> Result<BlockHash, GenesisDiscoveryError> {
    response.block_hashes.first().copied().ok_or(GenesisDiscoveryError::EmptyHashVector)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GenesisDiscoveryError {
    EmptyHashVector,
}

pub(crate) fn normalize_pruning_point_block(
    advertised_hash: BlockHash,
    block: RpcBlock,
    genesis_hash: BlockHash,
) -> Result<ValidatedNodeBlock, ResponseNormalizationError> {
    if advertised_hash == ORIGIN || block.header.hash != advertised_hash {
        return Err(malformed(RecoveryInputKind::MalformedPruningPointResponse));
    }
    normalize_full_block(block, genesis_hash)
        .map_err(|error| classify_block_error(error, RecoveryInputKind::MalformedPruningPointResponse))
}

pub(crate) fn normalize_catchup_sink_sample(
    advertised_hash: BlockHash,
    block: &RpcBlock,
) -> Result<CatchupSinkSample, ResponseNormalizationError> {
    if advertised_hash == ORIGIN || block.header.hash != advertised_hash {
        return Err(malformed(RecoveryInputKind::MalformedCatchupSinkResponse));
    }
    check_score(block.header.daa_score, MAX_DAA_SCORE, ScoreRangeFault::DaaScore)?;
    Ok(CatchupSinkSample { hash: block.header.hash, daa_score: block.header.daa_score })
}

pub(crate) fn normalize_get_blocks(
    low_hash: BlockHash,
    response: GetBlocksResponse,
    genesis_hash: BlockHash,
) -> Result<Vec<ValidatedNodeBlock>, ResponseNormalizationError> {
    let mut blocks = response.blocks.into_iter();
    let Some(anchor) = blocks.next() else {
        return Err(malformed(RecoveryInputKind::MalformedGetBlocks));
    };
    if anchor.header.hash != low_hash {
        return Err(malformed(RecoveryInputKind::MalformedGetBlocks));
    }

    let normalized = blocks
        .map(|block| {
            normalize_full_block(block, genesis_hash)
                .map_err(|error| classify_block_error(error, RecoveryInputKind::MalformedGetBlocks))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if normalized.last().is_some_and(|block| block.hash == low_hash) {
        return Err(malformed(RecoveryInputKind::MalformedGetBlocks));
    }
    Ok(normalized)
}

pub(crate) fn normalize_vspc_response(
    low_hash: BlockHash,
    response: &GetVirtualChainFromBlockV2Response,
) -> Result<VspcChange, ResponseNormalizationError> {
    let removed = response.removed_chain_block_hashes.as_slice();
    let added = response.added_chain_block_hashes.as_slice();
    let reason = if !removed.is_empty() && added.is_empty() {
        Some(MalformedVspcResponseReason::RemovedChainWithoutAddedPath)
    } else if removed.first().is_some_and(|hash| *hash != low_hash) {
        Some(MalformedVspcResponseReason::RemovedSourceMismatch)
    } else if added.last() == Some(&low_hash) {
        Some(MalformedVspcResponseReason::NonAdvancingAddedCursor)
    } else {
        None
    };

    if let Some(reason) = reason {
        return Err(malformed(RecoveryInputKind::MalformedVspcResponse(reason)));
    }

    Ok(VspcChange { removed: Arc::from(removed), added: Arc::from(added) })
}

pub(crate) fn normalize_recovery_header(
    requested_hash: BlockHash,
    block: &RpcBlock,
    genesis_hash: BlockHash,
) -> Result<ValidatedRecoveryHeader, ResponseNormalizationError> {
    if block.header.hash != requested_hash {
        return Err(malformed(RecoveryInputKind::MalformedGetBlock));
    }
    check_score(block.header.daa_score, MAX_DAA_SCORE, ScoreRangeFault::DaaScore)?;
    let blue_score = if requested_hash == genesis_hash {
        0
    } else {
        check_score(block.header.blue_score, MAX_BLUE_SCORE, ScoreRangeFault::BlueScore)?;
        block.header.blue_score
    };

    Ok(ValidatedRecoveryHeader {
        hash: block.header.hash,
        daa_score: block.header.daa_score,
        blue_work: block.header.blue_work,
        blue_score,
    })
}

pub(crate) fn normalize_requested_full_block(
    requested_hash: BlockHash,
    block: RpcBlock,
    genesis_hash: BlockHash,
) -> Result<ValidatedNodeBlock, ResponseNormalizationError> {
    if block.header.hash != requested_hash {
        return Err(malformed(RecoveryInputKind::MalformedGetBlock));
    }
    normalize_full_block(block, genesis_hash).map_err(|error| classify_block_error(error, RecoveryInputKind::MalformedGetBlock))
}

fn normalize_full_block(block: RpcBlock, genesis_hash: BlockHash) -> Result<ValidatedNodeBlock, BlockNormalizationError> {
    let header = block.header;
    check_score(header.daa_score, MAX_DAA_SCORE, ScoreRangeFault::DaaScore).map_err(response_to_block_error)?;

    if header.hash == genesis_hash {
        return Ok(ValidatedNodeBlock {
            hash: header.hash,
            selected_parent: ORIGIN,
            direct_parents: Vec::new(),
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: header.timestamp,
            daa_score: header.daa_score,
            blue_score: 0,
            blue_work: header.blue_work,
        });
    }

    check_score(header.blue_score, MAX_BLUE_SCORE, ScoreRangeFault::BlueScore).map_err(response_to_block_error)?;
    let verbose = block.verbose_data.ok_or(BlockNormalizationError::MissingVerboseData)?;
    let direct_parents = header.parents_by_level.into_iter().next().unwrap_or_default();
    if !direct_parents.contains(&verbose.selected_parent_hash) {
        return Err(BlockNormalizationError::SelectedParentNotDirect);
    }

    Ok(ValidatedNodeBlock {
        hash: header.hash,
        selected_parent: verbose.selected_parent_hash,
        direct_parents,
        blue_merge_set: verbose.merge_set_blues_hashes,
        red_merge_set: verbose.merge_set_reds_hashes,
        timestamp: header.timestamp,
        daa_score: header.daa_score,
        blue_score: header.blue_score,
        blue_work: header.blue_work,
    })
}

fn check_score(value: u64, maximum: u64, fault: ScoreRangeFault) -> Result<(), ResponseNormalizationError> {
    if value > maximum { Err(ResponseNormalizationError::ScoreOutOfRange(fault)) } else { Ok(()) }
}

const fn response_to_block_error(error: ResponseNormalizationError) -> BlockNormalizationError {
    match error {
        ResponseNormalizationError::ScoreOutOfRange(fault) => BlockNormalizationError::ScoreOutOfRange(fault),
        ResponseNormalizationError::Malformed(_) => unreachable!(),
    }
}

const fn classify_block_error(error: BlockNormalizationError, malformed_kind: RecoveryInputKind) -> ResponseNormalizationError {
    match error {
        BlockNormalizationError::ScoreOutOfRange(fault) => ResponseNormalizationError::ScoreOutOfRange(fault),
        BlockNormalizationError::MissingVerboseData | BlockNormalizationError::SelectedParentNotDirect => {
            ResponseNormalizationError::Malformed(malformed_kind)
        }
    }
}

const fn malformed(kind: RecoveryInputKind) -> ResponseNormalizationError {
    ResponseNormalizationError::Malformed(kind)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaspa_consensus_core::{BlueWorkType, blockhash::ORIGIN};
    use kaspa_rpc_core::{GetBlocksResponse, GetVirtualChainFromBlockV2Response, RpcBlock, RpcBlockVerboseData, RpcHeader};
    use kgi_model::{
        block::{BlockHash as Hash, MAX_BLUE_SCORE, MAX_DAA_SCORE},
        lifecycle::{MalformedVspcResponseReason, RecoveryInputKind, ScoreRangeFault},
    };

    use super::{
        GenesisDiscoveryError, ResponseNormalizationError, discover_genesis, normalize_catchup_sink_sample, normalize_full_block,
        normalize_get_blocks, normalize_pruning_point_block, normalize_recovery_header, normalize_requested_full_block,
        normalize_vspc_response,
    };

    #[test]
    fn ordinary_full_block_preserves_consumed_sequences_and_ignores_redundancy() {
        let own = hash(1);
        let selected_parent = hash(2);
        let repeated_parent = hash(3);
        let mut block = rpc_block(own, vec![selected_parent, repeated_parent, selected_parent]);
        let verbose = block.verbose_data.as_mut().expect("verbose data");
        verbose.hash = hash(90);
        verbose.blue_score = u64::MAX;
        verbose.merge_set_blues_hashes = vec![own, hash(4), hash(4)];
        verbose.merge_set_reds_hashes = vec![hash(5), selected_parent, hash(5)];

        let normalized = normalize_full_block(block, hash(0)).expect("valid ordinary block");

        assert_eq!(normalized.hash, own);
        assert_eq!(normalized.selected_parent, selected_parent);
        assert_eq!(normalized.direct_parents, vec![selected_parent, repeated_parent, selected_parent]);
        assert_eq!(normalized.blue_merge_set, vec![own, hash(4), hash(4)]);
        assert_eq!(normalized.red_merge_set, vec![hash(5), selected_parent, hash(5)]);
        assert_eq!(normalized.blue_score, 12);
    }

    #[test]
    fn ordinary_full_block_requires_verbose_and_selected_parent_membership() {
        let mut missing_verbose = rpc_block(hash(1), vec![hash(2)]);
        missing_verbose.verbose_data = None;
        assert!(normalize_full_block(missing_verbose, hash(0)).is_err());

        let mut missing_parent = rpc_block(hash(1), vec![hash(2)]);
        missing_parent.verbose_data.as_mut().expect("verbose").selected_parent_hash = hash(3);
        assert!(normalize_full_block(missing_parent, hash(0)).is_err());
    }

    #[test]
    fn genesis_synthesizes_relationships_and_blue_score() {
        let genesis = hash(7);
        let mut block = rpc_block(genesis, vec![hash(2)]);
        block.header.blue_score = u64::MAX;
        block.header.timestamp = u64::MAX;
        block.verbose_data = None;

        let normalized = normalize_full_block(block, genesis).expect("canonical Genesis");

        assert_eq!(normalized.hash, genesis);
        assert_eq!(normalized.selected_parent, ORIGIN);
        assert!(normalized.direct_parents.is_empty());
        assert!(normalized.blue_merge_set.is_empty());
        assert!(normalized.red_merge_set.is_empty());
        assert_eq!(normalized.blue_score, 0);
        assert_eq!(normalized.timestamp, u64::MAX);
    }

    #[test]
    fn full_block_checks_only_consumed_score_fields() {
        let mut daa = rpc_block(hash(1), vec![hash(2)]);
        daa.header.daa_score = MAX_DAA_SCORE + 1;
        assert_eq!(
            normalize_requested_full_block(hash(1), daa, hash(0)),
            Err(ResponseNormalizationError::ScoreOutOfRange(ScoreRangeFault::DaaScore))
        );

        let mut blue = rpc_block(hash(1), vec![hash(2)]);
        blue.header.blue_score = MAX_BLUE_SCORE + 1;
        assert_eq!(
            normalize_requested_full_block(hash(1), blue, hash(0)),
            Err(ResponseNormalizationError::ScoreOutOfRange(ScoreRangeFault::BlueScore))
        );

        let mut genesis = rpc_block(hash(9), vec![]);
        genesis.header.blue_score = u64::MAX;
        assert!(normalize_requested_full_block(hash(9), genesis, hash(9)).is_ok());

        let mut maxima = rpc_block(hash(7), vec![hash(2)]);
        maxima.header.daa_score = MAX_DAA_SCORE;
        maxima.header.blue_score = MAX_BLUE_SCORE;
        assert!(normalize_requested_full_block(hash(7), maxima, hash(0)).is_ok());
    }

    #[test]
    fn genesis_discovery_uses_first_hash_and_ignores_blocks() {
        let response = GetBlocksResponse::new(vec![hash(1), hash(2)], vec![rpc_block(hash(9), vec![hash(8)])]);
        assert_eq!(discover_genesis(&response), Ok(hash(1)));
        assert_eq!(
            discover_genesis(&GetBlocksResponse::new(vec![], vec![rpc_block(hash(9), vec![])])),
            Err(GenesisDiscoveryError::EmptyHashVector)
        );
    }

    #[test]
    fn get_blocks_ignores_parallel_hashes_and_preserves_block_order() {
        let low = hash(1);
        let response = GetBlocksResponse::new(
            vec![hash(90)],
            vec![
                rpc_block(low, vec![]),
                rpc_block(hash(2), vec![hash(8)]),
                rpc_block(hash(3), vec![hash(8)]),
                rpc_block(hash(2), vec![hash(8)]),
            ],
        );
        let page = normalize_get_blocks(low, response, hash(0)).expect("valid page");
        assert_eq!(page.iter().map(|block| block.hash).collect::<Vec<_>>(), vec![hash(2), hash(3), hash(2)]);
    }

    #[test]
    fn get_blocks_accepts_anchor_only_and_rejects_framing_errors() {
        let low = hash(1);
        assert!(
            normalize_get_blocks(low, GetBlocksResponse::new(vec![], vec![rpc_block(low, vec![])]), hash(0))
                .expect("anchor only")
                .is_empty()
        );

        for response in [
            GetBlocksResponse::new(vec![], vec![]),
            GetBlocksResponse::new(vec![], vec![rpc_block(hash(2), vec![])]),
            GetBlocksResponse::new(vec![], vec![rpc_block(low, vec![]), rpc_block(low, vec![hash(8)])]),
        ] {
            assert_eq!(
                normalize_get_blocks(low, response, hash(0)),
                Err(ResponseNormalizationError::Malformed(RecoveryInputKind::MalformedGetBlocks))
            );
        }
    }

    #[test]
    fn get_blocks_rejects_whole_page_but_preserves_range_fault() {
        let low = hash(1);
        let mut malformed = rpc_block(hash(2), vec![hash(8)]);
        malformed.verbose_data = None;
        assert_eq!(
            normalize_get_blocks(low, GetBlocksResponse::new(vec![], vec![rpc_block(low, vec![]), malformed]), hash(0),),
            Err(ResponseNormalizationError::Malformed(RecoveryInputKind::MalformedGetBlocks))
        );

        let mut excessive = rpc_block(hash(2), vec![hash(8)]);
        excessive.header.daa_score = MAX_DAA_SCORE + 1;
        assert_eq!(
            normalize_get_blocks(low, GetBlocksResponse::new(vec![], vec![rpc_block(low, vec![]), excessive]), hash(0),),
            Err(ResponseNormalizationError::ScoreOutOfRange(ScoreRangeFault::DaaScore))
        );
    }

    #[test]
    fn vspc_checks_minimal_framing_in_stable_order() {
        let low = hash(1);
        let cases = [
            (vspc(vec![hash(9)], vec![]), MalformedVspcResponseReason::RemovedChainWithoutAddedPath),
            (vspc(vec![hash(9)], vec![low]), MalformedVspcResponseReason::RemovedSourceMismatch),
            (vspc(vec![], vec![hash(2), low]), MalformedVspcResponseReason::NonAdvancingAddedCursor),
        ];
        for (response, reason) in cases {
            assert_eq!(
                normalize_vspc_response(low, &response),
                Err(ResponseNormalizationError::Malformed(RecoveryInputKind::MalformedVspcResponse(reason)))
            );
        }
    }

    #[test]
    fn vspc_preserves_duplicates_intersections_and_nonfinal_low_hash() {
        let low = hash(1);
        let response = vspc(vec![low, hash(2), hash(2)], vec![hash(2), low, hash(3), hash(3)]);
        let normalized = normalize_vspc_response(low, &response).expect("trusted path properties");
        assert_eq!(normalized.removed.as_ref(), &[low, hash(2), hash(2)]);
        assert_eq!(normalized.added.as_ref(), &[hash(2), low, hash(3), hash(3)]);
    }

    #[test]
    fn recovery_header_normalizes_genesis_without_raw_blue_score() {
        let genesis = hash(1);
        for raw_blue_score in [0, 1, u64::MAX] {
            let mut block = rpc_block(genesis, vec![]);
            block.header.blue_score = raw_blue_score;
            block.verbose_data = None;
            let normalized = normalize_recovery_header(genesis, &block, genesis).expect("Genesis recovery header");
            assert_eq!(normalized.blue_score, 0);
        }
    }

    #[test]
    fn recovery_header_attributes_and_checks_ordinary_scores() {
        let requested = hash(1);
        let block = rpc_block(hash(2), vec![]);
        assert_eq!(
            normalize_recovery_header(requested, &block, hash(0)),
            Err(ResponseNormalizationError::Malformed(RecoveryInputKind::MalformedGetBlock))
        );

        let mut excessive = rpc_block(requested, vec![]);
        excessive.header.blue_score = MAX_BLUE_SCORE + 1;
        assert_eq!(
            normalize_recovery_header(requested, &excessive, hash(0)),
            Err(ResponseNormalizationError::ScoreOutOfRange(ScoreRangeFault::BlueScore))
        );

        let mut ordinary = rpc_block(requested, vec![]);
        ordinary.verbose_data = None;
        let normalized = normalize_recovery_header(requested, &ordinary, hash(0)).expect("ordinary header does not need verbose data");
        assert_eq!(normalized.blue_score, ordinary.header.blue_score);
    }

    #[test]
    fn pruning_point_and_sink_apply_source_specific_attribution() {
        let block = rpc_block(hash(2), vec![hash(8)]);
        assert_eq!(
            normalize_pruning_point_block(hash(3), block.clone(), hash(0)),
            Err(ResponseNormalizationError::Malformed(RecoveryInputKind::MalformedPruningPointResponse))
        );
        assert_eq!(
            normalize_catchup_sink_sample(hash(3), &block),
            Err(ResponseNormalizationError::Malformed(RecoveryInputKind::MalformedCatchupSinkResponse))
        );
        assert_eq!(
            normalize_catchup_sink_sample(ORIGIN, &block),
            Err(ResponseNormalizationError::Malformed(RecoveryInputKind::MalformedCatchupSinkResponse))
        );
    }

    fn hash(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    fn rpc_block(hash_value: Hash, direct_parents: Vec<Hash>) -> RpcBlock {
        let selected_parent = direct_parents.first().copied().unwrap_or_else(|| hash(99));
        RpcBlock {
            header: RpcHeader {
                hash: hash_value,
                version: 0,
                parents_by_level: vec![direct_parents],
                hash_merkle_root: hash(10),
                accepted_id_merkle_root: hash(11),
                utxo_commitment: hash(12),
                timestamp: 10,
                bits: 0,
                nonce: 0,
                daa_score: 11,
                blue_work: BlueWorkType::from(13_u64),
                blue_score: 12,
                pruning_point: hash(14),
            },
            transactions: Vec::new(),
            verbose_data: Some(RpcBlockVerboseData {
                hash: hash_value,
                difficulty: 1.0,
                selected_parent_hash: selected_parent,
                transaction_ids: Vec::new(),
                is_header_only: false,
                blue_score: 12,
                children_hashes: Vec::new(),
                merge_set_blues_hashes: Vec::new(),
                merge_set_reds_hashes: Vec::new(),
                is_chain_block: true,
            }),
        }
    }

    fn vspc(removed: Vec<Hash>, added: Vec<Hash>) -> GetVirtualChainFromBlockV2Response {
        GetVirtualChainFromBlockV2Response {
            removed_chain_block_hashes: Arc::new(removed),
            added_chain_block_hashes: Arc::new(added),
            chain_block_accepted_transactions: Arc::new(Vec::new()),
        }
    }
}
