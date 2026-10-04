# Pinned Upstream Assumption Review: rusty-kaspa c338d495

Date: 2026-09-24

Upstream revision:
`c338d495bec29e4dc8b5149f99e8db6fa916ed4a`
(`Toccata cleanup - P2P follow-ups (#1101)`)

## Scope and method

This report applies the complete checklist in
`docs/architecture/verification.md` to the committed rusty-kaspa source at the
full SHA above. The local upstream checkout was clean and its `HEAD` matched
that SHA. Conclusions below are source-analysis results; no executable fixture
is required by the PUAR contract.

The stale-body-tip enumeration boundary is the accepted unverified risk named
by the verification contract and is outside this review.

Checklist item 8 was added by supplemental source analysis on 25 September
2026. That analysis inspected the same full committed source object; it does
not claim that the later local checkout HEAD still matched the pin.

## Result summary

| Item | Result |
| --- | --- |
| 1. Genesis discovery through `GetBlocks` | **Confirmed** |
| 2. Header-only `GetBlock` and low-hash recognition | **Confirmed** |
| 3. `GetBlocks` paging and Catchup fallback premises | **Confirmed** |
| 4. Virtual selected-sink change shapes | **Confirmed** |
| 5. `BlockAdded` duplicate and verbose-data behavior | **Confirmed** |
| 6. Local consensus-parameter resolution | **Confirmed** |
| 7. VSPC V2 batching and cursor behavior | **Confirmed** |
| 8. VSPC path/GetBlock selected-parent correlation | **Confirmed** |

No checklist item is `Not confirmed` or `Contradicted`. No architecture
escalation is required from this review.

## 1. Genesis discovery through `GetBlocks`

**Result: Confirmed.**

`RpcService::get_blocks_call` substitutes `self.config.genesis.hash` when
`request.low_hash` is `None`, prepends that low hash to the response, and
returns an empty `blocks` vector when `include_blocks` is false. The request
combination `(None, false, false)` is valid because only transactions without
blocks are rejected.

Evidence:

- `rpc/service/src/service.rs`, `RpcService::get_blocks_call`, lines 520-579:
  request validation, configured-Genesis substitution, inclusive low hash, and
  the `include_blocks` branch.

KGI impact and limitation: the first returned hash is the configured upstream
Genesis identity and no local Genesis constant is needed. This conclusion is
specific to the pinned revision.

## 2. Header-only `GetBlock` and low-hash recognition

**Result: Confirmed.**

The GetBlock path loads a block through
`get_block_even_if_header_only`. For a known header-only block, that method
returns the stored header with an empty transaction vector. Conversion then
loads GhostDAG data and constructs `RpcBlockVerboseData`, including the hash,
selected parent, blue score, and blue/red merge sets.

The `Some(low_hash)` branch of `get_blocks_call` recognizes a low hash by the
same successful GhostDAG lookup. Therefore a successful enriched header-only
GetBlock establishes the condition GetBlocks subsequently checks for that low
hash.

Evidence:

- `rpc/service/src/service.rs`, `RpcService::get_block_call`, lines 492-501.
- `consensus/src/consensus/mod.rs`,
  `Consensus::get_block_even_if_header_only`, lines 1427-1438, and
  `Consensus::get_ghostdag_data`, lines 1441-1448.
- `rpc/service/src/converter/consensus.rs`,
  `ConsensusConverter::get_block`, lines 61-100.
- `rpc/service/src/service.rs`, `RpcService::get_blocks_call`, lines 532-540.

KGI impact and limitation: a successful response supplies the material needed
for `MaterializedSyncAnchor`; KGI still owns validation that the returned hash
and required verbose fields are present and match its request.

## 3. `GetBlocks` paging and Catchup fallback premises

**Result: Confirmed.**

`get_blocks_call` uses a core budget of `mergeset_size_limit + 1`, obtains the
hashes between low and sink, prepends the inclusive low hash, and appends the
filtered sink anticone only when the core reaches the sink.

`SyncManager::antipast_hashes_between` walks selected-chain blocks forward.
For each chain step it emits `consensus_ordered_mergeset`, which is explicitly
the selected parent followed by the rest of the merge set in increasing blue
work order. It finally appends the highest selected-chain block reached. This
is the documented consensus-topological core order.

The short-page premise follows directly from the source bounds. Let `L` be the
merge-set limit. A below-sink capacity stop occurs only when
`blocks.len() + next_mergeset_size > L + 1`. Header validation guarantees
`next_mergeset_size <= L`, so `blocks.len() >= 2` at the stop. The algorithm
then appends the distinct `highest_reached` chain block, yielding at least
three normalized core blocks. Thus a normalized page shorter than three did
not stop below the sink because of the core capacity limit.

On a sink-reaching page, the sink ends the core and any filtered sink-anticone
members follow it. Virtual parent selection requires the sink's blue work to
exceed the remaining candidates; anticone traversal starts from those virtual
parents, and block ordering is `(blue_work, hash)`. Consequently, when an
anticone member is appended, the sink remains the page's global maximum but is
not the final element. This matches the global-maximum fallback premise.

Evidence:

- `rpc/service/src/service.rs`, `RpcService::get_blocks_call`, lines 542-564.
- `consensus/src/consensus/mod.rs`, `Consensus::get_hashes_between`, lines
  1308-1316, and `Consensus::get_anticone`, lines 1333-1338.
- `consensus/src/processes/sync/mod.rs`,
  `SyncManager::antipast_hashes_between`, lines 73-110.
- `consensus/src/model/stores/ghostdag.rs`, `GhostdagData::mergeset_size` and
  `GhostdagData::consensus_ordered_mergeset`, lines 111-113 and 172-180.
- `consensus/src/pipeline/header_processor/post_pow_validation.rs`,
  `HeaderProcessor::check_mergeset_size_limit`, lines 30-36.
- `consensus/src/pipeline/virtual_processor/processor.rs`,
  `VirtualProcessor::sink_search_algorithm`, lines 982-1045, and
  `VirtualProcessor::pick_virtual_parents`, especially lines 1048-1058.
- `consensus/src/processes/ghostdag/ordering.rs`, `SortableBlock::cmp`, lines
  38-41.

KGI impact and limitation: these predicates are one-way Catchup evidence under
the exact pinned implementation. A sink-reaching page with no appended
anticone member does not satisfy the global-maximum predicate merely by
reaching the sink; the other accepted transition paths remain necessary.

## 4. Virtual selected-sink change shapes

**Result: Confirmed.**

`calculate_chain_path` emits `removed` from the old sink backward to, but not
including, the common selected-chain ancestor. It emits `added` from immediately
after that ancestor toward the new sink. The limited form applies its bound
only to `added`; both forms preserve forward order for `added`.

The sink search examines current body tips and their ancestors by descending
`SortableBlock` order. The previous sink remains a valid reachable fallback,
so the selected sink cannot become a strict selected-chain ancestor of that
previous sink. The only shape that would have nonempty `removed` and empty
`added` is therefore excluded. A branch change has both sides nonempty.

`resolve_virtual` calculates the path even when `new_sink == prev_sink` and,
when subscribed, emits `VirtualChainChanged` without filtering an empty path.
It can therefore emit the documented fully empty no-op. Genesis is the common
ancestor at the bottom of every selected chain, and the path construction
excludes the common ancestor from both vectors, so Genesis never appears in
`added` or `removed`.

Evidence:

- `consensus/src/processes/traversal_manager.rs`,
  `DagTraversalManager::calculate_chain_path`, lines 34-59.
- `consensus/src/pipeline/virtual_processor/processor.rs`,
  `VirtualProcessor::resolve_virtual`, lines 309-318 and 348-374.
- `consensus/src/pipeline/virtual_processor/processor.rs`,
  `VirtualProcessor::sink_search_algorithm`, lines 982-1045.
- `consensus/src/processes/ghostdag/ordering.rs`, `SortableBlock::cmp`, lines
  38-41.

KGI impact and limitation: dropping the fully empty notification and rejecting
the nonempty-removed/empty-added shape are consistent with the pinned source.

## 5. `BlockAdded` duplicate and verbose-data behavior

**Result: Confirmed.**

Concurrent tasks for the same block hash are grouped and processed serially.
`BlockBodyProcessor::process_body` returns immediately when the body is already
present, before the notification call. The first successful body processing
commits its batch and then emits one `BlockAdded`; later processing of the same
body does not emit another notification.

RPC conversion normally enriches `BlockAdded` through
`ConsensusConverter::get_block`. If enrichment fails, the converter explicitly
falls back to `From<&Block> for RpcBlock`, whose `verbose_data` is `None`.

Evidence:

- `consensus/src/pipeline/deps_manager.rs`,
  `BlockTaskDependencyManager::register` and `end`, lines 183-211 and 239-264.
- `consensus/src/pipeline/body_processor/processor.rs`,
  `BlockBodyProcessor::process_body`, lines 178-221, and `commit_body`, lines
  234-250.
- `rpc/service/src/converter/consensus.rs`, `Converter::convert`, lines 700-707.
- `rpc/core/src/convert/block.rs`, `From<&Block> for RpcBlock`, lines 12-20.

KGI impact and limitation: treating a second BlockAdded for one hash within a
single active subscription as an invariant fault, while requiring recovery for
an Enabled notification without verbose data, matches the pinned behavior.

## 6. Local consensus-parameter resolution

**Result: Confirmed.**

`Params::from(NetworkId)` supports mainnet, testnet-10, devnet, and simnet. It
panics for a missing or unsupported testnet suffix. `Params::from(NetworkType::Testnet)`
returns the testnet-family defaults without that suffix check, which supports
KGI's prechecked unsupported-testnet fallback. Devnet and simnet resolve to
their defaults and `Params::override_params` replaces the complete block-rate
parameter group when `OverrideParams.blockrate` is present.

At this revision all default profiles above use 10 BPS block-rate parameters:

| KGI local resolution path | `bps` | `mergeset_size_limit` | `anticone_finalization_depth` |
| --- | ---: | ---: | ---: |
| Mainnet exact `NetworkId` | 10 | 248 | 591,258 |
| Testnet-10 exact `NetworkId` | 10 | 248 | 591,258 |
| Unsupported testnet suffix, testnet-family fallback | 10 | 248 | 591,258 |
| Devnet default | 10 | 248 | 591,258 |
| Simnet default | 10 | 248 | 591,258 |

The 10 BPS value uses target time 100 ms and GhostDAG `K = 124`.
The merge-set limit is `2K = 248`. The anticone finalization depth is:

```text
min(
    pruning_depth,
    finality_depth + merge_depth + 4 * mergeset_size_limit * K + 2 * K + 2
)
= min(1,080,000, 432,000 + 36,000 + 4 * 248 * 124 + 248 + 2)
= 591,258
```

For a devnet or simnet override, the same three public methods return values
from the resulting `Params`: `bps()` divides 1000 by the overridden target
milliseconds, `mergeset_size_limit()` returns the overridden limit, and
`anticone_finalization_depth()` applies the formula to the overridden
block-rate fields. `OverrideParams` rejects unknown fields during JSON
deserialization.

Evidence:

- `consensus/core/src/config/params.rs`, `BlockrateParams::new`, lines 193-207;
  `Params::bps`, lines 382-387; `Params::mergeset_size_limit`, lines 416-418;
  and `Params::anticone_finalization_depth`, lines 448-464.
- `consensus/core/src/config/params.rs`, `Params::override_params`, lines
  486-539; `From<NetworkType> for Params`, lines 556-564; `From<NetworkId> for
  Params`, lines 567-579; and the four default parameter constants, lines
  582-788.
- `consensus/core/src/config/bps.rs`, `Bps::ghostdag_k`, lines 35-45, and
  `Bps::mergeset_size_limit`, lines 75-85.
- `consensus/core/src/config/constants.rs`, finality, pruning, and merge-depth
  durations, lines 65-81.
- `rpc/core/src/model/message.rs`, `GetServerInfoResponse`, lines 2390-2400,
  and `rpc/service/src/service.rs`, `RpcService::get_server_info_call`, lines
  1311-1329.

KGI impact and limitation: GetServerInfo exposes the exact `NetworkId` but none
of these effective consensus values. The upstream daemon also permits an
override file on any non-mainnet network (`kaspad/src/daemon.rs`, lines
303-323), including testnet. KGI therefore cannot compare its selected local
defaults with a connected node's effective testnet, devnet, or simnet
overrides. The table is local source-derived configuration, not node-validated
state, exactly as the architecture requires.

## 7. VSPC V2 batching and cursor behavior

**Result: Confirmed.**

`get_virtual_chain_from_block_v2_call` uses an added-path and merged-block
budget of `10 * mergeset_size_limit`. It passes that limit to chain-path
construction, whose `removed` traversal is complete and whose limit applies
only to `added`. A `min_confirmation_count` of `None` bypasses head stripping.

An explicit `Some(RpcDataVerbosityLevel::None)` remains explicit rather than
falling back to `Full`. Its converted acceptance verbosity has both nested
sections present. At level `None`, header verbosity still includes the block
hash, so the converter does not take its no-acceptance-data early return and
produces a minimal chain acceptance entry for every returned complete prefix.

`get_blocks_acceptance_data` accumulates whole per-chain-block acceptance-data
entries and stops before the first entry that would exceed the merged-block
budget. The converter zips those entries with `chain_path.added`, and the RPC
handler truncates `added` to the resulting length. This can shorten only the
tail, preserving a complete prefix. A valid merge set is at most `L`, while
the budget is `10L`, so the first entry of a nonempty added path fits. At least
one added hash survives and, because chain-path construction excludes the
start/common ancestor, `added.last()` advances beyond the request cursor.

Evidence:

- `rpc/service/src/service.rs`,
  `RpcService::get_virtual_chain_from_block_v2_call`, lines 1345-1395.
- `consensus/src/processes/traversal_manager.rs`,
  `DagTraversalManager::calculate_chain_path`, lines 34-59.
- `rpc/core/src/convert/verbosity.rs`, conversions for
  `RpcHeaderVerbosity`, `RpcAcceptanceDataVerbosity`, and
  `RpcMergesetBlockAcceptanceDataVerbosity`, lines 41-45, 67-83, and 173-193.
- `rpc/service/src/converter/consensus.rs`,
  `ConsensusConverter::adapt_header_to_header_with_verbosity`, lines 202-235,
  and `get_chain_blocks_accepted_transactions`, lines 637-691.
- `consensus/src/consensus/mod.rs`, `Consensus::get_blocks_acceptance_data`,
  lines 1472-1497.
- `consensus/src/pipeline/header_processor/post_pow_validation.rs`,
  `HeaderProcessor::check_mergeset_size_limit`, lines 30-36.

KGI impact and limitation: the documented VSPC V2 request shape yields a
complete removed suffix, a complete added prefix, and an advancing cursor for
every valid nonempty response at the pinned revision. KGI still owns malformed
response validation and generation retirement.

## 8. VSPC path/GetBlock selected-parent correlation

**Result: Confirmed.**

Header processing inserts every block into the reachability structure using
that block's GhostDAG selected parent. VSPC chain-path construction walks the
resulting backward or forward selected chain. The consensus entry point holds
the pruning lock and accepts the low hash only when the retention root remains
on its chain, so the returned path remains within the retained chain segment.
GetBlock enrichment obtains the same block's GhostDAG data and exposes its
selected parent as `selected_parent_hash`. VSPC path adjacency and enriched
GetBlock therefore use the same selected-parent relation for a valid KGI query
at the pinned revision.

Evidence:

- `consensus/src/pipeline/header_processor/processor.rs`, header processing,
  lines 373-380.
- `consensus/src/processes/traversal_manager.rs`,
  `DagTraversalManager::calculate_chain_path`, lines 34-59.
- `consensus/src/consensus/mod.rs`,
  `Consensus::get_virtual_chain_from_block`, lines 846-865.
- `consensus/src/model/services/reachability.rs`, selected-chain iterator
  definitions, lines 134-160.
- `rpc/service/src/converter/consensus.rs`,
  `ConsensusConverter::get_block`, lines 61-83.

KGI impact and limitation: KGI can use an enriched GetBlock for the conflicting
child to attribute a persisted VSPC selected-parent mismatch. This correlation
is established only for the pinned revision; runtime attribution still handles
observable disagreement explicitly.

## Overall limitation

This PUAR establishes the eight assumptions only for rusty-kaspa
`c338d495bec29e4dc8b5149f99e8db6fa916ed4a`. It does not establish parity for
other revisions, custom builds, or a connected node's undisclosed effective
overrides.
