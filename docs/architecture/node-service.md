# NodeService connectivity and notification routing

## Scope and ownership

This document owns node connectivity, validated RPC generations, remote
subscription state, notification routing, and node-response normalization.
The [processing lifecycle](processing-lifecycle.md) owns recovery coordination
after NodeService reports a typed fault.

## NodeService — settled

`NodeService` is a permanent autonomous lifecycle worker. It owns connection,
reconnection, validation, IBD waiting, and publication of usable connection
generations.

```text
NodeService lifecycle worker
    -> connect
    -> validate
    -> wait for node to leave IBD
    -> publish Arc<ValidatedRpcClient>
```

`ValidatedRpcClient` represents exactly one validated physical connection
lifetime. Processing code receives this capability, not `NodeService`.
Its `Arc` identity is the generation identity; no separate generation number
is required. A clone never resolves through NodeService to a newer connection.
Every composite operation, runtime RPC, and subscription started through the
handle remains bound to that physical generation.

```rust
enum NodeServiceState {
    Connecting,
    Ready(Arc<ValidatedRpcClient>),
    Unavailable(NodeUnavailableReason),
    Rejected(NodeRejection),
    Stopped,
}

enum NodeServiceStatusState {
    Connecting,
    Ready,
    Unavailable,
    Rejected,
    Stopped,
}

struct ValidatedNodeStatus {
    network_id: NetworkId,
    server_version: String,
    rpc_api_version: Option<u16>,
    rpc_api_revision: Option<u16>,
}

struct NodeServiceStatus {
    state: NodeServiceStatusState,
    last_validated: Option<ValidatedNodeStatus>,
}

enum NodeServiceEvent {
    RpcRetired(Arc<ValidatedRpcClient>),
    RpcPublished(Arc<ValidatedRpcClient>),
    Rejected(NodeRejection),
}

impl NodeService {
    async fn shutdown(&self) -> Result<(), NodeServiceError>;
}
```

`NodeServiceState` and published status outlive individual validated client
generations. Connection validation precedes publication of every generation.
NodeService autonomously connects, validates, retires, reconnects, and
republishes without a request from Supervisor or ResyncEngine. It reports every
generation transition through one reliable ordered `NodeServiceEvent` stream.
The event path must be installed before NodeService can publish its initial
generation or enter `Rejected`.

`RpcPublished` carries the newly usable exact generation. `RpcRetired` carries
the exact generation that ceased to be usable. Repeated failure reports for an
already retired handle emit no duplicate retirement event or replacement
attempt. A replacement is newly validated, never republishes a retired `Arc`,
and is published only after retirement of the previous usable generation.
`Rejected` reports permanent validation or configuration rejection. Entering
`Rejected` retires any currently published generation, emits its retirement
first, and then emits `Rejected`. For an operation-detected generation failure,
NodeService completes retirement and enqueues `RpcRetired` before returning the
typed operation result to its caller. Events report lifecycle transitions;
they do not initiate reconnection.

Operation completion and retirement have one linearized result. An operation
that completes while its exact generation remains valid may return that
generation's result. If retirement wins first, the operation returns generation
loss or cancellation and never retries or completes through a replacement.
Retirement prevents new work through every clone. A fault or retirement report
always names the exact `Arc`, so a late report from an older generation cannot
retire or clear its replacement.

`shutdown` is terminal and idempotent; successful completion means
NodeService is `Stopped` and has released its owned connection resources.

`NodeServiceStatus` is the component observation consumed by the
[API status contract](api-service.md#status-observation--settled). It never carries
`ValidatedRpcClient`. Initially `last_validated` is `None`. Every
successful validation replaces it before publishing `Ready`; leaving `Ready`
preserves it through `Connecting`, `Unavailable`, `Rejected`, and `Stopped`.
In `Ready` it describes the current connection, while every other state
identifies it as the last successfully validated node. The observation is
process-local and is never persisted.

Transient connection, transport, and validation-RPC failures enter
`Unavailable` and are retried indefinitely with nominal delays:

```text
1s, 2s, 4s, 8s, 16s, 30s, 30s, ...
```

Each actual delay uses equal jitter from 50% through 100% of the nominal
delay, and every wait is shutdown-cancellable. Reset the sequence only after
NodeService has remained continuously Ready for 60 seconds; opening a
connection alone does not reset it. Network mismatch, incompatible RPC API,
and missing required notification capabilities publish terminal `Rejected`
and are not retried under unchanged configuration. Invalid local consensus
parameter configuration fails startup as defined below.

Validation requires:

- exact configured Kaspa network type and suffix;
- discovery of the node's configured Genesis hash through the validated RPC
  connection;
- compatible RPC API version/revision;
- `handle_stop_notify() == true`;
- `handle_message_id() == true`;
- node is not in IBD when the handle is published;
- gRPC automatic reconnect is disabled.

### RPC API compatibility

NodeService derives its required RPC API pair from the same compiled
`kaspa-rpc-core` dependency used by KGI:

```rust
use kaspa_rpc_core::api::ops::{RPC_API_REVISION, RPC_API_VERSION};

fn is_rpc_api_compatible(remote_version: u16, remote_revision: u16) -> bool {
    remote_version == RPC_API_VERSION && remote_revision >= RPC_API_REVISION
}
```

The API version is an exact compatibility boundary: a lower or higher remote
version is incompatible. Within that version, the revision is a
backward-compatible capability floor. A node at KGI's compiled revision or a
newer revision is compatible; an older revision may lack behavior required by
KGI and is incompatible. This follows the upstream distinction between an API
version change, which requires connection refusal, and a revision change,
which denotes a backward-compatible extension. KGI does not duplicate either
constant as a local literal.

Both remote values are mandatory inputs from the completed server-information
response. If either value is absent or cannot be represented, validation
rejects the connection as an incompatible RPC API. An opaque RPC-call or
generation failure before a complete response remains a transient validation
failure and does not invent observed values. Every incompatible result enters
the terminal `Rejected` state, publishes no `ValidatedRpcClient`, and records
the required and observed pair in diagnostic context. Successful validation
stores and exposes the observed remote values rather than substituting KGI's
local constants.

Legacy Go kaspad notification semantics are deliberately unsupported.

`ValidatedNodeInfo` copies only KGI-relevant values rather than retaining the
full rusty-kaspa params object:

```rust
struct ValidatedNodeInfo {
    network_id: NetworkId,
    genesis_hash: BlockHash,
    server_version: String,
    rpc_api_version: Option<u16>,
    rpc_api_revision: Option<u16>,
    consensus: KgiConsensusParams,
}

struct KgiConsensusParams {
    bps: u64,
    mergeset_size_limit: u64,
    anticone_finalization_depth: u64,
    /// KGI recovery threshold derived from the fields above; not a consensus
    /// parameter supplied by rusty-kaspa or the connected node.
    catchup_max_daa_gap: u64,
}
```

Add fields only when KGI behavior actually depends on them.

### Consensus parameter resolution

The public RPC surface does not expose the node's effective consensus
parameters. `NetworkId` and the RPC-discovered Genesis hash therefore validate
network identity, but do not prove that the node uses KGI's local parameter
values. `KgiConsensusParams` is an explicit local assumption used for recovery,
not a node-validated property.

After validating the exact `NetworkId`, resolve local rusty-kaspa `Params` as
follows:

```text
mainnet
    -> Params::from(network_id)

locally supported testnet NetworkId
    -> Params::from(network_id)
    -> warn and continue

unsupported testnet suffix
    -> Params::from(NetworkType::Testnet)
    -> warn and continue

devnet or simnet with a configured override-params file
    -> Params::from(network_id)
    -> parse rusty-kaspa OverrideParams from path
    -> Params::override_params(overrides)
    -> warn that equality with the node cannot be verified

devnet or simnet without an override-params file
    -> Params::from(network_id)
    -> warn that node overrides cannot be detected
```

At the pinned rusty-kaspa revision, `Params::from(NetworkId)` panics for an
unsupported testnet suffix rather than returning an error. The resolver checks
local support before calling it and uses the testnet-family fallback only for
an unsupported testnet suffix; panic catching is not control flow.

The resolved `override_params_file` setting from the
[process configuration contract](overview.md#process-configuration-and-command-entry--settled)
is valid only for configured devnet or simnet. It uses the same JSON
`OverrideParams` format as rusty-kaspa and is loaded once at process startup.
It is explicitly unsupported for mainnet and every testnet suffix. An
explicitly supplied file that is unreadable, malformed, or incompatible, or
use of the setting with an unsupported network, is a configuration error and
never falls back silently. The file is not persisted in database metadata.
Genesis remains RPC-discovered and is not taken from the file.

After applying any override, inspect the resolved raw `BlockrateParams` before
calling a derived rusty-kaspa method. The parameter set is admissible only
when all of these checks succeed:

```text
1 <= target_time_per_block <= 1000 milliseconds
mergeset_size_limit >= 2

checked(mergeset_size_limit + 1)
checked(10 * mergeset_size_limit)

checked(
    finality_depth
    + merge_depth
    + 4 * mergeset_size_limit * ghostdag_k
    + 2 * ghostdag_k
    + 2
)
```

The target-time bound prevents division by zero and a zero result from
rusty-kaspa's integer `bps()` calculation. The merge-set lower bound preserves
the lifecycle's normalized-page-length Catchup fallback. The two merge-set
operations protect the GetBlocks core budget and VSPC V2 added batch premise.
The checked anticone expression is an admissibility preflight for the pinned
upstream method, not an independently selected consensus formula.

Only after that preflight, obtain `bps`, `mergeset_size_limit`, and
`anticone_finalization_depth` from the resolved `Params` through `bps()`,
`mergeset_size_limit()`, and `anticone_finalization_depth()`. Construct the
KGI-owned threshold with checked arithmetic:

```text
catchup_max_daa_gap = max(
    checked(30 * bps),
    checked(mergeset_size_limit + 1),
)
```

Require `anticone_finalization_depth <= MAX_BLUE_SCORE` and
`catchup_max_daa_gap <= MAX_DAA_SCORE`. The resulting threshold provides at
least one complete GetBlocks core page of transition granularity. Its values
are 181 for the standard 1 BPS profile, 300 for the standard 10 BPS profile,
and 1,500 for a 50 BPS override with `mergeset_size_limit = 512`.

Any failed bound or checked operation returns this typed startup configuration
error:

```rust
enum ConsensusParameter {
    TargetTimePerBlock,
    MergeSetSizeLimit,
    GetBlocksCoreBudget,
    VspcV2AddedBatchSize,
    AnticoneFinalizationDepth,
    CatchupMaxDaaGap,
}

enum ConsensusParameterReason {
    BelowMinimum,
    AboveMaximum,
    ArithmeticOverflow,
}

struct InvalidConsensusParameters {
    parameter: ConsensusParameter,
    reason: ConsensusParameterReason,
}
```

The error publishes no `KgiConsensusParams` or validated client, does not enter
NodeService's transient retry loop, and never falls back to defaults. The
offending value or arithmetic operands belong in diagnostics and do not drive
control flow.

Every non-mainnet network emits a divergence warning that logs the exact
`NetworkId`, the parameter source, and all four `KgiConsensusParams` values,
then processing continues. Mainnet does not emit this warning because the
official rusty-kaspa daemon rejects parameter overrides there. The
[PUAR](verification.md#current-puar-result) checks local resolution and values
at the reference revision. It does not claim equality with a connected node or
custom build. Correct processing requires the node's effective values to match
the selected local values; divergence is an accepted operator risk rather than
a detectable runtime rejection.

### Genesis discovery

Genesis identity comes from the node, not from KGI's local consensus
parameters. This keeps identity correct for custom devnets and new network
suffixes whose Genesis hash KGI may not know. It does not validate the local
consensus parameter assumption above.

During validation of each physical RPC generation, NodeService makes this raw
request outside the ordinary normalized block pump:

```text
GetBlocks {
    low_hash: None,
    include_blocks: false,
    include_transactions: false,
}
```

KGI relies on `None` selecting the node's configured Genesis as the low hash
and the first returned `block_hashes` member being that Genesis. The
[PUAR](verification.md#current-puar-result) checks this upstream assumption
against the reference revision. Validation requires a nonempty hash vector and
copies its first hash into `ValidatedNodeInfo.genesis_hash`. The block vector is
unused and ignored. An opaque RPC-call failure or an empty hash vector fails
that validation attempt; NodeService never guesses or substitutes a locally
known Genesis hash.

This discovery call is deliberately distinct from `get_blocks` normalization
below. It neither constructs a synchronization page nor applies the explicit
inclusive-low-hash contract used by the pump.

### NotificationRouter

`NotificationRouter` implements rusty-kaspa's `Notify` trait and owns the two
bounded senders to the processors.

Router states:

```text
Disabled | Enabled | Retired
```

- Disabled drops notifications immediately.
- Enabled routes with nonblocking bounded sends.
- A full destination channel means notification loss: enqueue nothing, disable
  both streams, and report `SessionContinuityLost`. The shared
  [session-channel policy](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
  owns its disposition; disabling both streams is a router recovery action and
  does not assert loss on both streams.
- An Enabled `BlockAdded` is normalized to `ValidatedNodeBlock` before bounded
  delivery. On failure, disable both streams and enqueue no block. A score-range
  failure retains its `ScoreOutOfRange` classification; any other intrinsic
  normalization failure reports
  `NotificationInputInvalid(MalformedBlockAdded)`. The
  [processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
  owns both dispositions.
- Before attempting the bounded VSPC send, validate raw
  VirtualChainChanged notification structure:
  1. Empty `removed` and empty `added` is the valid upstream no-op. Discard it;
     it consumes no channel capacity, earns no overlap credit, changes no
     committed state, and requests no recovery.
  2. Nonempty `removed` with empty `added` is
     `RemovedChainWithoutAddedPath`.

  For case 2, enqueue nothing, disable both streams, and report
  `NotificationInputInvalid(MalformedVspcChange(reason))`. This rejection makes
  no attempt to derive a destination from an incomplete transition. The
  [processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
  owns the typed fault's disposition.
- Retired never routes again.

The subscription state belongs to `ValidatedRpcClient` and cannot outlive its
connection. The handle is clonable through `Arc` and protects notification
state with a private mutex.
The router preserves the node's ordered `removed` and `added` vectors without
duplicate-member or removed/added-intersection validation. Those are trusted
Kaspa path properties under the
[node trust boundary](overview.md#node-trust-boundary--settled).

Enable ordering:

```text
start BlockAdded remotely
start VirtualChainChanged remotely
enable router only when both succeeded
publish subscription Enabled only when both succeeded
```

The
[processing lifecycle](processing-lifecycle.md#entering-recovery-phases)
owns activation invocation and its caller-side preconditions. Once invoked,
NodeService owns the complete activation sequence above.

Disable ordering:

```text
disable router immediately
stop both remote subscriptions
```

Subscription changes are all-or-nothing. `SubscriptionControlFailed` is the
generation-preserving activation result: the first remote start failed before
anything became active, or a later start failed and every earlier start was
successfully rolled back. Before returning it, NodeService leaves the router
and subscription state Disabled, clears the installed notification channels,
keeps the exact validated handle admitted, and emits no `RpcRetired` event.

When activation rollback cannot be proven, or either remote unsubscription
fails, NodeService disables the router, retires the exact validated handle,
enqueues `RpcRetired` before completing the operation, and returns
`GenerationLost`. Disable attempts still issue both remote stops before this
retirement decision. Cancellation or an independently observed retirement
retains its existing typed outcome rather than being reclassified as a
subscription-control failure. The
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns the cross-worker fault and disposition of the generation-preserving
result and the coalescing of a generation-ending result with `RpcRetired`.

Callbacks received while the router remains Disabled during remote activation
are intentionally dropped; they receive no Catchup overlap credit and do not
themselves request recovery. NodeService does not replay dropped callbacks.
The consequences for Live admission and a block that later becomes required
belong to the
[processing lifecycle](processing-lifecycle.md#recovery-scope-and-omitted-body-tips).
Disabling is an immediate local cutoff, not a quiescence or transport fence.
An item whose bounded enqueue linearized before retirement may remain in the
old session's channel; processor gates and complete session teardown discard
such old-session work before another generation is activated.

The [PUAR](verification.md#current-puar-result) establishes that virtual
processing can emit a fully empty VirtualChainChanged notification when
processing a side block does not move the selected-chain sink. This
notification filter is distinct from handling an empty VSPC V2 RPC page in the
synchronization pump. The reviewed sink selection cannot produce a notification
with a nonempty removed chain and an empty added path; that shape is the fault
above, not another no-op.

In a short Live IBD episode, connection loss or a violated stream invariant
already causes recovery. NodeService has no separate continuous-IBD mode in
KGI v2.

Only NodeService sees raw rusty-kaspa notification and full-block types.
Outside NodeService, block payloads use the shared
[`ValidatedNodeBlock`](domain-model.md#shared-value-types--settled), and VSPC
payloads use [`VspcChange`](domain-model.md#vspc-value-types--settled).

### RPC normalization

Each published `ValidatedRpcClient` bounds its normalized runtime requests with:

```text
MAX_RPC_CONCURRENCY = 32
```

Every runtime operation waits cancellably for a permit before issuing its RPC.
Permit waiting is ordinary local backpressure, not notification loss or a
recovery fault. DependencyResolver independently admits at most its
BlockProcessor-owned concurrency limit, leaving capacity for recovery and VSPC
operations. Connection validation precedes publication and remains sequential;
it does not consume this runtime budget.
NodeService reports active and permit-waiting runtime RPC operations.

#### GetBlock not-found compatibility classification

The selected rusty-kaspa gRPC transport does not preserve structured
`ConsensusError::HeaderNotFound` or `ConsensusError::BlockNotFound` variants.
Its wire `RPCError` contains only a message, and the client reconstructs every
remote error as `RpcError::General(String)`. KGI nevertheless requires a
definitive GetBlock not-found result to remain distinct from other RPC
failures.

NodeService therefore owns one narrow compatibility exception to the rule that
error strings do not select control flow. For a failed GetBlock request with
requested hash `hash`, classify `RpcError::General(message)` as the KGI-owned
typed `BlockNotFound` outcome only when `message` is byte-for-byte equal to
either:

```rust
message == ConsensusError::HeaderNotFound(hash).to_string()
    || message == ConsensusError::BlockNotFound(hash).to_string()
```

The first form is the reviewed production result when GetBlock cannot find the
requested header. The second form covers the same requested-block absence when
rusty-kaspa reports the missing full block instead. KGI does not preserve this
upstream distinction: both mean that the requested block cannot be obtained
from the node and produce the same typed `BlockNotFound` result.

The comparison performs no trimming, case folding, prefix or suffix matching,
substring search, or classification of another RPC operation. A message built
for a different hash is not a match. Every nonmatching
`RpcError::General(message)` from a completed normalized runtime RPC becomes the
KGI-owned typed `RpcRequestFailed` outcome. It does not prove whether the cause
was remote application behavior, transport, timeout, channel failure, or local
client state, and it does not by itself retire the validated RPC generation.
The raw message remains diagnostic outside this private adapter.

Cancellation and generation loss are established by KGI-owned operation state,
not inferred from the flattened error. If either wins the operation-completion
race, its existing typed outcome takes precedence over `RpcRequestFailed`.
Validation RPCs and notification subscription control retain their dedicated
lifecycle contracts rather than using this runtime-operation outcome.
The [processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns the recovery and Live dispositions of `RpcRequestFailed`.

This exception is tied to the pinned rusty-kaspa client behavior. An upstream
or server message change may conservatively stop recognizing absence, but must
never broaden either match. Replacing this adapter requires an end-to-end
structured not-found discriminator in the selected production transport and a
newly accepted pinned-upstream review.

#### Client response-conversion failures

The selected rusty-kaspa client converts the wire response into its typed RPC
value before `ResponseNormalizer` receives it. NodeService classifies a
client-side conversion failure as malformed recovery evidence only when the
typed `RpcError` variant, its structured fields, and the requested operation
uniquely establish that conversion failed for response data consumed by KGI.
That classification returns the operation's existing source-specific
`RecoveryInputInvalid` value and retires the exact validated RPC generation.

A conversion failure attributable only to an ignored field, or lacking enough
field provenance to distinguish a consumed field from an ignored one, becomes
`RpcRequestFailed`. It retains the generation. This conservative result is
intentional: rusty-kaspa may convert fields that KGI deliberately does not
consume, and their conversion does not enlarge the
[node trust boundary](overview.md#node-trust-boundary--settled). The formatted
error text is diagnostic only. Exact object and field identifiers carried by
`MissingRpcFieldError` are structured converter evidence and may be matched;
no other diagnostic-string matching is permitted by this rule.

For the pinned client, apply this exhaustive operation-specific mapping:

| Operation | Attributable conversion failure | Malformed result |
|---|---|---|
| `GetSink` within `catchup_sink_sample` | `HexParsingError`; the response converter parses only the consumed sink hash | `MalformedCatchupSinkResponse` |
| Individual `GetBlock` | `MissingRpcFieldError` naming the response block or its block header; `RpcBlueWorkTypeParseError` | `MalformedGetBlock`, remapped to the composite source when applicable |
| `GetBlocks` | `MissingRpcFieldError` naming a block header, which is required even for the stripped anchor | `MalformedGetBlocks` |
| `GetBlockDagInfo` | none with the current converter | — |
| `GetVirtualChainFromBlockV2` | none with the current converter | — |

Every conversion failure not listed in the table is opaque. In particular:

- `GetBlockDagInfo` hash conversion cannot identify whether the consumed
  pruning-point hash or an ignored tip, virtual parent, or sink failed;
- `GetBlock` hash conversion covers both consumed relationship hashes and
  ignored header commitments, transaction data, and verbose fields;
- the current gRPC header converter reports invalid blue work as the same
  unqualified `HexParsingError`, rather than the field-specific blue-work
  variant;
- `GetBlocks` also converts the ignored parallel hash vector, the stripped
  anchor's unused payload, transactions, and ignored block fields without
  retaining the failing member or field; and
- VSPC V2 hash conversion does not distinguish consumed removed or added
  hashes from ignored acceptance data.

Missing or mismatched generic response-envelope payload errors likewise do not
prove that node-provided data consumed by KGI was malformed and remain opaque.
If a future adapter preserves additional field and member provenance,
NodeService may classify a newly attributable consumed-field failure as
malformed only after the operation table and pinned-upstream evidence are
updated. The [processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns the existing dispositions of `RecoveryInputInvalid` and
`RpcRequestFailed`.

#### Full-block normalization

NodeService is the sole constructor of `ValidatedNodeBlock`. One common
normalizer extracts the flattened fields and enforces the construction contract
owned by the
[domain model](domain-model.md#shared-value-types--settled). Raw `RpcBlock`,
optional verbose data, and an unvalidated shared-block wrapper never cross the
NodeService boundary.

For an ordinary non-Genesis block, the normalizer consumes the header's cached
`hash`, level-zero `parents_by_level`, `timestamp`, `daa_score`, `blue_score`,
and `blue_work`, plus verbose `selected_parent_hash`,
`merge_set_blues_hashes`, and `merge_set_reds_hashes`. Verbose data is therefore
mandatory for an ordinary `ValidatedNodeBlock`; absence returns the obtaining
operation's source-specific malformed-input result. The normalizer constructs
`direct_parents` by retaining the first occurrence of each level-zero parent
hash in node order and discarding later occurrences. It applies the same
first-occurrence rule independently to `merge_set_blues_hashes` and
`merge_set_reds_hashes`. The selected-parent hash must occur in the canonical
direct-parent sequence. If the block's authoritative header hash occurs in any
of the three raw relationship vectors, common normalization fails rather than
retaining or silently removing that occurrence.

The cached header hash is authoritative block identity and is never
recomputed. Repeated raw parent or merge-set positions are accepted
normalization input rather than malformed evidence. The normalizer does not
require the relationship vectors to be disjoint or compare redundant reported
hashes. In particular, a hash other than the block's own hash found in both
merge-set colors remains once in each canonical vector. Own-hash exclusion is
the minimum structural condition that prevents Storage from treating the
incoming block as one of its own references. Canonicalizing each vector at this
boundary prevents downstream components from retaining or independently
normalizing repeated observations that have no distinct relationship meaning
while preserving cross-vector semantics.

For the exact Genesis hash of the validated RPC generation, the normalizer
does not require verbose data. It copies the header hash, timestamp, DAA score,
and blue work, then constructs the domain-owned representation with synthetic
ORIGIN as selected parent, empty direct-parent and merge-set vectors, and blue
score zero. It ignores equivalent raw relationship and blue-score fields
rather than auditing the node's Genesis representation.

Transactions, parents above level zero, verbose hash, difficulty, transaction
IDs, `is_header_only`, verbose blue score, children hashes, and
`is_chain_block` are unused and ignored. In particular, an unexpected
transaction payload is not rejected merely because KGI requested
`include_transactions = false`.

The common normalizer checks every consumed DAA score and every consumed
ordinary blue score against the domain-owned `MAX_DAA_SCORE` and
`MAX_BLUE_SCORE`. Header-only operations likewise check each score they
consume before it enters a normalized processing value. An exact-Genesis
branch synthesizes blue score zero before any blue-score range check and does
not consume the raw Genesis blue score. An excessive consumed score returns the
typed `ScoreOutOfRange(DaaScore)` or `ScoreOutOfRange(BlueScore)` result. It is
not a malformed RPC shape and is never remapped to a source-specific
malformed-input kind.

The normalizer copies the domain-owned informational `Timestamp` unchanged and
performs no timestamp range validation. Storage owns its lossless `BIGINT`
encoding.

The operation that obtained a raw block additionally compares the trusted
header hash with any contextual requested or advertised identity on which
response attribution depends. Source-specific response classification remains
outside the common normalizer for every other construction failure: GetBlocks,
individual GetBlock, current-pruning-point, Catchup sink-sample, and BlockAdded
inputs retain their distinct fault classifications and lifecycle dispositions.
The [processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns range-fault and malformed-input dispositions.

#### Current pruning-point block

The current pruning point is obtained through one normalized composite
operation on the run's exact validated generation:

```rust
impl ValidatedRpcClient {
    async fn current_pruning_point_block(
        &self,
    ) -> Result<ValidatedNodeBlock, NodeError>;
}
```

The operation calls `GetBlockDagInfo`, reads a non-ORIGIN
`pruning_point_hash`, and then calls
`GetBlock(pruning_point_hash, include_transactions = false)` on the same
generation. The returned trusted header hash must equal
`pruning_point_hash`, and the result must pass common full-block
normalization. A repeated network value in `GetBlockDagInfo` is ignored: the
exact client already owns the network identity validated before publication.

The [processing lifecycle](processing-lifecycle.md) owns when Resync and Rebuild
invoke this operation and how they consume its normalized result.

An ORIGIN pruning-point hash, wrong returned trusted header hash, definitive
not-found for the advertised pruning point, or failure of common full-block
normalization for a reason other than score range is
`RecoveryInputInvalid(MalformedPruningPointResponse)`. The exact validated RPC
generation is retired and the shared malformed recovery-input policy applies.
An opaque `RpcRequestFailed`, cancellation, or generation loss is returned
without remapping.

#### Catchup sink sample

Catchup obtains its rolling sink marker through one normalized composite
operation on the run's exact validated generation:

```rust
struct CatchupSinkSample {
    hash: BlockHash,
    daa_score: u64,
}

impl ValidatedRpcClient {
    async fn catchup_sink_sample(
        &self,
    ) -> Result<CatchupSinkSample, NodeError>;
}
```

The operation calls `GetSink()`, rejects ORIGIN, then calls
`GetBlock(sink_hash, include_transactions = false)` on the same generation.
The immutable header must be present, its trusted cached hash must equal
`sink_hash`, and its DAA score must be in the domain-owned representable range.
No verbose data or redundant reported hash is consumed. The exact validated
Genesis hash is a valid sink under the domain-owned Genesis rules.

ORIGIN, definitive not-found for the just-advertised sink, missing or malformed
header data, or a hash mismatch is
`RecoveryInputInvalid(MalformedCatchupSinkResponse)`. The exact validated RPC
generation is retired and the shared malformed recovery-input policy applies.
A score outside the representable range retains the shared
`ScoreOutOfRange(DaaScore)` classification and does not retire the generation.
`RpcRequestFailed`, generation loss, and cancellation are returned without
remapping and are not malformed-response evidence.

The [Catchup lifecycle](processing-lifecycle.md#catchup-trigger) owns initial
and refresh call ordering, marker replacement, and recovery reaction.

#### GetBlocks and VSPC recovery responses

`get_blocks(low_hash, include_blocks = true)` is normalized inside NodeService:

- ignore the parallel `block_hashes` vector;
- require a nonempty block vector whose first trusted header hash is
  `low_hash`;
- strip that inclusive boundary block without normalizing its unused payload;
- accept zero normalized blocks after stripping that entry;
- normalize every remaining full block before returning any of them; and
- preserve their node-provided order and return `Vec<ValidatedNodeBlock>`.

For a nonempty normalized result, the final trusted header hash must differ
from `low_hash`; that final hash is the pump's unambiguous candidate next
cursor. An anchor-only response deliberately supplies no next cursor. KGI does
not validate duplicate members, parent-before-child order, consensus order, or
other graph topology in the page.

An empty block vector, a first trusted header hash other than `low_hash`, a
nonempty result whose final hash is `low_hash`, or any retained member that
fails common full-block normalization for a reason other than score range is
`RecoveryInputInvalid(MalformedGetBlocks)` when encountered by the recovery
pump. Reject the complete page before advancing its cursor or sending any
member to a processor. A valid anchor-only response that normalizes to zero
blocks is not malformed.

KGI requests full RPC blocks directly. Fetching hashes and then calling
`GetBlock` one by one has no accepted benefit for this local-node deployment.
For `GetVirtualChainFromBlockV2`, request
`min_confirmation_count = None` and
`data_verbosity_level = Some(RpcDataVerbosityLevel::None)`. KGI relies on this
combination preserving a minimal acceptance-data envelope and an advancing
`added.last()` cursor. The RPC's exact added chain-path batch size is
`10 * mergeset_size_limit`. This limit bounds `added`, while the complete
`removed` suffix is not batch-limited. The same numeric budget bounds merged
blocks loaded for acceptance data; the resulting acceptance-data length may
shorten `added`, but only to a complete prefix. The
[PUAR](verification.md#current-puar-result) checks these upstream assumptions
against the reference revision; KGI-owned request construction and response
handling are covered by
[verification.md](verification.md#nodeservice-and-rpc-behavior).

For recovery VSPC V2, a fully empty response is a valid pump hint. Classify
response violations precisely:

```text
empty added with nonempty removed -> RemovedChainWithoutAddedPath
nonempty removed whose first hash differs from low_hash
                                  -> RemovedSourceMismatch
added.last() equals low_hash      -> NonAdvancingAddedCursor
```

Apply the table in order so an input with more than one defect has one stable
reason. Each is
`RecoveryInputInvalid(MalformedVspcResponse(reason))` and follows the shared
[malformed recovery-response policy](processing-lifecycle.md#supervisor-and-recovery-intent--settled).
Evaluate these framing conditions before returning a normalized response or
advancing the provisional cursor. Preserve the original ordered vectors. KGI
does not reject duplicate members, removed/added intersections, or another
occurrence of `low_hash` inside `added`; those are trusted path properties
under the [node trust boundary](overview.md#node-trust-boundary--settled).
Internal selected-parent continuity is not observable from the RPC hash
vectors; the
[atomic VSPC transaction](storage.md#atomic-vspc-transaction--settled) owns its
validation.

#### Individual recovery GetBlock

```rust
impl ValidatedRpcClient {
    async fn recovery_header(
        &self,
        hash: BlockHash,
    ) -> Result<ValidatedRecoveryHeader, NodeError>;
}
```

During Resync preparation, this operation calls `GetBlock(hash, false)` and
requires the trusted header hash to equal `hash`. Both normalization branches
require and range-check the header DAA score and require blue work:

- for an ordinary block, require and range-check the raw header blue score and
  copy it into `ValidatedRecoveryHeader`;
- when `hash` is the exact Genesis of this validated RPC generation, do not
  require, read, or range-check the raw header blue score and set the normalized
  `blue_score` to zero.

The operation does not require verbose data, direct parents, merge sets,
timestamp, or transactions. A different hash or data missing from the
applicable branch is `RecoveryInputInvalid(MalformedGetBlock)`. A definitive
not-found response and a normalized DAA score that disagrees with committed
storage remain Resync-preparation evidence under the processing-lifecycle contract
rather than malformed response shapes. An opaque failed call is
`RpcRequestFailed`; cancellation and generation loss retain their KGI-owned
typed outcomes. Storage supplies the materialized ID, selected parent, and
stored DAA score; NodeService does not construct
`MaterializedSyncAnchor`.

#### Individual full-block GetBlock

DependencyResolver obtains missing dependencies, and VspcProcessor attributes
a persisted selected-parent conflict, through the run's exact validated
generation without holding a database transaction:

```rust
impl ValidatedRpcClient {
    async fn full_block(
        &self,
        hash: BlockHash,
    ) -> Result<ValidatedNodeBlock, NodeError>;
}
```

The operation calls `GetBlock(hash, include_transactions = false)`, requires
the returned trusted header hash to equal `hash`, and applies common full-block
normalization. A wrong hash or non-range construction failure is
`RecoveryInputInvalid(MalformedGetBlock)`. A definitive not-found result is
reported separately so each caller can apply its source-specific contract.
`RpcRequestFailed`, cancellation, and generation loss are returned without
remapping. The processing lifecycle owns the recovery-versus-Live disposition
of a malformed response.

### Runtime protocol violation and generation retirement

`ValidatedRpcClient` recognizes malformed raw RPC responses while
validating and normalizing them and returns the typed violation without a
normalized value. The calling RPC owner reports that violation to
NodeService before reporting its owner-directed fault. NodeService atomically
retires that exact published
`ValidatedRpcClient`, retires its NotificationRouter, prevents all clones from
starting further RPC or subscription work, closes the physical connection, and
enters `Unavailable`. It emits the exact `RpcRetired` event before returning the
typed violation to the calling RPC owner. A stale report for a generation
already replaced cannot retire the replacement. NodeService reconnects and
performs complete validation before emitting `RpcPublished` for another
generation.

The malformed response is never retried in place. Supervisor owns the recovery
budget and source/phase dispositions defined by the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled).
Malformed Genesis-discovery output occurs before a validated generation is
published and remains an ordinary NodeService validation failure under its
connection backoff; it does not consume this runtime malformed-input budget.
