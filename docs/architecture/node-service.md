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
never falls back silently. The file is not persisted in node metadata.
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
an empty block vector, then copies the first hash into
`ValidatedNodeInfo.genesis_hash`. Transport failure or malformed output fails
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
  VirtualChainChanged notification structure in this order:
  1. Empty `removed` and empty `added` is the valid upstream no-op. Discard it;
     it consumes no channel capacity, earns no overlap credit, changes no
     committed state, and requests no recovery.
  2. Nonempty `removed` with empty `added` is
     `RemovedChainWithoutAddedPath`.
  3. A repeated hash within either vector is `DuplicateChainMember`.
  4. A hash present in both vectors is `RemovedAddedIntersection`.

  For cases 2 through 4, enqueue nothing, disable both streams, and report
  `NotificationInputInvalid(MalformedVspcChange(reason))`. This ordering makes
  the reason stable when more than one defect is present. The
  [processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
  owns the typed fault's disposition.
- Retired never routes again.

The subscription state belongs to `ValidatedRpcClient` and cannot outlive its
connection. The handle is clonable through `Arc` and protects notification
state with a private mutex.

Enable ordering:

```text
precondition: ResyncEngine has sent Catchup to both processors for this run
start BlockAdded remotely
start VirtualChainChanged remotely
enable router only when both succeeded
publish subscription Enabled only when both succeeded
```

Catchup is a caller-established lifecycle precondition, not a NodeService
operation. ResyncEngine is the sole sender of processor commands and invokes
NodeService subscription activation only after satisfying that precondition.
NodeService owns the four activation steps following it.

Disable ordering:

```text
disable router immediately
stop both remote subscriptions
```

Subscription changes are all-or-nothing. If either remote start fails, stop
any subscription already started; retire the validated handle when rollback
cannot be proven. A partial subscription or unsubscription failure likewise
retires the handle. Callbacks received while the router remains Disabled
during remote activation are intentionally dropped; they receive no Catchup
overlap credit and do not themselves request recovery. NodeService does not
replay dropped callbacks. The consequences for Live admission and a block that
later becomes required belong to the
[processing lifecycle](processing-lifecycle.md#recovery-scope-and-omitted-body-tips).
Disabling is an immediate local cutoff, not a quiescence or transport fence.

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

#### Full-block normalization

NodeService is the sole constructor of `ValidatedNodeBlock`. One common
normalizer extracts the flattened fields and enforces the intrinsic validity
contract owned by the
[domain model](domain-model.md#shared-value-types--settled). Raw `RpcBlock`,
optional verbose data, and an unvalidated shared-block wrapper never cross the
NodeService boundary. Transactions are ignored.

The common normalizer checks raw DAA and blue scores against the domain-owned
`MAX_DAA_SCORE` and `MAX_BLUE_SCORE`. Apply the same checks to header-only node
responses before their scores enter a `MaterializedSyncAnchor`, Catchup
calculation, or other processing input. An excessive score returns the typed
`ScoreOutOfRange(DaaScore)` or `ScoreOutOfRange(BlueScore)` result. It is not a
malformed RPC shape and is never remapped to a source-specific malformed-input
kind.

For the exact Genesis hash of the validated RPC generation, the normalizer
also enforces the domain-owned zero-blue-score invariant. A nonzero value is an
intrinsic block-validation failure rather than a range failure and therefore
retains the obtaining operation's source-specific malformed-input
classification.

The normalizer copies the domain-owned informational `Timestamp` unchanged and
performs no timestamp range validation. Storage owns its lossless `BIGINT`
encoding.

The operation that obtained a raw block additionally validates its contextual
expected hash. Source-specific response classification remains outside the
common normalizer for every other intrinsic failure: GetBlocks, individual
GetBlock, current-pruning-point, Catchup sink-sample, and BlockAdded inputs
retain their distinct fault classifications and lifecycle dispositions. The
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
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

The operation calls `GetBlockDagInfo`, requires the response network to equal
`ValidatedNodeInfo.network_id`, reads a non-ORIGIN `pruning_point_hash`, and
then calls `GetBlock(pruning_point_hash, include_transactions = false)` on the
same generation. The returned block hash must equal `pruning_point_hash`, and
the result must pass common full-block normalization.

The [processing lifecycle](processing-lifecycle.md) owns when Resync and Rebuild
invoke this operation and how they consume its normalized result.

A mismatched response network, ORIGIN pruning-point hash, wrong returned block
hash, definitive not-found for the advertised pruning point, or failure of
common full-block normalization for a reason other than score range is
`RecoveryInputInvalid(MalformedPruningPointResponse)`. The exact validated RPC
generation is retired and the shared malformed recovery-input policy applies.
A transport failure, cancellation, or generation loss remains a session fault
and does not establish a reconciliation mismatch.

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
The immutable header must be present, its computed hash and every separately
reported block hash must equal `sink_hash`, and its DAA score must be in the
domain-owned representable range. The exact validated Genesis hash is a valid
sink under the domain-owned Genesis rules.

ORIGIN, definitive not-found for the just-advertised sink, missing or malformed
header data, or a hash mismatch is
`RecoveryInputInvalid(MalformedCatchupSinkResponse)`. The exact validated RPC
generation is retired and the shared malformed recovery-input policy applies.
A score outside the representable range retains the shared
`ScoreOutOfRange(DaaScore)` classification and does not retire the generation.
Transport failure, generation loss, and cancellation retain their distinct
session-level outcomes and are not malformed-response evidence.

The [Catchup lifecycle](processing-lifecycle.md#catchup-trigger) owns initial
and refresh call ordering, marker replacement, and recovery reaction.

#### GetBlocks and VSPC recovery responses

`get_blocks(low_hash, include_blocks = true)` is normalized inside NodeService:

- raw hash and block vectors must have equal length;
- the raw response must be nonempty and start with `low_hash`;
- hashes must match returned blocks;
- duplicate hashes are invalid;
- strip the inclusive `low_hash` entry;
- accept zero normalized blocks after stripping that entry;
- normalize every remaining full block before returning any of them; and
- return `Vec<ValidatedNodeBlock>`; a parallel hash vector is unnecessary.

Unequal vectors, an empty raw response, a first hash other than `low_hash`, a
hash/block disagreement, any duplicate, or any member that fails common
full-block normalization for a reason other than score range is
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
duplicate within either vector    -> DuplicateChainMember
hash present in both vectors      -> RemovedAddedIntersection
added.last() equals low_hash      -> NonAdvancingAddedCursor
removed.first() differs from low_hash, or low_hash occurs earlier in added
                                  -> LowHashPathMismatch
```

Apply the table in order so an input with more than one defect has one stable
reason. Each is
`RecoveryInputInvalid(MalformedVspcResponse(reason))` and follows the shared
[malformed recovery-response policy](processing-lifecycle.md#supervisor-and-recovery-intent--settled).
Evaluate all of these hash-observable conditions before returning a normalized
response. Internal selected-parent continuity is not observable from the RPC
hash vectors; the
[atomic VSPC transaction](storage.md#atomic-vspc-transaction--settled) owns its
validation.

#### Individual recovery GetBlock

During Resync preparation, `GetBlock(sink_hash, false)` must return exactly the
requested hash and the header data required to construct
`MaterializedSyncAnchor`. A different hash or missing required GhostDAG header
data is `RecoveryInputInvalid(MalformedGetBlock)`. A definitive not-found
response and a returned DAA score that disagrees with committed storage remain
reconciliation evidence under the processing-lifecycle contract rather than
malformed transport shapes.

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
the returned hash to equal `hash`, and applies common full-block normalization.
A wrong hash or non-range intrinsic normalization failure is
`RecoveryInputInvalid(MalformedGetBlock)`. A definitive not-found result is
reported separately so each caller can apply its source-specific contract.
Transport, cancellation, and generation loss remain session faults. The
processing lifecycle owns the recovery-versus-Live disposition of a malformed
response.

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
