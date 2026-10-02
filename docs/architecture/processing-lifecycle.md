# Processing lifecycle and recovery

## Scope and ownership

This document owns Supervisor recovery intent, cross-worker commands and
faults, ResyncEngine preparation and pumping, recovery phase transitions,
Catchup admission, overlap-based Live entry, and teardown order.

It coordinates component behavior without redefining it. Node connection,
subscription, and normalization rules belong to
[node-service.md](node-service.md); persistence and transaction mechanics to
[storage.md](storage.md); processor-local admission and overlap rules to
[block-processing.md](block-processing.md) and
[vspc-processing.md](vspc-processing.md); API publication effects to
[api-publication.md](api-publication.md); and ApiService control effects to
[api-service.md](api-service.md). Shared value types belong to
[domain-model.md](domain-model.md).

## Processing session and resource acquisition — settled

```rust
struct ProcessingSession {
    rpc: Arc<ValidatedRpcClient>,
    db: Arc<ValidatedDbClient>,
    graph_updates: GraphUpdateProducer,
}

struct SupervisorResourceState {
    rpc: Option<Arc<ValidatedRpcClient>>,
    db: Option<Arc<ValidatedDbClient>>,
}
```

Supervisor installs the reliable ordered `NodeServiceEvent` and
`StorageServiceEvent` paths before either service can publish an initial
generation or rejection. It initializes both fields to `None`.
`RpcPublished(client)` sets `rpc` to that exact client. `RpcRetired(client)`
clears it only when it still holds that exact `Arc`; a late retirement cannot
clear a newer generation. `NodeServiceEvent::Rejected(reason)` enters
Supervisor's Fatal lifecycle.
`ProcessingDbPublished(client)` sets it to that exact client.
`ProcessingDbRetired(client)` clears it only when it still holds that exact
`Arc`; a late retirement cannot clear a newer generation.
`StorageServiceEvent::Rejected(reason)` enters Supervisor's Fatal lifecycle.
The API variants follow the forwarding contract below.

Service events are the sole authority for Supervisor's RPC and processing DB
bindings. Supervisor never requests publication or reconnection. Each service
autonomously replaces its retired generation; Supervisor retains published
replacements for the next processing run and never injects them into an
existing `ProcessingSession`. Unexpected closure of either reliable event path
while Supervisor is Running reports
`Ownership(ManagedComponentUnavailable)` and enters the Fatal lifecycle.

When recovery is desired and the engine is Idle, Supervisor may retain either
resource while the other is absent. It starts no run until both exact
generations are present.
StorageService can open, lock, and inspect an Uninitialized database while the
RPC generation is absent. Once an RPC generation is published,
Supervisor supplies its `(network_id, genesis_hash)` through the idempotent
`initialize_if_uninitialized` operation when initialization is authorized.
StorageService alone performs any required `Uninitialized -> Empty` transition
and later publishes the resulting DB generations through its event stream.
Before starting, recheck that the captured recovery obligation is still the
latest `desired_recovery`, the engine is Idle, and `SupervisorResourceState`
still holds both exact `Arc` values selected for the run.

The exact `Arc<ValidatedRpcClient>`, `Arc<ValidatedDbClient>`, and session
`GraphUpdateProducer` are passed through `ResyncEngine::start` and then through the session to
their consumers, including DependencyResolver for the validated clients and
both processors for graph updates. This guarantees one resource and update
generation per run.
Before calling `start`, require the validated node's `(network_id,
genesis_hash)` to equal the immutable binding exposed by that DB generation.
A mismatch is terminal pairing rejection: never rebind the database or infer
that Rebuild can repair it.

When `RpcRetired(client)` or `ProcessingDbRetired(client)` names a generation
used by the active session, Supervisor applies the corresponding
generation-loss disposition exactly once: active recovery retains its current
obligation and retries after deactivation; Live establishes `Require(Resync)`.
A concurrent operation may report the same loss first. The operation fault and
retirement event coalesce, request at most one deactivation, and never weaken
`desired_recovery`. An owner-directed malformed-response fault remains
admissible after its `RpcRetired` event has requested deactivation: Supervisor
must still count it, apply any stronger recovery obligation, and preserve a
fourth-occurrence Fatal disposition. Once the run's `ValidatedRpcClient` has
classified that malformed response, the result is already produced for this
purpose. ResyncEngine's deactivation barrier must allow its caller to report
the corresponding typed fault before that caller is joined, and Supervisor
continues accepting such a fault from the retiring session until `Deactivated`.

## Supervisor and recovery intent — settled

```rust
enum RecoveryMode {
    Resync,
    Rebuild,
}
```

The [settled system shape](overview.md#system-shape--settled) owns Supervisor's
component references and their `Arc` representation. Their public methods are
Supervisor's complete downward control surface; it does not expose or depend
on their private mailbox representations.

Implement `Ord` with:

```text
Resync < Rebuild
```

Document in code that this ordering represents **recovery strength**, not
lifecycle or execution order.

Startup behavior:

- normal startup requests `Resync`;
- `--clear-db` requests `Rebuild`.

An Empty network-bound DB has no PP or sink for Resync reconciliation. The
engine reports `Require(Rebuild)`; Supervisor then starts a distinct
Rebuild run. Structurally valid but inconsistent processing contents also
require Rebuild, while partial/unsupported schema is rejected by
StorageService before publishing a DB client.

`desired_recovery` is the strongest recovery requirement the Supervisor must
eventually satisfy. Calling `start` does not consume it. `active_recovery` is
the recovery currently executing.

Repeated requirements coalesce with `max`. If a stronger requirement arrives
during an active run, request deactivation once, wait for `Idle`, then start
the stronger mode.

```rust
enum FaultDisposition {
    Retry,
    Require(RecoveryMode),
    Fatal,
}

enum Component {
    Supervisor,
    NodeService,
    StorageService,
    ResyncEngine,
    BlockProcessor,
    OrphanManager,
    DependencyResolver,
    VspcProcessor,
    ApiService,
}
enum ServiceKind { Node, Storage }
enum MalformedVspcNotificationReason {
    RemovedChainWithoutAddedPath,
    DuplicateChainMember,
    RemovedAddedIntersection,
    ResolvedSourceDiscontinuity,
    SelectedParentPathDiscontinuity,
    DuplicatePendingTransition,
    ContradictoryDestination,
    CompetingNextMove,
}
enum NotificationInputKind {
    MalformedBlockAdded,
    MalformedVspcChange(MalformedVspcNotificationReason),
}
enum MalformedVspcResponseReason {
    RemovedChainWithoutAddedPath,
    NonAdvancingAddedCursor,
    DuplicateChainMember,
    RemovedAddedIntersection,
    LowHashPathMismatch,
    ResolvedSourceDiscontinuity,
    SelectedParentPathDiscontinuity,
}
enum RecoveryInputKind {
    MalformedPruningPointResponse,
    MalformedCatchupSinkResponse,
    MalformedGetBlock,
    MalformedGetBlocks,
    MalformedVspcResponse(MalformedVspcResponseReason),
}
enum PersistenceFault {
    DefiniteFailure,
    RetryExhausted,
    AmbiguousCommit,
}
enum ScoreRangeFault {
    DaaScore,
    BlueScore,
    BoundarySealThreshold,
}
enum BoundedProcessingState {
    Orphans,
    VspcPending,
}
enum OwnershipFault {
    SessionDataEndpointLost,
    ManagedComponentUnavailable,
    InternalControlPathLost,
    UnexpectedWorkerTermination,
    InvalidLifecycleControl,
}
enum FaultKind {
    ServiceGenerationLost(ServiceKind),
    SessionContinuityLost,
    NotificationInputInvalid(NotificationInputKind),
    RecoveryInputInvalid(RecoveryInputKind),
    ReconciliationFailed,
    MaterialityViolation,
    DependencyUnavailable,
    BoundedStateExhausted(BoundedProcessingState),
    ScoreOutOfRange(ScoreRangeFault),
    Persistence(PersistenceFault),
    Ownership(OwnershipFault),
}
struct ComponentFault {
    source: Component,
    disposition: FaultDisposition,
    kind: FaultKind,
    diagnostic: Arc<str>,
}

struct SupervisorStatus {
    lifecycle: SupervisorLifecycle,
    desired_recovery: Option<RecoveryMode>,
    active_recovery: Option<RecoveryMode>,
}

enum SupervisorLifecycle {
    Running,
    Fatal,
    ShuttingDown,
    Stopped,
}

enum ProcessingState {
    Idle,
    Reconciling { mode: RecoveryMode },
    RebuildingDatabase,
    ResyncingDag,
    CatchingUp,
    Live,
    Deactivating,
    Stopped,
}

struct ProcessingStatus {
    state: ProcessingState,
}
```

`deactivation_requested` is internal and absent from public status.
Supervisor and ResyncEngine publish their respective status values under the
shared delivery contract below. The
[API status-observation contract](api-service.md#status-observation--settled) owns only
their composite public projection.

All component status publication is latest-value and lossy: each publication
replaces the previously observable value, and consumers may skip intermediate
states. Status never replaces explicit control operations, milestones, faults,
completed barriers, or validated capabilities as a lifecycle input. Exact
watch primitives remain deferred.

`ComponentFault` is the cross-worker control envelope. Component-local errors
may retain richer library-specific sources, but must be classified before
crossing an ownership boundary. The enum sketch fixes the required semantic
discriminants, not exact module placement or error-library syntax. Only typed
fields drive control, retry counters, and metrics; diagnostic strings never
do.

- `Retry` is meaningful only while a recovery is active. It aborts and fully
  deactivates the current attempt; after Idle and backoff, Supervisor reruns
  fresh preparation for the strongest unsatisfied recovery obligation. It
  never retries one RPC or page in place.
- A recoverable fault in Live must require at least `Resync`.
- Fault strings are diagnostics and must never drive control flow; retry
  causes used by policy are typed.
- A nonmaterialized hash directly named in VSPC `added`/`removed` and a
  resolver-confirmed unavailable dependency each require Rebuild directly:
  the DB can no longer be trusted against node state. RPC connection failure
  is not proof of dependency unavailability.
- `MaterialityViolation` from an incoming block hash already classified as a
  permanent boundary identity, or from an identity-only reference rejected by
  `RequireMaterialized`, requires Rebuild directly. Boundary leaves accepted
  under `AllowBoundaryIdentities` produce no such fault.
- A strict pre-Catchup materialization attempt with absent references only
  reports `ReconciliationFailed` and `Require(Resync)`. The same result during
  Catchup or Live produces no immediate lifecycle fault; its local handling
  belongs to the
  [BlockProcessor contract](block-processing.md#admission-and-materialization).
- `ScoreOutOfRange(DaaScore)` and `ScoreOutOfRange(BlueScore)` identify node
  values outside KGI's shared representable ranges.
  `ScoreOutOfRange(BoundarySealThreshold)` identifies a boundary-threshold
  addition that overflows `u64` or exceeds `MAX_BLUE_SCORE`. Each is `Fatal`:
  retry, Rebuild, or a replacement RPC generation cannot make the value
  representable. A range fault does not retire the validated RPC generation or
  consume the malformed recovery-response budget. The actual value or addition
  operands belong in diagnostics and do not select control flow.
- Storage's defensive `StorageError::ScoreOutOfRange` maps to the corresponding
  `ScoreOutOfRange(DaaScore)` or `ScoreOutOfRange(BlueScore)` fault and the same
  Fatal disposition; it is not a persistence retry.
- A malformed BlockAdded notification reports
  `NotificationInputInvalid(MalformedBlockAdded)`, disables notification
  routing, and requires Resync. It does not retire the validated RPC generation
  or consume the malformed recovery-response budget.
- A malformed VSPC notification reports
  `NotificationInputInvalid(MalformedVspcChange(reason))`, disables
  notification routing, and requires Resync under the same
  notification-source policy. It neither retires the validated RPC generation
  nor consumes the malformed recovery-response budget.
- `BoundedStateExhausted(Orphans)` and
  `BoundedStateExhausted(VspcPending)` each require Resync. ResyncEngine closes
  both routed notification streams and begins ordinary complete session
  teardown. Neither fault retires an RPC generation or consumes the malformed
  recovery-response budget, and a retained Rebuild obligation is never
  weakened to Resync.
- A `VspcSourceDiscontinuity` reported for a synthetic candidate is
  `RecoveryInputInvalid(MalformedVspcResponse(ResolvedSourceDiscontinuity))`.
  The notification form is
  `NotificationInputInvalid(MalformedVspcChange(ResolvedSourceDiscontinuity))`.

VspcProcessor's typed
[`VspcPathAttribution`](vspc-processing.md#readiness-and-materiality--settled)
result maps to lifecycle policy as follows:

| Attribution | Synthetic candidate | Notification candidate |
|---|---|---|
| `StoredParentConflict` | `ReconciliationFailed`, `Require(Rebuild)`, and keep the RPC generation | `ReconciliationFailed`, `Require(Rebuild)`, and keep the RPC generation |
| `CandidatePathConflict` | `RecoveryInputInvalid(MalformedVspcResponse(SelectedParentPathDiscontinuity))`; retire and count the RPC generation | `NotificationInputInvalid(MalformedVspcChange(SelectedParentPathDiscontinuity))`; disable routing and `Require(Resync)` |
| `StoredAndCandidateConflict` | The same recovery-input fault with a Rebuild obligation; retire and count the RPC generation | `NotificationInputInvalid(MalformedVspcChange(SelectedParentPathDiscontinuity))`, strengthened to Rebuild; disable routing |
| `AttributionBlockUnavailable` | Treat the synthetic current generation as malformed `SelectedParentPathDiscontinuity`; retire and count it | `NotificationInputInvalid(MalformedVspcChange(SelectedParentPathDiscontinuity))`; disable routing and `Require(Resync)` |

Only the two stored-state outcomes establish Rebuild. Notification outcomes
never retire the validated RPC generation or consume the malformed
recovery-response budget. A malformed individual full-block GetBlock, whether
issued by DependencyResolver or VspcProcessor attribution, retires the exact
RPC generation: during active recovery it consumes the shared malformed-input
budget, while in Live it requires Resync without consuming that recovery-only
budget. Attribution transport, cancellation, or generation loss is a session
fault and establishes neither candidate nor database blame.

A defensive storage `VspcMemberSetViolation` maps by candidate source. For a
synthetic candidate it becomes
`RecoveryInputInvalid(MalformedVspcResponse(reason))`; for a notification it
becomes `NotificationInputInvalid(MalformedVspcChange(reason))`. The reason is
the corresponding `DuplicateChainMember` or `RemovedAddedIntersection`
variant. Apply the same recovery-response or notification-source disposition
defined above; storage does not decide it.

When Supervisor still holds the same published RPC and processing DB
generations, whole-attempt recovery retries use nominal
delays `1s, 2s, 4s, 8s, 16s, 30s`, capped at `30s`, with equal jitter from 50%
through 100%. Reset that general backoff on `EnteredLive`, `RpcPublished` with
a new RPC generation, `ProcessingDbPublished` with a new DB generation, or a
stronger recovery obligation. RPC generation loss waits for `RpcPublished`;
DB generation loss waits for `ProcessingDbPublished`. Neither adds this delay.
`Require(Resync)` and `Require(Rebuild)` do not consume or wait on the Retry
sequence. All waits are lifecycle-cancellable.

Malformed recovery RPC responses use a separate shared budget across
`MalformedPruningPointResponse`, `MalformedCatchupSinkResponse`,
`MalformedGetBlock`, `MalformedGetBlocks`, and every `MalformedVspcResponse`
reason. NodeService's
[runtime protocol contract](node-service.md#runtime-protocol-violation-and-generation-retirement)
retires the exact `ValidatedRpcClient` generation that produced each such
occurrence before this lifecycle policy counts it. A violation observable in
the raw response discards that response without advancing a cursor or sending
processor input. `SelectedParentPathDiscontinuity` enters this policy only
after VspcProcessor's attribution probe proves that the synthetic candidate
disagrees with the current GetBlock selected parent. It prevents the atomic
VSPC mutation, aborts the complete attempt, and discards its provisional cursor
and queued synthetic suffix. The attribution table above owns any simultaneous
Rebuild obligation. The first three occurrences before `EnteredLive` each
permit another attempt after replacement; the fourth is `Fatal`. An accumulated
Rebuild obligation makes that next attempt Rebuild rather than Resync. The
counter is shared across all recovery-input kinds and VSPC reasons and survives
replacement RPC generations, so reconnecting repeatedly to the same
incompatible node cannot loop forever. Only `EnteredLive` resets it; a new RPC
generation or stronger recovery mode does not.

Each permitted malformed-input Retry waits for `RpcPublished` with a new
validated generation and never reuses or reissues the operation on the retired
handle. NodeService's reconnect backoff supplies the delay, so the general
recovery Retry delay is not added. A malformed VSPC notification
is a notification-input fault and therefore does not consume the malformed
recovery-response budget.

A definitive not-found dependency remains `DependencyUnavailable` and requires
Rebuild instead of being classified as malformed.

Storage owns local transaction retries, operation-outcome classification, and
database-generation retirement; see
[storage.md](storage.md#transaction-retries). Using those classifications, the
lifecycle applies this exhaustive persistence-fault policy:

| Persistence fault | Active recovery | Live |
|---|---|---|
| `DefiniteFailure` | `Fatal`; preserve the existing recovery obligation until shutdown | `Fatal` |
| `RetryExhausted` | Abort the session with `Retry` and retain the strongest current recovery obligation | Abort the session with `Require(Resync)` |
| `AmbiguousCommit` | Abort the session with `Retry` and retain the strongest current recovery obligation | Abort the session with `Require(Resync)` |

`ServiceGenerationLost(Storage)` aborts active recovery with `Retry` while
retaining its current obligation; in Live it requires Resync. Both it and
`AmbiguousCommit` wait for a later `ProcessingDbPublished` event rather than
reusing the retired generation, without adding the general recovery Retry
delay. Their operation faults coalesce with the exact-generation retirement
event as defined by the session-acquisition contract.

Every `Retry` or `Require` row performs ordinary complete session teardown; it
never reissues the failed operation. An ambiguous Rebuild transaction or an
ambiguity before `PpBoundarySealed` retains Rebuild. After that milestone the
retained obligation is already Resync. A Fatal persistence fault enters service
shutdown without first inventing a new recovery obligation.

Fault ownership:

```text
DependencyResolver / OrphanManager -> BlockProcessor
BlockProcessor / VspcProcessor     -> ResyncEngine
ResyncEngine                       -> Supervisor
```

Faults and milestones use reliable owner-directed events. Each run retains its
first causal fault diagnostically. A dropped barrier acknowledgement receiver
does not cancel the worker's completed teardown transition.

Internal child-worker command mailboxes are unbounded and prioritized, with
one logical producer per worker. They do not impose data-channel backpressure.
Supervisor-facing component methods hide any mailbox or event-loop mechanism.
Data, notification, and worker-to-worker channels are bounded and
cancellation-aware. Every such processing-session data channel has capacity:

```text
PROCESSING_DATA_CHANNEL_CAPACITY = 1024
```

The separately owned graph-update feed is not a processing data channel under
this constant. The following table is the exhaustive ownership and
session-channel disposition policy. The exact channel or worker belongs in
`diagnostic`; it does not select control flow.

Every bounded processing data channel reports current occupancy and high-water
mark. Capacity faults remain counted by their typed `FaultKind`.

| Condition | Fault kind | Active Resync/Rebuild | Live | Expected teardown or completed shutdown |
|---|---|---|---|---|
| Bounded session data channel is full and ordered continuity may be lost | `SessionContinuityLost` | `Require(Resync)`; coalescing never weakens an existing Rebuild obligation | `Require(Resync)` | No fault |
| Bounded session data endpoint is closed or unavailable | `Ownership(SessionDataEndpointLost)` | `Retry`, retaining the current recovery obligation | `Require(Resync)` | No fault |
| Supervisor-facing managed component is unavailable, or its reliable event path closes while it should be live | `Ownership(ManagedComponentUnavailable)` | `Fatal` | `Fatal` | No fault after completed component shutdown |
| Internal command path closes while its worker should be live | `Ownership(InternalControlPathLost)` | `Fatal` | `Fatal` | No fault after completed worker shutdown |
| A permanent worker exits or panics unexpectedly | `Ownership(UnexpectedWorkerTermination)` | `Fatal` | `Fatal` | No fault after completed worker shutdown |
| Duplicate, invalid-state, or otherwise invalid forward lifecycle command or milestone | `Ownership(InvalidLifecycleControl)` | `Fatal` | `Fatal` | Not applicable |

NotificationRouter destination saturation is the first row of this table. It
reports `SessionContinuityLost`; the particular notification destination is
diagnostic context and does not select a different disposition. Disabling both
notification streams after that loss belongs to the
[NotificationRouter contract](node-service.md#notificationrouter) and does not
mean that both streams independently lost a notification.

Every `Retry` or `Require` result performs complete session teardown under the
rules above. A session endpoint loss proves that the current run topology can
no longer make progress, but does not by itself prove persisted-state
inconsistency: recovery therefore retries its existing obligation, while Live
requires Resync to establish a new session. A managed-component or permanent
worker/control failure has no architecture-defined local restart and is
therefore Fatal. Cancellation or closure caused by the expected teardown path
produces no `ComponentFault`.

The Fatal ownership rows apply whenever Supervisor is Running, including while
it is Idle or attempting to start a session; the Active-recovery and Live
columns do not limit them to an installed processing run.

The graph-update feed is the exception: the
[API ingress contract](api-ingress.md#in-process-graph-update-feed--settled)
reports a continuity gap without blocking processing, and the
[API publication reconstruction contract](api-publication.md#universal-api-reconstruction--settled)
rebuilds the derived image without requesting processing recovery.
Detailed Tokio fairness and drain mechanics remain deferred in the
[decision register](../decisions/deferred.md).

`ResyncEngine::start` and the processor `Begin`, `Catchup`, and `Live` commands
are exact-once and state-specific. Duplicate or invalid-state delivery reports
`Ownership(InvalidLifecycleControl)`. Successful `start` means the operation
was accepted; later milestones
or faults report the run's outcome. The two processor `Live` sends need not be
simultaneous; enqueue is not completion. `ResyncEngine::deactivate` completes
only at Idle and is idempotent there. `ResyncEngine::shutdown` is terminal,
idempotent from every state, supersedes an in-progress `deactivate`, and
completes only after full shutdown. Processor Deactivate and Shutdown commands
retain their corresponding completed-barrier acknowledgements. Unexpected
internal command-channel closure while its worker is meant to live reports
`Ownership(InternalControlPathLost)`.

`Rebuild` intent must not outlive successful PP-boundary sealing.
`PpBoundarySealed` is an exact-once upward milestone event, never a command to
BlockProcessor. BlockProcessor emits it under its
[PP-boundary contract](block-processing.md#pp-boundary-phase-behavior--settled).
ResyncEngine observes it before permitting Catchup and propagates the milestone
to Supervisor. BlockProcessor has already enqueued `PublishPostSeal` to its
lifecycle-marker worker under its
[marker-delivery contract](block-processing.md#graph-lifecycle-marker-delivery--settled).
Supervisor only downgrades both desired and active recovery to `Resync`.
Duplicate or invalid-state milestone delivery reports
`Ownership(InvalidLifecycleControl)`. Entering Live satisfies
and clears the remaining recovery requirement.

## ResyncEngine — settled

Supervisor owns `Arc<ResyncEngine>` and uses this public control surface:

```rust
impl ResyncEngine {
    async fn start(
        &self,
        mode: RecoveryMode,
        session: ProcessingSession,
    ) -> Result<(), ResyncEngineError>;

    async fn deactivate(&self) -> Result<(), ResyncEngineError>;

    async fn shutdown(&self) -> Result<(), ResyncEngineError>;
}
```

These methods are the complete Supervisor-facing ResyncEngine control
interface. Private mailbox or event-loop mechanics remain internal to
`kgi-processing`.

The engine prepares one common structure for Resync and Rebuild:

```rust
struct PreparedSync {
    rpc: Arc<ValidatedRpcClient>,
    db: Arc<ValidatedDbClient>,
    anchor: MaterializedSyncAnchor,
    boundary_seal_blue_score: u64,
}
```

### Processor Begin payloads

```rust
struct BlockProcessorBegin {
    rpc: Arc<ValidatedRpcClient>,
    db: Arc<ValidatedDbClient>,
    anchor: MaterializedSyncAnchor,
    boundary_seal_blue_score: u64,
    graph_updates: GraphUpdateProducer,
}

struct VspcProcessorBegin {
    rpc: Arc<ValidatedRpcClient>,
    db: Arc<ValidatedDbClient>,
    anchor: MaterializedSyncAnchor,
    graph_updates: GraphUpdateProducer,
}
```

After preparation, ResyncEngine constructs both Begin payloads from the same
`PreparedSync` and the `GraphUpdateProducer` received by `start`, and
sends the mode-specific `BeginResync` or `BeginRebuild` command to each
processor. BlockProcessor receives the exact RPC and DB generations, the
committed anchor, the seal threshold, and its producer handle for the run's
fresh graph-update channel. VspcProcessor receives the same RPC and DB
generations, anchor, and another producer handle for that channel; it uses the RPC client only for the
selected-parent attribution contract owned by VspcProcessor. Both commands are
exact-once and have no acknowledgement. Once both have been enqueued, the
common pump may start.

Once prepared, both modes use the same block/VSPC synchronization pump. Their
only material difference is storage policy before PP-boundary sealing.

### Boundary seal threshold construction

ResyncEngine is the sole constructor of the boundary seal threshold for both
recovery modes:

```rust
enum BoundarySealThresholdError {
    Overflow,
    AboveMaximum,
}

fn construct_boundary_seal_blue_score(
    boundary_hash: BlockHash,
    boundary_blue_score: u64,
    genesis_hash: BlockHash,
    anticone_finalization_depth: u64,
) -> Result<u64, BoundarySealThresholdError>;
```

Its complete behavior is:

```text
boundary_hash == genesis_hash:
    0

otherwise:
    checked(boundary_blue_score + anticone_finalization_depth)
```

The Genesis branch consumes the zero-blue-score invariant already established
by either NodeService's normalized block or StorageService's processing-valid
database snapshot; it does not accept an arbitrary Genesis score. A malformed
node Genesis is rejected before this function and therefore before database
replacement.

The result must be at most the shared `MAX_BLUE_SCORE`. `Overflow` or
`AboveMaximum` reports `ScoreOutOfRange(BoundarySealThreshold)` with Fatal
disposition under the fault policy above; never wrap, saturate, or continue
with an unreachable threshold. The anticone depth comes from the run's exact
`ValidatedNodeInfo.consensus` and is current-session recovery input, not
persisted node metadata or a database-compatibility field.

For Resync, invoke the function with the database PP hash and persisted
`db_pp_blue_score` returned by reconciliation. For Rebuild, invoke it with the
normalized current node pruning-point hash and blue score before any storage
replacement. Only a successful result may populate
`PreparedSync.boundary_seal_blue_score` and the BlockProcessor Begin payload.
The valid Genesis boundary score is zero under the storage invariants, so its
threshold is zero and requires no addition.

### Resync preparation

ResyncEngine first calls
`ValidatedRpcClient::current_pruning_point_block()` on the run's exact RPC
generation. It then passes that block's hash to
`ValidatedDbClient::reconciliation_snapshot(current_node_pp)`. The
[storage contract](storage.md#reconciliation-snapshot--settled) solely owns the
returned state and snapshot shapes, database reads, committed-sink derivation,
and Materialized results for the current node PP and committed sink.
ResyncEngine owns the call ordering, result dispositions, and node-side
validation below; it does not reconstruct storage materiality.

An Empty state is genuinely fully empty and requests a distinct Rebuild run
because PP, score, and sink are absent. A
`NodePpNotMaterialized` result also requests Rebuild. A missing or incoherent
committed sink and any other valid schema with inconsistent processing
contents likewise request Rebuild but are not treated as Empty.

ResyncEngine uses the run's exact `Arc<ValidatedRpcClient>` and the normalized
[individual recovery GetBlock](node-service.md#individual-recovery-getblock)
contract to obtain the sink header. It compares the returned DAA score with the
stored sink DAA score, then constructs `MaterializedSyncAnchor` from the stored
ID, hash, and selected-parent hash plus the header's blue work and blue score.
A header-only node block is sufficient because no body or transactions are
needed. KGI relies on successful GetBlock GhostDAG enrichment also
establishing the recognition required to use the sink as a GetBlocks
`low_hash`; the
[PUAR](verification.md#current-puar-result) checks that upstream assumption
against the reference revision.

Resync requirements:

1. The current node PP returned by the run's exact validated RPC generation is
   Materialized in the database snapshot.
2. The committed VSPC sink used to construct `MaterializedSyncAnchor` is
   Materialized in the same database snapshot.
3. The node recognizes that committed sink as a usable `low_hash`.
4. `sink.blue_score >= boundary_seal_blue_score` produced by the common
   construction above.

For a Genesis PP the common threshold is zero. A coherent
Genesis-anchored database may use ordinary Resync even while the chain is
younger than `anticone_finalization_depth`; every other reconciliation check
above still applies. `Empty` remains distinct because it has no PP or committed
sink despite also storing `db_pp_blue_score = 0`.

A missing or inconsistent stored sink, a definitive absent response from the
node, a stored/returned DAA-score mismatch, or another failed reconciliation
check reports `Require(Rebuild)` to Supervisor. A transport failure,
cancellation, connection loss, or validated-client loss is instead a session
fault/retry and does not prove that Rebuild is required. NodeService owns
classification of a malformed GetBlock response as
`RecoveryInputInvalid(MalformedGetBlock)`; ResyncEngine applies the bounded
malformed-recovery-input policy above. Rebuild occurs as a separate run.

### Rebuild preparation

ResyncEngine obtains the mandatory current pruning-point block once through
`ValidatedRpcClient::current_pruning_point_block()` on the Rebuild run's exact
validated RPC generation. It constructs the boundary seal threshold from that
block under the common contract above. Only after successful construction does
it pass that same validated `ValidatedNodeBlock` to the only storage API that
clears processing data; StorageService establishes the API-read replacement
gate owned by the storage contract:

```rust
let anchor = db.rebuild_from_pruning_point(pp).await?;
```

The pruning point is mandatory. Storage owns the transaction, retained network
binding, PP-boundary representation, metadata replacement, cache publication,
and returned anchor, including the pruning point's selected-parent hash; see
[storage.md](storage.md#rebuild-transaction--settled). ResyncEngine must not
expose or use a general clear-without-PP primitive. It combines the returned
anchor with the already constructed threshold and exact clients to publish
`PreparedSync`; it does not derive the threshold from the returned sink.

### API session replacement and publication

The [StorageService lifecycle](storage.md#storageservice-lifecycle--settled)
owns API database-generation retirement and autonomous reacquisition. From the
already installed ordered `StorageServiceEvent` stream, Supervisor maps
`ApiDbRetired(client)` to `ApiDbGenerationEvent::Retired(client)` and
`ApiDbPublished(client)` to `ApiDbGenerationEvent::Published(client)`, then
forwards each result in order through
`ApiService::update_api_db_generation`. This is a normal downward control call,
like `reset`; ApiService never calls StorageService and sends no reverse
generation-loss event.

Supervisor does not start, coalesce, cancel, or classify API-generation
acquisition tasks. Forwarding a generation event changes neither processing
recovery intent nor the current processing run. Supervisor's transition into
terminal shutdown is the forwarding cutoff: it starts no new
`update_api_db_generation` call after that transition. StorageService is shut
down later under the global teardown order, and its subsequent events follow
the drain-and-discard rule below.

For every processing run, Supervisor creates one fresh paired
`GraphUpdateProducer` and `GraphUpdateReceiver`. It calls `ApiService::reset`
with the receiver and current `RecoveryMode`, then calls
`ResyncEngine::start` with the producer in `ProcessingSession`. ResyncEngine
only clones that producer
into both processor Begin payloads; it never invokes ApiService or produces
lifecycle markers. BlockProcessor consumes its clone under the linked marker
contract.

The session order is:

```text
ApiService::reset(graph_updates, recovery_mode)
    -> ResyncEngine::start(mode, session)
    -> GraphUpdate::PublishPostSeal
    -> GraphUpdate::Live
```

ApiService's complete `reset` effects belong to the
[ApiService reset contract](api-service.md#reset-and-recovery-time-availability--settled).
The [API publication lifecycle](api-publication.md#head-publication-lifecycle-and-stream-alignment--settled)
owns lifecycle-marker consumption and publication effects, while its
[reconstruction contract](api-publication.md#universal-api-reconstruction--settled)
owns the reaction to a reported gap. This document owns Supervisor's `reset`
call point and processor command ordering;
[BlockProcessor](block-processing.md#graph-lifecycle-marker-delivery--settled)
owns marker-command enqueue points. `reset` returns after reliable acceptance,
and processing never waits for its application or publication completion. A
recoverably aborted session sends no invalidation control; the next processing
attempt's `reset` call is the sole
ApiService session-supersession event. Resync-to-Rebuild escalation therefore
legitimately calls `reset` again with a fresh ingress.

For ordinary Resync, Supervisor calls `reset(Resync)` and then
`start(Resync, ...)`. After successful reconciliation, `BeginResync` starts
BlockProcessor in PostSeal; its
[Begin behavior](block-processing.md#graph-lifecycle-marker-delivery--settled)
owns the `PublishPostSeal` command enqueue. If reconciliation instead requests
Rebuild, complete teardown and start the distinct Rebuild attempt with another
`reset` call; no special API invalidation is required.

For Rebuild, Supervisor calls `reset(Rebuild)` and then `start(Rebuild, ...)`
without waiting for ApiService to apply the reset. StorageService's replacement
gate, rather than `reset`,
excludes API database phases before `rebuild_from_pruning_point` changes data.
After processor Begin, ResyncEngine permits no Catchup until it observes
BlockProcessor's definitely committed `PpBoundarySealed` event and forwards it
to Supervisor. The BlockProcessor seal contract owns its preceding marker
command enqueue.
For Genesis, BlockProcessor emits that milestone while handling `BeginRebuild`
under its [PP-boundary contract](block-processing.md#pp-boundary-phase-behavior--settled).
ResyncEngine handles it through the same forwarding path, including
Supervisor's `Rebuild -> Resync` downgrade.

`PpBoundarySealed` has no API database-generation role. StorageService
autonomously publishes the coherent replacement and Supervisor forwards that
event independently. `PublishPostSeal` may arrive before the replacement
`Published` event; ApiService then keeps construction pending until a current
generation is available. The API owner alone opens public database-backed
reads when the replacement publication becomes Active.

When the global Live conditions are satisfied, ResyncEngine sends the existing
processor Live commands and emits `EnteredLive`. The BlockProcessor marker
contract owns the effect of its Live command; Supervisor does not produce the
marker.

## Resync block/VSPC pump — settled

The common pump holds a normalized GetBlocks page and one pending VSPC V2
response. It obtains both through the exact
[NodeService RPC contract](node-service.md#rpc-normalization), including the
pinned VSPC request arguments and advancing-cursor assumption. An empty V2
page is a pump/Catchup hint, not a VSPC change.
When NodeService rejects a VSPC response as malformed, ResyncEngine dispatches
nothing, does not advance the cursor, and follows the shared malformed
recovery-response policy above.

Both synthetic streams start from the committed `MaterializedSyncAnchor` sink:

- GetBlocks uses the sink hash as its inclusive `low_hash`, and NodeService
  strips that repeated anchor during normalization;
- VSPC V2 uses the same sink hash as its traversal start, which is excluded
  from the returned `added` path;
- immediately after fresh Genesis bootstrap the sink is Genesis, while a later
  Genesis-anchored run may start from a newer committed sink; and
- synthetic ORIGIN is never an RPC anchor.

```text
request VSPC V2 from current low_hash
determine its destination
dispatch GetBlocks blocks until that destination has been sent to BlockProcessor
pause the remaining page suffix
send the synthetic VspcChange to VspcProcessor
request the next VSPC response
resume the page suffix
```

"Sent" means accepted by the BlockProcessor channel, not committed. Processor
failures either directly request recovery or cascade into an unprocessable
VSPC backlog that requests recovery.

After NodeService completes every hash-observable continuity check, each
incremental VSPC request uses the preceding response destination as its
provisional next `low_hash`. Selected-parent path continuity is validated
later by the
[atomic VSPC transaction](storage.md#atomic-vspc-transaction--settled). Its
typed failure aborts the run and discards this in-memory cursor and every later
queued synthetic response. Only definitely committed transitions establish the
ordered, gapless synthetic VSPC stream.

Notifications go directly to processors; the engine never journals or
redispatches them.

### Entering recovery phases

Before a fresh Begin:

1. disable/unsubscribe processing notifications;
2. ensure Supervisor has installed the fresh graph-update topology through
   `reset` and passed its producer through `start`; and
3. send Begin to BlockProcessor and VspcProcessor. `BeginResync` enqueues the
   `PublishPostSeal` worker command, while Rebuild waits for the post-Begin
   seal point.

Begin needs no acknowledgement. Before Catchup:

1. send Catchup commands, including lower bounds;
2. start both remote subscriptions with NotificationRouter still Disabled;
3. after both starts succeed, enable the router and publish the client's
   subscription state Enabled.

Processor-local notification gates are authoritative for immediate dropping.
Callbacks arriving while the router remains Disabled during activation are
intentionally dropped without overlap credit or a recovery request. Synthetic
pumps continue until the [Live admission](#live-admission) predicate is
satisfied.

### Catchup trigger

Call the run's exact validated RPC generation's
`catchup_sink_sample()` before starting the GetBlocks scan. Use its returned
hash and DAA score as the initial `catchup_sink` marker and track that marker
through the ordered synthetic VSPC pump:

```text
Unknown -- marker equals the synthetic cursor or occurs in added --> Present
Present -- marker in a later removed --> Removed
```

Only `Present` can authorize Catchup. Cursor equality covers an already
synchronized marker that will not be repeated in `added`. `Removed`
invalidates the marker. An `Unknown` marker's absence from `removed` is not
evidence because it may have been reorged before the pump admitted it. The
VSPC RPC's removed suffix is complete even when its added path is batch
limited.

Hold a normalized GetBlocks page containing the marker before dispatch and
call `catchup_sink_sample()` again for the refresh. Use checked DAA-score
subtraction between the returned sample and the marker. Use
`catchup_max_daa_gap` from the run's exact validated `KgiConsensusParams`; the
[NodeService parameter contract](node-service.md#consensus-parameter-resolution)
owns its checked construction and admissibility.

If the marker is `Present`, the fresh score is not lower, and the gap is at
most the threshold, queue both Catchup commands and complete subscription
activation before dispatching the entire held page under Catchup. If the gap
is larger, replace the marker with the fresh sink, reset it to `Unknown`, and
remain in Resync. Re-evaluate immediately when the replacement is already in
the held page. Otherwise continue scanning. Remember a marker already seen by
GetBlocks when its VSPC state has not caught up, and reconsider eligibility at
a later complete page boundary. A removed marker or decreasing score is
replaced and cannot authorize Catchup.

Failure of either sink-sample call applies its typed lifecycle disposition to
the complete recovery attempt. In particular, a failed refresh does not
dispatch the held GetBlocks page, enter Catchup, or replace the marker.
Malformed samples follow the shared malformed recovery-input policy; transport
or validated-generation loss retries the whole attempt while retaining the
current recovery obligation, and expected teardown cancellation is not a
fault.

The rolling marker is the primary path. If it has not authorized Catchup,
retain three independent fallbacks:

```text
the normalized GetBlocks page's global maximum ConsensusOrder is not final
OR
the normalized GetBlocks page contains fewer than three blocks
OR
the VSPC V2 page is empty
```

A stable sink-reaching page with appended anticone leaves the sink maximum
before the final block. Under the `L + 1` GetBlocks core budget, a below-sink
capacity stop has at least three normalized blocks, so `< 3` is a one-way
sink-reaching proof. Evaluate these predicates on the complete page and enter
Catchup before dispatching it.

An empty VSPC V2 page enters Catchup without moving its cursor. Requery from
the same cursor at the target block interval; a short nonempty page is not a
fallback. The rolling marker is the normal path and these fallbacks cover an
exceptional failure to transition through it. A material omission exposed
under strict pre-Catchup processing uses the existing `Require(Resync)` fault
path. The synthetic pump continues through Catchup and observes later reorgs.
ResyncEngine must have
observed BlockProcessor's definitely committed `PpBoundarySealed` event before
the primary path or a fallback can enter Catchup. Eligibility while still
PreSeal fails the current recovery.
Resync starts PostSeal only after reconciliation.

### Late notification filtering

Transport delivery cannot reliably distinguish a late message after
unsubscribe from an early message after resubscribe. Processor-local state and
objective session bounds therefore govern admission.

Begin resets processor-local state and closes notification gates. Catchup
supplies the objective block and VSPC lower bounds derived from the current
session anchors. Their exact filtering and credit rules belong to
[block-processing.md](block-processing.md#catchup-filtering-and-overlap) and
[vspc-processing.md](vspc-processing.md#catchup-filtering-crossing-and-overlap--settled).

## Catchup overlap and transition to Live — settled

Each processor owns an `AtomicBool` overlap flag shared read-only with the
engine. Begin resets it and Catchup begins measurement. ResyncEngine reads
both flags only at a fully dispatched GetBlocks-page boundary.

### Blocks

BlockProcessor owns source accounting, late filtering, overlap proof, and the
rule that valid orphans do not prevent Live; see
[block-processing.md](block-processing.md#catchup-filtering-and-overlap).

ResyncEngine owns an exact Catchup-only `catchup_sent` set. Initialize it on
Catchup, insert a hash only after successful synthetic-channel enqueue, and
filter later synthetic repeats before dispatch without granting overlap
credit. Clear it on a new Begin, successful global Live entry, or Deactivate.
A returned GetBlocks page must be fully dispatched before the engine observes
overlap or abandons its suffix.

### VSPC

VspcProcessor owns synthetic priority, notification retention, lower-bound
filtering, structural crossing, overlap proof, and its component-local Live
behavior; see
[vspc-processing.md](vspc-processing.md#catchup-filtering-crossing-and-overlap--settled)
and [vspc-processing.md](vspc-processing.md#component-local-live-transition--settled).
ResyncEngine only decides when to end synthetic production and enqueue that
component's Live command.

### Live admission

At a fully dispatched GetBlocks-page boundary:

```text
PostSeal == true
&& block_overlap == true
&& vspc_overlap == true
```

is the complete Live-admission predicate. It proves that both synthetic streams
have converged with their active notification streams at a complete GetBlocks
page boundary. It does not certify that KGI has copied the node's entire
retained body DAG or that no callback was dropped during subscription
activation.

When the predicate becomes true, the producer gate is open and every graph
update causally establishing it has already completed its offer. Only then
perform the transition in order:

1. stop issuing synthetic RPC requests and stop/join both synthetic producers;
2. successfully enqueue VspcProcessor's existing Live command;
3. successfully enqueue BlockProcessor's existing Live command;
4. emit `EnteredLive`.

The [BlockProcessor marker contract](block-processing.md#graph-lifecycle-marker-delivery--settled)
owns the API effect of its Live command.
`EnteredLive` remains the Supervisor milestone for recovery intent and retry
state; it has no API-marker role.

VspcProcessor's reaction and readiness behavior are defined in its focused
contract. BlockProcessor retains valid queued and orphan work. `EnteredLive`
does not mean either processor's queues or dependency state are empty.

#### Recovery scope and omitted body tips

KGI v2's required graph is observation-based and is not a complete snapshot of
the node's retained body DAG. The exact upstream stale-tip enumeration behavior
is owned as an
[accepted unverified risk](verification.md#accepted-unverified-upstream-risk-stale-tip-enumeration)
by the verification policy; Live admission does not depend on its outcome.

A retained node block is outside KGI's required graph unless it is observed
through a normal KGI input: GetBlocks, an Enabled BlockAdded notification,
dependency resolution for an admitted block, or VSPC chain membership. KGI
does not claim body-DAG snapshot completeness.

If an earlier omitted block later becomes required, the existing mechanisms
apply: BlockProcessor dependency resolution obtains missing ancestry;
resolver-confirmed unavailability requests Rebuild; a nonmaterialized VSPC
chain member requests Rebuild; and ordinary processing or transport invariant
failures request their settled recovery disposition. Live admission relies on
those mechanisms when an earlier omission becomes relevant.

Normal processor and stream invariants remain active in Live and request their
settled recovery dispositions when violated.

## Teardown and delivery semantics — settled

### Termination-triggered global shutdown — settled

Supervisor implements the
[`Shutdown`](overview.md#process-termination-signal-adapter--settled) target
contract and owns the single `Signals<Supervisor>` registration. Signal
installation completes before Supervisor permits managed components to enter
their running state; an installation panic therefore prevents any managed
component from entering Running.
The synchronous `Shutdown::shutdown` entry point performs no teardown work in
the signal callback. It submits the existing idempotent terminal-shutdown
trigger and returns. Supervisor consumes that trigger through its ordinary
lifecycle processing and enters terminal shutdown exactly once, even if
another cause or the second signal has already requested it. Supervisor never
forwards the signal registration to a service, processor, or worker.

The first signal therefore invokes the global shutdown sequence below rather
than defining another teardown path. Further signals follow the
[adapter-owned policy](overview.md#process-termination-signal-adapter--settled).
If the shutdown barriers complete, the process returns normally.

On Deactivate, ResyncEngine performs this barrier in order:

1. close local routing gates and disable notifications;
2. cancel and join synchronization producers under the already-produced RPC
   fault-delivery rule above;
3. clear engine-local buffers;
4. send `Deactivate` to processors;
5. await processor acknowledgements, including their teardown-barrier
   descendants; BlockProcessor's already accepted marker delivery follows its
   [separate drain rule](block-processing.md#graph-lifecycle-marker-delivery--settled)
   and is not part of this barrier;
6. release every processing-session clone of the validated RPC and DB handles;
7. drop `ProcessingSession`; and
8. emit `Deactivated` and enter Idle.

The owning services may retain their validated generations after Deactivate.
For global shutdown, Supervisor first enters terminal shutdown and starts no
new recovery attempt. Supervisor then starts `ApiService::shutdown` and
`ResyncEngine::shutdown` without waiting for either method to complete before
starting the other. It awaits both completed method barriers before calling
`NodeService::shutdown` and then `StorageService::shutdown`. This releases every
processing and API client before their owning services stop and keeps
StorageService last. Supervisor continues draining each reliable service-event
stream until its owning service completes shutdown. After entering terminal
shutdown it discards those events instead of retaining a generation, starting
recovery, or forwarding a new API generation. Closure of a service-event path
after that service's completed shutdown is expected termination rather than a
fault.

Dropping the graph-update receiver during this coordinated barrier unblocks a
marker worker awaiting lossless delivery. Resulting producer closure is
expected teardown cancellation rather than a processing fault. ApiService owns
the local effects and completion semantics of its
[shutdown barrier](api-service.md#apiservice-shutdown--settled); processor-specific
draining duties remain in their focused documents.

KGI applies no automatic shutdown timeout and performs no time-triggered
escalation. Supervisor waits for the shutdown barriers to complete unless the
process is terminated externally or the signal adapter applies its
[count-based operator escalation](overview.md#process-termination-signal-adapter--settled).

The graph-update path is deliberately separate. The
[API ingress contract](api-ingress.md#in-process-graph-update-feed--settled)
owns gap reporting, and the
[API publication reconstruction contract](api-publication.md#universal-api-reconstruction--settled)
owns rebuilding the API image without interrupting processing.
