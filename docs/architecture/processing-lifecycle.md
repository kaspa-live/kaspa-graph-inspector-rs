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
[vspc-processing.md](vspc-processing.md); and API publication effects to
[api.md](api.md). Shared value types belong to
[domain-model.md](domain-model.md).

## Processing session and resource acquisition — settled

```rust
struct ProcessingSession {
    rpc: Arc<ValidatedRpcClient>,
    db: Arc<ValidatedDbClient>,
}
```

Supervisor waits for usable node and storage generations only when a recovery
is desired and the engine is idle. The two waits are polled concurrently with
Supervisor commands/events; the Supervisor main loop must not block.

Partial acquisition may be retained while waiting for the other resource.
StorageService can open, lock, and inspect an Uninitialized database while the
node wait continues. Once the validated RPC generation is available,
Supervisor supplies its `(network_id, genesis_hash)` to StorageService for the
authorized atomic first initialization. StorageService alone performs the
`Uninitialized -> Empty` transition and publishes the resulting DB generation.
Before starting, recheck that the captured recovery obligation is still the
latest `desired_recovery`, the engine is Idle, and both services still publish
those exact acquired RPC and DB `Arc` generations as Ready.

The exact `Arc<ValidatedRpcClient>` and `Arc<ValidatedDbClient>` are
passed in `Start` and then through the session to all consumers, including
DependencyResolver. This guarantees one resource generation per run.
Before sending `Start`, require the validated node's `(network_id,
genesis_hash)` to equal the immutable binding exposed by that DB generation.
A mismatch is terminal pairing rejection: never rebind the database or infer
that Rebuild can repair it.

## Supervisor and recovery intent — settled

```rust
enum RecoveryMode {
    Resync,
    Rebuild,
}
```

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
eventually satisfy. Sending `Start` does not consume it. `active_recovery` is
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
enum NotificationStream { BlockAdded, Vspc, Both }
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
enum FaultKind {
    ServiceGenerationLost(ServiceKind),
    NotificationContinuityLost(NotificationStream),
    NotificationInputInvalid(NotificationInputKind),
    RecoveryInputInvalid(RecoveryInputKind),
    ReconciliationFailed,
    MaterialityViolation,
    DependencyUnavailable,
    BoundedStateExhausted(BoundedProcessingState),
    ScoreOutOfRange(ScoreRangeFault),
    Persistence(PersistenceFault),
    Ownership,
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
```

`deactivation_requested` is internal and absent from public status.
`SystemStatus` combines Supervisor, NodeService, StorageService, and processing
observations; it is eventually consistent and never a synchronization or
recovery input.

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

When the same validated RPC and DB generations remain Ready, whole-attempt
recovery retries use nominal delays `1s, 2s, 4s, 8s, 16s, 30s`, capped at
`30s`, with equal jitter from 50% through 100%. Reset that general backoff on
`EnteredLive`, a new validated RPC or DB generation, or a stronger recovery
obligation. Generation loss waits for the corresponding service reconnect
loop without adding this delay. `Require(Resync)` and `Require(Rebuild)` do not
consume or wait on the Retry sequence. All waits are lifecycle-cancellable.

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

Each permitted malformed-input Retry waits for NodeService to publish a new
validated RPC generation and never reuses or reissues the operation on the
retired handle. NodeService's reconnect backoff supplies the delay, so the
general recovery Retry delay is not added. A malformed VSPC notification
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
`AmbiguousCommit` wait for StorageService replacement rather than reusing the
retired generation, without adding the general recovery Retry delay.

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

Faults and milestones use reliable owner-directed events; latest-value
status may skip intermediate states. Each run retains its first causal fault
diagnostically. Unexpected permanent-worker exit, panic, closed command
channel, or invalid forward command is a typed ownership/session or fatal
fault, not ordinary DAG discontinuity. A dropped barrier acknowledgement
receiver does not cancel the worker's completed teardown transition.

Command mailboxes are unbounded and prioritized, with one logical producer per
worker. They do not impose data-channel backpressure. Data, notification, and
worker-to-worker channels are bounded and cancellation-aware. Full means
`Require(Resync)` when ordered session continuity may have been lost;
closed/unavailable means a session ownership fault; cancellation during
expected teardown is not a fault. The graph observer feed is the exception:
its loss invalidates the API image without disrupting processing.
Detailed Tokio fairness and drain mechanics remain deferred in the
[decision register](../decisions/deferred.md).

`Start`, processor `Begin`, `Catchup`, and each processor's `Live` are
exact-once, state-specific commands. Duplicate or invalid-state delivery is
Fatal. The two `Live` sends need not be simultaneous; enqueue is not
completion. Only `Deactivate` and `Shutdown` have completed-barrier
acknowledgements. `Deactivate` is idempotent to Idle. `Shutdown` is terminal
and idempotent from every state, supersedes an in-progress Deactivate, and
acknowledges only after full shutdown. Unexpected command-channel closure is
Fatal while its worker is meant to live.

`Rebuild` intent must not outlive successful PP-boundary sealing.
`PpBoundarySealed` is an exact-once upward milestone event, never a command to
BlockProcessor. BlockProcessor emits it under its
[PP-boundary contract](block-processing.md#pp-boundary-phase-behavior--settled).
ResyncEngine observes it before permitting Catchup, sends ApiService the
PostSeal publication trigger, and propagates the milestone to Supervisor.
Supervisor then downgrades both desired and active recovery to `Resync`.
Duplicate or invalid-state milestone delivery is Fatal. Entering Live satisfies
and clears the remaining recovery requirement.

## ResyncEngine — settled

Commands:

```text
Start { mode, rpc, db }
Deactivate
Shutdown
```

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
}

struct VspcProcessorBegin {
    rpc: Arc<ValidatedRpcClient>,
    db: Arc<ValidatedDbClient>,
    anchor: MaterializedSyncAnchor,
}
```

After preparation and the applicable API Reset/publication ordering below,
ResyncEngine constructs both Begin payloads from the same `PreparedSync` and
sends the mode-specific `BeginResync` or `BeginRebuild` command to each
processor. BlockProcessor receives the exact RPC and DB generations, the
committed anchor, and the seal threshold. VspcProcessor receives the same RPC
and DB generations and anchor; it uses the RPC client only for the
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
node Genesis is rejected before this function and therefore before API Reset
or database replacement.

The result must be at most the shared `MAX_BLUE_SCORE`. `Overflow` or
`AboveMaximum` reports `ScoreOutOfRange(BoundarySealThreshold)` with Fatal
disposition under the fault policy above; never wrap, saturate, or continue
with an unreachable threshold. The anticone depth comes from the run's exact
`ValidatedNodeInfo.consensus` and is current-session recovery input, not
persisted node metadata or a database-compatibility field.

For Resync, invoke the function with the database PP hash and persisted
`db_pp_blue_score` returned by reconciliation. For Rebuild, invoke it with the
normalized current node pruning-point hash and blue score before the API Reset
barrier or any storage replacement. Only a successful result may populate
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
block under the common contract above. Only after successful construction and
the API Reset barrier does it pass that same validated `ValidatedNodeBlock` to
the only storage API that clears processing data:

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

Every prepared processing run replaces API publication continuity through the
existing reliable processing-to-ApiService control path. The lifecycle sends
these controls in session order:

```text
Reset -> PublishPostSeal -> PublishLive
```

The effects of these controls, including historical-read availability and
GraphPublication behavior, are defined in
[api.md](api.md#reset-and-recovery-time-availability--settled). This document
owns their send points. `Reset` has a completed-effect acknowledgement;
`PublishPostSeal` and `PublishLive` are reliable and exact-once, but processing
does not wait for publication completion.

When a recoverable fault terminates a processing session after its Reset has
completed, send one reliable `InvalidateSession` control. Send it as soon as
the fault is accepted and before any later session's Reset. It is unnecessary
for a pre-Reset preparation failure or Fatal shutdown. This prevents an
aborted session from retaining an active publication state; ApiService owns
the control's complete effects, including historical-read availability.

For ordinary Resync, perform read-only reconciliation first. A failed
reconciliation requests Rebuild without resetting the API. After successful
reconciliation, send the session-replacement Reset and await its
acknowledgement, then send the PostSeal publication trigger before processor
Begin.

For Rebuild, obtain the pruning point, send the database-rebuild Reset, and
await its acknowledgement before `rebuild_from_pruning_point`. After processor
Begin, ResyncEngine sends the PostSeal publication trigger only when it
observes BlockProcessor's definitely committed `PpBoundarySealed` event.
For Genesis, BlockProcessor emits that milestone while handling `BeginRebuild`
under its [PP-boundary contract](block-processing.md#pp-boundary-phase-behavior--settled).
ResyncEngine handles it through the ordinary path, including the PostSeal
publication and Supervisor's `Rebuild -> Resync` downgrade.

When the global `EnteredLive` conditions are satisfied, ResyncEngine sends the
Live publication trigger.

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
2. complete the API Reset acknowledgement; for Resync also send
   `PublishPostSeal`, while Rebuild waits for the post-Begin seal event;
3. send Begin to BlockProcessor and VspcProcessor.

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

Once the predicate holds, perform the transition in order:

1. stop issuing synthetic RPC requests and stop/join both synthetic producers;
2. successfully enqueue VspcProcessor's existing Live command;
3. successfully enqueue BlockProcessor's existing Live command;
4. send ApiService the Live publication trigger; and
5. emit `EnteredLive`.

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

On Deactivate, ResyncEngine performs this barrier in order:

1. close local routing gates and disable notifications;
2. cancel and join synchronization producers;
3. clear engine-local buffers;
4. send `Deactivate` to processors;
5. await processor acknowledgements, including descendant barriers;
6. release every processing-session clone of the validated RPC and DB handles;
7. drop `ProcessingSession`; and
8. emit `Deactivated` and enter Idle.

The owning services may retain their validated generations. Shutdown stops
engine and processors before NodeService and then StorageService. Channel
failures follow the bounded-delivery and ownership semantics above;
component-specific draining duties remain in the focused processor documents.
Exact shutdown timeouts and escalation policy remain deferred in the
[decision register](../decisions/deferred.md).

The graph observer path is deliberately separate: observer loss invalidates
and reloads the API image without interrupting processing. Its behavior is
defined in [api.md](api.md#in-process-api-and-graph-observer-feed--settled).
