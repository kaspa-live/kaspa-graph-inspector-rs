# VSPC processing

## Scope and ownership

This document owns VspcProcessor sequencing, readiness, pending indexes,
history, Catchup overlap, notification crossing, and committed VSPC graph
publication. [NodeService](node-service.md) owns raw notification filtering.
The [processing lifecycle](processing-lifecycle.md) owns synthetic production
and phase coordination. [Storage](storage.md) owns the atomic VSPC mutation.

## Change semantics — settled

Rusty-kaspa orders a virtual selected-parent-chain change as:

```text
removed: old sink backward toward common ancestor, ancestor excluded
added:   common-ancestor child forward to new sink
```

Source and destination hashes derive as:

```text
source = removed.first()                 if removed is nonempty
       = selected_parent(added.first())  otherwise

destination = added.last()
```

The shared `VspcChange` and `ReadyVspcChange` representations are defined in
the [domain model](domain-model.md#vspc-value-types--settled). Pending changes
store `VspcChange` directly and readiness produces `ReadyVspcChange`.

Every admitted nonempty change has a nonempty `added` vector. A change with a
nonempty removed chain and an empty added path is impossible under the
selected-sink monotonicity assumption reviewed by the
[PUAR](verification.md#current-puar-result) and has no invented fallback
destination. The [NotificationRouter](node-service.md#notificationrouter) and
[RPC normalization](node-service.md#rpc-normalization) contracts own
source-specific rejection, and the
[processing lifecycle](processing-lifecycle.md#resync-blockvspc-pump--settled)
owns the resulting recovery disposition. Neither source admits the shape to
VspcProcessor.

NodeService also discards a raw notification with both vectors empty before
constructing or sending `VspcChange`. It earns no overlap credit and consumes
no processor capacity. An empty VSPC V2 page is synchronization information
owned by the pump, not a VSPC event. `added` and `removed` never contain
Genesis; a derived source can be Genesis for the first transition.

## Processor lifecycle and gates — settled

The Begin variants use the
[`VspcProcessorBegin`](processing-lifecycle.md#processor-begin-payloads)
payload owned by the processing lifecycle. Conceptual commands are
`BeginRebuild(VspcProcessorBegin)`, `BeginResync(VspcProcessorBegin)`,
`Catchup`, `Live`, `Deactivate`, and `Shutdown`. Commands have priority over
data inputs. VspcProcessor receives the processing session's exact validated
RPC and DB generations.

A Begin command resets all run-local pending state, history, overlap, and
phase state, installs the supplied RPC and DB generations, and closes the local
notification gate. It sets `committed_vspc_sink = anchor.point` and seeds the
new history from the same anchor as defined below, then enters the pre-Catchup
synthetic-priority phase. Continue polling the
notification receiver while the gate is closed and discard notifications
immediately rather than accumulating them. Catchup opens the gate with its
objective synthetic-sink lower bound. Begin has no acknowledgement. Deactivate
clears run-local state, releases both processing-session client clones, and
acknowledges only after the local barrier is complete.

## Pending state and history — settled

Resolution supports both event-before-block and block-before-event. Keep:

- one ordered synthetic FIFO;
- raw unresolved notifications indexed by destination hash;
- resolved notification candidates ordered by destination `ConsensusOrder`;
- pending ID to pending change;
- missing hash to waiting pending IDs;
- hash to recent `PersistedBlock` history; and
- `ConsensusOrder` to hash for ordered pruning and history lookup.

The dual-indexed materialization history is conceptually:

```rust
struct MaterializedHistory {
    by_hash: HashMap<BlockHash, PersistedBlock>,
    by_order: BTreeMap<ConsensusOrder, BlockHash>,
}
```

After clearing the prior run's history, either Begin command constructs and
inserts this seed into both indexes:

```rust
PersistedBlock {
    point: anchor.point,
    selected_parent: anchor.selected_parent,
}
```

For a Genesis anchor the selected parent is synthetic ORIGIN. This history
record is constructed locally from Begin; Genesis is never received through
the ordinary `BlockPersisted` data path. For every later block, the history
record comes from BlockProcessor's `PersistedBlock` delivery. The anchor seed
and later records provide the same non-null point and selected-parent shape.
The anchor and every `PersistedBlock` originate from a semantic
[`Materialized`](domain-model.md#identity-and-materiality-vocabulary--settled)
result. VspcProcessor receives later history entries only through
BlockProcessor's `PersistedBlock` delivery.

Block hash is the sole replay identity under the
[domain-model immutability contract](domain-model.md#shared-value-types--settled).
When `by_hash` already contains an incoming `PersistedBlock` hash,
VspcProcessor reuses the existing history record without comparing payload
fields and leaves both indexes unchanged. For a new hash, insert `by_hash` and
`by_order` as one local state transition with no await or observable partial
update between them.

These pending structures share one bounded capacity. A single
`HashMap<BlockHash, Vec<VspcChange>>` cannot represent multi-dependency
readiness and ordered candidate selection.
Define:

```text
MAX_PENDING_VSPC_CHANGES = 1024
```

VspcProcessor reports pending-candidate occupancy and high-water mark.

Capacity counts distinct pending candidates. The notification collision checks
below precede capacity admission because an existing-destination collision does
not consume a new slot. If a distinct synthetic or notification candidate
would exceed capacity, reject it before changing any pending index and do not
evict or coalesce existing state. VspcProcessor reports
`BoundedStateExhausted(VspcPending)`, then admits only lifecycle commands until
Deactivate clears the retained run-local state. The
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns routing shutdown and recovery disposition.

Resolve both source and destination before a notification becomes actionable.
An unresolved older notification must not head-block a later actionable one or
earn overlap merely because its raw destination hash is known.

Notification insertion first searches both unresolved and resolved pending
notification state for the destination hash. This check precedes endpoint
resolution and committed-sink filtering:

- no pending notification with that destination permits insertion;
- identical `removed` and `added` vectors report
  `DuplicatePendingTransition`; and
- different vectors for the same destination report
  `ContradictoryDestination`.

Neither collision is coalesced. Synthetic and notification candidates remain
separate streams, so a synthetic candidate and a notification with the same
destination do not collide under this rule; Catchup overlap handles that
case.

Before committing a notification in any phase, scan the currently resolved
candidates. At most one distinct notification candidate may be actionable
from the committed sink. Two or more such candidates report
`CompetingNextMove`; VspcProcessor must not choose between them.

VspcProcessor reports these three pending-state violations as
`MalformedVspcChange(DuplicatePendingTransition)`,
`MalformedVspcChange(ContradictoryDestination)`, or
`MalformedVspcChange(CompetingNextMove)`. The
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
solely owns their recovery disposition.

Committed VSPC transitions are continuous and monotonic:

```text
ready[n].source == committed_sink
ready[n].source == ready[n - 1].destination
```

Competing arrivals need not be monotonic before selection. Materialization
history pruning is phase-specific:

```text
Before Catchup: prune entries with order < committed VSPC sink.
Catchup:        do not prune after synthetic or notification commits.
Catchup→Live:   prune entries with order < committed VSPC sink.
Live:           prune entries with order < sink after every successful commit.
```

The strict `<` comparison retains the sink entry required for added-only
source resolution. Do not remove buffered notifications solely because their
destination is below the sink without applying the phase-specific filtering
and overlap rules.

In Live, a rare network-delayed notification that cannot resolve after history
pruning may remain non-actionable in the bounded pending structure. It must not
block a later actionable crossing transition.

## Readiness and materiality — settled

Any synthetic or notification change can commit, including during PreSeal,
only when both predicates hold:

```text
CHAIN_READY: ready.source == committed_vspc_sink
BLOCK_READY: ready.destination has a Materialized history entry
```

`resolve_vspc_readiness()` names this processor-local resolution and sequencing
step; it is not a storage API with an independently settled call signature.
VspcProcessor maintains pending/history state, constructs `ReadyVspcChange`,
and invokes the precise storage operations below. `BLOCK_READY` is established
only by the destination history entry described above.

An actionable reorg calls
`ValidatedDbClient::resolve_materialized_ids(removed + added)` once. The
ordered, cache-first result preserves input positions, including repeats, and
distinguishes absent from boundary-identity members. The batch proves the
semantic Materialized state for directly named chain members, but it does not
supply the destination point or create the history entry required by
`BLOCK_READY`. Added-only changes derive their source and member IDs from
retained materialization history without that database read.

Endpoint `VspcPoint` consensus order comes from `PersistedBlock` history. The
database batch resolves member IDs; it does not derive endpoint order or load
merge sets for the processor.

The head synthetic candidate must continue the committed synthetic sink. Once
its source resolves, a mismatch is a malformed synthetic path rather than a
pending competing candidate. Notifications retain the existing pending rule
because their arrival order need not match committed order.

A block named directly in `added` or `removed` that is confirmed
nonmaterialized is reported as a direct-chain materiality fault without
applying the change. The
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns its recovery disposition.
An identity-only member appearing only in an added block's merge set is an
outside-boundary reference and is ignored by coloring. These cases are not
equivalent.

The storage transaction owns database-relative validation and all persistence
and coloring behavior; see the
[atomic VSPC transaction](storage.md#atomic-vspc-transaction--settled).
The distinct `VspcSourceDiscontinuity` remains attributable to the candidate:
VspcProcessor reports it together with the synthetic or notification source
without running a selected-parent attribution probe.

`VspcPathDiscontinuity(conflict)` is neutral evidence. VspcProcessor holds the
failed head candidate and calls `rpc.full_block(conflict.child)` on its exact
session generation. The probe is lifecycle-cancellable and does not advance
the committed sink or make a synthetic cursor definitive. Compare the returned
`selected_parent` into this processor-local result:

```rust
enum VspcPathAttribution {
    StoredParentConflict,
    CandidatePathConflict,
    StoredAndCandidateConflict,
    AttributionBlockUnavailable,
}
```

```text
current == expected, current != stored:
    StoredParentConflict

current == stored, current != expected:
    CandidatePathConflict

current != expected, current != stored:
    StoredAndCandidateConflict

definitive not-found:
    AttributionBlockUnavailable
```

A malformed probe remains `MalformedGetBlock`. Transport, cancellation, or
generation loss establishes no attribution and retains its existing typed node
error. VspcProcessor reports the attribution result together with the candidate
source, or reports that probe error unchanged. The
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
solely owns their retirement, retry-budget, and recovery-strength consequences.
Definite readiness commits through
`ValidatedDbClient::apply_vspc_change(ready)` and adopts the returned
outcome's destination as the new committed sink.

## Catchup filtering, crossing, and overlap — settled

Catchup supplies an objective synthetic-sink lower bound:

```rust
struct VspcCatchup {
    synthetic_sink_lower_bound: VspcPoint,
}
```

Begin resets VspcProcessor's overlap flag and all run-local pending/history
state. Catchup begins overlap accounting. A notification destination below
the lower bound is a latecomer and is discarded without overlap credit.
Unresolved raw destinations and consensus-order comparisons alone never earn
credit.

While the coordinated pump supplies synthetic changes, VspcProcessor commits
that ordered stream with priority. Notifications may be consumed, resolved,
and considered for overlap, but cannot overtake an available synthetic
predecessor. A temporarily empty synthetic queue does not prove production has
ended and does not authorize a notification commit.

For committed synthetic sink `C`, classify each resolved notification in this
order:

1. Resolve `D = added.last()`. If `D.order <= C.order`, discard the
   notification under the lower-bound rules. Accepted retained history may
   prove the meeting and earn overlap credit. After the pending-destination
   collision check above, `D == C` is an already committed or overtaken
   notification and is discarded; it is not a detectable duplicate pending
   transition and never becomes an empty normalized change.
2. If `D.order > C.order` and `C.hash` occurs in `added`, discard `removed`
   and the `added` prefix through `C`. The necessarily nonempty added-only
   suffix has source `C` and is actionable after endpoint readiness.
3. Otherwise derive and resolve the original source. If it equals `C`, the
   original change is directly actionable.
4. Otherwise retain it pending and continue scanning later candidates.

The sink's occurrence in `removed`, a later destination, or a consensus-order
comparison cannot prove crossing. Resolve both endpoints of the resulting
actionable change before readiness.

A notification proved by accepted history to meet the committed synthetic
stream sets VspcProcessor's `AtomicBool` overlap flag. Filtered older
latecomers receive no credit. ResyncEngine owns observation of the overlap
flag and the global ordinary eligibility predicate.

## Component-local Live transition — settled

At global Live eligibility, the processing lifecycle stops and joins synthetic
production before sending VspcProcessor its existing exact-once `Live` command
ahead of BlockProcessor's Live command. VspcProcessor then:

1. clears or discards queued synthetic input;
2. ignores later synthetic input for the rest of the session;
3. prunes history below the committed sink with strict `<`; and
4. makes retained and new actionable notifications authoritative.

After this local transition, VspcProcessor accepts actionable notifications
without consulting synthetic input. The lifecycle proceeds directly to
BlockProcessor Live and global `EnteredLive`.

Available block material lets notification-driven sink advancement continue.
A notification whose needed block material has not arrived remains in
pre-resolution readiness while block processing and dependency resolution
continue. This waiting occurs before strict materiality resolution and does
not weaken the direct Rebuild fault when an actionable transition confirms a
nonmaterialized chain member.

## Commit and graph publication — settled

```rust
struct VspcCommitted {
    source: BlockHash,
    destination: BlockHash,
    removed: Arc<[BlockHash]>,
    added: Arc<[BlockHash]>,
    level_snapshots: Arc<[LevelCommitted]>,
}
```

For each ready transition, VspcProcessor invokes storage's atomic VSPC
transaction. Only definite commit advances its local committed sink and
history. Storage returns the destination `VspcPoint` and the complete final
snapshots of its affected levels; no caller-side destination or level-score
reconstruction is required.

After definite commit, VspcProcessor offers the corresponding
`VspcCommitted` update, including the storage-returned level snapshots, to the
run's ordered graph-update producer. The
[BlockProcessor delivery contract](block-processing.md#committed-block-delivery)
offers each newly materialized block's graph update before the `PersistedBlock`
that can make a VSPC transition ready. Therefore VspcProcessor cannot publish
a VSPC mutation ahead of its causal block updates once the shared producer gate
is open. Before that cut, both processors' API projection offers are
intentionally suppressed while their committed processing state continues.

Graph-update delivery is nonblocking for processing. A full channel reports a
gap under the [API ingress contract](api-ingress.md#in-process-graph-update-feed--settled)
rather than rolling back the committed VSPC transaction or requesting
processing recovery.

The committed sink remains derived from materialized VSPC membership in
storage. VspcProcessor does not persist a separate sink or checkpoint.
