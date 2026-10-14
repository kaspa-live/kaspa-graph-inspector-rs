# Block processing

## Scope and ownership

This document owns BlockProcessor, its graph lifecycle-marker delivery worker,
OrphanManager, DependencyResolver, block admission, PP-boundary phase behavior,
and committed block delivery. The
[storage contract](storage.md) owns persistent representation and transactions.
The [processing lifecycle](processing-lifecycle.md) owns recovery phase
coordination and the synthetic GetBlocks producer. [NodeService](node-service.md)
owns raw BlockAdded validation and routing.

## PP-boundary phase behavior — settled

ProcessingSession supplies `boundary_seal_blue_score` while initializing the
inert BlockProcessor after successful preparation. Its
[boundary threshold contract](processing-lifecycle.md#boundary-seal-threshold-construction)
owns construction and failure disposition; BlockProcessor only consumes the
supplied value.

Genesis has no discarded DAG past, so a Genesis PP is intrinsically sealed at
blue score zero. After the atomic Genesis rebuild transaction, Rebuild
initialization starts BlockProcessor directly in
`PostSeal`, enqueues `PublishPostSeal` to its lifecycle-marker worker, and then
emits the existing exact-once `PpBoundarySealed` milestone. Genesis never enters
the ordinary BlockProcessor or `PersistedBlock` path.

For every non-Genesis PP, Rebuild initialization starts in
`PreSeal { seal_blue_score: boundary_seal_blue_score }`. PreSeal blocks arrive
in consensus-topological order and use storage's `AllowBoundaryIdentities`
policy. Missing parents and merge-set identities may be outside the retained
PP boundary. The materialized retained portion of the PP anticone precedes
PP-future blocks that merge it.

For a non-Genesis PP, the **first** block whose blue score is at or above the
threshold uses `RequireMaterialized`, not the permissive policy. Only its
definite successful commit changes BlockProcessor's local phase to `PostSeal`
and authorizes the exact-once `PpBoundarySealed` milestone upward to
ProcessingSession. For an inserted sealing block, BlockProcessor emits that
milestone only after offering the block's graph update obtains the intentional
`SuppressedPreSeal` outcome and its marker worker accepts the
`PublishPostSeal` command. An ambiguous or failed transaction enqueues no
marker command and emits no milestone.
`PpBoundarySealed` is never a command sent back to BlockProcessor.

The [processing lifecycle](processing-lifecycle.md) owns propagation of the
milestone and the prohibition on entering Catchup before ProcessingSession
observes it. Successful Resync preparation initializes BlockProcessor in
PostSeal before its task starts. All PostSeal materialization is strict.

If the process stops after the Genesis rebuild transaction commits but before
the milestone is observed, restart derives the sealed boundary from committed
storage and ordinary Resync starts PostSeal. No replayed milestone or special
repair transaction is required.

Before Catchup, a missing dependency under strict policy requires Resync
rather than creating an orphan. During Catchup and Live, unresolved ordinary
dependencies create in-memory orphans. Entering Live preserves valid queued
and orphan work; it does not certify empty worker queues.

The observed retained PP anticone is complete enough for KGI visualization,
without claiming to contain every mathematical anticone block. Boundary
identity and ORIGIN semantics belong to the
[domain model](domain-model.md#pruning-point-boundary-and-origin--settled), and
their identity-only persistence belongs to
[storage](storage.md#persistent-representation--settled).

## BlockProcessor — settled

One event loop awaits inputs in strict priority:

```text
Command > Intern/resolved block > Notification block > Synthetic block
```

Use explicit higher-priority polling or draining around lower-priority work.
`tokio::select! { biased; ... }` alone does not guarantee this order under
continually ready inputs.

The [session construction](processing-lifecycle.md#processor-construction)
supplies one inert BlockProcessor with direct inputs for the immutable recovery
mode, exact RPC and DB generations, graph-update producer, and private
session-channel endpoints. BlockProcessor owns the inert marker worker and
OrphanManager values; OrphanManager owns its inert DependencyResolver, which
receives the exact RPC generation. None of these BlockProcessor-owned values
starts a task or producer during construction. Conceptual commands after task
startup are:

```text
Catchup { lower_bound, ... }
Live
Shutdown
```

After successful session preparation, ProcessingSession initializes the closed
notification gate, overlap state, empty orphan state, committed anchor, seal
threshold, and mode-specific initial phase, then starts the complete
BlockProcessor tree under the
[cross-processor construction contract](processing-lifecycle.md#processor-construction).
Initial task execution performs the local milestone and marker behavior owned
by the [PP-boundary contract](#pp-boundary-phase-behavior--settled). Resync
startup immediately enqueues `PublishPostSeal`; Rebuild startup follows the
marker-command enqueue points owned above. Processing cannot reach Catchup
until every startup marker command has been accepted by the marker
worker. That acceptance precedes the processing-lifecycle-owned successful
[`start` outcome](processing-lifecycle.md#processingsession--settled). Startup
does not wait for marker delivery through the graph-update channel.

Construction and initialization themselves create no BlockProcessor-specific
lifecycle result. A preparation failure leaves the entire inert tree owned by
ProcessingSession, which drops it during consuming shutdown without launching
its tasks. Child command or acknowledgement loss follows the
processing-lifecycle-owned startup or post-start fault path according to
whether the startup barrier has completed. Shutdown is the sole terminal
descendant barrier. No separate Genesis flag or mutable recovery-mode field
exists.

### Graph lifecycle-marker delivery — settled

Each BlockProcessor owns one small marker worker value for that session's
`GraphUpdateProducer`; its task starts with the rest of the BlockProcessor tree.
BlockProcessor sends it only two commands,
each at most once and in this order:

```text
PublishPostSeal
Live
```

The local command handoff cannot apply graph-update-channel backpressure to
BlockProcessor. The worker invokes the
[API feed's lossless marker operation](api-ingress.md#in-process-graph-update-feed--settled)
serially in its command FIFO. The API feed contract owns channel delivery,
interleaving, and gap semantics.

Handling BlockProcessor's existing `Live` command enqueues `Live` to its
lifecycle-marker worker; `EnteredLive` does not trigger API delivery. After
that command, no later marker exists for the session.

Shutdown closes the local marker-command input. The worker drains accepted
commands and exits after delivery, or exits when ApiService replacement or
shutdown drops that session feed's receiver. The worker owns only its clone of
the session-scoped `GraphUpdateProducer` and no validated RPC or DB client.
BlockProcessor joins it as part of terminal shutdown; the processing lifecycle
owns the cross-component ordering that ensures a closed receiver can release a
blocked lossless delivery. If no marker is pending, it exits immediately.

The local notification gate starts closed and opens on Catchup. Continue
polling its receiver while closed and discard received notifications
immediately. Do not accumulate them for later processing. NodeService rejects
an Enabled BlockAdded that cannot become a `ValidatedNodeBlock` before it
reaches this input.

### Admission and materialization

The first block filter checks processor phase and the source gate. It then
calls `ValidatedDbClient::block_presence(block_hash)`:

- an already materialized block deduplicates and still yields its
  `PersistedBlock` identity to downstream consumers;
- a new `ValidatedNodeBlock` is committed under the phase's current storage
  reference policy; and
- a permanent boundary identity used as the incoming block reports a typed
  materiality violation.

Reference validation and complete missing-reference reporting occur inside the
atomic `ValidatedDbClient::materialize_block(block, policy)` transaction. This
avoids splitting readiness from the commit that relies on it.

Handle its typed semantic failures in this precedence order:

1. `IncomingBoundaryIdentity` reports `MaterialityViolation`.
2. `NonMaterializedReferences` with any `identity_only` member reports the
   same violation. Do not admit the block to OrphanManager or attempt to
   resolve a permanent boundary identity; any accompanying `missing` hashes
   remain diagnostic context only.
3. `NonMaterializedReferences` containing only `missing` hashes under strict
   processing before Catchup reports `ReconciliationFailed`.
4. The same missing-only result during Catchup or Live admits the block to
   OrphanManager with the complete missing set.

`block_presence` failures and the `Database(ProcessingDbError)` branch of
`MaterializeBlockError` retain Storage's typed normalization. BlockProcessor
reports the corresponding cross-worker fault through the
[processing-lifecycle mapping](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
without inspecting diagnostics or reclassifying the database outcome.

PreSeal boundary absences are accepted only through
`AllowBoundaryIdentities`; they do not produce
`NonMaterializedReferences`. An `IncomingBoundaryIdentity` remains a violation
under either policy.
Transaction validation, hash interning, coordinate allocation, result shapes,
and commit behavior belong to the
[block materialization transaction](storage.md#block-materialization-transaction--settled).
The [processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns the cross-worker dispositions of both reported faults.

### Catchup filtering and overlap

Catchup supplies an objective compact-ID lower bound from the latest accepted
GetBlocks anchor or page. Conceptually:

```rust
struct BlockCatchup {
    known_id_lower_bound: CompactId,
}
```

A known BlockAdded below this lower bound is a latecomer and is discarded
without overlap credit. Do not reject an unknown hash using guessed consensus
order. A notification proven below the sealed PP is discarded as invalid.
Legitimate delayed notifications at or above the bound may deduplicate,
materialize, or orphan normally.

Post-preparation initialization sets BlockProcessor's overlap state. Catchup
starts accounting with:

```text
SourceBits::SYNTHETIC
SourceBits::NOTIFICATION
HashMap<BlockHash, SourceBits>
AtomicBool block_overlap
```

A hash admitted from both sources proves block overlap and sets the flag.
When the notification bit is already present for a hash, silently discard the
second BlockAdded before materialization or orphan admission. It changes no
source bit, earns no overlap credit, and requests no recovery. Implementations
may count it diagnostically. GetBlocks hashes may repeat across responses, so
ProcessingSession filters synthetic repeats before dispatch; a filtered repeat earns
no overlap credit. Complete-page observation and synthetic repeat-filtering
rules belong to the [processing lifecycle](processing-lifecycle.md).

### Committed block delivery

```rust
struct PersistedBlock {
    point: VspcPoint,
    selected_parent: BlockHash,
}
```

`PersistedBlock` is the delivery payload BlockProcessor emits after storage
establishes the block as
[`Materialized`](domain-model.md#identity-and-materiality-vocabulary--settled).
It carries the point and selected parent required by downstream consumers.

Every delivered `PersistedBlock` represents a non-Genesis block and therefore
has a mandatory selected parent. Genesis never enters the ordinary
BlockProcessor-to-consumer `PersistedBlock` path; VspcProcessor instead
constructs its initial history record from the session anchor. No
VSPC `added` or `removed` member is Genesis, although a derived VSPC source may
be Genesis.

After `materialize_block` returns `Inserted`, BlockProcessor offers its
returned [`BlockCommitted`](storage.md#block-materialization-transaction--settled)
value to the run's graph-update producer before delivering `PersistedBlock` to
VspcProcessor and OrphanManager. `SuppressedPreSeal` intentionally omits only
the API projection update; processing delivery continues. Once the producer
gate is open, completing the offer before `PersistedBlock` preserves
graph-update causal order. BlockProcessor preserves the storage materialization
lane's returned order when offering newly inserted `BlockCommitted` values.
`AlreadyMaterialized` produces no graph mutation but still delivers
`PersistedBlock`. A full graph-update channel does not delay or roll back the
committed block and does not suppress `PersistedBlock`; the session producer
reports the gap under the
[API ingress contract](api-ingress.md#in-process-graph-update-feed--settled).
If the offer instead returns `ReceiverClosed`, BlockProcessor still delivers
the committed block's `PersistedBlock` to VspcProcessor and OrphanManager;
materialization remains definite. The active-run and teardown handling of that
result belongs to the
[shared graph-update closure policy](processing-lifecycle.md#supervisor-and-recovery-intent--settled).

`PersistedBlock` delivery is asynchronous but cannot be silently lost after a
successful commit. A full bounded destination reports
`SessionContinuityLost`; a closed or unavailable receiver reports
`Ownership(SessionDataEndpointLost)`. Their phase-dependent dispositions and
the expected-teardown exception belong to the shared
[session-channel policy](processing-lifecycle.md#supervisor-and-recovery-intent--settled).
A later session rederives authoritative state from the database.

## OrphanManager — settled

The [system control tree](overview.md#system-shape--settled) defines
OrphanManager's component parent and child relationships. OrphanManager is the
sole producer of DependencyResolver work, and its resolver command and work
channels are private. OrphanManager has a prioritized unbounded command input
and a bounded data-message input so lifecycle commands remain prompt.

It owns:

- in-memory orphan blocks;
- the missing-dependency topology and frontier;
- reverse missing-hash-to-waiting-orphan indexes;
- `resolution_pending: HashSet<BlockHash>`;
- dependency request selection; and
- cancellation when natural arrival makes a request unnecessary.

Messages from BlockProcessor include new orphan, `BlockPersisted`, and
capacity/topology updates. DependencyResolver results return directly to
BlockProcessor's Intern/resolved lane; BlockProcessor retains result
classification and materialization and reports the resulting `AddOrphan` or
`BlockPersisted` state change to OrphanManager. `BlockPersisted` resolves every
affected edge. Every newly ready orphan is removed from orphan storage and
returned through the same BlockProcessor lane. No waiter belongs in the
committed block index, and deduplicated hashes are harmless.

BlockProcessor constructs OrphanManager with empty run-local state and its
private DependencyResolver, which receives the session's exact RPC generation
directly. Terminal shutdown is transitive: OrphanManager stops resolver work
admission, cancels and joins resolver tasks, releases the child's RPC-generation
clone, and completes its own barrier before acknowledging BlockProcessor.
After a resolver RPC succeeds, keep its hash in
`resolution_pending` until OrphanManager observes `AddOrphan` or
`BlockPersisted` for that hash. RPC completion alone must not reopen the request
gap before BlockProcessor accounts for the result.

Dependency selection uses orphan topology only. Age and DAA score are not
inputs. Define:

```text
MAX_ORPHAN_BLOCKS = 1024
ORPHAN_RESOLUTION_THRESHOLD = 256
```

When occupancy reaches the one-quarter threshold, request frontier hashes to
maximize release while retaining capacity for new arrivals.
OrphanManager reports orphan occupancy, high-water mark, and threshold
crossings.

Orphan capacity counts distinct stored block hashes. An orphan hash already in
the topology consumes no additional slot. If admitting a new orphan would
exceed capacity, reject that block before changing orphan storage, reverse
indexes, topology, or `resolution_pending`; do not evict an existing orphan.
OrphanManager reports `BoundedStateExhausted(Orphans)` through BlockProcessor,
then BlockProcessor and OrphanManager admit only the terminal shutdown command.
Entering
this or any other terminal local fault state immediately closes resolver work
admission and cancels and joins DependencyResolver. The acknowledged shutdown
barrier confirms that the child is stopped and its RPC-generation clone is
released. The
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns routing shutdown and recovery disposition.

An isolated orphan below the threshold is acceptable. Connection loss or a
full notification channel requests recovery. The
[NodeService activation contract](node-service.md#notificationrouter) owns
callback delivery. If a block absent from processor input later becomes a
dependency of an admitted block, normal dependency resolution exposes it. No
independent age fallback is required.

## DependencyResolver — settled

The [system control tree](overview.md#system-shape--settled) owns
DependencyResolver's component parent. Its local worker contract has:

- separate command and bounded work channels;
- the processing session's exact `Arc<ValidatedRpcClient>`;
- bounded concurrent GetBlock tasks;
- cancellation for outstanding tasks; and
- no database transaction held during node RPC.

Define:

```text
MAX_DEPENDENCY_RPC_CONCURRENCY = 8
```

Dependency resolution is exceptional repair work rather than an intensive
steady-state RPC lane. The limit allows parallel release of orphan topology
without dominating the validated node connection.
DependencyResolver reports active and permit-waiting RPC operations.

DependencyResolver is constructed with the session's exact RPC generation and
an empty work queue before OrphanManager is exposed to BlockProcessor. It is
never rebound to another generation.

OrphanManager sends Resolve work and `Cancel(hash)` when a block arrives
naturally. It owns the pending set and prevents duplicate requests. Resolver
results return on BlockProcessor's Intern/resolved lane, never its
notification lane. The resolver uses NodeService's normalized
[`full_block`](node-service.md#individual-full-block-getblock) operation; raw
GetBlock results never reach BlockProcessor.

`resolution_pending` remains set until the manager observes `AddOrphan` or
`BlockPersisted`. It is a set, not a queue or map.

A resolver-confirmed unavailable dependency requests `Require(Rebuild)`
because retained database contents can no longer be trusted against node
state. RPC or connection failure does not prove unavailability and follows
the validated-client/session fault path.

For OrphanManager-to-Resolver bounded work sends, full reports
`SessionContinuityLost`, while closed or unavailable reports
`Ownership(SessionDataEndpointLost)`. Their dispositions belong to the shared
[session-channel policy](processing-lifecycle.md#supervisor-and-recovery-intent--settled).
Resolver control-path faults and unexpected termination follow the
[owner-directed fault route](processing-lifecycle.md#supervisor-and-recovery-intent--settled).
During OrphanManager's transitive Shutdown barrier,
BlockProcessor continues draining and discarding resolver results so a full
result channel cannot deadlock task cancellation and join.
Cancelling a resolver task through `Cancel(hash)` or the transitive Shutdown
barrier prevents that task from delivering a resolver result. If the validated
RPC operation itself returns `NodeError::Cancelled` while BlockProcessor
remains active, BlockProcessor reports the lifecycle-owned active-cancellation
fault; it does not treat that result as successful resolver cancellation or
leave the dependency pending silently.
