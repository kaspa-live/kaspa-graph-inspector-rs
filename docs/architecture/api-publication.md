# API graph publication

## Scope and ownership

This document owns `GraphPublication`, its internal synchronization and request
capture, publication identity and lifecycle state, database seed construction,
graph-update alignment, activation, reconstruction, replacement, and the
lifetime of publication-scoped response reuse. The
[API graph model](api-graph.md) owns the contained view, delta, and history
semantics. [ApiService control](api-service.md) owns reset, the current-
publication slot, and API database-generation binding. Public HTTP, SSE,
window, cursor, cache-entry, selection, and delivery behavior belongs to the
[API protocol](api-protocol.md).

## Publication and seed values — settled

The following conceptual shapes define publication state and construction.
The synchronization types shown here are settled; collection types not shown
remain implementation choices. Window anchors, resolution, and normal
anchor-unavailability outcomes remain public API values.

```rust
enum GraphPublicationState {
    Synchronizing,
    Live,
    Stale,
}

struct GraphPublicationImage {
    state: GraphPublicationState,
    view: GraphView,
}

struct GraphPublication {
    publication_id: u64,
    revision_clock_origin: std::time::Instant,
    mutation: tokio::sync::Mutex<()>,
    image: tokio::sync::RwLock<GraphPublicationImage>,
    history: tokio::sync::RwLock<GraphHistory>,
    cache: Arc<GraphCache>,
    clients: DeltaClientRegistry,
}

enum GraphViewSeedRequest {
    HeadPublication,
    AnchoredWindow {
        anchor: GraphWindowAnchor,
        max_depth: u64,
    },
}

struct GraphViewSeed {
    resolution: GraphWindowResolution,
    max_depth: u64,
    levels: LevelSet,
    blocks: BlockSet,
    edges: EdgeSet,
    snapshot_max_materialized_id: CompactId,
    snapshot_vspc_sink: BlockHash,
}

enum GraphViewSeedOutcome {
    Loaded(GraphViewSeed),
    AnchorUnavailable(GraphWindowAnchorUnavailable),
}
```

Only `GraphPublication` has a `publication_id`. Each publication receives a
fresh random nonzero `u64`. `GraphCache` and `DeltaClientRegistry` are private
runtime state. The publication retains the cache's sole durable strong `Arc`;
cache jobs retain only `Weak<GraphCache>`.

[API protocol](api-protocol.md#publication-scoped-head-response-cache--settled)
owns cache entry identities and Head response-cache policy, while the
[publication wire contract](api-protocol.md#publication-wire-observation--settled)
owns client registration and wake scheduling.

When construction creates a `GraphPublication`, it initializes
`DeltaClientRegistry` with the same publication state and current revision as
the initial image before exposing either value. The registry therefore always
has a coherent observation for a concurrently established SSE registration.

Each construction candidate also receives a fresh monotonic clock origin when
its construction begins. The origin moves into `GraphPublication` if that
candidate reaches `Prewarming`; it is never exposed directly. Revision zero
has timestamp zero. Every later successful graph mutation records the elapsed
whole microseconds from that origin as its target revision timestamp. Values
are nondecreasing and adjacent revisions may have equal timestamps after
quantization. A replacement publication establishes a new clock domain, so
timestamps are comparable only under one `publication_id`. The
[graph owner](api-graph.md#revision-and-history-advancement--settled) owns the
timestamp carried by `GraphView` and `GraphDelta` and its composition rule.

## Publication runtime — settled

`PublicationRuntime` is a private `kgi-api-core` value whose lifetime is bound
to one processing session. It owns that session's `GraphUpdateReceiver`, graph
update worker, gap observation, seed construction, staging, alignment,
prewarming, and the writer side of every publication it installs. It also owns an
`Arc<ApiDbState>` clone through which construction awaits the latest valid API
database client. ApiService's private runtime slot contains at most one runtime;
during reset, ApiService may additionally retain the exchanged predecessor
only until its shutdown barrier completes. A runtime is never exported,
returned to a caller, or placed in a shared model crate.

One runtime persists across every intra-session reconstruction and may install
successive publications. It owns at most one unpublished Prewarming or visible
Active `Arc<GraphPublication>`. It begins ordinary graph-update application
while that publication is still Prewarming and later supplies an Arc clone to
ApiService's generation-fenced installation operation. The reset-time overlap,
installation authority, and current-slot replacement rule belong to the
[ApiService control contract](api-service.md#reset-control-and-recovery-effects--settled).

The runtime exposes one private completed barrier:

```rust
struct PublicationRuntime {
    cancellation: tokio_util::sync::CancellationToken,
    completion: PublicationRuntimeCompletion,
}

impl PublicationRuntime {
    async fn shutdown(&self) -> Result<(), PublicationRuntimeError>;
}
```

Each runtime starts exactly one long-lived worker as part of construction. The
worker owns the receiver, phase state, staging buffer, current seed attempt,
unpublished candidate or Prewarming publication, gap observation, and Active
publication Arc. During
`Constructing`, it selects between its database-seed future, graph-update
receiver, and cancellation rather than spawning an untracked seed task.
Dropping that future cancels an abandoned attempt. Intra-session reconstruction
remains inside the same worker.

`shutdown()` signals the runtime cancellation token and awaits the worker's
shared completion result. When the worker observes cancellation, it stops
consuming and applying graph updates, marks its active publication terminally
`Stale` if one exists, drops any seed attempt and unpublished candidate,
abandons an unpublished Prewarming publication and detaches from its mandatory
cache job,
releases its receiver and publication Arc, and completes. Every
potentially indefinite worker wait observes the same cancellation token, and
no worker child task may survive completion. The behavior is the same for
session replacement and ApiService shutdown. Repeated calls observe the same
completed result. The state transition uses the protocol-owned
[`PublicationState` delivery](api-protocol.md#publication-wire-observation--settled).

The runtime otherwise terminates only through `shutdown()`. A closed
`GraphUpdateReceiver` makes it quiescent: it stops consuming and constructing,
does not reconstruct or change publication state, and waits only for runtime
cancellation without spinning. Operational failures named below use
intra-session reconstruction instead of terminating the runtime. Unexpected
worker return or panic follows the ApiService-owned permanent-worker failure
path rather than defining another publication transition.

## Publication synchronization and request capture — settled

`GraphPublication.mutation` serializes graph mutations and lifecycle-state
transitions. It is a writer gate rather than a container for graph state.
`image` publishes the state and complete `GraphView` together, while `history`
publishes the independently readable `GraphHistory`. Cache and client-registry
synchronization is private to those values. Cache work never participates in
the graph mutation critical section. The short nonblocking client-registry
operations described below run at its tail so SSE observation preserves graph
and lifecycle order.

For an original `BlockCommitted` or `VspcCommitted` update, the runtime:

1. acquires `mutation` and requires the starting image and history revisions to
   be equal;
2. acquires the image write lock and applies the original update; when it
   produces a revision, it samples elapsed publication-local microseconds at
   delta creation and publishes the complete resulting view revision and
   timestamp;
3. releases the image lock;
4. when the update returned a delta, acquires the history write lock and
   appends and publishes that complete delta entry; and
5. releases the history lock, calls the ApiService-owned client registry's
   nonblocking `advance_head` with the appended revision, and then releases
   `mutation`.

A no-effect update changes neither value. A starting revision mismatch or a
failed apply or append follows the publication-projection failure contract
below. No next mutation can begin during the valid interval after the view has
advanced and before its delta has reached history. Readers remain allowed in
that interval; the graph model owns the meaning of its adjacent revisions. The
equality check is therefore a mutation-entry precondition, not a continuously
maintained equality invariant.

A lifecycle-state transition acquires `mutation`, changes `image.state` under
the image write lock, releases the image lock, and publishes the required state
message through the ApiService-owned client registry before releasing
`mutation`. It does not take the history lock or change a graph revision. The
registry operation does not await. Consequently, becoming terminally `Stale`
waits for an in-progress graph mutation to finish, the Stale image's final view
revision is already present in history, and its state message precedes any
later mutation's graph wakeup.

Read locks protect only bounded in-memory capture. Serialization, compression,
cache work, database access, network delivery, and request waiting never hold
an image or history lock. The private capture values are:

```rust
struct GraphViewCapture {
    publication_id: u64,
    view: GraphView,
}

struct GraphDeltaTargetCapture {
    publication_id: u64,
    state: GraphPublicationState,
    revision: u64,
    revision_timestamp_us: u64,
    high_level: u64,
}

struct GraphLevelCapture {
    publication_id: u64,
    revision: u64,
    levels: Arc<[HeadLevelValue]>,
}
```

These are private `kgi-api-core` values rather than wire DTOs. `HeadLevelValue`
is the protocol-owned level lookup value.

The capture operations below run against the exact `Arc<GraphPublication>`
supplied by ApiService's
[current-publication slot](api-service.md#reset-control-and-recovery-effects--settled).
The slot owner defines acquisition and replacement behavior.

A Head snapshot-cache job or Head-backed window holds one image read lock while
it captures an owned `Frozen` graph view. A window performs its complete
extraction against that same image. A level lookup similarly captures the
revision and every requested complete level under one image read lock; an
unavailable level produces no partial capture. Public publication identity and
state headers are finalized separately under the
[protocol-owned publication-context rule](api-protocol.md#publication-context-response-headers--settled).

A canonical delta operation briefly captures its publication ID, state, view
revision, revision timestamp, and high level under one image read lock,
releases it, and then asks history for a range ending at the captured revision.
The revision and timestamp form the coherent Head pair carried in the
protocol-owned response headers. History may still be one
revision behind that target. In that case the request uses the complete range
currently available and the protocol reports the remaining progress through
its ordinary continuation outcome; it neither waits for the append nor reloads
the image. A Stale capture cannot encounter this interval because the state
transition waits behind `mutation`.

History range capture clones the selected immutable
`Arc<GraphHistoryEntry>` values under the history read lock. Target selection,
budgeting, response-wide dictionary construction, JSON serialization, and gzip
compression then proceed without the lock. Later history pruning cannot
invalidate those captured entries.

## Database seed extent and projection — settled

`PublicationRuntime` obtains a `GraphViewSeed` through storage's
[`ValidatedApiDbClient`](storage.md#api-graph-projection-reads--settled). The
API-specific handle uses the separate capped read-only API pool; the processing
`ValidatedDbClient` is never used for graph loads or historical requests.

`HeadPublication` constructs the complete cache image. Its resolved and high
level are the current highest materialized database level, its `max_depth` is
exactly `MAX_CACHE_DEPTH`, and its low level is:

```text
max(1, high_level - MAX_CACHE_DEPTH + 1)
```

The [public anchored-window protocol](api-protocol.md#anchored-graph-windows--settled)
owns request validation and caps a positive requested depth before selecting a
projection source. An internal `AnchoredWindow` therefore receives an
effective `max_depth` only in `1..=MAX_WINDOW_DEPTH`. Storage resolves its
level, block-hash, or DAA anchor inside the same consistent read as the
projection. For an effective depth `d`, the nominal extent initially allocates:

```text
levels_above = (d - 1) / 2
levels_below = (d - 1) - levels_above
```

An even depth therefore puts its additional level on the lower historical
side. At level 1 or the current database head, shift unused capacity to the
opposite side so the extent contains up to `d` levels when the retained
database range permits it. Never extend outside that range. The returned
`GraphWindowResolution` records the resolved level and final inclusive bounds.

The seed contains every materialized block in the nominal extent as a complete
`GraphBlock`. Resolve every actual direct-parent and merge-set ID to its hash,
including references whose blocks are outside the extent. Direct parents use a
local vector order chosen by the projection; that order has no separate
semantic meaning.
For a non-Genesis block, `selected_parent_index` is the index of the stored
selected-parent hash in that local vector. No parent ordinal is persisted.
Genesis has an empty vector and `selected_parent_index = None`; synthetic
ORIGIN is not a direct parent.

Select every materialized child-parent edge whose span intersects the nominal
extent under the settled
[edge-span predicate](api-graph.md#vspc-projection-and-delta-composition--settled).
The seed therefore retains crossing edges with zero, one, or two endpoint
blocks in its block set. Each edge carries both hashes and coordinates. An
outside-PP sentinel parent produces no public edge.

The seed's levels are the union of every nominal level and every child or
parent endpoint level referenced by a selected edge. Each entry carries its
complete stored `size` and optional DAA score. `usage_count` is not loaded; the
`GraphView` constructor derives it from the selected edges. The same snapshot
also returns the global maximum materialized block ID as
`snapshot_max_materialized_id` and the committed materialized VSPC sink as
`snapshot_vspc_sink`. The maximum covers the complete `blocks` table rather
than only the projected window. Both fields are private construction metadata
consumed by alignment.

## Head-publication lifecycle and stream alignment — settled

One `PublicationRuntime` projects its processing session through these internal
states:

```text
PreSeal -- PublishPostSeal --> Constructing
    -- seed succeeds --> Aligning
    -- block and VSPC cuts crossed --> Prewarming
    -- tier 50 ready and eligible --> Active
```

ApiService creates a fresh runtime in `PreSeal` for each accepted `reset`; a
later reset replaces the whole runtime rather than moving the existing runtime
back to `PreSeal`. The complete replacement topology and control effects are
owned by the
[reset contract](api-service.md#reset-and-recovery-time-availability--settled).

In `PreSeal`, the runtime waits for the mandatory `PublishPostSeal` marker; the
producer gate prevents ordinary committed updates from entering the channel.
The marker establishes the reconstruction cut, initializes the target
publication state to `Synchronizing`, starts an empty staging buffer and a
fresh database seed, and enters `Constructing`. Every intentionally suppressed
update committed before this cut is covered by that newer database snapshot.
`Live` before `PublishPostSeal` violates the settled marker order. The
producer-side PostSeal precondition belongs to the
[BlockProcessor marker contract](block-processing.md#graph-lifecycle-marker-delivery--settled).

`Constructing` has exactly one responsibility: obtain a coherent revision-zero
`Head` view and revision-zero history while staging every subsequently received
`BlockCommitted` and `VspcCommitted`. A `Live` marker changes the
sticky target publication state to `Live`; it is not staged as graph data.
Construction never searches for the database/stream boundary. A successful
seed always enters `Aligning`.

`Aligning` alone connects the completed database candidate to both ordered
update sources. It tracks independent `block_cut_crossed` and
`vspc_cut_crossed` conditions, initially false. Until the block cut crosses,
classify `BlockCommitted` as follows:

```text
BlockCommitted.id <= snapshot_max_materialized_id
    -> skip: the database snapshot already covers this commit

BlockCommitted.id > snapshot_max_materialized_id
    -> apply and set block_cut_crossed = true
    -> GraphView::apply may return None or Some(GraphDelta)
```

Until the VSPC cut crosses, skip `VspcCommitted` updates whose `source` differs
from `snapshot_vspc_sink` because the seed already represents their effects.
Apply the first update whose `source` equals `snapshot_vspc_sink` and set
`vspc_cut_crossed = true`. Block IDs do not order VSPC-only transactions, so
neither condition substitutes for the other. Once one source has crossed its
cut, apply every later update from that source normally while continuing the
snapshot-relative classification for the other source.

The candidate may leave `Aligning` only after both conditions are true. A block
with an ID above the snapshot cut satisfies the block condition even when it
has no retained effect and `GraphView::apply` returns no delta. Observing only
one cut is insufficient. Consequently, an otherwise coherent candidate remains
in `Aligning` indefinitely if either source produces no qualifying
post-snapshot update; this idle-source delay is accepted and has no timeout or
synthetic fence.

Exhausting the current staging buffer before both cuts cross is normal.
The runtime remains in `Aligning` and applies the same per-source classification
to later updates. `Live` only updates the sticky target state and crosses
neither cut. There is no direct `Constructing -> Active` transition.

After both crossing updates have been applied, discard `snapshot_vspc_sink`,
stop all snapshot-relative classification, apply every later staged graph
update in channel order, and advance through a captured prewarming frontier.
Assign a fresh publication ID, publish the completed view and history
independently and atomically inside one still-unpublished
`Arc<GraphPublication>`, use the sticky target state, and enter `Prewarming`.
Neither the candidate nor its partially replayed state is externally visible.

`Prewarming` applies every later graph update through the same ordinary
mutation path as `Active`; it performs no snapshot-relative classification and
does not buffer updates while encoding. It captures one immutable
`PREWARMED_HEAD_WINDOW_DEPTH` snapshot, releases every graph lock, then performs
JSON serialization and gzip through the ApiService-owned encoding scheduler.
Cache construction creates no graph revision. While that detached work runs,
the unpublished publication continues advancing its view and history at the
ordinary update rate.

The mandatory tier-50 job uses the protocol-owned
[completion-time cache eligibility rule](api-protocol.md#publication-scoped-head-response-cache--settled)
and exposes terminal success to `PublicationRuntime` only for an admitted
candidate. Until then the runtime remains in `Prewarming`. On that success,
submit the complete publication `Arc` to ApiService's generation-fenced
installation operation. `Installed` lets the runtime enter `Active`. On
`Superseded`, the runtime abandons the candidate, does not reconstruct or
report failure, becomes quiescent, and waits for the reset-driven shutdown
cancellation. Installation creates no graph revision and does not drain a
hidden update backlog. The cached
snapshot revision can precede the publication's current revision; retained
canonical deltas connect that exact cursor to the current Head.

Failure of mandatory tier-50 capture, size checking, serialization, or
compression prevents installation. `PublicationRuntime` remains responsible
for retrying from a newer coherent capture. A previous coherent publication
remains independently servable as `Stale`, while initial startup has no graph
publication until prewarming succeeds. The
[protocol cache contract](api-protocol.md#publication-scoped-head-response-cache--settled)
owns the tier values, distance rules, and public selection behavior.

An `Active` publication has no snapshot sink, alignment state, or database
matching duty. Each ordinary graph update goes directly through
`GraphView::apply`. `None` produces no revision. `Some(delta)` first publishes
the new complete view revision and then appends and publishes the corresponding
complete history revision. Ordinary graph updates, Live, revision advancement,
and retained head-window movement keep the same publication ID. Only a later
session's `reset` call or a gap or failure that prevents the Active projection from
advancing leads to a replacement publication. API database-generation change
by itself does not invalidate Active; the
[API database binding](api-service.md#api-database-generation-binding--settled)
controls public database-backed reads independently, while
graph continuity comes from the ordered session stream rather than the seed
generation.

## Universal API reconstruction — settled

Every graph update is emitted only after the database commit it represents is
definite. `PublicationRuntime` therefore uses one lossless intra-session
reconstruction primitive for
graph-update gaps, staging overflow, construction or projection failure,
construction-side API query or pool failure, construction-side API
database-generation loss, and alignment invariant failure:

```text
restart_construction():
    cancel and abandon the current seed attempt, if any
    abandon the unpublished candidate, if any
    abandon the unpublished Prewarming publication and detach from its
        mandatory cache job, if any
    discard the current staging buffer
    drain the current session channel through the first observed Empty
    discard drained BlockCommitted and VspcCommitted values
    retain Live as a sticky target state
    record the current GraphUpdateGap generation
    start a newer database seed
    enter Constructing
```

Drained and dropped updates are covered by the newer database snapshot because
their commits precede that snapshot. Updates received after the observed empty
frontier are staged for the new attempt. A later gap-generation change invokes
the same primitive again. Before entering Prewarming, the runtime requires the
recorded gap generation still to be current; a later change during Prewarming
invokes the same reconstruction primitive.

Each seed attempt first awaits the ApiService-owned `ApiDbState::current()` and
captures its returned exact client without switching generations within that
attempt. A `GenerationLost` result invokes `restart_construction()`; the runtime
does not clear ApiDbState. `QueryFailed` and `InconsistentProjection` invoke the
same reconstruction primitive while leaving the current client unchanged. A
`None` result ends the pending construction work without starting another
attempt. The [database-binding owner](api-service.md#api-database-generation-binding--settled)
defines when either accessor returns a client or `None`.

`PublishPostSeal` is not tracked during reconstruction. Entering
`Constructing` already proves that the session consumed its mandatory marker;
API-local reconstruction in that session remains post-seal and requires no
second marker.

The primitive is identical from `Constructing`, `Aligning`, `Prewarming`, and
`Active`. `Constructing` or `Aligning` first abandons its unpublished
candidate. `Prewarming` abandons its unpublished publication and any mandatory
tier-50 waiter. The ApiService-owned tracked job can finish against detached
inputs and its weak insertion then fails harmlessly. `Active` first marks its
current publication terminally `Stale`
and stops applying updates to it. A previously Live target, or `Live`
encountered while draining, remains sticky, so its replacement is initially
Live. API-local reconstruction never requests processing Resync or Rebuild.

Define:

```text
API_STAGING_UPDATE_CAPACITY = 4096
```

The staging buffer counts `BlockCommitted` and `VspcCommitted` updates. `Live`
is sticky state rather than a staged entry, and `PublishPostSeal` establishes
the construction cut. The buffer never drops one staged entry in isolation.
If admitting another update would exceed its capacity, do not insert that
update and invoke `restart_construction()`, making the current receive frontier
the newer cut.
This capacity provides about 205 seconds at an expected 20 updates per second
and about 41 seconds at 100 updates per second.

Operational measurements include staging occupancy and high-water mark,
overflow count, and the resulting reconstruction count.

Throughout reconstruction, ApiService may continue serving the previous
coherent publication as `Stale` while the runtime constructs its replacement.

## Publication state and revision — settled

A `GraphPublication` has three externally meaningful states:

```text
Stale | Synchronizing | Live
```

Publication installation establishes its initial externally visible state
without inventing a lifecycle delta. The aligned replacement is normally
`Synchronizing`. If the ordered Live marker arrived while construction,
alignment, or prewarming was pending, install the completed image directly as
`Live`.

Once a publication is visible, lifecycle events admit only
`Synchronizing -> Live`, `Synchronizing -> Stale`, and `Live -> Stale`.
`Stale` is terminal; a replacement always has a fresh `publication_id`. These
state transitions do not modify `GraphView`, create `GraphDelta`, or advance
either view or history revision or revision timestamp. Repeating a terminal
Stale transition creates no additional state effect.

Becoming Stale does not destroy the publication's response cache or cancel its
response-construction jobs. The coherent view and history stop advancing.
Replacement does not cancel already running cache jobs or requests holding the
old publication. New operations use the replacement. Once no runtime, slot,
request, or SSE reference retains the old publication, dropping it releases
its cache entries and client identifiers; detached jobs do not retain that
publication-scoped state. The
[ApiService task contract](api-service.md#api-task-ownership-and-completion--settled)
owns detached-job completion, insertion after cache release, and terminal
job cancellation. The protocol owner defines how live SSE transports receive
replacement identifiers. Stale transition and client-registration behavior
follow the protocol owner. Public access while Stale and ordinary entry
eviction belong to the
[API protocol](api-protocol.md#publication-scoped-head-response-cache--settled).

`GraphPublication` owns lifecycle state; the contained graph values retain the
semantics owned by the [graph model](api-graph.md).

## Publication projection failure — settled

The active Head consumes original committed updates and produces deltas; it
never applies a delta to itself. Direct Head mutation failure, history append
revision mismatch, or another invariant failure in the publication's own
Head/history advancement path is an API projection failure. In `Constructing`
or `Aligning`, abandon the unpublished candidate and invoke
`restart_construction()`. In `Prewarming`, abandon the unpublished publication
and detach from its cache work before invoking the same primitive. In `Active`, first mark
the current publication terminally `Stale`, then invoke the same primitive.

Read-only retained-history selection and response or cache construction do not
advance the publication. A failure in that request path is request-local under
the
[public delivery contract](api-protocol.md#public-delivery-failures--settled);
it does not mark the publication `Stale` or invoke reconstruction.
