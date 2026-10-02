# API graph publication

## Scope and ownership

This document owns `GraphPublication`, publication identity and lifecycle state,
database seed construction, graph-update alignment, activation, reconstruction,
replacement, and the lifetime of publication-scoped response reuse. The
[API graph model](api-graph.md) owns the contained view, delta, and history
semantics. [ApiService control](api-service.md) owns reset and API
database-generation binding. Public HTTP, SSE, window, cursor, cache-entry,
selection, and delivery behavior belongs to the
[API protocol](api-protocol.md).

## Publication and seed values — settled

The following conceptual shapes define publication state and construction.
Container and collection types remain implementation choices. Window anchors,
resolution, and normal anchor-unavailability outcomes remain public API values.

```rust
enum GraphPublicationState {
    Synchronizing,
    Live,
    Stale,
}

struct GraphPublication {
    publication_id: u64,
    state: GraphPublicationState,
    view: GraphView,
    history: GraphHistory,
    cache: GraphCache,
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
runtime state;
[API protocol](api-protocol.md#publication-scoped-head-response-cache--settled)
owns cache entry identities and delta-cache policy, while the
[publication wire contract](api-protocol.md#publication-wire-observation--settled)
owns client registration and wake scheduling.

## Database seed extent and projection — settled

ApiService obtains a `GraphViewSeed` through storage's
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

ApiService projects one processing session through these internal states:

```text
AwaitReset -- reset() --> PreSeal
    -- PublishPostSeal --> Constructing
    -- seed succeeds --> Aligning
    -- block and VSPC cuts crossed --> Active
```

`reset` may preempt every installed-session state. Its complete topology and
state effects are owned by the
[reset contract](api-service.md#reset-and-recovery-time-availability--settled).

In `PreSeal`, ApiService waits for the mandatory `PublishPostSeal` marker; the
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

The candidate may enter `Active` only after both conditions are true. A block
with an ID above the snapshot cut satisfies the block condition even when it
has no retained effect and `GraphView::apply` returns no delta. Observing only
one cut is insufficient. Consequently, an otherwise coherent candidate remains
in `Aligning` indefinitely if either source produces no qualifying
post-snapshot update; this idle-source delay is accepted and has no timeout or
synthetic fence.

Exhausting the current staging buffer before both cuts cross is normal.
ApiService remains in `Aligning` and applies the same per-source classification
to later updates. `Live` only updates the sticky target state and crosses
neither cut. There is no direct `Constructing -> Active` transition.

After both crossing updates have been applied, discard `snapshot_vspc_sink`,
stop all snapshot-relative classification, apply every later staged graph
update in channel order, and advance through a captured activation frontier.
Updates arriving after that frontier remain for ordinary Active processing;
activation does not require an empty channel. Publish the completed view and
history independently and atomically, assign a fresh publication ID, use the
sticky target state, and enter `Active`. Neither the candidate nor its
partially replayed state is externally visible before activation.

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
definite. ApiService therefore uses one lossless reconstruction primitive for
graph-update gaps, staging overflow, construction or projection failure,
construction-side API query or pool failure, construction-side API
database-generation loss, and alignment invariant failure:

```text
restart_construction():
    abandon the unpublished candidate, if any
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
the same primitive again. Before activation, ApiService requires the recorded
generation still to be current.

`PublishPostSeal` is not tracked during reconstruction. Entering
`Constructing` already proves that the session consumed its mandatory marker;
API-local reconstruction in that session remains post-seal and requires no
second marker.

The primitive is identical from `Constructing`, `Aligning`, and `Active`.
`Constructing` or `Aligning` first abandons its unpublished candidate. `Active`
first marks its current publication terminally `Stale` and stops applying
updates to it. A previously Live target, or `Live` encountered while draining,
remains sticky, so its replacement is initially Live. API-local reconstruction
never requests processing Resync or Rebuild.

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

Throughout reconstruction, a previous coherent publication remains readable
as Stale.

## Publication state and revision — settled

ApiService has three externally meaningful publication states:

```text
Stale | Synchronizing | Live
```

Publication activation establishes its initial state without inventing a
lifecycle delta. The aligned replacement is normally `Synchronizing`. If the
ordered Live marker arrived while construction or alignment was pending,
publish the completed image directly as `Live`.

Once a publication is visible, lifecycle events admit only
`Synchronizing -> Live`, `Synchronizing -> Stale`, and `Live -> Stale`.
`Stale` is terminal; a replacement always has a fresh `publication_id`. These
state transitions do not modify `GraphView`, create `GraphDelta`, or advance
either view or history revision. Repeating a terminal Stale transition creates
no additional state effect.

Becoming Stale does not destroy the publication's response cache or cancel its
response-construction jobs. The coherent view and history stop advancing.
Replacement or destruction of the publication cancels and joins its unfinished
cache jobs and releases all cache entries and that publication's client
identifiers. The protocol owner defines how live SSE transports receive their
replacement identifiers. Stale transition and client-registration behavior
follow the protocol owner. Public access while Stale and
ordinary entry eviction belong to the
[API protocol](api-protocol.md#publication-scoped-head-response-cache--settled).

`GraphPublication` owns lifecycle state; the contained graph values retain the
semantics owned by the [graph model](api-graph.md).

## Publication projection failure — settled

The active Head consumes original committed updates and produces deltas; it
never applies a delta to itself. Direct Head mutation failure, history append
revision mismatch, unexpected failure to compose trusted retained history, or
another Head/history invariant that prevents advancement is an API projection
failure. In `Constructing` or `Aligning`, abandon the unpublished candidate and
invoke `restart_construction()`. In `Active`, first mark the current publication
terminally `Stale`, then invoke the same primitive.
