# API architecture

## Scope and ownership

This document owns ApiService, `GraphView`, `GraphDelta`, `GraphHistory`,
`GraphPublication`, API resource bulkheads, and the public graph API contract.
The [processing lifecycle](processing-lifecycle.md) owns when Supervisor sends
Reset and when processor phase commands occur. [Block processing](block-processing.md)
owns lifecycle-marker production. [Storage](storage.md) owns database
transactions, query implementation, and database-replacement exclusion. Web
client behavior belongs to the [Web architecture](web.md).

## In-process API and graph-update feed — settled

KGI v2 includes an in-process `ApiService` and a complete bounded-by-level
head-tracking `GraphView`, rather than directing each head request to expensive
PostgreSQL graph queries. The [system overview](overview.md#resource-isolation-and-scalability--settled)
owns deployment evolution and the single-writer constraint.

Every processing session owns one fresh ordered, bounded graph-update channel. The
channel topology is the session boundary: graph updates carry no session ID,
or cross-session stale-message filter. Processors offer
`BlockCommitted`/`VspcCommitted` only after their respective DB commits. The
[BlockProcessor delivery contract](block-processing.md#committed-block-delivery)
and [VspcProcessor producer contract](vspc-processing.md#commit-and-graph-publication--settled)
establish causal order, so a VSPC update cannot reach this channel ahead of
blocks it depends on.

The ordered stream has this semantic shape:

```rust
enum GraphUpdate {
    PublishPostSeal,
    BlockCommitted(BlockCommitted),
    VspcCommitted(VspcCommitted),
    Live,
}

struct GraphUpdateProducer {
    tx: Sender<GraphUpdate>,
    gate: Arc<GraphUpdateGate>,
}

struct GraphUpdateReceiver {
    rx: Receiver<GraphUpdate>,
    gap: GraphUpdateGap,
}

struct GraphUpdateGate {
    state: Mutex<GraphUpdateGateState>,
    gap: GraphUpdateGapReporter,
}

enum GraphUpdateGateState {
    PreSeal,
    Open,
}

enum GraphUpdateOfferOutcome {
    SuppressedPreSeal,
    Enqueued,
    GapReported,
}

enum GraphUpdateProducerError {
    ReceiverClosed,
}

impl GraphUpdateProducer {
    fn offer_block_committed(
        &self,
        update: BlockCommitted,
    ) -> Result<GraphUpdateOfferOutcome, GraphUpdateProducerError>;

    fn offer_vspc_committed(
        &self,
        update: VspcCommitted,
    ) -> Result<GraphUpdateOfferOutcome, GraphUpdateProducerError>;

    fn publish_post_seal(
        &self,
    ) -> Result<(), GraphUpdateProducerError>;

    async fn publish_live(
        &self,
    ) -> Result<(), GraphUpdateProducerError>;
}
```

`GraphUpdateProducer` is the cloneable session-scoped producer capability that
Supervisor supplies through ResyncEngine to both processors.
`GraphUpdateReceiver` is the corresponding single-consumer session capability
that Supervisor supplies to ApiService in `Reset`. Every producer clone shares
one `GraphUpdateGate`, initially `PreSeal`; the receiver does not expose that
producer-side gate.
`GraphUpdateGapReporter` and the consumer-side `GraphUpdateGap` refer to the
same session-local continuity state. These names fix the semantic capability
split. The mutex around `GraphUpdateGateState` is settled; concrete channel,
gap-counter, and wakeup types remain deferred.

The producer exposes distinct nonblocking operations for `BlockCommitted` and
`VspcCommitted`. Each operation holds the state mutex through classification
and `try_send`. A closed receiver returns `ReceiverClosed`. Otherwise, in
`PreSeal`, the operation discards the API projection update, returns
`SuppressedPreSeal`, and does not advance the gap. The underlying database
commit and processing-tier delivery remain valid. In `Open`, successful
delivery returns `Enqueued`; `Full` discards that delivery, advances the
session's reliable, coalescing gap generation, and returns `GapReported`.

The marker worker's `publish_post_seal` operation holds the same state mutex,
requires `PreSeal`, enqueues `GraphUpdate::PublishPostSeal`, changes the state
to `Open`, and releases the mutex. No ordinary value can enter before the
marker or race between its enqueue and the state transition. Because pre-seal
ordinary values are suppressed, this marker is the first value in the fresh
positive-capacity channel and cannot encounter `Full`; receiver closure leaves
the gate in `PreSeal` and reports supersession.

The marker worker's `publish_live` operation requires `Open`, releases the
state mutex, and then uses lossless delivery, awaiting channel capacity instead
of reporting an ordinary-update gap. Its command FIFO preserves
`PublishPostSeal` before `Live`; unrelated ordinary graph updates may
interleave before `Live`. ApiService uses the gap signal only to reconstruct
its derived read model; it never requests processing Resync or Rebuild.

The [BlockProcessor marker contract](block-processing.md#graph-lifecycle-marker-delivery--settled)
owns its worker and marker-command enqueue points. The
[processing lifecycle](processing-lifecycle.md#live-admission) owns the global
causality before the Live command. ApiService consumes the resulting channel
order without reconstructing those producer decisions. If an earlier causal
ordinary update reported `Full`, the advanced gap generation makes ApiService
reconstruct instead of treating that segment as gapless. Exact channel and
wakeup primitives remain deferred in the
[decision register](../decisions/deferred.md).

ApiService consumes storage's
[`BlockCommitted`](storage.md#block-materialization-transaction--settled) and
VspcProcessor's
[`VspcCommitted`](vspc-processing.md#commit-and-graph-publication--settled)
without redefining either producer payload. It consumes every successfully
delivered `BlockCommitted`, including updates for blocks below the current head
view. A block within the view follows normal insertion handling. A block below
`low_level` does not reintroduce its block into the view or extend its extent.
Its complete committed level snapshot updates that level when it remains cached
as an external endpoint for a crossing edge. Parent-level snapshots can seed
external levels required by the incoming child's edges. A later below-range
update publishes an atomic graph revision only when a supplied snapshot changes
retained endpoint state. An update for a level with no retained crossing-edge
endpoint has no visible view effect. The graph-view contract below owns endpoint
retention and removal.

Ordered `VspcProcessor` delivery certifies VSPC sequencing for ApiService.
`GraphView` does not retain a committed VSPC sink and ApiService does not repeat
the processor's source/destination continuity check. The settled
[VSPC projection contract](#vspc-projection-and-delta-composition--settled)
owns how the removed/added vectors and retained block metadata update the view
and produce a delta.

`MAX_CACHE_DEPTH = 1000` complete levels. Define a separate
`MAX_WINDOW_DEPTH <= MAX_CACHE_DEPTH`; its exact value remains deferred in the
[decision register](../decisions/deferred.md).
No arbitrary maximum block count may truncate a retained level: **every**
block and relevant edge endpoint for each cached level is available. Every
windowed endpoint caps requested depth to `MAX_WINDOW_DEPTH` and reports
the effective range. An oversized `/graph/head` request cannot fall back to
DB; it is capped.

The [graph-view contract](#graph-views-publication-revision-and-history--settled)
owns edge representation, window inclusion, endpoint retention, and lifetime.
The graph-update payload above supplies the committed parent information required
to apply that contract.

`CompactId` is private to storage/processing. Each HTTP graph response has
its own small numeric references and an included local ID-to-hash dictionary
covering **all** hashes it references, including off-window parent endpoints
and merge-set members. These local IDs are not persistent across responses or
instances; coordinates may diverge across independently allocated DBs.

The public block projection preserves its **actual direct-parent list** even
when some parents are outside the response or PP boundary and have no
drawable edge. A materialized Genesis is recognized from its empty actual
direct-parent list. The public projection and Web client do not need to expose
or consult the persisted `NodeMetadata.genesis_hash`, and no dedicated
Genesis-hash API endpoint is required.

## Graph views, publication, revision, and history — settled

The following conceptual shapes define the graph model. Container and
collection types remain implementation choices.

```rust
enum TrackingPolicy {
    Head,
    Fixed,
    Frozen,
}

enum GraphPublicationState {
    Synchronizing,
    Live,
    Stale,
}

enum GraphViewUpdateError {
    Frozen,
}

enum GraphViewExtractError {
    InvalidExtent,
    OutsideSourceExtent,
}

struct Level {
    size: u64,
    daa_score: Option<u64>,
}

struct LevelChange {
    level: u64,
    before: Option<Level>,
    after: Option<Level>,
}

struct FieldChange<T> {
    before: T,
    after: T,
}

struct EdgeId {
    parent: BlockHash,
    child: BlockHash,
}

struct GraphEdge {
    id: EdgeId,
    parent_coordinate: BlockCoordinate,
    child_coordinate: BlockCoordinate,
}

struct GraphBlock {
    hash: BlockHash,
    coordinate: BlockCoordinate,
    timestamp: Timestamp,
    daa_score: u64,
    selected_parent_index: Option<u32>,
    direct_parents: Arc<[BlockHash]>,
    blue_merge_set: Arc<[BlockHash]>,
    red_merge_set: Arc<[BlockHash]>,
    color: BlockColor,
    is_in_vspc: bool,
}

struct GraphView {
    max_depth: u64,
    low_level: u64,
    high_level: u64,
    current_revision_id: u64,
    tracking_policy: TrackingPolicy,
    levels: LevelSet, // each retained entry also keeps derived usage_count
    blocks: BlockSet,
    edges: EdgeSet,
}

struct GraphDelta {
    from_revision_id: u64,
    to_revision_id: u64,
    high_level: u64,
    block_changes: HashMap<BlockHash, Option<GraphBlock>>,
    edge_changes: HashMap<EdgeId, Option<GraphEdge>>,
    level_changes: Vec<LevelChange>,
    is_in_vspc_changes: HashMap<BlockHash, FieldChange<bool>>,
    color_changes: HashMap<BlockHash, FieldChange<BlockColor>>,
}

struct GraphHistory {
    current_revision_id: u64,
    deltas: OrderedDeltaList,
}

struct GraphPublication {
    publication_id: u64,
    state: GraphPublicationState,
    view: GraphView,
    history: GraphHistory,
}

enum GraphWindowAnchor {
    Level(u64),
    BlockHash(BlockHash),
    DaaScore(u64),
}

enum GraphViewSeedRequest {
    HeadPublication,
    AnchoredWindow {
        anchor: GraphWindowAnchor,
        max_depth: u64,
    },
}

struct GraphWindowResolution {
    resolved_level: u64,
    effective_start_level: u64,
    effective_end_level: u64,
}

struct GraphViewSeed {
    resolution: GraphWindowResolution,
    max_depth: u64,
    levels: LevelSet,
    blocks: BlockSet,
    edges: EdgeSet,
    snapshot_vspc_sink: BlockHash,
}
```

Only `GraphPublication` has a `publication_id`. A `GraphView` has no origin,
liveness, database, or publication status. A view constructed from a database
starts at revision zero. A subview extract inherits its source view's revision
and has `TrackingPolicy::Frozen`. `Head` and `Fixed` views accept updates;
`Head` advances its extent while `Fixed` retains its configured extent.
`Frozen` retains both its captured contents and revision.

### Frozen subview extraction — settled

```rust
impl GraphView {
    fn extract_subview(
        &self,
        low_level: u64,
        high_level: u64,
    ) -> Result<GraphView, GraphViewExtractError>;
}
```

Extraction requires `low_level <= high_level` and the requested nominal extent
to be fully contained in the source's nominal `[low_level, high_level]` extent.
An invalid order returns `InvalidExtent`; a well-ordered range outside the
source extent returns `OutsideSourceExtent`. Additional endpoint levels retained
outside the source's nominal extent do not expand the extractable range.

The operation reads one coherent source state, does not mutate the source, and
returns a view with:

```text
low_level           = requested low_level
high_level          = requested high_level
max_depth           = high_level - low_level + 1
current_revision_id = source.current_revision_id
tracking_policy     = Frozen
```

The result includes every source block whose coordinate level lies in the
requested inclusive extent. Each block retains its complete direct-parent and
merge-set arrays; an edge endpoint outside the extent does not cause its block
to be included.

Edges are selected with the settled span-intersection predicate below. The
result therefore includes crossing edges with both, one, or neither endpoint
block present. Because the requested extent is contained in the source extent,
the complete source edge set already contains every edge eligible for the
extract.

The result includes every level in the requested extent and every parent or
child endpoint level referenced by a selected edge, with the source level's
complete `size` and `daa_score`. Derived `usage_count` values are recomputed
from the selected edges rather than copied from the source. The nominal
`max_depth` does not include these additional endpoint levels.

Extraction produces no delta or history and does not advance either revision.
It can read a `Head`, `Fixed`, or `Frozen` source. Nested extraction is allowed
when the next requested extent is contained in the frozen source's nominal
extent. Concrete indexes, lock types, and immutable backing-data sharing remain
implementation choices.

The `publication_id` is a fresh random nonzero `u64` for each publication.
Together `(publication_id, revision)` form the public cursor. If the selected
wire format cannot represent every `u64` exactly, its encoding must preserve
the complete integer domain.

### Database seed extent and projection — settled

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

An `AnchoredWindow` accepts `max_depth` only in
`1..=MAX_WINDOW_DEPTH`. Storage resolves its level, block-hash, or DAA anchor
inside the same consistent read as the projection. For a requested depth `d`,
the nominal extent initially allocates:

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
extent under the settled predicate below. The seed therefore retains crossing
edges with zero, one, or two endpoint blocks in its block set. Each edge carries
both hashes and coordinates. An outside-PP sentinel parent produces no public
edge.

The seed's levels are the union of every nominal level and every child or
parent endpoint level referenced by a selected edge. Each entry carries its
complete stored `size` and optional DAA score. `usage_count` is not loaded; the
`GraphView` constructor derives it from the selected edges. The same snapshot
also returns the committed materialized VSPC sink as
`snapshot_vspc_sink`. This is construction metadata only and never enters a
public graph payload, `GraphView`, `GraphDelta`, or `GraphHistory`.

### Head-publication lifecycle and stream alignment — settled

ApiService projects one processing session through these internal states:

```text
AwaitReset -- Reset --> PreSeal
    -- PublishPostSeal --> Constructing
    -- seed succeeds --> Aligning
    -- boundary found --> Active
```

`Reset` may preempt every installed-session state. Its complete topology and
state effects are owned by the
[Reset contract](#reset-and-recovery-time-availability--settled).

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

`Aligning` alone connects the completed database candidate to the ordered
stream. Before the boundary, classify `BlockCommitted` as follows:

```text
block hash is present in GraphView
    -> skip

block hash is absent
    -> GraphView::apply(BlockCommitted)
    -> None: keep searching
    -> Some(GraphDelta): apply it and establish the boundary
```

The alternative boundary is the first `VspcCommitted` whose `source` equals
`snapshot_vspc_sink`; apply it and establish the boundary. Before either rule
succeeds, skip other VSPC updates as already represented by the seed. Whichever
rule succeeds first wins. An absent block outside retained state can produce no
delta and therefore cannot establish the boundary.

Exhausting the current staging buffer without a boundary is normal. ApiService
remains in `Aligning` and applies the same classification to later updates.
`Live` only updates the sticky target state and does not establish a boundary.
There is no alignment timeout and no direct `Constructing -> Active`
transition.

After the boundary update, discard `snapshot_vspc_sink`, stop all
snapshot-relative classification, apply every later staged graph update in
channel order, and advance through a captured activation frontier. Updates
arriving after that frontier remain for ordinary Active processing; activation
does not require an empty channel. Publish the completed view and history
independently and atomically, assign a fresh publication ID, use the sticky
target state, and enter `Active`. Neither the candidate nor its partially
replayed state is externally visible before activation.

An `Active` publication has no snapshot sink, alignment state, or database
matching duty. Each ordinary graph update goes directly through
`GraphView::apply`. `None` produces no revision. `Some(delta)` first publishes
the new complete view revision and then appends and publishes the corresponding
complete history revision. Ordinary graph updates, Live, revision advancement,
and retained head-window movement keep the same publication ID. Only a later
session's Reset or a gap or failure that prevents the Active projection from
advancing leads to a replacement publication. API database-generation change
by itself does not invalidate Active; continuity then comes from the ordered
session stream rather than the seed generation.

### Universal API reconstruction — settled

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

The staging buffer is bounded by update count. It never drops one staged entry
in isolation. Reaching its implementation-selected capacity invokes
`restart_construction()`, making the current receive frontier the newer cut.

An `AnchoredWindow` seed constructs a revision-zero `Fixed` view whose
`max_depth` is its effective nominal level count. It has no history or
publication ID and is discarded after the coherent HTTP response is built.
Database construction alone does not imply `Frozen`; that policy remains
reserved for subview extraction. Its request-local failures return their
ordinary API result and do not participate in the head-publication state
machine.

Throughout reconstruction, a previous coherent publication remains readable
as Stale. Without one, graph endpoints return `503 Service Unavailable`, while
the separate status/info lane remains available.

### Publication state and revision — settled

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

SSE carries the current state beside the graph cursor:

```rust
struct PublicationWakeup {
    publication_id: u64,
    revision: u64,
    state: GraphPublicationState,
}
```

The graph cursor remains `(publication_id, revision)`. A state-only transition
emits another wakeup with the same cursor and the new state. Reconnection emits
the latest cursor and current state. Graph data still comes exclusively from
HTTP snapshot or delta responses.

Head snapshots carry the current `GraphPublicationState`; their ETags vary with
it. Immutable delta payloads contain graph changes only and do not vary when
the publication state later changes. `GraphPublication` owns lifecycle state
while `GraphView` and `GraphHistory` remain free of origin, liveness, and
publication state.

The API response envelope, rather than `GraphView` or `GraphDelta`, carries the
owning `publication_id`. A published view response pairs it with the view's
current revision and publication state; a delta response pairs it with the
history interval actually returned.

`GraphView` owns application of `BlockCommitted`, `VspcCommitted`, and
`GraphDelta`. A `Frozen` view refuses each mutation entry point with
`GraphViewUpdateError::Frozen` before inspecting whether the input would have a
visible effect; its contents and revision remain unchanged. For `Head` and
`Fixed`, an update that changes no retained block, edge, or level returns no
delta and leaves the revision unchanged. Any retained change advances the view
revision and returns the corresponding `GraphDelta`; a retained level-size or
DAA-score change is sufficient even when the triggering block itself is outside
the block extent. Frozen refusal is distinct from an accepted no-effect update.

The view advances before its returned delta is appended to `GraphHistory`:

```text
GraphView n / GraphHistory n
    -> apply update to GraphView
GraphView n+1 / GraphHistory n
    -> append GraphDelta(n,n+1)
GraphView n+1 / GraphHistory n+1
```

The intermediate state is valid. A view or subview request may therefore
observe revision `n+1` while a delta-history request can reach only revision
`n`. There is no equality invariant between the two current revision fields.
`GraphHistory` nevertheless accepts a delta only when
`delta.from_revision_id == history.current_revision_id`, preserving its own
gapless sequence.

`GraphDelta.high_level` is the target view's high level. It is retained in the
internal history and included in the API delta payload. It is the semantic key
for history pruning and lets the Web consume the target head level without
deriving it from mutation contents. A delta does not carry `low_level` or a
separate coverage object; explicit graph mutations drive the receiving view's
lower-bound changes.

`level_changes` contains only levels whose `size` or `daa_score` actually
changed. `None` represents absence, so one shape covers creation, update, and
removal. Applying a level change requires the current value to equal `before`
before installing `after`. Gapless composition folds consecutive changes from
the first `before` to the last `after` and omits a level whose composed change
has no net effect. Because each change carries its pre-state, this composition
does not require the starting view.

Within the API graph model, `EdgeId` is the canonical immutable identity of a
child-parent link. `GraphEdge` adds the complete coordinates required for
extraction and drawing;
it does not depend on either endpoint block being present in the view. For
every `Some(edge)` in `edge_changes`, the map key equals `edge.id`. Likewise,
for every `Some(block)` in `block_changes`, the map key equals `block.hash`.
`GraphBlock` combines immutable block and graph data with the view's current
mutable `color` and `is_in_vspc` projection. Block add/remove changes carry the
complete value; dedicated VSPC mutations update only that projection.

The absolute map semantics are:

```text
Some(value) => ensure the identified edge or block value is present
None        => ensure the identified edge or block is absent
```

Block and edge composition is a pure right-biased map merge performed in
revision order. For each identity, the last entry replaces every earlier entry:

```text
Some(a), Some(b) => Some(b)
Some(a), None    => None
None,    Some(b) => Some(b)
None,    None    => None
```

No block or edge value comparison, equality validation, or conflict fault is
performed. An earlier addition or removal is canceled by the later resulting
state, but the last entry remains in the composed map: omission means untouched,
whereas `None` explicitly means absent. This composition is associative and
requires no starting view.

### VSPC projection and delta composition — settled

`GraphView` applies one `VspcCommitted` in the same mutation order as the
storage transaction, projected onto its retained blocks:

1. each present removed block leaves VSPC and is reset to `Gray`;
2. each present added block enters VSPC;
3. for every present added block in order, each present blue merge-set member
   becomes `Blue`, then each present red merge-set member becomes `Red`; and
4. affected retained levels receive their final current-VSPC DAA score.

Membership and color are separate mutable fields. The two change maps are
independent and may contain the same block hash. Each atomic update records the
field's value before the complete VSPC transition and its final value after all
steps; it omits a field whose final value equals its original value. Temporary
states within the transition never enter the delta.

An absent mutation target is ignored without a placeholder, deferred mutation,
storage lookup, or fault. The normal absent case is a block already in the past
below the view extent; its merge-set members are also in its past and cannot
affect the retained graph. A later absolute `Some(GraphBlock)` carries that
block's complete then-current projection and needs no replay of ignored field
changes. If all projected field and level effects are absent or no-ops, the
atomic VSPC update returns no delta and advances no view revision.

Gapless composition treats `block_changes`, `is_in_vspc_changes`, and
`color_changes` as independent collections. Block changes keep their
right-biased absolute semantics. For each field map, composition retains the
earliest `before` and latest `after`, omitting the entry when those values are
equal. It does not compare or validate intermediate values and requires no
starting view.

Delta application processes `block_changes` first, then
`is_in_vspc_changes`, then `color_changes`. A present field target adopts
`after` without validating `before`; an absent target is ignored. Field changes
therefore supersede the projection embedded in a `Some(GraphBlock)`. If the
absolute result is `None`, the later field applications have no effect. No
cross-collection cleanup or conflict validation is required. A composed
interval whose mutations cancel completely still advances from its recorded
`from_revision_id` to `to_revision_id`.

VSPC DAA-score projection uses the existing `level_changes` collection. After
capturing each affected retained level's original value, process every present
removed block as a final-score candidate of `None`, then every present added
block as `Some(block.daa_score)`. Addition therefore supplies the final score
when both paths affect one level. Emit one `LevelChange` per level only after
the complete transition. Both sides remain `Some(Level)` with identical
`size`; only `daa_score` changes. Omit the entry when the final score equals the
original score. Existing `LevelChange` composition then combines block-level
creation or size changes with the VSPC result and removes composed no-ops. A
retained level-score change alone is sufficient to produce a graph revision.

An edge belongs to an extent `[low_level, high_level]` when its level span
intersects that extent:

```text
edge.child_coordinate.level >= low_level
AND
edge.parent_coordinate.level <= high_level
```

This rule includes an edge even when neither endpoint block is contained in
the extent. It is also the conceptual selection predicate for subview
extraction; the exact index used to evaluate it efficiently is an
implementation choice. PP-boundary sentinel links at level 0 do not become
public `GraphEdge` values.

For update and pruning of a head-tracking source view, child membership owns
edge lifetime. Adding a child adds its parent edges and increments
`usage_count` on each referenced parent level. Removing a child removes every
edge whose `EdgeId.child` is that block and decrements the corresponding
parent-level counters. A parent block leaving the block extent does not remove
an edge while its child remains. An extracted view can independently retain a
crossing edge without retaining either endpoint block under the intersection
rule above.

`usage_count` is derived `GraphView` maintenance state. It is absent from the
public `Level` value and from `LevelChange`; applying edge changes maintains it
locally. A level inside the view extent remains regardless of its counter. In a
head-tracking view, an outside-extent parent level becomes removable when its
counter reaches zero. A fixed view never removes levels through this mechanism,
though updates can add referenced levels.

`GraphView` and `GraphDelta` contain no committed VSPC sink. ApiService trusts
the ordered, continuity-certified `VspcProcessor` output and does not duplicate
its continuity validation.

GraphHistory retention is level-scoped. All revisions and deltas produced while
a graph level remains within the retained head window are preserved. History
associated with that level becomes eligible for pruning only when the level
itself leaves the retained window. No independent revision-count or memory-size
cap is required or permitted by this design.

Kaspa does not permit unbounded revision production while the graph remains
indefinitely at one fixed level. Therefore retaining all history for every
level still inside the retained window is bounded by graph/window semantics. A
hypothetical infinite activity stream at one level is not a valid Kaspa
behavior and cannot justify an additional history cap. A cursor whose required
deltas left with their level normally requires a fresh view.

Gapless delta intervals from one publication compose sequentially:

```text
apply(Delta(a,b), image_at_a) = image_at_b
apply(Delta(b,c), image_at_b) = image_at_c
compose(Delta(a,b), Delta(b,c)) = Delta(a,c)
```

Composition requires exact equality between the left `to` and right `from`
revisions. The result uses `a` as `from`, `c` as `to`, and delta `c`'s
`high_level`. Composition is associative by graph-state effect. A composed
encoding need not be byte-identical to a directly constructed interval, but it
must have the same graph-state effect. Every API delta response uses a
self-contained response-local hash dictionary. Composition decodes the input
dictionaries to hashes and constructs a new dictionary for the result.

A request from revision `a` captures a desired history target `t`. If the
complete `Delta(a,t)` exceeds the response budget, return the largest nonempty
prefix `Delta(a,b)` that fits and ends at a complete revision boundary; the
client continues from `b`. Never split one atomic revision. If the first
required revision cannot fit, incremental advancement is unavailable and the
response requires a fresh view. No response splits a revision or returns a
structurally partial mutation. A publication mismatch, unavailable revision,
Stale publication, or cursor pruned by the level-scoped retention rule also
requires a fresh view. Whether an interval is encoded as individual retained
revisions or one composed patch remains an implementation choice under the
composition contract.

SSE is only an ordered `PublicationWakeup`, not the graph data channel.
Reconnection is not exactly-once. Each client has a bounded wakeup buffer. On
connection the server emits the latest history cursor and publication state;
subsequent history advancement, state change, or replacement publication emits
an updated wakeup. Slow clients receive coalesced wakeups and, if persistently
behind, are disconnected and recover through HTTP delta or view requests.

`representation_version` is the settled term for the graph payload schema.
An ETag for a head view distinguishes publication ID, revision, effective
window, negotiated response format, `representation_version`, and publication
state; `Cache-Control: no-cache` allows cheap revalidation/304. A fixed delta
interval `(publication_id,from,to)` is immutable and cacheable. A "to current"
query must revalidate. Historical database windows have no ETag in v2, and SSE
has no ETag.

ApiService maintains bounded server-side reuse of encoded head responses. It
caches immutable encoded bodies for a head view at one exact revision and
effective window, and for a delta over one exact
`(publication_id,from,to)` interval. Concurrent requests for the same absent
variant are single-flight. A view variant is identified by publication ID,
revision, publication state, effective window, negotiated response format,
`representation_version`, and content encoding. A delta variant is identified
by publication ID, its exact `from` and `to` revisions, negotiated response
format, `representation_version`, and content encoding; publication state is
not part of the immutable graph interval. A "to current" request captures an
exact target before it can join shared construction or use an encoded entry; a
bounded complete-prefix response is keyed by its actual returned interval.

An encoded-cache miss or eviction affects performance only: reconstruct the
response from the current `GraphView` or retained `GraphHistory`. Historical
database-backed windows remain uncached in v2, and SSE remains an uncached
cursor notification stream. Failure to retain a new encoded entry does not
reject an otherwise serviceable response, and cached bytes are never reused
across distinct variants. Cache pressure cannot affect graph correctness or
processing. Exact cache capacity, eviction policy,
implementation, and graph wire format remain deferred in the
[decision register](../decisions/deferred.md).

## Reset and recovery-time availability — settled

`Reset` is the sole out-of-band ApiService session-supersession control because
it replaces the graph-update input topology. ApiService's semantic commands
are:

```rust
enum ApiServiceCommand {
    Reset {
        graph_updates: GraphUpdateReceiver,
        recovery_mode: RecoveryMode,
    },
    Shutdown,
}
```

The `GraphUpdateReceiver` is fresh and belongs to exactly one processing
session. `RecoveryMode` is the Resync/Rebuild value owned by the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled).
No Reset or graph update carries a session ID. The gap
signal exposes a monotonically advancing generation and a wakeup; the exact
atomic and notification primitives remain implementation choices.

ApiService accepts exactly one Reset for each processing attempt.
Reset globally preempts `PreSeal`, `Constructing`, `Aligning`, or `Active`:

1. abandon any unpublished candidate and reconstruction state;
2. mark any current non-Stale publication terminally Stale;
3. drop the old `GraphUpdateReceiver` and install the fresh one;
4. establish the recovery-mode-specific historical-read effects below; and
5. enter `PreSeal`.

Reset is reliable but has no acknowledgement. Supervisor may continue session
startup immediately after sending it; Reset is not a processing barrier or a
database-replacement barrier. A previous publication already Stale needs no
additional graph or history mutation.

`PublishPostSeal` and `Live` are graph-update-feed markers rather than
out-of-band controls. They have no publication-completion acknowledgement. A
later session's Reset is the only session-supersession mechanism visible to
ApiService.

Ordinary teardown may drop the session's last producer and close the installed
receiver. Channel closure is not an ApiService lifecycle signal and does not
replace, invalidate, or reconstruct a publication. ApiService retains its
current state until the next Reset or another defined state transition.

An ordinary Resync Reset leaves historical DB reads available. During Rebuild,
public historical-window requests in `PreSeal`, `Constructing`, and `Aligning`
fail cleanly with `503 Service Unavailable` and a short `Retry-After`.
ApiService's private construction reads remain allowed when StorageService
publishes a usable API DB generation.

ApiService never rebinds public historical requests implicitly to whatever API
DB generation StorageService most recently published. A Rebuild construction
captures its replacement generation privately; only activation installs that
same generation for public historical requests. If Reset processing lags
behind storage replacement, retirement of the previously installed generation
still makes new database phases fail rather than crossing into the replacement
generation.

StorageService, rather than Reset, owns database-replacement exclusion. Its
[replacement gate](storage.md#api-read-exclusion-during-database-replacement--settled)
governs the API database phase. On the ApiService side, a request that already
detached its complete in-memory projection may finish delivering the old
coherent response; a request whose database phase the gate denies or cancels
returns `503`. ApiService neither coordinates replacement nor waits for the
remaining delivery of detached responses.

After Rebuild, historical reads reopen only when alignment completes and the
replacement publication becomes Active. Processing does not wait for that
publication. An ordered Live marker changes the sticky target or the Active
publication state without another database load or graph revision.

The storage owner defines the `TRUNCATE`/MVCC safety requirement and detailed
gate boundary. Reset does not participate in that exclusion mechanism.

### ApiService shutdown — settled

`Shutdown` is a reliable completed-barrier command. It is terminal,
idempotent, valid from `AwaitReset`, `PreSeal`, `Constructing`, `Aligning`, and
`Active`, and supersedes Reset processing, publication construction,
alignment, reconstruction, and Active update application. ApiService performs
the local barrier in order:

1. close public HTTP and status/info admission;
2. close every SSE stream;
3. drop the installed `GraphUpdateReceiver` and stop gap observation;
4. cancel publication construction, alignment, reconstruction, staging, and
   background encoding or cache work;
5. cancel and join every admitted request, serialization, delivery, and other
   API-owned task;
6. release every API DB permit, transaction, connection, validated client,
   pool handle, publication, and cache reference; and
7. acknowledge `Shutdown` and enter terminal `Stopped`.

The acknowledgement proves that no API admission, task, graph-update receiver,
or API database resource remains. A repeated `Shutdown` acknowledges the
already completed state; no Reset is accepted after shutdown begins. Exact
shutdown timeouts and forced escalation remain deferred under the shared
shutdown policy.

Shutdown creates no publication, `Stale` transition, graph revision, or SSE
wakeup because external admission closes first. `ReceiverClosed` caused by
this barrier is expected cancellation for the concurrently stopping processing
session and must not request API reconstruction, Resync, or Rebuild.

## DAA navigation and graph windows — settled

Every successful anchored window response carries `GraphWindowResolution`,
independently of whether its one anchor is a level, block hash, or DAA score.
The resolved level is the fixed focus selected for that request, and the
effective bounds are the actual capped block-level range returned around it:

```text
effective_start_level <= resolved_level <= effective_end_level
```

For a level anchor, `resolved_level` is the retained requested level. For a
block-hash anchor, it is the materialized block's coordinate level. For a DAA
anchor, it is the VSPC-floor result defined below. Resolution and graph contents
come from the same immutable graph view or consistent database transaction.

Accept `q` only within the shared
[`0..=MAX_DAA_SCORE` range](domain-model.md#shared-value-types--settled), then
resolve a DAA target by current VSPC floor, with the **highest level** among
score ties. The
[storage contract](storage.md#historical-read-contracts--settled) owns the
indexed database lookup and consistent historical transaction.

There may be VSPC-empty levels after reorg. If `q` precedes the retained PP
and no floor exists, report explicit no-retained-match. A `q` beyond the
current VSPC DAA resolves the current VSPC level. If `q` is at or above the
DAA of the current head view's lowest cached VSPC level, that view can resolve it; otherwise use the
storage lookup. Level resolution and its window must come from **one immutable
graph view** or one consistent DB transaction, never different revisions.
ApiService derives cached levels' final scores from `VspcCommitted` and its
cached block metadata; storage sends no level-score delta. Historical
DAA/window responses are not cached in v2.

The public graph API has conceptually:

- head snapshot, depth-independent delta, and SSE cursor wakeup;
- one capped window operation with exactly one anchor: level, block hash, or
  DAA score; the anchor resolves once to a fixed level; and
- status/info covering network, processing/API versions, node state, and
  current validated node server version.

Exact endpoint URLs, HTTP methods, the final wire schema, and the graph wire
format remain deferred in the [decision register](../decisions/deferred.md).

Every graph response carries its hash dictionary. A window extracted from a
`GraphPublication` carries that publication's ID and the extracted view's
revision in addition to `GraphWindowResolution`; that extracted view is
`Frozen`. A view constructed directly from a database starts at revision zero
and has no publication ID unless it is placed in a `GraphPublication`; its
chosen tracking policy determines whether it accepts updates. Database-backed
windows remain capped by `MAX_WINDOW_DEPTH`. Requests crossing the current head
view's lower bound take the consistent DB path; head depth itself never forces
this fallback.

## Resource isolation and saturation — settled

ApiService uses mandatory bulkheads beneath the system-wide processing
priority:

- a capped read-only API database pool separate from processing database
  capacity;
- bounded HTTP concurrency, query duration, response bytes and serialization
  CPU;
- bounded SSE clients and per-client buffers;
- level-scoped delta history, bounded cache memory, and bounded historical-read
  work;
- a separate memory-only status/info admission lane, so graph saturation
  cannot hide service state; and
- distinct budgets for head delivery and historical database reads.

API snapshot reload ranks above historical queries and below processing. On
saturation, reject or degrade API work explicitly. HTTP, database,
serialization, cache, and client saturation must not block processing,
truncate a response or cache image presented as complete, or silently lose a
graph update. Ordinary graph-update delivery remains nonblocking and reports
loss through the session gap signal owned above.

Every historical database-backed HTTP request separates database work from
response construction in this order:

```text
acquire HTTP admission
    -> acquire API DB permit and connection
    -> open one consistent read-only transaction
    -> resolve the anchor and materialize the complete bounded projection
    -> finish the transaction and release the connection and DB permit
    -> use bounded serialization/compression capacity
    -> deliver the response
```

Before serialization or network delivery begins, the request owns an in-memory
projection that borrows no PostgreSQL transaction, connection, row stream,
cursor, or API DB permit. A slow client may retain its HTTP admission and
bounded response-memory capacity, but never database capacity. Serialization,
compression, response-size, and client-delivery failures after that boundary
are API-local and do not retire a database generation or request processing
recovery. A connection-level failure during the database phase retains
StorageService's
[storage-generation failure classification](storage.md#storageservice-lifecycle--settled).

Releasing database resources and detaching the complete projection ends the
request's participation in StorageService's replacement gate. Response
construction cannot issue follow-up database reads. Reset need not wait for or
cancel the remaining serialization and delivery work.

If all `MAX_CACHE_DEPTH = 1000` complete levels exceed the cache memory
allowance, first drop an optional stale image when useful. Otherwise mark head
temporarily unavailable and retry a complete reload. Never publish partial
levels. Slow SSE clients follow the bounded coalescing and disconnect contract
above.

V2 exposes these operational measurements:

- request count, latency, and response bytes by endpoint;
- active and rejected SSE clients;
- encoded-response cache hits, misses, evictions, and coalesced identical
  requests;
- database permit and query time;
- delta-journal resets and slow-client disconnects; and
- BlockProcessor and VspcProcessor commit latency.

The metrics export mechanism and labels remain deferred in the
[decision register](../decisions/deferred.md).

API traffic up to configured rejection limits must not materially increase
either processor's commit latency. Exact API capacities and resource budgets
remain deferred in the [decision register](../decisions/deferred.md).
