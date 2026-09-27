# API architecture

## Scope and ownership

This document owns ApiService, `GraphView`, `GraphDelta`, `GraphHistory`,
`GraphPublication`, API resource bulkheads, and the public graph API contract. The
[processing lifecycle](processing-lifecycle.md) owns when recovery milestones
send API controls. [Storage](storage.md) owns database transaction and query
implementation. Web client behavior belongs to the
[Web architecture](web.md).

## In-process API and graph observer feed — settled

KGI v2 includes an in-process `ApiService` and a complete bounded-by-level
head-tracking `GraphView`, rather than directing each head request to expensive
PostgreSQL graph queries. The [system overview](overview.md#resource-isolation-and-scalability--settled)
owns deployment evolution and the single-writer constraint.

Processors send `BlockCommitted`/`VspcCommitted` through **one ordered,
bounded graph-update channel**, after their respective DB commits. The
[BlockProcessor delivery contract](block-processing.md#committed-block-delivery)
and [VspcProcessor producer contract](vspc-processing.md#commit-and-graph-publication--settled)
establish causal order, so a VSPC update cannot reach this channel ahead of
blocks it depends on.

The observer channel is nonblocking from the processing viewpoint:
failed/full delivery sets an out-of-band invalid flag; ApiService stops
publishing deltas, marks the current publication Stale, and reloads from DB.
Any coherent replacement uses a fresh publication ID and the session's current
lifecycle target. No processing recovery or producer sequence number is needed
for observer-only continuity. The invalid flag also catches loss of the
**last** update that no later sequence number could expose.

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
The observer payload above supplies the committed parent information required
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

### Seed construction and observer replay — settled

ApiService begins buffering graph updates before loading a head-publication
seed. On every new processing session it reconstructs the publication because
the previous session may have committed data without delivering its observer
update. Construct the seed as a revision-zero `Head` view and a revision-zero
history, then scan buffered updates for the snapshot boundary.

Before that boundary, classify `BlockCommitted` as follows:

```text
block hash is present in the view
    -> skip it

block hash is absent
    -> apply it
    -> no delta: keep searching
    -> delta returned: this update is the boundary
```

The alternative boundary is the first `VspcCommitted` whose `source` equals
`snapshot_vspc_sink`; apply that update as the boundary. Before either rule
succeeds, skip other VSPC updates as already represented. The first of the two
rules to succeed wins. From that update onward, apply every remaining buffered
and newly received update in channel order without further snapshot
classification or API-side VSPC continuity checking.

An absent block outside retained state can produce no delta and therefore does
not establish the boundary. If the buffer ends first, the seed is already a
coherent publishable image and the private boundary detector remains active
for later updates. Discard `snapshot_vspc_sink` when either rule eventually
succeeds.

Every applied visible update uses the ordinary mutation path: advance the view,
return its delta, then append that delta to history. Staging can therefore make
the first visible publication revision greater than zero. An update with no
retained effect advances neither revision. View replacement and history
replacement each become visible atomically to their own readers, but there is
no joint view-history snapshot or revision-equality invariant. Readers can
observe a complete view at revision `n + 1` while history independently remains
complete through revision `n`.

Successful head construction creates a fresh random nonzero publication ID and
uses the newest reliable lifecycle target: normally `Synchronizing`, or `Live`
when Live arrived while construction was pending. Readers never observe a
partly constructed view or a partly appended history. The previous coherent
publication remains terminally Stale and receives no later deltas.

An `AnchoredWindow` seed constructs a revision-zero `Fixed` view whose
`max_depth` is its effective nominal level count. It has no history or
publication ID and is discarded after the coherent HTTP response is built.
Database construction alone does not imply `Frozen`; that policy remains
reserved for subview extraction.

An API-pool or query failure, API database-generation loss, observer-invalid
flag, staging-buffer overflow, superseding Reset or `InvalidateSession`, seed
construction failure, or update-application failure abandons only the current
API attempt. Retry from a newer consistent snapshot while the processing
session remains current; never request processing Resync or Rebuild for an API
read-model failure. A previous coherent publication remains readable as Stale.
Without one, graph endpoints return `503 Service Unavailable`, while the
separate status/info lane remains available.

### Publication state and revision — settled

ApiService has three externally meaningful publication states:

```text
Stale | Synchronizing | Live
```

Publication construction establishes its initial state without inventing a
lifecycle delta. PostSeal normally publishes the coherent replacement as
`Synchronizing`. If the reliable Live control arrived while construction was
still pending, record that newer target and publish the completed image
directly as `Live`.

Once a publication is visible, the state-specific controls admit only
`Synchronizing -> Live`, `Synchronizing -> Stale`, and `Live -> Stale`.
`Stale` is terminal; a replacement always has a fresh `publication_id`. These
control-plane transitions do not modify `GraphView`, create `GraphDelta`, or
advance either view or history revision. A control that finds no publication,
or finds the current publication already `Stale`, creates no additional state
effect.

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

The existing reliable processing-to-ApiService control path carries
conceptual `Reset`, `PublishPostSeal`, `PublishLive`, and `InvalidateSession`
controls; no new recovery component is needed. The exact send points and
ordering belong to
[processing-lifecycle.md](processing-lifecycle.md#api-session-replacement-and-publication).
ApiService accepts exactly one Reset for each prepared processing session.
Common Reset effects are:

- mark a non-Stale previous publication Stale while allowing that coherent old
  image to remain readable;
- stop delta production for the old publication;
- prevent pending observer updates from the previous processing session from
  entering the replacement publication; and
- arm buffering for the new session before processor Begin.

Reset has a completed-effect acknowledgement. The acknowledgement establishes
those effects but does not mean that a replacement image has been published.
A previous publication already Stale and a still-pending unpublished
construction require no additional graph or history action. Ordinary
graph-update loss semantics do not weaken this reliable control barrier.
PostSeal and Live are reliable, exact-once, state-specific controls, but they
are not processing barriers and have no publication-completion
acknowledgement.

`InvalidateSession` is a reliable, exact-once terminal control for a recoverably
aborted processing session. It gives that session's active publication its
terminal Stale state without changing its graph revision, stops later deltas,
cancels any pending publication, and rejects later observer updates from the
invalidated session. It creates no `GraphPublication`, performs no DB reload,
and does not arm buffering for a replacement session. The next prepared session
still begins with its own Reset. Invalidation leaves
historical reads available unless the invalidated session's database-rebuild
Reset had already closed them; in that case they remain closed until a later
PostSeal publication reopens them.

An ordinary Resync Reset leaves new and in-flight historical DB reads
available.

The database-rebuild Reset has the additional completed effect of closing new
historical DB reads and boundedly draining or cancelling existing ones before
processing data is cleared.
Historical window requests during `RebuildingDatabase` and `PreSeal` fail
cleanly with `503 Service Unavailable` and a short `Retry-After`. Merely
catching SQL errors is insufficient: a query against a partly reconstructed
DB can succeed but return an incomplete graph. A read already in flight may
return its old coherent snapshot if it completes before the reset barrier;
otherwise cancel it and return 503. API reads cannot indefinitely delay
processing.

PostSeal starts the consistent snapshot plus buffered-update replay. Once
coherent, ApiService publishes a new `GraphPublication` in the current target
state. This is normally `Synchronizing`; it is initially `Live` when that
control arrived during loading. Publication reopens historical reads after
Rebuild. Processing does not wait for it to complete.

The Live trigger updates the same publication without another database reload.
If PostSeal loading has not finished, ApiService records the newer target and
publishes the completed image directly as Live. Otherwise it changes the
publication state to Live without changing the graph revision or history.

If `TRUNCATE` is used inside the atomic rebuild, its transactional rollback
does **not** make it generally MVCC-safe for concurrent pre-existing
snapshots. Implementation must prove pre-reset reads return one old coherent
image or get canceled, never mixed old/new tables. See PostgreSQL's
[`TRUNCATE`](https://www.postgresql.org/docs/current/sql-truncate.html) and
[MVCC caveat](https://www.postgresql.org/docs/current/mvcc-caveats.html)
documentation. The API read barrier and bounded queries serve this contract;
the selected reset mechanism must preserve the same observable behavior. Its
detailed cancellation and transaction mechanism remains deferred in the
[decision register](../decisions/deferred.md).

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
saturation, reject or degrade API work explicitly. Do not block processing,
truncate a response or cache image presented as complete, or silently drop a
processing notification.

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

Releasing database resources does not complete the HTTP request for purposes
of the acknowledged Rebuild Reset. The request remains tracked until delivery
or cancellation under the
[Reset contract](#reset-and-recovery-time-availability--settled), and response
construction cannot issue follow-up database reads.

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
