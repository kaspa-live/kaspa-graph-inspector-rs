# API graph model

## Scope and ownership

This document owns `GraphView`, `GraphDelta`, `GraphHistory`, their graph value
shapes, view tracking policies, update and extraction behavior, revision and
history advancement, delta application and composition, edge and level
retention, and history pruning. [API publication](api-publication.md) owns publication
identity, lifecycle, construction, alignment, and replacement. Public HTTP and
SSE representation belongs to the [API protocol](api-protocol.md).

## Graph update consumption — settled

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

`MAX_CACHE_DEPTH = 1000` complete levels. No arbitrary maximum block count may
truncate a retained level: **every** block and relevant edge endpoint for each
cached level is available.

The [graph-view contract](#graph-values-views-revisions-and-history--settled)
owns edge representation, window inclusion, endpoint retention, and lifetime.
The [storage-owned `BlockCommitted` payload](storage.md#block-materialization-transaction--settled)
supplies the committed parent information required to apply that contract.

## Graph values, views, revisions, and history — settled

The following conceptual shapes define the graph model. Container and
collection types remain implementation choices.

```rust
enum TrackingPolicy {
    Head,
    Fixed,
    Frozen,
}

enum GraphViewUpdateError {
    Frozen,
}

enum GraphDeltaApplyError {
    Frozen,
    RevisionMismatch {
        view_revision: u64,
        delta_from: u64,
    },
}

enum GraphHistoryAppendError {
    RevisionMismatch {
        history_revision: u64,
        delta_from: u64,
    },
}

enum GraphHistoryRangeError {
    StartPruned {
        requested: u64,
        oldest_available: u64,
    },
    StartUnavailable {
        requested: u64,
        current: u64,
    },
}

enum GraphHistoryRange {
    UpToDate {
        revision: u64,
    },
    Deltas {
        from: u64,
        requested_target: u64,
        actual_target: u64,
        entries: GraphDeltaList,
    },
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
```

A `GraphView` has no origin,
liveness, database, or publication status. A view constructed from a database
starts at revision zero. A subview extract inherits its source view's revision
and has `TrackingPolicy::Frozen`. `Head` accepts updates and advances its
extent. `Fixed` accepts original committed updates only with the post-update
Head view as its metadata cache while their extents overlap. `Frozen` retains
both its captured contents and revision.

## Frozen subview extraction — settled

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

`GraphView` owns application of `BlockCommitted`, `VspcCommitted`, and
`GraphDelta`. A `Frozen` view refuses each mutation entry point with
`GraphViewUpdateError::Frozen` before inspecting whether the input would have a
visible effect; its contents and revision remain unchanged. For `Head` and an
updating `Fixed` view, an update that changes no retained block, edge, or level
returns no delta and leaves the revision unchanged. Any retained change
advances that view's revision and returns the corresponding `GraphDelta`; a
retained level-size or DAA-score change is sufficient even when the triggering
block itself is outside the block extent. Frozen refusal is distinct from an
accepted no-effect update.

## Head block mutation — settled

For a `Head` view, applying one `BlockCommitted` first calculates the target
nominal extent:

```text
target_high = max(current_high, block.coordinate.level)
target_low  = max(1, target_high - max_depth + 1)
```

The operation then performs one atomic graph-state transition:

1. Convert the committed block to a complete `GraphBlock` and retain it when
   its level lies in the target extent.
2. For each actual direct parent with a coordinate, construct the immutable
   `GraphEdge` and retain it when its span intersects the target extent under
   the predicate owned below. An outside-PP parent has no coordinate and
   produces no edge.
3. Apply every relevant `LevelCommitted` snapshot to the inserted block level
   or a retained parent endpoint level. A snapshot replaces only the public
   `Level { size, daa_score }` value. It preserves an existing derived
   `usage_count`; a newly retained level starts with `usage_count = 0`.
4. Each retained edge addition increments the referenced parent level's
   `usage_count`. Multiple edges to one parent level contribute separately.
5. When `target_low` advances, remove every block below it. Removing a child
   removes each edge owned by that child and decrements the corresponding
   parent-level counters. A level that left the nominal extent is removed only
   when its counter is zero; otherwise it remains as an external endpoint.

The returned delta is the exact visible difference between the complete state
before and after that transition: retained block and edge additions or
removals, every created, changed, or removed public level value, and
`high_level = target_high`. Counter-only changes are derived maintenance and do
not enter `LevelChange`. If every public change collection is empty, the
operation returns `None` and does not advance the revision. Otherwise it
advances `n` to `n + 1` and returns `GraphDelta(n, n + 1)`.

This procedure also covers a committed block below `target_low`: its block and
edges are not reintroduced, while a supplied level snapshot can still update
an already retained external endpoint. Concrete indexes, collection types,
mutation staging, and lock boundaries remain implementation choices.

## Fixed updates through the Head cache — settled

An independently maintained `Fixed` view consumes the original ordered
`BlockCommitted` and `VspcCommitted` values, never a delta produced by Head.
For each such value, ApiService first applies it to Head and captures an
immutable reference to the resulting complete Head state. It then offers the
same original value and that post-update Head reference to Fixed even when Head
produced no visible delta. Head and Fixed revisions remain independent.

Before applying the value, evaluate the post-update nominal extents:

```text
fixed.low_level <= head.high_level
AND
head.low_level <= fixed.high_level
```

If they are disjoint, change the tracking policy from `Fixed` to `Frozen`
without applying that value or advancing the graph revision. Head bounds never
decrease, so this transition is terminal. Every later mutation entry point
then follows the ordinary Frozen refusal rule.

While the extents overlap, `BlockCommitted` keeps the Fixed nominal bounds
unchanged. It retains the complete block only when the block lies inside those
bounds, retains every parent edge whose span intersects them, and applies the
supplied snapshots to nominal or retained endpoint levels. Level values and
derived parent-level counters follow the Head mutation rules, but head movement
never prunes Fixed graph structure.

For `VspcCommitted`, resolve immutable metadata needed to project the
transition from the union of Fixed and post-update Head blocks. This lets an
added chain member above the Fixed extent supply its merge sets while
mutations apply only to blocks and levels retained by Fixed. Apply the
storage-owned remove-then-add ordering: retained removed
members leave VSPC and become Gray; retained added members enter VSPC; every
resolved added member's blue and red merge sets recolor matching retained Fixed
blocks. Independently apply every supplied final level snapshot whose level is
already retained by Fixed.

If block metadata required for the complete membership or color projection is
absent from both views, change Fixed to `Frozen` before applying any part of
that value. A snapshot supplies its affected level state directly and does not
require the corresponding removed or added block to exist in either view. Do
not partially mutate the view, query storage implicitly, or advance its
revision. The same terminal transition leaves the internal Fixed contents and
revision unchanged.

A `GraphDelta` produced by this Fixed lineage can still be applied to a
matching Fixed view under the general absolute-map and revision rules.
Applying a standalone Head-generated delta to Fixed is prohibited by the
[rejected-design register](../decisions/rejected.md): it lacks the original
committed event and does not guarantee all projection context.

Fixed-view coherence failure is local. Disjoint extents, unavailable required
Head-cache metadata, a same-lineage delta revision mismatch, or another failure
to apply a complete Fixed mutation leaves the internal view's graph contents
and revision unchanged and transitions its tracking policy to `Frozen`. It
never invalidates Head or reconstructs ApiService.

## Revision and history advancement — settled

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

Every `GraphDelta` is created with
`to_revision_id > from_revision_id`. Direct view mutation produces one-step
deltas; composition preserves a forward interval. Delta application and
history append trust this construction invariant and do not check it again.

```rust
impl GraphView {
    fn apply_delta(
        &mut self,
        delta: &GraphDelta,
    ) -> Result<(), GraphDeltaApplyError>;
}

impl GraphHistory {
    fn append(
        &mut self,
        delta: GraphDelta,
    ) -> Result<(), GraphHistoryAppendError>;
}
```

`apply_delta` first rejects `Frozen`, then requires
`current_revision_id == delta.from_revision_id`. It performs no target-level,
level-prestate, or mutation-content validation. After those checks, it applies
the complete delta atomically. Absolute block and edge changes maintain the
derived edge counters; every `LevelChange.after` directly supplies the public
level result; membership changes precede color changes so those field maps
supersede projection values carried by an added block. A Head view then sets:

```text
high_level = delta.high_level
low_level  = max(1, high_level - max_depth + 1)
```

A Fixed view retains both nominal bounds. Every accepted view sets
`current_revision_id = delta.to_revision_id`. An error leaves the complete view
and revision unchanged. A standalone Head-generated delta remains prohibited
for Fixed; only a delta from the same Fixed lineage has the required context.

`append` requires only
`history.current_revision_id == delta.from_revision_id`. On success, it takes
ownership of the delta, appends it to the ordered list, and sets
`current_revision_id = delta.to_revision_id` atomically. On mismatch, both
remain unchanged. A stored entry may be either a direct one-step delta or an
already aggregated delta. Consecutive stored entries remain gapless by their
outer interval boundaries; history neither requires nor reconstructs internal
revision boundaries within an aggregate.

`GraphDelta.high_level` is the high level of the revision lineage that owns the
delta at `to_revision_id`. A canonical Head delta therefore carries the target
Head high level. A delta from an internal Fixed lineage carries that Fixed
view's high level. The
[Head-bounded projection contract](api-protocol.md#head-bounded-fixed-delta-projection--settled)
owns this field's meaning in a derived public projected response. For canonical
and internal deltas, the field is retained in history, included in the API
delta payload, provides the semantic key for Head-history pruning, and exposes
the lineage high level without requiring derivation from mutation contents. A
delta does not carry `low_level` or a separate coverage object; explicit graph
mutations drive a Head view's lower-bound changes.

In canonical and internal lineage deltas, `level_changes` contains only levels
whose `size` or `daa_score` actually changed. There, `None` represents absence,
so one shape covers creation, update, and removal. Application installs `after`
without comparing the current value to `before`; `before` exists for
composition. Gapless composition folds consecutive changes from the first
`before` to the last `after` and omits a level whose composed change has no net
effect. Because each change carries its pre-state, this composition does not
require the starting view. Within the graph model, `before = None` means
absence. The
[Head-bounded projection contract](api-protocol.md#head-bounded-fixed-delta-projection--settled)
owns its sole presentation-only exception.

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

## VSPC projection and delta composition — settled

A mutable `Head` `GraphView` applies one `VspcCommitted` in the same mutation
order as the storage transaction, projected onto its retained blocks:

1. each present removed block leaves VSPC and is reset to `Gray`;
2. each present added block enters VSPC;
3. for every present added block in order, each present blue merge-set member
   becomes `Blue`, then each present red merge-set member becomes `Red`; and
4. every supplied final level snapshot whose level is already retained replaces
   that level's public `size` and `daa_score`, preserving its derived
   `usage_count`.

Membership and color are separate mutable fields. The two change maps are
independent and may contain the same block hash. Each atomic update records the
field's value before the complete VSPC transition and its final value after all
steps; it omits a field whose final value equals its original value. Temporary
states within the transition never enter the delta.

An absent block mutation target is ignored without a placeholder, deferred
mutation, storage lookup, or fault. The normal absent case is a block already
in the past below the view extent; its merge-set members are also in its past
and cannot affect retained blocks. Level snapshots are independent of block
presence: they update an already retained nominal or external endpoint level,
and are ignored when their level is absent. A later absolute
`Some(GraphBlock)` carries that block's complete then-current projection and
needs no replay of ignored field changes. If all projected field and level
effects are absent or no-ops, the atomic VSPC update returns no delta and
advances no view revision.

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

VSPC DAA-score projection uses the existing `level_changes` collection. For
each supplied snapshot whose level is retained, capture the original public
value and install the snapshot's complete final `size` and `daa_score` while
preserving the derived `usage_count`. Emit one `LevelChange` only when that
public value changed. Both sides remain `Some(Level)` because a VSPC update
does not create or remove levels. Existing `LevelChange` composition then
combines block-level creation or size changes with the VSPC result and removes
composed no-ops. A retained level-score change alone is sufficient to produce
a graph revision.

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
counter reaches zero. A Fixed view keeps its nominal extent and retained graph
structure until its terminal transition to Frozen under the contract above.

`GraphView` and `GraphDelta` contain no committed VSPC sink. ApiService trusts
the ordered, continuity-certified `VspcProcessor` output and does not duplicate
its continuity validation.

GraphHistory retention is level-scoped. All revisions and deltas produced while
a graph level remains within the retained head window are preserved. History
associated with that level becomes eligible for pruning only when the level
itself leaves the retained window. No independent revision-count or memory-size
cap is required or permitted by this design.

```rust
impl GraphHistory {
    fn prune(&mut self, head_low_level: u64);

    fn oldest_available_revision(&self) -> u64;

    fn range(
        &self,
        from_revision_id: u64,
        target_revision_id: u64,
    ) -> Result<GraphHistoryRange, GraphHistoryRangeError>;
}
```

Pruning removes the longest stored prefix for which:

```text
delta.high_level < head_low_level
```

It never removes a middle entry. An aggregated stored delta is indivisible and
uses the `high_level` of its final constituent: retain the whole aggregate
until that level leaves the Head extent, then remove the whole aggregate. This
may retain older constituent history longer than necessary and is safe.
Pruning never changes `current_revision_id`. If entries remain,
`oldest_available_revision()` is the first entry's `from_revision_id`;
otherwise it is `current_revision_id`.

After append, ApiService may prune against the current complete Head
`low_level` before publishing the new complete history value. A history reader
therefore observes either the previous complete value or the appended and
pruned complete value; pruning is never exposed halfway through.

Kaspa does not permit unbounded revision production while the graph remains
indefinitely at one fixed level. Therefore retaining all history for every
level still inside the retained window is bounded by graph/window semantics. A
hypothetical infinite activity stream at one level is not a valid Kaspa
behavior and cannot justify an additional history cap. A cursor whose required
deltas left with their level normally requires a fresh view.

Gapless canonical intervals selected from one `GraphHistory` compose
sequentially:

```text
apply(Delta(a,b), image_at_a) = image_at_b
apply(Delta(b,c), image_at_b) = image_at_c
compose(Delta(a,b), Delta(b,c)) = Delta(a,c)
```

Composition requires exact equality between the left `to` and right `from`
revisions. The result uses `a` as `from`, `c` as `to`, and delta `c`'s
`high_level`. Composition is associative by graph-state effect. A composed
encoding need not be byte-identical to a directly constructed interval, but it
must have the same graph-state effect. Every canonical Head delta response uses
a self-contained response-local hash dictionary. Composition decodes the
input dictionaries to hashes and constructs a new dictionary for the result.

For a range request, the starting revision must be an exact retained entry
boundary. A start before `oldest_available_revision()` returns `StartPruned`;
a start inside an aggregate, after current history, or otherwise absent returns
`StartUnavailable`. When the requested target equals the start, return
`UpToDate`. Otherwise, select gapless entries beginning exactly at the start.

The target need not be a retained boundary. If it lies inside an aggregated
entry, include that complete entry and set `actual_target` to its right
boundary. If it is beyond current history, use the current history right
boundary. The returned range always records both the requested and actual
targets. If that available right boundary equals the start, return `UpToDate`.
An aggregated entry is never split and its discarded internal boundaries are
not reconstructed.

ApiService rejects a publication mismatch or terminal Stale publication before
range selection. It then composes selected entries in order under the response
budget. Return `Complete` when the complete selected range fits. Otherwise
return the largest nonempty `Prefix` that fits and ends at a stored entry's
right boundary. If the first stored entry cannot fit, return
`FreshViewRequired(FirstStoredDeltaExceedsBudget)`. A publication mismatch,
Stale publication, `StartPruned`, or `StartUnavailable` maps to the matching
fresh-view reason. `UpToDate` is an ordinary success.

Every canonical range delta is structurally complete and reports its actual
`to_revision_id`, which may be later than the requested target because an
aggregate was indivisible, earlier because the response budget selected a
prefix, or current because the requested target was in the future. Composition
constructs a new response-local hash dictionary. Exact encoded-size measurement
and whether a complete interval is transmitted as its entries or one composed
patch remain implementation choices under the wire-format decision.
