# API protocol architecture

## Scope and ownership

This document owns the public HTTP and SSE graph contract, including public
graph limits and values, cursors, windows, Head-bounded Fixed projection,
public errors, ETags, response caching, and delivery behavior. ApiService
control and resource bulkheads belong to the
[ApiService architecture](api-service.md). Graph publication belongs to
[API graph publication](api-publication.md). `GraphView`, `GraphDelta`, and
`GraphHistory` belong to the [API graph model](api-graph.md).
The per-session graph-update channel, producer gate, lifecycle-marker delivery,
and gap reporting belong to [API graph-update ingress](api-ingress.md).
The [processing lifecycle](processing-lifecycle.md) owns when Supervisor calls
`reset` and when processor phase commands occur. [Block processing](block-processing.md)
owns lifecycle-marker production. [Storage](storage.md) owns database
transactions, query implementation, and database-replacement exclusion. Web
client behavior belongs to the [Web architecture](web.md).

## Public graph limits and identity — settled

Define `MAX_WINDOW_DEPTH` and `MAX_CACHE_LEVEL_DISTANCE` under this strict
bound:

```text
MAX_CACHE_LEVEL_DISTANCE + MAX_WINDOW_DEPTH < MAX_CACHE_DEPTH
```

Their exact values remain deferred in the
[decision register](../decisions/deferred.md). Every windowed endpoint caps
requested depth to `MAX_WINDOW_DEPTH` and reports the effective range. An
oversized `/graph/head` request cannot fall back to DB; it is capped. The
[graph model](api-graph.md) owns `MAX_CACHE_DEPTH` and complete-level retention.

The domain-owned `CompactId` crosses into ApiService only as the private
alignment cut and `BlockCommitted.id`; it is never a public block identity or
wire value. Each HTTP graph response has its own small numeric references and
an included local ID-to-hash dictionary covering **all** hashes it references,
including off-window parent endpoints and merge-set members. These local IDs
are not persistent across responses or instances; coordinates may diverge
across independently allocated DBs.

The public block projection preserves its **actual direct-parent list** even
when some parents are outside the response or PP boundary and have no
drawable edge. A materialized Genesis is recognized from its empty actual
direct-parent list. The public projection and Web client do not need to expose
or consult the persisted `NodeMetadata.genesis_hash`, and no dedicated
Genesis-hash API endpoint is required.

## Public API values — settled

The following conceptual shapes are shared across public response and window
contracts. Container and collection types remain implementation choices.
Graph-owned values are defined by the
[graph model](api-graph.md); publication-owned values are defined by
[API graph publication](api-publication.md).

```rust
enum FreshViewReason {
    PublicationMismatch,
    PublicationStale,
    StartPruned,
    StartUnavailable,
    DeltaDistanceExceeded,
    FirstStoredDeltaExceedsBudget,
}

enum DeltaResponseOutcome {
    UpToDate,
    Complete(GraphDelta),
    Prefix(GraphDelta),
    FreshViewRequired(FreshViewReason),
}

enum FixedProjectionEndReason {
    ExtentLeftHead,
    RequiredLevelUnavailable,
}

enum FixedDeltaResponseOutcome {
    UpToDate {
        revision: u64,
    },
    Complete(GraphDelta),
    Prefix(GraphDelta),
    Freeze {
        valid_prefix: Option<GraphDelta>,
        reason: FixedProjectionEndReason,
    },
    FreshViewRequired(FreshViewReason),
}

enum GraphWindowAnchor {
    Level(u64),
    BlockHash(BlockHash),
    DaaScore(u64),
}

enum GraphWindowAnchorUnavailable {
    LevelNotRetained {
        requested_level: u64,
    },
    BlockNotMaterialized {
        requested_hash: BlockHash,
    },
    NoRetainedDaaMatch {
        requested_score: u64,
    },
}

struct GraphWindowResolution {
    resolved_level: u64,
    effective_start_level: u64,
    effective_end_level: u64,
}

struct SystemStatus {
    kgi_version: String,
    supervisor: SupervisorStatus,
    node: NodeServiceStatus,
    storage: StorageServiceStatus,
    processing: ProcessingStatus,
}
```

Together `(publication_id, revision)` form the public cursor. If the selected
wire format cannot represent every `u64` exactly, its encoding must preserve
the complete integer domain.

`SystemStatus.kgi_version` is the compile-time package version of the running
KGI executable. Processing is part of that executable, so there is no separate
processing version. The internal `representation_version` used for graph ETag
and encoded-cache identity is not a status field, and v2 defines no separate
public API-version value.

The component observations are exactly Supervisor, NodeService,
StorageService, and processing. `node.last_validated` is the sole public source
of network identity, node server version, and upstream RPC API version and
revision. [NodeService](node-service.md#nodeservice--settled) owns that
component observation's lifecycle and current-versus-last meaning; the
[ApiService status-observation contract](api-service.md#status-observation--settled)
owns its availability in the composite response.
`SystemStatus` has no ApiService-status field: successful status admission
already proves that the memory-only status lane is serving, while graph
publication state is exposed through its own graph responses and SSE wakeups.

## Window construction and publication lineage — settled

For the database-backed window path, an `AnchoredWindow` seed constructs a
request-local, revision-zero `Fixed` view whose `max_depth` is its effective
nominal level count. `Fixed` selects its extent semantics; no updates are
delivered to this request-local view. ApiService serializes it into one coherent
HTTP response and then discards it. Because it is not placed in a
`GraphPublication`, the response has no publication ID, history, SSE cursor,
or public delta lineage. Its request-local failures return their ordinary API
result and do not participate in the head-publication state machine.

The Head-extracted window path is separate. A window whose complete effective
extent is extracted from the active Head publication carries that
publication's lineage metadata and can use the
[Head-bounded projection contract](#head-bounded-fixed-delta-projection--settled).
Database construction alone does not imply `Frozen`; that policy remains
reserved for subview extraction.

Without a coherent graph publication, graph endpoints return `503 Service
Unavailable`, while the separate status/info lane remains available.

## Publication wire observation — settled

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
the publication state later changes.

The API response envelope, rather than `GraphView` or `GraphDelta`, carries the
owning `publication_id`. A published view response pairs it with the view's
current revision and publication state; a delta response pairs it with the
history interval actually returned.

## Canonical Head delta responses — settled

ApiService rejects a publication mismatch before consulting history. An Active,
Synchronizing, or terminal Stale publication is otherwise eligible: Stale
history no longer advances, but clients may catch up through its final stalled
Head. For an eligible publication ApiService calls the graph-owned
[`GraphHistory::range`](api-graph.md#retained-history-range)
operation, which owns retained-entry selection and target extension across an
indivisible aggregate and returns the selected gapless
`GraphHistoryEntryList`.

Public outcomes map as follows:

- `GraphHistoryRangeError::BackwardTarget` returns `400 Bad Request`;
- publication mismatch returns
  `FreshViewRequired(PublicationMismatch)`;
- `GraphHistoryRangeError::StartPruned` returns
  `FreshViewRequired(StartPruned)`;
- `GraphHistoryRangeError::StartUnavailable` returns
  `FreshViewRequired(StartUnavailable)`; and
- `GraphHistoryRange::UpToDate` returns `DeltaResponseOutcome::UpToDate`.

Before cache lookup, joining a job, or constructing a delta-to-Head response,
use the first selected stored
entry's `delta.high_level` as the deliberately approximate source-level
boundary. If its distance behind the publication Head reaches or exceeds 80%
of `MAX_CACHE_LEVEL_DISTANCE`, return
`FreshViewRequired(DeltaDistanceExceeded)`. Do not reconstruct an exact source
level. A client beyond this gate reloads a complete applicable window.

For an eligible `GraphHistoryRange::Deltas`, ApiService uses the entries'
cumulative estimated costs to choose a complete target boundary under the soft
estimated response budget:

1. choose the requested available right boundary when its estimate fits;
2. otherwise choose the youngest reachable hot point, where a hot point is the
   source revision of an existing `CachedDelta`;
3. otherwise choose the youngest ordinary stored-entry boundary that fits.

When appending a canonical Head delta, ApiService supplies its cheap
`estimated_raw_bytes` from mutation counts and approximate field sizes for the
cacheable public representation. The estimate excludes block and edge
removals, is additive across entries, and performs no graph encoding or
compression. Exact weights remain deferred; the estimate selects a candidate
and never authorizes an oversized response.

The target is captured when the single-flight job starts and does not advance
with Head while that job is running. Compose the chosen entries through the
graph-owned [delta composition operation](api-graph.md#graph-delta-composition),
construct the response-local hash dictionary, encode once, and check the hard
response-byte limit. If the encoded result exceeds that limit, retry at an
earlier complete boundary, preferring another hot point. If even the first
complete stored entry cannot fit, return
`FreshViewRequired(FirstStoredDeltaExceedsBudget)`. An aggregated stored entry
is indivisible and is never truncated or split.

Every returned delta is complete under the public Head-window contract and
reports its actual `to_revision_id`. It may be later than the requested target because the graph
range extended through an indivisible aggregate, earlier because budgeting
selected a prefix, or current because the requested target was in the future.
Composition operates on graph hashes through the graph-owned operation; public
encoding then constructs a new response-local hash dictionary for the result.
The emitted response is one composed patch and must remain within the
configured byte limit.

Return `Complete` when the actual returned right boundary reaches or extends
through the request's explicit or captured target. Return `Prefix` when target
selection or encoded-size fallback stops at an earlier complete boundary.
These are request-specific envelope outcomes rather than fields of the encoded
delta body. A later request may therefore wrap the same cached body differently
as Head advances.

Every delta response carries `KGI-More-Available`. For `Complete` or `Prefix`,
ApiService compares the returned `to_revision_id` with the publication history Head observed while
assembling that response. Equality yields `false`; an earlier target yields
`true`. A client receiving `true` immediately requests again from the returned
target. A client receiving `false` waits for an SSE wakeup. For a Stale
publication the comparison uses its final stalled Head. A `false` observation
can become obsolete immediately after response assembly; ordinary SSE delivery
covers that race. For `UpToDate`, make the same comparison against the request's
starting revision.

Public cached Head deltas omit block and edge removals. Under the strict
[public graph-limit bound](#public-graph-limits-and-identity--settled), the
publication removal frontier cannot reach any
block or child-owned edge retained by an eligible client window. The client
uses `GraphDelta.high_level` and its own window depth to advance `low_level`,
remove blocks below that boundary, and remove edges when their child leaves the
window. VSPC membership and color changes remain ordinary field mutations and
are never discarded by this rule. Required level changes remain present.

Apply this omission only after composing the complete canonical history
interval, then construct the derived public `GraphDelta` through the checked
graph-owned constructor. The derived response is not a `GraphHistoryEntry`, is
never appended to server history, and is never applied to ApiService's complete
Head `GraphView`. Its completeness is relative to the bounded public client
window established above.

## Head-bounded Fixed delta projection — settled

A public anchored window extracted from the active Head publication can follow
that publication without acquiring its own server-side `GraphPublication` or
`GraphHistory`. Its serialized response carries the Head `publication_id` and
source revision, the source Head `high_level` at that revision, and its fixed
effective level extent. The source Head level is lineage metadata, while the
effective extent describes the serialized window. A database-backed window has
no such lineage; the API returns it as a non-updating serialized snapshot.

The public projection operation is conceptually:

```rust
fn range_for_extent(
    publication_id: u64,
    from_revision_id: u64,
    requested_to_revision_id: u64,
    low_level: u64,
    high_level: u64,
) -> FixedDeltaResponseOutcome;
```

ApiService first applies the ordinary Head-history publication, availability,
boundary, and target selection rules to obtain eligible gapless entries. That
validation rejects a backward target before projection under the canonical
Head-delta protocol. ApiService does not apply the canonical Head response-byte
budget before projection. It
projects eligible complete entries in order to `[low_level, high_level]` and
applies the Fixed response budget to the final projected representation. The
fixed extent must remain inside the Head nominal extent throughout every
returned entry:

```text
head.low_level <= low_level
AND
high_level <= head.high_level
```

The initial Head extraction establishes the upper-bound condition at
`from_revision_id`. Because Head bounds never decrease, each selected entry's
`high_level` and the Head `max_depth` determine whether its resulting lower
bound still covers `low_level`. An aggregated history entry is indivisible. If
one would move the Head lower bound above `low_level`, do not project that
entry. Return earlier complete entries as `Freeze.valid_prefix`, when present,
and `ExtentLeftHead`; otherwise return that terminal reason without a prefix.
The current active Head view used for endpoint-level enrichment must also still
contain the fixed extent. Once it does not, return `ExtentLeftHead`; never use a
database read to reconstruct a departed extent.

Projection retains blocks whose coordinates lie in the fixed extent; edges
whose level spans intersect it under the settled edge predicate; nominal
levels in the extent; external endpoint levels required by retained edges; and
VSPC-membership or color changes for retained blocks. Head pruning cannot
remove a retained fixed block or edge while the containment predicate holds.
Its block and edge removals therefore lie outside the fixed projection and are
discarded by the extent filter.

A newly retained edge may require an endpoint `Level` that did not change in
Head and therefore is absent from the stored Head delta. In that case,
ApiService copies the complete current `Level` from the active Head view into
the projected result as:

```text
LevelChange {
    level,
    before: None,
    after: Some(current_level),
}
```

Within a Head-bounded projected response, this `before = None` means that the
receiver's previous value is unspecified; it does not assert that the level
was absent. Its wire meaning is an absolute upsert: `after` supplies the
resulting value and `before` is not an application precondition. Canonical and
internal deltas retain the ordinary absence meaning of `None`. ApiService adds
this presentation-only change after composing the canonical projected
interval, so it never participates in canonical delta composition or changes
its no-net-change rules. If that level is unavailable, return
`Freeze { reason: RequiredLevelUnavailable, .. }`. This lookup never consults
PostgreSQL.

The endpoint level may be newer than the returned `to_revision_id`; this is an
accepted presentation approximation. Block, edge, VSPC-membership, and color
mutations remain exact for the returned interval. A synthesized level never
affects cursor continuity, containment, or mutation selection, and later
absolute level changes supersede the approximated value.

Budget selection treats every stored Head-history entry as an indivisible
unit. For each candidate prefix, ApiService projects and composes its complete
entries, then adds every required endpoint `Level` to that candidate result and
measures the resulting complete response. The measured representation includes
the projected mutations, synthesized endpoint levels, response-local hash
dictionary, and response envelope. An entry is accepted only when that final
representation fits the Fixed response budget. Projection never splits an
aggregated entry, drops an endpoint level, or presents a truncated entry as
complete.

If all eligible entries fit, return `Complete`. If at least one fits but the
next complete projected entry does not, return the accepted entries as
`Prefix`, ending at the last accepted stored-entry boundary. If the first
complete projected entry does not fit, return
`FreshViewRequired(FirstStoredDeltaExceedsBudget)`. Existing projection
failures retain their `Freeze` outcomes. Exact encoded-size measurement and
whether the accepted interval is transmitted as entries or one composed patch
remain implementation choices under the wire-format decision, but the emitted
response must remain within the configured byte limit.

Every projected response uses a self-contained response-local hash dictionary
and carries the Head `(publication_id, revision)` source cursor. A projected
interval may contain no retained mutation and still advance that cursor. Every
returned projected `GraphDelta.high_level` is the source Head high level at the
returned `to_revision_id`; it does not move either bound of the serialized
fixed extent. This derived response is not a Head-history entry or an internal
Fixed lineage delta: do not append it to `GraphHistory`, apply it to
ApiService's internal `GraphView`, or use it as an input to canonical delta
composition. The internal updating-`Fixed`
[contract](api-graph.md#fixed-updates-through-the-head-cache--settled) remains
unchanged.

Projected Fixed delta responses are generated on demand and are never cached.
They carry `Cache-Control: no-store`, have no ETag, do not enter encoded-body
caches or cache single-flight construction, and have no immutable interval
cache identity.

Publication mismatch, terminal Stale state, and unavailable Head history use
the ordinary fresh-view outcomes. Every `FixedProjectionEndReason` is a
terminal server outcome for that projected lineage; no continuation is
available through a later range on the same lineage.

## Public projection and delivery failures — settled

Fixed-view coherence failures follow the local disposition owned by the
[graph model](api-graph.md#fixed-updates-through-the-head-cache--settled).

Fresh-view range outcomes affect only their request. Serialization,
compression, response-size enforcement, client cancellation, and delivery
failure likewise remain request-local under the
[ApiService resource contract](api-service.md#resource-isolation-and-saturation--settled).
These request-local cursor and response failures never request processing
Resync or Rebuild.

SSE is only an ordered `PublicationWakeup`, not the graph data channel.
Reconnection is not exactly-once. Each client has a bounded wakeup buffer. On
connection the server emits the latest history cursor and publication state;
subsequent history advancement, state change, or replacement publication emits
an updated wakeup. Slow clients receive coalesced wakeups and, if persistently
behind, are disconnected and recover through HTTP delta or view requests.

`representation_version` is the settled term for the graph payload schema.
KGI v2 selects exactly one graph wire format after the deferred format
evaluation; clients do not negotiate among graph encodings. An ETag for a head
view distinguishes publication ID, revision, effective window,
`representation_version`, and publication state; `Cache-Control: no-cache`
allows cheap revalidation/304. A delta-to-current query must revalidate.
Historical database windows, projected Fixed deltas, and SSE have no ETag;
projected Fixed deltas additionally require `Cache-Control: no-store` under
their owning contract. HTTP compression is separate from graph-format
selection and remains part of the deferred encoding work.

## Publication-scoped Head response cache — settled

For the publication-owned `GraphCache`, defined by the
[publication value](api-publication.md#publication-and-seed-values--settled),
internal identities omit `publication_id` because the containing publication
already supplies it. The canonical cached delta value is:

```rust
struct CachedDelta {
    from_revision_id: u64,
    to_revision_id: u64,
    high_level: u64,
    encoded_body: Bytes,
}
```

`encoded_body` is the immutable encoded graph-delta body. It does not contain
the request-specific `Complete`/`Prefix` discriminator or progress state.
ApiService creates a lightweight response around those bytes and supplies the
outcome plus `KGI-More-Available` as defined by the canonical Head-delta
contract. For one cached edge, a delta-to-current outcome can move only from
`Complete` to `Prefix`, and the header only from `false` to `true`, as an Active
Head advances beyond `to_revision_id`; neither moves back within the
publication. Whether the lightweight HTTP wrapper is also retained is an
implementation detail.

The delta cache and its single-flight job map serve delta-to-current requests
and are keyed by source revision. Other exact-target canonical requests bypass
both. At most one logical job runs for a source revision. Concurrent requests
attach to that job even if Head advances after its target was captured. Success
publishes one `CachedDelta` at the source key and completes every waiter.
Failure returns the same request-local error to every waiter, removes the job,
and permits a later request to start another job. The soft-cost
target-selection policy defines cached source revisions as hot points for later
jobs.

The [publication lifecycle](api-publication.md#publication-state-and-revision--settled)
owns preservation, cancellation, and release of the cache and its jobs across
Stale and replacement. While Active or Stale, evict a `CachedDelta` when its
source revision is no longer retained or its target `high_level` is more than
`MAX_CACHE_LEVEL_DISTANCE` behind that publication's Head. These structural
bounds replace an independent encoded-byte cache cap.

An encoded-cache miss or eviction affects performance only: reconstruct the
response from the current `GraphView` or retained `GraphHistory`. Optional
exact-revision Head-view response reuse belongs to the same publication and may
be dropped at any time. Historical database-backed windows, projected Fixed
deltas, and SSE remain uncached in v2; projected Fixed requests also bypass
cache single-flight. Cache work cannot affect graph correctness or processing.
Concrete cache collections and the estimator weights remain deferred in the
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
and no floor exists, return `NoRetainedDaaMatch`. A `q` beyond the current VSPC
DAA resolves the current VSPC level. If `q` is at or above the
DAA of the current head view's lowest cached VSPC level, that view can resolve it; otherwise use the
storage lookup. Level resolution and its window must come from **one immutable
graph view** or one consistent DB transaction, never different revisions.
ApiService obtains cached levels' authoritative final scores from the complete
level snapshots carried by `VspcCommitted`. Historical DAA/window responses
are not cached in v2.

For a database-backed anchored window, each
`GraphWindowAnchorUnavailable` variant is a normal request-local `404 Not
Found` response. It does not enter the
[ApiService generation-loss path](api-service.md#api-database-generation-binding--settled),
reconstruct or stale a publication, or request processing recovery. The
[storage operation](storage.md#api-graph-projection-reads--settled) owns the
exact database condition producing each variant.

A disabled public-read gate, absence of a current API DB client, or denial or
cancellation by the storage replacement gate returns `503 Service Unavailable`
with a short `Retry-After`.

Public database-backed `ApiReadError` outcomes are exhaustive:

| Error | Public response | Binding path |
|---|---|---|
| `QueryFailed` | `500 Internal Server Error` | Non-generation-loss |
| `GenerationLost` | `503 Service Unavailable` with a short `Retry-After` | Generation-loss |
| `InconsistentProjection` | `500 Internal Server Error` | Non-generation-loss |

No error returns a partial graph or transparently retries the failed request.
The [ApiService binding contract](api-service.md#api-database-generation-binding--settled)
owns the exact `ApiDbState` transition for each named binding path. Public read
errors change no publication ID or state, view or history revision, SSE cursor,
or processing recovery obligation; they neither stale nor reconstruct an
Active Head.

Reject invalid anchor input before storage access with `400 Bad Request`. This
includes level zero, a DAA score outside the shared range, an invalid
`max_depth`, and a block hash that cannot be decoded into `BlockHash`. These
input errors are distinct from a valid typed anchor that has no retained
match.

## Public API surface — settled

The public graph API has conceptually:

- head snapshot, depth-independent delta, and SSE cursor wakeup;
- one capped window operation with exactly one anchor: level, block hash, or
  DAA score; the anchor resolves once to a fixed level;
- on-demand Head-bounded delta projection for a window extracted from the
  active Head publication; and
- status/info returning `SystemStatus`, including the KGI version, the exact
  component observations, and the current or last successfully validated
  network and node server information.

Exact endpoint URLs, HTTP methods, the final wire schema, and the graph wire
format remain deferred in the [decision register](../decisions/deferred.md).

Every graph response carries its hash dictionary. A window extracted from a
`GraphPublication` carries the lineage metadata required by the
[Head-bounded projection contract](#head-bounded-fixed-delta-projection--settled)
in addition to `GraphWindowResolution`; that extracted view is `Frozen`
internally, while its serialized response is eligible for that projected
lineage. A view constructed directly from a database starts at revision zero
and has no publication ID unless it is placed in a `GraphPublication`; its
chosen tracking policy determines whether it accepts updates. Database-backed
windows remain capped by `MAX_WINDOW_DEPTH` and have no public delta lineage.
Requests crossing the current head view's lower bound take the consistent DB
path; head depth itself never forces this
fallback.
