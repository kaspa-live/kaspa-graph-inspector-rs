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

## Common HTTP conventions — settled

The public HTTP contract is rooted at `/api/v1`. This `v1` identifies the
first public HTTP contract; it is independent of the KGI executable version,
the KGI v2 product generation, `representation_version`, publication IDs, and
graph revisions. It does not create a separate API-version field in
`SystemStatus`.

Canonical resource paths use lowercase nouns. Multiword path segments use
lowercase kebab-case. Graph resources are below `/api/v1/graph/`, while the
service-status resource is below `/api/v1/status`. Canonical paths have no
trailing slash. RPC-style action names such as `getHead` are not used.

The current public API is read-only. Snapshots, deltas, windows, SSE, and
status use `GET`; a GET request has no request body. `POST`, `PUT`, `PATCH`,
and `DELETE` are outside the current public API. `HEAD` is not part of the v1
contract unless accepted later.

Query parameters select cursors, revisions, depths, level extents, anchors,
and other request-specific projections. Cursors and block hashes do not become
path segments. Parameter names are case-sensitive lowercase snake_case and use
the architecture's semantic names, including `publication_id`,
`from_revision_id`, `to_revision_id`, `max_depth`, `block_hash`, `daa_score`,
and `client_id`; abbreviated aliases are not accepted.

Unsigned integer parameters use unsigned decimal text, block hashes use their
canonical hexadecimal representation, boolean parameters use exactly `true`
or `false`, and enum parameters use stable lowercase kebab-case values.
Request parsing is strict. A malformed value, negative unsigned value,
duplicate scalar parameter, unknown query parameter, mutually exclusive
combination, or missing required parameter returns `400 Bad Request`.
Endpoint-specific rules still determine whether a well-formed value is capped,
unavailable, or outside its semantic range.

This common contract fixes route and request syntax. Each endpoint section
owns its exact resource path, parameters, logical outcomes, and cache behavior.
The final response DTOs, error-body schema, graph encoding, and HTTP
compression policy remain deferred.

## Public graph limits and identity — settled

Define `MAX_WINDOW_DEPTH` and `MAX_CACHE_LEVEL_DISTANCE` under this strict
bound:

```text
MAX_CACHE_LEVEL_DISTANCE + MAX_WINDOW_DEPTH < MAX_CACHE_DEPTH
```

Their exact values remain deferred in the
[decision register](../decisions/deferred.md). Every windowed endpoint caps
requested depth to `MAX_WINDOW_DEPTH` and reports the effective range. An
oversized Head snapshot request cannot fall back to DB; it is capped. The
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
    ClientRegistrationRequired,
    WaitForWakeup {
        wake_at_revision_id: u64,
    },
    Complete(GraphDelta),
    Prefix(GraphDelta),
    FreshViewRequired(FreshViewReason),
}

enum DeltaContinuation {
    ReachedHead,
    ContinueImmediately,
    WaitForWakeup,
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

Each new or re-established SSE connection receives a dedicated
`ClientRegistration` message containing only a fresh unguessable opaque
`client_id`. The server emits that message before the connection's initial
`PublicationWakeup`; ordered SSE delivery makes the identifier current before
the client observes the graph cursor. The identifier's concrete token encoding
remains part of the deferred wire schema.

Publication replacement preserves the live SSE transport. After the
[publication lifecycle](api-publication.md#publication-state-and-revision--settled)
releases the old publication's identifier, the server registers the connection
in the replacement publication, emits a new `ClientRegistration`, and only then
emits the replacement `PublicationWakeup`. The registration message still
contains only the new identifier; publication identity, revision, and state
remain exclusive to the following wakeup. A state-only transition within one
publication does not allocate another identifier.

A delta-to-current HTTP request may carry the current identifier so ApiService
can update the matching publication-local wake schedule after choosing the
response. Supplying no identifier selects HTTP-only polling and is valid. A
supplied identifier is transport coordination only: it encodes no publication,
revision, network, or client identity. The request's `from_revision_id` is
always the authoritative graph cursor, including after retry or reconnect.

The publication-local scheduling value is conceptually:

```rust
struct DeltaClientWakeState {
    wake_at_revision_id: u64,
    wake_sent: bool,
}
```

`DeltaClientRegistry` is the publication-local mapping from each live opaque
identifier to exactly one such scheduling value; its concrete collection and
token representation remain implementation choices.

No current or highest graph revision is stored in this value. Registration
initializes the boundary from the client-supplied cursor and marks the initial
connection wakeup sent. Disconnect removes the registration. Reconnection and
publication replacement each create a new opaque identifier. A supplied
expired or wrong-publication identifier returns `ClientRegistrationRequired`;
the client first establishes a current SSE registration. An omitted identifier
creates no registry entry and never returns that outcome.

The API response envelope, rather than `GraphView` or `GraphDelta`, carries the
owning `publication_id`. A published view response pairs it with the view's
current revision and publication state; every successful delta response pairs
it with the history interval actually returned and the captured publication
state.

## Head snapshot endpoint — settled

The canonical Head snapshot operation is:

```http
GET /api/v1/graph/head?max_depth=N
```

`max_depth` is required. Values in `1..=MAX_WINDOW_DEPTH` are accepted as
requested; a larger well-formed value is capped to `MAX_WINDOW_DEPTH`; zero is
an invalid semantic value. Missing or malformed input follows the
[common request rules](#common-http-conventions--settled).

The endpoint extracts one coherent extent exclusively from the current
in-memory Head publication. It never reads PostgreSQL and never falls back to a
database-backed window. A successful logical response contains the owning
`publication_id`, captured revision, captured `GraphPublicationState`, the
effective capped depth and actual low/high extent, the complete graph
projection, and its response-local hash dictionary. Concrete DTO organization
remains part of the deferred wire schema; internal `TrackingPolicy` is not a
public value.

The captured `(publication_id, revision)` is the cursor for subsequent
depth-independent canonical Head deltas. Snapshot acquisition neither accepts
nor returns an SSE `client_id`; that identifier coordinates an established SSE
registration with delta-to-current requests.

`Synchronizing`, `Live`, and terminal `Stale` publications can each serve a
coherent snapshot with their exact state. Without a coherent publication, the
endpoint returns the graph-unavailable outcome. Head advancement or publication
replacement during serialization does not restart the request: the completed
response remains valid for its captured publication, revision, state, and
effective extent.

The endpoint accepts `If-None-Match`. Its ETag identity contains the
publication ID, revision, effective capped depth and actual level extent,
`representation_version`, and publication state. An exact match returns
`304 Not Modified`; otherwise the endpoint returns the complete captured
snapshot. A state-only transition therefore changes the ETag even when the
graph revision is unchanged, because the self-contained snapshot carries that
state. Responses use
`Cache-Control: no-cache`, requiring revalidation before reuse.

## Canonical Head delta responses — settled

Immutable delta payloads contain graph changes only and do not vary when the
publication state later changes.

The canonical Head delta operation is:

```http
GET /api/v1/graph/deltas?publication_id=P&from_revision_id=F[&client_id=C]
```

`publication_id` and `from_revision_id` are required. `client_id` is optional:
its presence selects SSE-coordinated delivery and its absence selects HTTP-only
polling. The public operation always targets the current Head captured for the
request and does not accept `to_revision_id`; exact-target range selection
remains internal to ApiService. Missing, malformed, duplicate, or additional
parameters follow the [common request rules](#common-http-conventions--settled).

ApiService rejects a publication mismatch before consulting history. A
delta-to-current request then validates a supplied publication-local
`client_id`.
A publication in `Synchronizing`, `Live`, or terminal `Stale` state is
otherwise eligible. Stale history no longer advances, but clients may catch up
through its final stalled Head. For an eligible request ApiService calls the
graph-owned
[`GraphHistory::range`](api-graph.md#retained-history-range)
operation, which owns retained-entry selection and target extension across an
indivisible aggregate and returns the selected gapless
`GraphHistoryEntryList`.

Public outcomes map as follows:

- publication mismatch returns
  `FreshViewRequired(PublicationMismatch)`;
- a request carrying an expired or wrong-publication `client_id` returns
  `ClientRegistrationRequired` before cache or history work;
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
level. A client beyond this gate reloads a complete applicable window. Define:

```text
HOT_DESTINATION_LEVEL_DISTANCE = MAX_CACHE_LEVEL_DISTANCE / 2
DELTA_CONVERGENCE_REVISION_INTERVAL = 4
```

Wake-boundary addition saturates at `u64::MAX`; revision arithmetic never
wraps.

The required initial v2 value is four. It is measured in graph revisions rather
than time. A later accepted tuning change may replace the value without
changing the convergence model. At the expected average of roughly twenty
graph revisions per second on a 10-BPS network, four revisions provide about
200 milliseconds for grouping clients without imposing a long live-view lag.
In the rules below, `I` denotes
`DELTA_CONVERGENCE_REVISION_INTERVAL`.

After the distance gate and before cache lookup, a Live delta-to-current
request with source `F` and Head `H` returns
`WaitForWakeup { wake_at_revision_id: F.saturating_add(I) }` when
`0 < H - F < I`.
It constructs and delivers no delta. For an SSE-coordinated request it also
replaces the client's wake boundary with that value and clears `wake_sent`;
an HTTP-only request has no registry state to update. This rule suppresses a
previously cached short entry in both modes. Synchronizing and Stale do not
defer at this gate.

For an eligible `GraphHistoryRange::Deltas`, ApiService uses the entries'
cumulative estimated costs to find the youngest complete boundary `Y` under
the soft estimated response budget. If no complete boundary fits that estimate,
the first complete stored entry remains the candidate for the mandatory hard
encoded-size check. A completed cache entry keyed by the
request's source revision is immutable and is served directly. On a cache miss,
delta-to-current target selection uses the approximate source-level distance:

1. At or inside `HOT_DESTINATION_LEVEL_DISTANCE`, use heat-oriented selection.
   When `Y >= F.saturating_add(I)`, choose the hottest retained complete
   boundary `T` in `[F.saturating_add(I), Y]`; if none has heat, choose `Y`.
   When `Y < F.saturating_add(I)`, choose `Y` as the necessary size-limited
   short target. Synchronizing chooses from the currently affordable range
   without imposing the minimum interval.
2. Beyond `HOT_DESTINATION_LEVEL_DISTANCE` but before the 80% fresh-view gate,
   minimize new CPU work. Choose the nearest younger boundary `R` that is the
   source of a completed `CachedDelta` and is reachable under the response
   budget, then construct the shortest bridge `F -> R`. A running job is not a
   completed bridge destination. If no such source exists, choose `Y`.

Heat for a revision is the number of canonical Head delta responses
successfully delivered with that actual `to_revision_id`, including every
waiter completed by a single-flight job. Destination heat is the primary
choice in the first region; ties prefer a destination with an outgoing
completed cache entry or running job and then the youngest revision. Requests
received from a revision are measured for observability, but that count never
contributes to heat. Do not separately count continuation states. A heat entry
becomes ineligible when its revision leaves retained history or its boundary
leaves the moving heat region.

When appending a canonical Head delta, ApiService supplies its cheap
`estimated_raw_bytes` from mutation counts and approximate field sizes for the
cacheable wire representation. The estimate discounts the block and edge
removals that the serializer will omit, is additive across entries, and
performs no graph encoding or compression. Exact weights remain deferred; the
estimate selects a candidate and never authorizes an oversized response.

The target is captured when the single-flight job starts and does not advance
with Head while that job is running. Compose the chosen entries through the
graph-owned [delta composition operation](api-graph.md#graph-delta-composition),
construct the response-local hash dictionary, encode once, and check the hard
response-byte limit. If the encoded result exceeds that limit, retry at an
earlier complete boundary, preferring another eligible heated destination in
the heat region. If even the first complete stored entry cannot fit, return
`FreshViewRequired(FirstStoredDeltaExceedsBudget)`. An aggregated stored entry
is indivisible and is never truncated or split.

Every returned delta is complete under the public Head-window contract and
reports its actual `to_revision_id`. It may be later than the selected target
because the graph range extended through an indivisible aggregate or earlier
because budgeting selected a prefix.
Composition operates on graph hashes through the graph-owned operation; public
encoding then constructs a new response-local hash dictionary for the result.
The emitted response is one composed patch and must remain within the
configured byte limit.

Return `Complete` when the actual returned right boundary reaches or extends
through the captured target. Return `Prefix` when target selection or
encoded-size fallback stops at an earlier complete boundary.
These are request-specific envelope outcomes rather than fields of the encoded
delta body. A later request may therefore wrap the same cached body differently
as Head advances.

Every successful delta-to-current response carries its captured
`GraphPublicationState` and exposes `DeltaContinuation` through exactly one of
these response headers:

```http
KGI-Delta-Continuation: head
KGI-Delta-Continuation: continue
KGI-Delta-Continuation: wakeup
```

The continuation is request-specific response metadata and is not part of the
immutable encoded delta body. For a Live publication, after a response ending
at cursor `T` against the then-observed Head `H`:

```text
T == H        => ReachedHead
H - T >= I    => ContinueImmediately
0 < H - T < I => WaitForWakeup
```

For a no-delta outcome, `T` is the unchanged request cursor. On an
SSE-coordinated request, `ReachedHead` and `WaitForWakeup` replace the client's
schedule with `T.saturating_add(I)` and clear `wake_sent`. Once Head reaches
that boundary, ApiService emits one SSE wakeup and marks it sent; further
history revisions emit no additional graph wakeup for that registration until
another delta response rearms it. `ContinueImmediately` replaces the boundary
in the same way but marks it sent: the HTTP response itself supplies the
immediate continuation and no duplicate SSE wakeup is needed for the
already-satisfied boundary. `UpToDate` uses the same rule with the request's
starting revision. A state-only transition and publication replacement always
emit their required wakeup independently of this graph-revision schedule.

An HTTP-only request has no wake schedule. `continue` instructs it to request
again immediately from `T`. A Live or Synchronizing response carrying `head`
or `wakeup` also carries a calculated polling delay:

```http
KGI-Delta-Retry-After-Ms: N
```

For that response, define `lag = H - T` and calculate in checked integer
arithmetic:

```text
N = 50 * max(1, DELTA_CONVERGENCE_REVISION_INTERVAL + 1 - lag)
```

The HTTP-only client waits at least `N` milliseconds before requesting again.
The retry header is absent from `continue` responses and from all
SSE-coordinated responses.

Synchronizing never parks a client for the four-revision interval: an
intermediate response uses `ContinueImmediately`, and ordinary graph wakeups
remain available. Stale cannot wait for future graph progress; entering Stale
wakes every armed client once, intermediate responses use
`ContinueImmediately`, and reaching the final stalled Head uses `ReachedHead`
without arming another graph-revision wakeup. The final Stale interval may be
shorter than four revisions. At that final Head an HTTP-only response carries
`KGI-Delta-Retry-After-Ms: 1000`; the client polls this same delta endpoint
until publication replacement produces `FreshViewRequired(PublicationMismatch)`,
then obtains the replacement Head snapshot. The SSE-coordinated response omits
the retry header and does not rearm graph-revision delivery; publication
replacement remains observable through the existing SSE transport.

The soft estimate and hard encoded-size limit take precedence over ordinary
four-revision spacing. An encoded-size fallback may therefore produce and
cache a shorter complete prefix. Its continuation depends only on the
remaining distance to the then-observed Head. CPU-oriented bridges may also be
shorter because their purpose is to join an existing cached lane.

The serialized cached Head-delta body omits block and edge removals. Under the
strict [public graph-limit bound](#public-graph-limits-and-identity--settled),
the publication removal frontier cannot reach any
block or child-owned edge retained by an eligible client window. The client
uses `GraphDelta.high_level` and its own window depth to advance `low_level`,
remove blocks below that boundary, and remove edges when their child leaves the
window. VSPC membership and color changes remain ordinary field mutations and
are never discarded by this rule. Required level changes remain present.

Every in-memory `GraphDelta` retains the complete canonical contract through
history selection, composition, target capture, and delivery to the serializer.
The serializer is the sole omission point: while producing the cacheable wire
body, it skips removal-valued entries in `block_changes` and `edge_changes` and
omits their unused hashes from the response-local dictionary. It does not
construct a filtered `GraphDelta`. `CachedDelta` retains only the resulting
encoded bytes. No canonical delta in history, composition, or ApiService's Head
view loses those removals.

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
Live history advancement follows the registered four-revision wake schedule;
state change and replacement wakeups bypass it. Synchronizing continues to
emit ordinary coalesced history wakeups, and entering Stale wakes armed clients
once. Slow clients receive coalesced wakeups and, if persistently behind, are
disconnected and recover through HTTP delta or view requests.

`representation_version` is the settled term for the graph payload schema.
KGI v2 selects exactly one graph wire format after the deferred format
evaluation; clients do not negotiate among graph encodings. Head snapshot
conditional caching follows the
[endpoint contract](#head-snapshot-endpoint--settled). A delta-to-current query
must revalidate.
Historical database windows, projected Fixed deltas, and SSE have no ETag;
projected Fixed deltas additionally require `Cache-Control: no-store` under
their owning contract. HTTP compression is separate from graph-format
selection and remains part of the deferred encoding work.

## Publication-scoped Head response cache — settled

For the publication-owned `GraphCache`, defined by the
[publication value](api-publication.md#publication-and-seed-values--settled),
internal identities omit `publication_id` because the containing publication
already supplies it. `GraphCache` contains the source-keyed completed entries,
source-keyed single-flight jobs, and destination-response heat counters. Heat
and request-source counters saturate rather than wrap. Concrete collections
remain deferred. The canonical cached delta value is:

```rust
struct CachedDelta {
    from_revision_id: u64,
    to_revision_id: u64,
    high_level: u64,
    encoded_body: Bytes,
}
```

`encoded_body` is the immutable encoded graph-delta body. It does not contain
the request-specific `Complete`/`Prefix` discriminator or
`DeltaContinuation`. ApiService creates a lightweight response around those
bytes and derives both values from the request target, then-current publication
state, and Head. Whether the lightweight HTTP wrapper is also retained is an
implementation detail.

The delta cache and its single-flight job map serve both SSE-coordinated and
HTTP-only delta-to-current requests and are keyed by source revision. At most
one logical job runs for a source revision. Concurrent requests attach to that
job even if Head advances after its target was captured. Success publishes one
`CachedDelta` at the source key and completes every waiter.
Failure returns the same request-local error to every waiter, removes the job,
and permits a later request to start another job. Each successfully delivered
waiter increments the actual destination's heat independently. A completed
entry is never promoted as Head advances; later requests from its exact source
reuse it until structural eviction.

The [publication lifecycle](api-publication.md#publication-state-and-revision--settled)
owns preservation, cancellation, and release of the cache and its jobs across
Stale and replacement. While the publication remains installed or is retained
as Stale, evict a `CachedDelta` when its
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
or processing recovery obligation; they neither stale nor reconstruct the
installed Head.

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

Remaining endpoint-specific resource paths and parameter sets, the final wire
schema, and the graph wire format remain deferred in the
[decision register](../decisions/deferred.md). HTTP methods and common route
and request syntax follow the settled conventions above.

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
