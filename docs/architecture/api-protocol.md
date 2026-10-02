# API protocol architecture

## Scope and ownership

This document owns the public HTTP and SSE graph contract, including public
graph limits and values, cursors, windows, Head-level lookup, public errors,
ETags, response caching, and delivery behavior. ApiService
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
The remaining endpoint-specific response DTOs, error-body schema, graph
encoding, and HTTP compression policy remain deferred under the common DTO
rules below.

## Common transport DTO rules — settled

The [core crate structure](overview.md#core-crate-structure--settled) owns the
placement of public semantic values and runtime serialization. At that boundary,
`GraphView` and `GraphDelta` remain in their canonical graph-owned forms until
serialization; the serializer performs response-local hash substitution and
only those omissions authorized by this protocol. No earlier intermediate
transport graph replaces either canonical value.

Each endpoint has its own success response. V2 has no universal success wrapper
with generic `data`, `status`, or `metadata` fields. A response with multiple
semantic variants uses an explicit discriminator; a decoder never infers the
variant from the incidental presence or absence of optional fields. Absence is
distinct from zero, `false`, an empty collection, and every sentinel value.

Every public `u64`, including revisions, levels, scores, timestamps, and
publication IDs, preserves its complete value. The selected graph encoding
determines the exact native or lossless alternative representation. Ordered
semantic collections, including direct parents and blue and red merge sets,
preserve their order. Sets, maps, and their serialized collection forms remain
unordered unless an endpoint-specific contract states otherwise; clients must
not derive meaning from their traversal order.

Internal implementation state does not enter the public schema. This includes
`TrackingPolicy`, retained-level usage counters, `CompactId`, history byte
estimates and cumulative costs, cache heat, client wake state, and database or
RPC generation identifiers.

`representation_version` remains internal and is not a response or
`SystemStatus` field. It changes when graph payload field meaning,
hash-reference representation, or graph encoding changes in a way that
invalidates cached graph bytes or ETags. Public errors use their own common
schema and never reuse a graph success DTO.

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
wire value. Each HTTP graph response uses the response-local
[hash dictionary and graph value DTOs](#serialized-graph-values-and-hash-dictionary--settled)
defined below. Its local references are not persistent across responses or
instances; coordinates may diverge across independently allocated DBs.

The public block projection preserves its **actual direct-parent list** even
when some parents are outside the response or PP boundary and have no
drawable edge. A materialized Genesis is recognized from its empty actual
direct-parent list. The public projection and Web client do not need to expose
or consult the persisted `NodeMetadata.genesis_hash`, and no dedicated
Genesis-hash API endpoint is required.

## Serialized graph values and hash dictionary — settled

The serialization-only hash reference is a plain alias rather than a newtype:

```rust
type HashRef = u32;
```

Every serialized graph body that references hashes carries this response-local
dictionary:

```rust
hashes: Vec<BlockHash>
```

`HashRef` is the zero-based index of its hash in that vector. The reference is
valid only inside the response that carries the dictionary. The dictionary
covers every serialized block identity, direct parent, blue or red merge-set
member, edge endpoint, VSPC-membership target, and color-change target,
including references whose blocks are outside the nominal window.

Dictionary construction traverses graph values in the serializer's actual
order and interns each hash on first encounter. Conceptually it uses a
`Vec<BlockHash>` plus a `HashMap<BlockHash, u32>` so a repeated hash reuses its
already assigned reference. Dictionary order and numeric references need not
repeat across independent serializations of the same graph meaning. A cached
delta's settled serializer-only removal omission happens before interning those
omitted entries, so a hash used only by an omitted block or edge removal does
not enter the dictionary. The selected graph encoding determines whether each
dictionary hash uses raw bytes or canonical hexadecimal text.

The serialized graph subvalues are:

```rust
struct LevelDto {
    level: u64,
    size: u64,
    daa_score: Option<u64>,
}

struct GraphBlockDto {
    hash: HashRef,
    coordinate: BlockCoordinate,
    timestamp: u64,
    daa_score: u64,
    selected_parent_index: Option<u32>,
    direct_parents: Vec<HashRef>,
    blue_merge_set: Vec<HashRef>,
    red_merge_set: Vec<HashRef>,
    color: BlockColor,
    is_in_vspc: bool,
}

struct EdgeIdDto {
    parent: HashRef,
    child: HashRef,
}

struct GraphEdgeDto {
    id: EdgeIdDto,
    parent_coordinate: BlockCoordinate,
    child_coordinate: BlockCoordinate,
}
```

`selected_parent_index` indexes the same block's `direct_parents` vector; it
is unrelated to `HashRef` and is absent for Genesis. Direct parents and the
blue and red merge sets preserve their graph-owned semantic order. Canonical
and serialized graph values retain the full `u64` level, slot, level-size,
timestamp, and score domains.

Levels, blocks, edges, and all change collections are unordered on the wire.
Only their contents are significant; a client builds any indexes required for
display or mutation. The serializer converts directly from the captured
`GraphView` or composed `GraphDelta`; it does not replace either canonical
value earlier, compare repeated immutable values, or introduce a
content-conflict validation step. Internal graph and service fields excluded
by the common DTO rules never enter these values.

## Complete graph response DTOs — settled

Head snapshots and anchored windows reuse one complete graph body:

```rust
struct GraphDataDto {
    hashes: Vec<BlockHash>,
    levels: Vec<LevelDto>,
    blocks: Vec<GraphBlockDto>,
    edges: Vec<GraphEdgeDto>,
}
```

`hashes` is the response-local dictionary for every `HashRef` in `blocks` and
`edges`. The collections are unordered except for the graph-owned ordered
vectors inside each block. The complete projection and its membership follow
the [graph-owned extraction and edge rules](api-graph.md#frozen-subview-extraction--settled).
Consequently, `levels` can validly contain external endpoint levels below or
above the nominal block extent.

The Head snapshot response is:

```rust
struct HeadSnapshotResponseDto {
    publication_id: u64,
    revision: u64,
    state: GraphPublicationState,
    max_depth: u64,
    low_level: u64,
    high_level: u64,
    graph: GraphDataDto,
}
```

`max_depth` is the effective capped request depth. `low_level..=high_level` is
the actual nominal block extent and can be shorter than `max_depth` near the
pruning-point boundary. `(publication_id, revision)` is the canonical delta
cursor and `state` is the captured publication state. `TrackingPolicy` and the
weak ETag remain outside the body.

An anchored window uses an explicit source discriminator:

```rust
enum GraphWindowSourceDto {
    Head {
        publication_id: u64,
        revision: u64,
        state: GraphPublicationState,
        head_high_level: u64,
    },
    Database,
}

struct GraphWindowResponseDto {
    source: GraphWindowSourceDto,
    resolution: GraphWindowResolution,
    graph: GraphDataDto,
}
```

The `Head` variant carries the complete lineage needed to consume canonical
Head deltas. `head_high_level` describes the source Head rather than the fixed
window's effective end. The `Database` variant contains no publication ID,
revision, state, or synthetic revision-zero cursor and creates no public delta
lineage.

`GraphWindowResolution.effective_start_level..=effective_end_level` is the
nominal fixed block extent; the linked graph-owned projection can additionally
retain external endpoint levels. The response does not echo the requested
anchor because `resolved_level` is the retained fixed focus after level,
block-hash, or DAA resolution.

Each response comes from one immutable capture: one Head image for a Head
snapshot, one Head extraction for a Head-backed window, or one detached
consistent database projection for a database-backed window. Advancement or
replacement during serialization does not alter the captured response. Graph
data, resolution, and lineage are never mixed across captures.

## Public API values — settled

The following conceptual shapes are shared across public response and window
contracts. Container and collection types remain implementation choices.
Graph-owned values are defined by the
[graph model](api-graph.md); publication-owned values are defined by
[API graph publication](api-publication.md).

```rust
enum FreshViewReason {
    PublicationMismatch,
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

struct HeadLevelValue {
    level: u64,
    value: Level,
}

enum HeadLevelLookupOutcome {
    Complete {
        publication_id: u64,
        revision: u64,
        state: GraphPublicationState,
        levels: Arc<[HeadLevelValue]>,
    },
    LevelUnavailable {
        requested_level: u64,
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
publication's lineage metadata and can follow it through the
[canonical Head delta endpoint](#canonical-head-delta-responses--settled).
Database construction alone does not imply `Frozen`; that policy remains
reserved for subview extraction.

Without a coherent graph publication, graph endpoints return `503 Service
Unavailable`, while the separate status lane remains available.

## Publication wire observation — settled

The public SSE operation is:

```http
GET /api/v1/graph/wakeups?publication_id=P&from_revision_id=F
```

`publication_id` and `from_revision_id` are required and describe the
client's actual graph cursor when it opens the stream. The endpoint accepts no
`client_id`, target revision, window depth or extent, anchor, or request body.
Malformed, duplicate, missing, or additional parameters follow the
[common request rules](#common-http-conventions--settled).

The supplied cursor initializes transport scheduling only. It does not
constrain the initial messages to that publication or revision. If a coherent
current publication exists, ApiService accepts a well-formed stale cursor,
registers the connection in the current publication, and reports that
publication below. Without a coherent graph publication, return the ordinary
graph-unavailable `503 Service Unavailable` outcome instead of opening an
empty stream.

A successful operation returns `200 OK` with `Content-Type:
text/event-stream` and `Cache-Control: no-store`. It has no ETag, graph body,
response-local hash dictionary, database access, `GraphCache` participation,
or delta construction. Detailed event-field encoding remains part of the
deferred wire schema.

SSE uses three ordered logical messages:

```rust
struct ClientRegistration {
    client_id: OpaqueClientId,
}

struct PublicationWakeup {
    publication_id: u64,
    revision: u64,
}

struct PublicationState {
    publication_id: u64,
    revision: u64,
    state: GraphPublicationState,
}
```

Their SSE event names are respectively `client-registration`,
`publication-wakeup`, and `publication-state`. `PublicationWakeup` means that
the graph cursor has advanced far enough for the registered client to attempt
HTTP catch-up. `PublicationState` establishes or changes publication state;
its revision is the publication cursor at that state observation. In
particular, a `Stale` message carries the final stalled Head revision so a
lagging client can catch up to it. Neither message carries graph data, which
still comes exclusively from HTTP snapshot or delta responses.

Each new or re-established SSE connection receives a dedicated
`ClientRegistration` message containing only a fresh unguessable opaque
`client_id`, then the current `PublicationState`, then the current
`PublicationWakeup`. Ordered SSE delivery makes the identifier and state
current before the client observes the graph cursor. The identifier's concrete
token encoding remains part of the deferred wire schema. The initial wakeup is
always emitted; ApiService does not assume that the request cursor remains
current while the stream is established.

Publication replacement preserves the live SSE transport. After the
[publication lifecycle](api-publication.md#publication-state-and-revision--settled)
releases the old publication's identifier, the server registers the connection
in the replacement publication and emits, in order, a new
`ClientRegistration`, the replacement `PublicationState`, and the replacement
`PublicationWakeup`. The registration message still contains only the new
identifier. A state transition within one publication emits only
`PublicationState` and does not allocate another identifier or pretend that a
graph revision occurred. State delivery and the replacement sequence bypass
the ordinary graph-revision wake schedule.

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
initializes the boundary from the client-supplied cursor and marks the
connection's mandatory initial graph wakeup sent. Disconnect removes the
registration. Reconnection and publication replacement each create a new
opaque identifier. A supplied expired or wrong-publication identifier returns
`ClientRegistrationRequired`; the client first establishes a current SSE
registration. An omitted identifier creates no registry entry and never
returns that outcome.

SSE delivery is not exactly once and provides no event replay. KGI does not
use SSE `Last-Event-ID` as a graph revision, publication ID, or `client_id`.
Reconnection instead creates a new registration and delivers the current
state and graph cursor through the ordered initial sequence above.

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
projection, and its response-local hash dictionary through the
[`HeadSnapshotResponseDto`](#complete-graph-response-dtos--settled). Internal
`TrackingPolicy` is not a public value.

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

The endpoint accepts `If-None-Match` and emits a weak ETag. Its semantic
identity contains the publication ID, revision, effective capped depth and
actual level extent, `representation_version`, and publication state. The weak
validator permits independently serialized equivalent snapshots to order
unordered collections and assign response-local hash references differently.
An exact semantic match returns `304 Not Modified`; otherwise the endpoint
returns the complete captured snapshot. A state-only transition therefore
changes the ETag even when the graph revision is unchanged, because the
self-contained snapshot carries that state. Responses use
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
starting revision. `PublicationState` and the ordered publication-replacement
sequence are delivered independently of this graph-revision schedule.

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
emits `PublicationState` regardless of the graph wake schedule, intermediate
responses use `ContinueImmediately`, and reaching the final stalled Head uses
`ReachedHead` without arming another graph-revision wakeup. The final Stale
interval may be shorter than four revisions. At that final Head an HTTP-only
response carries `KGI-Delta-Retry-After-Ms: 1000`; the client polls this same
delta endpoint until publication replacement produces
`FreshViewRequired(PublicationMismatch)`, then obtains the replacement Head
snapshot. The SSE-coordinated response omits the retry header and does not
rearm graph-revision delivery; publication replacement remains observable
through the existing SSE transport.

The soft estimate and hard encoded-size limit take precedence over ordinary
four-revision spacing. An encoded-size fallback may therefore produce and
cache a shorter complete prefix. Its continuation depends only on the
remaining distance to the then-observed Head. CPU-oriented bridges may also be
shorter because their purpose is to join an existing cached lane.

The serialized cached Head-delta body omits block and edge removals. Under the
strict [public graph-limit bound](#public-graph-limits-and-identity--settled),
the publication removal frontier cannot reach any
block or child-owned edge retained by an eligible client window. The client
uses `GraphDelta.high_level` according to its view policy. A Head-following
client advances its lower bound and removes objects that leave it; the
[Web contract](web.md#fixed-views--settled-behavior-with-deferred-pacing) owns
the fixed-window reaction. VSPC membership and color changes remain ordinary
field mutations and are never discarded by this rule. Required level changes
remain present.

Every in-memory `GraphDelta` retains the complete canonical contract through
history selection, composition, target capture, and delivery to the serializer.
The serializer is the sole omission point: while producing the cacheable wire
body, it skips removal-valued entries in `block_changes` and `edge_changes` and
omits their unused hashes from the response-local dictionary. It does not
construct a filtered `GraphDelta`. `CachedDelta` retains only the resulting
encoded bytes. No canonical delta in history, composition, or ApiService's Head
view loses those removals.

## Fixed-window reuse of canonical Head deltas — settled

A public anchored window extracted from an addressable Head publication follows
that publication through the canonical
[`GET /api/v1/graph/deltas`](#canonical-head-delta-responses--settled)
operation. There is no dedicated Fixed-delta endpoint and no server-side
extent projection. The response remains the canonical Head delta selected,
constructed, cached, and encoded under that endpoint's ordinary contract.

The initial window supplies the fixed effective extent and source Head cursor.
The [Web owner](web.md#fixed-views--settled-behavior-with-deferred-pacing) owns
containment, extent filtering, missing-level detection, atomic application,
and its terminal freeze behavior. ApiService retains no registration for the
fixed extent and reconstructs no extent-specific intermediate prefix.

A retained edge in the filtered result can require an endpoint level that was
unchanged and therefore absent from the canonical delta. The public lookup is:

```http
GET /api/v1/graph/levels?publication_id=P&level=L1[&level=L2...]
```

`publication_id` and at least one `level` value are required. `level` is
an explicitly repeated collection parameter rather than a scalar, so repeated
occurrences are valid and form a set; repeated values have no additional
meaning. Every value must be a positive unsigned level. Other malformed,
unknown, or additional parameters follow the
[common request rules](#common-http-conventions--settled). The endpoint accepts
no graph revision, anchor, depth, or `client_id`. Its maximum request
cardinality is part of the deferred HTTP resource budgets.

ApiService captures one immutable image of the identified Head publication and
looks up every requested level in that image. `Synchronizing`, `Live`, and
terminal `Stale` publications are all eligible while addressable. Success
returns `HeadLevelLookupOutcome::Complete` with the captured publication ID,
revision, state, and one complete absolute `Level` value per distinct
requested level. A value can be newer than the canonical delta being enriched;
it is presentation context and does not change cursor continuity, containment,
or mutation selection.

The lookup is all-or-nothing. If any requested level is absent from the
captured Head image, return `LevelUnavailable` naming one unavailable level
and no partial values. If the publication is no longer addressable, return
`FreshViewRequired(PublicationMismatch)`. Head advancement or replacement
after the immutable capture does not invalidate a completed response.

The endpoint never reads PostgreSQL, enters `GraphCache`, or joins cache
single-flight work. It has no ETag and carries `Cache-Control: no-store`.
Because its values contain no hashes, it needs no response-local hash
dictionary.

## Common HTTP outcomes — settled

Endpoint-specific typed outcomes remain authoritative. The common HTTP status
describes their transport class without replacing the typed reason.

Successful complete and prefix graph responses, `UpToDate`,
`WaitForWakeup`, Head-level lookup, anchored windows, and status return `200
OK`. `UpToDate` and `WaitForWakeup` are successful protocol results rather
than empty `204` responses because their response still carries publication
and continuation metadata. A matching Head ETag returns `304 Not Modified`.

Strict request parsing and endpoint-specific invalid semantic values return
`400 Bad Request`. An unknown route returns `404 Not Found`. A known v1
resource requested with an unsupported method returns `405 Method Not Allowed`
with `Allow: GET`.

Normal graph-object lookup misses return `404 Not Found`. These are the three
`GraphWindowAnchorUnavailable` variants and
`HeadLevelLookupOutcome::LevelUnavailable`; their typed reason distinguishes
them from an unknown route.

Cursor and registration recovery returns `409 Conflict` with the applicable
typed outcome:

```text
DeltaResponseOutcome::ClientRegistrationRequired
DeltaResponseOutcome::FreshViewRequired(reason)
HeadLevelLookupOutcome::FreshViewRequired(reason)
```

All `FreshViewReason` variants use this one status. They require abandoning or
repairing the current transport or graph lineage, so v2 does not divide them
between `409 Conflict` and `410 Gone`.

Rejection before response commitment because the applicable bounded public
HTTP, status, historical-read, serialization, or SSE admission lane is full
returns:

```http
429 Too Many Requests
Retry-After: 1
Cache-Control: no-store
```

This means KGI remains operational but cannot admit that public work now. A
persistently slow client detected after SSE establishment is disconnected
under the SSE delivery contract; a second HTTP outcome cannot be sent after
stream commitment.

Temporary absence of a capability or usable service state returns:

```http
503 Service Unavailable
Retry-After: 1
Cache-Control: no-store
```

This includes no coherent graph publication, disabled Rebuild-time API reads,
no current validated API DB generation, replacement-gate denial or
cancellation, `ApiReadError::GenerationLost`, and temporary inability to
publish the complete required in-memory Head. The distinction is that `429`
reports full admission capacity while `503` reports an unavailable capability
or service state. `Retry-After` uses HTTP seconds and is separate from the
successful delta protocol's millisecond `KGI-Delta-Retry-After-Ms` header.

Unexpected request-local failures before response commitment return `500
Internal Server Error` with `Cache-Control: no-store`. This includes
`ApiReadError::QueryFailed`, `ApiReadError::InconsistentProjection`, and
serialization or compression failure. Such failures do not stale a graph
publication or request processing recovery. A failure after HTTP or SSE
commitment terminates that response or stream rather than attempting a second
HTTP outcome.

KGI never presents a truncated graph as success. A delta `Prefix` remains
successful because it is a complete patch through its reported
`to_revision_id`. When a complete response cannot be produced, return the
applicable typed `409`, temporary `503`, or unexpected `500` outcome.

Cache behavior is:

| Response | Cache policy |
|---|---|
| Head snapshot `200` or `304` | `Cache-Control: no-cache` |
| Canonical delta success | `Cache-Control: no-cache` |
| Anchored window | `Cache-Control: no-store` |
| Head-level lookup | `Cache-Control: no-store` |
| SSE | `Cache-Control: no-store` |
| Status | `Cache-Control: no-store` |
| Any `4xx` or `5xx` | `Cache-Control: no-store` |

The delta operation targets the current Head, so the same URL must revalidate
rather than serve an older response blindly. Anchored windows have no v2 ETag
and the same anchor may resolve differently after graph changes.

The eventual error DTO distinguishes the semantic categories
`invalid-request`, `not-found`, `conflict`, `busy`, `unavailable`, and
`internal`. Endpoint-specific typed reasons remain subordinate to that
category. Exact DTO fields and encoding remain part of the deferred wire and
error-body schema.

## Public delivery failures — settled

Fresh-view range outcomes affect only their request. Serialization,
compression, response-size enforcement, client cancellation, and delivery
failure likewise remain request-local under the
[ApiService resource contract](api-service.md#resource-isolation-and-saturation--settled).
These request-local cursor and response failures never request processing
Resync or Rebuild.

SSE carries ordered registration, publication-state, and graph-wakeup messages;
it is not the graph data channel. Each client has a bounded delivery buffer.
Live history advancement follows the registered four-revision graph-wakeup
schedule, while `PublicationState` and the replacement sequence bypass it.
Synchronizing continues to emit ordinary coalesced graph wakeups. Pending
graph wakeups may coalesce to the latest cursor; registration and state
messages retain the ordering required by their owning contract. A persistently
slow client is disconnected and recovers by reconnecting SSE and using HTTP
delta or view requests.

`representation_version` is the settled term for the graph payload schema.
KGI v2 selects exactly one graph wire format after the deferred format
evaluation; clients do not negotiate among graph encodings. Head snapshot
conditional caching follows the
[endpoint contract](#head-snapshot-endpoint--settled). A delta-to-current query
must revalidate.
Historical database windows, Head-level lookup, and SSE have no ETag. Head-level
lookup and SSE require `Cache-Control: no-store` under their owning contracts.
HTTP compression is separate from graph-format selection and
remains part of the deferred encoding work.

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
be dropped at any time. Historical database-backed windows, Head-level lookup,
and SSE remain uncached in v2; the lookup also bypasses cache single-flight.
Cache work cannot affect graph correctness or processing.
Concrete cache collections and the estimator weights remain deferred in the
[decision register](../decisions/deferred.md).

## Anchored graph windows — settled

The canonical anchored-window operation is one resource with three mutually
exclusive request forms:

```http
GET /api/v1/graph/window?max_depth=N&level=L
GET /api/v1/graph/window?max_depth=N&block_hash=H
GET /api/v1/graph/window?max_depth=N&daa_score=Q
```

`max_depth` and exactly one of `level`, `block_hash`, or `daa_score` are
required. Zero or multiple anchor parameters, `level = 0`, an undecodable
block hash, and a DAA score outside `0..=MAX_DAA_SCORE` return `400 Bad
Request` before storage access. A positive `max_depth` is capped to
`MAX_WINDOW_DEPTH`; zero is invalid. Other malformed, duplicate, or additional
parameters follow the [common request rules](#common-http-conventions--settled).
The endpoint accepts no publication cursor, revision, `client_id`, or source
selection parameter.

After ordinary graph-endpoint admission establishes a coherent current Head
publication, ApiService first attempts to resolve the anchor and extract the
complete effective window from one immutable image of it. If either the anchor
or any required part of the effective extent is unavailable there, it uses one
consistent database transaction for both resolution and projection. It never
combines the two sources. Crossing the retained Head view's lower bound
requires the database path; the Head view's configured depth alone does not.

Every successful response uses the
[`GraphWindowResponseDto`](#complete-graph-response-dtos--settled). Its
Head-source variant carries the lineage used for
[canonical Head-delta reuse](#fixed-window-reuse-of-canonical-head-deltas--settled).
The database-source variant creates no public delta lineage; its request-local
revision-zero `Fixed` view is serialized once and discarded.

Neither source enters the publication-owned `GraphCache`. Historical
database-backed responses retain their settled no-ETag behavior. Every
anchored-window response follows the
[common `no-store` cache policy](#common-http-outcomes--settled).

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
with `Retry-After: 1`.

Public database-backed `ApiReadError` outcomes are exhaustive:

| Error | Public response | Binding path |
|---|---|---|
| `QueryFailed` | `500 Internal Server Error` | Non-generation-loss |
| `GenerationLost` | `503 Service Unavailable` with `Retry-After: 1` | Generation-loss |
| `InconsistentProjection` | `500 Internal Server Error` | Non-generation-loss |

No error returns a partial graph or transparently retries the failed request.
The [ApiService binding contract](api-service.md#api-database-generation-binding--settled)
owns the exact `ApiDbState` transition for each named binding path. Public read
errors change no publication ID or state, view or history revision, SSE cursor,
or processing recovery obligation; they neither stale nor reconstruct the
installed Head.

These input errors are distinct from a valid typed anchor that has no retained
match.

## Status endpoint — settled

The public service-status operation is:

```http
GET /api/v1/status
```

It accepts no query parameters, publication cursor, graph selection,
`client_id`, or request body. Malformed or additional input follows the
[common request rules](#common-http-conventions--settled). V2 exposes no
separate `/api/v1/info` resource because the existing `SystemStatus` contains
both the running KGI version and the component observations.

A successful request returns `200 OK` with the complete current
[`SystemStatus`](#public-api-values--settled) and `Cache-Control: no-store`.
The endpoint has no ETag or conditional-request behavior. Its detailed
transport DTO remains part of the deferred wire schema.

Status success is independent of graph publication, node connection,
processing-session, API database-generation, and historical-read availability.
Connecting, unavailable, recovering, rejected, fatal, and stopped component
conditions are values in the successful response rather than endpoint
failures. Absence of a validated node observation likewise returns success
with `node.last_validated = None`.

ApiService supplies the value under its
[status-observation contract](api-service.md#status-observation--settled); the
protocol adds no cross-component sampling barrier. Every successful response
contains the complete `SystemStatus` shape rather than a partial component set.

Status admission, isolation, and shutdown follow the
[ApiService resource contract](api-service.md#resource-isolation-and-saturation--settled).
Their public consequence is that graph endpoint saturation or unavailability
does not make an otherwise admitted status request unavailable. The common
public status-lane saturation outcome remains part of the shared HTTP outcome
work.

## Public API surface — settled

The public API has conceptually:

- head snapshot, depth-independent delta, and the SSE registration, state, and
  graph-wakeup stream;
- one capped window operation with exactly one anchor: level, block hash, or
  DAA score; the anchor resolves once to a fixed level;
- canonical Head-delta reuse for a fixed window extracted from a Head
  publication, plus all-or-nothing retained Head-level lookup; and
- status returning `SystemStatus`, including the KGI version, the exact
  component observations, and the current or last successfully validated
  network and node server information.

The remaining endpoint response and error-body schemas, graph wire format, and
HTTP compression policy remain deferred in the
[decision register](../decisions/deferred.md). HTTP methods and common route
and request syntax follow the settled conventions above.

Every graph response that references hashes carries its hash dictionary; the
Head-level lookup references none. A window extracted from a
`GraphPublication` carries the lineage metadata required to reuse
[canonical Head deltas](#fixed-window-reuse-of-canonical-head-deltas--settled)
in addition to `GraphWindowResolution`; that extracted view is `Frozen`
internally, while its serialized response remains eligible to follow the Head
lineage. A view constructed directly from a database starts at revision zero
and has no publication ID unless it is placed in a `GraphPublication`; its
chosen tracking policy determines whether it accepts updates. Database-backed
windows remain capped by `MAX_WINDOW_DEPTH` and have no public delta lineage.
