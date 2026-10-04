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
`from_revision_id`, `to_revision_id`, `target_depth`, `max_depth`,
`block_hash`, `daa_score`, and `client_id`; abbreviated aliases are not
accepted. `target_depth` belongs only to the canonical Head endpoint;
`max_depth` remains the bounded projection depth for anchored windows.

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
All successful graph response bodies use JSON. They carry
`Content-Type: application/json`. Clients do not negotiate among graph
encodings. Successful graph delivery uses the single
[gzip path](#graph-http-compression--settled) below.

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
publication IDs, is an unsigned decimal JSON string so browsers preserve its
complete value. Ordered semantic collections, including direct parents and
blue and red merge sets, preserve their order. Sets, maps, and their serialized
collection forms remain unordered unless an endpoint-specific contract states
otherwise; clients must not derive meaning from their traversal order.

JSON is the initial v2 graph format because it requires no browser codec,
schema generator, or second wire toolchain and lets implementation begin from
the settled DTOs. Benchmarking MessagePack and optionally CBOR against the
implemented JSON path is a later optimization rather than an implementation
prerequisite. Replacing JSON requires an accepted architecture change and a
new `representation_version`; changing an already released `/api/v1` graph
representation is a public compatibility change.

Internal implementation state does not enter the public schema. This includes
`TrackingPolicy`, retained-level usage counters, `CompactId`, history byte
estimates and cumulative costs, cache heat, client wake state, and database or
RPC generation identifiers.

`representation_version` remains internal and is not a response or
`SystemStatus` field. It changes when graph payload field meaning,
hash-reference representation, or graph encoding changes in a way that
invalidates cached graph bytes or ETags. Public errors use their own common
schema and never reuse a graph success DTO.

## Graph HTTP compression — settled

Every request to a graph endpoint must accept the `gzip` content coding under
standard HTTP `Accept-Encoding` semantics. Route, method, and request-syntax
validation retain their ordinary precedence. After those checks and before
graph lookup, cache access, history work, database access, or serialization, a
request that does not accept gzip returns the typed `406 Not Acceptable`
`gzip-required` error.

Every body-bearing successful graph response carries:

```http
Content-Type: application/json
Content-Encoding: gzip
Vary: Accept-Encoding
```

Bodyless delta outcomes and `304 Not Modified` still require a request that
accepts gzip, carry `Vary: Accept-Encoding`, and carry no `Content-Encoding`.
Every response from a graph endpoint carries `Vary: Accept-Encoding`. Error
bodies, including `gzip-required`, use the ordinary uncompressed common JSON
error representation. The gzip requirement does not apply to status, SSE,
`/kgi-config.json`, or static Web assets; their owning contracts remain
independent.

Define:

```text
SOFT_DELTA_ESTIMATED_BYTES = 4 MiB
MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES = 16 MiB
```

The soft limit guides only canonical-delta target selection. Crossing it moves
selection to an earlier complete history boundary; it never splits a stored
entry or authorizes an oversized result.

The hard limit measures the complete uncompressed JSON body of every Head
snapshot, canonical delta, anchored window, and Head-level lookup response.
Compression ratio never authorizes a larger logical response. Gzip runs only
after JSON serialization and that size check succeed. A compression failure
retains the common request-local failure policy and publishes no cache entry.

Compression does not alter graph semantics. Gzip quality does not change
`representation_version`, graph revision, cache identity, or the weak Head
ETag. The exact gzip quality is an implementation tuning choice. Successful
graph responses support no identity body and no second compressed variant.

## Public graph limits and identity — settled

Define:

```text
MAX_WINDOW_DEPTH = 250
MAX_CACHE_LEVEL_DISTANCE = 500
MAX_CACHE_LEVEL_DISTANCE + MAX_WINDOW_DEPTH < MAX_CACHE_DEPTH

CACHED_HEAD_WINDOW_DEPTH_STEP = 50
PREWARMED_HEAD_WINDOW_DEPTH = 50
CACHED_WINDOW_REFRESH_LEVEL_DISTANCE = 40
CACHED_WINDOW_MAX_LEVEL_DISTANCE = 100
```

With the graph-owned `MAX_CACHE_DEPTH = 1000`, the public limits leave 250
complete levels between the oldest eligible cached-delta source and the oldest
level of the largest client window. Anchored-window requests cap positive
depth to `MAX_WINDOW_DEPTH` and report the effective range. A Head
`target_depth` must instead be in `1..=MAX_WINDOW_DEPTH`; an oversized value is
invalid request input and cannot fall back to the database. The
[graph model](api-graph.md) owns `MAX_CACHE_DEPTH` and complete-level retention.

The Head cache derives its finite tiers rather than accepting arbitrary cache
keys:

```text
selected_depth =
    min(round_up(target_depth, CACHED_HEAD_WINDOW_DEPTH_STEP),
        MAX_WINDOW_DEPTH)
```

The initial values therefore produce depths `50`, `100`, `150`, `200`, and
`250`. `PREWARMED_HEAD_WINDOW_DEPTH` must be one derived tier. The refresh and
maximum distances measure the cached snapshot's `high_level` behind its
publication's current Head and satisfy:

```text
CACHED_WINDOW_REFRESH_LEVEL_DISTANCE
    < CACHED_WINDOW_MAX_LEVEL_DISTANCE
    < MAX_CACHE_LEVEL_DISTANCE
```

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
not enter the dictionary. Each dictionary hash uses canonical hexadecimal
JSON text.

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
`GraphView` or selected immutable `GraphHistoryEntry` values; it does not
replace those canonical values earlier, compare repeated immutable values, or
introduce a content-conflict validation step. Internal graph and service fields
excluded by the common DTO rules never enter these values.

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
    revision_timestamp_us: u64,
    low_level: u64,
    high_level: u64,
    graph: GraphDataDto,
}
```

`low_level..=high_level` is the actual nominal block extent and can be shorter
than the selected cache tier near the pruning-point boundary.
`(publication_id, revision)` is the canonical delta cursor.
`revision_timestamp_us` is that revision's publication-local monotonic
timestamp and initializes browser replay timing. The request's
`target_depth`, selected cache tier, publication state, `TrackingPolicy`, and
weak ETag remain outside the body.

An anchored window uses an explicit source discriminator:

```rust
enum GraphWindowSourceDto {
    Head {
        publication_id: u64,
        revision: u64,
        revision_timestamp_us: u64,
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

The `Head` variant carries the complete lineage and revision timestamp needed
to consume and replay canonical Head deltas. `head_high_level` describes the
source Head rather than the fixed window's effective end. The `Database`
variant contains no publication ID, revision, revision timestamp, or synthetic
revision-zero cursor and creates no public delta lineage. Neither variant
embeds publication state.

`GraphWindowResolution.effective_start_level..=effective_end_level` is the
nominal fixed block extent; the linked graph-owned projection can additionally
retain external endpoint levels. The response does not echo the requested
anchor because `resolved_level` is the retained fixed focus after level,
block-hash, or DAA resolution.

Each response body comes from one immutable capture: one cached Head snapshot,
one Head extraction for a Head-backed window, or one detached consistent
database projection for a database-backed window. Advancement or replacement
during serialization does not alter that body. Graph data, resolution, and
lineage are never mixed across captures. The publication-context headers
follow the independent rule below.

## Publication-context response headers — settled

Every DAG query reports the identity and current state of its associated
publication through this pair:

```http
KGI-Publication-Id: 42
KGI-Publication-State: synchronizing | live | stale
```

This applies to Head snapshots, canonical deltas, anchored windows, and
Head-level lookups, including bodyless successful outcomes and `304 Not
Modified`. A response produced after selecting a coherent publication retains
that exact `Arc<GraphPublication>`. `KGI-Publication-Id` is its immutable
unsigned decimal publication ID; ApiService samples the same publication's
latest state when finalizing `KGI-Publication-State`. Consequently, the state
can be newer than the immutable graph capture carried by the body. The two
headers always appear together. A request rejected before selecting a
publication, and the graph-unavailable response when no coherent publication
exists, carries neither.

`GraphPublicationState` never appears in a Head snapshot, canonical delta,
anchored-window, or Head-level body. Both headers are request-specific and are
not stored in compressed cache entries or used to invalidate graph bytes.
Publication identity participates in an endpoint's ETag only where that
endpoint's ETag contract says so; publication state never participates. A
state-only transition can therefore produce `304 Not Modified` with the same
`KGI-Publication-Id` and the new `KGI-Publication-State` value. Dedicated SSE
`publication-state` messages and the status response retain their separately
owned roles and are not cached DAG response bodies.

The header ID describes the publication context of the response. It does not
create graph lineage that the body does not carry. In particular, a
database-backed anchored window remains without a public delta lineage; its
`GraphWindowSourceDto::Database` discriminator remains authoritative.

## Canonical delta response DTOs — settled

A body-bearing canonical delta response carries an ordered batch of complete
retained history entries. Each entry contains only the target values required
by a client to apply that replay unit:

```rust
struct LevelMutationDto {
    level: u64,
    value: Option<Level>,
}

struct FieldMutationDto<T> {
    hash: HashRef,
    value: T,
}

struct GraphDeltaEntryDto {
    from_revision_id: u64,
    to_revision_id: u64,
    revision_timestamp_us: u64,
    high_level: u64,

    block_upserts: Vec<GraphBlockDto>,
    edge_upserts: Vec<GraphEdgeDto>,
    level_mutations: Vec<LevelMutationDto>,
    is_in_vspc_mutations: Vec<FieldMutationDto<bool>>,
    color_mutations: Vec<FieldMutationDto<BlockColor>>,
}

struct GraphDeltaBatchDto {
    hashes: Vec<BlockHash>,
    deltas: Vec<GraphDeltaEntryDto>,
}
```

`hashes` is one response-local dictionary shared by every hash reference in
every entry. The serializer walks entries in their required replay order and
their graph values in its actual serialization order, interning each hash on
first encounter with the common vector plus hash-to-reference index. As with
other graph bodies, unordered collection order and numeric references need not
repeat across independent serializations of the same meaning.

Each entry represents one selected `GraphHistoryEntry`. It is normally an
original one-step delta and may instead be an already composed, indivisible
history entry. Its `revision_timestamp_us` is the publication-local timestamp
of `to_revision_id`; a composed entry therefore carries its final
constituent's timestamp. Entries are ordered and gapless. A batch never splits
an entry or reconstructs internal boundaries discarded by earlier history
composition.

A level mutation with `Some(value)` installs that complete resulting level
value; `None` removes the level. Membership and color mutations install their
complete resulting field values. `block_upserts` and `edge_upserts` contain
the serialized `Some(value)` results from that entry's canonical change maps.
For every entry independently, the serializer omits removal-valued block and
edge changes and excludes hashes referenced only by those omitted values from
the shared dictionary. It omits each canonical change's composition-only
`before` value. The complete in-memory `GraphDelta` remains unchanged; the
serialization boundary is the sole omission point.

All mutation collections in an entry may be empty while its revision interval,
timestamp, and `high_level` still advance. The batch carries no auxiliary
endpoint-level context. A Fixed browser view obtains a complete level absent
from its own projection through the separately owned
[Head-level lookup](#fixed-window-reuse-of-canonical-head-deltas--settled).

Every successful canonical delta response carries the common publication-state
header plus these exact delta headers:

```http
KGI-Publication-Id: P
KGI-Head-Revision-Id: H
KGI-Head-Revision-Timestamp-Us: HT
KGI-Delta-Outcome: complete | prefix | up-to-date | wait-for-wakeup
KGI-Delta-Continuation: head | continue | wakeup
```

`H` and `HT` are one coherent capture of the selected publication's Head
revision and its publication-local timestamp. They are request-specific and
stay outside cached bytes, so they may be newer than the final cursor in a
reused batch. Both use unsigned decimal `u64` text. A client adopts them only
as a pair.

`wait-for-wakeup` additionally carries:

```http
KGI-Delta-Wake-At-Revision: R
```

That header is absent for every other outcome. The HTTP-only polling rules may
also add `KGI-Delta-Retry-After-Ms`. These request-specific headers do not
enter the cacheable body.

`complete` and `prefix` return one encoded `GraphDeltaBatchDto` containing at
least one entry. `up-to-date` and `wait-for-wakeup` return an empty body with
`200 OK`; their headers make them meaningful successful outcomes rather than
`204 No Content`. `ClientRegistrationRequired` and
`FreshViewRequired(reason)` remain typed `409 Conflict` error bodies and carry
none of the delta-success headers above. The common publication-context headers
remain governed independently by whether the request selected a coherent
publication.

For a body-bearing response:

```text
request.from_revision_id == body.deltas.first.from_revision_id
body.deltas[i].to_revision_id == body.deltas[i + 1].from_revision_id
body.deltas.last.from_revision_id < body.deltas.last.to_revision_id
```

Its resulting cursor is
`(KGI-Publication-Id, body.deltas.last.to_revision_id)`. An `up-to-date` or
`wait-for-wakeup` response leaves the request cursor unchanged.

## Head-level lookup response DTO — settled

The successful retained Head-level lookup response is:

```rust
struct HeadLevelLookupResponseDto {
    publication_id: u64,
    revision: u64,
    levels: Vec<LevelDto>,
}
```

`levels` is unordered and contains exactly one complete `LevelDto` for every
distinct requested level. All entries and the publication cursor come from one
immutable Head capture. The response carries no hash dictionary because no
value references a block hash. Publication state follows the common graph
response-header contract rather than entering the DTO.

Success returns this DTO with `200 OK`, `Cache-Control: no-store`, and no ETag.
An unavailable requested level remains the typed all-or-nothing `404` outcome;
a publication mismatch remains a typed `409` fresh-view outcome. Their bodies
belong to the common error schema rather than this success DTO.

## Status response DTO — settled

Status uses its dedicated JSON response:

```rust
struct SystemStatusDto {
    kgi_version: String,
    supervisor: SupervisorStatusDto,
    node: NodeServiceStatusDto,
    storage: StorageServiceStatusDto,
    processing: ProcessingStatusDto,
}

struct SupervisorStatusDto {
    lifecycle: SupervisorLifecycle,
    desired_recovery: Option<RecoveryMode>,
    active_recovery: Option<RecoveryMode>,
}

struct NodeServiceStatusDto {
    state: NodeServiceStatusState,
    last_validated: Option<ValidatedNodeStatusDto>,
}

struct ValidatedNodeStatusDto {
    network_id: NetworkIdDto,
    server_version: String,
    rpc_api_version: Option<u16>,
    rpc_api_revision: Option<u16>,
}

struct NetworkIdDto {
    network_type: NetworkType,
    suffix: Option<u32>,
}

struct StorageServiceStatusDto {
    state: StorageServiceStatusState,
}

struct ProcessingStatusDto {
    state: ProcessingStateName,
    mode: Option<RecoveryMode>,
}
```

Every field is present. Each `Option` uses explicit JSON `null`; no optional
field is omitted. `ProcessingStatusDto.mode` is `resync` or `rebuild` exactly
when `state` is `reconciling` and is `null` for every other processing state.
`node.last_validated` directly projects the component-owned optional sticky
observation; its current-versus-last meaning remains owned by
[NodeService](node-service.md#nodeservice--settled).

Enum values use these exact lowercase kebab-case strings:

```text
SupervisorLifecycle:
running | fatal | shutting-down | stopped

RecoveryMode:
resync | rebuild

NodeServiceStatusState:
connecting | ready | unavailable | rejected | stopped

StorageServiceStatusState:
connecting | awaiting-initialization | ready | unavailable | rejected | stopped

ProcessingStateName:
idle | reconciling | rebuilding-database | resyncing-dag |
catching-up | live | deactivating | stopped

NetworkType:
mainnet | testnet | devnet | simnet
```

These strings define only the public representation. The focused component
owners retain the meanings and lifecycle rules of their status values.
`NetworkIdDto` preserves the validated network type and optional numeric
suffix structurally instead of depending on a display string.

Success returns one complete `SystemStatusDto` with `200 OK`, `Content-Type:
application/json`, `Cache-Control: no-store`, and no ETag. It contains no
ApiService status, graph publication state or cursor, validated capability or
generation identity, component rejection or unavailability reason, recovery
fault, diagnostic, or `representation_version`.

## Error response DTO — settled

Every `4xx` or `5xx` response uses one direct JSON object:

```rust
struct ApiErrorDto {
    category: ApiErrorCategory,
    code: ApiErrorCode,
    message: String,
    details: Option<ApiErrorDetailsDto>,
}

enum ApiErrorDetailsDto {
    WindowAnchorUnavailable(WindowAnchorUnavailableDetailsDto),
    HeadLevelUnavailable {
        requested_level: u64,
    },
    FreshViewRequired {
        reason: FreshViewReason,
    },
    GraphResponseTooLarge {
        max_uncompressed_bytes: u64,
        actual_uncompressed_bytes: u64,
    },
}

enum WindowAnchorUnavailableDetailsDto {
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
```

Every field is present and `details` is explicit JSON `null` when the selected
code has no structured context. The code discriminates the details shape, so
the details object has no redundant outer type field. Window-anchor details
carry the exact `reason` string `level-not-retained`,
`block-not-materialized`, or `no-retained-daa-match`. `FreshViewRequired`
carries the exact reason string `publication-mismatch`, `start-pruned`,
`start-unavailable`, `delta-distance-exceeded`, or
`first-stored-delta-exceeds-budget`.

Categories and codes use these exact lowercase kebab-case values:

| HTTP | Category | Code | Details |
|---|---|---|---|
| `400` | `invalid-request` | `invalid-request` | `null` |
| `405` | `invalid-request` | `method-not-allowed` | `null` |
| `406` | `not-acceptable` | `gzip-required` | `null` |
| `404` unknown route | `not-found` | `route-not-found` | `null` |
| `404` window anchor | `not-found` | `window-anchor-unavailable` | matching anchor details |
| `404` Head level | `not-found` | `head-level-unavailable` | `requested_level` |
| `409` registration | `conflict` | `client-registration-required` | `null` |
| `409` lineage | `conflict` | `fresh-view-required` | `reason` |
| `422` | `unprocessable` | `graph-response-too-large` | `max_uncompressed_bytes`, `actual_uncompressed_bytes` |
| `429` | `busy` | `capacity-exhausted` | `null` |
| `503` | `unavailable` | `service-unavailable` | `null` |
| `503` encoding queue timeout | `unavailable` | `encoding-timeout` | `null` |
| `500` | `internal` | `internal-error` | `null` |

The direct error object has no universal `data` wrapper. `message` is a concise
safe English explanation for humans; clients use `category`, `code`, and typed
`details` for control and must not depend on message text. Public `u64` detail
values are unsigned decimal JSON strings, and block hashes use canonical
hexadecimal text.

Every error response carries `Content-Type: application/json` and
`Cache-Control: no-store`. The common outcome contract additionally requires
`Allow: GET` for `405` and `Retry-After: 1` for `429` and `503`. A graph
endpoint's error response is uncompressed and carries `Vary: Accept-Encoding`.

The body never exposes internal error variants, database or RPC generations,
diagnostics, stack traces, SQL errors, filesystem paths, or serialization
library messages. In particular, `ApiReadError` values map to the generic
public `service-unavailable` or `internal-error` codes selected by their
settled HTTP dispositions.

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
    Complete(GraphHistoryEntryList),
    Prefix(GraphHistoryEntryList),
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

Together `(publication_id, revision)` form the public cursor. Their wire values
follow the [common decimal-string `u64` mapping](#common-transport-dto-rules--settled).

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
publication state is exposed through graph-response headers and dedicated SSE
state messages.

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
or delta construction. SSE control payloads use the fixed compact JSON
representation below.

SSE uses three ordered message payloads:

```rust
struct ClientRegistrationDto {
    client_id: String,
}

struct PublicationWakeupDto {
    publication_id: u64,
    revision: u64,
}

struct PublicationStateDto {
    publication_id: u64,
    revision: u64,
    state: GraphPublicationState,
}
```

Their SSE event names are respectively `client-registration`,
`publication-wakeup`, and `publication-state`. The event name is the message
discriminator; its JSON object contains no redundant type field. Public `u64`
fields are unsigned decimal JSON strings so browsers preserve their complete
range. `GraphPublicationState` uses the exact lowercase strings
`synchronizing`, `live`, and `stale`. JSON property order has no meaning.

The complete frames have these shapes:

```text
event: client-registration
data: {"client_id":"K7m..."}

event: publication-wakeup
data: {"publication_id":"42","revision":"123456"}

event: publication-state
data: {"publication_id":"42","revision":"123456","state":"live"}
```

Each semantic message occupies one complete SSE event terminated by a blank
line. KGI emits no SSE `id:` field. `PublicationWakeupDto` means that the graph
cursor has advanced far enough for the registered client to attempt HTTP
catch-up. `PublicationStateDto` establishes or changes publication state; its
revision is the publication cursor at that state observation. In particular,
a `stale` message carries the final stalled Head revision so a lagging client
can catch up to it. Neither message carries graph data, which still comes
exclusively from HTTP snapshot or delta responses.

The server creates `client_id` from a uniformly random `u64`, serializes its
eight big-endian bytes as canonical unpadded base64url, and therefore emits
exactly eleven characters. Before registration it checks the current
publication registry and regenerates a colliding value. The token is transport
coordination rather than authentication and encodes no publication, revision,
network, or client identity. A delta request reuses this exact token. Invalid
length, alphabet, padding, or noncanonical encoding is malformed request input;
a well-formed token absent from the addressed publication produces the settled
`ClientRegistrationRequired` outcome.

Each new or re-established SSE connection receives a dedicated
`ClientRegistrationDto` message containing only a fresh opaque `client_id`,
then the current `PublicationStateDto`, then the current
`PublicationWakeupDto`. Ordered SSE delivery makes the identifier and state
current before the client observes the graph cursor. The initial wakeup is
always emitted; ApiService does not assume that the request cursor remains
current while the stream is established.

Publication replacement preserves the live SSE transport. After the
[publication lifecycle](api-publication.md#publication-state-and-revision--settled)
releases the old publication's identifier, the server registers the connection
in the replacement publication and emits, in order, a new
`ClientRegistrationDto`, the replacement `PublicationStateDto`, and the
replacement `PublicationWakeupDto`. The registration message still contains
only the new identifier. A state transition within one publication emits only
`PublicationStateDto` and does not allocate another identifier or pretend that
a graph revision occurred. State delivery and the replacement sequence
bypass the ordinary graph-revision wake schedule.

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
identifier to exactly one such scheduling value; its concrete collection
remains an implementation choice.

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
owning `publication_id`. A published graph body pairs it with the revision of
the immutable representation. Publication state remains independent under the
[common response-header contract](#publication-context-response-headers--settled).

## Head snapshot endpoint — settled

The canonical Head snapshot operation is:

```http
GET /api/v1/graph/head?target_depth=N
```

`target_depth` is required and records the browser's presentation intent and
the corresponding request metric. Values in `1..=MAX_WINDOW_DEPTH` are
accepted. Zero and a value above `MAX_WINDOW_DEPTH` are invalid semantic
values. Missing or malformed input follows the
[common request rules](#common-http-conventions--settled).

The endpoint selects the derived cache tier for `target_depth` and serves a
coherent cached snapshot exclusively from the current in-memory Head
publication. It never reads PostgreSQL and never falls back to a
database-backed window. A successful logical response contains the owning
`publication_id`, snapshot revision, actual low/high extent, complete graph
projection, and response-local hash dictionary through the
[`HeadSnapshotResponseDto`](#complete-graph-response-dtos--settled). Internal
`TrackingPolicy`, `target_depth`, and selected tier are not public body values.
The selected tier is at least `target_depth`; the Web trims a larger nominal
extent to its requested presentation depth. Caching therefore never
deliberately returns a shorter extent. The natural pruning-point boundary can
still make the available extent shorter.

The captured `(publication_id, revision)` is the cursor for subsequent
depth-independent canonical Head deltas. Snapshot acquisition neither accepts
nor returns an SSE `client_id`; that identifier coordinates an established SSE
registration with delta-to-current requests.

`Synchronizing`, `Live`, and terminal `Stale` publications can each serve a
coherent snapshot. Without a coherent publication, the endpoint returns the
graph-unavailable outcome. A cached snapshot can precede the publication's
current revision; its body cursor remains the exact starting point for
canonical delta catch-up. Head advancement or publication replacement during
delivery does not restart the request. The exact retained publication's ID and
latest state are added under the common publication-context header contract.

The endpoint accepts `If-None-Match` and emits a weak ETag. Its semantic
identity contains the publication ID, snapshot revision, actual level extent,
and `representation_version`. It excludes `target_depth`, selected tier, and
publication state. The weak validator permits independently serialized
equivalent snapshots to order unordered collections and assign response-local
hash references differently. An exact semantic match returns `304 Not
Modified` with the current publication-context headers; otherwise the endpoint
returns the complete cached snapshot. Responses use
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
boundary. Define:

```text
DELTA_RELOAD_LEVEL_DISTANCE = 400
HOT_DESTINATION_LEVEL_DISTANCE = 250
DELTA_CONVERGENCE_REVISION_INTERVAL = 15
MAX_DELTA_CONVERGENCE_REVISION_INTERVAL = 30
```

If the source boundary's distance behind the publication Head reaches or
exceeds `DELTA_RELOAD_LEVEL_DISTANCE`, return
`FreshViewRequired(DeltaDistanceExceeded)`. Do not reconstruct an exact source
level. A client beyond this gate reloads a complete applicable window. The
reload distance is 80% of `MAX_CACHE_LEVEL_DISTANCE`; the hot-destination
distance is half of it.

Wake-boundary addition saturates at `u64::MAX`; revision arithmetic never
wraps.

Both intervals are measured in graph revisions rather than time and are
initial tuning values. `DELTA_CONVERGENCE_REVISION_INTERVAL` is the preferred
response span and wake boundary. Heat may extend a selected destination up to
`MAX_DELTA_CONVERGENCE_REVISION_INTERVAL` so clients converge on a shared
younger cache point without creating an unbounded replay unit. Budget fallback,
a nearer captured Head or Stale terminal Head, and an existing nearer cache
boundary can produce a shorter interval. An indivisible composed history entry
can cross the maximum because the protocol never splits it. In the rules below,
`I` denotes the preferred interval and `M` the maximum interval.

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
`SOFT_DELTA_ESTIMATED_BYTES`. If no complete boundary fits that estimate,
the first complete stored entry remains the candidate for the mandatory hard
uncompressed-JSON-size check. From that affordable prefix, let `C` be the
youngest complete boundary no later than `F.saturating_add(M)`. If the first
stored entry itself crosses that boundary, its indivisible right boundary is
`C`. A completed cache entry keyed by the request's source revision is
immutable and is served directly; every entry built under this contract
already observes that maximum except for an indivisible aggregate. On a cache
miss, delta-to-current target selection uses the approximate source-level
distance:

1. At or inside `HOT_DESTINATION_LEVEL_DISTANCE`, use heat-oriented selection.
   When `C >= F.saturating_add(I)`, let `P` be the first complete boundary at
   or beyond `F.saturating_add(I)`. Choose the hottest retained complete
   boundary `T` in `[P, C]`; if none has heat, choose `P`. When `C` is earlier
   than the preferred interval, choose `C` as the necessary short target.
   Synchronizing chooses from the currently affordable bounded range without
   imposing the preferred minimum.
2. Beyond `HOT_DESTINATION_LEVEL_DISTANCE` but before
   `DELTA_RELOAD_LEVEL_DISTANCE`,
   minimize new CPU work. Choose the nearest younger boundary `R` that is the
   source of a completed `CachedDelta` and is reachable under the response
   budget through `C`, then construct the shortest bridge `F -> R`. A running
   job is not a completed bridge destination. If no such source exists, choose
   `C`.

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

Once the elected builder selects the cache destination, that destination is
fixed and does not advance with Head while the job is running. Preserve the
chosen `GraphHistoryEntryList` as ordered replay units, construct one
response-local hash dictionary across the complete list, serialize one
`GraphDeltaBatchDto`, and check its uncompressed byte length against the hard
response limit. If the
batch exceeds that limit, remove complete entries from the right and retry at
the resulting earlier boundary, preferring another eligible heated destination
in the heat region. Recompute the shared dictionary for the retained prefix. If
even the first complete stored entry cannot fit, return
`FreshViewRequired(FirstStoredDeltaExceedsBudget)`. An aggregated stored entry
is indivisible and is never truncated or split.

Every returned entry is complete under the public Head-window contract. The
batch's actual right boundary is its last `to_revision_id`; it may be later
than the selected target because the graph range extended through an
indivisible aggregate or earlier because the hard budget selected a prefix.
The graph-owned composition operation remains available for history compaction
and other graph uses, but cache construction does not compose the selected
entries merely to create the public response. Gzip compression and cache
insertion occur only after the complete batch passes the hard check.

For each delivery, return `Complete` when the actual returned right boundary
reaches or extends through that request's coherently captured Head revision.
Return `Prefix` when cache-destination selection or uncompressed-JSON-size
fallback stops at an earlier complete boundary. These are request-specific
envelope outcomes rather than fields of the encoded batch. A later request may
therefore wrap the same cached body differently as Head advances.

Every successful delta-to-current response uses the
[canonical delta response headers](#canonical-delta-response-dtos--settled).
The continuation is request-specific response metadata and is not part of the
immutable encoded delta-batch body. For a Live publication, after a response
ending at cursor `T` against the then-observed Head `H`:

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

Synchronizing never parks a client for the preferred revision interval: an
intermediate response uses `ContinueImmediately`, and ordinary graph wakeups
remain available. Stale cannot wait for future graph progress; entering Stale
emits `PublicationState` regardless of the graph wake schedule, intermediate
responses use `ContinueImmediately`, and reaching the final stalled Head uses
`ReachedHead` without arming another graph-revision wakeup. The final Stale
interval may be shorter than the preferred interval. At that final Head an
HTTP-only response carries `KGI-Delta-Retry-After-Ms: 1000`; the client polls
this same delta endpoint until publication replacement produces
`FreshViewRequired(PublicationMismatch)`, then obtains the replacement Head
snapshot. The SSE-coordinated response omits the retry header and does not
rearm graph-revision delivery; publication replacement remains observable
through the existing SSE transport.

The soft estimate and hard uncompressed-JSON-size limit take precedence over
ordinary preferred-interval spacing. A size fallback may therefore produce and
cache a shorter complete prefix. Its continuation depends only on the remaining
distance to the then-observed Head. CPU-oriented bridges may also be shorter
because their purpose is to join an existing cached lane.

The serialized cached Head-delta body omits block and edge removals. Under the
strict [public graph-limit bound](#public-graph-limits-and-identity--settled),
the publication removal frontier cannot reach any
block or child-owned edge retained by an eligible client window. The client
uses each serialized entry's `high_level` according to its view policy. A
Head-following client advances its lower bound and removes objects that leave it; the
[Web contract](web.md#fixed-views--settled) owns
the fixed-window reaction. VSPC membership and color changes remain ordinary
field mutations and are never discarded by this rule. Required level changes
remain present.

Every in-memory `GraphDelta` retains the complete canonical contract through
history selection, composition, target capture, and delivery to the serializer.
The serializer is the sole omission point: for each selected entry, while
producing the cacheable batch, it skips removal-valued entries in
`block_changes` and `edge_changes` and omits hashes used only by those removed
values from the response-local dictionary. It does not construct a filtered
`GraphDelta`. `CachedDelta` retains only the resulting gzip body and its
uncompressed JSON byte count. No canonical delta in history, composition, or
ApiService's Head view loses those removals.

## Fixed-window reuse of canonical Head deltas — settled

A public anchored window extracted from an addressable Head publication follows
that publication through the canonical
[`GET /api/v1/graph/deltas`](#canonical-head-delta-responses--settled)
operation. There is no dedicated Fixed-delta endpoint and no server-side
extent projection. The response remains the canonical Head delta batch
selected, constructed, cached, and encoded under that endpoint's ordinary
contract.

The initial window supplies the fixed effective extent and source Head cursor.
The [Web owner](web.md#fixed-views--settled) owns
per-entry containment, extent filtering, missing-level detection and lookup,
atomic application, and terminal freeze behavior. ApiService retains no
registration for the fixed extent and reconstructs no extent-specific
intermediate prefix.

A retained edge in a filtered replay entry can require an endpoint level that
was unchanged in Head and therefore absent from that canonical delta. Atomic
delivery does not supply unchanged source state. The public lookup is:

```http
GET /api/v1/graph/levels?publication_id=P&level=L1[&level=L2...]
```

Define:

```text
MAX_HEAD_LEVELS_PER_REQUEST = 64
```

`publication_id` and at least one `level` value are required. `level` is
an explicitly repeated collection parameter rather than a scalar, so repeated
occurrences are valid and form a set; repeated values have no additional
meaning. Every value must be a positive unsigned level. Other malformed,
unknown, or additional parameters follow the
[common request rules](#common-http-conventions--settled). The endpoint accepts
no graph revision, anchor, depth, or `client_id`. Deduplicate repeated values
before applying `MAX_HEAD_LEVELS_PER_REQUEST`. Exceeding it is invalid request
input.

ApiService captures one immutable image of the identified Head publication and
looks up every requested level in that image. `Synchronizing`, `Live`, and
terminal `Stale` publications are all eligible while addressable. Success
returns `HeadLevelLookupOutcome::Complete` with the captured publication ID,
revision, and one complete absolute `Level` value per distinct requested level,
serialized as the
[`HeadLevelLookupResponseDto`](#head-level-lookup-response-dto--settled). A
value can be newer than the canonical delta being enriched; it is presentation
context and does not change cursor continuity, containment, or mutation
selection.

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
OK`. The exact canonical delta bodies and success headers, including the empty
`UpToDate` and `WaitForWakeup` bodies, are defined by the
[delta response DTO contract](#canonical-delta-response-dtos--settled). A
matching Head ETag returns `304 Not Modified`.

Strict request parsing and endpoint-specific invalid semantic values return
`400 Bad Request`. An unknown route returns `404 Not Found`. A known v1
resource requested with an unsupported method returns `405 Method Not Allowed`
with `Allow: GET`.

After those request checks, a graph request that does not accept gzip returns
`406 Not Acceptable` with category `not-acceptable`, code `gzip-required`, and
`details: null`. This capability check precedes every graph, cache, history,
database, serialization, and compression operation.

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

A complete Head snapshot, anchored window, or Head-level lookup whose
uncompressed JSON exceeds `MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES` returns
`422 Unprocessable Content` with code `graph-response-too-large`, identifies
the hard byte limit and the observed uncompressed JSON size, and returns no
partial graph. The client may retry with a smaller depth or fewer levels.
Canonical deltas retain their earlier-boundary
fallback and `FreshViewRequired(FirstStoredDeltaExceedsBudget)` outcome when
the first indivisible stored entry cannot fit.

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
publish the complete required in-memory Head. It also includes expiry of an
already reserved graph-encoding queue wait, with code `encoding-timeout`. The
distinction is that `429`
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
applicable typed `409`, size-bound `422`, temporary `503`, or unexpected `500`
outcome.

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

The [common error DTO](#error-response-dto--settled) owns the exact category,
code, typed details, and JSON representation for each failure above.

## Public delivery failures — settled

Fresh-view range outcomes affect only their request. Serialization,
compression, response-size enforcement, client cancellation, and delivery
failure likewise remain request-local under the
[ApiService resource contract](api-service.md#resource-isolation-and-saturation--settled).
These request-local cursor and response failures never request processing
Resync or Rebuild.

SSE carries ordered registration, publication-state, and graph-wakeup messages;
it is not the graph data channel. Each client has a bounded delivery buffer.
Live history advancement follows the registered preferred-interval graph-wakeup
schedule, while `PublicationState` and the replacement sequence bypass it.
Synchronizing continues to emit ordinary coalesced graph wakeups. Pending
graph wakeups may coalesce to the latest cursor; registration and state
messages retain the ordering required by their owning contract. A persistently
slow client is disconnected and recovers by reconnecting SSE and using HTTP
delta or view requests.

`representation_version` is the settled term for the graph payload schema.
The single JSON graph format follows the common transport DTO rules above.
Head snapshot conditional caching follows the
[endpoint contract](#head-snapshot-endpoint--settled). A delta-to-current query
must revalidate.
Historical database windows, Head-level lookup, and SSE have no ETag. Head-level
lookup and SSE require `Cache-Control: no-store` under their owning contracts.
The [graph gzip contract](#graph-http-compression--settled) owns transfer
compression and leaves only exact gzip quality to implementation tuning.

## Publication-scoped Head response cache — settled

For the publication-owned `GraphCache`, defined by the
[publication value](api-publication.md#publication-and-seed-values--settled),
internal identities omit `publication_id` because the containing publication
already supplies it. `GraphCache` contains tier-keyed Head snapshot entries and
jobs, source-keyed delta entries and jobs, and destination-response heat
counters. Heat and request-source counters saturate rather than wrap. The
[ApiService task contract](api-service.md#api-task-ownership-and-completion--settled)
owns the cache's concrete slot collections, mutual exclusion, single-flight
completion transport, and task execution. The canonical cached delta value is:

```rust
struct CachedDelta {
    from_revision_id: u64,
    to_revision_id: u64,
    high_level: u64,
    uncompressed_json_bytes: u64,
    gzip_body: Bytes,
}
```

`gzip_body` is the final compressed JSON encoding of
[`GraphDeltaBatchDto`](#canonical-delta-response-dtos--settled).
`uncompressed_json_bytes` is the byte length checked against the hard response
limit before compression; `gzip_body.len()` is the transferred body size. The
cached `from_revision_id` and `to_revision_id` are the first and last entry
boundaries, and `high_level` is the last entry's target Head level. The entry
timestamps and count remain inside the immutable encoded body; they need no
second semantic representation in `CachedDelta`. The
cached value contains no publication ID, publication state, sampled Head
revision/timestamp pair, response-outcome discriminator, continuation, wake
boundary, or retry delay. ApiService reuses
those exact compressed bytes and adds request-specific HTTP headers. Whether
that lightweight HTTP wrapper is also retained is an implementation detail.

Every `CachedDelta` is individually bounded by
`MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES`. `GraphCache` has no independent total
encoded-byte cap; publication lifetime, retained source boundaries, and target
level distance provide its structural bound.

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
owns preservation and release of publication-scoped cache state across Stale
and replacement. The
[ApiService task contract](api-service.md#api-task-ownership-and-completion--settled)
owns job completion and terminal cancellation. While the publication remains
installed or is retained as Stale, evict a `CachedDelta` when its
source revision is no longer retained or its target `high_level` is more than
`MAX_CACHE_LEVEL_DISTANCE` behind that publication's Head. These structural
bounds replace an independent encoded-byte cache cap.

An encoded-cache miss or eviction affects performance only: reconstruct the
response from the current `GraphView` or retained `GraphHistory`. Historical
database-backed windows and Head-level lookup are serialized, size-checked,
compressed once, delivered, and discarded. They and SSE remain uncached in
v2; the lookup also bypasses cache single-flight. Cache work cannot affect
graph correctness or processing.

Canonical delta delivery observes the selected and delivered entry count and
revision span, cache hit/join/build result, uncompressed and gzip byte sizes,
and soft- or hard-budget fallback. These measurements tune the initial 15/30
revision intervals and response estimates; they never alter cursor validity or
graph contents. Exact metric names and export mechanics remain deferred.

### Tiered Head snapshot cache

`GraphCache` also contains one completed-entry slot and one single-flight job
slot for each derived Head snapshot depth. A completed entry has this semantic
shape:

```rust
struct CachedHeadSnapshot {
    depth: u64,
    revision: u64,
    revision_timestamp_us: u64,
    low_level: u64,
    high_level: u64,
    uncompressed_json_bytes: u64,
    etag: HeadEtag,
    gzip_body: Bytes,
}
```

The containing publication supplies the publication ID. The gzip body is the
final encoding of `HeadSnapshotResponseDto` and therefore contains that ID and
the snapshot cursor and revision timestamp, but contains no publication state,
request `target_depth`, or cache-tier field. Each entry is individually subject
to `MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES`. The fixed number of tiers and that
per-entry limit structurally bound this cache without another total byte cap.

For a valid request, derive its selected depth using the public tier formula.
Choose work and response reuse in this order:

1. If the selected tier has an eligible completed snapshot, serve it.
2. Otherwise, if one or more larger tiers have eligible completed snapshots,
   serve the smallest such tier immediately and ensure one selected-tier job
   is running.
3. Otherwise, if a selected-tier job is running, join it.
4. Otherwise, if one or more larger covering jobs are running, join the
   smallest such job and do not start a concurrent selected-tier job.
5. Otherwise start and join one selected-tier job.

Serving or joining a larger tier does not change the request's selected tier.
Case 2 creates the selected tier for later requests; case 4 avoids duplicating
expensive extraction, serialization, and compression during the same demand
burst. If no later request follows a larger running job, the missing selected
tier requires no speculative construction.

For an entry with captured `high_level = C` and current publication Head level
`H`, define `distance = H - C`; Head levels do not decrease within a
publication. An entry is handled as follows:

```text
distance < CACHED_WINDOW_REFRESH_LEVEL_DISTANCE
    eligible; serve without refresh

CACHED_WINDOW_REFRESH_LEVEL_DISTANCE
    <= distance < CACHED_WINDOW_MAX_LEVEL_DISTANCE
    eligible; when served as the request's selected tier, ensure one
    demand-driven refresh job for that tier

distance >= CACHED_WINDOW_MAX_LEVEL_DISTANCE
    ineligible; never serve it
```

The `50` tier is constructed preemptively only while opening a new
publication. No tier, including `50`, is refreshed merely because Head
advances. Every later construction or refresh is triggered by request demand.
A refresh retains the previous completed entry until the replacement succeeds
and atomically replaces only that tier. Failure preserves any previous
completed entry. Waiter completion, job removal, and retry availability belong
to the
[ApiService task contract](api-service.md#api-task-ownership-and-completion--settled).

`Synchronizing` and `Live` use the moving distance rules. A terminal `Stale`
publication stops advancing, so its eligible entries remain usable and its
missing tiers can still be built on demand while the publication remains
addressable. Publication replacement releases the complete tier cache with
the containing publication under the publication-lifetime contract.

The estimator weights remain deferred in the
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

A successful request projects the complete current
[`SystemStatus`](#public-api-values--settled) into the
[`SystemStatusDto`](#status-response-dto--settled). The endpoint's exact
success content type, cache policy, and excluded internal fields are owned by
that response contract; it has no conditional-request behavior.

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

The graph wire format and mandatory gzip delivery are the settled
representations defined above. Only exact gzip quality remains deferred in the
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
