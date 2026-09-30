# API architecture

## Scope and ownership

This document owns ApiService control, API resource bulkheads, and the public
graph API contract. Graph publication belongs to
[API graph publication](api-publication.md). `GraphView`, `GraphDelta`, and `GraphHistory`
belong to the [API graph model](api-graph.md).
The per-session graph-update channel, producer gate, lifecycle-marker delivery,
and gap reporting belong to [API graph-update ingress](api-ingress.md).
The [processing lifecycle](processing-lifecycle.md) owns when Supervisor calls
`reset` and when processor phase commands occur. [Block processing](block-processing.md)
owns lifecycle-marker production. [Storage](storage.md) owns database
transactions, query implementation, and database-replacement exclusion. Web
client behavior belongs to the [Web architecture](web.md).

## In-process API — settled

KGI v2 includes an in-process `ApiService` and a complete bounded-by-level
head-tracking `GraphView`, rather than directing each head request to expensive
PostgreSQL graph queries. The [system overview](overview.md#resource-isolation-and-scalability--settled)
owns deployment evolution and the single-writer constraint.

Define `MAX_WINDOW_DEPTH <= MAX_CACHE_DEPTH`; its exact value remains deferred
in the [decision register](../decisions/deferred.md). Every windowed endpoint
caps requested depth to `MAX_WINDOW_DEPTH` and reports the effective range. An
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

The following conceptual shapes remain shared across public response and window
contracts during this staged extraction. Container and collection types remain
implementation choices. Graph-owned values are defined by the
[graph model](api-graph.md); publication-owned values are defined by
[API graph publication](api-publication.md).

```rust
enum FreshViewReason {
    PublicationMismatch,
    PublicationStale,
    StartPruned,
    StartUnavailable,
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
```

Together `(publication_id, revision)` form the public cursor. If the selected
wire format cannot represent every `u64` exactly, its encoding must preserve
the complete integer domain.

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
boundary, and target selection rules to obtain eligible gapless entries. It
does not apply the canonical Head response-byte budget before projection. It
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
failure likewise remain request-local under the resource contract below. These
request-local cursor and response failures never request processing Resync or
Rebuild.

SSE is only an ordered `PublicationWakeup`, not the graph data channel.
Reconnection is not exactly-once. Each client has a bounded wakeup buffer. On
connection the server emits the latest history cursor and publication state;
subsequent history advancement, state change, or replacement publication emits
an updated wakeup. Slow clients receive coalesced wakeups and, if persistently
behind, are disconnected and recover through HTTP delta or view requests.

`representation_version` is the settled term for the graph payload schema.
An ETag for a head view distinguishes publication ID, revision, effective
window, negotiated response format, `representation_version`, and publication
state; `Cache-Control: no-cache` allows cheap revalidation/304. A "to current"
Head-delta query must revalidate. Historical database windows, projected Fixed
deltas, and SSE have no ETag; projected Fixed deltas additionally require
`Cache-Control: no-store` under their owning contract.

ApiService maintains bounded server-side reuse of encoded head responses. It
caches immutable encoded bodies for a head view at one exact revision and
effective window, and for a delta over one exact
`(publication_id,from,to)` interval. Concurrent requests for the same absent
variant are single-flight. A view variant is identified by publication ID,
revision, publication state, effective window, negotiated response format,
`representation_version`, and content encoding. A canonical Head-delta variant
is identified by publication ID, its exact `from` and `to` revisions,
negotiated response format, `representation_version`, and content encoding;
publication state is not part of the immutable graph interval. A "to current"
request captures an exact target before it can join shared construction or use
an encoded entry; a bounded complete-prefix response is keyed by its actual
returned interval.
Projected Fixed delta requests bypass this cache and its single-flight path.

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

### API database generation binding — settled

ApiService owns its generation-update control value and local database state:

```rust
enum ApiDbGenerationEvent {
    Retired(Arc<ValidatedApiDbClient>),
    Published(Arc<ValidatedApiDbClient>),
}

struct ApiDbState {
    current: Option<Arc<ValidatedApiDbClient>>,
    public_reads_enabled: bool,
}
```

It initializes as `current = None` and `public_reads_enabled = true`.
StorageService owns generation retirement and autonomous replacement under the
[storage lifecycle](storage.md#storageservice-lifecycle--settled). Supervisor
maps StorageService's ordered API-generation variants to
`ApiDbGenerationEvent` and forwards them through the control method below.
ApiService never calls StorageService, requests reacquisition, or emits a
generation-loss event.

`Published(client)` sets `current = Some(client)`. `Retired(lost)` clears
`current` only when it still holds that exact `Arc`; a late retirement for an
older generation cannot clear a newer binding. Repeating either event for the
same `Arc` is idempotent. The reliable Supervisor call returns after this local
state transition has completed.

A public database-backed request is admitted only when
`public_reads_enabled` is true and `current` is `Some(client)`. It clones that
exact `Arc`, releases the ApiService state lock, and performs its complete
database phase without holding the lock, switching generations, or retrying
transparently. A disabled gate or absent client returns `503 Service
Unavailable` with a short `Retry-After`. A complete projection detached before
a concurrent state change may still finish delivery under the storage-owned
replacement gate.

Publication construction may use `current` independently of the public-read
gate and captures one exact client for each database seed attempt. When no
client is present, construction remains pending. `Published` wakes such pending
work. If a public or construction operation returns `GenerationLost`,
ApiService immediately clears the client only when it is still current. The
public request returns `503`; construction invokes `restart_construction()` and
waits for or uses the latest binding. StorageService has already retired the
failed generation and started autonomous reacquisition, so the later forwarded
`Retired` event is an idempotent confirmation. For a construction seed attempt,
`QueryFailed` and `InconsistentProjection` both abandon that attempt and invoke
ordinary `restart_construction()` without clearing a still-current client.

Generation events by themselves change no graph publication ID, state, view
revision, history, or SSE cursor and do not invoke processing Resync or Rebuild.
An Active graph publication therefore remains usable while database-backed
reads temporarily return `503`.

### Reset control and recovery effects — settled

`reset` is the sole out-of-band ApiService session-supersession operation
because it replaces the graph-update input topology.
Supervisor owns `Arc<ApiService>` and uses this public control surface:

```rust
impl ApiService {
    async fn update_api_db_generation(
        &self,
        event: ApiDbGenerationEvent,
    ) -> Result<(), ApiServiceError>;

    async fn reset(
        &self,
        graph_updates: GraphUpdateReceiver,
        recovery_mode: RecoveryMode,
    ) -> Result<(), ApiServiceError>;

    async fn shutdown(&self) -> Result<(), ApiServiceError>;
}
```

The methods are ApiService's complete Supervisor-facing control interface.
Their private mailbox or event-loop mechanics remain internal to
`kgi-api-core`. Successful `update_api_db_generation` returns after the local
idempotent binding transition is complete. Successful `reset` means the
operation was reliably accepted for ordered processing; it does not wait for
application. Successful `shutdown` means ApiService completed the shutdown
barrier. An unavailable component returns `ApiServiceError` under the
parent-to-child failure semantics owned by the
[processing lifecycle](processing-lifecycle.md#teardown-and-delivery-semantics--settled).

`reset` installs the fresh session receiver supplied under the
[graph-update ingress contract](api-ingress.md). `RecoveryMode` is the
Resync/Rebuild value owned by the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled).

ApiService accepts exactly one `reset` call for each processing attempt. It
globally preempts `PreSeal`, `Constructing`, `Aligning`, or `Active`:

1. abandon any unpublished candidate and reconstruction state;
2. mark any current non-Stale publication terminally Stale;
3. drop the old `GraphUpdateReceiver` and install the fresh one;
4. establish the recovery-mode-specific `ApiDbState` effects below; and
5. enter `PreSeal`.

The call is reliable but has no application-completion barrier. Supervisor may
continue session startup after it returns; `reset` is not a processing barrier
or a database-replacement barrier. A previous publication already Stale needs
no additional graph or history mutation.

`PublishPostSeal` and `Live` are graph-update-feed markers rather than
out-of-band controls. They have no publication-completion acknowledgement. A
later session's `reset` call is the only session-supersession mechanism visible
to ApiService.

Ordinary teardown may drop the session's last producer and close the installed
receiver. Channel closure is not an ApiService lifecycle signal and does not
replace, invalidate, or reconstruct a publication. ApiService retains its
current state until the next `reset` call or another defined state transition.

An ordinary Resync `reset` preserves both fields of `ApiDbState`. A Rebuild
`reset` sets `public_reads_enabled = false` without clearing `current`; a
repeated Rebuild reset has the same idempotent effect. During Rebuild, public
database-backed requests in `PreSeal`, `Constructing`, and `Aligning` fail
cleanly with `503 Service Unavailable` and a short `Retry-After`.

Keeping `current` does not authorize a stale Rebuild read. StorageService
retires the old generation before database replacement, and the replacement
gate prevents that handle from successfully observing replaced contents. If a
construction attempt reaches the retired handle before Supervisor forwards its
`Retired` event, `GenerationLost` clears it and invokes reconstruction. A
`Published` replacement becomes immediately available to construction while
the public-read gate remains disabled.

On ordinary initialized startup, StorageService's initial `Published` event
provides the first usable generation and the normal Resync reset preserves it.
A coherent network-bound Empty database may likewise supply that generation;
its valid anchored requests produce ordinary typed anchor-unavailability
outcomes. If the initial Resync then leads to a distinct Rebuild run under the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled),
that run's Rebuild reset disables public database reads as above.

ApiService never rebinds an in-flight public request or seed attempt to a newly
published API DB generation. Each operation finishes against its captured
client or reports its actual failure. If `reset` processing lags behind storage
replacement, exact-`Arc` retirement and publication updates still prevent a
late old-generation event from clearing the replacement.

StorageService, rather than `reset`, owns database-replacement exclusion. Its
[replacement gate](storage.md#api-read-exclusion-during-database-replacement--settled)
governs the API database phase. On the ApiService side, a request that already
detached its complete in-memory projection may finish delivering the old
coherent response; a request whose database phase the gate denies or cancels
returns `503`. ApiService neither coordinates replacement nor waits for the
remaining delivery of detached responses.

After Rebuild, public database-backed reads reopen only when alignment
completes and the replacement publication becomes Active; that activation sets
`public_reads_enabled = true`. If `current` is absent then, the graph
publication still activates and database-backed requests continue returning
`503` until StorageService publishes another generation. Processing does not
wait for that publication. An ordered Live marker changes the sticky target or
the Active publication state without another database load or graph revision.

The storage owner defines the `TRUNCATE`/MVCC safety requirement and detailed
gate boundary. `reset` does not participate in that exclusion mechanism.

### ApiService shutdown — settled

`shutdown` is a reliable completed-barrier operation. It is terminal,
idempotent, valid from `AwaitReset`, `PreSeal`, `Constructing`, `Aligning`, and
`Active`, and supersedes `reset` processing, publication construction,
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
7. enter terminal `Stopped` and complete the `shutdown` call.

Successful method completion proves that no API admission, task, graph-update
receiver, or API database resource remains. A repeated `shutdown` returns
success for the already completed state. Once shutdown begins, ApiService
accepts neither `reset` nor `update_api_db_generation`; an update completed
before shutdown began is subsequently cleared by the shutdown barrier. Exact
shutdown timeouts and forced escalation remain deferred under the shared
shutdown policy.

`shutdown` creates no publication, `Stale` transition, graph revision, or SSE
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
Found` response. It does not retire the API database generation, change
`ApiDbState`, reconstruct or stale a publication, or request processing
recovery. The [storage operation](storage.md#api-graph-projection-reads--settled)
owns the exact database condition producing each variant.

Public database-backed `ApiReadError` outcomes are exhaustive:

| Error | Public response | Local effect |
|---|---|---|
| `QueryFailed` | `500 Internal Server Error` | Keep the exact API DB client |
| `GenerationLost` | `503 Service Unavailable` with a short `Retry-After` | Clear the client only if it is still current |
| `InconsistentProjection` | `500 Internal Server Error` | Keep the exact API DB client |

No error returns a partial graph or transparently retries the failed request.
For `QueryFailed` and `InconsistentProjection`, the public failure changes no
`ApiDbState` field, publication ID or state, view or history revision, SSE
cursor, or processing recovery obligation. It neither stales nor reconstructs
an Active Head. `GenerationLost` has the client-binding effect defined above
but likewise leaves graph publication and processing recovery unchanged.

Reject invalid anchor input before storage access with `400 Bad Request`. This
includes level zero, a DAA score outside the shared range, an invalid
`max_depth`, and a block hash that cannot be decoded into `BlockHash`. These
input errors are distinct from a valid typed anchor that has no retained
match.

## Status observation — settled

ApiService receives one observation source from each component under the
[shared status-delivery contract](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
and constructs:

```rust
struct SystemStatus {
    supervisor: SupervisorStatus,
    node: NodeServiceStatus,
    storage: StorageServiceStatus,
    processing: ProcessingStatus,
}
```

The composition root wires these sources without creating a dependency from
`kgi-api-core` to `kgi-node` or `kgi-processing`. Exact watch primitives remain
an implementation choice. Reads do not wait for a cross-component barrier, so
`SystemStatus` is eventually consistent and must never drive synchronization,
recovery, command admission, or resource selection.

`SystemStatus` is an API projection value in `kgi-api-model`; its component
values live in `kgi-model` and retain their focused component owners. The node
observation exposes the sticky `last_validated` value owned by NodeService.
When NodeService is `Ready`, that value describes the current connection;
otherwise the response presents it as last successfully validated rather than
currently usable. ApiService does not call NodeService or persist this
observation.

The public graph API has conceptually:

- head snapshot, depth-independent delta, and SSE cursor wakeup;
- one capped window operation with exactly one anchor: level, block hash, or
  DAA score; the anchor resolves once to a fixed level;
- on-demand Head-bounded delta projection for a window extracted from the
  active Head publication; and
- status/info covering network, processing/API versions, component state, and
  the current or last successfully validated node server version.

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
graph update. Ordinary graph-update delivery and loss reporting follow the
[graph-update ingress contract](api-ingress.md).

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
construction cannot issue follow-up database reads. `reset` need not wait for or
cancel the remaining serialization and delivery work.

If the complete cache depth required by the [graph model](api-graph.md) exceeds
the cache memory allowance, first drop an optional stale image when useful.
Otherwise mark head temporarily unavailable and retry a complete reload. Never
publish partial levels. Slow SSE clients follow the bounded coalescing and
disconnect contract above.

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
