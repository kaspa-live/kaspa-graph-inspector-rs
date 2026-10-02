# ApiService architecture

## Scope and ownership

This document owns the ApiService Supervisor-facing control surface, API
database-generation binding, reset and shutdown behavior, status aggregation,
resource admission, bulkheads, saturation, and operational measurements.
[API graph publication](api-publication.md) owns publication construction and
reconstruction. [The API protocol](api-protocol.md) owns HTTP, SSE, windows,
cursors, public errors, and cache semantics.

## Service shape — settled

KGI v2 includes an in-process `ApiService` and a complete bounded-by-level
head-tracking `GraphView`, rather than directing each head request to expensive
PostgreSQL graph queries. The [system overview](overview.md#resource-isolation-and-scalability--settled)
owns deployment evolution and the single-writer constraint.

## Reset and recovery-time availability — settled

The following database binding, reset, and shutdown contracts form
ApiService's recovery-time control boundary.

## API database generation binding — settled

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
transparently. A disabled gate or absent client rejects database-backed request
admission; the [API protocol](api-protocol.md#anchored-graph-windows--settled)
owns its HTTP response. A complete projection detached before a concurrent
state change may still finish delivery under the storage-owned replacement
gate.

Publication construction may use `current` independently of the public-read
gate and captures one exact client for each database seed attempt. When no
client is present, construction remains pending. `Published` wakes such pending
work. If a public or construction operation returns `GenerationLost`,
ApiService immediately clears the client only when it is still current. The
public request fails under the public API mapping; construction invokes the
publication-owned
[`restart_construction()`](api-publication.md#universal-api-reconstruction--settled) and
waits for or uses the latest binding. StorageService has already retired the
failed generation and started autonomous reacquisition, so the later forwarded
`Retired` event is an idempotent confirmation. For a construction seed attempt,
`QueryFailed` and `InconsistentProjection` both abandon that attempt and invoke
the publication-owned `restart_construction()` without clearing a still-current
client.

For a public operation, `QueryFailed` and `InconsistentProjection` leave
`ApiDbState` unchanged. Normal anchor-unavailability and invalid-request
outcomes likewise do not enter generation-loss handling. Only
`GenerationLost` performs the exact-current-client clearing above.

Generation events by themselves change no graph publication ID, state, view
revision, history, or SSE cursor and do not invoke processing Resync or Rebuild.
An Active graph publication therefore remains usable while database-backed
reads are temporarily unavailable.

## Reset control and recovery effects — settled

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
`Ownership(ManagedComponentUnavailable)` parent-to-child failure semantics
owned by the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled).

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
database-backed request admission remains disabled in `PreSeal`, `Constructing`,
and `Aligning`.

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
reports database-backed read unavailability to the public API. ApiService
neither coordinates replacement nor waits for the remaining delivery of
detached responses.

After Rebuild, public database-backed reads reopen only when alignment
completes and the replacement publication becomes Active; that activation sets
`public_reads_enabled = true`. If `current` is absent then, the graph
publication still activates and database-backed requests continue receiving an
unavailable admission outcome until StorageService publishes another
generation. Processing does not wait for that publication. An ordered Live
marker changes the sticky target or the Active publication state without
another database load or graph revision.

The storage owner defines the `TRUNCATE`/MVCC safety requirement and detailed
gate boundary. `reset` does not participate in that exclusion mechanism.

## ApiService shutdown — settled

`shutdown` is a reliable completed-barrier operation. It is terminal,
idempotent, valid from `AwaitReset`, `PreSeal`, `Constructing`, `Aligning`, and
`Active`, and supersedes `reset` processing, publication construction,
alignment, reconstruction, and Active update application. ApiService performs
the local barrier in order:

1. close public graph HTTP and status admission;
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
before shutdown began is subsequently cleared by the shutdown barrier.
ApiService adds no component-local timeout or escalation to this barrier; the
Supervisor waiting policy belongs to the
[processing lifecycle](processing-lifecycle.md#teardown-and-delivery-semantics--settled).

`shutdown` creates no publication, `Stale` transition, graph revision, or SSE
wakeup because external admission closes first. `ReceiverClosed` caused by
this barrier is expected cancellation for the concurrently stopping processing
session and must not request API reconstruction, Resync, or Rebuild.

## Status observation — settled

ApiService receives the Supervisor, NodeService, StorageService, and processing
observation sources under the
[shared status-delivery contract](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
and combines their latest values with the running executable's static version
to construct the protocol-owned
[`SystemStatus`](api-protocol.md#public-api-values--settled).

The composition root wires these sources without creating a dependency from
`kgi-api-core` to `kgi-node` or `kgi-processing`. Exact watch primitives remain
an implementation choice. Reads do not wait for a cross-component barrier, so
`SystemStatus` is eventually consistent and must never drive synchronization,
recovery, command admission, or resource selection.

`SystemStatus` lives in `kgi-api-model`; its component values live in
`kgi-model` and retain their focused component owners. ApiService exposes the
latest received `NodeServiceStatus` unchanged: `last_validated = None` makes
network and node-version information unavailable, while `Some(value)` keeps
that information present regardless of the accompanying node state.
NodeService owns the value's lifecycle and current-versus-last meaning.
ApiService does not call NodeService or persist this observation.

## Resource isolation and saturation — settled

ApiService uses mandatory bulkheads beneath the system-wide processing
priority:

- a capped read-only API database pool separate from processing database
  capacity;
- bounded HTTP concurrency, query duration, response bytes and serialization
  CPU;
- bounded SSE clients and per-client buffers;
- level-scoped delta history, publication-scoped structurally bounded response
  reuse, and bounded historical-read work;
- a separate memory-only status admission lane, so graph saturation
  cannot hide service state; and
- distinct budgets for head delivery and historical database reads.

The initial v2 limits are:

```text
MAX_HEAD_HTTP_REQUESTS = 64
MAX_HISTORICAL_HTTP_REQUESTS = 8
MAX_STATUS_HTTP_REQUESTS = 16

API_DB_POOL_SIZE = 8
MAX_PUBLIC_HISTORICAL_DB_READS = 6
API_DB_QUERY_TIMEOUT = 30 seconds

MAX_GRAPH_ENCODING_JOBS = 4
MAX_QUEUED_GRAPH_ENCODING_JOBS = 32
MAX_HISTORICAL_ENCODING_JOBS = 2
GRAPH_ENCODING_QUEUE_TIMEOUT = 30 seconds

GRAPH_HTTP_DELIVERY_TIMEOUT = 60 seconds

MAX_SSE_CLIENTS = 1024
SSE_CLIENT_BUFFER_CAPACITY = 8
```

The Head lane admits Head snapshots, canonical deltas, and Head-level lookups.
The historical lane admits database-backed anchored windows. The independent
status lane admits only the memory-only status operation. Admission is
nonblocking; the [API protocol](api-protocol.md#common-http-outcomes--settled)
owns the public saturation response.

Public historical reads may occupy at most six of the eight API pool
connections. The remaining pool capacity is available to publication
construction, generation validation, and replacement work. ApiService applies
the query timeout to the complete database phase. Timeout cancels that phase,
releases its transaction, permit, and connection, and follows the existing
request-local internal-failure path; it does not by itself retire the API
database generation.

The four active encoding jobs cover graph JSON serialization and gzip
compression. Historical work may occupy at most two, preserving capacity for
publication and Head work. A cache hit that already owns final gzip bytes uses
no encoding job. A request joining an existing single-flight job consumes no
additional queue entry or active job.

The encoding queue holds at most 32 reserved or ready jobs in addition to the
four active jobs. Publication construction and Head work rank ahead of
historical work. A database-backed request reserves queue capacity before
starting its database phase. Failure to reserve performs no database work and
is admission saturation. Once reserved, the detached projection waits for an
encoder without retaining any database resource. If it cannot begin within
`GRAPH_ENCODING_QUEUE_TIMEOUT`, remove the job and report temporary service
unavailability under the API protocol. Cancellation removes a queued job and
releases its reservation.

The delivery timeout begins only when a complete gzip graph response is ready.
A client that cannot receive it within the limit loses that response and its
HTTP admission and response memory are released. SSE streams are exempt from
this duration and use their bounded delivery behavior instead.

The SSE client buffer capacity counts complete semantic messages. Numeric
client and buffer limits do not weaken the protocol-owned coalescing, ordered
registration and state delivery, or slow-client disconnect rules.

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
    -> reserve bounded encoding-queue capacity
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

If the complete Head depth required by the [graph model](api-graph.md) exceeds
the graph-view memory allowance, first drop an optional older coherent image
when useful. Otherwise mark Head temporarily unavailable and retry a complete
reload. Never publish partial levels. Encoded delta reuse has no independent
ApiService-owned resource rule beyond the structural bounds owned by the
[API protocol](api-protocol.md#publication-scoped-head-response-cache--settled).
Slow SSE clients follow the bounded coalescing and disconnect contract in the
[API protocol](api-protocol.md#public-delivery-failures--settled).

V2 exposes these operational measurements:

- request count, latency, and response bytes by endpoint;
- admission occupancy and rejection count for the Head, historical, and status
  lanes;
- active and rejected SSE clients, per-client buffer high-water marks, graph
  wakeup coalescing, and slow-client disconnects;
- encoded-response cache hits, misses, evictions, and coalesced identical
  requests;
- destination concentration, request-source counts, cached segment length,
  per-client revision wakeups, requests per catch-up, and size-limited
  short-prefix frequency;
- database pool and public-read permit occupancy, query duration, and query
  timeouts;
- encoding reservation, queue, and active-job occupancy, queue timeouts, and
  encoding duration;
- response delivery duration and delivery timeouts;
- delta-journal resets; and
- BlockProcessor and VspcProcessor commit latency.

The metrics export mechanism and labels remain deferred in the
[decision register](../decisions/deferred.md).

API traffic up to the settled rejection limits must not materially increase
either processor's commit latency.
