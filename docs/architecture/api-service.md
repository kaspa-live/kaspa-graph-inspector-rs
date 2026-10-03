# ApiService architecture

## Scope and ownership

This document owns the ApiService Supervisor-facing control surface, API
database-generation binding, current-publication slot, reset and shutdown
behavior, status aggregation, resource admission, bulkheads, saturation, and
operational measurements, including API-owned task completion.
[API graph publication](api-publication.md) owns publication construction and
reconstruction. [The API protocol](api-protocol.md) owns HTTP, SSE, windows,
cursors, public errors, and cache semantics.

## Service shape — settled

KGI v2 includes an in-process `ApiService` and a complete bounded-by-level
head-tracking `GraphView`, rather than directing each head request to expensive
PostgreSQL graph queries. The [system overview](overview.md#resource-isolation-and-scalability--settled)
owns deployment evolution and the single-writer constraint.

ApiService installs one reliable upward event path before starting any
permanent worker:

```rust
enum ApiServiceEvent {
    Failed(ApiServiceError),
}
```

An unexpected `PublicationRuntime` worker or encoding-scheduler return or panic
emits `Failed` exactly once. Expected completion after its owning shutdown
signal emits no event. Cache jobs, HTTP handlers, and SSE connections use their
request-local outcomes and never emit this event. Supervisor owns the Fatal
reaction and event-path closure policy.

## Reset and recovery-time availability — settled

The following database binding, reset, and shutdown contracts form
ApiService's recovery-time control boundary.

## API database generation binding — settled

ApiService owns one shared database state and gives a clone to each private
`PublicationRuntime`. The state uses latest-value asynchronous delivery;
`tokio::sync::watch` is the settled primitive:

```rust
enum ApiDbGenerationEvent {
    Retired(Arc<ValidatedApiDbClient>),
    Published(Arc<ValidatedApiDbClient>),
}

enum ApiDbSnapshot {
    Running {
        current: Option<Arc<ValidatedApiDbClient>>,
        public_reads_enabled: bool,
    },
    Stopped,
}

struct ApiDbState {
    tx: tokio::sync::watch::Sender<ApiDbSnapshot>,
}

impl ApiDbState {
    async fn current(&self) -> Option<Arc<ValidatedApiDbClient>>;
    fn public_read_client(&self) -> Option<Arc<ValidatedApiDbClient>>;
}
```

It initializes with `Running { current: None, public_reads_enabled: true }`.
StorageService owns generation retirement and autonomous replacement under the
[storage lifecycle](storage.md#storageservice-lifecycle--settled). Supervisor
maps StorageService's ordered API-generation variants to
`ApiDbGenerationEvent` and forwards them through the control method below.
ApiService never calls StorageService, requests reacquisition, or emits a
generation-loss event.

`Published(client)` sets `current = Some(client)` only for the valid published
handle. `Retired(lost)` clears `current` only when it still holds that exact
`Arc`; a late retirement for an older generation cannot clear a newer binding.
Repeating either event for the same `Arc` is idempotent. Each accepted event
publishes the resulting complete snapshot. The reliable Supervisor call
returns after this local state transition has completed.

`public_read_client()` is an immediate snapshot read. It returns a clone only
when the state is `Running`, public reads are enabled, a current client exists,
and that client's storage-owned `is_valid()` state is true. It otherwise
returns `None`, which rejects database-backed request admission under the
[API protocol](api-protocol.md#anchored-graph-windows--settled). A request that
obtains a client performs its complete database phase against that exact
generation without holding an ApiDbState lock, switching generations, or
retrying transparently. A complete projection detached before a concurrent
state change may still finish delivery under the storage-owned replacement
gate.

`current().await` is the construction-side accessor. It ignores the public-read
gate and waits without a local timeout until a complete snapshot contains a
current client whose storage-owned `is_valid()` state is true. It returns
`Some(client)` for that exact generation and returns `None` only after the
state becomes `Stopped`. Pending construction is therefore woken by a
published replacement or service shutdown without an exposed subscription or
separate wakeup API.

If a public or construction operation returns `GenerationLost`, the exact
client is already terminally invalid under the
[storage-owned validity contract](storage.md#storageservice-lifecycle--settled).
The public request fails under the public API mapping. Construction invokes
the publication-owned
[`restart_construction()`](api-publication.md#universal-api-reconstruction--settled),
whose next seed awaits a valid current client. Neither path clears ApiDbState;
the ordered forwarded `Retired` event performs exact-Arc housekeeping and
wakes watchers. StorageService has already started autonomous reacquisition.
For a construction seed attempt, `QueryFailed` and
`InconsistentProjection` both abandon that attempt and invoke reconstruction
without changing ApiDbState.

For a public operation, `QueryFailed` and `InconsistentProjection` leave
`ApiDbState` unchanged. Normal anchor-unavailability and invalid-request
outcomes likewise do not enter generation-loss handling. `GenerationLost`
also changes no snapshot directly: invalidity makes the exact client
immediately unusable, and the ordered generation event owns its removal.

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
Supervisor calls them directly on `Arc<ApiService>`; there is no separate
Supervisor-facing command handle or command mailbox. Successful
`update_api_db_generation` returns after the local idempotent binding
transition is complete. Successful `reset` means the new runtime is installed
and its predecessor, if any, completed shutdown. It does not wait for graph
construction, alignment, publication activation, or processing progress.
Successful `shutdown` means ApiService completed the shutdown barrier. An
unavailable component returns `ApiServiceError` under the
`Ownership(ManagedComponentUnavailable)` parent-to-child failure semantics
owned by the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled).

ApiService owns exactly one private runtime slot:

```rust
enum ApiRuntimeSlot {
    AwaitReset,
    Running(PublicationRuntime),
    Stopped,
}

struct ApiService {
    runtime: tokio::sync::Mutex<ApiRuntimeSlot>,
    api_db: Arc<ApiDbState>,
    current_publication:
        tokio::sync::watch::Sender<Option<Arc<GraphPublication>>>,
    cancellation: tokio_util::sync::CancellationToken,
    cache_jobs: tokio_util::task::TaskTracker,
    encoding: EncodingScheduler,
    // admission and request infrastructure
}
```

`PublicationRuntime` is a directly owned private `kgi-api-core` value, never a
public capability or model value. Its complete behavior belongs to the
[publication owner](api-publication.md#publication-runtime--settled). The
runtime lifecycle mutex serializes each complete `reset` and `shutdown` call;
the two operations cannot exchange or stop runtimes concurrently. API
database-generation updates do not take this mutex and remain independently
serialized by ApiDbState. Once ApiDbState becomes `Stopped`, later generation
updates are rejected.

`reset` gives the fresh session receiver supplied under the
[graph-update ingress contract](api-ingress.md) to a new runtime.
`RecoveryMode` is the
Resync/Rebuild value owned by the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled).

ApiService accepts exactly one `reset` call for each processing attempt. Under
the runtime lifecycle mutex it establishes the recovery-mode-specific
ApiDbState effect below, starts a new runtime with the fresh receiver and an
`Arc<ApiDbState>` clone, exchanges that runtime into the slot, then awaits the
old runtime's `shutdown()`, if any. Starting the new runtime before stopping
the old one lets the new receiver drain immediately. Successful return is a
runtime-replacement barrier, but is not a processing, publication-activation,
or database-replacement barrier.

During the bounded overlap, either still-running runtime may supply a coherent
publication to ApiService's current-publication slot. Installation authority
does not depend on which runtime occupies `ApiRuntimeSlot`; no runtime identity,
revocable installation capability, or current-runtime check is used. The most
recent completed installation is current. The publication owner defines
runtime installation and shutdown effects.

The current-publication slot is a latest-value asynchronous value initialized
to `None`. `tokio::sync::watch` is the settled primitive. Each runtime receives
a sender clone and can install a complete publication with the equivalent of:

```rust
fn install_publication(
    slot: &tokio::sync::watch::Sender<Option<Arc<GraphPublication>>>,
    publication: Arc<GraphPublication>,
) {
    slot.send_replace(Some(publication));
}

fn current_publication(&self) -> Option<Arc<GraphPublication>> {
    self.current_publication.borrow().clone()
}
```

Installation atomically replaces the complete `Arc`; it never mutates a
previous publication through the slot. A public operation reads and clones the
slot once, retains that exact publication for its complete execution, and does
not restart or rebind when another publication is installed. A concurrent
replacement therefore affects later operations only. Publication replacement
notification can observe the same watch value; no separate replacement signal
or `arc-swap` dependency is required.

A runtime never clears the slot. ApiService stores `None` only at initial
construction and when terminal shutdown releases the current publication.

`PublishPostSeal` and `Live` are graph-update-feed markers rather than
out-of-band controls. They have no publication-completion acknowledgement. A
later session's `reset` call is the only session-supersession mechanism visible
to ApiService.

Ordinary teardown may drop the session's last producer and close its receiver.
The publication owner defines the runtime's quiescent reaction; closure is not
a session-supersession control call.

An ordinary Resync `reset` preserves both fields of `ApiDbState`. A Rebuild
`reset` sets `public_reads_enabled = false` without clearing `current`; a
repeated Rebuild reset has the same idempotent effect. During Rebuild, public
database-backed request admission remains disabled in `PreSeal`, `Constructing`,
and `Aligning`.

Keeping `current` does not authorize a stale Rebuild read. StorageService
retires the old generation before database replacement, and the replacement
gate prevents that handle from successfully observing replaced contents. If a
construction attempt reaches the retired handle before Supervisor forwards its
`Retired` event, `GenerationLost` exposes its invalidity and invokes
reconstruction. A `Published` replacement becomes immediately available to construction while
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

## API task ownership and completion — settled

ApiService tracks dynamic cache work with `tokio_util::task::TaskTracker` and
owns one encoding scheduler. Fixed workers retain their own completion
barriers; Axum owns handler execution, and ApiService proves handler completion
through request guards rather than duplicate task handles.

### Cache and encoding jobs

On a source-keyed cache miss, ApiService captures the immutable history-entry
Arcs and target, registers one running single-flight job, and spawns one
tracked cache orchestration task. Concurrent requests from that source attach
to its shared result even when Head advances. The job holds only detached
immutable inputs and no image or history lock.

Cancelling one request detaches that waiter without cancelling the shared job.
On success, the job completes every waiter and installs its `CachedDelta` only
when its weak reference to the originating cache can still be upgraded. On
failure, it completes every waiter with the same request-local error. Either
outcome removes the running-job registration and permits a later retry.
The publication owner defines job preservation and release of the old cache
and client registry across publication replacement.

ApiService owns one scheduler with this semantic shape:

```rust
struct EncodingScheduler {
    cancellation: tokio_util::sync::CancellationToken,
    completion: EncodingSchedulerCompletion,
    // bounded reservations, queued jobs, and active encoding permits
}

impl EncodingScheduler {
    async fn shutdown(&self) -> Result<(), EncodingSchedulerError>;
}
```

It owns every queued and active graph JSON serialization and gzip operation.
Individual request and cache tasks never spawn untracked compression work.
The scheduler enforces the existing total and historical limits and priority.
A historical request reserves bounded queue capacity before database work,
then submits the detached projection through that reservation after releasing
all database resources.

Scheduler shutdown rejects new reservations and submissions, cancels queued
jobs, and completes their waiters with expected shutdown cancellation.
Already running blocking serialization or compression cannot be interrupted
safely; the bounded active jobs finish, their output is discarded, and the
scheduler then completes.

### HTTP requests and SSE connections

Each admitted API request owns a private guard containing its applicable lane
permit, service-cancellation observation, and completion notification:

```rust
struct ApiRequestGuard {
    // admission permit, cancellation, and completion
}
```

Axum executes the handler future. For a graph response, the response body
retains the guard through complete delivery, delivery failure, client
disconnect, delivery timeout, or shutdown cancellation. A request cancelled
before response commitment returns the applicable unavailable result when
possible; cancellation after commitment terminates the body or stream and
never attempts a second response.

Each accepted SSE connection owns an SSE admission guard, one bounded semantic
message receiver, its current publication-local identifier and registration,
and connection cancellation. It observes the current-publication watch. On
replacement it performs the protocol-owned registration transition and ordered
initial sequence. Normal disconnect or cancellation removes the current
registration before releasing the guard. Mandatory-buffer admission follows
the protocol-owned slow-client rule and never blocks publication mutation or
shutdown.

## ApiService shutdown — settled

`shutdown` is a reliable completed-barrier operation. It is terminal,
idempotent, valid from `AwaitReset`, `PreSeal`, `Constructing`, `Aligning`, and
`Active`, and is serialized with `reset` by the runtime lifecycle mutex.
ApiService performs the local barrier in order:

1. close public graph HTTP and status admission;
2. publish `ApiDbSnapshot::Stopped`, waking pending construction and making
   public database reads unavailable;
3. take the installed runtime, if any, and await its publication-owned
   `shutdown()` barrier, allowing its final `Stale` transition to propagate
   through the normal ordered SSE path to currently registered clients;
4. cancel every SSE connection, then cancel admitted HTTP operations and wait
   for all SSE and request guards to be released;
5. close `cache_jobs` to new registration and cancel every unfinished cache
   job;
6. signal encoding-scheduler shutdown and await both `cache_jobs.wait()` and
   `EncodingScheduler::shutdown()`;
7. replace the current-publication value with `None`, then release every API DB
   permit, transaction, connection, validated client, pool handle, publication,
   and cache reference; and
8. set `ApiRuntimeSlot::Stopped` and complete the `shutdown` call.

Shutdown waits for the final Stale state to enter each currently registered
client's bounded SSE buffer, not for its network delivery. A client whose
buffer cannot accept that mandatory notification is disconnected under the
normal slow-client rule before the remaining connections are cancelled.

Successful method completion proves that no API admission, task, graph-update
receiver, or API database resource remains. A repeated `shutdown` returns
success for the already completed state. Runtime-lifecycle serialization rejects
`reset` once shutdown begins. Publishing `ApiDbSnapshot::Stopped` is the
generation-update cutoff: an update linearized before that transition may
complete and is then cleared by the transition, while a later update is
rejected.
ApiService adds no component-local timeout or escalation to this barrier; the
Supervisor waiting policy belongs to the
[processing lifecycle](processing-lifecycle.md#teardown-and-delivery-semantics--settled).

Publication-state and graph effects of the awaited runtime barrier belong to
the [publication runtime contract](api-publication.md#publication-runtime--settled).
Public HTTP admission is already closed while the protocol-owned state message
uses the still-open ordered SSE path. `ReceiverClosed` caused by this barrier
is expected cancellation for the concurrently stopping processing session and
must not request API reconstruction, Resync, or Rebuild.

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
