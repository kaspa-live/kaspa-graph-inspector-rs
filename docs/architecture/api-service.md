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
enum ApiDbGenerationMessage {
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
    fn begin_rebuild(&self);
    fn set_public_reads_enabled(&self, enabled: bool);
}
```

It initializes with `Running { current: None, public_reads_enabled: true }`.
`set_public_reads_enabled` is a private synchronous mutation used by
ApiService's publication-installation transition while it holds the
publication-install boundary. It updates a `Running` snapshot without changing
`current` and cannot change `Stopped`.
`begin_rebuild` is a private synchronous mutation under that same boundary. It
atomically sets `current = None` and `public_reads_enabled = false` without
retiring or invalidating the previously bound storage-owned client. A later
exact-client `Retired` message is therefore harmless, and only a subsequent
`Published` message can bind the Rebuild runtime to an API generation.
StorageService owns generation retirement and autonomous replacement under the
[storage lifecycle](storage.md#storageservice-lifecycle--settled). Supervisor
delivers `ApiDbGenerationMessage` values through the control method below under
the [processing lifecycle](processing-lifecycle.md#api-session-replacement-and-publication).
ApiService never calls StorageService, requests reacquisition, or emits a
generation-loss event.

`Published(client)` sets `current = Some(client)` only for the valid published
handle. `Retired(lost)` clears `current` only when it still holds that exact
`Arc`; a late retirement for an older generation cannot clear a newer binding.
Repeating either message for the same `Arc` is idempotent and does not publish a
new watch revision. A state-changing accepted message publishes the resulting
complete snapshot. The reliable Supervisor call returns after this local state
transition has completed.

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
the ordered forwarded `Retired` message performs exact-Arc housekeeping and
wakes watchers. Outside a deliberate Rebuild replacement failure,
StorageService has already started autonomous reacquisition. The storage-owned
replacement contract defines when a later Rebuild attempt creates the next
gated generation.
For a construction seed attempt, `QueryFailed` and
`InconsistentProjection` both abandon that attempt and invoke reconstruction
without changing ApiDbState.

For a public operation, `QueryFailed` and `InconsistentProjection` leave
`ApiDbState` unchanged. Normal anchor-unavailability and invalid-request
outcomes likewise do not enter generation-loss handling. `GenerationLost`
also changes no snapshot directly: invalidity makes the exact client
immediately unusable, and the ordered generation message owns its removal.

Generation messages by themselves change no graph publication ID, state, view
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
        message: ApiDbGenerationMessage,
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
construction, alignment, publication installation, or processing
progress.
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
    Stopping(Option<PublicationRuntime>),
    Stopped,
}

struct PublicationInstallState {
    runtime_generation: u64,
}

enum PublicationInstallOutcome {
    Installed,
    Superseded,
}

struct ApiCancellation {
    http: tokio_util::sync::CancellationToken,
    sse: tokio_util::sync::CancellationToken,
    cache_jobs: tokio_util::sync::CancellationToken,
}

struct ApiService {
    runtime: tokio::sync::Mutex<ApiRuntimeSlot>,
    publication_install:
        Arc<std::sync::Mutex<PublicationInstallState>>,
    api_db: Arc<ApiDbState>,
    current_publication:
        tokio::sync::watch::Sender<Option<Arc<GraphPublication>>>,
    cancellation: ApiCancellation,
    cache_jobs: tokio_util::task::TaskTracker,
    encoding: EncodingScheduler,
    // admission and request infrastructure
}
```

`PublicationInstallState` initializes with generation zero before any runtime
exists. The first accepted reset advances it before constructing generation
one; every later accepted reset advances it exactly once.

The three ApiService cancellation tokens are independent roots. There is no
universal parent token whose cancellation could bypass the ordered shutdown
barrier. Runtime replacement does not cancel any of them. The
`PublicationRuntime` and `EncodingScheduler` retain their separately owned
tokens because their lifetimes and completion barriers differ from HTTP, SSE,
and shared cache work.

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
the runtime lifecycle mutex it briefly enters the publication-install
synchronization boundary, advances `runtime_generation` with checked
arithmetic, and applies the recovery-mode-specific ApiDbState effect below.
Exhausting the `u64` generation space is physically unreachable within one
process lifetime and is an internal invariant violation: checked advancement
must panic rather than add a typed `ApiServiceError` or recoverable `reset`
outcome. The new generation becomes authoritative before the successor runtime
starts, so this transition immediately revokes every predecessor's
publication-installation and database-read-gate authority.

ApiService then starts a new runtime with the fresh receiver, the captured
generation, and an `Arc<ApiDbState>` clone, exchanges that runtime into the
slot, and awaits the old runtime's `shutdown()`, if any. Starting the new
runtime before stopping the old one lets the new receiver drain immediately.
Successful return is a runtime-replacement barrier, but is not a processing,
publication-installation, or database-replacement barrier. The runtime slot
contains at most one runtime; during this bounded overlap, `reset` additionally
retains the exchanged predecessor only until its shutdown barrier completes.
`reset` is accepted only while the slot is `AwaitReset` or `Running`. A
`Stopping` or `Stopped` slot rejects it with `ControlUnavailable` without
advancing the runtime generation or constructing a successor; dropping the
supplied receiver closes that unused session ingress.

The current-publication slot is a latest-value asynchronous value initialized
to `None`. `tokio::sync::watch` is the settled primitive. Each runtime receives
a sender clone and its captured generation. It can request installation only
through the ApiService-owned operation equivalent to:

```rust
impl ApiService {
    fn install_publication(
        &self,
        runtime_generation: u64,
        recovery_mode: RecoveryMode,
        publication: Arc<GraphPublication>,
    ) -> PublicationInstallOutcome {
        let state = self.publication_install.lock().unwrap();
        if state.runtime_generation != runtime_generation {
            return PublicationInstallOutcome::Superseded;
        }
        self.current_publication.send_replace(Some(publication));
        if recovery_mode == RecoveryMode::Rebuild {
            self.api_db.set_public_reads_enabled(true);
        }
        PublicationInstallOutcome::Installed
    }

    fn current_publication(&self) -> Option<Arc<GraphPublication>> {
        self.current_publication.borrow().clone()
    }
}
```

The generation check, complete-`Arc` replacement, and conditional Rebuild gate
reopening are serialized against reset by the one publication-install
boundary. A generation mismatch returns `Superseded` without changing either
the slot or the gate. Installation never mutates a previous publication
through the slot. A public operation reads and clones the slot once, retains
that exact publication for its complete execution, and does not restart or
rebind when another publication is installed. A concurrent replacement
therefore affects later operations only. Publication replacement notification
can observe the same watch value; no separate replacement signal or
`arc-swap` dependency is required.

A runtime never clears the slot. ApiService stores `None` only at initial
construction and when terminal shutdown releases the current publication.

`Catchup` and `Live` are graph-update-feed markers rather than
out-of-band controls. They have no publication-completion acknowledgement. A
later session's `reset` call is the only session-supersession mechanism visible
to ApiService.

Ordinary teardown may drop the session's last producer and close its receiver.
The publication owner defines the runtime's quiescent reaction; closure is not
a session-supersession control call.

An ordinary Resync `reset` preserves both fields of `ApiDbState`. A Rebuild
`reset` invokes `begin_rebuild`; a repeated Rebuild reset has the same
idempotent effect. During Rebuild, public
database-backed request admission remains disabled in `AwaitingCatchup`, `Constructing`,
and `Aligning`.

Clearing `current` prevents the Rebuild runtime from capturing the old API
generation. The next forwarded `Published` message from the storage-owned
[replacement lifecycle](storage.md#api-read-exclusion-during-database-replacement--settled)
makes its generation immediately available through `current().await`. A first
construction read may then remain pending inside storage under that contract.
The ApiService-owned public-read gate remains disabled throughout.

On ordinary initialized startup, the initial `Published` message provides the
first usable generation and the normal Resync reset preserves it.
A coherent network-bound Empty database may likewise supply that generation;
its valid anchored requests produce ordinary typed anchor-unavailability
outcomes. If the initial Resync then leads to a distinct Rebuild run under the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled),
that run's Rebuild reset unbinds the old API generation and disables public
database reads as above.

ApiService never rebinds an in-flight public request or seed attempt to a newly
published API DB generation. Each operation finishes against its captured
client or reports its actual failure. If `reset` processing lags behind storage
replacement, exact-`Arc` retirement and publication updates still prevent a
late old-generation message from clearing the replacement.

StorageService, rather than `reset`, owns database-replacement exclusion. Its
[replacement gate](storage.md#api-read-exclusion-during-database-replacement--settled)
governs the API database phase. On the ApiService side, a request that already
detached its complete in-memory projection may finish delivering the old
coherent response; a request whose database phase the gate denies or cancels
reports database-backed read unavailability to the public API. ApiService
neither coordinates replacement nor waits for the remaining delivery of
detached responses.

After Rebuild, public database-backed reads reopen only when `Aligning`
completes and the replacement publication becomes `Active`; that
current-generation installation sets `public_reads_enabled = true` under the
publication-install boundary. A superseded runtime cannot reopen the gate. If
`current` is absent then, the graph
publication is still installed and database-backed requests continue receiving an
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

ApiService owns the runtime synchronization inside the protocol-owned
`GraphCache`. It uses a small custom publication-local cache rather than a
general cache library. `GraphCache` privately synchronizes one
`GraphCacheState`; the concrete synchronization primitive is an implementation
choice:

```rust
struct GraphCacheState {
    deltas: DeltaCache,
    head_slots: HashMap<u32, CacheSlot<CachedHeadSnapshot>>,
}

struct DeltaCache {
    slots_by_source: BTreeMap<u64, DeltaCacheSlot>,
}

struct DeltaCacheSlot {
    first_revision_timestamp_ms: u64,
    size_ceiling_revision: Option<u64>,
    completed: Option<Arc<CachedDelta>>,
    running: Option<RunningDeltaJob>,
}

struct RunningDeltaJob {
    target_revision: u64,
    job: Arc<CacheJob<CachedDelta>>,
}

struct CacheSlot<T> {
    completed: Option<Arc<T>>,
    running: Option<Arc<CacheJob<T>>>,
}

enum CacheJobState<T> {
    Pending,
    Complete(Arc<Result<Arc<T>, CacheBuildError>>),
    InvalidatedByReplayFloor {
        replay_floor_revision: u64,
    },
}

struct CacheJob<T> {
    cancellation: CancellationToken,
    state: tokio::sync::watch::Sender<CacheJobState<T>>,
}
```

Key meaning, entry eligibility, exact-tier selection, and checks which must
precede cache lookup belong to the
[protocol cache contract](api-protocol.md#publication-scoped-head-response-cache--settled).
After those checks, one short cache-state critical section applies the required
slot operation atomically. For a Head request it uses the protocol-selected
tier key to return its eligible completed entry, physically remove its
obsolete completed entry, join its running job, or install one `Pending` job
and designate that caller as its only builder. A missing Head-tier slot is
therefore installed before its builder performs extraction, serialization, or
compression.

For a delta request, the initial cache operation performs pruning and exact-
source lookup. It returns an adequate completed value, joins the existing
running job, or reports that construction may be required. The first two
outcomes consult no history. Only the construction-required outcome captures
the atomic history range and first revision timestamp needed for the source-
keyed slot, then performs a second cache operation. Concurrent callers may
duplicate that immutable capture, but the second operation either joins a job
installed meanwhile or installs the one pending job and elects its builder.
This prevents a miss burst from duplicating expensive encoding work without
placing an incompletely timestamped slot in the ordered cache. The
[publication-owned capture linearization](api-publication.md#publication-synchronization-and-request-capture--settled)
encloses this cache operation. A request needing a farther target joins the
current job and re-evaluates after it completes. Once elected, the builder
performs the soft estimate and any encoding outside the cache critical section.
The builder spawns one tracked orchestration task with a child of
`ApiCancellation::cache_jobs`.

A source-keyed delta job captures its immutable atomic history-entry Arcs and
current dyadic candidate target. Each individual encoding attempt keeps that
target fixed; size evidence may make the same pending job descend before its
next attempt. A Head snapshot tier job captures one immutable Frozen
extraction.
Head-tier job creation follows the request-driven policy owned by the
[protocol cache contract](api-protocol.md#publication-scoped-head-response-cache--settled).
Every such job uses the same tracked scheduler and final response construction
path. Every job holds only detached immutable inputs during JSON serialization
and gzip and no
publication-core or cache-state guard.

`DeltaCache` is a nested value accessed only within the enclosing
`GraphCache` state critical section. It is not another synchronization or
lifecycle owner and stores no copy of the history-owned replay floor.
`slots_by_source` supplies exact lookup, source-keyed single-flight ownership,
replacement, and query-driven prefix pruning in one index. Every slot records
the first selected revision timestamp before either a completed value or
running job is exposed. Because these timestamps are nondecreasing with source
revision, pruning removes successive first entries and stops at the first
unexpired slot; it never scans a retained suffix or consults history.

The protocol owner decides cache adequacy and whether a request must enlarge.
Under one short critical section, `DeltaCache` retains an older completed value
while installing at most one running job for the source. Every later request
joins that job. After completion, a waiter whose captured target remains
farther re-enters the slot and elects or joins the next enlargement. On
successful completion, exact job identity removes the running job and
atomically replaces the completed value only when the candidate reaches
farther. On ordinary failure it removes the matching running job and preserves
the preceding completed value.

`size_ceiling_revision` is the exclusive candidate target known to reach the
soft estimate bound. The running job lowers that ceiling under the cache
critical section, updates its recorded target to the next lower protocol
target, and continues while remaining `Pending`. Its waiters stay subscribed
to the same job. If the next lower completed value already exists, the job
installs an equivalent `size_limited` cached value sharing its encoded bytes
rather than starting another encoding.

At the beginning of a delta query, the caller supplies the protocol-captured
Head timestamp. `DeltaCache` removes its expired source prefix before lookup.
Removing a slot detaches its job from cache insertion; that job and already
admitted waiters may still finish through their retained Arcs. No Head
update or background task calls this age-pruning operation.

The publication mutation tail invokes a distinct synchronous replay-floor
invalidation when history advances its floor. Under one cache critical section
it applies the
[protocol-owned invalidation set](api-protocol.md#publication-scoped-head-response-cache--settled)
to both slot collections and, before releasing that same guard, changes every
detached running job from `Pending` to `InvalidatedByReplayFloor`. A running
slot selected by this operation must still contain a `Pending` job; any other
state is an internal invariant failure. The operation returns those already
terminal job handles. After leaving the cache critical section, the caller
cancels their work without awaiting it. Delta waiters re-enter the operation
and obtain the protocol-owned fresh-view outcome from the history floor;
Head-tier waiters re-enter their exact tier and build or join a new capture.
Exact slot/job identity prevents any detached result from repopulating the
cache or replacing the invalidation state.

If ordinary completion acquired the cache guard first, removed its matching
running job, and installed its completed value, later floor invalidation
removes that value but does not rewrite the job's terminal result; the common
final floor-admission check rejects public use. If floor invalidation acquired
the guard first, completion finds no matching slot and cannot insert or change
the already published invalidation state. These are the only two race orders.

For a Head-tier job, successful encoding is one candidate attempt rather than
terminal job completion. The orchestration task applies the protocol-owned
completion-time eligibility check against the containing publication. An
obsolete candidate is dropped, while the same `CacheJob` stays `Pending` in
the same running slot and captures and submits a newer Frozen attempt. It does
not notify waiters or expose the discarded bytes. Only a candidate passing the
complete protocol-owned age, replay-floor, and ordinary-history eligibility
check continues into the success path below. This retry remains one tracked
single-flight job; later requests for that exact tier join it rather than
creating another capture or encoding task.

Each waiter subscribes to the job's `watch` sender and selects terminal job
completion against its own request cancellation token. Cancelling one request
drops only that receiver and does not modify a waiter collection or cancel the
shared job. The job may finish and populate its originating publication cache
after its last current waiter disappears. Publication replacement likewise
does not cancel a job holding captured immutable inputs or remaining waiters.

On delta success or an eligible Head-tier success, the job upgrades its
`Weak<GraphCache>`, enters the cache-state critical section, and verifies that
the applicable exact tier or source slot still points to that job. A delta
success removes the matching source job and installs the shared completed
`Arc` when it reaches farther than the prior completed value. A Head-tier
success installs into its originating tier slot. On ordinary failure or
terminal cancellation, the job
clears only its matching tier or source job while preserving every unrelated
job and prior completed entry. Each ordinary completion, failure, or
cancellation path leaves the critical section before atomically changing
`Pending` to its terminal state through `watch`; replay-floor invalidation is
the sole exception and performs that transition under the cache guard as
defined above. The terminal value is stored even if every current waiter has
already detached, and no competing completion may replace it. A later request
may therefore retry without rejoining the failed job. If the originating cache
can no longer be upgraded, the job skips insertion but still completes its
existing receivers.

Cache-state synchronization protects only map and prefix-pruning operations,
replay-floor invalidation, size-ceiling advancement, and pointer identity
checks. No guard may
cross an `.await`, extraction, history capture, size estimation,
serialization, compression, response delivery, or waiter suspension. The
custom slots are required because
completed-value reuse, exact source-job identity, weak publication-local
insertion, source-ordered pruning, and monotonic
replacement form one cache protocol; a general-purpose cache would not replace
that synchronization.

Publication installation starts no cache job and has no cache-readiness
condition. The [publication lifecycle](api-publication.md#head-publication-lifecycle-and-stream-alignment--settled)
owns that boundary.
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
Under the system-wide
[process execution model](overview.md#process-execution-model--settled), the
encoding scheduler is the sole submitter of these CPU phases to
`tokio::task::spawn_blocking`. It submits work only after the applicable active
encoding permit has been acquired and detached inputs are ready, retains that
permit until the blocking call returns, and never uses Tokio's blocking-pool
capacity as admission. Graph JSON serialization and gzip compression do not
run directly on an async runtime worker.

The scheduler enforces the existing total and historical limits and priority.
A historical request reserves bounded queue capacity before database work,
then submits the detached projection through that reservation after releasing
all database resources.

Scheduler shutdown rejects new reservations and submissions, cancels queued
jobs, and completes their waiters with expected shutdown cancellation.
Already running blocking serialization or compression cannot be interrupted
safely; the bounded active jobs finish, their output is discarded, and the
scheduler then completes.

### SSE client registry and mailbox

ApiService implements each publication's protocol-owned SSE client
coordination with one small nonawaiting registry and one ordered mailbox per
SSE connection:

```rust
struct SseClientRegistryState {
    publication_state: GraphPublicationState,
    head_revision_id: u64,
    clients: HashMap<u64, SseClientRegistration>,
}

struct SseClientRegistration {
    schedule: DeltaWakeSchedule,
    mailbox: Weak<SseMailbox>,
    disconnect: tokio_util::sync::CancellationToken,
}

struct SseMailboxState {
    closed: bool,
    queue: VecDeque<SseMailboxItem>,
}

enum SseMailboxItem {
    Required(SseMessage),
    GraphWakeup(PublicationWakeupDto),
}

enum SseMessage {
    ClientRegistration(ClientRegistrationDto),
    PublicationWakeup(PublicationWakeupDto),
    PublicationState(PublicationStateDto),
}

enum DeltaCompletionResult {
    Accepted,
    RegistrationMissing,
}
```

`SseClientRegistry` owns synchronized `SseClientRegistryState`.
Each `SseMailbox` owns synchronized `SseMailboxState` and asynchronous receiver
readiness. The exact synchronization and readiness primitives, internal state
subdivision, and any lock ordering are implementation choices.

The map key is the random `u64` whose wire encoding is owned by the
[publication protocol](api-protocol.md#publication-wire-observation--settled).
The registry's publication-wide Head revision is the latest revision already
published by the publication core. It closes the race between graph advancement
and an HTTP response rearming a client and is not a per-client highest revision.
Each SSE connection owns the strong `Arc<SseMailbox>`, while the registry
retains only a weak reference and the connection-local cancellation token.

Registry operations and mailbox mutation linearize through short, nonawaiting
state transitions. An operation spanning registry and mailbox state preserves
the atomic behavior defined below and must be deadlock free. No synchronization
guard crosses an `.await`. Asynchronous readiness is only a hint for the single
receiver; the bounded queue remains the sole owner of message ordering and
contents.

Registration generates and collision-checks its random key while serialized
by the registry, admits the complete protocol-owned initial registration state
and message batch, and then inserts the registration. Publication replacement
executes the protocol-owned removal and new-registration sequence as one
serialized operation against the applicable registries and mailbox. Failure to
admit a required batch installs no partial registration.

A lifecycle notification records the new publication state, then attempts its
required message against every registered mailbox as one serialized registry
operation. This supplies the before-or-after race ordering required by the
[publication wire contract](api-protocol.md#publication-wire-observation--settled).
Failed weak upgrades and clients whose mailbox
rejects required admission are removed; their connection tokens are cancelled
after the registry operation completes.

After the publication core atomically publishes a new graph revision,
`advance_head` records the revision and linearly scans registrations as one
serialized registry operation. It applies the protocol-owned graph-wakeup and
coalescing rules to each applicable `DeltaWakeSchedule`. A delta response
completion likewise runs as one atomic registry operation and looks up the
supplied identifier. Absence returns `RegistrationMissing` without changing
another registration. Presence applies the protocol-selected schedule action
and returns `Accepted`. The HTTP handler commits its successful response only
after `Accepted`; it maps `RegistrationMissing` through the protocol-owned
completion disposition. These operations provide the linearization required
by the
[delta continuation contract](api-protocol.md#canonical-head-delta-responses--settled)
without independently defining its scheduling semantics.

The scan is bounded by `MAX_SSE_CLIENTS`. With the initial 1,024-client limit,
a direct scan avoids a second boundary index and its rearm, replacement, and
disconnect maintenance. The registry creates no timer and no per-client task;
the existing Axum SSE handler is the mailbox's only receiver. Mailbox admission,
coalescing, required-batch atomicity, and slow-client behavior belong to the
[public delivery contract](api-protocol.md#public-delivery-failures--settled).
The mailbox queue never exceeds `SSE_CLIENT_BUFFER_CAPACITY`. Closing a mailbox
rejects later admission and notifies its receiver; the receiver returns no
further item after the closed queue has been drained.

### HTTP requests and SSE connections

Each admitted API request owns a private guard containing its applicable lane
permit, service-cancellation observation, and completion notification:

```rust
struct ApiRequestGuard {
    cancellation: tokio_util::sync::DropGuard,
    // admission permit and completion registration
}
```

Admission creates a unique child of `ApiCancellation::http` and places its
drop guard in `ApiRequestGuard`. Cancelling that child affects only the
request. Dropping the handler or response body cancels its remaining
request-local work; a request-local timeout may cancel the same child without
affecting another request. Database and other request-local futures observe
that token where cancellation is applicable. A cache wait observes it only to
detach that waiter: it never receives or cancels the shared job token.

Axum executes the handler future. For a graph response, the response body
retains the guard through complete delivery, delivery failure, client
disconnect, delivery timeout, or shutdown cancellation. A request cancelled
before response commitment returns the applicable unavailable result when
possible; cancellation after commitment terminates the body or stream and
never attempts a second response.

Each accepted SSE connection owns an SSE admission guard, one bounded
`SseMailbox`, its current publication-local identifier and registration, and a
drop guard for a unique child of `ApiCancellation::sse`. It observes the
current-publication watch. On replacement it performs the protocol-owned
registration transition and ordered initial sequence without replacing or
cancelling that token. Normal disconnect, connection-local failure, slow-client
disconnection, or cancellation affects only that child and removes the current
registration before releasing the guard. Mandatory-mailbox admission follows
the protocol-owned slow-client rule and never blocks publication mutation or
shutdown.

Cancellation requests termination but does not prove cleanup. HTTP and SSE
completion remains established by their guards, cache completion by
`TaskTracker`, and runtime and encoding completion by their explicit shutdown
barriers.

## ApiService shutdown — settled

`shutdown` is a reliable completed-barrier operation. It is terminal,
idempotent, valid from `AwaitReset`, `AwaitingCatchup`, `Constructing`, `Aligning`,
and `Active`, and is serialized with `reset` by the runtime
lifecycle mutex.
ApiService performs or resumes the local barrier in order:

1. close public graph HTTP and status admission;
2. replace `AwaitReset` with `Stopping(None)` or `Running(runtime)` with
   `Stopping(Some(runtime))`; an existing `Stopping` slot resumes the barrier,
   while `Stopped` completes immediately;
3. publish `ApiDbSnapshot::Stopped`, waking pending construction and making
   public database reads unavailable;
4. while the slot is `Stopping(Some(runtime))`, retain that runtime in the slot
   and await its publication-owned `shutdown()` barrier, allowing its final
   `Stale` transition to propagate through the normal ordered SSE path to
   currently registered clients; replace it with `Stopping(None)` only after
   that barrier completes;
5. cancel `ApiCancellation::sse`, then
   `ApiCancellation::http`, and wait for all SSE and request guards to be
   released;
6. close `cache_jobs` to new registration, cancel
   `ApiCancellation::cache_jobs`, and cancel every unfinished cache job;
7. signal encoding-scheduler shutdown and await both `cache_jobs.wait()` and
   `EncodingScheduler::shutdown()`;
8. replace the current-publication value with `None`, then release every API DB
   permit, transaction, connection, validated client, pool handle, publication,
   and cache reference; and
9. replace `Stopping(None)` with `Stopped` and complete the `shutdown` call.

Entering `Stopping` is the irreversible ApiService shutdown cutoff. Admission
is already closed, and the transition occurs before `ApiDbState::stop()` and
before the first await. Cancellation of the caller or an early barrier error
therefore leaves the slot in `Stopping`, retains any runtime whose barrier has
not completed, and cannot restore reset authority. Every shutdown step is
idempotent or retains its own completion state, so a later `shutdown` call
reacquires the lifecycle mutex and resumes the same barrier. `Stopping` can
advance only to `Stopped`; it never returns to `AwaitReset` or `Running`. Reset
eligibility is determined solely by this runtime lifecycle slot; ApiService
must not infer it indirectly from `ApiDbState`.

Shutdown waits for the final Stale state to enter each currently registered
client's bounded SSE buffer, not for its network delivery. A client whose
buffer cannot accept that mandatory notification is disconnected under the
normal slow-client rule before the remaining connections are cancelled.

Successful method completion proves that no API admission, task, graph-update
receiver, or API database resource remains. A repeated `shutdown` returns
success for the already completed state. Runtime-lifecycle serialization and
the retained `Stopping` state reject `reset` once shutdown begins. Publishing
`ApiDbSnapshot::Stopped` is the generation-update cutoff: an update linearized
before that transition may complete and is then cleared by the transition,
while a later update is rejected.
ApiService adds no component-local timeout or escalation to this barrier; the
Supervisor waiting policy belongs to the
[processing lifecycle](processing-lifecycle.md#teardown-and-delivery-semantics--settled).

Publication-state and graph effects of the awaited runtime barrier belong to
the [publication runtime contract](api-publication.md#publication-runtime--settled).
Public HTTP admission is already closed while the protocol-owned state message
uses the still-open ordered SSE path. Releasing the graph-update receiver is
the ApiService-side effect of this barrier; the resulting producer disposition
belongs to the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled).

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

ApiService has no independent response-memory semaphore or admission lane.
The existing owners bound response memory at the points where it is allocated
and retained:

- an active encoding job owns its transient JSON and gzip buffers, so
  `MAX_GRAPH_ENCODING_JOBS` bounds simultaneous encoding allocations;
- `GraphCache` owns completed cached bodies, and delivery clones their shared
  immutable `Bytes` rather than copying the body for each waiter;
- a completed uncached historical body remains owned by its request under the
  historical HTTP permit until delivery ends; and
- a Head-level lookup remains owned by its request under the Head HTTP permit
  and its protocol-owned cardinality bound.

Every HTTP request keeps its applicable admission guard through response
delivery. Pre-reserving a response's maximum possible size would strand most
of that capacity, while acquiring memory admission after encoding would allow
the allocation first and could then retain a completed body while waiting.
Charging every cache waiter for the shared body would instead count one
allocation many times, and a separate request-memory permit would not bound the
publication cache. The owner-based model avoids those mismatches while keeping
all allocations within already bounded work.

`MAX_GRAPH_ENCODING_JOBS = 4` is the initial processing-protective concurrency
limit. Cache reuse and source-keyed single-flight mean concurrent client count
does not determine the number of encodes, and the historical sublimit leaves
two active slots for publication and Head work. Increase this limit only from
observed encoding queue latency, Head response latency, idle CPU capacity,
processor commit latency, and active encoding memory measurements.

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
HTTP admission and any request-owned body are released. A cached body remains
owned by `GraphCache` independently of that waiter. SSE streams are exempt from
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
request-owned projection or completed body, but never database capacity.
Serialization, compression, response-size, and client-delivery failures after
that boundary are API-local and do not retire a database generation or request
processing recovery. A connection-level failure during the database phase
retains StorageService's
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
- encoded-response cache hits, misses, pruned entries, and coalesced identical
  requests, including raw requested Head depth, selected Head tier,
  publication-local tier age in milliseconds, query-time obsolete-entry
  removal, tier-job joins, construction duration, construction failure, and
  completion-time obsolete-candidate retries;
- delta cache hits, joins, builds, enlargements, cache age and prefix pruning,
  serialized source jobs, size-ceiling reductions and in-job descent,
  dyadic target and delivered spans, active interval, per-client revision
  wakeups, requests per catch-up, size-limited short-prefix frequency,
  replay-floor advances, invalidated delta and Head-tier jobs, estimated versus
  actual canonical-delta JSON bytes, and estimator-invariant violations;
- database pool and public-read permit occupancy, query duration, and query
  timeouts;
- encoding reservation, queue, and active-job occupancy, queue timeouts, and
  encoding duration, together with active transient uncompressed and gzip
  buffer bytes;
- completed uncached delivery bytes and publication-cache encoded bytes;
- response delivery duration and delivery timeouts;
- delta-journal resets; and
- BlockProcessor and VspcProcessor commit latency.

The metrics export mechanism and labels remain deferred in the
[decision register](../decisions/deferred.md).

API traffic up to the settled rejection limits must not materially increase
either processor's commit latency.
