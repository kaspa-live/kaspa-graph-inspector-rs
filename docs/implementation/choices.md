# KGI v2 implementation choices

This non-normative record captures durable implementation choices made under
the constraints in the focused architecture. It does not amend those
contracts. Choices are recorded when their first dependent implementation
tranche begins; unresolved choices remain in the
[deferred register](../decisions/deferred.md).

## 5 October 2026: shared model and graph-update ingress

### Initial module boundaries

`kgi-model` starts with these modules:

- `block`: block identity aliases, timestamps, colors, validated node blocks,
  consensus order, database-local coordinates, materiality, sync anchors, and
  persisted-block delivery values;
- `graph_update`: committed block/VSPC projection payloads and the ordered
  graph-update enum;
- `lifecycle`: recovery modes, cross-component fault values, and component
  status observations; and
- `vspc`: normalized and ready VSPC transition values.

`lib.rs` exposes these as public modules without re-exporting their contents at
the crate root. Consumers use qualified imports such as
`kgi_model::lifecycle::RecoveryMode`, preserving the semantic concern in each
type's path. These boundaries can split when a module gains independent
behavior; crate ownership and the acyclic dependency graph remain unchanged.

`kgi-api-ingress` starts with:

- `channel`: channel construction, producer and receiver capabilities, the
  producer gate, offer outcomes, and producer errors; and
- `gap`: the session-local coalescing continuity-generation reporter and
  consumer observation.

`lib.rs` exposes these as public modules without re-exporting their contents at
the crate root, matching `kgi-model`'s module-qualified import style.
Operational counters stay next to the channel state until a common metrics
adapter is implemented.

### Upstream value crates and revision

Use the following workspace dependency declarations for Kaspa value crates:

```toml
kaspa-consensus-core = { git = "https://github.com/kaspanet/rusty-kaspa.git", tag = "v2.1.0" }
kaspa-hashes = { git = "https://github.com/kaspanet/rusty-kaspa.git", tag = "v2.1.0" }
kaspa-math = { git = "https://github.com/kaspanet/rusty-kaspa.git", tag = "v2.1.0" }
```

The component status values use
`kaspa_consensus_core::network::NetworkId`;
`kgi_model::block::BlockHash` aliases `kaspa_hashes::Hash`; and
`kgi_model::block::BlueWork` aliases
`kaspa_math::Uint192`. This keeps upstream representation and ordering while
avoiding a dependency from `kgi-model` on consensus processing, RPC, or service
crates.
The workspace dependency selection advanced from `v2.0.1` to `v2.1.0` on
6 October 2026. The committed Cargo lockfile records the resolved source
commit. The upstream tag resolves to
`01b532e8b553523216471682649693af92f0fd16`.

This tag is the implementation dependency selection for the initial model.
PUAR acceptance and runtime node-compatibility scope remain owned by the
[verification contract](../architecture/verification.md).

### Tokio channel, gate, and gap primitives

Use `tokio::sync::mpsc::channel` for the bounded graph-update channel. Its
cloneable `Sender` and single `Receiver` match the session capability split,
and `try_send` distinguishes full capacity from receiver closure.

Use `std::sync::Mutex<GraphUpdateGateState>` for the producer gate. Every
ordinary classification and `try_send` occurs in one short, non-async critical
section. Lossless Live-marker delivery releases this mutex before awaiting
channel capacity, so no Tokio task may block while holding it. Mutex poisoning
is an internal invariant failure rather than a recoverable session condition.

Use `tokio::sync::watch<u64>` for gap observation. The value is a monotonically
increasing session-local generation starting at zero. Reporting a gap advances
the generation and publishes the latest value with `send_replace`; multiple
reports may coalesce into one wakeup while the changed generation remains
observable. The consumer records the last generation it has handled and uses
`borrow_and_update` plus `changed` so registration cannot lose a report between
inspection and waiting. Counter overflow is an internal invariant failure.

The channel, gate, and watch endpoints are created together. The receiver owns
both consumer endpoints, so they cannot be closed independently through the
public API.

### Error conventions

Library crates use explicit typed error enums derived with `thiserror::Error`.
They do not return `anyhow::Error`, boxed dynamic errors, or select control flow
from error strings except for the single NodeService-owned
[GetBlock compatibility adapter](../architecture/node-service.md#getblock-not-found-compatibility-classification).
Component-local errors may retain concrete upstream sources; before an error
crosses an ownership boundary it is classified into the shared typed fault
vocabulary, with `Arc<str>` used only for diagnostic context.

The top binary and `xtask` may use `anyhow` for command-boundary context where
no caller branches on the error. They must still preserve typed component
errors until command dispatch has selected the process result, and they must
apply the architecture's secret-redaction requirements.

Tests use direct typed matching rather than formatted-message matching.

### Rust test runner

Use `cargo nextest run --workspace --locked` for workspace unit and integration
tests, following rusty-kaspa's test-runner split. Run
`cargo test --doc --workspace --locked` separately because Nextest does not run
rustdoc tests. Targeted development runs may narrow the package or test filter
while retaining Nextest. No custom Nextest profile is added until a concrete
test needs repository-specific retry, timeout, or grouping behavior.

## 5 October 2026: process configuration and signals

### Configuration dependencies and value protection

Use `clap` with its derive API for the top-crate command grammar, `serde` derive
and `toml` for the explicitly selected configuration file, and `url` for parsed
URL values. Keep these dependencies in the smallest owning crate: command,
source-loading, and resolution dependencies belong to `kgi`, while `kgi-core`
depends only on crates needed by its foundational configuration, signal, and
timing infrastructure. Direct dependency versions shared with rusty-kaspa
follow its selected `v2.1.0` workspace where applicable.

Retain the logging system from rusty-kaspa's `core/src/log` when process
logging is implemented. `LoggingConfig.level` therefore carries that logger's
root-or-subsystem filter expression, and its optional directory maps directly
to file logging being enabled or disabled. Logger initialization remains a
later top-crate startup step; the current configuration increment defines only
the resolved values it will consume.

Do not use clap's environment-variable integration. Capture the supported
environment variables once into an explicit input and pass that input, the
parsed CLI layer, and any parsed TOML layer to the resolver. This makes source
precedence and invalid-value behavior directly testable without mutating the
process environment.

Database URLs and reinitialization tokens use dedicated value wrappers with
redacted `Debug` implementations and narrowly scoped secret accessors. Raw
source structs containing either value do not derive unrestricted `Debug` or
`Display`. Configuration errors retain a field and source classification but
do not retain or format the rejected source value. TOML deserialization errors
are mapped to a redacted top-level diagnostic because a parser-provided source
snippet can contain the database URL.

These choices implement the secret-handling and parsed-value requirements in
the [process configuration contract](../architecture/overview.md#process-configuration-and-command-entry--settled).

### Top-crate module boundaries

Start the private top-crate implementation with these modules:

- `cli`: clap argument and subcommand shapes plus parsing from an injected
  argument iterator;
- `config`: raw source layers, explicit file and environment loading,
  resolution, static validation, and typed diagnostics; and
- `command`: the validated service and administrative invocation values that
  form the later dispatch boundary.

Keep `main` as the composition entry. `kgi-core::config` contains the resolved
configuration value structs and their protected value types only, preserving
the ownership boundary in the focused architecture. Resolver tests stay next
to the private top-crate modules until a public library boundary is needed by
another crate.

### Signal adapter dependency and tests

Use `ctrlc` with termination-signal support for `kgi-core::signals`, matching
the pinned rusty-kaspa implementation dependency. Keep callback counting and
weak-target behavior in an internal method callable by unit tests; the
installed handler calls that same method. Verify forced third-signal process
termination in a subprocess so the test runner itself cannot exit.

The public adapter shape and callback behavior remain owned by the
[process termination contract](../architecture/overview.md#process-termination-signal-adapter--settled).

## 6 October 2026: NodeService implementation foundation

### Crate and module boundaries

Start `kgi-node` with these public modules:

- `consensus`: `KgiConsensusParams`, its typed validation errors, and the
  startup-time local parameter resolver;
- `error`: NodeService, validated-RPC, rejection, and operation error values;
- `rpc`: `ValidatedRpcClient`, `ValidatedNodeInfo`, normalized response values,
  and the public operations on one validated generation; and
- `service`: `NodeService`, its state and ordered events, status observation,
  construction, and lifecycle methods.

Keep the upstream client adapter, raw-response normalization,
`NotificationRouter`, and service-loop commands in private `client`,
`normalization`, `notification`, and `runtime` modules.
Tests remain beside their owning module, with integration tests added only for
cross-module generation and routing order. `lib.rs` exposes the public modules
without wildcard re-exports, so consumers retain paths such as
`kgi_node::rpc::ValidatedRpcClient` and `kgi_node::service::NodeService`.

These are Rust placement choices only. The focused
[NodeService architecture](../architecture/node-service.md) remains the owner
of connection, generation, normalization, and notification behavior.

### Upstream and support dependencies

Use the `v2.1.0` `kaspa-grpc-client` in direct-notification mode as the physical
client. Its production connector uses the constructor whose automatic
reconnect argument is fixed to `false`; NodeService, rather than the upstream
client, owns replacement generations. Use `kaspa-rpc-core` for RPC request,
response, notification, and API compatibility values, `kaspa-notify` for the
notification trait and scopes, `kaspa-consensus-core` for local parameter
resolution, and `kaspa-core` for logging through the retained upstream logging
facade. The direct `log` dependency exists only because the exported
`kaspa-core` logging macros expand through that crate; production calls retain
the `kaspa-core` facade. Raw upstream values do not leave `kgi-node`.

Use `serde_json` only to decode the upstream `OverrideParams` representation,
`url` for the already parsed endpoint, `thiserror` for typed errors, and
`async-trait` for private testable adapter traits. Tokio supplies the worker,
channels, status observation, completion barriers, and RPC permits;
`kgi-core::timing` supplies one cloneable `Timing` dependency wrapping the
shared object-safe clock and jitter interfaces. All
rusty-kaspa crates use the same workspace tag and lockfile revision accepted by
the [current PUAR](../architecture/verification.md#current-puar-result).

### Lifecycle and generation primitives

Run NodeService ownership in one Tokio task and serialize its state changes in
that task. Use private unbounded Tokio MPSC channels for its low-rate reliable
control queue and ordered `NodeServiceEvent` stream. Construction creates the
event receiver before the task can start, preserving the installation boundary
without exposing the command sender. Supervisor-facing async methods submit
private commands and use Tokio one-shot completion barriers; callers never
observe or depend on the mailbox representation.

Publish `NodeServiceStatus` through a Tokio watch channel because status is a
latest-value observation rather than a reliable lifecycle input. Keep the
worker join handle under the service owner and make the completed shutdown
result reusable by later callers.

Each `ValidatedRpcClient` uses a private atomic admission flag and a Tokio
semaphore for the focused-owner runtime RPC limit. An operation obtains its
permit and then confirms generation admission before issuing an upstream call.
Retirement closes admission before disconnecting the physical client. A
private synchronous mutex protects the router and subscription state needed by
rusty-kaspa's synchronous notification callback; no mutex guard crosses an
await. Generation-ending reports use the generic `kgi-core` retirement request
envelope with a node-local weak generation target and reason. The envelope's
one-shot barrier carries the service-side `Result`; NodeService acknowledges
success only after event enqueue and maps a failed barrier to the existing
operation-level retirement-control error.

### Deterministic test seams

Define private object-safe `RpcConnector` and `RpcConnection` traits containing
only the operations KGI consumes. The production adapters delegate to
`kaspa-grpc-client` and `kaspa-rpc-core`; unit tests use scripted connections
that return real upstream response value types. This avoids implementing the
complete upstream RPC trait or duplicating a gRPC server while still testing
KGI-owned request construction, normalization, retirement, and ordering.

Inject the shared `kgi-core::timing::Timing` value through NodeService's private
runtime dependencies. Production combines the Tokio clock and entropy-seeded
equal-jitter source. Tests combine a manually advanced clock and scripted
jitter, so reconnect slots, the Ready reset boundary, and shutdown cancellation
contain no wall-clock sleeps or
probabilistic assertions. Exact delays and reset behavior remain owned by the
[NodeService lifecycle](../architecture/node-service.md#nodeservice--settled).

### Validated-generation composition

Keep RPC API compatibility checking inside NodeService's private connection
validation path. It is not exposed as a public free function or utility type;
the published result is the semantic `ValidatedNodeInfo` attached to the exact
generation. The same path constructs the raw Genesis-discovery request and
polls server information during IBD. The private clock supplies a one-second
IBD polling interval so those waits and shutdown races remain deterministic in
tests.

Each `ValidatedRpcClient` owns one `Arc<ResponseNormalizer>` and passes a clone
to one stable `Arc<NotificationRouter>`. Subscription activation installs fresh
session destinations into that router before starting remote subscriptions;
successful deactivation clears them for a later session, while generation
retirement permanently retires the router. The public `NotificationChannels`
value groups the two bounded processor senders and reliable fault sender
without exposing router internals.

The permanent service task keeps physical connection, validation, event, and
retirement ownership serialized in one loop. Stale retirement barriers are
acknowledged during connect, validation, retry, and rejected phases as well as
Ready, so a late report cannot disturb a replacement or remain blocked behind
one. Tests use identity jitter when asserting nominal slots and the manual
clock for both the Ready reset boundary and shutdown-cancellable waits.

## 7 October 2026: StorageService implementation foundation

### PostgreSQL client and migration stack

Use SQLx `0.9.0` with its PostgreSQL driver, Tokio runtime, embedded migration
support, and Rustls with WebPKI roots. Disable SQLx's default feature set so
KGI does not compile unused database drivers or JSON support. The `macros`
feature is enabled only for embedding the migration directory; production SQL
uses runtime `query`, `query_as`, and `query_scalar` APIs. KGI therefore does
not require a live database during an ordinary build, and checked-in SQLx
offline query metadata remains deferred until a concrete benefit justifies
the additional workflow.

Migration files remain solely under `crates/kgi-storage/migrations/` and are
embedded in the binary. Use SQLx's own migration-history table and checksum
validation. Every migration remains forward-only and transaction-compatible;
the administrative replacement path will run the embedded migration set
inside its owning outer transaction rather than introduce a second schema
definition. When SQLx migration history exists, inspect its maximum version
before comparing the database's table set with the current binary's known
layout, so ordinary future migrations that add tables retain the specific
`SchemaTooNew` classification.

After migration, construct a physical-layout fingerprint from
`information_schema` and `pg_catalog`. Match the exact KGI table, column,
default, identity, and constraint signatures, and require the complete set of
constraint-backed and query-path index signatures, including index validity,
readiness, access method, key order, sort options, and partial predicate. Extra
operator-created indexes do not alter KGI semantics and are permitted; an
altered or missing required object is `UnsupportedSchema`.

### Concrete SQL representations

Implement the SQL representation owned by the
[storage contract](../architecture/storage.md#persistent-representation--settled)
with SQLx runtime binds and rows. For the concrete fields left open there, use
PostgreSQL `SMALLINT` for `BlockColor`, the canonical `NetworkId` string as
`TEXT`, and nullable `TEXT` for the optional administrative reinitialization
token. Conversion helpers remain private to `kgi-storage`; they return typed
errors rather than exposing driver conversion failures across the crate
boundary.

Use the fixed session advisory-lock key `0x4b47_4932_0000_0001` on one
dedicated SQLx `PgConnection` outside both pools. PostgreSQL scopes advisory
locks to the connected database, so the same application key independently
protects each database. Start the processing pool at four connections. The
separate API pool takes its size from the
[ApiService resource owner](../architecture/api-service.md#resource-isolation-and-saturation--settled).
The private processing-pool size does not become a public API value.

### Crate and module boundaries

Start `kgi-storage` with public `error`, `generation`, and `service` modules.
They own the typed storage failures, `DatabaseBinding` and validated generation
capabilities, and the Supervisor-facing service lifecycle respectively. Keep
SQLx adaptation, migration execution, schema inspection, processing-state
inspection, service-loop commands, and retirement plumbing in private
`database`, `migration`, `schema`, `state`, and `runtime` modules. Consume the
generic `Timing` wrapper from `kgi-core::timing`. Expose the public
modules without crate-root wildcard re-exports.

These boundaries do not expose SQLx pools, connections, transactions, raw
queries, or schema-classification details. Add each module only with its first
implemented behavior rather than creating placeholder APIs.

### Validated generations and stored session state

Keep `DatabaseBinding`, `ValidatedDbClient`, `ValidatedApiDbClient`, and the
storage-owned session-state result values together in the public `generation`
module. `LockedDatabase::prepare` consumes the advisory-lock capability and
returns `PreparedDatabase` only after migration and physical-layout validation.
`ValidatedDbClient::load_session_state` delegates to the private state adapter
and exposes no SQLx value. `ProcessingStateInspection` owns the repeatable-read,
read-only transaction used by both session loading and initial generation
classification.

The adapter implements the storage-owned
[bounded processing-state classification](../architecture/storage.md#bounded-processing-state-classification--settled)
contract. Query shape and indexes remain implementation details only within
that settled complexity boundary. It converts immutable network-binding decode,
construction, and cardinality defects directly to `UnsupportedSchema`; those
defects never enter the mutable processing-state `Inconsistent` path.

### PostgreSQL integration tests

Use `testcontainers-modules` with the official `postgres:17-alpine` image and
SQLx itself as the test client. Each fixture receives an isolated temporary
database and requires no ambient `DATABASE_URL`. Keep the image tag explicit
so a routine dependency or image update cannot silently change database
behavior. Tests use runtime SQL APIs, run through Nextest, and retain container
handles until every database assertion and cleanup barrier has completed.

## 8 October 2026: Permanent StorageService lifecycle

### Worker, observation, and retirement plumbing

Run StorageService ownership in one Tokio task. Use private unbounded Tokio
MPSC channels for its low-rate control queue, reliable ordered lifecycle-event
stream, and exact-generation retirement reports. The public service handle
retains the task join handle and exposes latest-value status through a Tokio
watch channel. Initialization, shutdown, and operation-detected retirement use
one-shot completion barriers; the retirement barrier carries
`Result<(), StorageError>` so an event-path failure reaches the reporting
operation rather than being mistaken for completed retirement. Initialization
requests received while the database is unavailable remain in FIFO order.

Both validated client types compose one private generic generation runtime that
owns their pool, terminal atomic admission flag, retirement sender, and weak
self-reference. A private generation-kind trait maps each concrete client type
to its storage-local processing-or-API retirement target. Reports use the
generic `kgi-core` retirement request envelope with that target and no reason
payload. The service task compares the weak target with the currently owned
exact `Arc`, suppresses stale and repeated reports, changes validity before
event publication, and completes an operation barrier only after the retirement
event has been enqueued. Retirement synchronously marks the SQLx pool closed,
then moves its potentially blocking drain into a tracked Tokio task so the
serialized lifecycle worker can publish and validate the replacement without
waiting for checked-out connections. Terminal worker completion joins every
tracked drain. Processing and API pool generations remain independently
replaceable. Losing the dedicated advisory-lock connection uses the same
owned-generation retirement path before the service reacquires database
ownership; the [storage lifecycle](../architecture/storage.md#storageservice-lifecycle--settled)
owns the exact conditional event contract.

### Connection lifecycle and deterministic timing

Retain the parsed database URL privately in StorageService and create fresh
SQLx pools for every replacement generation. Poll the dedicated advisory-lock
connection once per second throughout the active lifecycle, including while a
missing pool generation is opening or waiting for retry, and recheck it after
initial or replacement generation validation immediately before publication.
Unpublished clients from a failed initial ownership recheck are invalidated and
drained without emitting retirement events. This interval is a private
health-check implementation choice; the reconnect delays, equal-jitter range,
and 60-second Ready reset remain defined by the
[storage lifecycle](../architecture/storage.md#storageservice-lifecycle--settled).

Inject the private lock connector, generation opener, and shared
`kgi-core::timing::Timing` value into the worker. Production uses SQLx plus the
Tokio clock and entropy-seeded equal-jitter source; tests use scripted
connection results, gated initial and replacement opening, individually
controlled sleeps, and identity jitter. PostgreSQL container tests
terminate the actual advisory-lock backend to verify exact retirement,
suppression of initial or replacement publication until ownership is
reacquired, and autonomous republication. The Rebuild replacement gate and API
database read-lease drain are recorded in the later implementation entries
below rather than this general connection lifecycle slice.

Map connection-class failures specifically at a persistent-mutation COMMIT
boundary to the shared `Persistence(AmbiguousCommit)` value; ordinary SQLx
operations retain the generic connection-loss mapping. The initialization
integration fixture can inject acknowledgement loss after PostgreSQL has
accepted COMMIT, then drops the uncertain connection and runs normal database
preparation again. This verifies the storage owner's transaction-outcome
contract without teaching the service to replay an uncertain mutation.

## 9 October 2026: Storage persistence operations

### Processing-generation caches and identity reads

The processing-generation caches implement the
[storage-owned cache requirement](../architecture/storage.md#caches-and-identity-resolution--settled).
Start each block-scaled processing cache at 50,000 entries: forward identity,
materialized-block metadata, and merge sets. The current-sink cache has
capacity one. The initial block-scaled capacity targets the recent processing
working set and is deliberately tunable from observed hit rates, eviction
rates, and database-read latency.

Put the storage-specific public result types in the module-qualified
`kgi_storage::operation` namespace. Deduplicate only the SQL misses before
constructing the cold batch's left-joined query.

### Materialization transaction execution

Implement the
[storage-owned transaction policy](../architecture/storage.md#transaction-retries)
through one private closure-based runner shared by processing mutations. It
classifies raw SQLx errors before their SQLSTATE is erased and consumes the
shared injectable `Timing` value. Give each processing generation a
`tokio::sync::Mutex` materialization
lane; hold its guard through the complete transaction, any retry delay, definite
commit, and cache publication. This preserves sequential allocation without
serializing the separately owned VSPC mutation lane.

Keep materialization-specific SQL and committed-payload construction in the
private `materialization` module, shared block and direct-parent row insertion
in the private `generation::block_rows` module, and reusable retry
classification in the private `transaction` module.
Each materialization attempt applies the storage-owned cache-completeness rule
when seeding its resolved-identity map. Collect all incomplete values into one
transaction-local batch query. Cache publication need not republish complete
hits.
Intern the remaining absent identities in one `BYTEA[]` statement using
`WITH ORDINALITY`; the insertion source and returned rows follow the
storage-owned first-occurrence order. Insert direct-parent rows in one
parallel-array `UNNEST` statement; boundary identities use their persisted
`(0,0)` sentinel.

### VSPC transaction execution

Give each processing generation a second `tokio::sync::Mutex` lane for VSPC
mutations. Hold it through complete-transaction retries, definite commit, and
processing-cache publication. The materialization and VSPC lane guards are
independent, so their PostgreSQL transactions may overlap as required by the
[storage concurrency contract](../architecture/storage.md#internal-concurrency).

Store compact IDs, rather than hashes, in the generation-owned merge-set
cache. Materialization already resolves those IDs transactionally, and VSPC
coloring consumes the same persisted representation. Load direct VSPC member
rows for only the first-occurrence-distinct IDs that the storage-owned
completeness rule sends to SQL, while retaining the original ordered vectors
for path validation and transition semantics. Load merge sets only for
distinct added IDs that miss the cache.

Compute the final merge-member color for each compact ID by replaying every
added position in memory in its blue-then-red order, then issue one parallel
ID/color `UNNEST` update. This preserves the settled last-write result across
cross-color overlap and repeated added positions without one SQL statement per
member. Update and return all distinct affected level states through one
transaction-local CTE, so the committed outcome requires no post-commit read.

## 10 October 2026: API database replacement gate

### Read leases and replacement-attempt fencing

Use this concrete synchronization mechanism for the
[storage-owned replacement gate](../architecture/storage.md#api-read-exclusion-during-database-replacement--settled)
as one private `ApiReplacementGate` shared by every API generation opened by a
StorageService instance. Represent the architecture's API database phase in
code with an RAII `ApiDbReadLease`; the lease name distinguishes the concrete
synchronization object from a lifecycle phase. The gate keeps a short
standard-library mutex around its admission state and active-lease registry.
Each lease owns a watch-based cancellation receiver, removes its unique entry
synchronously on drop, and wakes replacement drainers.

Fence each close-and-replace attempt with a monotonic in-memory epoch carried
by its exclusive token. Reopening admission requires both the current epoch
and an empty active-lease registry, preventing a cancelled or timed-out older
attempt from reopening a newer replacement. A separate Tokio watch revision
is only a wakeup mechanism for gated readers and drainers; it carries no
generation or replacement identity. API-generation retirement is serialized
against lease acquisition by a generation-local standard-library mutex and
wakes gated acquisition through a validity watch channel.

## 11 October 2026: Rebuild transaction prerequisites

### Processing mutation exclusion and cache-set publication

Use one generation-local `tokio::sync::RwLock` as the shared processing
mutation guard. Materialization and VSPC mutations hold read permits in
addition to their independent lane mutexes, so those lanes retain their
permitted overlap. Rebuild will acquire the write permit with `try_write`; it
will not wait behind an unexpected mutation after predecessor-session shutdown.

Group the four independent Moka domains in one `ProcessingCacheDomains` value
and hold the current value through `ArcSwap`. Short synchronous interactions
use its borrowed `load` guard, while an operation that retains the cache set
across an await takes one owned `load_full` snapshot for its complete cache
interaction. Rebuild will construct and seed a fresh domain set off-path, then
publish all four domains through one atomic pointer replacement after definite
commit. This avoids an outer mutex around ordinary Moka access and prevents
observers from combining predecessor and replacement domains.

### PostgreSQL processing-data clearing

Use one transactional `TRUNCATE parents, blocks, levels, block_identifiers,
processing_metadata RESTART IDENTITY RESTRICT` statement for pruning-point
rebuild. Listing the complete processing-table dependency set keeps `RESTRICT`
explicit, resets compact-ID allocation in the same transaction, and avoids a
retained-graph delete scan. Its table locks rely on the storage-owned API
replacement gate, processing mutation guard, and process advisory lock rather
than an additional explicit table-lock statement.

### Rebuild lifecycle wiring

Carry rebuild requests from `ValidatedDbClient` to the permanent
StorageService worker through a private unbounded exact-generation channel.
The request owns the normalized pruning-point value and a result-bearing
one-shot completion path. This keeps API-generation lifecycle changes and gate
tokens inside their existing worker owner while leaving the SQL transaction in
the private `generation::rebuild` module beside the other processing
transactions.

Prepare the identity insertion order once, intern it with one ordinal batch,
and derive all block, parent, merge-set, anchor, and cache values from that
single returned ID map. The worker continues polling its advisory-lock health,
shutdown command, retirement requests, and cleanup tasks while API pool
creation and lease draining are pending. It starts the replacement-exclusive
future when admission closes and polls it concurrently with fresh API pool
creation, so the drain timeout is measured from the cancellation request rather
than from later publication. Once the transaction begins, it runs to a definite
or ambiguous database outcome even if lifecycle retirement is selected
concurrently; the worker then applies the exact outcome to the already selected
lifecycle disposition.

## 12 October 2026: Processing child workers

### Module and channel boundaries

Keep OrphanManager and DependencyResolver in private `block_processor::orphan`
and `block_processor::dependency` modules. Their owner-facing data values stay
beside BlockProcessor so the later processor loop can consume one resolution
lane without exposing child protocols from `kgi-processing`. Keep shared typed
child control failures, worker faults, and session-data path diagnostics in
the private `block_processor::error` module; operation-local topology errors
remain with their implementation.

Use separate `tokio::sync::mpsc::unbounded_channel` command mailboxes and
bounded Tokio data mailboxes. Explicit command draining precedes every lower
priority receive. Size every processing data mailbox with the capacity owned
by the [processing lifecycle](../architecture/processing-lifecycle.md#supervisor-and-recovery-intent--settled).
Terminal shutdown acknowledgements use Tokio one-shots.

Construct processing data mailboxes through the foundational
`kgi_core::channel::bounded_channel` wrapper. It preserves Tokio's bounded
sender, receiver, and full/closed result behavior while maintaining one shared
current-occupancy and sticky high-water snapshot. The same wrapper crosses the
NodeService notification-routing boundary, so notification callbacks and the
processor owner expose one measurement for the same queue. Prioritized command
mailboxes and the separately owned graph-update ingress do not use this
wrapper.

The DependencyResolver worker is private and generic over a minimal async RPC
operation adapter. OrphanManager constructs that child with the exact
`Arc<ValidatedRpcClient>` supplied to the session; tests instantiate
the same nested workers with deterministic RPC behavior. The adapter uses
static dispatch and does not create a public mock surface or boxed futures.

### Orphan topology and resolver scheduling

Represent orphan storage by block hash, each block's distinct unresolved
reference set, and a reverse missing-hash-to-dependent-block index.
Keep the focused owner's `resolution_pending` state as a distinct hash set.

When active, rank frontier hashes by the number of distinct stored orphans
transitively blocked behind them, with block hash as a deterministic tie
breaker. Send Resolve requests in that order. This realizes the focused
owner's topology-only maximum-release policy without using arrival time or
consensus scores. Resolve and best-effort Cancel use the same bounded FIFO
mailbox, preserving their single-producer causal order.

DependencyResolver enforces the
[architecture-owned local allowance](../architecture/block-processing.md#dependencyresolver--settled)
with a Tokio semaphore. Tasks waiting for a local permit and tasks holding one
are measured separately. Per-hash cancellation aborts either state. Terminal
Shutdown aborts and joins all tasks before acknowledging.

### Lifecycle-marker worker primitive

Use a Tokio unbounded MPSC channel for the BlockProcessor-owned marker worker.
Its architecture-owned protocol admits at most the two ordered commands for
one session, so the channel is intrinsically bounded by the producer protocol
while keeping command handoff independent of graph-update-channel capacity.
Dropping the sole command sender closes the input and the worker drains
accepted commands. The graph-update producer remains the sole owner of
lossless marker backpressure and receiver-closure classification, and
BlockProcessor joins the worker during terminal session shutdown.

### BlockProcessor owner loop

Keep the one-session BlockProcessor loop in the private
`block_processor::processor` module. Use the same prioritized unbounded command
mailbox and separate bounded Tokio mailboxes for resolved, notification, and
synthetic inputs. Represent Catchup source accounting with a compact bit value
stored by block hash, and expose overlap observation through a cloneable
read-only wrapper around `Arc<AtomicBool>`.

Use private statically dispatched database and RPC adapters for deterministic
owner-loop tests. The production implementations delegate directly to
`ValidatedDbClient` and `ValidatedRpcClient`; the adapters add no boxed future,
public mock API, or alternate runtime behavior. Preserve child-fault provenance
as a typed `Component` field while forwarding it through OrphanManager.

Keep marker-worker receiver closure in a shared atomic flag so a later local
handoff failure can remain distinct from an internal command-path failure. The
flag is diagnostic coordination inside the worker primitive; graph-update gate
state and delivery semantics remain owned by the ingress capability.

## 14 October 2026: VspcProcessor owner loop

Keep the one-session VspcProcessor in the private `vspc_processor` module. Use
an unbounded Tokio command mailbox and separate bounded mailboxes for
notification and synthetic changes; receive BlockProcessor's persisted-block
stream through its separately bounded channel. Drain commands before data and
use one consuming one-shot acknowledgement for terminal shutdown.

Represent pending candidates with monotonically allocated local `u64` keys.
Use hash maps for candidate, missing-hash, and notification-destination
indexes, a `BTreeMap<ConsensusOrder, PendingId>` for resolved notification
selection, and a `VecDeque<PendingId>` for the synthetic FIFO. Keep the two
materialization-history indexes in private hash and ordered maps and update
them within one synchronous worker transition. This is the concrete
implementation of the structures owned by the
[VSPC processing contract](../architecture/vspc-processing.md#pending-state-and-history--settled),
not an additional protocol.

Expose overlap through a cloneable read-only wrapper around `Arc<AtomicBool>`
and pending occupancy through a small mutex-protected metrics snapshot. Use
private statically dispatched database and RPC adapters for focused tests; the
production adapters delegate to the exact `ValidatedDbClient` and
`ValidatedRpcClient` retained by the worker and add no public mock surface or
boxed futures.

## 14 October 2026: API graph model foundation

### Module and collection boundaries

Place canonical graph values and the later `GraphView` implementation in the
public `kgi_api_model::graph` module, canonical changes and `GraphDelta` in the
public `kgi_api_model::delta` module, and retained-history behavior in a public
`kgi_api_model::history` module when that behavior is implemented. Keep public
imports module-qualified and add no crate-root wildcard re-exports. Publication,
runtime, cache, encoding, and transport DTO behavior remains in later modules
and owning crates rather than entering the canonical graph model.

Use a numerically ordered map for retained levels, hash maps for block, edge,
and delta-change identity indexes, and private per-level state for the derived
edge-usage count. Preserve semantic relationship order in immutable
`Arc<[T]>` arrays. These collections are graph-model implementation details;
public access uses values and read-only iteration rather than exporting mutable
collection ownership.

Place `LevelExtent` and `LevelExtentError`, whose semantics are owned by the
[API graph model](../architecture/api-graph.md#graph-values-views-revisions-and-history--settled),
in `kgi_api_model::graph` alongside `GraphView`.

## 16 October 2026: API graph projection reads

### Projection model placement and collections

Place the shared window-anchor, resolution, unavailable-anchor, and graph-seed
values in a public `kgi_api_model::projection` module. Keep imports
module-qualified and add no crate-root re-exports. This module contains owned
cross-crate values only; SQL rows, database errors, transactions, generation
state, and publication runtime state remain in their owning crates.

Represent one detached graph seed with `BTreeMap<u32, Level>` for numerically
ordered levels, `HashMap<BlockHash, GraphBlock>` for blocks, and
`HashMap<EdgeId, GraphEdge>` for edges. These are the same owned collection
shapes accepted by `GraphView::from_database_projection`, so Storage can detach
one coherent projection without an intermediate copy or a second public
container vocabulary. Map iteration order has no graph semantics.

These choices implement the shared-value side of the
[database seed contract](../architecture/api-publication.md#publication-and-seed-values--settled)
and leave the storage operation and validation behavior with the
[API graph projection owner](../architecture/storage.md#api-graph-projection-reads--settled).

## 16 October 2026: API publication mutation kernel

### Module, synchronization, and dependency boundary

Place the publication state, atomic core, writer gate, immutable captures,
lifecycle transitions, and original-update mutation kernel in the private
`kgi_api_core::publication` module. Expose only crate-visible operations needed
by later ApiService modules and add no crate-root re-exports. The increment
uses `tokio::sync::Mutex` and `tokio::sync::RwLock` exactly as selected by the
[publication owner](../architecture/api-publication.md#publication-synchronization-and-request-capture--settled).

Add direct workspace dependencies on `tokio`, `rand`, and `thiserror` alongside
the existing KGI crate dependencies of `kgi-api-core`. Tokio
provides the settled asynchronous locks, `rand` provides publication identity
sampling, and `thiserror` implements the private typed kernel failure. Do not
add a `kgi-core` dependency: publication timing is a monotonic revision clock,
not retry scheduling, and the settled crate dependency direction does not
include that edge.

Sample each publication ID independently from `rand::rngs::OsRng`, retrying
the reserved zero value. Keep the externally observed representation as the
architecture-owned `u64`; `NonZeroU64` may remain a local construction aid but
does not create a second public identity type.

### Canonical delta raw-byte estimate

Estimate one original canonical Head delta with fixed additive weights. The
estimator traverses only mutation collections already present in `GraphDelta`
and collects the distinct hashes that its cacheable wire entry would emit. It
performs no DTO construction, numeric `HashRef` assignment, JSON serialization,
or compression. Use representative five-digit `HashRef` lengths and these
weights:

| Contribution | Estimated bytes |
|---|---:|
| Delta entry and amortized batch framing | 230 |
| Each distinct emitted hash dictionary entry | 68 |
| Block upsert excluding relationship array items | 236 |
| Each direct-parent or merge-set `HashRef` item in a block upsert | 6 |
| Edge upsert | 152 |
| Level installation (`after = Some`) | 68 |
| Level removal (`after = None`) | 38 |
| VSPC-membership mutation | 32 |
| Color mutation | 25 |
| Block or edge removal | 0 |

The structural constants include the approximate compact-JSON field names,
punctuation, scalar values, and fixed `HashRef` occurrences, but not hexadecimal
dictionary strings. The entry base includes its lineage fields and empty
mutation arrays plus an additive share of the outer `GraphDeltaBatchDto`
framing. A temporary `HashSet<BlockHash>` counts each hash once
across block upserts and their relationship arrays, edge upserts, membership
mutations, and color mutations. The 68-byte charge approximates one quoted
64-character hexadecimal hash plus its collection separator. Block and edge
removals neither contribute structure nor enter the set because the settled
delta serializer omits them. Level and field removals remain charged because
those mutations stay on the wire. Level installation and removal use separate
weights because the latter emits `null` instead of a complete `LevelDto`.

The bounded graph level, slot, and level-size fields are JSON numbers. Relative
to their earlier decimal-string representation, the entry framing excludes one
pair of quotes for `head_level`, each block excludes two pairs for its
coordinate, each edge excludes four pairs for its endpoint coordinates, and a
level mutation excludes one or two pairs according to whether it removes or
installs a level.

The estimator remains additive across history entries. Consequently, a hash
repeated by different entries can still be charged once per entry even though
the later batch serializer shares one response-local dictionary. This bounded
cross-entry overestimate avoids constructing the eventual batch dictionary
during publication while removing the much larger within-entry overestimate.

Use checked `u64` arithmetic for every contribution. Exhausting the estimate
is an internal invariant failure under the same physically unattainable
publication-lifetime workload rationale as cumulative history overflow. A
later composed history entry obtains its estimate by adding its constituent
entry estimates rather than re-estimating the composed mutation maps, so
composition cancellation cannot retroactively change planning cost.

## 16 October 2026: graph wire codec foundation

### Module and dependency boundary

Place representation-independent public protocol values in the later public
`kgi_api_model::protocol` module. Place transport DTOs, response-local hash
substitution, and JSON conversion in the private `kgi_api_core::codec` module,
with no crate-root re-exports. Canonical graph and protocol values do not
derive Serde traits and retain no serialization dependency; the codec alone
constructs private serializable representations at the boundary owned by the
[API protocol](../architecture/api-protocol.md#common-transport-dto-rules--settled).

Add direct workspace dependencies on `serde` and `serde_json` to
`kgi-api-core`. Serde derives the private DTO encoders, while `serde_json`
produces the compact JSON bytes and supports exact schema tests. These
dependencies remain absent from `kgi-api-model`.

Add a direct `kaspa-consensus-core` dependency to `kgi-api-core` when status
DTO conversion is introduced. The shared semantic status continues to carry
the `kgi-model`-owned `ValidatedNodeStatus`; the codec names the upstream
`NetworkType` only to exhaustively construct the protocol-owned structural
network value without parsing or depending on a display string. This creates
no dependency on NodeService or `kgi-node`.

### Representation and Head validator identity

Start the internal graph-payload `representation_version` at `1` and store it
as a private `u32`. Changing the version remains governed by the API protocol;
the integer width and initial value are implementation choices and do not add
a public response field.

Represent the protocol-owned semantic Head validator identity with the private
`HeadEtagInput` value. It contains the current representation version,
publication ID, snapshot revision, and one canonical `LevelExtent`. The
eventual validator formatting and HTTP header handling remain outside this
codec increment.

## 17 October 2026: API graph encoding foundation

### Compression implementation

Use `flate2` with gzip compression level `6` for the single settled graph
transfer representation. Set the gzip modification time to zero so equivalent
detached JSON input produces deterministic compressed bytes. This records the
protocol-permitted exact-quality implementation tuning choice without changing
cache identity or protocol semantics.

Place capped JSON serialization and gzip construction in the private
`kgi_api_core::encoding` module. Serialize through a limit-enforcing writer so
the hard uncompressed response bound is detected before allocating an
arbitrarily oversized JSON buffer, and retain the successful compressed body
as shared immutable `bytes::Bytes`. The bounded encoding scheduler introduced
by the same increment remains the sole production caller and the sole
submitter of this work to Tokio's blocking executor.

### Scheduler admission and execution

Represent the encoding scheduler's short synchronous state with one standard
mutex, FIFO queues for priority and historical work, a reservation-ID set, and
an active-job map. Admission starts work directly when an applicable active
slot is free; otherwise it occupies one of the bounded queued-or-reserved
positions. Completing an active job dispatches priority work before historical
work while enforcing the historical sublimit. This avoids a permanent
dispatcher task without changing the limits and priority owned by the
[ApiService task contract](../architecture/api-service.md#cache-and-encoding-jobs).

Track the small async owner of every active blocking call with a scheduler-
private `TaskTracker`. The production blocking adapter delegates only to
`tokio::task::spawn_blocking`; tests replace that adapter with a controlled
executor. Request cancellation or queue timeout removes queued work, while an
already active blocking call remains tracked through completion and its result
is discarded. Terminal shutdown closes the tracker after rejecting admission
and completing queued and active waiters with the shutdown result, then joins
the still-running blocking owners.

### Cache kernel placement and task tracking

Place the publication-local response cache, typed cached values, generic slot
and job primitives, and shared waiter transport in the private
`kgi_api_core::cache` module. `GraphPublication` creates and durably owns its
cache `Arc`; orchestration tasks retain only a weak cache reference and their
exact running job. A small service-level `CacheJobTracker` combines the
`TaskTracker` required by the ApiService contract with the cache-job
cancellation root that the later service lifecycle will own.

Use Tokio `watch` for one retained pending-or-terminal job state. This lets
independent request cancellation detach one receiver, preserves the terminal
result after every current receiver has gone, and avoids a separate waiter
collection. Cache insertion or exact running-slot removal occurs under the
short standard mutex before `send_replace` publishes that terminal value.

## 19 October 2026: API client scheduling foundation

### Module and identifier implementation

Place the publication-local client registry, connection mailbox, and their
private scheduling values in the private `kgi_api_core::client` module. Keep
the serializable SSE payloads in `kgi_api_core::codec::sse`; the scheduling
module composes those DTOs without moving wire representation into registry
state. Add no crate-root re-exports.

Use the `base64` crate's `URL_SAFE_NO_PAD` engine to encode the protocol-owned
random `u64` client value. A small private `ClientIdentifier` temporarily
couples the raw candidate with canonical wire conversion while collision
checking and durable storage remain inside the registry implementation.
Identifier generation accepts `&mut impl RngCore`,
allowing production randomness and deterministic collision fixtures to share
the same path without a global random source or test-only encoding logic.

### Mailbox admission boundary

Represent required-message admission with const-generic fixed arrays. The
caller transfers the complete one- or three-message batch only after one
capacity check under the mailbox mutex, avoiding an intermediate collection
and making partial insertion structurally unavailable. Return a small private
`Closed`/`Full` result so the registry can apply one removal policy while
tests can still distinguish the rejected mailbox condition.

### Registry randomness and deferred disconnects

Use `OsRng` directly for production registration and keep the shared
`register_with` path generic over `RngCore` for deterministic collision tests.
The registry does not retain a random generator or add a second synchronization
domain around it.

Collect cancellation-token clones while holding registry and mailbox locks,
then cancel them only after the registry guard has left scope. This keeps
slow-client and stale-weak cleanup synchronous without allowing cancellation
observers to re-enter registry work under its mutex.

### Registry and mailbox synchronization

Use separate standard-library mutexes for `SseClientRegistryState` and each
`SseMailboxState`. Registry operations are synchronous, bounded by the client
limit, and mutate both publication observation and registration state; a Tokio
mutex would introduce an await into nonawaiting publication tails, while an
`RwLock` or concurrent map would not preserve those compound transitions.
Keep immutable publication identity, publication state, latest Head, and
registrations in one registry state. Registry operations derive publication
identity from that state rather than accepting a caller-supplied value.
Keep mailbox closure and queue contents in one mailbox state so close and
admission cannot linearize independently.

Retain Tokio `Notify` solely as the single receiver's readiness hint. An MPSC
channel would duplicate the authoritative queue and prevent queued-wakeup
coalescing and repositioning, while `watch` would not preserve required-message
order. The receiver registers its notification future before inspecting the
queue and never treats a notification as message state.

For an operation touching both domains, acquire registry state before mailbox
state. The receiver acquires only mailbox state, and disconnect cleanup
acquires only registry state. Publication replacement will acquire two registry
states in stable address order before the shared mailbox; no path acquires a
registry while retaining a mailbox guard. Readiness notification and connection
cancellation occur after releasing the applicable state guards.

## 21 October 2026: API database binding foundation

### Module placement and deterministic generation seam

Place the ApiService-owned latest API database-generation value in the private
`kgi_api_core::api_db` module, with no crate-root re-export. Use the settled
Tokio watch sender as the sole synchronized snapshot owner.

Keep the production type parameter defaulted to
`kgi_storage::generation::ValidatedApiDbClient`, while expressing the one
required observation through a private `ApiDbGeneration` trait. This permits
deterministic unit fixtures for validity and exact `Arc` identity without
adding a public Storage test constructor, opening a PostgreSQL fixture, or
introducing a trait object into the production binding.

### Update linearization

Apply each generation event through `watch::Sender::send_if_modified` rather
than adding an outer mutex beside the settled watch value. The closure performs
the event classification while holding the watch value's write guard and uses
its boolean result to control notification. A captured typed result
distinguishes rejection from an accepted no-op without adding another state
lock. The event semantics are owned by
[API Service](../architecture/api-service.md#api-database-generation-binding--settled).

### Snapshot observation and waiting

Use `watch::Sender::borrow` for the immediate accessor so it allocates no
receiver and retains no binding lock after cloning the selected generation.
The waiting accessor subscribes before its first inspection, then alternates
`borrow_and_update` with `changed`; this closes the inspection-to-suspension
race without polling or exposing a receiver. Keep Rebuild, admission-gate, and
terminal mutations on `send_if_modified` as well, leaving the watch value as
the module's only synchronization domain. The accessor and transition
semantics remain owned by
[API Service](../architecture/api-service.md#api-database-generation-binding--settled).
