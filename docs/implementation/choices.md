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
`kgi-core::timing` supplies the shared clock and jitter mechanism. All
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
envelope with a node-local weak generation target and reason. Its one-shot
barrier ensures ordered retirement is complete before the operation returns
its typed result.

### Deterministic test seams

Define private object-safe `RpcConnector` and `RpcConnection` traits containing
only the operations KGI consumes. The production adapters delegate to
`kaspa-grpc-client` and `kaspa-rpc-core`; unit tests use scripted connections
that return real upstream response value types. This avoids implementing the
complete upstream RPC trait or duplicating a gRPC server while still testing
KGI-owned request construction, normalization, retirement, and ordering.

Inject the shared object-safe `kgi-core::timing` clock and jitter interfaces
through NodeService's private runtime dependencies. Production uses the shared
Tokio clock and entropy-seeded equal-jitter source. Tests provide a manually
advanced clock and scripted jitter, so reconnect slots, the Ready reset
boundary, and shutdown cancellation contain no wall-clock sleeps or
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
definition.

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
inspection, service-loop commands, retirement plumbing, and deterministic
timing in private `database`, `migration`, `schema`, `state`, `runtime`, and
`timing` modules. Expose the public modules without crate-root wildcard
re-exports.

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
that settled complexity boundary.

### PostgreSQL integration tests

Use `testcontainers-modules` with the official `postgres:17-alpine` image and
SQLx itself as the test client. Each fixture receives an isolated temporary
database and requires no ambient `DATABASE_URL`. Keep the image tag explicit
so a routine dependency or image update cannot silently change database
behavior. Tests use runtime SQL APIs, run through Nextest, and retain container
handles until every database assertion and cleanup barrier has completed.
