# KGI v2 architecture overview

## Scope and ownership

This document owns the system boundary, component ownership, core crate and
repository structure, direction of control and data flow, process
configuration and command entry, release and deployment layout, logging, and
system-wide resource isolation. Shared
identities and graph terminology belong to
the [domain model](domain-model.md). Component behavior belongs to the linked
focused document.

KGI v2 covers the processing and API tiers of the Kaspa Graph Inspector Rust
rewrite. `rusty-kaspa` is the node reference. Go KGI and
`simply-kaspa-indexer` are behavioral references, not implementation
templates.

## System shape — settled

```text
kgi (binary and composition root)
└── Supervisor (control tree below)

Supervisor
├── Arc<NodeService> ───────────► Arc<ValidatedRpcClient> (one connection)
├── Arc<StorageService>
│   ├───────────────────────────► Arc<ValidatedDbClient> (one processing generation)
│   └───────────────────────────► Arc<ValidatedApiDbClient> (one API generation)
├── Arc<ResyncEngine>
│   ├── Arc<BlockProcessor>
│   │   ├── OrphanManager
│   │   ├── DependencyResolver
│   │   └── lifecycle-marker worker
│   └── Arc<VspcProcessor>
└── Arc<ApiService> ────────────► GraphPublication
                                  ├── Head GraphView / GraphHistory
                                  └── publication-scoped GraphCache

NotificationRouter (implements Notify)
├── BlockAdded ────────────────► BlockProcessor notification input
└── VirtualChainChanged ───────► VspcProcessor notification input

BlockProcessor ── PersistedBlock ──► OrphanManager, VspcProcessor
BlockProcessor/VspcProcessor
    ── per-session ordered graph updates ──► ApiService
BlockProcessor ── lifecycle markers ──► ApiService
ResyncEngine ── lifecycle milestones ──► Supervisor
NodeService ── ordered RPC generation events ──► Supervisor
StorageService ── ordered DB generation events ──► Supervisor
Supervisor ── update API DB generation / reset / shutdown ──► ApiService
```

The top `kgi` crate is the binary, composition root, and home of Supervisor. It
constructs the long-lived components and gives Supervisor their `Arc` values.
Autonomous long-lived workers form a control tree. `ResyncEngine` owns both
processors and a run's `ProcessingSession`; `BlockProcessor` owns
OrphanManager, DependencyResolver, and its lifecycle-marker worker. Supervisor
invokes public control methods on the four components it manages. Internal
owners may retain explicit child-worker command protocols. Children report
reliable faults and milestones upward. Strong reference cycles are forbidden.
`Arc<Component>` is the settled Supervisor-facing shape; separate control
handles are unnecessary.

Every worker serializes its local state changes in one event loop despite
concurrent inputs. Live ingestion remains subscription-based. Notifications
travel directly from NotificationRouter to the processors and never pass
through ResyncEngine.

ApiService is an in-process, derived read-model service. Its work and freshness
have lower priority than processing. [API ingress](api-ingress.md) owns
graph-update loss reporting, the [API graph model](api-graph.md) owns the
derived graph state, and [API graph publication](api-publication.md) owns
reconstruction and publication behavior.

## Responsibility boundaries — settled

| Component | Owned state and responsibility | Focused contract |
|---|---|---|
| Supervisor | Orchestration state and the strongest unsatisfied recovery obligation | [Processing lifecycle](processing-lifecycle.md) |
| NodeService | Node connectivity, validated RPC generations, and notification routing | [Node service](node-service.md) |
| StorageService | Database connectivity, validated DB generations, persistence, and caches | [Storage](storage.md) |
| ResyncEngine | Processing sessions, recovery preparation, synchronization, and phase coordination | [Processing lifecycle](processing-lifecycle.md) |
| BlockProcessor | Block admission and materialization coordination | [Block processing](block-processing.md) |
| OrphanManager | In-memory orphan topology and dependency demand | [Block processing](block-processing.md) |
| DependencyResolver | Bounded node retrieval for requested dependencies | [Block processing](block-processing.md) |
| VspcProcessor | VSPC sequencing, readiness, and coloring coordination | [VSPC processing](vspc-processing.md) |
| ApiService | Graph publications, views, delta history, control, and graph API serving | [API graph model](api-graph.md), [API graph publication](api-publication.md), [ApiService](api-service.md), [API protocol](api-protocol.md) |

NodeService, StorageService, ResyncEngine, and Supervisor each own their
respective service, processing, or orchestration state. Published statuses are
observations of those owners, never a second source of lifecycle authority.

## Core crate structure — settled

Architectural contract ownership and Rust crate placement are independent.
Focused architecture documents remain the semantic owners of their contracts
even when a value type lives in a shared crate.

The core crate structure fixes these acyclic boundaries. An arrow points from
a crate to one of its dependencies:

```text
kgi-core
kgi-model
kgi-api-model   ──► kgi-model
kgi-api-ingress ──► kgi-model
kgi-node        ──► kgi-model
kgi-storage     ──► kgi-model + kgi-api-model
kgi-processing  ──► kgi-model + kgi-api-ingress + kgi-node + kgi-storage
kgi-api-core    ──► kgi-model + kgi-api-model + kgi-api-ingress + kgi-storage
kgi             ──► kgi-core + kgi-model + kgi-api-ingress + kgi-node + kgi-storage
                  + kgi-processing + kgi-api-core
```

`kgi-core` has no dependency on another KGI crate. It contains reusable
process-level infrastructure that is outside the domain model and component
services. Its initial `config` module contains the process configuration value
structs. Its initial `signals` module contains the platform termination adapter
described below.

`kgi-model` contains shared domain values and cross-component message values,
including `RecoveryMode`, `ParentCommitted`, `LevelCommitted`,
`BlockCommitted`, `VspcCommitted`, `GraphUpdate`, and the component status
values. Their focused component documents still own their semantics.

`kgi-api-model` is a pure graph-projection contract crate. It contains shared
API graph values and request/result values such as `Level`, `GraphBlock`,
`EdgeId`, `GraphEdge`, `GraphWindowAnchor`, `GraphViewSeedRequest`,
`GraphWindowAnchorUnavailable`, `GraphWindowResolution`, `GraphViewSeed`,
`GraphViewSeedOutcome`, `SystemStatus`, and public delta value shapes. It has no
service workers, database implementation, HTTP server, channel runtime, or
PostgreSQL types.

`kgi-api-ingress` owns the graph-update ingress: `GraphUpdateProducer`,
`GraphUpdateReceiver`, `GraphUpdateGate`, gap signaling, bounded-channel
construction, and producer-gate behavior. It depends on the async runtime and
`kgi-model`, but not on `kgi-api-model`, `kgi-storage`, or `kgi-api-core`.

`kgi-node` owns NodeService, `ValidatedRpcClient`, `NodeServiceEvent`,
NotificationRouter, RPC normalization, and subscription handling. It depends
on `kgi-model` and the node/RPC libraries, but not on `kgi-processing`, storage,
or either API crate.
The composition root constructs the processor notification channels and passes
only their sender handles to `kgi-node`. Their payload types belong to
`kgi-model`, so `kgi-node` does not depend on concrete processor types from
`kgi-processing`.

`kgi-storage` implements StorageService, validated database clients,
persistence, `StorageServiceEvent`, and the API projection read against
`kgi-api-model` contracts. It must not depend on `kgi-api-core` or
`kgi-api-ingress`. `kgi-api-core` owns ApiService,
`ApiDbGenerationEvent`, graph publication behavior, HTTP/SSE, and API runtime
state.
`kgi-processing` owns ResyncEngine and the processing workers, consumes the
graph-update producer, and does not depend on `kgi-api-core`.

The top `kgi` crate owns Supervisor, process composition, CLI command dispatch,
and global shutdown and is the sole crate that depends on `kgi-core`. Internal
module boundaries remain deferred in the
[decision register](../decisions/deferred.md).

## Process configuration and command entry — settled

`kgi-core::config` owns only the configuration value structs. The top `kgi`
crate owns CLI argument and subcommand shapes, configuration loading, source
precedence, resolution, static validation, command dispatch, and process
startup. It runs any component-owned semantic validator required after static
resolution and distributes only the resolved values each component requires.
No component receives the complete process configuration.

The resolved configuration has this semantic shape:

```rust
struct KgiConfig {
    network: NetworkConfig,
    node: NodeConfig,
    database: DatabaseConfig,
    http: HttpConfig,
    logging: LoggingConfig,
    web: WebConfig,
}
```

`KgiConfig` and its section types belong to `kgi-core::config`.

The concrete field types are parsed values suitable for their consumers, such
as `NetworkId`, socket addresses, URLs, and paths, rather than unvalidated
strings. `WebConfig` includes the effective Web root and the optional
block-explorer URL template.

### Sources and precedence

Configuration resolves once with this precedence, from lowest to highest:

```text
compiled defaults < explicitly selected TOML < environment < explicit CLI
```

Only a value explicitly present in a higher source replaces a lower value.
The operator selects a TOML file with `--config-file <path>` or
`KGI_CONFIG_FILE`; the CLI value wins when both are present. KGI loads no
implicit configuration file. A selected file that is missing, unreadable,
invalid UTF-8, or invalid TOML is a configuration error.

Every TOML-backed configuration struct applies the equivalent of:

```rust
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
```

The representation therefore uses kebab-case field names, defaults absent
fields, and rejects unknown fields at every nesting level. Environment and CLI
parsing are equally strict; an invalid explicitly supplied value is never
treated as absent and never falls back to a lower source. The fully resolved
`KgiConfig` is immutable for the process lifetime. KGI provides no live reload,
environment interpolation, or generic secret-provider abstraction in v2.

The supported configuration surface is:

| Value | TOML | Environment | CLI |
|---|---|---|---|
| Mainnet selector | `network.mainnet` | `KGI_MAINNET` | `--mainnet` |
| Testnet selector | `network.testnet` | `KGI_TESTNET` | `--testnet` |
| Devnet selector | `network.devnet` | `KGI_DEVNET` | `--devnet` |
| Simnet selector | `network.simnet` | `KGI_SIMNET` | `--simnet` |
| Testnet suffix | `network.netsuffix` | `KGI_NETSUFFIX` | `--netsuffix` |
| Consensus override file | `network.override-params-file` | `KGI_OVERRIDE_PARAMS_FILE` | `--override-params-file` |
| Node RPC URL | `node.rpc-url` | `KGI_NODE_RPC_URL` | `--node-rpc-url` |
| Database URL | `database.url` | `KGI_DATABASE_URL` | `--database-url` |
| First initialization authorization | `database.initialize` | `KGI_INITIALIZE_DB` | `--initialize-db` |
| HTTP listen address | `http.listen` | `KGI_HTTP_LISTEN` | `--http-listen` |
| Log level | `logging.level` | `KGI_LOG_LEVEL` | `--log-level` |
| Log directory | `logging.directory` | `KGI_LOG_DIR` | `--log-dir` |
| Disable file logging | `logging.no-files` | `KGI_NO_LOG_FILES` | `--no-log-files` |
| Web root | `web.root` | `KGI_WEB_ROOT` | `--web-root` |
| Block-explorer URL template | `web.block-explorer-url-template` | `KGI_BLOCK_EXPLORER_URL_TEMPLATE` | `--block-explorer-url-template` |

A complete TOML file may therefore have this shape; every section and every
field remains optional except that the final resolved database URL is
required:

```toml
[network]
testnet = true
netsuffix = 10
# override-params-file = "/etc/kgi/devnet-params.json"

[node]
rpc-url = "grpc://127.0.0.1:16210"

[database]
url = "postgresql://kgi@127.0.0.1/kgi"
initialize = false

[http]
listen = "127.0.0.1:8080"

[logging]
level = "info"
directory = "./logs"
no-files = false

[web]
# root = "/opt/kgi/share/kgi/web"
# block-explorer-url-template = "https://explorer.example/blocks/{hash}"
```

The destructive service-start token deliberately has no TOML form. It is
accepted only as `KGI_REINITIALIZE_DB_TOKEN` or
`--reinitialize-db-token <token>`. `--clear-db` is CLI-only. `--yes` is
accepted only by the one-shot administrative command defined below. The
[storage contract](storage.md#database-bootstrap-and-validation--settled) owns
the behavior authorized by these values.

### Network selection and defaults

The four network selectors are one precedence group. Within a source, at most
one may be true. The highest-precedence source containing one true selector
selects the complete network family; selectors from lower sources do not
participate. With no selector, KGI uses mainnet. This permits an explicit
`--mainnet` to replace a lower-source non-mainnet selection.

`netsuffix` follows ordinary field precedence. It is valid only with testnet
and defaults to `10` when testnet is selected. Any suffix with mainnet,
devnet, or simnet is invalid. The result is one exact `NetworkId` including
the testnet suffix.

When `node.rpc-url` is absent, KGI derives the loopback gRPC endpoint from the
selected network type using rusty-kaspa's default RPC port:

```text
mainnet     grpc://127.0.0.1:16110
testnet-*   grpc://127.0.0.1:16210
simnet      grpc://127.0.0.1:16510
devnet      grpc://127.0.0.1:16610
```

An explicit RPC URL replaces that default but does not weaken NodeService's
exact connected-network validation. The database URL has no default and is
required. The remaining defaults are:

```text
http.listen       = 127.0.0.1:8080
logging.level     = info
logging.directory = ./logs
logging.no-files  = false
web.root          = derived from the executable's release layout
web.block-explorer-url-template = absent
```

The Docker configuration explicitly replaces the HTTP address with
`0.0.0.0:8080` and the log directory with its mounted `/var/log/kgi` path.
Conventional service packaging likewise selects `/var/log/kgi`; the compiled
default remains suitable for an interactive local invocation.

### Command surface and startup-only actions

The command surface is:

```text
kgi [service options]
kgi database reinitialize [administrative options]
```

There is no `run` or `serve` subcommand. Global configuration-file selection
and the network, node, database, logging, and consensus-override inputs needed
by the administrative operation remain available to
`database reinitialize`. That command alone accepts `--yes`. It constructs
only the bounded RPC and database capabilities required by the
[storage-owned administrative transaction](storage.md#administrative-reinitialization--settled);
it does not start NodeService, StorageService, ApiService, the HTTP server,
processors, ResyncEngine, or Supervisor. It executes once and exits: a
definite successful commit exits successfully, while cancellation, rejection,
definite failure, and ambiguous commit exit nonzero. A later ordinary service
invocation observes the resulting network-bound Empty database and follows
the normal recovery lifecycle.

The persistent initialization authorization is available through every
ordinary configuration source in the table. `--clear-db` instead expresses
one service invocation's initial Rebuild intent, remains retained across
retries in that process, and also authorizes first initialization when
applicable. The declarative reinitialization token performs its storage-owned
idempotent check during service startup and, after either performing or
skipping replacement, continues ordinary startup. Initialization,
`--clear-db`, and a reinitialization token are mutually exclusive.
KGI exposes no migration toggle: StorageService automatically applies every
supported older-v2 migration under its
[schema lifecycle](storage.md#database-bootstrap-and-validation--settled).

### Validation, startup ordering, and failures

Process entry follows this order:

```text
parse command shape
    -> help/version may exit successfully without configuration
load explicitly selected TOML
    -> apply environment
    -> apply explicit CLI
    -> resolve defaults and derived values
    -> validate the complete invocation and KgiConfig
    -> initialize logging
    -> branch to service startup or the one-shot administrative command
```

Before complete static validation succeeds, KGI performs no node or database
connection, HTTP bind, logging-file mutation, signal installation, or
component startup. Configuration diagnostics produced before logging
initialization go directly to standard error. Static validation rejects:

- a selected configuration file or explicit value that cannot be parsed;
- an invalid network-selector group or suffix combination;
- a missing or syntactically invalid database URL;
- a syntactically invalid node RPC URL or HTTP listen address;
- an explicitly configured log directory together with `logging.no-files =
  true`; the unused compiled default directory does not make `no-files = true`
  invalid;
- an invalid combination of startup-only database actions;
- `--yes` outside `database reinitialize`; and
- a Web runtime template that violates the
  [runtime Web configuration contract](#http-composition-and-runtime-web-configuration--settled).

The resolved consensus override setting is additionally subject to the
[NodeService-owned network, file, and parameter validation](node-service.md#consensus-parameter-resolution)
before any component starts. The configuration layer does not duplicate those
domain checks.

During service startup, failure to initialize the selected log destination,
resolve the effective Web root, or bind the HTTP listener is a startup failure.
Temporary node and database unavailability belongs to the existing service
retry lifecycles rather than configuration validation; terminal service
rejection belongs to Supervisor's Fatal lifecycle. Ordinary service shutdown
exits successfully, while startup failure, terminal runtime failure, and
unsuccessful administrative execution exit nonzero. Argument-parser help and
version output exit successfully; its usage and configuration errors use its
ordinary nonzero result.

Database URLs and reinitialization tokens are sensitive. KGI supports the
database URL through all three operator sources, while production deployments
should prefer `KGI_DATABASE_URL` or a protected mounted TOML file over a
process-visible CLI argument. The token remains environment-or-CLI only. No
log, status value, confirmation, error, panic, or unrestricted `Debug` output
may expose either value. Diagnostics may name the field and source and may
show a redacted database host, port, and database name, but never credentials,
username, query parameters, full URL, or token; parsing errors do not echo the
rejected value. StorageService retains the resolved database connection
configuration needed for autonomous reconnection. Credential rotation
requires process restart.

## Process termination signal adapter — settled

`kgi-core::signals` adapts operating-system termination to a weakly held
shutdown target. Its public contract follows the rusty-kaspa core signal
pattern:

```rust
pub trait Shutdown {
    fn shutdown(self: &Arc<Self>);
}

pub struct Signals<T: Shutdown + Send + Sync + 'static> {
    target: Weak<T>,
    iterations: AtomicU64,
}

impl<T: Shutdown + Send + Sync> Signals<T> {
    pub fn new(target: &Arc<T>) -> Self;
    pub fn init(self: &Arc<Self>);
}
```

On Unix, the handler recognizes `SIGINT` and `SIGTERM`. On Windows, it
recognizes Ctrl+C. `init()` installs the handler with `ctrlc::set_handler` and
calls `.expect("Error setting signal handler")`; installation failure therefore
panics. The processing lifecycle owns installation timing.

The adapter retains only `Weak<T>` and therefore cannot extend its target's
lifetime. Its callback upgrades that weak reference and invokes
`Shutdown::shutdown`. The concrete target, registration ownership, and target
reaction belong to the
[processing lifecycle](processing-lifecycle.md#termination-triggered-global-shutdown--settled).

Each callback atomically increments `iterations` with `Ordering::SeqCst`. The
first and second callbacks print `^SIGTERM - shutting down...`, upgrade the
weak target, and invoke `Shutdown::shutdown` when that target still exists.
They have no debounce, delay, grace timer, or elapsed-time threshold. The third
callback prints `^SIGTERM - halting` and immediately calls
`std::process::exit(1)` before inspecting the target.

The registered handler remains alive after shutdown is first requested so it
can observe subsequent signals. It owns no component resource and is
process-lifetime infrastructure rather than a member of the component shutdown
order; normal process termination ends it.

## Repository, Web build, and release structure — settled

The repository is one virtual Cargo workspace. Product crates live below
`crates/`; build orchestration lives outside the production dependency graph;
the browser application is a sibling frontend project under `web/`:

```text
kaspa-graph-inspector-rs/
├── .cargo/config.toml
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── LICENSE
├── AGENTS.md
├── crates/
│   ├── kgi/
│   ├── kgi-core/
│   ├── kgi-model/
│   ├── kgi-api-model/
│   ├── kgi-api-ingress/
│   ├── kgi-node/
│   ├── kgi-storage/
│   │   └── migrations/
│   ├── kgi-processing/
│   └── kgi-api-core/
├── tools/xtask/
├── web/
│   ├── package.json
│   ├── package-lock.json
│   ├── vite.config.ts
│   ├── vitest.config.ts
│   ├── playwright.config.ts
│   ├── public/
│   ├── src/
│   └── tests/
├── fixtures/
│   ├── node/
│   ├── storage/
│   ├── graph/
│   └── web/
├── deploy/
│   ├── docker/
│   │   ├── Dockerfile
│   │   ├── compose.example.yaml
│   │   └── compose.dev.yaml
│   └── systemd/
│       ├── kgi.service
│       └── kgi.env.example
├── scripts/
└── docs/
```

`crates/kgi-storage/migrations/` is the sole migration-file location because
StorageService owns schema and migration execution. Shared stable fixtures
live below the repository-level `fixtures/`; test logic remains with the
component that owns the behavior. The [verification contract](verification.md)
owns the exact Web test runners and placement.

The KGI v2 browser application retains the KGI v1 React, TypeScript, MUI,
Emotion, PixiJS, React Spring, UI, and visualization code wherever compatible.
During import, Vite replaces the deprecated Create React App build layer as an
isolated tooling change. Adapting the imported application to the v2 HTTP,
SSE, publication, and graph contracts is separate work. Generated
`target/`, root `dist/`, `web/dist/`, `web/node_modules/`, coverage, and browser
test-report directories are untracked.

The repository supplies a Cargo alias for the tooling command:

```text
cargo xtask bundle
cargo xtask bundle --target <rust-target>
cargo xtask bundle --profile <cargo-profile>
```

`tools/xtask` is a workspace tooling crate, not a production dependency. The
default bundle profile is `release`. The bundle command uses `npm ci`, the
committed npm lockfile, `cargo build --locked`, and the committed Cargo
lockfile. It builds the Web assets and `kgi` binary before
constructing a temporary release directory, copies the complete matching
outputs, adds the license and minimal release provenance, and exposes the
final versioned directory only through an atomic same-filesystem rename. A
failure leaves no final partial bundle. The command packages only; it does not
deploy, upload, publish, or alter a running installation.

The portable bundle layout is:

```text
dist/kgi-<version>-<target>/
├── bin/kgi
├── share/kgi/web/
├── LICENSE
└── release.json
```

`release.json` records the KGI package version, full source commit, and target.
The workspace package version is authoritative; the private Web package has no
independent release version. In the absence of a configured `web.root`, the
binary derives the standard Web root at `../share/kgi/web` relative to its real
executable location. The
[process configuration contract](#process-configuration-and-command-entry--settled)
owns the override's operator sources. When Web serving is enabled, an invalid
or missing effective root fails startup rather than exposing an apparently
healthy server without its UI.

## HTTP composition and runtime Web configuration — settled

`kgi-api-core` depends on Axum and exposes the routes for the
[protocol-owned `/api/v1` surface](api-protocol.md#common-http-conventions--settled)
as a router awaiting its application state:

```rust
pub fn router() -> axum::Router<Arc<ApiService>>;
```

`Arc<ApiService>` is Axum application state rather than a request extension.
The top `kgi` crate owns that Arc, nests the API router at `/api/v1`, composes
the runtime Web configuration and static application delivery, and calls
`with_state(api_service)` once on the completed router before serving it.
The resulting route ownership is:

```text
/api/v1/...       kgi-api-core HTTP and SSE
/kgi-config.json  top-crate Web runtime configuration
/assets/...       immutable Vite assets
/*                browser application fallback
```

The API router owns an explicit protocol-compliant fallback and method
handling. An unknown or unsupported `/api/v1` request therefore produces the
API-owned `404` or `405` response and can never fall through to the browser
application.

The top crate uses Tower's `ServiceBuilder` for explicit middleware ordering
and applies one outer `tower_http::trace::TraceLayer` to the completed HTTP
application. Exact span fields, logging callbacks, and metrics integration
remain implementation choices under the
[observability deferral](../decisions/deferred.md). It uses
`tower_http::services::ServeDir` for immutable Vite assets and
`tower_http::services::ServeFile` for the browser-application fallback.

KGI installs no global `CompressionLayer`, `TimeoutLayer`,
`ConcurrencyLimitLayer`, `CorsLayer`, `CatchPanicLayer`, or request-body
transformation layer. Graph compression is the API protocol's explicit
single-representation path. Timeouts, concurrency, admission, cancellation,
and panic disposition retain their focused owners and cannot be replaced by a
router-wide default. SSE therefore inherits tracing but no ordinary HTTP
timeout, concurrency, or compression policy. Production uses one origin and
needs no CORS policy.

Axum, Tower, and tower-http crate versions are implementation dependency
choices. A version change is architectural only when it changes one of the
settled behaviors above.

The top `kgi` crate's Supervisor owns the bound listener and Axum server task.
ApiService owns `/api/v1` admission, request work, response delivery tracking,
and SSE connection state after routing; it does not own the listener or static
Web requests. An ordinary ApiService reset changes publication registrations
without restarting the router, listener, HTTP request infrastructure, or SSE
connections. Terminal server failure and graceful-shutdown ordering belong to
the [Supervisor lifecycle](processing-lifecycle.md#teardown-and-delivery-semantics--settled).

The production browser and API share one origin. Browser code uses the fixed
relative `/api/v1` contract and obtains its own site identity from
`window.location.origin`; neither value is deployment-specific Vite input.
The build is deployment-neutral and is never rebuilt for a node, network, or
installation.

The only current runtime Web value is:

```rust
struct WebRuntimeConfig {
    block_explorer_url_template: Option<String>,
}
```

`GET /kgi-config.json` always returns the complete public representation. The
exact absent form and one configured example are:

```json
{"block_explorer_url_template":null}
{"block_explorer_url_template":"https://explorer.example/block/{hash}"}
```

The exact property name is `block_explorer_url_template`; it is always present
and is the object's only property. `None` encodes as JSON `null`, never as an
omitted property. A configured value encodes as a JSON string with ordinary
JSON escaping. Serialization adds no insignificant whitespace or trailing
newline.

A configured template is a valid HTTP or HTTPS URL containing exactly one
`{hash}` placeholder. It is public presentation configuration and may never
contain credentials or expose environment variables generically. It is owned
and served by the top `kgi` crate and does not belong to `kgi-api-core`,
`kgi-api-model`, or `SystemStatus`. The
[process configuration contract](#process-configuration-and-command-entry--settled)
owns its operator sources and startup validation. Vite never reads that
deployment setting.

Every body-bearing successful response is uncompressed and carries:

```http
Content-Type: application/json
Cache-Control: no-cache
ETag: "kgi-config-<sha256>"
```

`<sha256>` is the 64-character lowercase hexadecimal SHA-256 of the exact
UTF-8 response body. This is a strong ETag: the same public representation has
the same validator across process restarts, while a different representation
has a different validator. The endpoint has no content negotiation or
compression path: it always returns JSON regardless of `Accept`, ignores
`Accept-Encoding`, and emits no `Content-Encoding`.

`If-None-Match` uses the standard weak comparison required for GET. No current
match returns `200 OK` with the complete representation. A matching list
member or `*` returns `304 Not Modified` with the current `ETag` and
`Cache-Control: no-cache`, no body, and no `Content-Type` or
`Content-Encoding`. Malformed conditional headers follow ordinary HTTP header
rejection rather than changing configuration semantics.

Hashed Vite assets use long-lived immutable caching. `index.html` and the
runtime configuration require revalidation. The
[Web contract](web.md#delivery-and-runtime-configuration--settled) owns browser
behavior when consuming the optional value.

## Deployment and logging — settled

Docker and conventional installation use the portable bundle without
embedding Web assets in the executable. The Docker image installs it under
`/opt/kgi`, runs one unprivileged KGI process, exposes one HTTP listener, sends
console logs to stdout/stderr, and delivers termination directly to the
[process signal adapter](#process-termination-signal-adapter--settled)
through its exec-form entry point. PostgreSQL and the rusty-kaspa node remain
external.
The release tree is read-only.

KGI also writes bounded rotating files by default, following the rusty-kaspa
operational shape: `kgi.log` is the complete log and `kgi_err.log` contains
warning/error records, both with compressed size-based archives. Logging
operator sources and validation belong to the
[process configuration contract](#process-configuration-and-command-entry--settled).
Exact rotation size and archive count remain deferred.

The standard mutable log path is `/var/log/kgi`; conventional packages create
it for the `kgi` service user and containers mount it as a writable log volume.
Multiple instances use distinct configured log directories. Console logging
continues while file logging is active. KGI needs no local volume for
correctness state because authoritative graph state is in PostgreSQL, but the
log volume preserves operational history across replacement.

The conventional layout is:

```text
/opt/kgi/releases/<version-target>/
├── bin/kgi
├── share/kgi/web/
├── LICENSE
└── release.json

/opt/kgi/current -> releases/<version-target>
/etc/kgi/
/var/log/kgi/
```

The supplied systemd unit runs as the `kgi` user, reads
`/etc/kgi/kgi.env`, starts `/opt/kgi/current/bin/kgi`, restarts on failure, and
delivers `SIGTERM` for graceful shutdown through the process-termination
[adapter](#process-termination-signal-adapter--settled) and
[Supervisor lifecycle](processing-lifecycle.md#termination-triggered-global-shutdown--settled).
Exact hardening directives and any external service-manager stop timeout remain
deployment choices.

An upgrade extracts a new immutable release, stops KGI and awaits graceful
shutdown, atomically replaces `current`, and starts the new release. Stopping
before the switch prevents the old binary from serving new assets. Resolving
the executable's real path keeps a running process bound to its own matching
asset directory. Binary-and-Web rollback is separate from database migration
compatibility and never promises schema rollback.

The Docker image uses the equivalent `/opt/kgi/bin/kgi` and
`/opt/kgi/share/kgi/web` layout and exposes port `8080`. It stores no
credentials, operator configuration, initialization authorization,
reinitialization token, or consensus override in the image. Read-only
configuration and secrets are supplied at runtime. A persistent
reinitialization token retains the
[storage-owned idempotence semantics](storage.md#administrative-reinitialization--settled)
across container replacement.

The development Compose file may provide PostgreSQL and other local
dependencies. The production image has no requirement that PostgreSQL or the
node share its container, Compose project, or host.

The database ownership lock permits only one active KGI writer. Container and
service upgrades therefore stop the old process before starting the new one;
an overlapping rolling or blue-green replacement is invalid. External reverse
proxy TLS preserves the same-origin contract and is the initial production
shape; direct KGI TLS is not required. Exact liveness/readiness endpoints stay
deferred, and `/api/v1/status` proves HTTP reachability rather than processing
readiness.

## Interaction rules — settled

- Supervisor invokes public control methods on its managed `Arc<Component>`
  values; their private mailbox or event-loop implementation is not an
  architectural interface.
- Internal lifecycle control flows from parent to child.
- Reliable faults and milestones flow from child to parent.
- Managed sibling components do not acquire one another's validated service
  generations. NodeService publishes the RPC generation lifecycle and
  StorageService publishes both DB generation lifecycles to Supervisor.
  Supervisor retains the current RPC and processing DB generations only for a
  future `ProcessingSession` and maps API-generation events through
  ApiService's public control surface.
- Data channels connect the explicit producers and consumers shown above;
  they do not create lifecycle ownership.
- The exact validated RPC and DB generations acquired for a processing run
  stay associated with that run.
- StorageService may open, lock, and inspect an Uninitialized database before
  NodeService is Ready. Supervisor supplies the validated
  `(network_id, genesis_hash)` only for StorageService's atomic first
  initialization; the resulting network-bound Empty database is the first
  usable state.
- Before starting a processing run, Supervisor requires an exact match between
  the validated node identity and the immutable binding exposed by the
  validated DB generation. A mismatch is rejected rather than rebound or
  recovered through Rebuild.
- Processing commits precede their graph updates. API projection behavior
  cannot redefine processing commit semantics.
- Web is an API consumer outside the worker control tree; its behavior is
  defined in the [Web architecture](web.md).

## Resource isolation and scalability — settled

Processing has reserved database connections and execution capacity and keeps
priority over every read-only API workload. API projection failure never
becomes processing recovery. The
[API feed contract](api-ingress.md#in-process-graph-update-feed--settled) owns
graph-update gap signaling. The
[API resource contract](api-service.md#resource-isolation-and-saturation--settled) owns
the concrete pools, admission lanes, limits, and saturation behavior.

KGI v2 starts with one in-process ApiService and one processing stack. This
shape may later evolve into separate stateless or read-only API replicas with
appropriate cache, proxy, and database scaling; a few thousand concurrent
clients may make that separation useful. Database replication is not required
for v2. The design does not authorize multiple independent processors writing
the same database.

Exact tracing and operational endpoints remain deferred in the
[decision register](../decisions/deferred.md).
