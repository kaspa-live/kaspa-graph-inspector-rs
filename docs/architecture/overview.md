# KGI v2 architecture overview

## Scope and ownership

This document owns the system boundary, component ownership, core crate and
repository structure, direction of control and data flow, release and
deployment layout, logging, and system-wide resource isolation. Shared
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
services. Its initial `signals` module contains the platform termination
adapter described below.

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

The top `kgi` crate owns Supervisor, process composition, CLI entry points, and
global shutdown and is the sole crate that depends on `kgi-core`. Internal
module boundaries remain deferred in the
[decision register](../decisions/deferred.md).

## Process termination signal adapter — settled

`kgi-core::signals` adapts operating-system termination to a weakly held
shutdown target. Its public contract follows the rusty-kaspa core signal
pattern with KGI-specific installation and repeated-signal behavior:

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
    pub fn init(self: &Arc<Self>) -> Result<(), SignalInstallError>;
}
```

On Unix, the handler recognizes `SIGINT` and `SIGTERM`. On Windows, it
recognizes Ctrl+C. Signal installation reports a typed `SignalInstallError`;
the processing lifecycle owns its startup disposition.

The adapter retains only `Weak<T>` and therefore cannot extend its target's
lifetime. Its callback upgrades that weak reference and invokes
`Shutdown::shutdown`. The concrete target, registration ownership, and target
reaction belong to the
[processing lifecycle](processing-lifecycle.md#termination-triggered-global-shutdown--settled).

The first recognized signal invokes the target once. A second recognized
termination signal before process exit logs forced termination and immediately
exits with failure status. Unlike the rusty-kaspa reference implementation,
which exits on its third callback, KGI deliberately applies forced exit to the
second signal. This is an explicit operator escalation and does not select the
still-deferred automatic shutdown timeouts or timeout-escalation policy. The
registered handler remains alive after the first signal so it can observe that
second signal. It owns no component resource and is process-lifetime
infrastructure rather than a member of the component shutdown order; normal
process termination ends it.

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
independent release version. The binary resolves the standard Web root at
`../share/kgi/web` relative to its real executable location and also accepts
an explicit `--web-root` override. When Web serving is enabled, an invalid or
missing root fails startup rather than exposing an apparently healthy server
without its UI.

## HTTP composition and runtime Web configuration — settled

`kgi-api-core` exposes the Axum router for the
[protocol-owned `/api/v1` surface](api-protocol.md#common-http-conventions--settled).
The top `kgi` crate composes that router with the runtime Web configuration and
static application delivery:

```text
/api/v1/...       kgi-api-core HTTP and SSE
/kgi-config.json  top-crate Web runtime configuration
/assets/...       immutable Vite assets
/*                browser application fallback
```

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

`GET /kgi-config.json` always returns the complete public representation. A
configured template is a valid HTTP or HTTPS URL containing exactly one
`{hash}` placeholder; invalid configuration fails process startup. It is
public presentation configuration and may never contain credentials or expose
environment variables generically. It is owned and served by the top `kgi`
crate and does not belong to `kgi-api-core`, `kgi-api-model`, or
`SystemStatus`.

The operator supplies this value through
`--block-explorer-url-template <template>` or the equivalent
`KGI_BLOCK_EXPLORER_URL_TEMPLATE` environment configuration. Vite never reads
that deployment setting.

Hashed Vite assets use long-lived immutable caching. `index.html` and the
runtime configuration require revalidation; the latter has a configuration
ETag. The [Web contract](web.md#delivery-and-runtime-configuration--settled)
owns browser behavior when consuming the optional value.

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
warning/error records, both with compressed size-based archives. The CLI
exposes `--log-dir`, `--no-log-files`, and
`--log-level`, with equivalent `KGI_LOG_DIR`, `KGI_NO_LOG_FILES`, and
`KGI_LOG_LEVEL` configuration. Supplying a log directory while disabling file
logging is invalid. Exact rotation size and archive count remain deferred.

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
Exact hardening directives and automatic shutdown escalation remain deferred.

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
