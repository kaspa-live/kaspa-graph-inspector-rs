# KGI v2 architecture overview

## Scope and ownership

This document owns the system boundary, component ownership, core crate
structure, direction of control and data flow, and system-wide resource
isolation. Shared identities and graph terminology belong to
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
└── Arc<ApiService> ────────────► HeadGraphCache

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
have lower priority than processing. The [API contract](api.md) owns
graph-update loss, reconstruction, and publication behavior.

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
| ApiService | Graph publications, views, delta history, and graph API serving | [API](api.md) |

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
kgi-api-model   ──► kgi-model
kgi-api-ingress ──► kgi-model
kgi-node        ──► kgi-model
kgi-storage     ──► kgi-model + kgi-api-model
kgi-processing  ──► kgi-model + kgi-api-ingress + kgi-node + kgi-storage
kgi-api-core    ──► kgi-model + kgi-api-model + kgi-api-ingress + kgi-storage
kgi             ──► kgi-model + kgi-api-ingress + kgi-node + kgi-storage
                  + kgi-processing + kgi-api-core
```

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
global shutdown. Migrations, Web assets, and internal module boundaries remain
deferred in the
[decision register](../decisions/deferred.md).

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
[API feed contract](api.md#in-process-api-and-graph-update-feed--settled) owns
graph-update gap signaling. The
[API resource contract](api.md#resource-isolation-and-saturation--settled) owns
the concrete pools, admission lanes, limits, and saturation behavior.

KGI v2 starts with one in-process ApiService and one processing stack. This
shape may later evolve into separate stateless or read-only API replicas with
appropriate cache, proxy, and database scaling; a few thousand concurrent
clients may make that separation useful. Database replication is not required
for v2. The design does not authorize multiple independent processors writing
the same database.

Exact tracing, operational endpoints, and deployment layout remain
deferred in the [decision register](../decisions/deferred.md).
