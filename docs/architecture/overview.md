# KGI v2 architecture overview

## Scope and ownership

This document owns the system boundary, component ownership, direction of
control and data flow, and system-wide resource isolation. Shared identities
and graph terminology belong to
the [domain model](domain-model.md). Component behavior belongs to the linked
focused document.

KGI v2 covers the processing and API tiers of the Kaspa Graph Inspector Rust
rewrite. `rusty-kaspa` is the node reference. Go KGI and
`simply-kaspa-indexer` are behavioral references, not implementation
templates.

## System shape — settled

```text
Supervisor
├── Arc<NodeService> ───────────► Arc<ValidatedRpcClient> (one connection)
├── Arc<StorageService> ────────► Arc<ValidatedDbClient> (one DB generation)
├── Arc<ResyncEngine>
│   ├── Arc<BlockProcessor>
│   │   ├── OrphanManager
│   │   ├── DependencyResolver
│   │   └── lifecycle-marker worker
│   └── Arc<VspcProcessor>
└── Arc<ApiService> ────────────► HeadGraphCache

NotificationRouter (implements Notify)
├── BlockAdded ────────────────► BlockProcessor notification input
└── VirtualChainChanged ──────► VspcProcessor notification input

BlockProcessor ── PersistedBlock ──► OrphanManager, VspcProcessor
BlockProcessor/VspcProcessor
    ── per-session ordered graph updates ──► ApiService
BlockProcessor ── lifecycle markers ──► ApiService
ResyncEngine ── lifecycle milestones ──► Supervisor
Supervisor ── Reset(fresh graph-update input) / Shutdown ──► ApiService
```

Autonomous long-lived workers form a control tree. `ResyncEngine` owns both
processors and a run's `ProcessingSession`; `BlockProcessor` owns
OrphanManager, DependencyResolver, and its lifecycle-marker worker. Parent
components command their children. Children report reliable faults and
milestones upward. Strong reference cycles are forbidden. `Arc<Component>` is
a valid initial shape; thin handles are not required.

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

## Interaction rules — settled

- Lifecycle control flows from parent to child.
- Reliable faults and milestones flow from child to parent.
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
