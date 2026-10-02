# Deferred implementation decisions

These choices are deliberately left to implementation work. Implementations
must stay within the linked settled architecture and record choices where they
become durable project constraints.

1. Cargo workspace and module layout outside the settled
   [core crate structure](../architecture/overview.md#core-crate-structure--settled),
   including migrations, Web assets, and internal module boundaries. The top
   binary/composition crate is the settled `kgi` crate.
2. PostgreSQL Rust client, migration framework, and concrete SQL types.
3. Exact capacities for processor channels, the per-session graph-update
   channel and ApiService staging buffer, orphan and VSPC pending memory,
   DependencyResolver and RPC concurrency, and HTTP
   work, plus concrete cache, destination-heat, and client-wake registry
   collections and raw-byte estimator weights. Local RPC
   scheduling and batching remain implementation choices only where the
   focused architecture does not fix request boundaries or batch semantics.
   Moka is the current cache-library candidate.
4. Orphan occupancy threshold within the settled range of approximately one
   quarter through one third.
5. Detailed Tokio fairness, drain, graph-update gap counter/wakeup primitives,
   per-client revision-wakeup scheduling primitives,
   component-status watch primitives, BlockProcessor marker-worker primitives,
   ApiService task structure, and concrete storage synchronization mechanics.
   A shared mutation lock plus per-lane mutexes is one valid storage shape;
   exact lock types remain an implementation choice.
6. The remaining endpoint-specific public transport DTOs, wire and error-body
   schemas, and the exact representation of the settled operations under the
   common DTO rules in the
   [API protocol](../architecture/api-protocol.md). Select exactly one graph
   response format for v2; clients will not negotiate among graph encodings.
   Benchmark JSON against appropriate binary formats such as CBOR, MessagePack,
   and Protobuf across server construction and serialization, compression,
   transfer, browser decoding, and graph-model construction. Select the HTTP
   compression policy separately.
7. Exact `MAX_WINDOW_DEPTH` and `MAX_CACHE_LEVEL_DISTANCE` under the settled
   strict sum bound beneath `MAX_CACHE_DEPTH = 1000`; the soft estimated and
   hard encoded response budgets; HTTP and SSE budgets; and the adaptive
   fixed-view delay curve and cap. Treat
   traffic-share estimates and the numeric SSE-client
   limit as load-test inputs rather than fixed architecture constants.
8. Detailed shared/exclusive permit, historical-read cancellation, and
   transaction mechanism for StorageService's database-replacement gate. Safe
   transaction locking is one candidate; another mechanism is acceptable when
   it preserves the storage-owned exclusion contract.
9. Exact metrics export and labels, tracing, operational endpoints, and
   deployment layout. Required v2 observability and processing-latency
   acceptance remain settled.
10. Exhaustive parity matrix and additional fixtures beyond the required
    [verification baseline](../architecture/verification.md).
11. Shutdown timeouts and escalation policy.
Unlisted code-level choices remain implementation details only while they
preserve every settled contract and do not resolve an item in
[open.md](open.md) implicitly.
