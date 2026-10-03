# Deferred implementation decisions

These choices are deliberately left to implementation work. Implementations
must stay within the linked settled architecture and record choices where they
become durable project constraints.

1. Internal Rust module boundaries within the settled
   [crate and repository structure](../architecture/overview.md#repository-web-build-and-release-structure--settled).
2. PostgreSQL Rust client, migration framework, and concrete SQL types.
3. Concrete cache, destination-heat, client-wake registry, and bounded-work
   queue collections, plus raw-byte estimator weights. Local RPC
   scheduling and batching remain implementation choices only where the
   focused architecture does not fix request boundaries or batch semantics.
   Moka is the current cache-library candidate.
4. Detailed Tokio fairness, drain, graph-update gap counter/wakeup primitives,
   per-client revision-wakeup scheduling primitives,
   component-status watch primitives, BlockProcessor marker-worker primitives,
   and concrete storage synchronization mechanics.
   A shared mutation lock plus per-lane mutexes is one valid storage shape;
   exact lock types remain an implementation choice.
5. Exact gzip compression quality for the settled graph delivery contract in
   the [API protocol](../architecture/api-protocol.md#graph-http-compression--settled).
6. Detailed shared/exclusive permit, historical-read cancellation, and
   transaction mechanism for StorageService's database-replacement gate. Safe
   transaction locking is one candidate; another mechanism is acceptable when
   it preserves the storage-owned exclusion contract.
7. Exact metrics export and labels, tracing, operational endpoints, file-log
   rotation size and retained archive count, service hardening directives, and
   concrete container base images. Required v2 observability,
   processing-latency acceptance, and the deployment layout remain settled.
8. Exhaustive parity matrix and additional fixtures beyond the required
    [verification baseline](../architecture/verification.md).
Unlisted code-level choices remain implementation details only while they
preserve every settled contract and do not resolve an item in
[open.md](open.md) implicitly.
