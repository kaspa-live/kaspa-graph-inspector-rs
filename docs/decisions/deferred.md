# Deferred implementation decisions

These choices are deliberately left to implementation work. Implementations
must stay within the linked settled architecture and record choices where they
become durable project constraints.

1. Internal Rust module boundaries within the settled
   [crate and repository structure](../architecture/overview.md#repository-web-build-and-release-structure--settled).
2. PostgreSQL Rust client, migration framework, and concrete SQL types.
3. Concrete bounded-work queue collections and raw-byte estimator weights.
   Local RPC
   scheduling and batching remain implementation choices only where the
   focused architecture does not fix request boundaries or batch semantics.
4. Detailed Tokio fairness, drain, graph-update gap counter/wakeup primitives,
   component-status watch primitives, and BlockProcessor marker-worker
   primitives.
5. Exact gzip compression quality for the settled graph delivery contract in
   the [API protocol](../architecture/api-protocol.md#graph-http-compression--settled).
6. Exact metrics export and labels, tracing, operational endpoints, file-log
   rotation size and retained archive count, service hardening directives, and
   concrete container base images. Required v2 observability,
   processing-latency acceptance, and the deployment layout remain settled.
7. Exhaustive parity matrix and additional fixtures beyond the required
    [verification baseline](../architecture/verification.md).
Unlisted code-level choices remain implementation details only while they
preserve every settled contract and do not resolve an item in
[open.md](open.md) implicitly.
