# Superseded designs

These designs were replaced by later accepted decisions. They are retained to
explain why older handoffs or implementation notes may use different terms.

| Superseded design | Current replacement | Current owner |
|---|---|---|
| Complete the initial scan before subscribing, then repair only from final VSPC. | Catchup overlap and ordinary recovery. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Create placeholder block rows at levels 0/1 and promote them later. | Boundary identities and separate materialized blocks. | [Domain model](../architecture/domain-model.md), [storage](../architecture/storage.md) |
| Treat any identity-row existence as block materiality. | `BlockPresence` states and the retained-past invariant included in `Materialized`. | [Domain model](../architecture/domain-model.md) |
| Prune VSPC history through `sink` using `<=`. | Strict-below-sink pruning. | [VSPC processing](../architecture/vspc-processing.md) |
| Estimate Catchup with a large sink-anticone window, tenfold merge-set margin, or fixed X/Y page rule. | Rolling-sink proximity and retained fallbacks. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Allow only ResyncEngine reconciliation to request Rebuild. | Typed direct Rebuild requests. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Gate global Live on a fixed body-tip snapshot, materiality batch, and bounded extra GetBlocks pages. | Overlap-based Live admission without a body-DAG completeness claim. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Pass raw `SharedNodeBlock` values between components and derive a separate `BlockMaterialization`. | One flattened `ValidatedNodeBlock` crosses the NodeService boundary. | [Domain model](../architecture/domain-model.md), [NodeService](../architecture/node-service.md) |
| Use an `Auto` recovery mode that falls through from Resync to Rebuild. | Explicit Resync and Rebuild obligations. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Defer the API outside KGI v2. | In-process API and Web-facing graph contract in v2. | [API](../architecture/api.md) |
| Identify API continuity with `GraphEpoch` stored on every graph image. | `GraphPublication.publication_id`, with views carrying only their own revision. | [API](../architecture/api.md) |
| Carry `HeadGraphCoverage` in every delta. | Explicit graph mutations plus the delta lineage `high_level`. | [API](../architecture/api.md) |
| Recheck VSPC source/sink continuity inside ApiService. | Trust the ordered, continuity-certified `VspcProcessor` output. | [API](../architecture/api.md) |
| Use a cross-session observer stream, global invalid flag, and `InvalidateSession` control. | Fresh per-session graph-update ingress and `ApiService::reset`-owned supersession. | [API](../architecture/api.md) |
| Publish a database seed before locating its graph-update boundary and keep a private detector active. | Explicit construction and alignment before activation. | [API](../architecture/api.md) |
| Treat graph-update channel closure as an ApiService session event. | `ApiService::reset` is the sole ApiService session-supersession mechanism. | [API](../architecture/api.md) |
| Have ResyncEngine reset ApiService and deliver lifecycle markers directly. | Supervisor installs the session ingress; BlockProcessor owns lifecycle-marker delivery. | [Processing lifecycle](../architecture/processing-lifecycle.md), [block processing](../architecture/block-processing.md) |
| Use completion of a Rebuild reset as the database-replacement barrier. | StorageService excludes API database phases with its replacement gate. | [Storage](../architecture/storage.md) |
| Have ApiService report API database-generation loss and Supervisor acquire and install context-specific replacements. | StorageService autonomously retires and replaces its API generation; Supervisor only forwards its ordered generation events to ApiService. | [Storage](../architecture/storage.md), [processing lifecycle](../architecture/processing-lifecycle.md), [API](../architecture/api.md) |
| Have Supervisor obtain processing DB generations through `StorageService::wait_until_usable()`. | StorageService publishes ordered processing-generation events; Supervisor retains the current generation only for construction of a new processing session. | [Storage](../architecture/storage.md), [processing lifecycle](../architecture/processing-lifecycle.md) |
| Have Supervisor obtain RPC generations through `NodeService::wait_until_usable()`. | NodeService publishes ordered RPC-generation events; Supervisor retains the current generation only for construction of a new processing session. | [NodeService](../architecture/node-service.md), [processing lifecycle](../architecture/processing-lifecycle.md) |
