# Settled decision index

This file indexes accepted KGI v2 outcomes. The linked focused architecture is
the complete contract and rationale source; this index does not restate its
mechanics.

| Concern | Settled outcome | Current owner |
|---|---|---|
| Identity and materiality | Separate persistent identity, materiality, PP-boundary, and ORIGIN semantics. | [Domain model](../architecture/domain-model.md), [storage](../architecture/storage.md) |
| Database lifecycle | Network-bound idempotent bootstrap and explicit database replacement. | [Storage](../architecture/storage.md) |
| Node capability | Validated RPC generations and normalized node inputs. | [NodeService](../architecture/node-service.md) |
| Block processing | Phase-aware block admission, materialization, and dependency recovery. | [Block processing](../architecture/block-processing.md), [storage](../architecture/storage.md) |
| VSPC processing | Ordered readiness-gated VSPC processing and publication. | [VSPC processing](../architecture/vspc-processing.md) |
| Recovery lifecycle | Explicit Resync/Rebuild coordination and overlap-based Live admission. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| API publication | A private session-bound runtime owns database-seeded publication-identified graph views, composable revision deltas, publication-scoped Head response reuse, revision-coordinated delivery, and historical reads. | [ApiService](../architecture/api-service.md), [API graph model](../architecture/api-graph.md), [API graph publication](../architecture/api-publication.md), [API protocol](../architecture/api-protocol.md), [storage](../architecture/storage.md) |
| Resource isolation | Processing-priority resource isolation and bounded API load. | [Overview](../architecture/overview.md), [ApiService](../architecture/api-service.md) |
| Resource limits | Fixed initial graph, processing, RPC, API ingress, publication, HTTP, database-read, encoding, and SSE bounds. | [API protocol](../architecture/api-protocol.md), [API ingress](../architecture/api-ingress.md), [API publication](../architecture/api-publication.md), [ApiService](../architecture/api-service.md), [processing lifecycle](../architecture/processing-lifecycle.md), [block processing](../architecture/block-processing.md), [VSPC processing](../architecture/vspc-processing.md), [NodeService](../architecture/node-service.md) |
| Web behavior | Publication-aware, hash-identified live and fixed graph views. | [Web](../architecture/web.md) |
| Verification | Accepted pinned upstream review and executable KGI verification. | [Verification](../architecture/verification.md#current-puar-result) |
| Core crate structure | A top composition crate over a process infrastructure core containing configuration values and signal handling, plus acyclic model, node, API model, API ingress, storage, processing, and API core crates. | [Overview](../architecture/overview.md#core-crate-structure--settled) |
| Process termination | Supervisor-targeted process signals use count-based operator escalation. | [Signal adapter](../architecture/overview.md#process-termination-signal-adapter--settled), [Supervisor lifecycle](../architecture/processing-lifecycle.md#termination-triggered-global-shutdown--settled) |
| Process configuration | One immutable, strictly validated process configuration resolves explicit TOML, environment, and CLI sources before command-specific startup. | [Configuration and command entry](../architecture/overview.md#process-configuration-and-command-entry--settled) |
| Repository and deployment | One Cargo workspace with the retained React Web migrated to Vite, reproducible atomic binary-and-asset bundles, runtime Web configuration, conventional and Docker layouts, and persistent rotating logs. | [Repository and release](../architecture/overview.md#repository-web-build-and-release-structure--settled), [HTTP composition](../architecture/overview.md#http-composition-and-runtime-web-configuration--settled), [deployment](../architecture/overview.md#deployment-and-logging--settled) |
| Graph wire format | One JSON representation with lossless decimal-string `u64` values, delivered through one mandatory gzip path. | [API representation](../architecture/api-protocol.md#common-transport-dto-rules--settled), [gzip delivery](../architecture/api-protocol.md#graph-http-compression--settled) |

Later standalone accepted ADRs may be added to this directory and indexed here.
