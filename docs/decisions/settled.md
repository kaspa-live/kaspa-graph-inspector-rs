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
| API publication | Database-seeded publication-identified graph views, composable revision deltas, and historical reads. | [API graph model](../architecture/api-graph.md), [API graph publication](../architecture/api-publication.md), [API protocol](../architecture/api-protocol.md), [storage](../architecture/storage.md) |
| Resource isolation | Processing-priority resource isolation and bounded API load. | [Overview](../architecture/overview.md), [ApiService](../architecture/api-service.md) |
| Web behavior | Publication-aware, hash-identified live and fixed graph views. | [Web](../architecture/web.md) |
| Verification | Accepted pinned upstream review and executable KGI verification. | [Verification](../architecture/verification.md#current-puar-result) |
| Core crate structure | A top composition crate over acyclic model, node, API model, API ingress, storage, processing, and API core crates. | [Overview](../architecture/overview.md#core-crate-structure--settled) |

Later standalone accepted ADRs may be added to this directory and indexed here.
