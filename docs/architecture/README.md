# KGI v2 architecture index

This document defines the current ownership boundaries and navigation for the
KGI v2 architecture. It contains no component behavior; the focused documents
below own their named contracts.

## Architecture authority

The focused architecture documents listed below collectively form the current
normative KGI v2 architecture. Each behavioral contract has one focused owner.
Accepted changes update that owner and the applicable decision register in the
same change.

The [17 September](../history/handoffs/handoff-2026-09-17.md) and
[20 September](../history/handoffs/handoff-2026-09-20.md) handoffs preserve
provenance only. They do not participate in normative precedence or resolve a
conflict with a focused owner.

## Contract conventions and scope

The repository-wide collaboration roles, reading order, and conflict rules are
defined in [`AGENTS.md`](../../AGENTS.md). Within architecture documents:

- text marked **settled** is a requirement;
- Rust, SQL, and message fragments fix semantic shape only and need not compile
  as written; the [core crate structure](overview.md#core-crate-structure--settled)
  is fixed while the remaining workspace and module layout stays deferred in
  the [decision register](../decisions/deferred.md);
- mechanics not fixed by a contract remain implementation choices; and
- a conflict must be reported and resolved in the owning architecture or an
  accepted ADR, never blended silently.

The KGI v2 system boundary is defined in [overview.md](overview.md). Work listed
in [future-work.md](../future-work.md) is outside v2 until promoted by an
accepted architecture decision. Open requirements and implementation choices
are tracked separately in the [decision register](../decisions/README.md).

Legacy handoffs and earlier planning material may use these names:

| Legacy name | Current name |
|---|---|
| `NodeClient` | `NodeService` |
| `ValidatedStorageService` | `ValidatedDbClient` |
| `Cycle 1` | `PreSeal` |
| `Quiesce` | `Deactivate` |
| `check_block_materiality()` | `ValidatedDbClient::block_presence()` |
| `load_reconciliation_state()` | `ValidatedDbClient::reconciliation_snapshot()` |

Behavioral replacements are recorded in
[superseded.md](../decisions/superseded.md) and rejected proposals in
[rejected.md](../decisions/rejected.md).

## Focused documents

Each contract has one owning document; other documents link to it instead of
restating it.

| Document | Sole concern |
|---|---|
| `README.md` | Architecture authority, reading order, ownership boundaries, and navigation. It contains no system behavior. |
| `overview.md` | System boundary, component ownership, core crate and repository structure, process and data-flow shape, process configuration and command entry, release and deployment layout, logging, and system-wide resource isolation. |
| `domain-model.md` | Shared identities and graph vocabulary: hashes, compact IDs, coordinates, consensus order, materiality, PP boundary, and common value types. |
| `node-service.md` | Node connection lifecycle, validated RPC generations, subscriptions, notification routing, and RPC normalization. |
| `storage.md` | Database lifecycle, schema and metadata, persistent identities, transactions, caches, and PostgreSQL behavior. |
| `block-processing.md` | BlockProcessor, OrphanManager, DependencyResolver, block admission, and PP-boundary sealing behavior. |
| `vspc-processing.md` | VSPC normalization, readiness, sequencing, coloring, and VSPC-specific behavior during recovery and Live. |
| `processing-lifecycle.md` | Supervisor, recovery intent, ResyncEngine, Resync/Rebuild preparation, the common pump, Catchup and Live admission, fault policy, retries, and teardown. |
| `api-ingress.md` | Per-session graph-update channel, producer and receiver capabilities, pre-seal gate, lifecycle markers, and gap reporting. |
| `api-graph.md` | Graph values, views, tracking policies, revisions, deltas, history, mutation and extraction behavior, composition, and retention. |
| `api-publication.md` | Graph publication identity and lifecycle, database seed construction, stream alignment, activation, reconstruction, replacement, and publication-scoped response-reuse lifetime. |
| `api-service.md` | ApiService control, database-generation binding, reset, shutdown, status aggregation, resource bulkheads, and saturation. |
| `api-protocol.md` | Public HTTP and SSE graph contract, limits, cursors, windows, errors, ETags, response caching, and delivery behavior. |
| `api.md` | Navigation for the focused API architecture documents. It contains no system behavior. |
| `web.md` | Browser client behavior and presentation requirements. |
| `verification.md` | Required fixtures, integration scenarios, acceptance checks, and upstream assumptions. It references contracts without redefining them. |

## Architecture ownership map

The project-wide [single ownership policy](../../AGENTS.md#single-ownership-policy)
applies to every architecture change. The focused-document table above assigns
the sole owner for each concern. The following rules resolve boundaries within
that map:

- shared names and value semantics belong to `domain-model.md`; behavior that
  uses them belongs to the responsible component;
- a coordinating component owns cross-component ordering and lifecycle
  transitions, while each component owns its local reaction;
- `storage.md` owns transaction, durability, schema, and cache-publication
  semantics even when another component requests the operation;
- `processing-lifecycle.md` owns recovery phase changes, while the processor
  documents own processor behavior within each phase;
- `verification.md` owns verification requirements and links to the contract
  under test; and
- decision registers, handoffs, audits, and review reports have the
  non-behavioral roles assigned by `AGENTS.md` and do not become competing
  architecture owners.

## Handoff coverage map

This table records the verified dispatch of every numbered section in the
historical [20 September handoff](../history/handoffs/handoff-2026-09-20.md).
Where a section contained more than one concern, the row records the exact
split. The focused owner, rather than this historical mapping, defines current
behavior.

| Handoff section | Focused owner and dispatch rule | Cutover status |
|---|---|---|
| §0, Reading rules and scope | `README.md` for architecture authority and navigation; `../README.md` for repository-wide document classes. Scope constraints on future work go to `../future-work.md`. | Verified and cut over |
| §1, System shape and ownership | `overview.md`. Shared type names introduced only as vocabulary go to `domain-model.md`. | Verified and cut over |
| §2, Shared identities and graph vocabulary | `domain-model.md`. Persistence-specific enforcement and schema representation go to `storage.md`. | Verified and cut over |
| §3, Lifecycle, intent, commands, and channels | `processing-lifecycle.md` for lifecycle, command direction, cross-worker channel defaults, fault classification, retries, and milestones. The per-session API graph-update feed belongs to `api-ingress.md`. Reusable identity/value definitions go to `domain-model.md`; service-specific reconnect rules go to `node-service.md` or `storage.md`. | Verified and cut over |
| §4, NodeService and validated RPC | `node-service.md`. | Verified and cut over |
| §5, StorageService lifecycle and DB bootstrap | `storage.md`. Supervisor reactions to StorageService state link to `processing-lifecycle.md`. | Verified and cut over |
| §6, Retained persistence model and cache | `storage.md`. Shared materiality vocabulary links to `domain-model.md`. | Verified and cut over |
| §7, Block materialization and PP boundary | `storage.md` owns materialization transactions, schema invariants, and cache publication; `block-processing.md` owns when BlockProcessor requests those operations and reacts to PP sealing. | Verified and cut over |
| §8, BlockProcessor, orphan topology, dependency resolution | `block-processing.md`. Cross-worker recovery dispositions link to `processing-lifecycle.md`. | Verified and cut over |
| §9, VSPC changes, readiness, history, and atomic coloring | `vspc-processing.md` owns sequencing and readiness; `storage.md` owns the atomic coloring transaction; shared VSPC value definitions go to `domain-model.md`. | Verified and cut over |
| §10, ResyncEngine preparation and common pump | `processing-lifecycle.md`. Node RPC normalization used by the pump links to `node-service.md`; component-local admission behavior links to `block-processing.md` and `vspc-processing.md`. | Verified and cut over |
| §11, Catchup, overlap, Live, and late transport messages | `processing-lifecycle.md` owns phase transitions, overlap-based admission, timing, and global Live entry. `block-processing.md` and `vspc-processing.md` own their local phase behavior. `node-service.md` owns routing and transport-message handling. | Verified and cut over |
| §12, Teardown and delivery/failure semantics | `processing-lifecycle.md` owns teardown order, barriers, owner-directed faults, and session completion. Component-specific draining duties remain in the relevant component document. | Verified and cut over |
| §13, In-process API and graph-update feed | `api-ingress.md`. Producer-side graph-update delivery guarantees remain in the relevant processor document and are referenced by `api-ingress.md`. | Verified and cut over |
| §14, API snapshot, revision, delta, SSE, and ETags | `api-graph.md` owns graph values, views, revisions, deltas, and history. `api-publication.md` owns publication identity and lifecycle. `api-protocol.md` owns SSE, ETags, and public response behavior. | Verified and cut over |
| §15, Reset and recovery-time API availability | `api-service.md` owns `reset`; `api-publication.md` owns publication behavior; `processing-lifecycle.md` owns the Supervisor `reset` call point and processor phase ordering; `block-processing.md` owns lifecycle-marker production; `storage.md` owns API-read exclusion during database replacement. | Verified and cut over |
| §16, DAA navigation and window API | `api-protocol.md`. Storage query semantics needed by these endpoints remain in `storage.md`. | Verified and cut over |
| §17, Web behavior | `web.md`. Wire contracts consumed by the browser belong to `api-protocol.md`. | Verified and cut over |
| §18, Resource isolation and scalability | `overview.md` owns the system-wide isolation model. Concrete component budgets and saturation behavior remain with each component; API budgets belong to `api-service.md`. | Verified and cut over |
| §19, Tests and verification obligations | `verification.md`, organized by the architecture owner being verified. Additional choices go to `../decisions/deferred.md`. | Verified and cut over |
| §20, Remaining implementation decisions and v2.1 boundary | Unresolved v2 architecture goes to `../decisions/open.md`; constrained implementation choices go to `../decisions/deferred.md`; execution order goes to `../implementation/sequence.md`; work outside v2 remains in `../future-work.md`. | Verified and cut over |
| §21, Rejected designs | `../decisions/rejected.md` or `../decisions/superseded.md`, according to whether the design was never accepted or was replaced after acceptance. | Verified and cut over |

## Coverage verification

All handoff sections §0–§21 have been checked against their dispatched
owners and the later accepted decisions incorporated during reconciliation.
Every current behavioral contract, open requirement, deferred implementation
choice, v2.1 boundary, and rejected or superseded design has an owner outside
the handoff. Local links and heading anchors in the current architecture and
decision documents have also been checked.

Two independent losslessness reviews checked the reorganization against its
pre-extraction base. Their findings were resolved before cutover, including
the final `Inconsistent` database initialization clarification.

## Cutover record

The authority cutover completed after confirming that:

1. every row in the coverage map was applied;
2. every dispatched section was checked against later accepted changes;
3. no focused document gives a handoff precedence on conflict;
4. no current contract exists only in a handoff;
5. repository cross-references and collaboration instructions identify the
   focused owners consistently; and
6. independent review confirmed that the reorganization preserved accepted
   behavior and ownership.

The focused files are therefore the normative architecture. The superseded
handoffs reside under [`docs/history/handoffs/`](../history/handoffs/) as
non-normative provenance.
