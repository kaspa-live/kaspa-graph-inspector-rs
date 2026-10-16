# Implementation status

Updated 16 October 2026.

The architecture bootstrap, post-handoff reconciliation, focused-document
extraction, and two independent losslessness reviews are complete. The focused
documents named by `docs/architecture/README.md` are the normative KGI v2
architecture; the superseded handoffs are historical provenance.

The implementation foundation is in progress from reviewed architecture
baseline commit `f2cab146a3633964a69ebcdcb045d4729c657f0a`. The buildable
virtual Cargo workspace, settled crate dependency skeleton, storage migration
location, Vite/React Web workspace, fixture locations, and atomic `xtask`
bundle command are present. The retained KGI v1 browser source and replay
fixtures have been imported from source commit
`2ec895375f1161af1e57a58ed94db5977584d97d`; its Create React App build seam
has been replaced by Vite and the imported source builds with the workspace's
React 19 and TypeScript 7 toolchain. The shared `kgi-model` domain, lifecycle,
fault, status, VSPC, and committed graph-update values are implemented against
the selected rusty-kaspa `v2.1.0` value crates. The session-scoped graph-update
ingress implements the shared pre-seal gate, nonblocking ordinary offers,
lossless lifecycle markers, coalescing gap observation, and ingress metrics.
ApiService orchestration remains behavior-free scaffolding; `kgi-api-core`
contains the in-memory publication kernel described below. The processing
crate contains the session-scoped BlockProcessor and VspcProcessor slices
described below.
The first
StorageService implementation slice selects SQLx with PostgreSQL and Rustls,
embeds the forward-only metadata and processing-schema migrations, acquires the
dedicated advisory ownership lock, classifies database bootstrap states, and
performs the atomic idempotent `Uninitialized -> Empty` network binding. Its
post-migration physical-layout fingerprint validates column defaults and
identity mode, constraints, constraint-backed indexes, and required query-path
indexes before any database generation can be published. Malformed immutable
network bindings produce terminal unsupported-schema rejection rather than
transient connection retry. A recognized future migration version is classified
before current-layout table validation, including when that migration added
new tables.
The validated-generation storage slice adds processing and read-only API pool
generations without exposing SQLx resources across the crate boundary. The
processing pool is independently capped at four connections, the API pool at
eight, and API sessions default to read-only. One `ValidatedDbClient` type is
used for compatible Empty, Initialized, and Inconsistent contents; it proves
generation ownership, compatible schema, and immutable `DatabaseBinding`.
Immutable network metadata and mutable `ProcessingMetadata` use separate
singleton tables; Empty has no processing-metadata row, and invalid processing
metadata produces semantic Inconsistent without invalidating the client. Only
coherent Empty and Initialized contents initially produce an API capability.
API validity is shared and terminal, while retirement authority remains private
to storage. The processing client also loads a fresh storage-owned session
state in one repeatable-read transaction. Initial publication and session
loading share a bounded classifier based on existence checks, indexed PP and
VSPC-sink lookup, and primary-key identity resolution; they perform no retained
graph audit. The permanent autonomous StorageService lifecycle now owns
connection, advisory-lock health, initialization authorization, exact
processing and API generation publication and retirement, independent pool
replacement, terminal rejection, retry with equal jitter and its Ready reset,
and terminal idempotent shutdown. The implemented session-state operation
reports connection loss through a private exact-generation retirement barrier,
so the ordered retirement event is enqueued before `GenerationLost` returns;
event-enqueue failure is instead returned through that result-bearing barrier.
Deterministic lifecycle tests cover the nominal retry sequence and reset
boundary; PostgreSQL integration tests cover publication order, exact and
independent replacement, advisory-lock contention, lock loss with both
two-generation and processing-only states, lock loss during blocked replacement
opening, initial and replacement pre-publication ownership rechecks,
nonblocking replacement while a retired pool still has a checked-out
connection, terminal joining of that pool drain, missing required index and
constraint rejection, initialization COMMIT acknowledgement loss with
truth-based reconnect classification, and fatal event-path closure.
The service-owned API replacement gate, generation-spanning read-lease
registry, bounded drain/cancellation primitive, atomically swappable processing
cache set, and shared processing mutation guard are implemented. The processing
client now sends exact-generation rebuild requests to the permanent service
worker. That worker owns gated API retirement and publication, lease draining,
advisory-lock rechecks, and outcome-specific generation disposition. The
pruning-point replacement transaction performs the settled ordered identity
interning and complete processing-data replacement, constructs the sync anchor,
and atomically swaps the fully seeded cache set only after definite commit.
PostgreSQL integration tests cover non-Genesis and exact-Genesis replacement,
preflight score rejection, an old-generation lease delaying mutation, definite
rollback, setup preparation failure, drain timeout, setup cancellation,
an already retired generation retaining a live lease, replacement-control loss
before and after fresh API publication, pre-commit connection loss with
rolled-back truth, and ambiguous commit with truth-derived processing
republication. No open architecture requirement currently blocks the remaining
storage work.
Validated processing operations now normalize their common failures through
`ProcessingDbError`, while materialization, VSPC mutation, and Rebuild retain
their operation-specific semantic wrappers. Internal and non-persisted
representation failures remain distinct from invalid persisted-state evidence
before crossing into processing.

The imported browser still uses the v1 graph data source, models, and update
behavior, so it is not yet compatible with the v2 HTTP, SSE, publication, and
graph contracts. Runtime Web configuration, concrete Docker and conventional
installation files, and the file-log directory model remain unimplemented.

The public [graph wire format](../architecture/api-protocol.md#common-transport-dto-rules--settled)
and [mandatory gzip delivery path](../architecture/api-protocol.md#graph-http-compression--settled)
are settled. Exact gzip quality remains deferred; optional later binary-format
benchmarking does not block implementation.

The first production tranche is complete through graph-update ingress commit
`4ed0d2f`. Its initial module
boundaries, upstream value-crate pin, graph-update channel and gap primitives,
and error conventions are recorded in
[implementation choices](choices.md#5-october-2026-shared-model-and-graph-update-ingress).
The shared model and session-scoped graph-update ingress portions are complete.
Workspace formatting, Cargo check, Nextest, doctests, and Clippy with warnings
denied pass. The Web Vitest baseline passes with no test files present, and the
Vite production build passes with its existing large-chunk warning. This
increment is ready for review before worker or API consumers are added.

The next infrastructure increment now implements the immutable `kgi-core`
configuration values, protected database and token values, explicit TOML,
environment, and CLI source resolution, static validation, and the top-crate
command grammar. It also implements the `kgi-core::signals` adapter with its
weak shutdown target, first- and second-signal graceful requests, and
third-signal forced exit. Process composition and dispatch remain pending, as
does the Supervisor-owned installation that connects the signal adapter to the
global shutdown lifecycle.

The NodeService increment is implemented through its validated-generation
lifecycle. The accepted rusty-kaspa `v2.1.0` PUAR is present, and
the `kgi-node` module boundaries, direct dependencies, lifecycle primitives,
and deterministic RPC, clock, and jitter test seams are recorded in
[implementation choices](choices.md#6-october-2026-nodeservice-implementation-foundation).
`kgi-node` now resolves and validates local consensus assumptions and contains
the generation-bound raw-response normalizer for full blocks, recovery headers,
pruning-point and Catchup samples, GetBlocks pages, VSPC V2 responses, and
BlockAdded notifications. The private normalizer is shared by each validated
client and its stable notification router. It preserves the minimized
trusted-node boundary. RPC API compatibility and Genesis discovery are private
steps in NodeService connection validation rather than standalone public
helpers.
The architecture-level GetBlock not-found representation blocker is resolved
by the NodeService-owned
[compatibility contract](../architecture/node-service.md#getblock-not-found-compatibility-classification)
and the lifecycle-owned opaque RPC-failure disposition.
Validated-generation RPC execution now binds every operation and composite
request to one physical connection, applies the runtime concurrency bound,
constructs the required requests, normalizes their responses, classifies the
pinned exact-message GetBlock compatibility forms, and linearizes completion
against cancellation or retirement. Malformed recovery responses use an
exact-generation retirement request and completion barrier; opaque RPC failures
and range faults do not retire the generation. The operation-specific
response-conversion adapter classifies only failures uniquely attributable to
fields KGI consumes as malformed; failures in ignored or ambiguously identified
fields remain opaque. Its tests exercise the pinned wire-to-typed converters
directly. The private `NotificationRouter` implements the synchronous rusty-kaspa callback,
generation-context BlockAdded normalization, empty and malformed virtual-chain
filtering, bounded nonblocking delivery, and shared disablement after malformed
input, saturation, or unexpected endpoint loss. Each validated generation now
owns ordered all-or-nothing subscription activation, rollback, deactivation,
and retirement on incomplete remote cleanup. The permanent NodeService worker
connects with upstream automatic reconnection disabled, validates the node,
waits through IBD, publishes and retires exact `Arc` generations, preserves
last-validated status, applies jittered reconnect backoff with its Ready reset,
and provides terminal idempotent shutdown. Deterministic tests cover the owner
loop, subscription failure paths, publication and retirement order, IBD gate,
retry sequence and reset boundary, and the operation-retirement completion
barrier.

The processing-worker increment is complete through its standalone processor
boundary. Private OrphanManager and DependencyResolver child loops are
implemented. Each worker tree is constructed inert with
its exact session resources, launched once after initialization, and consumed
through terminal shutdown; no reusable Begin, Deactivate, or generation-rebind
path remains. OrphanManager owns bounded normalized-block storage, reverse
missing-reference topology, topology-ranked frontier selection, logical
resolution-pending state, natural-arrival cancellation, and induced ready-block
delivery. It owns DependencyResolver as a private child; that worker owns the
exact session RPC generation, its local concurrency allowance, cancellable
GetBlock tasks, and typed result delivery without reclassifying node outcomes.
Separate prioritized command paths preempt bounded data backlogs. Existing
focused tests cover inert construction, reverse-topology release, exact
threshold and capacity boundaries, maximum-release ordering, pending-state
retention, Resolve/Cancel FIFO order, active and waiting RPC limits,
node-outcome preservation, nested fault propagation, full-result-channel
failure, cancellation, terminal backlog preemption, and exact RPC-generation
release. Unexpected loss of an active processor or descendant command path is
reported through the typed ownership-fault path rather than interpreted as
ordinary shutdown.
The BlockProcessor-owned lifecycle-marker worker provides the nonblocking
two-command local FIFO, lossless graph-ingress delivery, accepted-command drain
after local closure, and receiver-closure termination without retaining RPC or
database generations. Terminal BlockProcessor shutdown joins it after draining
accepted markers. The BlockProcessor owner loop implements explicit input
priority, notification gating, phase-specific materialization policy, PostSeal
transition, graph-before-persisted delivery, Catchup lower-bound and
source-overlap accounting, child fault provenance, and typed storage and node
fault normalization. Focused tests cover inert construction and initialization,
single launch, final startup-handoff failure, Genesis and non-Genesis marker
paths, strict sealing, closed-gate discard, late notification filtering, exact
overlap, post-commit delivery loss, API receiver closure, terminal input
preemption, complete descendant joining, and session-generation release.
The VspcProcessor owner loop now retains its exact session RPC and processing
database generations, anchor-derived sink and materialization history,
multi-index pending state, synthetic FIFO, ordered resolved notifications,
overlap flag, and pending metrics. Its inert construction and single launch,
strict command priority, event-before-block resolution, notification collision and
capacity handling, synthetic priority, Catchup filtering and crossing,
component-local Live transition, one-batch member resolution, typed path
attribution, definite-commit graph publication, terminal state clearing, and
generation release have focused coverage. Every bounded processing data
mailbox now exposes current occupancy and sticky high-water measurements
through the processor snapshots; exact-capacity tests cover the BlockProcessor
notification and synthetic inputs, resolution delivery, OrphanManager input,
DependencyResolver work, persisted-block delivery, and both VspcProcessor
inputs. Together with the previously
completed session-scoped graph-update ingress, these workers complete
implementation-sequence step 5. ProcessingSession construction,
synthetic pump ownership, notification-router endpoint installation, global
Live coordination, and Supervisor event handling remain for the integration
point.

The non-normative [implementation sequence](sequence.md) records
the proposed work order and prerequisites.

Implementation increment 6 is implemented: `kgi-api-model` now contains the
canonical graph values, checked deltas, seeded and extracted views, Head and
Fixed mutation, delta application and composition, and level-scoped retained
history. Focused tests cover graph extent and projection behavior, derived edge
and level maintenance, terminal Fixed freezing, all canonical delta mutation
and composition forms, gapless history append, cumulative estimates,
prefix-only pruning, detached entry lifetime, and retained-range selection.
Publication, database projection reads, encoding, transport, and ApiService
runtime behavior remain in the separately numbered increments recorded in
[the implementation sequence](sequence.md#active-increment-6-canonical-graph-model).
Completion validation passed workspace formatting, Cargo check, 337/337
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.

Implementation increment 7 is implemented: `kgi-api-model` now contains the
API-owned projection request, anchor, resolution, unavailability, and seed
values, while `kgi-storage` provides the generation-bound
`ValidatedApiDbClient::load_graph_view_seed` operation. The storage projection
resolves Head, level, block-hash, and DAA-floor scopes and constructs complete
graph seeds inside one stable read-only PostgreSQL snapshot. Its database
reads include nominal blocks, canonical relationships, crossing edges and
their external endpoint levels, selected-parent indexes, current projection
fields, the materialized-ID alignment cut, and committed VSPC sink. API reads
participate in the replacement gate and preserve exact-generation retirement
semantics without coupling the processing generation to API-pool failure.
PostgreSQL integration coverage exercises the complete maximum-depth Head,
short retained ranges, every anchor and boundary shift, stable-snapshot
visibility, zero-, one-, and two-endpoint crossing edges, sentinel exclusion,
ordinary misses, incoherent projections, cancellation, query and connection
failure, exact API-generation retirement, and processing/API pool isolation.
Publication construction and request serving remain in the later increments
recorded in [the implementation sequence](sequence.md#remaining-reviewable-increments).
Completion validation passed workspace formatting, Cargo check, 357/357
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.

Implementation increment 8 is implemented: the private `kgi-api-core`
publication kernel now owns publication identity and timing, lifecycle state,
the atomically observed image, the serialized mutation boundary, independently
readable retained history, and immutable request captures. Original block and
VSPC updates advance the Head image before appending their estimated-cost
history entries, prune against the resulting Head extent, and preserve
no-effect updates without revision advancement. Lifecycle transitions and
full-view, subview, delta-target, and retained-history captures are present;
protocol-owned level capture, client notifications, caching, construction from
Storage, and the long-lived publication runtime remain in later increments.
Deterministic tests cover the observable image-before-history interval, writer
ordering and caller-cancellation completion, lifecycle transitions, no-effect
updates, publication-local millisecond timestamps, detached frozen views,
retained history surviving pruning, fresh publication identities, and typed
projection failures.
Completion validation passed workspace formatting, Cargo check, 377/377
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.
