# Implementation status

Updated 27 October 2026.

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
ingress implements the accepted AwaitingCatchup gate and Catchup marker,
along with nonblocking ordinary offers, lossless lifecycle markers, coalescing
gap observation, and ingress metrics.
`kgi-api-core` contains the in-memory publication kernel and ApiService
lifecycle integration described below. The processing crate contains the
session-scoped BlockProcessor and VspcProcessor slices described below.
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
are settled. The private encoding implementation uses `flate2` quality 6 with
a zero modification time, as recorded in the implementation choices; optional
later binary-format benchmarking does not block implementation.

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
The superseded BlockProcessor-owned lifecycle-marker worker and its API marker
effects have been removed. The ProcessingSession-owned worker remains scheduled
for increment 21. The BlockProcessor owner loop implements explicit input
priority, notification gating, phase-specific materialization policy, PostSeal
transition, graph-before-persisted delivery, Catchup lower-bound and
source-overlap accounting, child fault provenance, and typed storage and node
fault normalization. Focused tests cover inert construction and initialization,
single launch, final startup-milestone failure, Genesis and non-Genesis sealing
without API marker publication, closed-gate discard, late notification
filtering, exact overlap, post-commit delivery loss, API receiver closure,
terminal input preemption, complete descendant joining, and session-generation
release.
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
[the implementation sequence](sequence.md#remaining-reviewable-increments).
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
the atomically observed Head-and-history core, the serialized mutation
boundary, and immutable request captures. Original block and VSPC updates
mutate the Head, append their estimated-cost history entries, and prune against
the resulting Head extent synchronously under one core write lock. No-effect
updates preserve both graph values without revision advancement. Lifecycle
transitions and full-view, subview, delta-target, and retained-history captures
are present; protocol-owned level capture, client notifications, caching,
construction from Storage, and the long-lived publication runtime remain in
later increments.
Deterministic tests cover atomic core visibility, writer ordering, cancellation
before mutation begins, lifecycle transitions, no-effect updates,
publication-local millisecond timestamps, detached frozen views,
retained history surviving pruning, fresh publication identities, and typed
projection failures.
Completion validation passed workspace formatting, Cargo check, 377/377
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.

Implementation increment 9 is implemented: representation-independent public
API outcomes and status values live in `kgi-api-model`, while private
`kgi-api-core` DTOs provide the sole Serde boundary for graph responses,
canonical delta batches, common errors, lifecycle and status observations,
window sources, and SSE control payloads. Complete graph bodies and ordered
delta batches use first-occurrence response-local hash dictionaries; ordered
parent and merge-set vectors remain ordered, unordered graph collections carry
no wire-order meaning, and serializer-only block and edge removals are omitted
before interning their hashes. Public `u64` values use decimal JSON strings,
bounded graph values use numeric `u32` and `u16` fields, absent semantic values
remain explicit `null`, and colors use the protocol-owned numeric codes. The
canonical graph and retained history remain serialization-free and unchanged.
The internal representation version and semantic Head-ETag input are present
for later cache integration. Encoding scheduling, compression, response-size
enforcement, caching, logical request operations, HTTP routing, and response
delivery remain in later increments.
Completion validation passed workspace formatting, Cargo check, 402/402
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.

Implementation increment 10 is implemented: `kgi-api-core` now contains the
bounded encoding scheduler, capped JSON serialization and deterministic gzip
construction, publication-local graph response cache, source-keyed delta and
destination-heat indexes, and the five derived Head cache tiers. Cache jobs
share immutable terminal values, remain tracked independently of individual
waiters, use weak publication-local insertion, and preserve prior Head values
through refresh and failed replacement. Focused tests cover encoding limits
and failures, scheduler priority and saturation, request cancellation and
terminal draining, delta and Head single-flight, no-waiter and dropped-cache
completion, shared body allocations, structural pruning, exact Head selection,
soft refresh, hard-age retry, and unobserved Head-tier cancellation during
cache-job shutdown. Logical request operations, client wakeup state, publication
construction, HTTP routing, and response delivery remain in the later
increments recorded in
[the implementation sequence](sequence.md#remaining-reviewable-increments).
Completion validation passed workspace formatting, Cargo check, 436/436
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.

Implementation increment 11 is implemented: each `GraphPublication` now owns
its publication-local SSE client registry and each connection owns one bounded,
ordered mailbox. The registry provides atomic initial and replacement batches,
fresh opaque identifiers, lifecycle observations, coalesced Head wakeups,
completion-time schedule replacement, exact disconnect removal, and slow or
stale client cleanup. Registry scans are synchronous under one state mutex;
mailbox readiness contains no message state, and the design creates neither
per-client tasks nor timers. Publication construction, original-update tails,
and lifecycle transitions keep registry state aligned with the publication
core under the shared mutation order. Focused tests cover capacity and
identifier collisions, atomic admission, mailbox ordering and closure,
Head/rearm and registration/lifecycle linearizations in both orders,
replacement, stale weak registrations, and maximum-revision saturation.
Completion validation passed workspace formatting, Cargo check, 470/470
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.

Implementation increment 12 is implemented: the private `kgi-api-core`
`api_db` module contains the watch-backed API database binding, its ordered
generation-event and complete-snapshot values, synchronous update and gate
entry points, immediate and waiting accessors, and terminal cutoff. Production
specialization remains `ValidatedApiDbClient`; a private generic validity seam
supports deterministic tests without exposing a Storage test constructor or a
trait object. The module has no crate-root re-export and imports neither graph
publication values nor StorageService lifecycle authority. It observes only
the validated generation's terminal validity, so this increment changes no
graph publication or Storage lifecycle state. Focused tests cover initial
absence, exact generation identity and turnover, stale and repeated events,
invalid handles, immediate admission, construction waiting and wakeup,
Rebuild gate transitions, both sides of the stop cutoff, terminal waiter
release, and generation-reference release. Seed construction, public request
handlers, and ApiService integration remain in later increments.
Completion validation passed workspace formatting, Cargo check, 486/486
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.

Implementation increment 13 is implemented: the private
`kgi-api-core::publication::runtime` module now contains the single
session-bound graph-publication worker and its repeatable completed shutdown
barrier. The worker consumes the Catchup-gated session feed, loads one detached
Head seed through the exact current API database generation, stages concurrent
commits, aligns independent materialization and VSPC cuts, and synchronously
submits complete publications through a narrow private installation
capability. Continuity gaps, staging overflow, database read failures,
alignment failures, and active projection failures share one intra-session
reconstruction path. Receiver closure quiesces the worker, while terminal
shutdown abandons pending construction, makes an active publication Stale,
releases the receiver, and retains its completion result for repeated callers.
The implementation adds no ApiService control operation, public request path,
current-publication slot, or concrete generation fence; those remain in later
increments recorded in
[the implementation sequence](sequence.md#remaining-reviewable-increments).
Focused tests cover marker gating, exact-generation seeding, both alignment
orders and idle-source waiting, installation supersession, every reconstruction
failure class, sticky Live and gap fences, staging measurements, shutdown from
every worker state, quiescent receiver closure, inline seed cancellation,
Active-to-Stale ordering, and retained worker-loss results.
Completion validation passed workspace formatting, Cargo check, 511/511
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.

Implementation increment 14 is implemented: `kgi-api-core::ApiService` now
owns the reliable service-event path, API database binding, current-publication
slot, publication-runtime lifecycle and installation generation, independent
cancellation roots, cache-job tracker, encoding scheduler, and latest-value
status inputs. Resync and Rebuild resets install the successor runtime before
joining its predecessor; a narrow generation-fenced capability performs
complete publication replacement and conditionally reopens Rebuild database
reads. Publication-runtime and encoding-scheduler completion observers remain
owned and joinable, map unexpected permanent-work loss to the reliable event
path, and remain silent on expected shutdown. The terminal service barrier is
serialized with reset, stops database updates and construction waiters, joins
the runtime, cache work, and encoding scheduler, clears the publication, and
retains idempotent completion. Logical graph operations, public admission
lanes, request and SSE guards, and Axum transport remain in increments 15–20.
Focused verification covers construction, installation fencing, Rebuild gate
authority, exact publication identity, database binding and status snapshots,
runtime replacement and receiver release, generation exhaustion, independent
cancellation domains, exact-once worker-failure reporting, reset/shutdown
serialization, shutdown from every publication-runtime state, final Stale
mailbox ordering, database-generation cutoff races, event-path release,
terminal resource release, and repeated shutdown. Completion validation passed
workspace formatting, Cargo check, 535/535 Nextest tests including Docker
PostgreSQL integration, Cargo doctests, Clippy with warnings denied, and
`git diff --check`.

Implementation increment 15 is implemented: `GraphHistory` now retains only
atomic one-revision entries with absolute cumulative byte estimates, an
inclusive replay floor, append-time replayability, and detached immutable range
handles. The publication mutation kernel classifies every original update,
advances replay barriers inside the coherent core mutation, synchronously
invalidates affected publication-local cache state, and cancels detached work
after releasing bounded locks. Delta and Head candidates share a narrow final
replay-floor admission result for later logical operations. Focused tests cover
validation precedence, cumulative-cost boundaries, ordinary pruning, monotonic
barriers, detached ranges, exact cache invalidation, invalidated waiters,
construction cancellation and resource release, both completion race orders,
publication ordering, and both final-admission linearizations. Logical graph
operations and the broader response-cache redesign remain in the later
increments recorded in
[the implementation sequence](sequence.md#remaining-reviewable-increments).
Completion validation passed workspace formatting, Cargo check, 553/553
Nextest tests including Docker PostgreSQL integration, Cargo doctests, Clippy
with warnings denied, and `git diff --check`.

Implementation increment 16 is implemented: the publication-local response
cache now uses only the exact configured Head tiers and one source-ordered
`DeltaCache`. Query-driven age pruning removes an expired source prefix;
replay-floor invalidation remains synchronized across both cache domains.
Source-keyed delta work is serialized through fixed-target jobs selected only
after pre-election sizing. Successful farther results replace older completed
values, fitting completed bytes can be reclassified without encoding, and
failed or cancelled work retains no separate size evidence. The encoding
scheduler now owns a canonical-delta mode that measures the complete JSON,
reports one estimator overrun before gzip, and still returns the complete
compressed body; non-delta graph encoding retains its enforced hard limit.
Focused tests cover exact-tier access, ordered prefix pruning, both delta
elections, source serialization and enlargement, fixed-target installation,
reclassification, weak insertion, replay-floor and completion races,
estimator bounds, diagnostic overrun, cancellation, and terminal joining.
Logical Head and level operations remain in increment 17, while canonical
delta selection, continuation, and registration completion remain in
increment 18. Completion validation passed workspace formatting, Cargo check,
565/565 Nextest tests including Docker PostgreSQL integration, Cargo doctests,
Clippy with warnings denied, and `git diff --check`.

Implementation increment 17 is implemented: `ApiService` now provides the
complete logical in-memory Head snapshot and retained Head-level operations.
Head snapshots select only the smallest configured covering tier, use the
publication-owned two-election cache path, admit matching prospective weak
validators without construction, recapture obsolete candidates within the
same tracked job, and linearize final cached delivery against the replay
floor. Retained level lookup captures all requested values under one
publication-core read lock, encodes through the priority scheduler, and keeps
the result request-owned without entering the response cache or database.
Both operations retain the exact selected publication across later state
change or replacement and preserve request-local cancellation and encoding
failures. Focused tests cover every target-tier boundary, eligible reuse,
construction-free exact and wildcard matches, both second-election races,
matching initial and second-election work, exact and wildcard query-time
obsolete removal, both
prospective-admission/replay-floor lock orders, first-tier completion-time
recapture, replay-floor invalidation and re-entry, weak-validator identity,
Stale serving, all-or-nothing levels, cache bypass, replacement after capture,
cancellation, and encoding failure. Canonical delta selection and continuation remain in
increment 18; HTTP parsing, admission, headers, and delivery remain in later
increments. Completion validation passed workspace formatting, Cargo check,
582/582 Nextest tests including Docker PostgreSQL integration, Cargo doctests,
Clippy with warnings denied, and `git diff --check`.

Implementation increment 18 is implemented: `ApiService` now owns one mutable
process-wide active delta interval and the complete logical canonical-delta
operation. Requests validate publication, cursor, and optional registration in
order; capture one coherent selection Head; prune and query the source-keyed
cache; select dyadic targets; enforce the history replay floor and five-minute
source horizon; size retained atomic history before the second election;
serialize source construction and enlargement; and refresh the delivery Head
during final replay-floor admission without reselecting the fixed target.
Successful results distinguish complete, prefix, up-to-date, and wakeup
outcomes, derive request-specific continuation and HTTP-only retry metadata,
and atomically validate or rearm SSE registrations. Size descent installs only
its fitting fixed target, persists `size_limited` only on successful
completion, reuses or reclassifies existing bytes, and never treats Head- or
state-limited shortening as size evidence. Production initializes the active
interval to four and performs no adaptive update while the controller remains
open architecture work. Focused verification covers every ladder and interval,
history fitting without redundant capture, validation precedence, cache hits,
short-job joining and enlargement, size-limited reuse and reclassification,
the exact source-age boundary, Synchronizing/Live waits, terminal Stale
catch-up, registration disappearance, Head/rearm ordering, and both interval-
change directions without rewriting existing schedules. Anchored windows,
public admission, HTTP/SSE transport, and the adaptive controller remain in
later increments. Completion validation passed workspace formatting, Cargo
check, 608/608 Nextest tests including Docker PostgreSQL integration, Cargo
doctests, Clippy with warnings denied, and `git diff --check`.
