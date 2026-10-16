# KGI v2 implementation sequence

Status: planning only. This is a non-normative execution plan; the focused
documents named by the [architecture index](../architecture/README.md) and later
accepted ADRs govern behavior.

## Entry gate

The architecture bootstrap, reconciliation, focused-document extraction, and
losslessness reviews are resolved. Production implementation may begin from a
committed revision containing the authority cutover. Implementation records
that baseline revision in `docs/implementation/status.md` before its first
production change.

Open architecture requirements still block their dependent implementation;
they do not reinstate a repository-wide implementation hold.

## Immediate implementation tranche

The first reviewable production increment is the shared model plus one complete
session-scoped graph-update ingress. It establishes a real cross-crate contract
used by processing and ApiService without depending on node connectivity,
PostgreSQL, HTTP, or the imported Web application.

Execute it as these small, ordered changes:

1. Record only the deferred choices needed by this tranche: the initial
   `kgi-model` and `kgi-api-ingress` module boundaries, the exact pinned
   rusty-kaspa crates used for hash and work values, Tokio channel primitives,
   the gap-generation/wakeup primitive, and error-library conventions. Keep
   the PostgreSQL and replacement-gate choices for the first storage change.
2. Implement the shared domain values and graph-update payloads in
   `kgi-model`, including identity, coordinate, score-range, consensus-order,
   VSPC, recovery/fault, status, and committed-update values required by the
   ingress boundary. Add focused tests for value invariants and recovery-mode
   ordering rather than tests that only mirror derives.
3. Implement `kgi-api-ingress` as the complete bounded session capability:
   channel construction, cloneable producer, single receiver, shared gate,
   coalescing gap observation, ordinary nonblocking offers, and lossless
   lifecycle-marker delivery. Exercise concurrent gate ordering, PreSeal
   suppression, full-channel gaps, closure, marker ordering, and wakeup
   coalescing with deterministic tests.
4. Run formatting, workspace unit and integration tests through Nextest,
   Cargo doctests, Clippy with warnings denied, and the Web build/test baseline.
   Update `status.md` with the durable choices and exact completed scope, then
   submit this increment for review before adding worker or API consumers.

After that increment, proceed in this order:

1. implement `kgi-core` configuration values and signal handling, then the
   top-crate configuration resolver and command entry;
2. implement NodeService normalization and validated-generation lifecycle
   against the accepted pinned upstream evidence;
3. select and record the PostgreSQL stack, then implement StorageService
   bootstrap, schema lifecycle, ownership lock, and validated processing/API
   generations; and
4. continue with persistence transactions and workers under the sequence
   below.

The first tranche deliberately avoids placeholder service APIs and broad mock
frameworks. Add a public component surface when its owning behavior is
implemented and can be verified end to end at the narrowest useful level.

## Completed implementation milestones

The first five rows are retained as historical implementation milestones. They
predate the reviewable-increment rule below and summarize work already merged;
they do not define future PR boundaries.

| Milestone | Work and prerequisite | Completion evidence |
|---|---|---|
| 1. Pin interfaces and test references | Create the settled [crate, repository, and bundle structure](../architecture/overview.md#repository-web-build-and-release-structure--settled), including the top `kgi` composition crate, `kgi-core` with its signal adapter based on rusty-kaspa `core/src/signals.rs`, storage-owned migration location, Web workspace, and `xtask` tooling. Resolve the PostgreSQL client, migration framework, concrete SQL types, and internal module boundaries in the [deferred decision register](../decisions/deferred.md). Pin dependency and reference revisions and run the [PUAR](../architecture/verification.md#pinned-upstream-assumption-review-policy). Establish Go KGI parity fixtures, PostgreSQL integration setup, and browser graph fixtures. Define shared types, internal worker commands, bounded data-channel contracts, graph-update payloads and gap signaling, concrete error types, and the Supervisor-facing component methods. | A buildable workspace, reproducible empty release bundle, the dated PUAR report, tests of KGI-owned interface behavior, and recorded implementation choices. |
| 2. Build validated capabilities | Implement `NodeService`, one-connection `ValidatedRpcClient`, the ordered `NodeServiceEvent` stream, RPC validation/normalization, and all-or-nothing notification routing. Implement `StorageService`, schema/network validation, processing and API validated DB generations, the ordered `StorageServiceEvent` stream, migrations, and transaction/cache publication rules. These can advance independently once milestone 1 interfaces are stable. | Connection-generation, ordered publication/retirement, stale-event suppression, network/schema rejection, IBD, subscription failure, GetBlocks normalization, and ambiguous DB commit tests. |
| 3. Implement persistence invariants | Add identity/materiality lookup, ordered hash interning, transactional block materialization and coordinate allocation, PP rebuild transaction, committed VSPC sink derivation, atomic VSPC coloring and final level DAA scores. Keep ordinary orphans in memory and boundary identities permanent. | PostgreSQL tests for PP `(1,0)`, anticone levels, strict versus PreSeal references, dedup, no boundary promotion, VSPC reorgs, and crash/ambiguous seal outcomes. |
| 4. Establish database replacement safety | Implement the autonomously reconnecting read-only API pool, the API variants of StorageService's ordered generation stream, shared/exclusive replacement gate, generation retirement, and bounded database-phase cancellation/drain. Prove an old detached projection or clean 503 outcome for Rebuild races, including the chosen PostgreSQL clear strategy. Keep public Rebuild historical reads closed until an aligned replacement becomes Active; leave them available for Resync. | PostgreSQL race tests for concurrent API database phases, rebuild, independent API-pool reconnection, ordered generation events, and any `TRUNCATE` behavior; bounded gate completion. This milestone must pass before enabling a runnable rebuild. |
| 5. Build processor workers | Implement OrphanManager, DependencyResolver, and BlockProcessor's lifecycle-marker worker, then BlockProcessor's priority/gates/materialization path. Implement VspcProcessor's pending indexes, source/destination continuity, phase-specific pruning and overlap. Give every processing session a fresh ordered graph-update ingress with its settled producer gate; preserve block-before-`PersistedBlock` order and report nonblocking delivery gaps. | Worker tests for topology, cancellation races, priorities, pre-seal suppression, full versus closed channels, gap signaling, causal processor milestones, lossless BlockProcessor-delivered lifecycle markers, PreSeal seal transition, VSPC sequencing, and duplicate detection. |

## Remaining reviewable increments

Beginning with increment 6, one numbered increment is exactly one pull request.
An increment may contain multiple ordered commits and its review corrections,
but it must not share a PR with an adjacent increment. Do not begin the next
increment until the current PR is merged and the next branch is based on the
updated main branch. If implementation exposes a missing architecture decision,
resolve and commit that prerequisite before resuming the affected increment;
do not enlarge the PR silently. Any future regrouping requires updating this
sequence first.

The former broad Step 6 is now increments 6–17, former Step 7 is increments
18–22, and former Step 8 is increments 23–27. Increments 6 and 7 are complete;
the active work is increment 8, without an additional nesting level.

| Increment / PR | Isolated production scope | Completion evidence |
|---|---|---|
| 6. Canonical graph model | Complete the pure `kgi-api-model` graph values, seeded and extracted views, Head and Fixed mutation, delta application and composition, and level-scoped history. Add no publication, database, cache, encoding, router, or SSE runtime. | Focused graph-model tests for every mutation, extraction, composition, and history contract, followed by the full Rust validation suite. |
| 7. API graph projection reads | Add the API-owned seed request/result values needed by Storage and implement `ValidatedApiDbClient::load_graph_view_seed` plus the indexed historical anchor reads. Keep all SQL, snapshot, generation-loss, and projection-coherence behavior in `kgi-storage`; add no publication worker or HTTP path. | PostgreSQL tests for Head and every anchored seed, stable-snapshot consistency, crossing edges and external levels, DAA floor/tie behavior, normal misses, incoherent projection, cancellation, and exact API-generation loss. |
| 8. Publication state and mutation kernel | Implement `GraphPublication` state, writer serialization, atomic view/history advancement, immutable captures, lifecycle transitions, and projection-failure detection in `kgi-api-core`. Do not yet construct publications from Storage or expose request serving. | Deterministic writer-gate and atomic-core tests for state ordering, cancellation, no-effect updates, capture coherence, pruning-safe history handles, and failure isolation. |
| 9. Graph wire values and codec | Add the remaining public protocol values and the DTO conversion/serialization boundary, including response-local hash dictionaries, decimal-string integers, delta-entry projection, common errors, status DTOs, and semantic ETag inputs. Reuse the seed and anchor values introduced by increment 7. Keep scheduling, gzip execution, cache retention, and HTTP routing out of this PR. | Codec and schema tests covering every DTO variant, exact null/discriminator rules, dictionary reuse, unordered traversal equivalence, omitted wire removals, full-width integers, colors, and safe errors. |
| 10. Encoding scheduler and response cache | Implement the bounded blocking encoding scheduler, JSON size enforcement and gzip execution, publication-local single-flight cache slots, Head tiers, source-keyed delta entries, heat bookkeeping, weak insertion, and tracked cache-job completion. | Deterministic queue, priority, cancellation, hard-age retry, tier selection, single-flight, weak-cache, shared-body, size-limit, gzip-failure, and scheduler-shutdown tests. |
| 11. Client wake registry and SSE mailboxes | Implement the publication-local client registry and bounded ordered mailbox independently of Axum transport. Cover registration, lifecycle messages, graph-wakeup scheduling and coalescing, completion-time rearming, replacement, disconnect, and slow-client removal. | Race tests for atomic required batches, state-before-wakeup ordering, Head/rearm linearization, stale registrations, replacement sequences, queue bounds, and closure. |
| 12. API database binding | Implement `ApiDbState`, ordered exact-generation updates, public-read admission, construction-side waiting, Rebuild gating, and terminal stop behavior. Do not add seed construction or public request handlers. | Tests for stale and repeated events, invalid handles, waiting and wakeup, Rebuild disable/reopen authority, generation turnover, and stop/update races. |
| 13. Publication runtime | Implement the session-bound `PublicationRuntime`: PreSeal handling, database seeding, staging, dual-cut alignment, prewarming, activation requests, gap reconstruction, projection-failure reconstruction, quiescent receiver closure, and its completed shutdown barrier. | Deterministic state-machine tests for every construction state, both alignment cuts, idle-source waits, overflow and gap cuts, sticky Live, supersession, prewarm retry, database failure classes, worker failure, and cancellation with no surviving task. |
| 14. ApiService lifecycle and publication installation | Implement ApiService's event path, runtime generation fence, reset/current-publication slots, publication installation, cancellation roots, status aggregation, and terminal service barrier. This PR integrates increments 8–13 but adds no Axum router. | Control-level tests for Resync/Rebuild reset, predecessor overlap and join, superseded installation, publication replacement, database-read reopening, unexpected worker failure, shutdown from every runtime state, task/resource release, and idempotence. |
| 15. In-memory Head operations | Implement the logical Head snapshot, canonical delta, and retained Head-level lookup operations over one captured publication, including cache selection, history budgeting, continuation and registration completion. Keep transport parsing and Axum response delivery out of this PR. | Operation tests for tiers and ETags, every delta outcome and distance/budget boundary, cache bridges and heat, Stale catch-up, all-or-nothing levels, publication mismatch, and request-local failures. |
| 16. Anchored windows, status, and API admission | Implement Head-first anchored-window resolution with database fallback, detached historical projections, status composition, the independent Head/historical/status admission lanes, database-read permits and timeouts, and request-owned resource accounting. | Tests for all anchors and misses, immutable-source selection, DB/gate races, detached-resource lifetime, status independence, lane saturation, query timeout, and no partial graph. |
| 17. HTTP and SSE transport | Implement the `kgi-api-core` Axum router, strict request parsing, HTTP outcome/header mapping, gzip requirements, body delivery guards/timeouts, SSE connection tasks, API fallback, and method handling over the completed logical operations. | Router-level tests for every route and method, malformed input precedence, exact headers/errors/cache policy, conditional requests, SSE framing/replacement/slow clients, committed-response failure, middleware isolation, and router shutdown. |
| 18. ProcessingSession construction and preparation | Implement the non-reusable session owner, inert actor-tree construction, Resync and Rebuild preparation, processor initialization/launch, startup outcomes and cancellation, initial marker handoff, and consuming actor-tree shutdown. Do not yet add the synchronization pump or Supervisor. | Session tests for both preparation modes and all typed outcomes, threshold construction, exact generation retention/release, startup cancellation races, partial-launch cleanup, marker acceptance, and complete descendant joining. |
| 19. ProcessingSession synchronization and Live transition | Add the common GetBlocks/VSPC pump, rolling Catchup trigger and fallbacks, notification activation, synthetic dispatch, Catchup accounting, overlap admission, producer shutdown, ordered processor Live commands, and `EnteredLive`. | Deterministic pump tests for page/cursor order, held-page behavior, marker refresh, all fallbacks, malformed and cancelled RPC outcomes, cross-page deduplication, overlap at complete page boundaries, and abandonment of synthetic work at Live. |
| 20. Supervisor recovery orchestration | Implement service-event resource binding, exact-generation retirement coalescence, fault/recovery policy and backoff, session slot transitions, API DB event forwarding, per-attempt ingress/reset, milestone handling, retry/Fatal behavior, and status publication. | End-to-end orchestration tests for startup, Resync-to-Rebuild replacement, stronger intent, malformed-input budget, persistence and ownership faults, stale service events, API forwarding, same-generation reuse, and no cross-session state. |
| 21. Logging and administrative command | Initialize the retained rusty-kaspa-derived console/file logging after validated configuration and implement the complete one-shot database reinitialization dispatch and exit behavior. Preserve secret redaction and validation-before-effects; leave ordinary service dispatch to increment 22 rather than adding a placeholder composition path. | Command-level tests for help/version, logging modes and failure, administrative success and failure exits, redaction, startup ordering, and absence of every long-lived component in the one-shot command. |
| 22. Service and HTTP composition with global shutdown | Implement ordinary service dispatch; compose the long-lived service `Arc`s, Supervisor, listener, API router, runtime Web configuration and static files; install the Supervisor-owned signal adapter; run Axum; and implement the complete concurrent global shutdown order. | Integration and subprocess tests for startup ordering, API/static fallback isolation, runtime configuration and assets, service rejection, Axum failure, first/second/third signals, session/API/server concurrency, event draining, and final NodeService-then-StorageService shutdown. |
| 23. Web protocol boundary and runtime configuration | Replace the imported v1 transport types with strict v2 DTO decoding, decimal-string `bigint` values, response-local hash resolution, API error handling, publication headers, and `/kgi-config.json` plus explorer-template behavior. Do not yet implement live replay or change graph presentation policy. | Vitest coverage for every DTO/error variant, malformed payload rejection, full-width values, colors, dictionaries, runtime-config caching/failures, and safe explorer expansion. |
| 24. Web update acquisition and replay | Implement SSE registration/state/wakeup handling, single in-flight delta acquisition, continuation behavior, bounded batch admission, publication-local replay timing, cursor separation, replacement, reconnect, and cancellation. | Deterministic Vitest and fixture-server tests for ordered batches, queue bounds, replay pacing/acceleration, stale completion, identifier races, transport failure, malformed batches, and publication replacement. |
| 25. Web Head and Fixed graph behavior | Adapt the retained UI and visualization model to hash identity, Head snapshot trimming, canonical delta application, fixed-window filtering and freeze, missing-level lookup, DAA focus, Genesis recognition, explicit refresh, and external explorer presentation. | Graph-state unit tests plus Playwright flows for Head follow, fixed retention/freeze, crossing edges, level enrichment, DAA anchors, selection/navigation, canvas startup, and publication replacement. |
| 26. Deployment and release artifacts | Complete the conventional and Docker layouts, runtime log directories, systemd and Compose examples, static-asset cache behavior, and atomic `xtask` bundle/container construction against the composed binary and Web build. | Reproducible bundle and image checks, matching binary/assets and provenance, missing-root failure, unprivileged/read-only deployment, writable logs, direct signal delivery, and atomic failure cleanup. |
| 27. Full-stack acceptance | Add only the remaining cross-component fixtures and acceptance coverage needed to prove the focused verification contract across node RPC, PostgreSQL, recovery, API/Web, packaging, logging, and resource isolation; update implementation status with the final evidence. | Full Rust, PostgreSQL, Vitest, deterministic and complete-stack Playwright, parity, packaging, shutdown, and load/isolation suites pass from a clean checkout. |

### Active increment 8: publication state and mutation kernel

Increment 8 is exactly one reviewable PR. It implements the in-memory
[publication synchronization and capture kernel](../architecture/api-publication.md#publication-synchronization-and-request-capture--settled)
in `kgi-api-core` over the canonical graph model completed by increment 6.
Publication construction from Storage, runtime reconstruction, response-cache
work, client wake delivery, logical request operations, encoding, transport,
and HTTP serving belong to later increments.

Execute increment 8 as these ordered points:

1. Record the private publication-module boundary, direct crate dependencies,
   publication-identity generation, and the concrete cheap history-cost
   estimator weights used by this first dependent implementation.
2. Add the publication state, atomic core, and kernel owner; initialize it from
   an already constructed Head `GraphView` and its matching `GraphHistory`, and
   provide the publication-local identity and monotonic revision clock.
3. Implement serialized original `BlockCommitted` and `VspcCommitted`
   application, view/history advancement and pruning, no-effect handling, and
   typed detection of publication-projection failures.
4. Implement publication lifecycle transitions and the immutable view,
   delta-target, and history-range captures whose value dependencies already
   exist. Defer protocol-owned level capture, client-registry notification,
   and cache integration to their owning later increments.
5. Add deterministic tests for the writer gate and atomic core, mutation and
   state ordering, cancellation before atomic mutation, no-effect updates,
   revision timestamps, immutable capture coherence, retained-history handles
   surviving pruning, fresh publication identities, and projection-failure
   isolation.
6. Update implementation status and run formatting, workspace unit and
   integration tests through Nextest, Cargo doctests, Cargo check, Clippy with
   warnings denied, and `git diff --check`.

This increment adds no Storage calls, publication worker, `GraphCache`,
`DeltaClientRegistry`, level DTO, ApiService control, encoding scheduler,
router, HTTP path, or SSE behavior. Its production scope is `kgi-api-core`;
the existing graph model changes only if focused testing exposes a defect in
its owned behavior.

## Cross-increment ordering

The remaining increments are merge-ordered even where their internal contracts
could be developed independently. This keeps each new branch based on the
reviewed public and private interfaces of its prerequisites and makes one PR
the complete review unit for one increment. The API remains mandatory rather
than optional; database replacement safety remains in StorageService, and loss
of ordinary graph updates reconstructs only the API image.

## Decisions to make during implementation

The [deferred decision register](../decisions/deferred.md) is the sole list of
choices left to implementation. Resolve each entry at its first dependent step
and record durable choices while preserving settled behavior.
Fault classification and retry/backoff defaults are fixed by the
[processing lifecycle contract](../architecture/processing-lifecycle.md) and
must not change while resolving deferred choices. Assess critical rusty-kaspa
ordering and notification assumptions through the PUAR, and test KGI's
behavior for valid inputs and observable violations. A genuine architecture
ambiguity or conflict goes to the Architecture role before dependent
implementation continues.

KGI v2.1 candidates in [future work](../future-work.md) are excluded unless an
accepted architecture decision promotes them.
