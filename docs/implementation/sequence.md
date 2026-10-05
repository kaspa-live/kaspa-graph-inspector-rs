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

## Sequence after the gate

| Step | Work and prerequisite | Completion evidence |
|---|---|---|
| 1. Pin interfaces and test references | Create the settled [crate, repository, and bundle structure](../architecture/overview.md#repository-web-build-and-release-structure--settled), including the top `kgi` composition crate, `kgi-core` with its signal adapter based on rusty-kaspa `core/src/signals.rs`, storage-owned migration location, Web workspace, and `xtask` tooling. Resolve the PostgreSQL client, migration framework, concrete SQL types, and internal module boundaries in the [deferred decision register](../decisions/deferred.md). Pin dependency and reference revisions and run the [PUAR](../architecture/verification.md#pinned-upstream-assumption-review-policy). Establish Go KGI parity fixtures, PostgreSQL integration setup, and browser graph fixtures. Define shared types, internal worker commands, bounded data-channel contracts, graph-update payloads and gap signaling, concrete error types, and the Supervisor-facing component methods. | A buildable workspace, reproducible empty release bundle, the dated PUAR report, tests of KGI-owned interface behavior, and recorded implementation choices. |
| 2. Build validated capabilities | Implement `NodeService`, one-connection `ValidatedRpcClient`, the ordered `NodeServiceEvent` stream, RPC validation/normalization, and all-or-nothing notification routing. Implement `StorageService`, schema/network validation, processing and API validated DB generations, the ordered `StorageServiceEvent` stream, migrations, and transaction/cache publication rules. These can advance independently once step 1 interfaces are stable. | Connection-generation, ordered publication/retirement, stale-event suppression, network/schema rejection, IBD, subscription failure, GetBlocks normalization, and ambiguous DB commit tests. |
| 3. Implement persistence invariants | Add identity/materiality lookup, ordered hash interning, transactional block materialization and coordinate allocation, PP rebuild transaction, committed VSPC sink derivation, atomic VSPC coloring and final level DAA scores. Keep ordinary orphans in memory and boundary identities permanent. | PostgreSQL tests for PP `(1,0)`, anticone levels, strict versus PreSeal references, dedup, no boundary promotion, VSPC reorgs, and crash/ambiguous seal outcomes. |
| 4. Establish database replacement safety | Implement the autonomously reconnecting read-only API pool, the API variants of StorageService's ordered generation stream, shared/exclusive replacement gate, generation retirement, and bounded database-phase cancellation/drain. Prove an old detached projection or clean 503 outcome for Rebuild races, including the chosen PostgreSQL clear strategy. Keep public Rebuild historical reads closed until an aligned replacement becomes Active; leave them available for Resync. | PostgreSQL race tests for concurrent API database phases, rebuild, independent API-pool reconnection, ordered generation events, and any `TRUNCATE` behavior; bounded gate completion. This step must pass before enabling a runnable rebuild. |
| 5. Build processor workers | Implement OrphanManager, DependencyResolver, and BlockProcessor's lifecycle-marker worker, then BlockProcessor's priority/gates/materialization path. Implement VspcProcessor's pending indexes, source/destination continuity, phase-specific pruning and overlap. Give every processing session a fresh ordered graph-update ingress with its settled producer gate; preserve block-before-`PersistedBlock` order and report nonblocking delivery gaps. | Worker tests for topology, cancellation races, priorities, pre-seal suppression, full versus closed channels, gap signaling, causal processor milestones, lossless BlockProcessor-delivered lifecycle markers, PreSeal seal transition, VSPC sequencing, and duplicate detection. |
| 6. Publish the in-process graph API | Implement the settled [Axum/Tower composition](../architecture/overview.md#http-composition-and-runtime-web-configuration--settled), [JSON wire schema](../architecture/api-protocol.md#common-transport-dto-rules--settled), [mandatory gzip path](../architecture/api-protocol.md#graph-http-compression--settled), ApiService cancellation domains, custom publication-local single-flight cache slots, ordered bounded SSE mailboxes and client wake registry, and focused-owner resource and response-memory ownership limits, then implement `GraphPublication`, `GraphView`, level-scoped `GraphHistory`, explicit construction/alignment, universal drain-and-rebase reconstruction, external parents, publication-timed revisions, response-wide hash dictionaries, composable Head deltas, ordered replay batches, retained Head-level lookup, SSE, ETags, DAA/window queries, status, bounded API resources, and the terminal shutdown barrier. | Router-state, API-fallback, middleware-isolation, independent request/SSE/cache cancellation, cache builder election, shared watch completion, weak cache insertion, atomic SSE registration batches, wakeup coalescing, rearm/Head races, state-before-wakeup ordering, state-machine, alignment-boundary, gap/rebase, graph-delta composition, ordered delta-batch replay/expiry and fixed-window filtering, all-or-nothing Head-level lookup, replaced publication, slow SSE clients, DAA tie/floor, crossing-edge, API saturation, shared cached-body delivery without per-waiter copies or a second response-memory semaphore, and shutdown resource-release tests. |
| 7. Integrate recovery lifecycle | Implement ResyncEngine preparation, common GetBlocks/VSPC pump, Catchup overlap, the ordered processor/global Live transition, session teardown, top-crate Supervisor recovery intent, Supervisor-owned termination handling, and global shutdown ordering. Retain NodeService's published RPC generation and StorageService's published processing DB generation until both can form a new session. Have Supervisor react to either retirement through session teardown, use replacements only for new sessions, preserve typed malformed-response faults that follow RPC retirement, map ordered API DB events through ApiService's control method, install each API ingress through `reset`, let BlockProcessor deliver lifecycle markers independently, and route rebuild through the tested StorageService replacement gate. | End-to-end Resync, Rebuild, RPC plus processing and API DB generation loss and autonomous replacement, exact-generation stale-event suppression, retirement/fault coalescence, recovery escalation including consecutive Resync/Rebuild resets, interruption, retained `--clear-db` intent, session-clone release, VSPC synthetic-input abandonment at Live, successful-enqueue accounting, Catchup cross-page dedup, continued VSPC notification progress, current-DB-sink Resync disposition, complete-page overlap-based Live-boundary tests, idempotent first- and second-signal shutdown requests, third-signal forced exit, and concurrent API/processing shutdown before service shutdown. |
| 8. Integrate Web behavior and verify release | Import the KGI v1 React application, replace Create React App with Vite as an isolated tooling change, and then adapt the Web graph model to hash identity, SSE cursor catch-up, head-following and fixed-view freeze, DAA anchor behavior, and runtime Web configuration. Run Vitest, deterministic and full-stack Playwright flows, Go parity, KGI RPC handling, PostgreSQL, recovery, packaging, logging, and resource-isolation tests against the complete stack. | A stable reviewable implementation commit/diff, an atomic conventional bundle and equivalent container image, updated implementation status, and evidence for the focused [verification contract](../architecture/verification.md). |

Steps 5 and 6 may be developed in parallel after their shared graph-update and
`reset` interfaces are fixed, but integration must preserve their causal order.
The API cannot be treated as a later optional service, while database
replacement safety belongs to StorageService and loss of ordinary graph updates
only reconstructs the API image.

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
