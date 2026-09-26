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

## Sequence after the gate

| Step | Work and prerequisite | Completion evidence |
|---|---|---|
| 1. Pin interfaces and test references | Resolve workspace/crate/module layout, PostgreSQL client, migration framework, and concrete SQL types in the [deferred decision register](../decisions/deferred.md). Pin dependency and reference revisions and run the [PUAR](../architecture/verification.md#pinned-upstream-assumption-review-policy). Establish Go KGI parity fixtures, PostgreSQL integration setup, and browser graph fixtures. Define shared types, worker commands, bounded data-channel contracts, graph observer payloads, concrete error types, and a separate acknowledged API Reset control. | A buildable skeleton, the dated PUAR report, tests of KGI-owned interface behavior, and recorded implementation choices. |
| 2. Build validated capabilities | Implement `NodeService` and one-connection `ValidatedRpcClient`, RPC validation/normalization, and all-or-nothing notification routing. Implement `StorageService`, schema/network validation, one-generation `ValidatedDbClient`, migrations, and transaction/cache publication rules. These can advance independently once step 1 interfaces are stable. | Connection-generation, network/schema rejection, IBD, subscription failure, GetBlocks normalization, and ambiguous DB commit tests. |
| 3. Implement persistence invariants | Add identity/materiality lookup, ordered hash interning, transactional block materialization and coordinate allocation, PP rebuild transaction, committed VSPC sink derivation, atomic VSPC coloring and final level DAA scores. Keep ordinary orphans in memory and boundary identities permanent. | PostgreSQL tests for PP `(1,0)`, anticone levels, strict versus PreSeal references, dedup, no boundary promotion, VSPC reorgs, and crash/ambiguous seal outcomes. |
| 4. Establish API reset safety | Implement the read-only API pool and bounded historical-read admission, cancellation/drain, and acknowledged `Reset`. Prove pre-reset in-flight reads yield one old coherent image or 503, including the chosen PostgreSQL clear strategy. Keep historical reads closed until a coherent new API image is publishable. | PostgreSQL race tests for Reset, concurrent historical reads, rebuild, and any `TRUNCATE` behavior; bounded barrier completion. This step must pass before enabling a runnable rebuild. |
| 5. Build processor workers | Implement OrphanManager and DependencyResolver, then BlockProcessor's priority/gates/materialization path. Implement VspcProcessor's pending indexes, source/destination continuity, phase-specific pruning and overlap. Route committed graph updates on one ordered channel, with block update before `PersistedBlock` delivery. | Worker tests for topology, cancellation races, priorities, full versus closed channels, PreSeal seal transition, VSPC sequencing, duplicate detection, and observer loss. |
| 6. Publish the in-process graph API | Resolve the open graph-model items, endpoint URLs, HTTP methods, wire schema and format, `MAX_WINDOW_DEPTH`, and API resource budgets from the applicable decision registers, then implement `GraphPublication`, `GraphView`, level-scoped `GraphHistory`, snapshot/replay, external parents, invalidation/reload, publication revisions, response-local hash dictionaries, composable deltas, SSE, ETags, DAA/window queries, status, and bounded API resources. Reopen historical reads only with a coherent new image. | Snapshot/replay, terminal observer loss, delta composition/expiry, replaced publication, slow SSE clients, DAA tie/floor, crossing edges, and API saturation tests. |
| 7. Integrate recovery lifecycle | Implement ResyncEngine preparation, common GetBlocks/VSPC pump, Catchup overlap, the ordered processor/global Live transition, session teardown, and Supervisor recovery intent. Start usable RPC/DB acquisition concurrently. Route rebuild through the tested API Reset acknowledgement before DB clear. | End-to-end Resync, Rebuild, recovery escalation, interruption, retained `--clear-db` intent, session-clone release, VSPC synthetic-input abandonment at Live, successful-enqueue accounting, Catchup cross-page dedup, continued VSPC notification progress, current-DB-sink Resync disposition, and complete-page overlap-based Live-boundary tests. |
| 8. Integrate Web behavior and verify release | Resolve the adaptive fixed-view delay curve and cap from the [deferred decision register](../decisions/deferred.md). Adapt the Web graph model to hash identity, SSE cursor catch-up, head-following and fixed-view freeze, DAA anchor behavior, and distance-adaptive updates. Run Go parity, KGI RPC handling, PostgreSQL, browser, recovery, and resource-isolation tests against the complete stack. | A stable reviewable implementation commit/diff, updated implementation status, and evidence for the focused [verification contract](../architecture/verification.md). |

Steps 5 and 6 may be developed in parallel after their shared observer and
Reset protocols are fixed, but integration must preserve their causal order.
The API cannot be treated as a later optional service: Reset is on the rebuild
safety path, while loss of ordinary graph updates only invalidates the API
image.

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
