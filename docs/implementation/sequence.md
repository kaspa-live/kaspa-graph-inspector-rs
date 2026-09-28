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
| 1. Pin interfaces and test references | Resolve workspace/crate/module layout, PostgreSQL client, migration framework, and concrete SQL types in the [deferred decision register](../decisions/deferred.md). Pin dependency and reference revisions and run the [PUAR](../architecture/verification.md#pinned-upstream-assumption-review-policy). Establish Go KGI parity fixtures, PostgreSQL integration setup, and browser graph fixtures. Define shared types, worker commands, bounded data-channel contracts, graph-update payloads and gap signaling, concrete error types, and the reliable API Reset and Shutdown controls. | A buildable skeleton, the dated PUAR report, tests of KGI-owned interface behavior, and recorded implementation choices. |
| 2. Build validated capabilities | Implement `NodeService` and one-connection `ValidatedRpcClient`, RPC validation/normalization, and all-or-nothing notification routing. Implement `StorageService`, schema/network validation, one-generation `ValidatedDbClient`, migrations, and transaction/cache publication rules. These can advance independently once step 1 interfaces are stable. | Connection-generation, network/schema rejection, IBD, subscription failure, GetBlocks normalization, and ambiguous DB commit tests. |
| 3. Implement persistence invariants | Add identity/materiality lookup, ordered hash interning, transactional block materialization and coordinate allocation, PP rebuild transaction, committed VSPC sink derivation, atomic VSPC coloring and final level DAA scores. Keep ordinary orphans in memory and boundary identities permanent. | PostgreSQL tests for PP `(1,0)`, anticone levels, strict versus PreSeal references, dedup, no boundary promotion, VSPC reorgs, and crash/ambiguous seal outcomes. |
| 4. Establish database replacement safety | Implement the read-only API pool and StorageService's shared/exclusive replacement gate, API-generation retirement, and bounded database-phase cancellation/drain. Prove an old detached projection or clean 503 outcome for Rebuild races, including the chosen PostgreSQL clear strategy. Keep public Rebuild historical reads closed until an aligned replacement becomes Active; leave them available for Resync. | PostgreSQL race tests for concurrent API database phases, rebuild, and any `TRUNCATE` behavior; bounded gate completion. This step must pass before enabling a runnable rebuild. |
| 5. Build processor workers | Implement OrphanManager, DependencyResolver, and BlockProcessor's lifecycle-marker worker, then BlockProcessor's priority/gates/materialization path. Implement VspcProcessor's pending indexes, source/destination continuity, phase-specific pruning and overlap. Give every processing session a fresh ordered graph-update ingress with its settled producer gate; preserve block-before-`PersistedBlock` order and report nonblocking delivery gaps. | Worker tests for topology, cancellation races, priorities, pre-seal suppression, full versus closed channels, gap signaling, causal processor milestones, lossless BlockProcessor-delivered lifecycle markers, PreSeal seal transition, VSPC sequencing, and duplicate detection. |
| 6. Publish the in-process graph API | Resolve endpoint URLs, HTTP methods, wire schema and format, `MAX_WINDOW_DEPTH`, and API resource budgets from the applicable decision registers, then implement `GraphPublication`, `GraphView`, level-scoped `GraphHistory`, explicit construction/alignment, universal drain-and-rebase reconstruction, external parents, publication revisions, response-local hash dictionaries, composable deltas, SSE, ETags, DAA/window queries, status, bounded API resources, and the terminal shutdown barrier. | State-machine, alignment-boundary, gap/rebase, delta composition/expiry, replaced publication, slow SSE clients, DAA tie/floor, crossing-edge, API saturation, and shutdown resource-release tests. |
| 7. Integrate recovery lifecycle | Implement ResyncEngine preparation, common GetBlocks/VSPC pump, Catchup overlap, the ordered processor/global Live transition, session teardown, Supervisor recovery intent, and global shutdown ordering. Start usable RPC/DB acquisition concurrently. Have Supervisor install each API ingress without awaiting Reset, let BlockProcessor deliver lifecycle markers independently, and route rebuild through the tested StorageService replacement gate. | End-to-end Resync, Rebuild, recovery escalation including consecutive Resync/Rebuild Resets, interruption, retained `--clear-db` intent, session-clone release, VSPC synthetic-input abandonment at Live, successful-enqueue accounting, Catchup cross-page dedup, continued VSPC notification progress, current-DB-sink Resync disposition, complete-page overlap-based Live-boundary tests, and concurrent API/processing shutdown before service shutdown. |
| 8. Integrate Web behavior and verify release | Resolve the adaptive fixed-view delay curve and cap from the [deferred decision register](../decisions/deferred.md). Adapt the Web graph model to hash identity, SSE cursor catch-up, head-following and fixed-view freeze, DAA anchor behavior, and distance-adaptive updates. Run Go parity, KGI RPC handling, PostgreSQL, browser, recovery, and resource-isolation tests against the complete stack. | A stable reviewable implementation commit/diff, updated implementation status, and evidence for the focused [verification contract](../architecture/verification.md). |

Steps 5 and 6 may be developed in parallel after their shared graph-update and
Reset protocols are fixed, but integration must preserve their causal order.
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
