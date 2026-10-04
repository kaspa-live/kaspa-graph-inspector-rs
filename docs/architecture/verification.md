# Verification contract

## Scope and evidence rules

This document owns the minimum evidence required for the KGI v2 architecture.
It does not redefine component behavior: each requirement below links to the
focused contract whose observable result it verifies.

Use the narrowest useful test level for behavior owned by KGI, but retain
integration coverage wherever the result depends on PostgreSQL, browser graph
behavior, or a multi-worker ordering boundary. Inject clocks and jitter sources
when wall-clock behavior is under test. Upstream correctness assumptions use
the source-analysis policy below rather than executable fixtures created only
to prove rusty-kaspa behavior.

These obligations are the required baseline. Additional matrix breadth belongs
to the [deferred decision register](../decisions/deferred.md).

## Pinned Upstream Assumption Review policy

The **Pinned Upstream Assumption Review**, abbreviated **PUAR**, is the specific
source analysis of KGI's upstream correctness assumptions against one exact
rusty-kaspa commit. The current pinned reference revision is:

```text
c338d495bec29e4dc8b5149f99e8db6fa916ed4a
```

This pin makes the architecture analysis reproducible. It identifies the
rusty-kaspa source used to assess KGI's assumptions; it is not a runtime
version restriction. KGI may connect to other node versions after ordinary
validation, but the PUAR provides no correctness claim for those versions or
for custom builds.

The PUAR checklist is:

1. For [Genesis discovery](node-service.md#genesis-discovery),
   `GetBlocks(None, false, false)` returns the configured Genesis hash first and
   returns no blocks.
2. For the NodeService
   [individual recovery GetBlock](node-service.md#individual-recovery-getblock)
   used by [Resync preparation](processing-lifecycle.md#resync-preparation),
   header-only `GetBlock` supplies the required GhostDAG data and establishes
   that the returned block is recognized as a GetBlocks low hash.
3. For the NodeService
   [GetBlocks normalization](node-service.md#getblocks-and-vspc-recovery-responses)
   used by the [Catchup trigger](processing-lifecycle.md#catchup-trigger), the
   `L + 1` GetBlocks core budget, consensus-topological ordering, page
   construction, global-maximum fallback premise, and the `< 3` normalized
   length sink-reaching premise match the design.
4. For [notification routing](node-service.md#notificationrouter) and
   [VSPC change semantics](vspc-processing.md#change-semantics--settled),
   virtual selected-sink behavior preserves the documented removed/added
   ordering, excludes changes with a nonempty removed chain and an empty added
   path, can emit the documented fully empty no-op, and never places Genesis in
   `added` or `removed`.
5. For [notification routing](node-service.md#notificationrouter) and
   [block overlap](block-processing.md#catchup-filtering-and-overlap),
   BlockAdded duplicate and verbose-data behavior matches those contracts,
   including the enrichment-failure form without verbose data.
6. For NodeService
   [consensus parameter resolution](node-service.md#consensus-parameter-resolution),
   exact-network, unsupported-testnet fallback, and devnet/simnet override
   resolution produce the documented local `bps`, `mergeset_size_limit`, and
   `anticone_finalization_depth` values. The review records that RPC exposes no
   comparison with the node's effective overrides; it does not describe these
   local values as node-validated.
7. For NodeService
   [RPC normalization](node-service.md#getblocks-and-vspc-recovery-responses),
   VSPC V2 with `min_confirmation_count = None` and
   `RpcDataVerbosityLevel::None` preserves the documented batching, complete
   removed suffix, minimal acceptance-data envelope, complete-prefix
   shortening, and advancing cursor behavior.
8. For [VSPC path attribution](vspc-processing.md#readiness-and-materiality--settled),
   a valid retained-range VSPC query emits adjacent chain-path members using
   the same GhostDAG selected-parent relation that enriched GetBlock exposes.

For each item, inspect the committed source at the full SHA and report one of:

```text
Confirmed
Not confirmed
Contradicted
```

Each result contains a concise conclusion, relevant rusty-kaspa paths and
symbols, and any material KGI impact or limitation. A PUAR is evidence about
the reference revision rather than proof across node versions. No fixture or
test is required solely to establish an upstream checklist item. Executable
tests remain required for KGI's handling of valid input and observable
violations.

The instruction **Run the Pinned Upstream Assumption Review** runs the complete
checklist against the current reference revision. **Run a PUAR against
rusty-kaspa `<full-sha>`** selects an explicit revision. The resulting dated,
non-normative report belongs in:

```text
docs/reviews/YYYY-MM-DD-rusty-kaspa-<short-sha>-assumptions.md
```

A report does not change the reference pin or architecture automatically.
`Not confirmed` or `Contradicted` is reported to Architecture before dependent
implementation proceeds. Changing the reference revision requires a new PUAR;
otherwise repeat the review only when a compatibility problem or relevant
upstream change gives a concrete reason.

### Current PUAR result

Architecture accepts the
[24 September 2026 PUAR](../reviews/2026-09-24-rusty-kaspa-c338d495-assumptions.md)
against the full pinned revision above. All eight checklist items are
`Confirmed`; none is `Not confirmed` or `Contradicted`. The focused contracts
may therefore rely on those reviewed upstream behaviors for the pinned
revision, subject to their stated KGI validation and recovery rules.

This section is the sole normative owner of the PUAR acceptance status. The
report remains non-normative source analysis evidence. The accepted result does
not extend to another rusty-kaspa revision, a custom build, or a connected
node's undisclosed effective overrides.

### Accepted unverified upstream risk: stale-tip enumeration

The exact stale-body-tip enumeration boundary of GetBlocks is deliberately not
a PUAR checklist item. Whether lowering `low_hash` can expose every stored tip
outside the current Virtual traversal is an accepted unverified upstream risk.
The [recovery scope](processing-lifecycle.md#recovery-scope-and-omitted-body-tips)
does not claim body-DAG snapshot completeness or make Live admission depend on
that behavior, so a PUAR report need not assess this premise.

## NodeService and RPC behavior

Verify the [NodeService contract](node-service.md#nodeservice--settled) and
[RPC normalization](node-service.md#rpc-normalization) with:

1. Common full-block normalization produces the flattened
   `ValidatedNodeBlock` and rejects missing header or verbose data, inconsistent
   hashes, duplicate direct parents, contradictory self-reference, and an
   ordinary selected parent absent from the direct parents. Cover the exact
   validated-Genesis ORIGIN exception and its mandatory zero blue score
   separately.
2. GetBlocks fixtures cover the inclusive low hash, unequal hash/block vector
   lengths, hash/block disagreement, duplicates, a valid response that
   normalizes to zero blocks, and a page with one invalid full-block member.
   For every malformed recovery shape, assert typed whole-page rejection, no
   cursor or processor advancement, and retirement of the exact RPC
   generation.
3. Genesis discovery request construction with `low_hash = None`, blocks and
   transactions disabled, plus response validation for a nonempty hash vector,
   empty block vector, transport failure, and malformed output. Given a valid
   response, NodeService uses its first hash without substituting a local
   Genesis constant.
4. Consensus parameter resolution uses exact `NetworkId` parameters when
   supported. Mainnet emits no divergence warning. Every non-mainnet profile,
   including supported testnet and simnet, warns with the exact network,
   parameter source, and all four selected values, then continues. An
   unsupported testnet suffix uses testnet-family defaults. Devnet and simnet
   without an override-params file use their defaults; with the setting they
   parse and apply rusty-kaspa `OverrideParams`. An unreadable, malformed, or
   incompatible explicit file and use of the setting with mainnet or any
   testnet suffix fail configuration without fallback. No path represents the
   selected local values as having been compared with the node. Before any
   derived rusty-kaspa call, cover accepted target-time bounds `1` and `1000`,
   rejected values `0` and `1001`, the accepted minimum merge-set limit `2`,
   rejected smaller limits, and checked failures for the GetBlocks budget,
   VSPC batch size, and raw anticone expression. Also reject a derived
   anticone depth above `MAX_BLUE_SCORE` and assert every accepted Catchup
   threshold is at most `MAX_DAA_SCORE`. Every rejection returns
   `InvalidConsensusParameters`, publishes no usable parameter or client
   generation, performs no default fallback, and enters no connection retry.
5. Given a fully empty `VirtualChainChanged`, NodeService drops it before
   bounded delivery with no overlap credit, processor-capacity use, or
   recovery. Distinguish it from an empty VSPC V2 RPC page.
6. VSPC V2 request construction uses `min_confirmation_count = None` and
   `data_verbosity_level = Some(RpcDataVerbosityLevel::None)`. Mocked valid
   complete-prefix and advancing-cursor responses exercise KGI's pump behavior.
   Reject a removed chain without an added path, duplicate members,
   removed/added intersection, a nonadvancing nonempty added cursor, a nonempty
   removed path whose first hash differs from `low_hash`, and any occurrence of
   `low_hash` in `added` as the corresponding `MalformedVspcResponse` reason,
   with generation retirement and no cursor advancement.
7. Mocked notification and VSPC V2 responses with a nonempty removed chain and
   an empty added path. Assert the source-specific dispositions: a notification
   reports
   `NotificationInputInvalid(MalformedVspcChange(RemovedChainWithoutAddedPath))`
   and requires Resync;
   `ValidatedRpcClient` rejects a synthetic response as
   `MalformedVspcResponse(RemovedChainWithoutAddedPath)` without returning a
   normalized change, and ResyncEngine follows the shared malformed
   recovery-response policy without dispatch or cursor advancement.
8. The subscription activation order across the
   [processing lifecycle](processing-lifecycle.md#entering-recovery-phases) and
   [NotificationRouter](node-service.md#notificationrouter): ResyncEngine alone
   sends both processor Catchup commands before invoking NodeService activation;
   NodeService sends no processor command; routing remains Disabled until both
   remote starts succeed; and callbacks received during that interval are
   dropped without overlap credit or immediate recovery.
9. Verify the
   [current pruning-point block contract](node-service.md#current-pruning-point-block)
   with success, the Genesis exception, non-Genesis parent validation, every
   listed malformed response condition, exact-generation retirement, and
   transport or generation loss without inferring a database mismatch. An
   exact-Genesis response with blue score one is
   `RecoveryInputInvalid(MalformedPruningPointResponse)`, retires the producing
   RPC generation, consumes the shared malformed-input budget, and produces no
   boundary threshold, `PreparedSync`, or storage mutation. Supervisor may
   already have completed that attempt's `reset` call; the next attempt
   supersedes it with a fresh ingress.
10. Verify the
    [Catchup sink-sample contract](node-service.md#catchup-sink-sample) with
    ordinary and exact-Genesis success, ORIGIN, an advertised sink that is
    definitively not found, missing or malformed header data, wrong computed or
    reported hashes. Exact-Genesis success includes a representable nonzero DAA
    score. Every malformed case returns `MalformedCatchupSinkResponse`, retires
    the exact RPC generation, and consumes the shared malformed-input budget.
    Cover an out-of-range DAA score without generation retirement or budget
    consumption, transport and generation loss as session faults, and a
    distinct cancellation outcome.
11. Individual full-block GetBlock validates the requested hash and every
    `ValidatedNodeBlock` invariant. Malformed output retires the exact RPC
    generation; definitive not-found and transport failure retain their
    distinct classifications.
12. BlockAdded normalization failure disables routing, enqueues no block, and
    reports `NotificationInputInvalid(MalformedBlockAdded)` without retiring
    the RPC generation.
13. NotificationRouter applies the VSPC structural checks in their specified
    order. Exercise the valid empty no-op and each typed nonempty-removed,
    duplicate-member, and removed/added-intersection result; malformed input
    is not enqueued and disables routing without retiring the RPC generation.
14. Saturate each NotificationRouter destination independently. The triggering
    notification is not enqueued, both streams are disabled, and the router
    reports `SessionContinuityLost`; the particular destination remains
    diagnostic context and does not create a notification-specific fault.
15. Exercise `MAX_DAA_SCORE` and `MAX_BLUE_SCORE` successfully, then exceed
    each by one in full-block and header-only responses. Every excessive node
    value reports the corresponding `ScoreOutOfRange` fault, returns no
    normalized value, is Fatal without retiring the RPC generation, and does
    not consume the malformed recovery-response budget. Include BlockAdded,
    GetBlocks, current-pruning-point, Catchup sink-sample, and individual
    GetBlock sources. A full block timestamp of `u64::MAX` passes normalization
    unchanged and produces no timestamp-specific fault.

Use injected clocks and deterministic jitter to verify the independent
[NodeService](node-service.md#nodeservice--settled) and
[StorageService](storage.md#storageservice-lifecycle--settled) reconnect
sequences: nominal exponential slots through the 30-second cap, reset only
after 60 seconds continuously Ready, shutdown cancellation, and terminal
rejection without retry. Exercise the inclusive 50% through 100% jitter range.
Install the reliable ordered `NodeServiceEvent` path before initial RPC
publication or rejection. Verify initial and replacement `RpcPublished`, exact
`RpcRetired`, suppression of repeated retirement, retirement before an
operation returns its generation-ending error, terminal `Rejected`, and Fatal
unexpected event-path closure while Supervisor is Running.
Install the reliable ordered `StorageServiceEvent` path before either initial
DB generation can be published. Verify initial and replacement processing and
API publications, exact-`Arc` retirements, suppression of repeated retirement,
and a terminal `Rejected` event without relying on lossy status.

## Domain, storage, and PostgreSQL

Verify the [PP boundary model](domain-model.md#pruning-point-boundary-and-origin--settled),
[persistent representation](storage.md#persistent-representation--settled),
and [rebuild transaction](storage.md#rebuild-transaction--settled) with:

1. Rebuild materializes PP at `(level = 1, slot = 0)`; retained anticone roots
   and nonroots follow the parent-derived placement rule; boundary identities
   never become block rows.
2. Genesis and any PP whose selected parent is ORIGIN retain a non-null ORIGIN
   identity without inventing a direct parent. Web Genesis detection uses the
   actual absence of direct parents rather than visible-edge absence.
3. Hash interning follows the universal five-stage order. Strict
   materialization creates no boundary identity.
4. Database startup distinguishes Uninitialized, Empty, Initialized,
   structurally inconsistent processing contents, and rejected schemas.
   Persistent initialization authorization is idempotent for a compatible
   database and never rebinds its network. Interactive initialization
   identifies the database and complete network binding without exposing
   credentials; noninteractive initialization requires explicit
   authorization. `--clear-db`
   may authorize first initialization but never claims or rebinds an existing
   incompatible schema.
5. One-shot administrative reinitialization requires explicit confirmation,
   replaces only a recognized KGI schema, binds the new Empty database to the
   validated `(network_id, genesis_hash)`, and exits. A changed declarative
   token performs the same reset once; restart with the stored token preserves
   the database. Unknown tables are never claimed or destroyed, and neither
   form can expose a partially recreated schema.
6. Advisory-lock contention rejects with `DatabaseAlreadyInUse`, and lock loss
   retires both DB generations and emits their exact retirement events without
   directly changing processing-session or API publication state. Compatible
   v2 migrations are ordered,
   transactional, revalidated, and finish before client publication. Cover
   newer-schema and v1/unsupported rejection, failed migration without client
   publication, and the prohibition on automatic down or online migration.
7. Storage may open, lock, and inspect Uninitialized contents before node
   validation, but only atomic publication of complete `NodeMetadata` crosses
   `Uninitialized -> Empty`. Inject a crash around this transaction and prove
   that no partially bound Empty state can appear.
8. Node metadata is non-null and distinguishes Empty from Genesis-anchored
   initialization despite both using `db_pp_blue_score = 0`. Reject the same
   `NetworkId` paired with a different Genesis rather than rebinding or
   rebuilding.
9. A Genesis anchor is materialized at `(1,0)`, belongs to VSPC, has ORIGIN as
   selected parent and zero actual direct parents, and has a coherent committed
   sink. Any additional boundary identity classifies processing contents as
   Inconsistent and requires Rebuild.
10. `rebuild_from_pruning_point` either publishes the complete replacement or
   leaves the previous contents intact: Compact-ID allocation restarts, the PP
   level receives its VSPC DAA score, every block row is materialized, boundary
   identities remain identity-only, and replacement caches publish only after
   definite commit. An ambiguous commit retires the DB generation without
   reporting successful Rebuild.
11. In a processing-valid database, every `blocks` row represents the semantic
    Materialized state, including retained-past closure. Ordinary processing
    performs no individual block/identity deletion or boundary-identity
    promotion; parent rows have no foreign keys or cascade semantics; merge-set
    IDs validate transactionally; and coordinate uniqueness plus `levels.size`
    updates are enforced in the insertion transaction.
12. Verify the
    [reconciliation snapshot](storage.md#reconciliation-snapshot--settled) with
    `Existing` including its Materialized `node_pp` and committed sink,
    `NodePpNotMaterialized` for absent and identity-only node pruning points,
    coherent Empty, missing or incoherent sink contents, other Inconsistent
    contents, and operational storage failure as distinct outcomes.
13. Exercise checked score conversion at zero and both domain maxima. SQL
    rejects writes outside the persisted ranges. Compatible bound contents
    with a negative score or the DAA sentinel stored as a real block score are
    classified `Inconsistent`, while a defensive out-of-range storage input is
    rejected before mutation with the typed DAA or blue
    `StorageError::ScoreOutOfRange` reason. Round-trip timestamps `0`,
    `i64::MAX`, `i64::MAX + 1`, and `u64::MAX` through the signed `BIGINT`
    bit-pattern encoding; upper-half negative storage values are not
    inconsistent contents.

Verify the [block materialization transaction](storage.md#block-materialization-transaction--settled)
and [PP seal behavior](block-processing.md#pp-boundary-phase-behavior--settled)
with an ordering fixture for a non-Genesis PP: the threshold block commits
before BlockProcessor enters PostSeal, enqueues `PublishPostSeal` to its
lifecycle-marker worker, or emits `PpBoundarySealed`. The sealing block's graph
offer returns `SuppressedPreSeal` before the marker-command enqueue, which
precedes the milestone. For Genesis, verify that the rebuild transaction
establishes an intrinsically sealed boundary, `BeginRebuild` enters PostSeal
directly, enqueues `PublishPostSeal` to the worker, and then emits the same
exact-once milestone without ordinary Genesis materialization. Inject crash and
ambiguous-commit outcomes around both paths; an unproven non-Genesis seal emits
no marker or milestone, while restart after a committed Genesis rebuild derives
PostSeal from storage and proceeds through ordinary Resync.

Verify the rebuild pruning point establishes the Materialized base case and
every successful insertion preserves the invariant from Materialized
references or permitted boundary-identity leaves. A definite
`materialize_block` success and `BlockPresence::Materialized` dedup each
authorize `PersistedBlock`; a bare row observation, compact ID, absent hash, or
boundary identity does not. No storage mutation path may create a block row
without establishing the same invariant. Under `RequireMaterialized`, verify
one pre-mutation `NonMaterializedReferences` result reports complete, disjoint,
first-occurrence-ordered `missing` and `identity_only` arrays and rolls back all
writes and cache publication. Verify `IncomingBoundaryIdentity` precedence for
the block's own hash. Under `AllowBoundaryIdentities`, existing identity-only
references and newly absent references succeed as boundary leaves and do not
produce that strict-policy result.

For an inserted block, verify that the returned `BlockCommitted` contains its
committed ID and new coordinate plus every direct parent's coordinate from the
same committed transaction, with the coordinate absent for an outside-boundary
parent. Its non-repeating `level_snapshots` must contain the complete resulting
block level and every distinct materialized parent level, preserve an existing
level's DAA score while its size changes, translate the no-VSPC sentinel to
`None`, and exclude outside-boundary parents. Cover the selected-parent index
and committed initial color and VSPC membership. Inserted outcomes and their
BlockProcessor offers preserve increasing IDs while permitting allocation
gaps; `AlreadyMaterialized` returns no graph-update payload.
Verify BlockProcessor forwards the inserted payload before `PersistedBlock`;
full-channel delivery advances the session gap signal but does not suppress the
later `PersistedBlock` delivery or request processing recovery.

Verify the [atomic VSPC transaction](storage.md#atomic-vspc-transaction--settled)
for source continuity, every removed and added selected-parent relationship,
the removed/added pivot, direct-chain materiality, duplicate/intersection
rejection through typed pre-mutation `VspcMemberSetViolation`, source-specific
lifecycle mapping of that defensive error, typed pre-mutation
`VspcSourceDiscontinuity`, and structured
`VspcPathDiscontinuity(VspcPathConflict)` evidence, atomic membership and
coloring changes, merge-set members represented only by boundary identity, and
final level-score publication. Its definite outcome contains one complete
post-commit snapshot for every distinct affected level, including a level whose
remove/add sequence restores its original score, and requires no post-commit
projection read.

Verify [transaction retries](storage.md#transaction-retries) with PostgreSQL
integration fixtures. Only SQLSTATE `40001` and `40P01` retry the complete
transaction, using the nominal `10/50/250ms` slots. Assert no pre-commit cache
publication and exercise the inclusive 50% through 100% jitter range. A
nonretryable failure with proven rollback maps to `DefiniteFailure` without
retiring a still-valid generation; connection loss before commit maps to
`ServiceGenerationLost(Storage)` and retires it; connection loss with unknown
commit outcome maps to `AmbiguousCommit`, publishes no cache state, performs no
local retry, and retires it. Each retirement emits exactly one
`ProcessingDbRetired`, and the validated replacement emits
`ProcessingDbPublished`. Exercise both actual commit and rollback behind that
ambiguous result and require the replacement generation to derive the
resulting database truth. Retry exhaustion leaves the generation valid.

## Recovery lifecycle and Catchup

Verify [Resync preparation](processing-lifecycle.md#resync-preparation) with
fixtures that combine the normalized current pruning-point block, its
Materialized result, stored sink ID/hash/selected-parent hash/DAA score, and an
exact no-transactions GetBlock header. Cover:

- exact current-node-PP discovery and successful Materialized result;
- successful `MaterializedSyncAnchor` construction, including a header-only
  node block and a coherent committed Materialized sink;
- exact RPC and DB generation propagation into both processor Begin payloads,
  including VspcProcessor initialization of its committed sink and history
  seed;
- a definitively absent sink, incoherent stored sink, and a DAA mismatch
  requiring Rebuild;
- an absent or identity-only current node PP requiring Rebuild without
  retiring the valid RPC generation;
- transport or session failure without inferring Rebuild; and
- a response carrying the wrong hash or missing required GhostDAG header data
  as `MalformedGetBlock`, retiring the exact RPC generation without inferring
  Rebuild.

Verify the common
[boundary seal threshold construction](processing-lifecycle.md#boundary-seal-threshold-construction)
through both preparation modes. Cover a zero Genesis threshold, a non-Genesis
result exactly at `MAX_BLUE_SCORE`, checked-add overflow, and an otherwise
representable sum above that maximum. Both failures report
`ScoreOutOfRange(BoundarySealThreshold)` without wrapping or saturation and
produce no `PreparedSync` or processor Begin. Resync uses the reconciled DB PP
hash and score. Rebuild uses the normalized node PP hash and score and performs
this check before database replacement. The Genesis branch returns
the constant zero from an input whose owning source has already established the
shared Genesis invariant.

Verify Rebuild obtains one normalized current pruning-point block, completes
StorageService's API-read replacement gate, and passes that same
`ValidatedNodeBlock` to `rebuild_from_pruning_point` rather than rediscovering
it or mixing RPC generations. Its successful threshold and returned anchor populate the same
`PreparedSync` and exact BlockProcessor Begin payload. Malformed pruning-point
responses use the shared malformed-input budget; transport and generation loss
retain their session-fault disposition.

Also cover a coherent Genesis-anchored database below anticone finalization
depth, Empty-versus-Genesis discrimination, exact node/database
`(network_id, genesis_hash)` matching, and both synthetic streams anchored at
the committed materialized VSPC sink. Immediately after bootstrap that sink is
Genesis; after advancement it may be newer. ORIGIN must never be sent to an
RPC. A mismatch in immutable node identity is rejected rather than requesting
Rebuild.

Verify the [Catchup trigger](processing-lifecycle.md#catchup-trigger) with:

1. Initial `catchup_sink_sample()` before the GetBlocks scan and refresh only
   after holding a complete page containing the eligible marker. A failed
   initial sample starts no scan; a failed refresh leaves the held page
   undispatched, enters no Catchup, and does not replace the marker. Cover the
   malformed-sample, score-range, transport, generation-loss, and cancellation
   dispositions through their NodeService-owned typed outcomes.
2. Rolling marker replacement, replacement already present in the held page,
   cursor equality, and `Unknown -> Present -> Removed` tracking.
3. Checked and decreasing DAA-score cases and the page-aware threshold for the
   standard 1 and 10 BPS profiles and a 50 BPS override with
   `mergeset_size_limit = 512`, using the prevalidated
   `catchup_max_daa_gap`. Catchup must precede dispatch of the complete held
   page only when its VSPC-established marker satisfies the contract.
4. Independent fixtures for the global-maximum-position fallback, normalized
   GetBlocks length below three, and empty VSPC V2 page.
5. A material pre-Catchup omission using the existing strict-processing
   `Require(Resync)` path.
6. The upward seal milestone path from BlockProcessor through ResyncEngine to
   Supervisor. It never returns to BlockProcessor as a command, and no Catchup
   path is admitted before ResyncEngine observes it; global Live still requires
   PostSeal.

Verify [phase entry](processing-lifecycle.md#entering-recovery-phases),
[processor-local block overlap](block-processing.md#catchup-filtering-and-overlap),
[processor-local VSPC overlap](vspc-processing.md#catchup-filtering-crossing-and-overlap--settled),
and [global Live admission](processing-lifecycle.md#live-admission)
together. Required cases are:

1. Command priority, the Begin gate and immediate closed-gate drop, objective
   lower-bound filtering, both overlap flags, observation only at full-page
   boundaries, and Live entry while valid ordinary orphans remain.
2. Cross-response synthetic block repeats; add to the Catchup-only sent-hash
   set only after a successful enqueue and grant no overlap credit to a
   filtered repeat. This set does not participate in Live admission.
3. A temporary empty synthetic VSPC queue before component-local Live does not
   release a notification. The Live transition discards queued synthetic
   input and makes notifications authoritative for the rest of the session.
4. Notification-driven VSPC sink advancement and notification changes held in
   pre-resolution readiness by unfinished block dependencies.
5. At a complete GetBlocks-page boundary, satisfying PostSeal plus both overlap
   flags causes the lifecycle to stop and join both synthetic producers, then
   enqueue VspcProcessor Live before BlockProcessor Live and finally publish
   global Live.
6. Live admission depends only on PostSeal and both overlap flags at a complete
   page boundary, without a retained-body-tip completeness result. It makes no
   additional `GetBlockDagInfo` call for body-tip coverage, waits for no extra
   GetBlocks page, and does not inspect body-tip materiality. A dropped
   activation callback for an otherwise unobserved stale body tip neither
   blocks Live nor requests recovery.
7. An omitted block that later appears as an admitted block dependency or a
   VSPC chain member follows the existing dependency or recovery disposition.

Verify the [fault and retry policy](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
with injected clocks and deterministic jitter:

- `RpcPublished(R1)` supplies the exact RPC client for a new session but never
  rebinds an existing one; `RpcRetired(R1)` clears only the matching Supervisor
  binding and requests at most one deactivation when an operation fault reports
  the same loss concurrently; a later `RpcPublished(R2)` is retained until the
  engine is Idle;
- when a malformed-response retirement arrives before its owner-directed
  fault, deactivation begins but preserves delivery of the already-produced
  result; the later fault is accepted before `Deactivated`, consumes the shared
  malformed-input budget, preserves any stronger recovery obligation, and
  reaches Fatal on the fourth occurrence;
- `ProcessingDbPublished(G1)` supplies the exact DB client for a new session
  but never rebinds an existing one; `ProcessingDbRetired(G1)` clears only the
  matching Supervisor binding and requests at most one deactivation when an
  operation fault reports the same loss concurrently; a later
  `ProcessingDbPublished(G2)` is retained until the engine is Idle;
- every persistence-fault row in recovery and Live, including Fatal
  `DefiniteFailure`, retained Resync and Rebuild obligations, DB-generation
  retention versus retirement, complete session teardown, and the prohibition
  on reissuing an ambiguous transaction;
- a recoverable failure after `reset` sends no API invalidation control; only
  the next processing attempt's `reset` call supersedes the installed
  ApiService session;
- whole-attempt recovery Retry rather than in-place page/RPC retry;
- the general delay sequence and 30-second cap;
- no second delay while awaiting a replacement service generation;
- the general Retry backoff resets on Live, a new resource generation, and a
  stronger obligation;
- recovery requirements do not consume Retry backoff;
- one shared counter across malformed pruning-point, Catchup sink-sample,
  GetBlock, GetBlocks, and VSPC response kinds and all VSPC reasons, including
  `RemovedChainWithoutAddedPath`, `LowHashPathMismatch`,
  `ResolvedSourceDiscontinuity`, and the later attributed
  `SelectedParentPathDiscontinuity`: each malformed synthetic
  occurrence retires its producing generation, the first three permit another
  attempt after replacement, the fourth is Fatal, replacement generations and
  stronger recovery do not reset the counter, `EnteredLive` does reset it, and
  NodeService reconnect delay is not combined with the general recovery Retry
  delay;
- a later path conflict holds the candidate and leaves the sink and cursor
  unchanged while the exact VspcProcessor RPC generation probes its child;
  exercise current-parent equality with the expected parent, stored parent, and
  neither parent, for both synthetic and notification sources;
- persisted-parent disagreement requires Rebuild without retiring a generation
  when the candidate agrees with the probe; candidate disagreement applies the
  source policy; a synthetic conflict on both sides records Rebuild, retires and
  counts the generation, while the notification form records Rebuild without
  retirement or malformed-budget consumption;
- malformed, definitively absent, transport-failed, cancelled, and
  generation-lost attribution probes retain their distinct dispositions; and
- both `BoundedStateExhausted` variants atomically reject the triggering input,
  disable both notification streams, require Resync without weakening Rebuild,
  retire no RPC generation, consume no malformed-input budget, and proceed
  through complete session teardown; and
- typed fault kinds, never diagnostics, drive policy and counters.

Exercise every row of the lifecycle-owned
[ownership and session-channel disposition table](processing-lifecycle.md#supervisor-and-recovery-intent--settled),
including distinct full and closed bounded-channel faults during active
recovery, Live, and expected teardown; unavailable ApiService control;
internal command-path closure; unexpected worker termination; and invalid
lifecycle control. Assert that typed fault fields, rather than diagnostics,
select every disposition.

For every bounded processing data channel, exercise the exact focused-owner
capacity, its final successful ordered send, and the next full-channel result.
Verify current occupancy and high-water measurements without applying that
capacity to prioritized lifecycle command paths or the separately owned graph
update feed. Saturate DependencyResolver's local RPC allowance and prove the
validated client's larger total RPC allowance still admits recovery and VSPC
work; cancellation must release both allowances.

Verify [teardown](processing-lifecycle.md#teardown-and-delivery-semantics--settled)
by asserting that both processors' session-scoped RPC and DB clones are
released before `Deactivated` and cancellation races finish.

## Block processing and dependency resolution

Verify [block admission](block-processing.md#admission-and-materialization),
[committed delivery](block-processing.md#committed-block-delivery),
[OrphanManager](block-processing.md#orphanmanager--settled), and
[DependencyResolver](block-processing.md#dependencyresolver--settled) with:

1. Reverse orphan topology releases every newly ready child.
2. Resolver pending state remains until `AddOrphan` or `BlockPersisted`.
3. Resolver cancellation and deactivation are safe with full child-result
   channels.
4. A resolver-confirmed unavailable dependency requires Rebuild, whereas RPC
   or connection failure does not establish unavailability.
5. A failed post-commit `PersistedBlock` delivery cannot be treated as a
   harmless duplicate or omission.
6. A malformed resolver full-block response retires the exact RPC generation:
   during Catchup it follows the shared malformed recovery-input budget, while
   during Live it requires Resync without consuming that budget.
7. An incoming hash already classified as a permanent boundary identity, or a
   strict materialization result containing any identity-only reference,
   reports `MaterialityViolation` and requires Rebuild without orphan or
   resolver admission. When the same result also contains absent hashes, the
   identity-only disposition takes precedence.
8. A strict missing-only result before Catchup reports
   `ReconciliationFailed` and `Require(Resync)`. During Catchup and Live, the
   complete missing set enters ordinary orphan/dependency resolution without a
   partial database write.
9. A second BlockAdded for a hash whose notification source bit is already set
   is discarded before materialization and orphan admission without changing
   overlap or requesting recovery. A synthetic copy followed by the first
   notification still establishes ordinary overlap.
10. Fill orphan storage with distinct hashes, then verify that another distinct
    orphan reports `BoundedStateExhausted(Orphans)` without partial topology,
    reverse-index, or pending-resolution mutation and accepts only lifecycle
    commands until Deactivate. Re-admitting an existing orphan at capacity
    consumes no new slot and does not report exhaustion.
11. At the focused owner's exact orphan threshold and capacity, verify
    dependency selection begins at the threshold and admission remains valid
    through capacity. Hold resolver RPCs open and prove active work never
    exceeds its focused-owner concurrency bound while cancellation and queued
    work continue to make progress.

## VSPC processing

Verify [change semantics](vspc-processing.md#change-semantics--settled),
[pending history](vspc-processing.md#pending-state-and-history--settled),
[readiness](vspc-processing.md#readiness-and-materiality--settled), and
[Catchup crossing](vspc-processing.md#catchup-filtering-crossing-and-overlap--settled)
with:

- removed/added source and destination derivation;
- the added-only path with no database source;
- an actionable reorg resolving `removed` followed by `added` through one
  ordered, cache-first storage batch;
- strict-`<` sink-preserving history pruning before Catchup, at the
  Catchup-to-Live transition, and during Live, with pruning disabled throughout
  Catchup;
- bounded pending order where one unready candidate does not block a later
  actionable candidate;
- pending-destination collision checks precede capacity admission; at capacity,
  an existing collision keeps its notification fault, while a distinct
  synthetic or notification candidate reports
  `BoundedStateExhausted(VspcPending)` without partial index mutation, eviction,
  or coalescing and accepts only lifecycle commands until Deactivate;
- exercise the exact focused-owner pending capacity at its below, equal, and
  over-capacity boundaries and record its high-water measurement;
- repeated `PersistedBlock` delivery for one hash reuses the first history
  record without content comparison and leaves both history indexes unchanged;
  a new hash updates both indexes in one local transition;
- structural crossing through `added` only;
- a pending exact notification replay, a same-destination contradictory body,
  and multiple distinct actionable moves from the committed source, each with
  its typed notification fault;
- collision detection before committed-sink filtering, while a later exact
  replay after the first transition committed is a destination-equal obsolete
  discard rather than a pending-duplicate fault;
- cross-stream destination equality handled as Catchup overlap rather than a
  notification collision;
- absence of overlap credit for unresolved or filtered candidates;
- missing nonretained merge-set members at the PP boundary;
- direct nonmaterialized chain members requiring Rebuild;
- a resolved synthetic source mismatch classified as
  `ResolvedSourceDiscontinuity` rather than retained pending; and
- a storage `VspcPathDiscontinuity` held without mutation while VspcProcessor
  performs the settled source-specific attribution.

## API and Web

Verify the [graph-update feed](api-ingress.md#in-process-graph-update-feed--settled)
and [graph model](api-graph.md#graph-values-views-revisions-and-history--settled)
for causal order, one fresh ingress per processing session, no cross-session
message tagging, the mutex-protected `PreSeal -> Open` producer gate,
nonblocking ordinary delivery, a reliable coalescing gap generation even when
the final open-gate ordinary update is lost, and lossless causal
`PublishPostSeal` and `Live` markers. Verify all producer clones share the gate;
pre-seal block and VSPC offers return `SuppressedPreSeal` without entering the
channel or advancing the gap; `PublishPostSeal` is the first channel value and
opens the gate without a racing ordinary offer; and later `Full` outcomes
advance the gap. Verify BlockProcessor's dedicated marker worker absorbs Live
marker backpressure without blocking its main loop, preserves PostSeal before
Live, and permits unrelated open-gate graph updates to interleave before Live.
`BeginResync` enqueues `PublishPostSeal`, a Rebuild seal enqueues it before the
upward milestone, and handling BlockProcessor's Live command enqueues `Live`
without using `EnteredLive` as an API trigger. After the gate opens, a causal
graph offer must complete before its dependent processing delivery; on `Full`,
the gap generation must advance first. Cover complete view levels, external
parent-edge endpoints and level sizes, actual parent presence independent of
visible edges, and fresh publication identity after each replacement. Fill the
feed through its focused-owner capacity, verify the final admitted update
preserves order, then verify the next ordinary offer reports a gap while a
lossless lifecycle marker waits for capacity.
Close the graph-update receiver after definite block and VSPC commits and
verify both offers return `ReceiverClosed` without advancing the gap or
reporting a processing fault. BlockProcessor must still deliver the committed
`PersistedBlock`; VspcProcessor must retain its committed sink and history.
For an unexpected active-run publication-worker exit, verify the independent
`ApiServiceEvent::Failed` path enters Fatal exactly once. Repeat closure during
session replacement and coordinated shutdown and verify it is expected
cancellation with no processing recovery request.

Cover `BlockCommitted` conversion to `GraphBlock`, including the selected-parent
index for an ordinary block, the Genesis `None` case, and propagation of the
committed initial color and VSPC membership.

For below-range block updates, verify that every delivered `BlockCommitted` is
consumed, its complete level snapshot updates retained external state, and the
resulting endpoint change is published as one atomic revision. Parent snapshots
seed exact external level state for new crossing edges. The update must neither
expand the view extent nor restore the below-range block or level as head-view
content. An update for a level with no retained crossing-edge endpoint produces
no visible revision, and unreferenced external endpoint metadata can be
discarded.

Verify the settled [head block mutation](api-graph.md#head-block-mutation--settled)
below and at full depth, at the existing high level, at a new high level, and
with a below-range commit. Cases cover block and child-owned edge pruning,
existing and new endpoint counters, multiple edges sharing a parent level,
counter-only maintenance, and removal or survival of an external level.

Verify [Fixed updates through Head](api-graph.md#fixed-updates-through-the-head-cache--settled)
with both crossing orders at the overlap boundary, an original update that
produces no Head delta but changes Fixed, block metadata split between the two
views, an above-Fixed added member that recolors a retained merge-set block,
and a retained endpoint-level DAA change. Cover independent revisions, missing
required metadata, and the first post-update disjoint extent. The latter two
must preserve the last coherent image and make every later mutation fail under
the Frozen rule. No case may apply a Head-generated delta directly to the
internal Fixed `GraphView` or perform a storage read.

Verify the settled graph-model core with a view and history initially at
revision `n`. A retained mutation advances the view to `n+1` and returns
`GraphDelta(n,n+1)` before history append. During that interval, a view read may
return `n+1` while a history request exposes only `n`; appending the delta then
advances history to `n+1`. A no-effect update returns no delta and advances
neither revision. History rejects a nongapless append and accepts a gapless
aggregated interval without requiring internal one-step boundaries. Construct
direct and composed deltas only through `GraphDelta::new`; verify it
rejects equal and backward revision endpoints, exposes the accepted endpoints
and target timestamp through read-only accessors, and offers no revision
mutation path. Revision zero has timestamp zero. Direct revisions receive the
publication-local elapsed timestamp sampled for their creation; adjacent
microsecond-quantized revisions may have equal timestamps. Composition adopts
the final constituent's timestamp. Replace a publication and verify neither
server nor Web compares the two publication-local timestamp domains.

Exercise the publication-owned writer gate by pausing between image publication
and history append. A concurrent image capture may observe `n+1`, a history
capture may expose only `n`, and no second graph mutation or lifecycle-state
transition may pass the gate. A delta request targeting `n+1` must return only
complete history through `n` with the ordinary continuation metadata for its
captured target, without waiting or recapturing the image. After append, a
Stale transition must publish state only after both values expose `n+1`. Hold
captured immutable history-entry Arcs across pruning and complete composition
after releasing the history read lock.

Verify `GraphHistory::range` checks target ordering before retained-boundary
selection: a target below the start returns `BackwardTarget` and constructs no
delta, while equality remains `UpToDate`. Separately, send the public canonical
delta endpoint a `from_revision_id` greater than its coherently captured Head.
It must return the common `400 invalid-request` response before client
registration validation, cache lookup, or history work, so `BackwardTarget`
cannot arise from that public input.

Verify every subview extract inherits its source revision and revision
timestamp with
`TrackingPolicy::Frozen`. `BlockCommitted` and `VspcCommitted` mutation entry
points return `GraphViewUpdateError::Frozen`; delta application returns
`GraphDeltaApplyError::Frozen`. Each refuses before inspecting the input or
changing contents or revision. Keep this distinct from a no-effect update
accepted by `Head` or `Fixed`.

Verify [frozen subview extraction](api-graph.md#frozen-subview-extraction--settled)
from `Head`, `Fixed`, and `Frozen` sources, including nested extraction. Cover
invalid and outside extents; inherited revision and calculated nominal depth;
complete in-range blocks; crossing edges with zero, one, and two endpoint
blocks; all nominal and endpoint levels with exact state; recomputed derived
counters; and absence of source mutation, delta production, or history.

Level-change cases cover create, update, remove, and no-net-change composition;
application of `after` without validating `before`; a size-only external-level
mutation; and DAA score changes. Verify each delta carries the `high_level` of
the lineage that owns its revision interval and carries no lower coverage
object. Retain all Head deltas whose `high_level` remains in the retained head
window, including multiple revisions at one level, and make them eligible for
pruning only after that level leaves the window. Cover indivisible aggregated
entries, longest-prefix pruning, derived oldest available revision, and
unchanged current revision. No count or byte pressure may prune history
earlier.

Verify the [graph-view edge and absolute-map contract](api-graph.md#graph-values-views-revisions-and-history--settled)
with zero, one, and two endpoint blocks retained; Head child addition and
removal; parent-only removal under Head pruning; Fixed crossing-edge addition;
outside-extent parent levels under both policies; and PP-boundary sentinel
links. Cover all four right-biased `Some`/`None` block/edge composition pairs,
retention of the final explicit entry, map-key construction from the value
identity, and composition without value comparison or a starting view. Keep
VSPC projection treatment separate.

Verify the settled
[VSPC projection and composition contract](api-graph.md#vspc-projection-and-delta-composition--settled)
with removed and added membership, removal-to-Gray, blue/red overlap, one hash
changed in both field maps, and mutation targets absent from the receiving
view. Cover independent field-map composition, net no-op removal, field changes
overriding an absolute block addition, a final absolute removal, and an empty
composed mutation that still advances its revision interval. Level cases cover
removed-only VSPC-empty state, remove/add replacement at one level, restored
original score, block-created level followed by VSPC scoring, and a level-only
graph revision. In particular, retain an external endpoint level below the
nominal extent, omit every removed and added block at that level from the view,
and verify that its supplied final snapshot still updates its DAA score and
produces the corresponding level-only delta. A snapshot for a level absent
from the view is ignored.

Verify [anchored graph windows](api-protocol.md#anchored-graph-windows--settled)
for floor selection and tie break, the sentinel result, a reorg-created
VSPC-empty level, atomic level-score publication, and navigation plus window
consistency from one image. Accept zero and `MAX_DAA_SCORE` as query bounds and
reject the no-VSPC sentinel as a real DAA query.

Verify successful level, block-hash, and DAA anchors return a
`GraphWindowResolution` whose resolved level lies in its effective capped
range and whose resolution and graph contents come from the same immutable
graph view or database transaction. Browser cases retain the original anchor,
keep the returned level fixed across eligible canonical Head deltas, and re-resolve
only when explicit refresh resubmits that anchor. A database-backed window has
no public delta lineage and remains frozen until that refresh.

Exercise each settled `GET /api/v1/graph/window` request form. Require
`max_depth` and exactly one anchor; reject zero or multiple anchors and every
additional parameter before storage access. Accept every positive depth, cap
values above `MAX_WINDOW_DEPTH`, and report the effective extent. Cover a
complete Head extraction and database fallback for an anchor or extent outside
the retained Head image. Each response must use only one immutable source. The
Head result carries its complete publication lineage and source Head level;
the database result carries none and is discarded after serialization. Neither
path may insert a response into `GraphCache`. Both successful paths carry the
current `KGI-Publication-Id` and `KGI-Publication-State` headers outside the
body.

Decode both anchored-window source variants. The Head variant carries exactly
its publication ID, revision, revision timestamp, and source Head high level;
the Database variant carries none of those fields and invents no revision-zero cursor. Neither body
contains publication state. Both carry one `GraphWindowResolution` and one
complete `GraphDataDto`, do not echo the request anchor, and may retain
external endpoint levels outside the effective nominal bounds.

Require the nested source objects to use the exact `type` discriminator values
`head` and `database`. The Head object contains its four lineage properties;
the Database object contains no additional property. Reject a missing or
unknown discriminator in the browser decoder rather than inferring a variant
from field presence.

For database-backed anchor misses, verify exact-level absence returns
`LevelNotRetained`, an unknown or identity-only block hash returns
`BlockNotMaterialized`, and a DAA score before every retained VSPC score returns
`NoRetainedDaaMatch`. Each is an ordinary `AnchorUnavailable` result from the
same stable transaction, produces `404`, and changes no API DB generation,
publication, or processing state. Level zero, out-of-range DAA, zero or
malformed depth, and malformed hash input produce `400` without a storage
call. A DAA score beyond the current VSPC score remains a successful
current-VSPC resolution.

Verify the settled
[fixed-window reuse of canonical Head deltas](api-protocol.md#fixed-window-reuse-of-canonical-head-deltas--settled)
from a Head-extracted anchored window. Deliver the same canonical encoded batch
through a Head consumer and a fixed-window consumer. For every replay entry,
the Web filters blocks inside the fixed extent, intersecting edges, nominal and
retained endpoint levels, and membership or color changes for retained blocks.
Cover crossing edges with neither endpoint block present and mutations entirely
outside the extent. A filtered entry with no visible mutation must still
advance the browser applied cursor and timestamp and adopt the canonical
entry's Head high level without changing either fixed bound.

While successive target Heads still contain the complete fixed extent, verify
omitted canonical block and edge removals do not remove fixed objects. Then
supply a batch in which earlier entries remain eligible and a later entry's
target Head has crossed the fixed lower bound. The Web applies the earlier
entries, rejects the first ineligible entry and every later entry, preserves
its last coherent image and applied cursor, and freezes. Repeat with one
indivisible composed entry crossing the boundary and require that complete
entry to remain unapplied. Also cover publication replacement and expired
history. A terminal Stale publication remains addressable: canonical catch-up
and missing-level lookup continue through its final stalled Head.

Exercise
`GET /api/v1/graph/levels?publication_id=P&level=L1[&level=L2...]`.
Require one valid publication ID and at least one positive level, treat repeated
level parameters and values as a set, and reject malformed or additional
parameters. Deduplicate repeated values before enforcing the focused-owner
cardinality; accept exactly the maximum and reject one distinct level beyond it
before publication lookup. Capture one immutable `Synchronizing`, `Live`, or
`Stale` Head image and decode one `HeadLevelLookupResponseDto` containing
exactly one complete unordered `LevelDto` per distinct requested level plus the
capture's publication ID and revision. Require the same publication ID in
`KGI-Publication-Id` and current state only in `KGI-Publication-State`, with no
hash dictionary, ETag, or partial capture. Cover a value newer than the entry
being enriched. A missing level returns one all-or-nothing `LevelUnavailable`
outcome; an unaddressable publication returns
`FreshViewRequired(PublicationMismatch)`. Neither outcome returns partial
levels.

When retained edges in one or more queued entries need unchanged endpoint
levels, group distinct missing levels up to the request maximum, split a larger
set across complete requests, and retain no endpoint context in
`GraphDeltaBatchDto`. Stage each result with its first dependent locally
filtered entry and apply that entry atomically. Preserve the local derived
edge-usage counter while replacing the level's public size and DAA score. Let
earlier entries replay while a later lookup remains pending; reaching the
dependent entry pauses there, and lookup completion recalculates replay against
the unchanged deadline. Lookup failure applies no dependent entry, preserves
earlier applied entries, and freezes only the browser fixed image. The lookup
never reads PostgreSQL, enters `GraphCache`, joins cache single-flight work,
carries a hash dictionary or ETag, or omits `Cache-Control: no-store`.

Verify browser replay from a snapshot-initialized applied revision and
timestamp. One admitted batch must replay at its observed publication-local
pace and finish at its calculated deadline. Admit a later batch before the
first finishes and require timer replacement plus acceleration across the
complete remaining queue. Cover equal entry timestamps, zero remaining server
interval, an expired deadline, and a calculated zero delay with immediate
replay. Each graph entry applies atomically.

Replay an indivisible composed wire entry whose block upsert carries earlier
membership and color values while that same hash also has final membership and
color mutations. Under both ordinary Head replay and retained Fixed replay,
apply the graph-owned phases once and require the visible block to end with
both field-mutation target values. JSON field order and collection iteration
must not alter the result.

Permit at most one delta request in flight and ten admitted batches. A valid
tenth batch is admitted in full, a partially consumed batch still counts, and
further acquisition pauses without discarding the response or advancing from
the applied cursor. Finish the oldest batch, release its dictionary and DTO
storage, and resume from the final received cursor while honoring the latest
sticky continuation and coalesced SSE demand. Transport failure preserves
valid queued work; an invalid or nongapless batch has no partial admission.
Publication replacement, explicit refresh, view destruction, and shutdown
cancel timers and release every batch.

Adopt `KGI-Head-Revision-Id` and
`KGI-Head-Revision-Timestamp-Us` only as one coherent HTTP pair. Calculate
revision and time lag from the last applied entry rather than the first queued
or last received entry. Do not combine an SSE revision with an older HTTP
timestamp. Exercise the required observability for response entry count and
span, cache result, raw and gzip size, browser queue occupancy, applied lag,
replay acceleration or missed deadline, and Fixed level-lookup activity; exact
metric names remain deferred.

Verify deltas and client behavior across
[API publication](api-publication.md#head-publication-lifecycle-and-stream-alignment--settled) and
[Web update acquisition](web.md#update-acquisition--settled): ordered batch
replay and expiry, response-local hash dictionaries,
terminal Stale state, replacement publication identity, SSE slow clients,
fixed-view freeze, DAA focus, and Live arriving during construction,
alignment, or Prewarming. The latter must install the completed image directly
as Live.

Verify the settled
[publication-state contract](api-publication.md#publication-state-and-revision--settled):
initial Synchronizing, direct initial Live, visible `Synchronizing -> Live`,
and terminal Stale transitions leave graph view and history revisions and
timestamps unchanged. Cover the runtime-replacement `reset` barrier without waiting for
construction, `Prewarming`, or installation, cancellation of unpublished work,
no additional effect for an already Stale publication, canonical delta catch-up
through the Stale publication's final Head, request-specific state-header
changes with unchanged Head snapshot bytes and ETag, and immutable delta bytes
across later state changes. SSE cases cover dedicated `PublicationState`
delivery without a graph revision, reconnect reporting current state, Web
adoption without a delta request, and a fresh publication ID for a replacement
image.

Verify the settled [core composition](overview.md#core-crate-structure--settled)
and [Supervisor control](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
boundaries: the top `kgi` crate owns the four component `Arc` values and uses
their public async methods without a Supervisor-facing command handle or
command enum. In particular, verify the
[ApiService control boundary](api-service.md#reset-and-recovery-time-availability--settled):
Supervisor can call `reset` and await the `shutdown` barrier while
`kgi-processing` remains independent of `kgi-api-core`; `reset` returns after
the new runtime is installed and its predecessor has shut down, without waiting
for graph construction, `Prewarming`, or publication installation. Verify
status observations are latest-value and lossy, remain available through the separate memory-only
lane, and never influence lifecycle decisions. Node status begins without a
validated observation, replaces it before each Ready publication, and
preserves it with explicit last-successfully-validated meaning through every
non-Ready state, including Stopped. No status path reads or writes PostgreSQL
or gives ApiService a NodeService dependency. The public `SystemStatus` reports
the running KGI package version and exactly the Supervisor, NodeService,
StorageService, and processing observations. It exposes network, node server,
and upstream RPC versions only through `node.last_validated`; it contains no
separate processing version, API version, `representation_version`, or
ApiService-status field.

Verify the
[process signal adapter](overview.md#process-termination-signal-adapter--settled)
and [Supervisor trigger](processing-lifecycle.md#termination-triggered-global-shutdown--settled)
without bypassing Supervisor. Verify that the registration holds only a weak
target reference. A first supported platform signal invokes the Supervisor
shutdown target, does no teardown work in the signal callback, and causes
exactly one entry into Supervisor's terminal-shutdown transition. No managed
component receives or polls the registration. A handler-installation failure
panics with the settled expectation message before managed components enter
their running state. Verify that a second signal invokes the idempotent target
again without creating another shutdown transition. In a subprocess test, a
third signal during graceful shutdown prints the settled halting message and
immediately exits with status `1`; one or two signals followed by completed
Supervisor barriers exit normally. Hold a shutdown barrier open after the
first or second signal and verify that KGI neither times out nor escalates
without the third signal or external process termination.

Exercise `GET /api/v1/status` without parameters or a request body. Require a
complete JSON `SystemStatusDto`, `200 OK`, `Content-Type: application/json`,
`Cache-Control: no-store`, and no ETag or conditional-request behavior. Require
every field to be present and every absent optional value to encode as explicit
JSON `null`. Round-trip every exact lowercase kebab-case component state,
recovery mode, processing-state name, and network type. Require processing
mode only for `reconciling`, preserve a custom network suffix, and cover both
optional upstream RPC version fields. Verify the endpoint remains successful
without a graph publication, validated node, processing session, API database
generation, or enabled historical-read gate, including
`node.last_validated = None` and component unavailable, recovery, rejection,
fatal, and stopped observations.
No request may access PostgreSQL, call node RPC, inspect a graph publication or
`GraphCache`, perform graph serialization, issue recovery control, or wait for
a cross-component snapshot. Graph-lane saturation must not hide status. Reject
all query parameters, and expose no separate `/api/v1/info` route.

Verify the API's common HTTP conventions independently of endpoint DTOs. Every
public route is rooted at `/api/v1`, uses its canonical lowercase noun path
without a trailing slash, and accepts only the settled `GET` operation without
a request body. Query names are case-sensitive snake_case and have no
abbreviated aliases. Cover unsigned decimal integers, canonical hexadecimal
block hashes, exact lowercase booleans, and lowercase kebab-case enums. Reject
malformed or negative numeric values, duplicate scalar parameters, unknown
parameters, mutually exclusive selections, and missing required parameters
with `400 Bad Request`. No rejected request reaches graph construction,
history selection, or storage.

Verify the common transport DTO rules at the final serialization boundary.
Keep canonical `GraphView` and `GraphDelta` values intact until that boundary,
exercise explicit response-variant discrimination and absence independently
from zero or empty values, and round-trip the full public `u64` domain through
its unsigned decimal JSON-string representation and the Web's `bigint`
conversion boundary. Require `Content-Type: application/json` for every
successful graph body. Check ordered graph collections retain their semantic
order, unordered collections remain semantically equivalent across different
traversal orders, and no internal-only field enters a public payload. A
representation-significant schema or encoding change must alter the internal
representation version used by graph cache identity and Head ETags.

Require every property declared by a selected response shape to be present.
Encode absent level DAA score, Genesis selected-parent index, and removed-level
mutation value as explicit JSON `null`; distinguish each from zero and from an
ordinary value. Round-trip BlockColor codes `0`, `1`, and `2` through both a
block upsert and a color mutation, and require the Web decoder to reject every
other integer, string, `null`, or omitted color before changing graph state.

Verify the protocol-owned
[graph gzip contract](api-protocol.md#graph-http-compression--settled) across
every graph endpoint. A syntactically valid graph request that does not accept
gzip returns the typed `406` `gzip-required` response before graph, cache,
history, database, serialization, or compression work. Invalid routes,
methods, and request syntax retain their earlier `404`, `405`, or `400`
precedence. Every graph-endpoint response carries
`Vary: Accept-Encoding`; every successful graph body additionally carries
`Content-Encoding: gzip`, while `304` and bodyless delta success carry no
`Content-Encoding`. Error bodies remain uncompressed common JSON. Status, SSE,
runtime configuration, and static assets do not inherit this graph-only rule.
Force compression failure after successful serialization and size checking;
require the existing request-local `500` outcome and no cache insertion.

Build the [settled HTTP composition](overview.md#http-composition-and-runtime-web-configuration--settled)
from the `kgi-api-core` router awaiting `Arc<ApiService>`, supply that state
once at the composition root, and exercise API, runtime-configuration, asset,
and browser-fallback routes through the completed application. An unknown
`/api/v1` route returns the protocol `404`, and an unsupported method on a
known API route returns its `405`; neither may serve `index.html`. A
non-API browser route uses the SPA fallback, and an asset uses static-file
delivery. The outer trace layer must preserve every response and streaming
behavior. Verify graph bodies are compressed exactly once, SSE remains open
beyond the ordinary graph-delivery timeout, and API lane saturation remains
ApiService-owned rather than becoming router-wide backpressure.

Verify graph-value serialization uses zero-based response-local `u32` hash
references and a first-encounter dictionary built from the serializer's actual
traversal. Repeat hashes across block identity, ordered parent and merge-set
vectors, edge endpoints, and VSPC field targets and require one dictionary
entry per response with consistent local references. Randomize internal
hash-map iteration and require decoded graph equivalence without requiring
identical encoded order, dictionary order, or reference numbers. Round-trip
the complete `u64` domains retained by levels, coordinates, level sizes,
timestamps, and scores; preserve ordered parent and merge-set vectors and the
Genesis selected-parent-index absence. A cached delta must not intern a hash
used only by a serializer-omitted block or edge removal. Serialization adds no
immutable-value comparison or conflict-validation path.

Verify the common HTTP outcome map and exact `ApiErrorDto` together.
Complete and prefix graph results, delta `UpToDate` and `WaitForWakeup`,
anchored windows, Head-level lookup, and status use `200`; only an exact Head
ETag match uses `304`. Unknown routes use `404`; known resources reject
unsupported methods with `405` and `Allow: GET`. Anchor and Head-level lookup
misses use typed `404` outcomes. A valid graph request that does not accept
gzip uses typed `406` category `not-acceptable`, code `gzip-required`, and
explicit `null` details. Registration and every fresh-view reason use
typed `409` outcomes. Oversized complete non-delta graph responses use the
typed `422` `graph-response-too-large` result, identify the hard limit, and
contain no partial graph. Verify the exact structured maximum and observed
uncompressed byte details.
Admission-lane saturation before commitment uses `429`,
while unavailable graph or database capabilities use `503`; both carry
`Retry-After: 1`. Unexpected request-local failure before commitment uses
`500`. After commitment, delivery failure closes the response or stream rather
than emitting another outcome.

Require `Cache-Control: no-cache` on successful Head snapshots and canonical
deltas. Require `Cache-Control: no-store` on anchored windows, Head-level
lookup, SSE, status, and every `4xx` or `5xx`. Cover the common paired
`KGI-Publication-Id` and `KGI-Publication-State` headers on every Head snapshot,
canonical delta, anchored-window, and Head-level response produced after a
coherent publication is selected, including bodyless success and `304`.
Require the header ID to identify the exact retained publication and no
publication-state field in any of those graph bodies. Sample that publication's
latest state while finalizing headers; a pre-admission
rejection or graph-unavailable response with no publication omits both headers.
Cover a body capture followed by a state transition and require the newer
header with the unchanged immutable body. No cached gzip bytes or ETag identity
may include that state. No successful graph response may be truncated; a delta
prefix is complete through its reported target. Cover
every exact category/code mapping and require one direct JSON error object with
all fields present. Exercise each typed window-anchor detail, Head-level detail,
and fresh-view reason; encode absent details as explicit `null`, public `u64`
values as unsigned decimal strings, and block hashes as canonical hexadecimal
text. Require `Content-Type: application/json`, `Cache-Control: no-store`, the
settled `Allow` and `Retry-After` headers, and client control independent of the
human message. No response exposes internal variants, generations, diagnostics,
stack traces, SQL or filesystem information, or serialization-library text.

Verify `GET /api/v1/graph/head` requires `target_depth`, rejects zero and every
value above `MAX_WINDOW_DEPTH`, and accepts the inclusive valid range. Exercise
rounding immediately below, at, and above each 50-level tier boundary. Every
successful response comes only from one coherent in-memory cached Head extent,
reports its actual bounds, carries a complete local hash dictionary plus the
snapshot publication ID, revision, and revision timestamp, and never accesses
PostgreSQL. It echoes neither `target_depth` nor the selected tier. Decode the exact
`HeadSnapshotResponseDto`, including one complete `GraphDataDto`, and allow
retained external endpoint levels outside its nominal low/high bounds. Cover
`Synchronizing`, `Live`, terminal `Stale`, and no coherent publication. Advance
or replace the publication during delivery and verify the cached response
remains internally coherent. The endpoint neither accepts nor returns
`client_id`.

For every successful Head response, require the body tier to cover
`target_depth` except for the natural pruning-point boundary. Have the Web trim
a larger tier to the requested nominal extent, remove edges with removed
children, retain external parent endpoint levels, and derive its local usage
counters. No cache-selected response may require a placeholder or
progressive-fill presentation.

Exercise `If-None-Match` across an unchanged response, snapshot revision
change, actual-extent change, state-only transition, and publication
replacement.
Require a weak Head ETag. Independently serialize the same semantic snapshot
with different unordered collection and dictionary orders and require the same
weak identity and `304 Not Modified`. Every semantic identity change returns
the complete snapshot. Verify `target_depth`, selected tier, and publication
state do not enter ETag identity. A state-only transition keeps the same cached
bytes and ETag, and both `200` and `304` carry the newly sampled
`KGI-Publication-Id` and newly sampled `KGI-Publication-State`. Verify
`Cache-Control: no-cache`.

Delta cases cover Frozen and revision compatibility as the only application
checks, direct target-state installation, Head bound recalculation, fixed-bound
retention, and equality between sequential application and a directly or
incrementally composed interval. Cover associative graph-state effects across
three adjacent intervals; later-value, insertion-folding, final `high_level`,
and final target-timestamp selection; and rejection of nongapless composition.
Protocol selection must reject a publication mismatch before history work.

For one body-bearing response, select an ordered gapless
`GraphHistoryEntryList` containing ordinary one-step entries, an already
composed entry, and both forms together. Require one `GraphDeltaEntryDto` per
selected history entry and never reconstruct the composed entry's discarded
internal boundaries. The first entry starts at the requested revision, every
adjacent boundary matches, and the final entry supplies the returned cursor.
A requested target inside an aggregate extends through its right boundary; a
future target returns through available history; a starting cursor inside an
aggregate is unavailable.

At the serialization boundary, verify that every in-memory canonical delta
retains its complete change maps and composition `before` values while its
`GraphDeltaEntryDto` emits only target values. Cover level installation and
removal, membership and color target values, block and edge upserts, and an
all-empty mutation body that still advances the revision interval. Omit block
and edge removals separately from every wire entry and omit hashes referenced
only by those removals. Traverse all retained entries under one deterministic
first-encounter hash dictionary; references in different entries to the same
hash must share one dictionary item. `GraphDeltaBatchDto` contains no endpoint-
level context.

For every appended history entry, verify `estimated_raw_bytes` and the absolute
`cumulative_estimated_bytes` boundary at `delta.to_revision_id`. Cover range
cost subtraction before and after prefix pruning, the first retained left
boundary derivation, and an aggregated entry exposing only its final cost
boundary. Exercise the delta-to-Head regions at the exact half-distance and 80%
level boundaries. In the heat region, cover no heated boundary, competing
destination counts, the outgoing-cache/job and youngest-revision tie breaks,
and exclusion when a boundary leaves retained history or the moving heat
region. Confirm request-from counts and continuation kinds do not change heat.
In the CPU-oriented region, cover an exact cache hit, the shortest bridge to
the nearest reachable completed cached source, exclusion of a running job as a
bridge destination, and bounded fallback when no bridge is reachable.

Exercise the preferred 15-revision destination, heat-selected extensions
through revision 30, a nearer captured or terminal Head, and an affordable
short prefix. No newly selected ordinary batch crosses 30 revisions; an
indivisible stored aggregate may do so. Force the complete batch's final
uncompressed JSON over its hard limit and verify removal of complete entries
from the right, rebuilding the shared dictionary for each candidate prefix.
The first indivisible entry exceeding the limit requires a fresh view. A gzip
body that transfers efficiently does not permit an over-limit uncompressed
representation.

For tiered Head snapshot reuse, derive exactly `50`, `100`, `150`, `200`, and
`250` from the initial limits. Require tier `50` to finish before publication
installation and require every later build or refresh to be request-driven.
Exercise selection in this order: eligible selected entry; smallest eligible
completed larger entry plus selected-tier construction; selected-tier job;
smallest covering larger job without concurrent selected-tier construction;
and a new selected-tier job. Concurrent requests joining one tier must share
one capture, serialization, and compression result.

At distances 39, 40, 99, and 100, verify respectively direct reuse; reuse plus
one demand-triggered refresh; reuse plus that same single refresh; and complete
entry exclusion with shared reconstruction. Head advancement without a request
must start no refresh, including for tier `50`. A successful refresh atomically
replaces only its tier. Failure gives all on-demand waiters the same typed
error, preserves the previous entry, removes the job, and permits retry. A
Stale publication can reuse and construct tiers against its final Head.

Pause a demand-built Head-tier job during detached encoding and advance Head
across its completion thresholds. A candidate completing at distance 40 or 99
is installed and delivered; when served as the request's selected tier it
ensures one successor refresh, while use as a larger covering tier does not.
A candidate completing at distance 100 is neither inserted nor published as a
terminal result: the same job remains pending, every concurrent waiter stays
joined, and a newer capture eventually supplies their one shared eligible
result. Advancing Head after an eligible candidate's admission must not revoke
that installed entry or its already waiting responses. Repeat the hard-age
case for mandatory prewarming.

For canonical Head deltas, verify one
publication-owned job per source revision: concurrent requests join it after
its target is captured, success publishes one `CachedDelta`, and failure gives
all waiters the same request-local error after removing the job. Count every
successfully delivered waiter independently toward the destination heat. A
cache hit reuses the exact gzip bytes and is never
promoted or replaced because Head advanced; publication ID and state, outcome,
continuation, wake boundary, and retry delay remain outside those bytes. Wrap
one cached batch body with different valid request-specific publication and
coherent Head revision/timestamp headers without
serializing, compressing, or decompressing it. Verify that `CachedDelta`
retains the first and last boundaries, final `high_level`, checked
uncompressed JSON byte count, and final gzip body without duplicating entry
timestamps or count as semantic fields. Verify
one immutable outgoing entry per source revision and
convergence of distinct sources on a heated destination. Exercise an
SSE-coordinated and an HTTP-only request from the same source and verify both
reuse the same cache entry or join the same job.

For a Live publication and preferred interval 15, cover no-delta waiting at
Head distances one through fourteen; target eligibility beginning at
`F + 15`; heat extension through `F + 30`; no-heat fallback; `ReachedHead`,
`ContinueImmediately`, and `WaitForWakeup`; one wakeup
at the armed boundary; suppression of further graph wakeups until rearming;
and `PublicationState` or the replacement sequence bypassing the schedule.
Verify the canonical
`GET /api/v1/graph/deltas` endpoint requires `publication_id` and
`from_revision_id`, accepts an optional `client_id`, rejects a public
`to_revision_id`, and emits the exact publication, publication-state, coherent
Head revision/timestamp, outcome, and continuation headers required by the
protocol. Require a batch body for
`complete` and `prefix`; require an empty `200 OK` body for `up-to-date` and
`wait-for-wakeup`. The wake-boundary header appears only for
`wait-for-wakeup`. Neither typed `409 Conflict` delta error carries the delta
success headers. Verify body-bearing results advance the cursor to the final entry's right
boundary while the two empty success outcomes preserve the request
cursor. The opaque client ID selects only wake state, while every HTTP
`from_revision_id` remains the authoritative graph cursor. Its omission must
select HTTP-only polling without `ClientRegistrationRequired`; cover that
outcome for an expired and wrong-publication supplied identifier before cache
or history work. Remove a validated registration while its request waits for a
shared cache job, then require the completion-time atomic rearm to return
`ClientRegistrationRequired` instead of committing the selected success. The
shared cache result remains reusable and no other registration changes.
Exercise
`GET /api/v1/graph/wakeups?publication_id=P&from_revision_id=F`: require both
cursor parameters, reject unsupported parameters, and return graph unavailable
without opening a stream when no coherent publication exists. A successful
stream has the settled event-stream content type, `Cache-Control: no-store`,
and no ETag, graph body, hash dictionary, database access, graph-cache work, or
delta construction. A stale but well-formed request cursor must still receive
the current publication.

On initial connection and reconnection, require an ID-only
`ClientRegistrationDto`, then `PublicationStateDto`, then
`PublicationWakeupDto` in order. Encode each as one complete compact-JSON SSE
event under its exact event name, without a redundant type property or SSE
`id:` field. Round-trip zero and maximum `u64` publication and revision values
as unsigned decimal JSON strings, reject other state spellings, and ignore JSON
property order. Generate the client token from random `u64` big-endian bytes;
require its eleven-character canonical unpadded-base64url representation and
regenerate a forced active-registry collision. Reject malformed token syntax,
while a canonical unknown or wrong-publication token produces
`ClientRegistrationRequired`.

Exercise the concrete client registry and mailbox with the final admissible
client and the saturated next registration. Require one synchronous registry
critical section to collision-check the random key, admit the complete initial
three-message batch, and install the registration. The batch must appear
entirely or not at all. Repeat for publication replacement after removing the
old registration, using the same mailbox, a fresh identifier, and the
replacement Head as the inert sent boundary. Drop the mailbox owner without
cleanup and verify its weak registry entry is removed on the next operation.
No registry or mailbox mutex guard may cross an `.await`.

Queue an ordinary old-publication wakeup before replacement and verify old
registration removal discards it before appending the replacement batch.
Required old messages already queued must remain ahead of that complete new
batch.

Advance Head repeatedly while an ordinary graph wakeup remains queued. Verify
that the mailbox retains one coalescible item with the latest cursor and moves
it after any required state item enqueued in the meantime. Required admission
must never evict that graph item. Fill the remaining queue with required items
and verify that the next required item or atomic batch disconnects the client;
also verify that a first ordinary wakeup against a queue already full of
required items disconnects it. The mailbox's `Notify` wakes its single receiver
without becoming a second source of message contents or ordering.

Race an HTTP rearm against Head advancement in both orders. Rearming must
remove an ordinary wakeup still queued under the previous schedule and must
immediately enqueue a newly armed boundary already reached by the registry's
latest history-published Head. If the SSE task already took the prior wakeup,
permit that non-exactly-once prompt without changing the authoritative HTTP
cursor. Assert that the registry scan never creates a timer or per-client
task.

Reconnect SSE within one publication while a coordinated delta request using
the previous identifier is in flight. The Web must cancel that request,
preserve its admitted queue and cursors, ignore a late old-identifier response
in full, and retry from the unchanged received cursor with the fresh
identifier. Cover both `ClientRegistrationRequired` arriving before the fresh
registration and arriving after it: only the former reconnects SSE again. The
successful retry must atomically arm the fresh registration.

Replace a publication without reconnecting its SSE transport and verify the
old ID is invalidated before the same three-message sequence reports the
replacement. A state transition emits only `PublicationStateDto`, does not
replace the ID, and bypasses the graph-wakeup schedule. Entering Stale carries
the final stalled Head revision. Verify `Last-Event-ID` has no graph-cursor,
publication, registration, or replay meaning. Cover saturation rather than
wrap at the maximum revision. Synchronizing never parks for the interval.
Entering Stale reports state independently of graph-wakeup arming and permits
an interval shorter than 15 through the final stalled Head without rearming.
For HTTP-only `head` and `wakeup`, verify the protocol-owned calculated
`KGI-Delta-Retry-After-Ms` values at representative lags zero through fourteen
and its absence
from `continue` and SSE-coordinated responses. At a Stale final Head require
1000 milliseconds, repeated same-endpoint polling while that publication
remains current, and a publication-mismatch outcome followed by a replacement
Head snapshot after replacement. The SSE-coordinated Web must wait for the
replacement wakeup instead.
Force both an estimated and a final uncompressed JSON budget to select a
shorter prefix; that prefix remains valid and cacheable and derives
continuation from its remaining Head distance. Also cover a CPU-oriented
bridge shorter than 15.

Verify Stale preserves cache entries and permits existing and new jobs to
finish against the stalled Head. Publication replacement must allow a running
job and every attached request to finish against their captured immutable
history entries. Cancelling one request detaches only that waiter. Once the old
publication has no remaining runtime, slot, request, or SSE reference, its
cache and client registry are released; a detached job may complete its
remaining waiters, but its weak cache insertion must then fail harmlessly.
Terminal ApiService shutdown cancels and joins unfinished jobs. Evict an entry
when its source revision leaves history and when its target level falls more
than `MAX_CACHE_LEVEL_DISTANCE` behind Head; either miss reconstructs an
equivalent response. At
`DELTA_RELOAD_LEVEL_DISTANCE`, require a fresh window before starting a job.
Historical DB
windows bypass this cache.
Head-level lookups also bypass cache lookup, insertion, and cache single-flight,
return `Cache-Control: no-store`, and produce no ETag. Fixed-window consumers
must receive the same cached canonical Head response as other consumers. Cache
work must not delay or fault processing.
Verify every Head tier stores its post-compression gzip body, weak ETag, actual
extent, snapshot cursor and timestamp, and uncompressed JSON byte count while
excluding `target_depth`, selected-tier metadata, and publication state from
the body.
Historical database-backed windows and
Head-level lookups serialize, check their uncompressed JSON size, compress,
deliver, and discard without entering `GraphCache`.

Under the strict
`MAX_CACHE_LEVEL_DISTANCE + MAX_WINDOW_DEPTH < MAX_CACHE_DEPTH` bound, verify
the composed in-memory Head delta still contains every canonical block and edge
removal immediately before serialization, while its cached wire body omits
those removal entries and their now-unused dictionary hashes. Advance a
maximum-depth eligible browser window and prove its local `high_level`/depth
pruning produces the same retained blocks and child-owned edges as the complete
canonical history. Retain every VSPC membership, color, and required level
mutation.

Verify the settled
[database-seed projection](api-publication.md#database-seed-extent-and-projection--settled),
[head-publication lifecycle](api-publication.md#head-publication-lifecycle-and-stream-alignment--settled), and
[storage read operation](storage.md#api-graph-projection-reads--settled).
Cover a `MAX_CACHE_DEPTH` head seed; odd and even anchored depths; shifting at
level 1 and the database head; level, block-hash, and DAA anchors; and a retained
range shorter than the requested depth. Resolution and every projected row must
come from one read-only stable snapshot.

Projection cases cover complete nominal blocks; crossing edges with zero, one,
and two endpoint blocks; all nominal and endpoint levels; exclusion of the
outside-PP sentinel edge; complete direct-parent and merge-set hashes; local
`selected_parent_index`; Genesis without ORIGIN; current color and VSPC state;
and both construction-only alignment values. Verify that the ID cut is the
maximum over the complete materialized block table, including an ID whose
block is outside the projected window. Recompute derived level usage from
edges. Exercise the separate capped API pool and prove its saturation or
generation loss cannot consume or retire a processing-pool connection.

Lifecycle cases cover ApiService `AwaitReset`, construction of a fresh private
runtime in `PreSeal`, waiting for the first-channel PostSeal marker while
producer-side suppression remains active,
`Constructing -> Aligning -> Prewarming -> Active`, and the prohibition on
direct construction-to-installation. Alignment covers block IDs at and below
the snapshot cut, a first greater ID with no retained effect, a first greater ID
that returns a delta, and the matching VSPC source. Exercise both crossing
orders. After one cut crosses, later updates from that source apply while the
other source continues its snapshot-relative filtering. Empty staging and
either single-cut state remain Aligning and publish no candidate, including an
indefinitely idle second source. Only after both cuts cross, apply all later
interleaved updates through a captured prewarming frontier, assign the
publication identity, and continue applying newer arrivals normally while the
tier-50 capture is serialized and compressed. Cache work must create no graph
revision and must not accumulate a hidden update backlog. Install only after
the tier is complete and less than 100 levels behind the then-current Head.
Cover a tier that becomes hard-ineligible during construction and is rebuilt
from a newer capture, mandatory tier failure and retry without installation,
and installation with the current publication revision ahead of the cached
snapshot cursor. The first client catches up from that cursor through retained
ordinary deltas. Cover first publication revisions zero and above zero, plus
independently atomic view and history visibility at adjacent revisions.

Verify the current-publication watch starts at `None`, atomically installs a
complete publication Arc, and accepts installation only from the current
runtime generation. During reset-time overlap, let the successor install
publication N before the predecessor attempts to install publication O; the
predecessor receives `Superseded`, N remains current, and predecessor shutdown
cannot mark N Stale or replace it. A request captures the slot once and
completes against that exact Arc after a concurrent replacement; a later
request observes the replacement. Runtime shutdown never clears the slot,
while terminal ApiService shutdown replaces it with `None` after the runtime
barrier and before releasing publication resources.

Pre-seal suppression produces no gap. Verify the database seed started after
the marker covers every intentionally suppressed commit, including block and
VSPC commits racing with the marker transition; an offer ordered after the
transition enters the channel instead. No pre-seal seed or second marker is
required.

Exercise the runtime's universal intra-session reconstruction primitive from
`Constructing`, `Aligning`, `Prewarming`, and `Active`. The first two cancel any
seed attempt and abandon their candidate; `Prewarming` abandons its unpublished
publication and detaches its mandatory tier waiter while the tracked job may
finish without inserting; `Active` first becomes terminally Stale. In every
case,
drain through the first observed channel Empty, discard graph mutations,
preserve sticky Live, capture the newest gap generation, and start a newer
seed. Updates after the empty frontier must be staged, while a later
gap-generation change restarts reconstruction again.
Cover staging overflow, query/generation/projection failure, alignment or
application failure, and a gap during each state. None may request processing
recovery. `PublishPostSeal` is not repeated during same-session reconstruction.
Changing the API database generation alone must not replace an otherwise
advancing Active publication.

At the focused-owner staging capacity, verify that only block and VSPC values
consume entries, Live remains sticky without consuming one, and the next
ordinary update restarts construction without retaining a partial staged
suffix. Record occupancy, high-water, overflow, and reconstruction metrics.

Verify `ApiDbState` independently from graph publication. Install the reliable
StorageService-to-Supervisor event path before initial generation publication.
ApiService starts with no current client and public reads enabled, so a
database-backed request returns `503`. StorageService publishes `G1`, emits
`ApiDbPublished(G1)`, and Supervisor maps it to `Published(G1)` for
`update_api_db_generation`. ApiService adopts it without changing publication
ID, state, view or history revisions, or SSE cursor. An ordinary Resync
preserves both snapshot fields. Verify `public_read_client()` is immediate and
returns a client only while the gate is open and the current exact client is
valid. Verify `current().await` ignores that gate, waits through an absent or
invalid current client, wakes on a valid published replacement, and returns
`None` only after ApiDbState becomes `Stopped`.

From an Active publication using `G1`, make a public database operation report
`GenerationLost`. StorageService must atomically retire `G1`, emit exactly one
ordered `ApiDbRetired(G1)`, start autonomous reacquisition, and later emit
`ApiDbPublished(G2)` without an ApiService request. Supervisor maps those to
the corresponding API control events. Before returning the error,
StorageService makes `G1.is_valid()` false permanently. The failed operation
returns `503` without directly clearing ApiDbState; the invalid client is
immediately inadmissible, and forwarding the exact retirement removes it from
the latest snapshot idempotently. A late `Retired(G1)` after `G2` cannot clear
`G2`, a retired Arc is never republished, and an in-flight operation never
switches or retries generations. Exercise the same loss from a construction
seed attempt: the runtime invokes reconstruction, waits in `current()` while no
valid client is available, and resumes with the forwarded replacement. Prove
ApiService emits no reverse generation-loss event and Supervisor creates no
acquisition task.

For both a public database-backed anchored window and a Head seed attempt,
exercise `QueryFailed` and `InconsistentProjection`. A public request returns
`500` with no partial graph, transparent retry, client clearing, `ApiDbState`
change, publication mutation, SSE wakeup, or processing recovery. A seed
attempt abandons that candidate and invokes ordinary reconstruction while
retaining the exact current API DB client. `InconsistentProjection` never
substitutes for an anchor-unavailability result.

For Rebuild, verify `reset(Rebuild)` disables public reads without clearing the
current client. StorageService retirement prevents the old client from reading
replaced contents, emits `ApiDbRetired`, and autonomously publishes the coherent
replacement. Supervisor forwards both events in order. `Published(G2)` makes
`G2` available to construction while public reads remain disabled; replacement
publication installation after `Prewarming` enables reads. If the current
generation is lost after the seed has detached but before installation,
alignment and `Prewarming` may finish, installation still enables the gate, and
database-backed requests remain `503` until a later `Published(G3)`. Repeated
retirement/publication delivery is idempotent, and
neither generation turnover nor an absent current client invalidates an
otherwise advancing Active graph publication.

Verify [Reset and recovery-time availability](api-service.md#reset-and-recovery-time-availability--settled)
with ordinary Resync and Rebuild integration scenarios. Verify the fresh
`GraphUpdateReceiver` and exact recovery mode. From `PreSeal`, `Constructing`,
`Aligning`, `Prewarming`, and `Active`, start the fresh runtime before
exchanging it into the private lifecycle slot, then await the old runtime's
uniform shutdown before `reset` returns. Verify the new receiver can drain during the bounded runtime
overlap and that reset completion waits for neither construction nor
publication installation. Serialize concurrent reset and shutdown calls through
the runtime mutex; reset after `Stopped` is rejected and repeated shutdown is
successful. Advance the private runtime generation before the successor starts
and reject every predecessor installation after that boundary. Exercise a
successor installation followed by a predecessor installation attempt and
predecessor shutdown; the successor publication remains current. During a
later Rebuild reset, delay an older Rebuild runtime's activation until after
the new generation disables public reads, then verify its `Superseded` outcome
cannot install its publication or reopen the gate.
Resync preserves public database-backed reads, and failed reconciliation followed by
Rebuild calls `reset` again with a fresh ingress. Rebuild relies on
StorageService rather than `reset` to close API database admission, retire the
old API generation, and boundedly drain or cancel active database phases.
Verify a detached old projection may complete delivery while
an undetached request returns 503, public reads reopen only after the
replacement completes `Prewarming` and becomes `Active`, and
`PpBoundarySealed` has no API generation role.
Cover `PublishPostSeal` arriving before StorageService's replacement
`Published` event and construction remaining pending. No in-flight request
rebinds to a newly published storage generation,
and no request observes a partial or mixed generation, including with
PostgreSQL `TRUNCATE`. Verify ordinary sender teardown may close the installed
receiver. The runtime then becomes quiescent, does not reconstruct or change
publication state, and remains owned until its later `shutdown()`. A marker
worker with an accepted delivery may outlive BlockProcessor Deactivate without
retaining an RPC or DB client, then exits after delivery or receiver
replacement.

Exercise the
[publication-runtime worker](api-publication.md#publication-runtime--settled)
as one long-lived worker started by the runtime constructor. During
construction, make database seeding, graph-update receipt, and cancellation
win its selection in turn; reconstruction drops an in-progress seed attempt
without leaving a child task. Runtime shutdown cancels and joins the worker,
marks an active publication Stale through the ordinary state path, and returns
the same completed result on repetition. Closing the receiver makes the worker
quiescent until shutdown without spinning, reconstructing, or changing
publication state. An unexpected worker exit emits exactly one reliable
`ApiServiceEvent::Failed` and enters Supervisor Fatal; expected shutdown emits
no failure event.

Exercise source-keyed cache single-flight with multiple waiters, one cancelled
waiter, shared success and shared request-local failure, running-job removal,
and weak insertion both before and after the origin cache is dropped. Verify
that one short cache critical section elects exactly one builder, installs its
pending job after required pre-cache checks and before cache-build target
selection or build-input retention, and lets every concurrent miss join that
job. No cache mutex guard may survive
into capture, extraction, an `.await`, encoding, delivery, or waiter
suspension. On success, require completed insertion before the shared `watch`
result; on failure, require matching-job removal before the shared error so an
immediate retry starts new work. A cancelled receiver must not alter the job,
and a dropped origin cache must prevent insertion without preventing receiver
completion. Exercise completion after every waiter has detached and require
`send_replace` to retain the terminal job value without restoring a cache slot.

Verify all cache orchestration tasks are registered in the ApiService task
tracker. Include tier-keyed Head snapshot jobs and mandatory `Prewarming` work
in the same tracked cache and encoding infrastructure. For encoding, reserve
bounded historical capacity before database work,
release every database resource before submitting the detached projection,
and enforce the settled total and historical concurrency limits. Scheduler
shutdown rejects new work, cancels queued work, lets already active blocking
serialization or compression finish while discarding its output, and joins
the scheduler without a publication lock or database resource held.

Verify the [ApiService shutdown barrier](api-service.md#apiservice-shutdown--settled)
from `AwaitReset`, `PreSeal`, `Constructing`, `Aligning`, `Prewarming`, and
`Active`, including races with `reset`, marker delivery, gap reconstruction,
database projection,
serialization, detached response delivery, SSE wakeups, and API-generation
publication on either side of Supervisor's terminal-shutdown cutoff. An update
completed before the cutoff is cleared by shutdown; an event observed after
the cutoff is discarded without an `update_api_db_generation` call, and a
direct update call after ApiDbState becomes `Stopped` is rejected. Admission
closes first. Runtime shutdown performs its ordinary terminal `Stale`
transition and allows that mandatory state message to enter every currently
registered client's normal ordered bounded SSE path before connections are
cancelled. Shutdown does not await network delivery; a full client buffer
invokes the ordinary slow-client disconnection rule. Verify API request guards
remain held through complete body delivery, failure, disconnect, timeout, or
cancellation; cancellation before response commitment returns the applicable
unavailable outcome when possible, while cancellation after commitment
terminates delivery without a second response. The graph receiver drop
unblocks a waiting marker worker. Closing and cancelling the cache task tracker
and shutting down the encoding scheduler leave no API task or database
resource at method completion; no graph revision is emitted, and repeated
`shutdown` is idempotent. Verify ApiDbState becomes `Stopped`, wakes a pending
`current()`, and rejects later generation updates.
Supervisor signals Axum graceful shutdown, then starts ApiService and
ResyncEngine shutdown without waiting for one to complete before starting the
other. The listener accepts no new connection, existing connections remain
available for final SSE buffering, and ordinary static responses may finish.
Supervisor awaits both method barriers and the Axum server task, then calls
NodeService shutdown followed by StorageService shutdown. Unexpected Axum
return or panic while Running and unexpected permanent ApiService-worker exit
each enter Fatal through their owner-defined observation path; expected
shutdown completion does not. Producer closure during that barrier requests no
reconstruction or processing recovery.
NodeService and StorageService may continue their autonomous generation
lifecycles until their later shutdown barriers. Supervisor drains and discards
both reliable event streams after terminal shutdown rather than retaining a
generation, starting recovery, or forwarding another API generation. Event-path
closure after its owning service completes shutdown is expected.

Exercise the
[ApiService cancellation domains](api-service.md#api-task-ownership-and-completion--settled)
independently. Cancelling one HTTP child drops only its request-local work and
detaches its cache waiter; sibling requests and the shared cache job continue.
Cancelling one SSE child removes only that connection's registration and
permit, while publication replacement preserves the connection token.
ApiService reset cancels none of the HTTP, SSE, or cache-job roots. During
terminal shutdown, the final Stale notification enters the ordinary SSE
buffers before the SSE root is cancelled; the HTTP root follows, then the
cache-job root after request and connection guards complete. Token
cancellation alone must not satisfy a guard, task-tracker, runtime, or encoding
completion barrier.

Graph-update and API projection tests cover a non-Genesis block whose selected
parent index addresses the expected member of `direct_parents`, plus Genesis
with `selected_parent_index = None`, an empty `direct_parents` list, and no
synthetic ORIGIN parent.
Genesis recognition must not require the public projection or Web client to
expose or consult persisted `NodeMetadata.genesis_hash`.

Browser graph tests cover the [Web contract](web.md): update acquisition,
fixed-view freeze and follow-live behavior, stable block identity, direct
parent rendering, and Genesis recognition from zero actual direct parents.
Verify a Head-backed fixed view introduces no distance-based timer and follows
the ordinary canonical-delta continuation and SSE-wakeup flow while its extent
remains eligible. Retention expiry still freezes the last coherent image for
explicit refresh.

Web unit tests use Vitest beside the owning source under `web/src/`. Browser
flows use Playwright under `web/tests/browser/`, with test-only support under
`web/tests/support/` and shared recorded inputs under `fixtures/web/`. The
frequent browser suite runs Chromium. Firefox and WebKit are release smoke
coverage rather than multiplying every routine run.

The normal browser suite starts Vite and a deterministic test-only HTTP/SSE
fixture server that replays recorded public payloads without reimplementing
graph behavior. The same Playwright scenarios can target an externally started
complete KGI stack. Pure graph mutation, delta, decoding, and browser state
machines stay at the Vitest level; Playwright covers actual HTTP, SSE,
publication replacement, canvas startup, navigation, and interaction. Keep
visual snapshots few and run them in one pinned Chromium and operating-system
environment; semantic graph correctness does not depend on pixel comparison.
`npm run test:browser` selects the deterministic fixture server;
`npm run test:browser:full` selects the externally started complete stack.

Verify the
[repository and release contract](overview.md#repository-web-build-and-release-structure--settled):
the bundle exposes no partial final directory, contains one matching binary
and complete Web build, derives the standard Web root, rejects a missing root,
and serves the deployment-neutral application with its runtime configuration.
For `/kgi-config.json`, verify the exact compact absent-template body, the
exact property name and configured-string encoding, the `application/json`
content type, `Cache-Control: no-cache`, absence of content encoding, and the
strong `"kgi-config-<sha256>"` validator computed over the exact body.
Verify stable validators for identical bodies across restart, changed
validators for changed bodies, nonmatching conditional `200` responses, and
matching list-member and `*` requests returning bodyless `304` responses with
the current validator and cache directive. The endpoint remains uncompressed
regardless of `Accept-Encoding`.

Web decoder tests accept the exact `null` and valid-string shapes, retain the
previous validated value after `304`, and reject a missing property, an extra
property, a non-string/non-null value, malformed JSON, and an invalid template.
Template validation covers zero and multiple literal placeholders, a lone
percent-encoded `%7Bhash%7D`, non-HTTP schemes, credentials, and text whose
probe expansion is not an absolute valid URL. Verify that substitution occurs
in the raw template before WHATWG parsing can encode the braces and that
startup retains the raw template rather than the normalized probe.

For a known selected block, expand with its full canonical hash and assert the
exact normalized destination serialized by the URL parser. Reject dictionary
indices, abbreviated hashes, database IDs, and hashes from another block as
substitution sources. Inject a final expansion or validation failure and
verify that the Web performs no navigation, hides the action for that retained
configuration, and preserves graph state and selection.
Exercise fetch and decode failures with no prior valid value. Every absent,
invalid, or unavailable value hides the explorer action without delaying or
changing graph rendering. Packaging checks also cover console plus rotating
file logging, explicit file-log disablement, invalid simultaneous log options,
and writable log-volume replacement without local correctness state.

Verify the
[process configuration contract](overview.md#process-configuration-and-command-entry--settled)
in the top `kgi` crate using the `kgi-core::config` value structs. Use
table-driven resolution across compiled defaults, an explicitly selected
strict TOML file, environment values, and explicit CLI values. Cover absent
and unreadable selected files, unknown TOML fields, invalid explicit values,
field-level precedence, atomic network-selector precedence, conflicting
selectors, implicit mainnet, testnet's default and explicit suffix, forbidden
suffix combinations, every network-derived default RPC endpoint, the required
database URL, the derived and explicitly configured Web roots, and explicit
overrides of every compiled default. Exercise the Web-root override through
TOML, environment, and CLI sources.

Cover service entry and `database reinitialize` independently. Assert that
help and version need no configuration; complete validation precedes logging
files, signals, connections, HTTP binding, and component startup; the one-shot
command starts none of the long-lived service graph and exits after its
definite outcome; and successful token handling continues service startup.
Exercise the source restrictions and mutual exclusion of initialization,
clear, token, and confirmation options. Configuration, diagnostic, status,
confirmation, panic, and debug paths must redact database credentials and the
token. Temporary endpoint unavailability enters the owning service lifecycle
rather than static configuration failure.

## Resource isolation and performance

API load tests verify the
[system isolation contract](overview.md#resource-isolation-and-scalability--settled)
and [API bulkheads](api-service.md#resource-isolation-and-saturation--settled).
Exercise status, head, and historical admission lanes independently, including
their focused-owner rejection limits, while measuring both processors' commit latency.
Also cover bounded slow-SSE behavior, Head graph-memory exhaustion with
complete levels, explicit rejection or temporary unavailability, and the required
operational measurements. Saturated API work must not materially delay
processing or produce a partial graph image.

At every focused-owner admission boundary, exercise the final accepted request
and the next rejected request independently for Head, historical, status, and
SSE work. Fill an SSE delivery buffer with coalescible graph wakeups, then with
required ordered messages, and verify coalescing or disconnect respectively.
An HTTP graph delivery exceeding its owner-defined duration closes after
commitment and releases admission and any request-owned body. A delivery from
`GraphCache` must share the cached immutable `Bytes` allocation rather than
copying the body per waiter; cancelling or timing out one waiter does not
release the cache-owned allocation.

For encoding work, fill the reservation queue before issuing a historical
request and verify rejection performs no database access. With a reservation
held, complete and detach a historical projection while every encoder is busy;
the request must wait without a database resource and then encode when admitted.
Exercise the queue timeout as temporary service unavailability, cancellation
as reservation release, single-flight waiters without extra queue entries, and
the focused-owner active and historical encoding limits. Publication and Head
work must continue while historical encoders are saturated. Assert that at
most four encodes run concurrently and at most two are historical, that no
second response-memory semaphore is acquired before or after encoding, and
that many waiters on one source-keyed job still produce one encode. Track the
transient encoding buffers, completed uncached delivery bodies, and cache-owned
encoded bytes independently while exercising these paths.

For historical request resource lifetime, stall response delivery after the
complete database projection has been materialized and verify that no
transaction, connection, row stream, or API DB permit remains held while
processing obtains its reserved database capacity. Cover client disconnect,
serialization or compression failure, and an oversized response after database
release. Query/database-replacement races must retain the storage gate's old
coherent detached result or clean 503 outcome even when database resources were
released before response construction completed.
Saturate public historical reads and prove the pool capacity reserved for
publication and generation work remains usable. Force the focused-owner query
timeout and verify transaction, permit, and connection release through the
request-local internal-failure path without retiring the database generation.

Go KGI parity fixtures, rusty-kaspa RPC and notification fixtures, PostgreSQL
integration tests, and browser graph tests accompany the applicable groups
above.
