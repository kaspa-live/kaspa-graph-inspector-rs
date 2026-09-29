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
   without `--override-params-file` use their defaults; with the option they
   parse and apply rusty-kaspa `OverrideParams`. An unreadable, malformed, or
   incompatible explicit file and use of the option with mainnet or any
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
14. Exercise `MAX_DAA_SCORE` and `MAX_BLUE_SCORE` successfully, then exceed
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
   Persistent `--initialize-db` is idempotent for a compatible database and
   never rebinds its network. Interactive initialization identifies the
   database and complete network binding without exposing credentials;
   noninteractive initialization requires explicit authorization. `--clear-db`
   may authorize first initialization but never claims or rebinds an existing
   incompatible schema.
5. One-shot administrative reinitialization requires explicit confirmation,
   replaces only a recognized KGI schema, binds the new Empty database to the
   validated `(network_id, genesis_hash)`, and exits. A changed declarative
   token performs the same reset once; restart with the stored token preserves
   the database. Unknown tables are never claimed or destroyed, and neither
   form can expose a partially recreated schema.
6. Advisory-lock contention rejects with `DatabaseAlreadyInUse`, and lock loss
   retires the DB generation. Compatible v2 migrations are ordered,
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
local retry, and retires it. Exercise both actual commit and rollback behind
that ambiguous result and require the replacement generation to derive the
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

Verify [teardown](processing-lifecycle.md#teardown-and-delivery-semantics--settled)
by asserting that both processors' session-scoped RPC and DB clones are
released before `Deactivated`, cancellation races finish, and full versus
closed bounded channels retain their distinct dispositions.

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

Verify the [graph-update feed](api.md#in-process-api-and-graph-update-feed--settled)
and [graph publication contract](api.md#graph-views-publication-revision-and-history--settled)
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
visible edges, and fresh publication identity after each replacement.

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

Verify the settled [head block mutation](api.md#head-block-mutation--settled)
below and at full depth, at the existing high level, at a new high level, and
with a below-range commit. Cases cover block and child-owned edge pruning,
existing and new endpoint counters, multiple edges sharing a parent level,
counter-only maintenance, and removal or survival of an external level.

Verify [Fixed updates through Head](api.md#fixed-updates-through-the-head-cache--settled)
with both crossing orders at the overlap boundary, an original update that
produces no Head delta but changes Fixed, block metadata split between the two
views, an above-Fixed added member that recolors a retained merge-set block,
and a retained endpoint-level DAA change. Cover independent revisions, missing
required metadata, and the first post-update disjoint extent. The latter two
must preserve the last coherent image and make every later mutation fail under
the Frozen rule. No case may apply a Head-generated delta to Fixed or perform a
storage read.

Verify the settled graph-model core with a view and history initially at
revision `n`. A retained mutation advances the view to `n+1` and returns
`GraphDelta(n,n+1)` before history append. During that interval, a view read may
return `n+1` while a history request exposes only `n`; appending the delta then
advances history to `n+1`. A no-effect update returns no delta and advances
neither revision. History rejects a nongapless append and accepts a gapless
aggregated interval without requiring internal one-step boundaries.

Verify every subview extract inherits its source revision with
`TrackingPolicy::Frozen`. `BlockCommitted` and `VspcCommitted` mutation entry
points return `GraphViewUpdateError::Frozen`; delta application returns
`GraphDeltaApplyError::Frozen`. Each refuses before inspecting the input or
changing contents or revision. Keep this distinct from a no-effect update
accepted by `Head` or `Fixed`.

Verify [frozen subview extraction](api.md#frozen-subview-extraction--settled)
from `Head`, `Fixed`, and `Frozen` sources, including nested extraction. Cover
invalid and outside extents; inherited revision and calculated nominal depth;
complete in-range blocks; crossing edges with zero, one, and two endpoint
blocks; all nominal and endpoint levels with exact state; recomputed derived
counters; and absence of source mutation, delta production, or history.

Level-change cases cover create, update, remove, and no-net-change composition;
application of `after` without validating `before`; a size-only external-level
mutation; and DAA score changes. Every API delta carries its target
`high_level` and no lower coverage object. Retain all deltas whose `high_level`
remains in the retained head window, including multiple revisions at one
level, and make them eligible for pruning only after that level leaves the
window. Cover indivisible aggregated entries, longest-prefix pruning, derived
oldest available revision, and unchanged current revision. No count or byte
pressure may prune history earlier.

Verify the [graph-view edge and absolute-map contract](api.md#graph-views-publication-revision-and-history--settled)
with zero, one, and two endpoint blocks retained; Head child addition and
removal; parent-only removal under Head pruning; Fixed crossing-edge addition;
outside-extent parent levels under both policies; and PP-boundary sentinel
links. Cover all four right-biased `Some`/`None` block/edge composition pairs,
retention of the final explicit entry, map-key construction from the value
identity, and composition without value comparison or a starting view. Keep
VSPC projection treatment separate.

Verify the settled
[VSPC projection and composition contract](api.md#vspc-projection-and-delta-composition--settled)
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

Verify [DAA navigation and graph windows](api.md#daa-navigation-and-graph-windows--settled)
for floor selection and tie break, the sentinel result, a reorg-created
VSPC-empty level, atomic level-score publication, and navigation plus window
consistency from one image. Accept zero and `MAX_DAA_SCORE` as query bounds and
reject the no-VSPC sentinel as a real DAA query.

Verify successful level, block-hash, and DAA anchors return a
`GraphWindowResolution` whose resolved level lies in its effective capped
range and whose resolution and graph contents come from the same immutable
graph view or database transaction. Browser cases retain the original anchor,
keep the returned level fixed across eligible projected deltas, and re-resolve
only when explicit refresh resubmits that anchor. A database-backed window has
no public delta lineage and remains frozen until that refresh.

Verify the settled
[Head-bounded Fixed projection](api.md#head-bounded-fixed-delta-projection--settled)
from a Head-extracted anchored window. Cover a fully contained extent, an empty
projected mutation that still advances the Head cursor, blocks inside the
extent, crossing edges with neither endpoint block present, nominal and
external endpoint levels, and retained VSPC-membership and color changes. A raw
Head delta must never be delivered as the projected response.

Advance Head until the fixed lower bound leaves its nominal extent. Verify an
indivisible crossing history entry is not projected, any earlier complete
prefix is returned and applied, and the browser then freezes. Also cover no
valid prefix, publication replacement, Stale publication, and expired history.
When a retained edge needs an unchanged endpoint level, fetch it from the
current Head view; accept that this level can be newer than the returned
cursor, but never use it for coverage or cursor decisions. Absence of that
required level freezes only the browser fixed image. Head pruning removals
outside the fixed projection are discarded without affecting Head or the
browser image.

Verify deltas and client behavior across
[API publication](api.md#graph-views-publication-revision-and-history--settled) and
[Web update acquisition](web.md#update-acquisition--partially-open): sequential delta
composition and expiry, response-local hash dictionaries,
terminal Stale state, replacement publication identity, SSE slow clients,
fixed-view freeze, DAA focus, and Live arriving during construction or
alignment. The latter must publish the completed image directly as Live.

Verify the settled
[publication-state contract](api.md#publication-state-and-revision--settled):
initial Synchronizing, direct initial Live, visible `Synchronizing -> Live`,
and terminal Stale transitions leave graph view and history revisions
unchanged. Cover reliable `reset` processing without an application-completion barrier,
cancellation of unpublished construction, no additional effect for an already
Stale publication, delta rejection for Stale, head-snapshot state and ETag
changes, and immutable delta bytes across later state changes. SSE cases cover
state-only wakeups repeating one graph cursor, reconnect reporting current
state, Web adoption without a delta request, and a fresh publication ID for a
replacement image.

Verify the settled [core composition](overview.md#core-crate-structure--settled)
and [Supervisor control](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
boundaries: the top `kgi` crate owns the four component `Arc` values and uses
their public async methods without a Supervisor-facing command handle or
command enum. In particular, verify the
[ApiService control boundary](api.md#reset-and-recovery-time-availability--settled):
Supervisor can call `reset` and await the `shutdown` barrier while
`kgi-processing` remains independent of `kgi-api-core`; `reset` returns after
reliable acceptance without waiting for application. Verify status observations are
latest-value and lossy, remain available through the separate memory-only
lane, and never influence lifecycle decisions. Node status begins without a
validated observation, replaces it before each Ready publication, and
preserves it with explicit last-successfully-validated meaning through every
non-Ready state, including Stopped. No status path reads or writes PostgreSQL
or gives ApiService a NodeService dependency.

Delta cases cover Frozen and revision compatibility as the only application
checks, direct target-state installation, Head bound recalculation, fixed-bound
retention, and equality between sequential application and a directly or
incrementally composed interval;
associative graph-state effects across three adjacent intervals; later-value,
insertion-folding, final `high_level`, and response-dictionary composition; and
rejection of cross-publication or nongapless composition. Under a small response
budget, verify advancement through complete stored entries. Cover a requested
target inside an aggregate by extending through its right boundary, a future
target by returning through current history, and a prefix ending at an earlier
stored boundary. A starting cursor inside an aggregate is unavailable. A first
stored entry that cannot fit requires a fresh snapshot rather than a partial
delta.

For encoded head-response reuse, issue concurrent identical snapshot requests
at one cursor and effective window and verify that they share one construction,
serialization, and compression result. Do the same for an exact delta
interval. Requests differing in publication ID, revision, interval,
effective window, response format, `representation_version`, or content
encoding must not share encoded bytes. Head-view requests differing in
publication state must not share encoded bytes; one immutable delta interval
may reuse its bytes across later state changes. A "to current" request must capture an
exact target before reuse; a bounded complete-prefix response is cached by its
actual returned interval. Eviction must reconstruct an equivalent response
from the current graph view or retained deltas. Historical DB windows bypass
this cache. Projected Fixed delta requests also bypass cache lookup, insertion,
and cache single-flight, return `Cache-Control: no-store`, and produce no ETag.
Repeated identical requests therefore execute independent on-demand
projection. Cache pressure must not delay or fault processing.

Verify the settled
[database-seed projection](api.md#database-seed-extent-and-projection--settled),
[head-publication lifecycle](api.md#head-publication-lifecycle-and-stream-alignment--settled), and
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

Lifecycle cases cover `AwaitReset -> PreSeal`, waiting for the first-channel
PostSeal marker while producer-side suppression remains active,
`Constructing -> Aligning`, and the prohibition on direct
construction-to-activation. Alignment covers block IDs at and below the
snapshot cut, a first greater ID with no retained effect, a first greater ID
that returns a delta, and the matching VSPC source. Exercise both crossing
orders. After one cut crosses, later updates from that source apply while the
other source continues its snapshot-relative filtering. Empty staging and
either single-cut state remain Aligning and publish no candidate, including an
indefinitely idle second source. Only after both cuts cross, apply all later
interleaved updates through a captured activation frontier and leave newer
arrivals for Active. Cover first publication revisions zero and above zero,
plus independently atomic view and history visibility at adjacent revisions.

Pre-seal suppression produces no gap. Verify the database seed started after
the marker covers every intentionally suppressed commit, including block and
VSPC commits racing with the marker transition; an offer ordered after the
transition enters the channel instead. No pre-seal seed or second marker is
required.

Exercise the universal reconstruction primitive from Constructing, Aligning,
and Active. The first two abandon their candidate; Active first becomes
terminally Stale. In every case, drain through the first observed channel
Empty, discard graph mutations, preserve sticky Live, capture the newest gap
generation, and start a newer seed. Updates after the empty frontier must be
staged, while a later gap-generation change restarts reconstruction again.
Cover staging overflow, query/generation/projection failure, alignment or
application failure, and a gap during each state. None may request processing
recovery. `PublishPostSeal` is not repeated during same-session reconstruction.
Changing the API database generation alone must not replace an otherwise
advancing Active publication.

Verify [Reset and recovery-time availability](api.md#reset-and-recovery-time-availability--settled)
with ordinary Resync and Rebuild integration scenarios. Verify the fresh
`GraphUpdateReceiver` and exact recovery mode; global preemption from
PreSeal, Constructing, Aligning, and Active; and old receiver replacement.
Supervisor proceeds after `reset` returns without waiting for its application.
Resync preserves historical reads, and failed reconciliation followed by
Rebuild calls `reset` again with a fresh ingress. Rebuild relies on
StorageService rather than `reset` to close API database admission, retire the
old API generation, and boundedly drain or cancel active database phases.
Verify a detached old projection may complete delivery while
an undetached request returns 503, public reads reopen only after the aligned
replacement becomes Active, public reads never rebind implicitly to a newly
published storage generation, and no request observes a partial or mixed
generation, including with PostgreSQL `TRUNCATE`. Verify ordinary sender
teardown may close the installed receiver, channel closure is not a lifecycle
signal, and a marker worker with an accepted delivery may outlive
BlockProcessor Deactivate without retaining an RPC or DB client, then exits
after delivery or receiver replacement.

Verify the [ApiService shutdown barrier](api.md#apiservice-shutdown--settled)
from `AwaitReset`, `PreSeal`, `Constructing`, `Aligning`, and `Active`, including
races with `reset`, marker delivery, gap reconstruction, database projection,
serialization, detached response delivery, and SSE wakeups. Admission and SSE
close first; the graph receiver drop unblocks a waiting marker worker; every
API task and database resource ends before method completion; no publication
or revision mutation is emitted; and repeated `shutdown` is idempotent.
Supervisor starts ApiService and ResyncEngine shutdown without waiting for one
to complete before starting the other, awaits both method barriers, and only
then calls NodeService shutdown followed by StorageService shutdown. Producer
closure during that barrier requests no reconstruction or processing recovery.

Graph-update and API projection tests cover a non-Genesis block whose selected
parent index addresses the expected member of `direct_parents`, plus Genesis
with `selected_parent_index = None`, an empty `direct_parents` list, and no
synthetic ORIGIN parent.
Genesis recognition must not require the public projection or Web client to
expose or consult persisted `NodeMetadata.genesis_hash`.

Browser graph tests cover the [Web contract](web.md): update acquisition,
fixed-view freeze and follow-live behavior, stable block identity, direct
parent rendering, and Genesis recognition from zero actual direct parents.
For distance-adaptive fixed views, verify no added throttling while the head is
visible and at distances through 10, then verify that distance 11 enters the
configured increasing-delay policy without breaking contiguous projected
delta catch-up or explicit-refresh fallback after Head retention expires.

## Resource isolation and performance

API load tests verify the
[system isolation contract](overview.md#resource-isolation-and-scalability--settled)
and [API bulkheads](api.md#resource-isolation-and-saturation--settled).
Exercise status, head, and historical admission lanes independently, including
configured rejection limits, while measuring both processors' commit latency.
Also cover bounded slow-SSE behavior, cache-memory exhaustion with complete
levels, explicit rejection or temporary unavailability, and the required
operational measurements. Saturated API work must not materially delay
processing or produce a partial graph image.

For historical request resource lifetime, stall response delivery after the
complete database projection has been materialized and verify that no
transaction, connection, row stream, or API DB permit remains held while
processing obtains its reserved database capacity. Cover client disconnect,
serialization or compression failure, and an oversized response after database
release. Query/database-replacement races must retain the storage gate's old
coherent detached result or clean 503 outcome even when database resources were
released before response construction completed.

Go KGI parity fixtures, rusty-kaspa RPC and notification fixtures, PostgreSQL
integration tests, and browser graph tests accompany the applicable groups
above.
