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
01b532e8b553523216471682649693af92f0fd16
```

This pin makes the architecture analysis reproducible. It identifies the
rusty-kaspa source used to assess KGI's assumptions; it is not a runtime
version restriction. KGI may connect to other node versions after ordinary
validation, but the PUAR provides no correctness claim for those versions or
for custom builds.

The PUAR checklist is:

1. For [Genesis discovery](node-service.md#genesis-discovery),
   `GetBlocks(None, false, false)` returns the configured Genesis hash first and
   makes that first hash usable as the generation's Genesis identity. KGI does
   not rely on the accompanying block vector.
2. For the NodeService
   [individual recovery GetBlock](node-service.md#individual-recovery-getblock)
   used by [Resync preparation](processing-lifecycle.md#resync-preparation),
   header-only `GetBlock` supplies the required DAA score and blue work, supplies
   the ordinary-block blue score, and establishes that the returned block is
   recognized as a GetBlocks low hash. KGI does not depend on the raw Genesis
   blue score.
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
9. For NodeService
   [individual recovery GetBlock](node-service.md#individual-recovery-getblock),
   an unknown requested hash produces
   `ConsensusError::HeaderNotFound(requested_hash)`; that variant and
   `ConsensusError::BlockNotFound(requested_hash)` have distinct exact display
   forms; and the gRPC conversion preserves either error's display string in
   `RPCError.message` before reconstructing it as `RpcError::General` in the
   selected Rust client. The review also records that unrelated remote and
   client failures can use the same `General` variant, so the variant itself is
   not a semantic discriminator.
10. For NodeService
    [client response-conversion classification](node-service.md#client-response-conversion-failures),
    the selected client performs the documented eager conversions for
    `GetSink`, `GetBlock`, `GetBlocks`, `GetBlockDagInfo`, and
    `GetVirtualChainFromBlockV2`; its error variants and structured
    missing-field identifiers retain exactly the documented attribution,
    ambiguity, and ignored-field boundaries.

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

Architecture accepts the expanded
[9 October 2026 PUAR](../reviews/2026-10-09-rusty-kaspa-01b532e8-assumptions.md)
against the full pinned revision above. All ten checklist items are
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
   `ValidatedNodeBlock` from the exact consumed header and verbose fields. An
   ordinary block requires verbose data and selected-parent membership, uses
   the trusted cached header hash and header blue score, preserves canonical
   first-occurrence order independently in each relationship vector, and
   ignores every named unused RPC field. Supply repeated positions in the
   level-zero parents and in each merge-set color; each resulting vector
   contains every hash once at its first occurrence. Include one hash in both
   merge-set colors and verify that cross-vector overlap remains accepted.
   Separately place the block's own authoritative hash in each of the three
   relationship vectors and verify common normalization rejects every case;
   no source operation may receive a self-referential `ValidatedNodeBlock`.
   Separately, cover exact-Genesis construction without verbose data: ORIGIN,
   empty parent and merge-set vectors, and blue score zero are synthesized
   while timestamp, representable DAA score, and blue work come from the
   header.
2. GetBlocks fixtures cover an empty block vector, the inclusive trusted header
   hash, a valid anchor-only response, a nonempty result whose final hash equals
   `low_hash`, and a page with one invalid retained full-block member. Assert
   that mismatched or unequal parallel hash vectors and duplicate block hashes
   are ignored, node block order is preserved, and the final retained header
   hash is the next cursor. For every malformed recovery shape, assert typed
   whole-page rejection, no cursor or processor advancement, and retirement of
   the exact RPC generation.
3. Genesis discovery request construction with `low_hash = None`, blocks and
   transactions disabled, plus response handling for a nonempty hash vector,
   either empty or nonempty ignored block vectors, opaque RPC-call failure,
   and an empty hash vector. Given a valid response, NodeService uses its first
   hash without substituting a local Genesis constant.
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
   Reject a removed chain without an added path, a nonadvancing nonempty added
   cursor, and a nonempty removed path whose first hash differs from `low_hash`
   as the corresponding `MalformedVspcResponse` reason, with generation
   retirement and no cursor advancement. Accept and preserve duplicate members,
   removed/added intersections, and a nonfinal occurrence of `low_hash` in
   `added` when the retained framing conditions hold.
7. Mocked notification and VSPC V2 responses with a nonempty removed chain and
   an empty added path. Assert the source-specific dispositions: a notification
   reports
   `NotificationInputInvalid(MalformedVspcChange(RemovedChainWithoutAddedPath))`
   and requires Resync;
   `ValidatedRpcClient` rejects a synthetic response as
   `MalformedVspcResponse(RemovedChainWithoutAddedPath)` without returning a
   normalized change, and ProcessingSession follows the shared malformed
   recovery-response policy without dispatch or cursor advancement.
8. The subscription activation order across the
   [processing lifecycle](processing-lifecycle.md#entering-recovery-phases) and
   [NotificationRouter](node-service.md#notificationrouter): ProcessingSession alone
   sends both processor Catchup commands before invoking NodeService activation
   with that session's fresh block, VSPC, and reliable fault endpoints;
   NodeService sends no processor command; routing remains Disabled until both
   remote starts succeed; and callbacks received during that interval are
   dropped without overlap credit or immediate recovery. Disabling clears all
   three endpoints. Reuse the same healthy validated generation in a later
   session and prove a callback or router fault can reach only the newly
   installed endpoints. Cover a failed first start and a failed second start
   with successful rollback: both return `SubscriptionControlFailed`, retain
   the exact generation, emit no `RpcRetired`, and permit a fresh activation.
   A failed rollback and a failure of either remote stop disable routing, clear
   the endpoints, retire the exact generation, enqueue `RpcRetired` before
   returning `GenerationLost`, and never expose a partial subscription. Verify
   that both remote stops are attempted.
9. Verify the
   [current pruning-point block contract](node-service.md#current-pruning-point-block)
   with success, canonical Genesis construction, non-Genesis selected-parent
   membership, every listed malformed response condition, exact-generation
   retirement, and `RpcRequestFailed` or generation loss without inferring a
   database mismatch. A repeated response-network value is ignored. An
   exact-Genesis response with a nonzero or out-of-range raw blue score still
   normalizes to the domain-owned zero and may produce the zero boundary
   threshold. Supervisor may already have completed a failed attempt's `reset`
   call; the next attempt supersedes it with a fresh ingress.
10. Verify the
    [Catchup sink-sample contract](node-service.md#catchup-sink-sample) with
    ordinary and exact-Genesis success, ORIGIN, an advertised sink that is
    definitively not found, missing or malformed header data, and a trusted
    header hash different from the advertised sink. Ignore verbose data and
    redundant reported hashes. Exact-Genesis success includes a representable
    nonzero DAA score. Every malformed case returns
    `MalformedCatchupSinkResponse`, retires the exact RPC generation, and
    consumes the shared malformed-input budget.
    Cover an out-of-range DAA score without generation retirement or budget
    consumption, `RpcRequestFailed` and generation loss as session faults, and
    a distinct cancellation outcome.
11. Individual full-block GetBlock validates the requested hash and every
    `ValidatedNodeBlock` invariant. Malformed output retires the exact RPC
    generation; definitive not-found and `RpcRequestFailed` retain their
    distinct classifications. Exercise the NodeService-owned
    [GetBlock compatibility classification](node-service.md#getblock-not-found-compatibility-classification):
    the exact `HeaderNotFound` and `BlockNotFound` messages built for the
    requested hash each yield the same definitive not-found result, while a
    different hash, changed case, leading or trailing content, and unrelated
    general errors do not. Every nonmatch
    becomes `RpcRequestFailed`, leaves the exact generation valid, and exposes
    its message only as diagnostics. After this adapter, only its typed result
    may select caller or lifecycle behavior.
12. Exercise the NodeService-owned
    [client response-conversion mapping](node-service.md#client-response-conversion-failures)
    at the actual pinned wire-to-typed conversion boundary and at each runtime
    operation mapper. Cover malformed sink hex; missing individual GetBlock
    response block and block header; the dedicated blue-work conversion error;
    a missing GetBlocks block header; and exact-generation retirement before
    returning each attributable malformed result. Cover ambiguous hash or
    blue-work conversion in GetBlock, invalid ignored GetBlocks parallel hashes
    and payload fields, both consumed and ignored GetBlockDagInfo hashes, and
    both consumed and ignored VSPC fields as opaque `RpcRequestFailed` results
    that retain the generation. Unknown structured missing-field pairs and
    generic response-envelope conversion errors are opaque. These fixtures
    must prove that formatted diagnostics never select this policy.
13. BlockAdded normalization failure disables routing, enqueues no block, and
    reports `NotificationInputInvalid(MalformedBlockAdded)` without retiring
    the RPC generation.
14. NotificationRouter drops the valid empty VSPC no-op and rejects a nonempty
    removed path with an empty added path without enqueueing it, disabling
    routing without retiring the RPC generation. Duplicate members and
    removed/added intersections are preserved and delivered normally.
14. Saturate each NotificationRouter destination independently. The triggering
    notification is not enqueued, both streams are disabled, and the router
    reports `SessionContinuityLost`; the particular destination remains
    diagnostic context and does not create a notification-specific fault.
15. Exercise `MAX_DAA_SCORE` and `MAX_BLUE_SCORE` successfully, then exceed
    each consumed score by one in the full-block and header-only operations
    that use it. Every excessive consumed value reports the corresponding
    `ScoreOutOfRange` fault, returns no normalized value, is Fatal without
    retiring the RPC generation, and does not consume the malformed
    recovery-response budget. Include BlockAdded, retained GetBlocks members,
    current-pruning-point, Catchup sink-sample DAA, and both individual GetBlock
    forms as applicable. An ignored score does not create a fault. In
    particular, exact Genesis ignores raw blue score and synthesizes zero. A
    full block timestamp of `u64::MAX` passes normalization unchanged and
    produces no timestamp-specific fault.
    For `recovery_header`, cover an ordinary missing and out-of-range blue score
    plus exact-Genesis absent, nonzero, and out-of-range raw blue scores. Every
    exact-Genesis case returns `ValidatedRecoveryHeader.blue_score = 0` while
    still requiring and range-checking DAA score and requiring blue work.
16. RPC API compatibility uses the `RPC_API_VERSION` and `RPC_API_REVISION`
    constants from KGI's compiled `kaspa-rpc-core`. Accept an exact version
    with an equal or greater remote revision. Reject lower and higher versions,
    an exact version with a lower revision, and a completed response missing
    either value. Every rejection is terminal, publishes no validated RPC
    generation, and reports the required and observed pair. A transport or
    generation failure before the complete response remains transient and does
    not enter terminal rejection.

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
unexpected event-path closure while Supervisor is Running. Race an in-flight
operation with retirement and require one linearized old-generation success or
generation-loss result, never completion through the replacement. Verify that
a stale retirement or operation report cannot affect the replacement, that an
existing processing session never adopts it, and that the retired router
enqueues no later callbacks while already-enqueued work remains confined to
old-session teardown.
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
2. Exact Genesis retains a non-null ORIGIN identity without inventing a direct
   parent. Web Genesis detection uses the actual absence of direct parents
   rather than visible-edge absence.
3. Hash interning follows the storage-owned reference-first order without a
   separate selected-parent stage. Exact Genesis explicitly interns synthetic
   ORIGIN before its own hash. Strict materialization creates no boundary
   identity.
4. Database startup distinguishes Uninitialized, Empty, Initialized,
   boundedly detected Inconsistent processing contents, and rejected schemas.
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
   retires every currently published DB generation and emits its exact
   retirement event without directly changing processing-session or API
   publication state. Cover lock loss both with and without a current API
   generation. Compatible
   v2 migrations are ordered,
   transactional, revalidated, and finish before client publication. Cover
   newer-schema and v1/unsupported rejection and the prohibition on automatic
   down or online migration. At the StorageService boundary, exercise both a
   dirty migration marker and a checksum mismatch. Each publishes no validated
   generation, emits exactly one terminal `Rejected(MigrationFailed)` event,
   completes pending initialization with that rejection, performs no reconnect
   or migration retry, and makes Supervisor enter Fatal. A later successful
   service shutdown proves cleanup and does not substitute for the rejection.
   Separately inject migration execution connection loss and verify transient
   `Unavailable`, shutdown-cancellable service-backoff retry of the complete
   startup attempt, and no terminal rejection.
7. Storage may open, lock, and inspect Uninitialized contents before node
   validation, but only atomic publication of complete `DatabaseBinding` crosses
   `Uninitialized -> Empty`. Inject a crash around this transaction and prove
   that no partially bound Empty state can appear.
8. `DatabaseBinding` and `ProcessingMetadata` use separate singleton rows and
   retain their independent lifecycles. Empty has the binding and no processing
   metadata; Genesis-anchored Initialized has processing metadata with score
   zero. The single `ValidatedDbClient` exposes the exact binding for compatible
   Empty, Initialized, and Inconsistent contents. Reject the same `NetworkId`
   paired with a different Genesis rather than rebinding or rebuilding.
9. A Genesis anchor is materialized at `(1,0)`, belongs to VSPC, has ORIGIN as
   selected parent and zero actual direct parents, and has a coherent committed
   sink. Rebuild and ordinary writes preserve the rule that ORIGIN is its only
   boundary identity; session loading does not rescan all identities to prove
   that global invariant.
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
    [stored session state](storage.md#stored-session-state--settled) with
    coherent `Empty`, `Initialized` carrying the exact database PP hash and
    score plus committed sink ID, hash, selected parent, and DAA score, semantic
    `Inconsistent` for each bounded metadata or anchor defect, and operational
    storage failure as distinct outcomes. Initial generation publication and
    the later session read use the same classifier, while the latter returns a
    fresh snapshot. Verify the storage-owned
    [bounded-cost contract](storage.md#bounded-processing-state-classification--settled)
    with query-plan evidence or a large fixture demonstrating that retained
    graph growth introduces no retained-table scan, recount, or traversal.
13. Exercise checked score conversion at zero and both domain maxima. SQL
    rejects writes outside the persisted ranges. Compatible bound contents
    with a negative metadata score or the DAA sentinel stored on the committed
    sink are classified `Inconsistent`. A negative processing-metadata score still publishes
    exactly one `ValidatedDbClient` with the exact binding and no API client;
    it constructs no `ProcessingMetadata`, stores no processing metadata in the
    client, and never invents or converts a score. Rebuild keeps that exact
    processing client, publishes a fresh API generation behind the closed
    replacement gate before mutation without emitting `ApiDbRetired`, and opens
    that same generation only after definite commit.
    A defensive out-of-range storage input is rejected before mutation with
    the typed DAA or blue
    `ProcessingDbError::ScoreOutOfRange` reason. Round-trip timestamps `0`,
    `i64::MAX`, `i64::MAX + 1`, and `u64::MAX` through the signed `BIGINT`
    bit-pattern encoding; upper-half negative storage values are not
    inconsistent contents.
    Separately exercise coordinate and level-size bounds: materialized levels
    `1` and `u32::MAX`, slots `0` and `MAX_LEVEL_SLOT`, and sizes `1` and
    `MAX_LEVEL_SIZE` round-trip through their SQL representations. SQL rejects
    level zero, a level above `u32::MAX`, slot `u16::MAX`, size zero, and a size
    above `MAX_LEVEL_SIZE`. Deriving a child above `u32::MAX` and allocating
    beyond the maximum level size each panic as internal invariants; the latter
    leaves the transaction and all persistent state unchanged.
    `BlockCoordinate::new` accepts `(1, 0)` and
    `(u32::MAX, MAX_LEVEL_SLOT)` and rejects level zero and slot `u16::MAX`.
14. Verify the storage-owned
    [processing-cache requirement](storage.md#caches-and-identity-resolution--settled):
    the forward identity, materialized-block metadata, merge-set, and
    capacity-one current-sink domains use separate Moka caches. Exercise a
    complete VSPC member cache hit from block metadata alone and missing block
    metadata through one batched SQL fallback, current-sink seeding from fresh
    session state, and replacement by the destination only after definite VSPC
    commit. Rollback and ambiguous commit publish no transaction-derived cache
    state. Rebuild and a replacement processing generation replace all four
    domains together and cannot observe predecessor entries.

Verify the [block materialization transaction](storage.md#block-materialization-transaction--settled)
and [PP seal behavior](block-processing.md#pp-boundary-phase-behavior--settled)
with an ordering fixture for a non-Genesis PP: the threshold block commits
before BlockProcessor enters PostSeal or emits `PpBoundarySealed`. The sealing
block's graph offer returns `SuppressedAwaitingCatchup` before the milestone.
For Genesis, verify that the rebuild transaction establishes an intrinsically
sealed boundary, Rebuild construction creates BlockProcessor in PostSeal, and
then emits the same exact-once milestone without ordinary Genesis
materialization. Neither seal path emits an API marker. Inject crash and
ambiguous-commit outcomes around both paths; an unproven non-Genesis seal emits
no milestone, while restart after a committed Genesis rebuild derives PostSeal
from storage and proceeds through ordinary Resync.

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
committed ID and new coordinate plus every canonical direct parent's coordinate
from the same committed transaction, with the coordinate absent for an
outside-boundary parent. Its non-repeating `level_snapshots` must contain the
complete resulting block level and every distinct materialized parent level,
preserve an existing level's DAA score while its size changes, translate the
no-VSPC sentinel to `None`, and exclude outside-boundary parents. Supply a
canonical validated parent sequence and verify Storage preserves its order,
creates one row and one `ParentCommitted` per relation, computes the
selected-parent index in that sequence, and produces no repeated coordinate or
level contribution. Supply canonical blue and red merge-set vectors with a
cross-color overlap and verify Storage preserves both ordered arrays without
another normalization step. Cover a selected parent first encountered in a
merge-set group and one first encountered among direct parents; neither case may
depend on the selected parent's numeric ID position. Rebuild consumes the same
canonical shared value.
Verify materialization relies on the shared own-hash exclusion, deduplicates
only the reference prefix of its interning order, does not append the selected
parent as a separate interning stage, and appends the incoming block hash
directly. Cover committed initial color and VSPC membership.
Inserted outcomes and their
BlockProcessor offers preserve increasing IDs while permitting allocation
gaps; `AlreadyMaterialized` returns no graph-update payload.
Verify BlockProcessor forwards the inserted payload before `PersistedBlock`;
full-channel delivery advances the session gap signal but does not suppress the
later `PersistedBlock` delivery or request processing recovery.

Verify the [atomic VSPC transaction](storage.md#atomic-vspc-transaction--settled)
for source continuity, every removed and added selected-parent relationship,
the removed/added pivot, direct-chain materiality, distinct identity resolution
with original repeated positions preserved, typed pre-mutation
`VspcSourceDiscontinuity`, and structured
`VspcPathDiscontinuity(VspcPathConflict)` evidence, atomic membership and
coloring changes, merge-set members represented only by boundary identity, and
final level-score publication. Cover duplicate members and a removed/added
intersection without a member-set rejection: ordinary source, materiality, and
path validation still applies, and any admitted change follows the existing
ordered removal, addition, and coloring steps. Its definite outcome contains
one complete post-commit snapshot for every distinct affected level, including
a level whose remove/add sequence restores its original score, and requires no
post-commit projection read.

Verify the successful VSPC path compares compact member and selected-parent
IDs without resolving hashes. For the first ID conflict, verify one bounded
in-transaction query resolves the distinct child, expected-parent, and
stored-parent IDs into `VspcPathConflict`; incomplete or invalid evidence
resolution returns
`ApplyVspcChangeError::Database(ProcessingDbError::InvalidStoredState)` rather
than a partial conflict.

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

Verify the
[validated processing-operation error boundary](storage.md#validated-processing-operation-errors--settled)
for `load_session_state`, `block_presence`, `resolve_materialized_ids`,
`materialize_block`, `apply_vspc_change`, and
`rebuild_from_pruning_point`. Their common database branches expose only
`ProcessingDbError`; raw `StorageError` database, connection, rejection,
control, event-path, worker, setup, and replacement-control variants cannot
escape through that capability. Cover these classifications and their
[lifecycle mapping](processing-lifecycle.md#supervisor-and-recovery-intent--settled):

- a failed nonmutating query returns `QueryFailed`, leaves the exact generation
  usable, retries the complete recovery attempt with ordinary backoff, and
  requires Resync in Live without waiting for a replacement generation;
- invalid persisted evidence from a bounded later read returns
  `InvalidStoredState`, retains the generation for replacement, and tears down
  the session before Supervisor starts a distinct Rebuild session;
- an internal or representation failure that Storage cannot safely continue
  after returns `ServiceUnavailable` and is Fatal as
  `Ownership(ManagedComponentUnavailable)` from StorageService;
- connection loss returns `GenerationLost` only after exact-generation
  retirement and follows `ServiceGenerationLost(Storage)`; and
- mutation failures retain their exact `PersistenceFault` classification and
  never become `QueryFailed`.

## Recovery lifecycle and Catchup

Verify [Resync preparation](processing-lifecycle.md#resync-preparation) with
fixtures that combine the stored database PP hash and score, stored sink
ID/hash/selected-parent hash/DAA score, and an exact no-transactions GetBlock
header. Cover:

- proof that Resync does not request or resolve the current node PP;
- successful `MaterializedSyncAnchor` construction from
  `ValidatedRecoveryHeader` and a coherent committed Materialized sink,
  including exact Genesis with normalized blue score zero;
- exact RPC and DB generation propagation through direct arguments into the
  complete inert component graph, followed by initialization of both processors
  from one `PreparedSync`, including VspcProcessor's committed sink and history
  seed;
- a definitively absent sink, incoherent stored sink, and a DAA mismatch
  requiring Rebuild;
- a Resync preparation result of `Require(Rebuild)` causing complete teardown
  before Supervisor creates a fresh Rebuild session, without any in-place mode
  change, reused session channel, or Rebuild component initialization or task
  launch from the retiring Resync attempt;
- proof that a Rebuild session never produces `RequireRebuild`, and that its
  startup can only complete as `Started`, `Failed` with its typed disposition,
  or `Cancelled` after an existing teardown request;
- `RpcRequestFailed` or another session failure without inferring Rebuild; and
- a response carrying the wrong trusted header hash or missing required header
  DAA score or blue work, plus an ordinary response missing blue score, as
  `MalformedGetBlock`, retiring the exact RPC generation without inferring
  Rebuild; and
- proof that ProcessingSession uses the normalized hash, DAA score, blue work, and
  blue score without reading or reinterpreting the raw RPC header.

Verify the common
[boundary seal threshold construction](processing-lifecycle.md#boundary-seal-threshold-construction)
through both preparation modes. Cover a zero Genesis threshold, a non-Genesis
result exactly at `MAX_BLUE_SCORE`, checked-add overflow, and an otherwise
representable sum above that maximum. Both failures report
`ScoreOutOfRange(BoundarySealThreshold)` without wrapping or saturation and
produce no `PreparedSync`, component initialization, or component-task launch.
Resync uses the stored DB PP
hash and score. Rebuild uses the normalized node PP hash and score and performs
this check before database replacement. The Genesis branch returns the
constant zero from NodeService's canonical Genesis representation or
StorageService's processing-valid Genesis state without inspecting a redundant
raw node blue score.

Verify Rebuild obtains one normalized current pruning-point block, completes
StorageService's API-read replacement gate, and passes that same
`ValidatedNodeBlock` to `rebuild_from_pruning_point` rather than rediscovering
it or mixing RPC generations. Its successful threshold and returned anchor
populate the same `PreparedSync` used to initialize BlockProcessor. Malformed pruning-point
responses use the shared malformed-input budget; `RpcRequestFailed` and
generation loss retain their session-fault disposition.
Under the
[storage-owned post-start cancellation contract](storage.md#api-read-exclusion-during-database-replacement--settled),
request session and service cancellation immediately before and at every
observable replacement-transaction boundary. Before transaction start,
ordinary cancellation may complete without running replacement. After start,
verify the storage future is never dropped: definite success publishes caches,
opens the gate, returns the anchor, and causes the session startup barrier to
return `Cancelled` without processor initialization; definite failure and
ambiguous outcome complete their owner-defined cleanup and retain their typed
`Failed` disposition. Supervisor must remain responsive while awaiting each
outcome and invoke consuming session shutdown only afterward.

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
6. The upward seal milestone path from BlockProcessor through ProcessingSession to
   Supervisor. It never returns to BlockProcessor as a command, and no Catchup
   path is admitted before ProcessingSession observes it; global Live still requires
   PostSeal.

Verify [phase entry](processing-lifecycle.md#entering-recovery-phases),
[processor-local block overlap](block-processing.md#catchup-filtering-and-overlap),
[processor-local VSPC overlap](vspc-processing.md#catchup-filtering-crossing-and-overlap--settled),
and [global Live admission](processing-lifecycle.md#live-admission)
together. Required cases are:

1. Synchronous, infallible construction of the complete inert component graph;
   installation in the Supervisor's `Active` slot before exact-once `start`;
   and no yield, component task, producer, notification destination, marker,
   milestone, fault, or processing result between installation and the first
   poll of `start`. Cover the asynchronous startup outcomes `Started`,
   `RequireRebuild`, `Failed`, and `Cancelled`. Preparation failure and
   `RequireRebuild` leave the graph inert until consuming shutdown. `Started`
   proves complete initialization and task launch plus acceptance of every
   mode-specific initial local handoff. A failure during final launch or
   handoff joins any tasks already launched and never exposes a partial pump.
   While `start` is pending, verify Supervisor continues handling termination,
   service-generation events, ordered API database-generation forwarding, and
   stronger recovery requirements. Retire the exact RPC generation and the
   exact processing DB generation independently after `new` but before the
   first `start` poll, during preparation, during final task launch and local
   handoff, and immediately around `Started`. At every boundary exercise both
   observable orderings. Loss observed before `Started` must produce the
   operation-level `Failed` outcome carrying
   `FaultKind::ServiceGenerationLost(service)` or the retirement-driven
   `Cancelled` outcome, coalesce both observations, and complete shutdown
   without rebinding. `Started` observed first must use the ordinary
   active-session generation-loss path. Request shutdown at every preparation
   await boundary and verify that cancellation is signalled without dropping
   the startup future, the invoked operation owner completes its required
   outcome and cleanup, and consuming shutdown then joins the session.
   Then cover command priority, the initially closed
   notification gate and immediate closed-gate drop, objective
   lower-bound filtering, both overlap flags, observation only at full-page boundaries,
   and Live entry while valid ordinary orphans remain.
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
  binding and requests at most one session shutdown when an operation fault
  reports the same loss concurrently; a later `RpcPublished(R2)` is retained
  for the next session;
- a generation-preserving `SubscriptionControlFailed` aborts active recovery
  with `Retry`, retains its recovery obligation and RPC generation, uses the
  general backoff, and consumes no malformed-input budget; a rollback or
  unsubscription failure instead races its returned `GenerationLost` against
  the already-enqueued exact `RpcRetired`, and either observation order
  requests at most one session shutdown and cannot reuse the retired generation;
- when a malformed-response retirement arrives before its owner-directed
  fault, session shutdown begins but preserves delivery of the already-produced
  result; the later fault is accepted before the shutdown barrier completes,
  consumes the shared malformed-input budget, preserves any stronger recovery obligation, and
  reaches Fatal on the fourth occurrence;
- `ProcessingDbPublished(G1)` supplies the exact DB client for a new session
  but never rebinds an existing one; `ProcessingDbRetired(G1)` clears only the
  matching Supervisor binding and requests at most one session shutdown when an
  operation fault reports the same loss concurrently; a later
  `ProcessingDbPublished(G2)` is retained for the next session;
- every persistence-fault row in recovery and Live, including Fatal
  `DefiniteFailure`, retained Resync and Rebuild obligations, DB-generation
  retention versus retirement, complete session teardown, and the prohibition
  on reissuing an ambiguous transaction;
- both storage-owned `RebuildSetupFault` reasons aborting a Rebuild attempt
  with `Retry`, retaining its Rebuild obligation and processing generation,
  using the general backoff, and waiting for no replacement processing
  generation; reject either fault outside Rebuild as an invalid lifecycle
  condition;
- replacement initiates consuming session shutdown and ApiService reset without
  awaiting either first; reset releases a marker blocked on the old receiver,
  the old actor tree is fully joined before its successor starts, and the fresh
  producer can wait in Supervisor while service generations are unavailable;
- whole-attempt recovery Retry rather than in-place page/RPC retry;
- the general delay sequence and 30-second cap;
- no second delay while awaiting a replacement service generation;
- the general Retry backoff resets on Live, a new resource generation, and a
  stronger obligation;
- recovery requirements do not consume Retry backoff;
- one shared counter across malformed pruning-point, Catchup sink-sample,
  GetBlock, GetBlocks, and VSPC response kinds and all VSPC reasons, including
  `RemovedChainWithoutAddedPath`, `RemovedSourceMismatch`,
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
- malformed, definitively absent, `RpcRequestFailed`, and generation-lost
  attribution probes retain their distinct dispositions; processor shutdown
  locally cancels a blocked attribution probe without a fault, while an
  RPC-returned `NodeError::Cancelled` during active VSPC processing produces
  the lifecycle-owned Fatal ownership fault; and
- both `BoundedStateExhausted` variants atomically reject the triggering input,
  disable both notification streams, require Resync without weakening Rebuild,
  retire no RPC generation, consume no malformed-input budget, and proceed
  through complete session teardown; and
- after the NodeService-owned GetBlock compatibility adapter has produced its
  typed result, typed fault kinds rather than diagnostics drive policy and
  counters.

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
by asserting that Supervisor changes its unique slot from `Active` to
`Stopping`, both
processors and every descendant release their session-scoped RPC and DB clones,
all actor tasks and channel endpoints are joined or dropped, and only then may
the slot become `Empty` and a successor start. Reuse the same still-valid
service-generation `Arc` values
in a later session and prove that no old message, task, buffer, or actor state
crosses the session boundary.

## Block processing and dependency resolution

Verify [block admission](block-processing.md#admission-and-materialization),
[committed delivery](block-processing.md#committed-block-delivery),
[OrphanManager](block-processing.md#orphanmanager--settled), and
[DependencyResolver](block-processing.md#dependencyresolver--settled) with:

1. `ProcessingSession::new` constructs an inert session marker worker,
   BlockProcessor, OrphanManager, and private DependencyResolver bound to the exact session RPC
   generation before any component task or data sender exists. Successful
   preparation initializes the complete tree before its tasks launch. Shut that
   tree down with handlers and resolver
   work active, then construct another session and verify no queued value,
   pending set, task, channel endpoint, or client clone from the predecessor is
   reachable. Cover preparation failure before task launch and closed child
   command or acknowledgement paths both before and after the startup barrier,
   using the startup result and reliable upward fault path respectively.
2. Reverse orphan topology releases every newly ready child.
3. Resolver pending state remains until `AddOrphan` or `BlockPersisted`.
4. Resolver cancellation and transitive Shutdown are safe with
   full child-result channels, release the resolver's exact RPC-generation
   clone, and propagate resolver control-path faults through OrphanManager.
5. A resolver-confirmed unavailable dependency requires Rebuild, whereas RPC
   or connection failure does not establish unavailability.
6. A failed post-commit `PersistedBlock` delivery cannot be treated as a
   harmless duplicate or omission.
7. A malformed resolver full-block response retires the exact RPC generation:
   during Catchup it follows the shared malformed recovery-input budget, while
   during Live it requires Resync without consuming that budget.
8. An incoming hash already classified as a permanent boundary identity, or a
   strict materialization result containing any identity-only reference,
   reports `MaterialityViolation` and requires Rebuild without orphan or
   resolver admission. When the same result also contains absent hashes, the
   identity-only disposition takes precedence.
9. A strict missing-only result before Catchup reports
   `ReconciliationFailed` and `Require(Resync)`. During Catchup and Live, the
   complete missing set enters ordinary orphan/dependency resolution without a
   partial database write.
10. A second BlockAdded for a hash whose notification source bit is already set
   is discarded before materialization and orphan admission without changing
   overlap or requesting recovery. A synthetic copy followed by the first
   notification still establishes ordinary overlap.
11. Fill orphan storage with distinct hashes, then verify that another distinct
    orphan reports `BoundedStateExhausted(Orphans)` without partial topology,
    reverse-index, or pending-resolution mutation and accepts only terminal
    shutdown. Its resolver child immediately stops admitting work, cancels and
    joins its tasks, and remains stopped through that transitive barrier.
    Re-admitting an existing orphan at capacity
    consumes no new slot and does not report exhaustion.
12. At the focused owner's exact orphan threshold and capacity, verify
    dependency selection begins at the threshold and admission remains valid
    through capacity. Hold resolver RPCs open and prove active work never
    exceeds its focused-owner concurrency bound while cancellation and queued
    work continue to make progress.
13. Natural-arrival and transitive-shutdown cancellation of resolver work emits
    no dependency result or fault. A resolver RPC that instead completes with
    `NodeError::Cancelled` while BlockProcessor remains active produces the
    lifecycle-owned Fatal `Ownership(ManagedComponentUnavailable)` fault.

## VSPC processing

Verify [change semantics](vspc-processing.md#change-semantics--settled),
[pending history](vspc-processing.md#pending-state-and-history--settled),
[readiness](vspc-processing.md#readiness-and-materiality--settled), and
[Catchup crossing](vspc-processing.md#catchup-filtering-crossing-and-overlap--settled)
with:

- construction from anchor-derived state and exact generations before any
  input sender is exposed; after shutdown and reconstruction, no predecessor
  synthetic, notification, persisted-block, pending, or history value is
  observable and the first new-session input is retained;
- removed/added source and destination derivation;
- the added-only path with no database source;
- an actionable reorg resolving `removed` followed by `added` through one
  ordered storage resolution call;
- strict-`<` sink-preserving history pruning before Catchup, at the
  Catchup-to-Live transition, and during Live, with pruning disabled throughout
  Catchup;
- bounded pending order where one unready candidate does not block a later
  actionable candidate;
- pending-destination collision checks precede capacity admission; at capacity,
  an existing collision keeps its notification fault, while a distinct
  synthetic or notification candidate reports
  `BoundedStateExhausted(VspcPending)` without partial index mutation, eviction,
  or coalescing and accepts only terminal shutdown;
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
message tagging, the mutex-protected `AwaitingCatchup -> Open` producer gate,
nonblocking ordinary delivery, a reliable coalescing gap generation even when
the final open-gate ordinary update is lost, and lossless causal
`Catchup` and `Live` markers. Verify all producer clones share the gate;
pre-Catchup block and VSPC offers return `SuppressedAwaitingCatchup` without
entering the channel or advancing the gap; `Catchup` is the first channel value and
opens the gate without a racing ordinary offer; and later `Full` outcomes
advance the gap. Verify ProcessingSession's dedicated marker worker absorbs
Live marker backpressure without blocking either processor or the session
coordinator, preserves Catchup before Live, and permits unrelated open-gate
graph updates to interleave before Live. Resync and Rebuild startup and
PP-boundary sealing enqueue no API marker. Every accepted Catchup trigger first
hands Catchup to the worker without awaiting graph-channel delivery, then sends
both processor commands and activates subscriptions before dispatching its held
page. Verify every commit offered before the atomic marker/open boundary is
covered by the later seed, every offer after it enters the stream, and overlap
is accepted. Delay marker-worker execution after Catchup handoff until both
processors reach Live; verify their completed offers remain seed-covered, then
release the worker and observe ordered Catchup and Live delivery without a
Catchup completion acknowledgement. Global Live admission hands Live to the worker before emitting
`EnteredLive`; that milestone does not acknowledge API delivery. After the gate opens, a causal
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
consumed, its complete level snapshot updates retained external size while
preserving an existing DAA score, and a resulting size change is published as
one atomic revision. Parent snapshots seed complete external level state for
new crossing edges; verify exact size and permit the settled contextual DAA
score. The update must neither expand the view extent nor restore the
below-range block or level as head-view content. An update for a level with no
retained crossing-edge endpoint produces no visible revision, and unreferenced
external endpoint metadata can be discarded.

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
revision `n`. A retained view mutation returns `GraphDelta(n,n+1)`, and the
enclosing publication appends that delta before atomically exposing both values
at revision `n+1`. No capture can observe adjacent revisions. A no-effect
update returns no delta and advances neither revision. History rejects a
nongapless append and rejects every forward interval wider than one revision
with `NonAtomicDelta`. The accepted atomic delta retains its target revision
timestamp without adding a source timestamp to history. Construct
direct and composed deltas only through `GraphDelta::new`; verify it
rejects equal and backward revision endpoints, exposes the accepted endpoints
and target timestamp through read-only accessors, and offers no revision
mutation path. Revision zero has timestamp zero. Direct revisions receive the
publication-local elapsed timestamp sampled for their creation; adjacent
millisecond-quantized revisions may have equal timestamps. Composition adopts
the final constituent's timestamp. Sample immediately below and at a whole
millisecond boundary to verify truncation to elapsed whole milliseconds.
Replace a publication and verify neither server nor Web compares the two
publication-local timestamp domains.

Exercise the publication-owned writer gate and atomic core with captures racing
one retained update. A view, delta-target, or history capture observes either
the complete revision-`n` pair or the complete revision-`n+1` pair, never
adjacent revisions. No second graph mutation or lifecycle-state transition may
pass the gate, and a Stale transition publishes only after the complete
revision-`n+1` pair. Hold captured immutable history-entry Arcs across pruning
and complete composition after releasing the core read lock.

Cancel an update while it awaits writer-gate or core ownership and verify that
both values remain at revision `n`. Once mutation begins, verify that it
publishes the complete revision-`n+1` pair without an await point, pending
mutation state, or spawned completion task. Abandon an unpublished publication
without transitioning it to Stale and prove that no mutation task or retained
publication resource survives its drop.

Verify `GraphHistory::range` checks target ordering before retained-boundary
selection: a target below the start returns `BackwardTarget` and constructs no
delta, while equality remains `UpToDate`. Separately, send the public canonical
delta endpoint a `from_revision_id` greater than its coherently captured Head.
It must return the common `400 invalid-request` response before client
registration validation, cache lookup, or history work, so `BackwardTarget`
cannot arise from that public input.

Verify the graph-owned
[`LevelExtent`](api-graph.md#graph-values-views-revisions-and-history--settled)
accepts inclusive single-level and `[1, u32::MAX]` extents, computes their
depth without overflow, and rejects zero or reversed bounds. Cover inclusive
boundary behavior for level containment, extent containment, overlap, and edge
span intersection. Verify every `GraphView` retains one such extent whose
depth does not exceed its positive configured `max_depth` and that its bound
accessors expose that same value.

Verify `GraphDelta::new` rejects a zero lineage `head_level` with
`ZeroHeadLevel`, preserves its existing non-forward-revision rejection, and
gives revision ordering precedence when both inputs are invalid. Direct
mutation and composition continue to construct positive lineage bounds.

Verify every subview extract inherits its source revision and revision
timestamp with
`TrackingPolicy::Frozen`. `BlockCommitted` and `VspcCommitted` mutation entry
points return `GraphViewUpdateError::Frozen`; delta application returns
`GraphDeltaApplyError::Frozen`. Each refuses before inspecting the input or
changing contents or revision. Keep this distinct from a no-effect update
accepted by `Head` or `Fixed`.

Verify [frozen subview extraction](api-graph.md#frozen-subview-extraction--settled)
from `Head`, `Fixed`, and `Frozen` sources, including nested extraction. Cover
invalid raw extent construction and outside-source extraction; inherited
revision and calculated nominal depth;
complete in-range blocks; crossing edges with zero, one, and two endpoint
blocks; all nominal and endpoint levels with exact state; recomputed derived
counters; and absence of source mutation, delta production, or history.

Level-change cases cover create, update, remove, and no-net-change composition;
application of `after` without validating `before`; a size-only external-level
mutation; and DAA score changes. Verify each delta carries the `head_level` of
the lineage that owns its revision interval and carries no lower coverage
object. Retain all Head deltas whose `head_level` remains in the retained head
window, including multiple revisions at one level, and make them eligible for
pruning only after that level leaves the window. Cover longest-prefix pruning,
derived oldest available revision, every retained revision as an exact range
and cumulative-cost boundary, and unchanged current revision.
No count or byte pressure may prune history earlier.

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

Deterministically overlap materialization of a child whose retained parent
level already exists with a VSPC transaction that changes that parent level's
DAA score. Deliver the two committed outcomes in both possible orders and
verify that the final `GraphView` combines the materialization snapshot's size
with the VSPC snapshot's DAA score and equals the corresponding persisted
fields. Neither order may restore the other transaction's contextual field.
Repeat with the VSPC snapshot arriving while the level is absent and the later
block update first retaining it only as an external endpoint: the VSPC update
has no visible effect, the seeded size is exact, and the contextual DAA score
is accepted. This case must not require cross-lane transaction or delivery
serialization.

Verify [anchored graph windows](api-protocol.md#anchored-graph-windows--settled)
for floor selection and tie break, the sentinel result, a reorg-created
VSPC-empty level, atomic level-score publication, and navigation plus window
consistency from one image. Accept zero and `MAX_DAA_SCORE` as query bounds and
reject the no-VSPC sentinel as a real DAA query. Retain an external endpoint
below Head's nominal extent with a contextual DAA score and verify that
in-memory DAA-floor resolution excludes it; a query below nominal in-memory
coverage uses the consistent database path.

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
entry's `head_level` without changing either fixed bound.

While successive target Heads still contain the complete fixed extent, verify
omitted canonical block and edge removals do not remove fixed objects. Then
supply a batch in which earlier entries remain eligible and a later entry's
target Head has crossed the fixed lower bound. The Web applies the earlier
entries, rejects the first ineligible entry and every later entry, preserves
its last coherent image and applied cursor, and freezes. Also cover publication replacement and expired
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

Replay one atomic wire entry whose block upsert carries earlier
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
`KGI-Head-Revision-Timestamp-Ms` only as one coherent HTTP pair. Calculate
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
or alignment. The completed image must install directly as Live.

Verify the settled
[publication-state contract](api-publication.md#publication-state-and-revision--settled):
initial Synchronizing, direct initial Live, visible `Synchronizing -> Live`,
and terminal Stale transitions leave graph view and history revisions and
timestamps unchanged. Cover the runtime-replacement `reset` barrier without waiting for
construction or installation, cancellation of unpublished work,
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
for graph construction or publication installation. Verify
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
history selection, or storage. Level parameters accept `u32::MAX` and reject
the next integer as outside the graph-level domain.

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
levels `1` and `u32::MAX`, slots `0` and `MAX_LEVEL_SLOT`, and level sizes `1`
and `MAX_LEVEL_SIZE` as JSON numbers. Reject zero levels and sizes, slot
`u16::MAX`, negative values, fractions, and values beyond each bounded domain.
Round-trip the complete public `u64` timestamp and score domains as decimal
strings; preserve ordered parent and merge-set vectors and the Genesis
selected-parent-index absence. A cached delta must not intern a hash
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
selection immediately below, at, and above each configured tier boundary and
derive exactly `20`, `50`, `100`, `150`, `200`, and `250`. Every
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
the selected tier to the requested nominal extent, remove edges with removed
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
three adjacent intervals; later-value, insertion-folding, final `head_level`,
and final target-timestamp selection; and rejection of nongapless composition.
Protocol selection must reject a publication mismatch before history work.

For one body-bearing response, select an ordered gapless
`GraphHistoryEntryList` of atomic one-step entries. Require one
`GraphDeltaEntryDto` per revision, exact adjacent boundaries, preservation of
every target timestamp, and the final entry as the returned cursor. A future
target returns through available history; every retained starting revision is
an exact boundary.

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
cost subtraction before and after prefix pruning and the first retained left
boundary derivation. Verify an exact cumulative value of `u64::MAX` is accepted and that
the next positive estimate panics before changing the revision, cumulative
value, or retained entries. A simultaneous revision mismatch must return
`RevisionMismatch` before cumulative arithmetic and must not panic. A matching
revision with a multi-revision delta must return `NonAtomicDelta` before
cumulative arithmetic and leave the history unchanged.

Drive `GraphPublication` original-update application with an estimate one byte
below `SOFT_DELTA_ESTIMATED_BYTES` and require it to supply `Replayable` so
history retains the atomic entry. At the exact soft limit and above it, require
the publication to supply `ReplayBarrier`; history advances the replay floor
to the target revision, clears retained entries, omits the uncrossable entry,
and preserves normal current-revision and cumulative-cost advancement.
Repeating the case must move the floor only forward. A forward range from below
the floor returns `StartBeforeReplayFloor`; a source exactly at the floor
remains eligible for later retained entries.

Verify dyadic successor selection from zero, unaligned sources, every power of
two through the 128-revision cap, and sources divisible by larger powers. Cover
the exact normalization spans `4..=7`, including `17 -> 24`, followed by
`24 -> 32 -> 64 -> 128`. For `F = 32`, verify the descending boundaries
`64, 48, 40, 36, 34, 33`; for `F = 17`, verify `24, 20, 18`. A moving Live
Head applies the active interval only to descent, while Synchronizing and
Stale can descend through the final atomic revision. Verify every allowed
active interval from 1 through 128 and reject any controller output outside
that set. ApiService must initialize the interval to 4 and retain its current
value across publication reset and replacement; the open controller decision
supplies no further executable policy.

Force additive estimates to reach the soft limit at successive dyadic
boundaries while rebuilding one shared dictionary for each admitted candidate.
Permit descent below both the four-revision base and active interval. Mark a
shortened successful value `size_limited`, reuse it without another enlargement
attempt, and distinguish Head- or state-limited shortening that remains
enlargeable. Prove, at worst-case integer, dictionary-reference, and framing
widths, that every canonical candidate whose estimate is below the soft limit
fits the hard uncompressed limit. A gzip body that transfers efficiently must
not permit a Head snapshot, anchored window, or Head-level lookup to exceed its
hard logical-body limit.

Make one atomic `R -> R + 1` entry reach the soft limit. During application of
that original update, require `GraphPublication` to classify the entry, advance
the history replay floor through `R + 1`, and prune the affected caches without
attempting delta JSON encoding or exposing the new core between those actions.
Physically remove every earlier delta source slot and every below-floor
completed Head tier, and invalidate the affected delta jobs plus all running
Head-tier jobs. Later requests below the floor must perform no encoding work,
while a request or snapshot from exactly `R + 1` remains eligible. Give delta
waiters the fresh-view outcome and make Head-tier waiters re-enter their
selected tier. Repeat with a later uncrossable atomic entry and prove the floor
never moves backward. Inject a deliberately broken estimator whose below-soft
candidate exceeds the hard limit. Require one error-level diagnostic with the
estimate and observed size, followed by successful compression, cache
insertion, and delivery of the complete response. It must not descend, advance
the replay floor, or produce fresh-view recovery. Require the canonical-delta
estimate-versus-actual measurement for admitted candidates and increment the
estimator-invariant-violation measurement exactly once for this shared
construction rather than once per waiter.

Pause delta and Head responses after they acquire completed pre-floor cache
values but before replay-floor admission, then advance the replay floor. Both
must reject those values at their final floor check; the delta request returns
the fresh-view outcome and the Head request re-enters its exact tier. Admit a
response first and prove that later floor advancement does not revoke it. Race
invalidation against job completion in both cache-lock orders and require
exactly one terminal transition. In the invalidation-first order, pause after
slot removal but before terminal-state publication while the invalidator still
holds the cache guard, let completion contend for that guard, then require
`InvalidatedByReplayFloor` to become terminal before completion proceeds.
Encoding success, ordinary failure, and cancellation must not overwrite it.
In the completion-first order, let completion remove its running slot and
install its value before invalidation acquires the guard; require `Complete` to
remain the job result, the completed cache value to be removed, and final floor
admission to reject its public delivery.

For tiered Head snapshot reuse, install a publication with no encoded tier and
require the first request for each applicable tier, including `20`, to start
that exact tier's single-flight job. Exercise only these selected-tier cases:
eligible completed entry, running-job join, and missing-entry construction.
A completed or running larger tier must never supply the response, start the
selected-tier job, or suppress its construction. Concurrent requests joining
one tier share one capture, serialization, and compression result; requests
for distinct missing tiers may start their distinct jobs.

Using publication-local revision timestamps, verify reuse at age 89,999
milliseconds and physical removal at age 90,000. Head advancement alone must
neither remove the entry nor start work. A request for another tier must leave
the obsolete unqueried entry physically present. The first later request for
that entry's own tier removes it atomically and joins or installs exactly one
selected-tier job. An eligible hit starts no refresh. Failure gives all
on-demand waiters the same typed error, removes the job, and permits retry.

While a `Synchronizing` publication remains below the 90-second age boundary,
advance it far enough for ordinary level-scoped history pruning to move
`oldest_available_revision` beyond a completed tier's cursor without advancing
the replay floor. With no matching completed delta source, require the next
exact-tier request to remove that snapshot, reject its matching ETag rather
than return `304`, and construct a newer snapshot whose cursor remains
replayable. Also pause a tier candidate during detached encoding, move the
ordinary-history boundary beyond its cursor, and require the still-pending job
to discard that candidate and complete from one newer shared capture.

Pause a demand-built Head-tier job during detached encoding and advance Head
across the completion-time age boundary. A candidate completing at age 89,999
milliseconds is installed and delivered. At age 90,000 it is neither inserted
nor published as a terminal result: the same job remains pending, every
concurrent waiter stays joined, and a newer capture eventually supplies their
one shared eligible result. Advancing Head after an eligible candidate's
admission must not revoke that installed entry or its already waiting
responses. Repeat the obsolete-candidate case for the publication's first
tier-20 request. For a terminal `Stale` publication, verify that age remains
fixed, an eligible entry remains reusable, and querying an age- or ordinary-
history-obsolete entry removes and rebuilds it from the final Head. Starting
from the obsolete tier cursor, require `StartPruned` fresh-view recovery to
receive that rebuilt snapshot and then reach Head without selecting the old
tier again, both when the publication remains `Synchronizing` and after it
becomes `Stale`.

For canonical Head deltas, verify at most one publication-owned job per source.
Concurrent requests join it even when they selected different targets. A
waiter needing a farther target re-enters after successful completion, starts
or joins the serialized enlargement, and never receives the inadequate shorter
body. Ordinary failure gives that job's waiters the same request-local error
and preserves the preceding completed value.
Pause one caller after its initial source miss, immutable history capture, and
pre-election sizing but before its second cache election. Let another caller
install and complete that source job. When the result is adequate for the
paused caller's fitting target, require the second election to return or
reclassify the same completed value without another job or encoding. When it is
inadequate, require the second election to preserve it and install only the one
required fixed-target enlargement job. In both cases, the paused caller
releases its redundant captured entries unless it becomes the enlargement
builder.
Wrap one cached batch body with different valid request-specific publication
and coherent Head revision/timestamp headers without serializing, compressing,
or decompressing it. Verify that `CachedDelta` retains its boundaries,
`size_limited` state, measured uncompressed JSON byte count, and final gzip body
without duplicating target timestamps, Head levels, or entry count as semantic
fields. Its source slot separately retains the first atomic entry's target
timestamp. Exercise an SSE-coordinated and HTTP-only request from the same
source and verify both use the same source slot.

Complete a short non-size-limited value, advance Head so the dyadic target is
farther while its job is still running, and require the next request to join
that job. After the shorter completion, require the waiter to elect or join the
farther enlargement without receiving the shorter body. Preserve the previous
completed value throughout enlargement and replace it only after successful
farther completion. A response already committed to delivery remains valid.

Make one request-selected target cross the soft estimate and, separately, the
hard uncompressed limit. Before the second cache election, require estimate
descent to determine the farthest fitting target and its `size_limited`
classification. The one slot transition must install only the fitting fixed-
target job; no job or terminal result may exist for an excluded target. The
same source and request-selected target must produce an identical fitting
target and classification for concurrent callers; treat disagreement as an
internal invariant failure rather than reconciling the observations. Allow
different request-selected targets to produce different fitting results. The
injected hard-limit crossing below the soft estimate logs the estimator-
invariant violation, does not change the classification, and still publishes
the complete cached result. When the fitting lower boundary already exists as
the completed value, reuse its bytes and reclassify it without encoding.
Serialization, gzip, cancellation, and unrelated job failures must persist no
size evidence.
Pause a non-size-limited job at the eventual fitting target, then let a farther
request discover that the target is size-limited. Require that request to join
the unchanged job and, after completion, reclassify the same encoded bytes on
re-entry without changing the running target or encoding again.
After a fitting job fails or is cancelled, require the slot to retain no size
evidence. A retry must repeat deterministic estimation before installing only
the fitting target.

For query-time pruning, populate sparse source revisions with nondecreasing and
equal timestamps in one ordered cache. At cache ages 299,999 and 300,000
milliseconds, respectively retain and physically remove the complete expired
prefix before exact lookup, touching no retained suffix. Verify that Head
advancement and idle wall time alone remove nothing, a later query performs the
accumulated pruning, and Stale fixes cache age. A detached job removed with an
expired slot may complete admitted waiters but cannot reinsert. After pruning,
an absent source inside the horizon constructs on demand from atomic history;
an absent source at the boundary returns `DeltaSourceExpired` without
resurrection. Construct a completed source entry, ordinarily prune that source
from `GraphHistory` while its cache entry remains within the five-minute
horizon, and require the retained completed hit to succeed without any history
lookup.

For a Live publication, cover `ReachedHead`, `ContinueImmediately`, and
`WaitForWakeup`; one wakeup
at the armed boundary; suppression of further graph wakeups until rearming;
and `PublicationState` or the replacement sequence bypassing the schedule.
Verify the canonical
`GET /api/v1/graph/deltas` endpoint requires `publication_id` and
`from_revision_id`, accepts an optional `client_id`, rejects a public
`to_revision_id`, and emits the exact publication, publication-state, coherent
Head revision/timestamp, outcome, and continuation headers required by the
protocol. Require a batch body for
`complete` and `prefix`; require an empty `200 OK` body for `up-to-date` and
`wait-for-wakeup`. The wake-boundary header appears exactly when the
continuation is `wakeup`, including on a body-bearing prefix, and is absent for
`head` and `continue`. Neither typed `409 Conflict` delta error carries the
delta success headers. Verify body-bearing results advance the cursor to the final entry's right
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
client and the saturated next registration. Require one serialized registry
operation to collision-check the random key, admit the complete initial
three-message batch, and install the registration. The batch must appear
entirely or not at all. Repeat for publication replacement after removing the
old registration, using the same mailbox, a fresh identifier, and the
replacement Head as the inert sent boundary. Drop the mailbox owner without
cleanup and verify its weak registry entry is removed on the next operation.
No registry or mailbox synchronization guard may cross an `.await`.

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
required items disconnects it. The mailbox's asynchronous readiness mechanism
wakes its single receiver without becoming a second source of message contents
or ordering.

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

Exercise the
[Web-owned fresh-view recovery contract](web.md#update-acquisition--settled)
for every `FreshViewReason` under both Head-following and Fixed policies.
Include a same-publication replay-floor advance, queued unapplied batches,
wakeups during Head snapshot reacquisition, retained-registration rearming,
registration disappearance during recovery, and publication replacement
racing the reload. Assert every owner-defined cancellation, queue, SSE,
snapshot, cursor, reload, and freeze effect without treating a typed fresh-view
outcome as an ordinary retry from the rejected cursor.

Replace a publication without reconnecting its SSE transport and verify the
old ID is invalidated before the same three-message sequence reports the
replacement. A state transition emits only `PublicationStateDto`, does not
replace the ID, and bypasses the graph-wakeup schedule. Entering Stale carries
the final stalled Head revision. Verify `Last-Event-ID` has no graph-cursor,
publication, registration, or replay meaning. Cover saturation rather than
wrap at the maximum revision. Synchronizing never parks for the interval.
Entering Stale reports state independently of graph-wakeup arming and permits
descent through the final stalled Head without rearming.
For HTTP-only `head` and `wakeup`, verify the protocol-owned calculated
`KGI-Delta-Retry-After-Ms` values at representative selected wake boundaries
and its absence
from `continue` and SSE-coordinated responses. At a Stale final Head require
1000 milliseconds, repeated same-endpoint polling while that publication
remains current, and a publication-mismatch outcome followed by a replacement
Head snapshot after replacement. The SSE-coordinated Web must wait for the
replacement wakeup instead.
Change the active interval upward while registrations are armed and retain
their old boundaries. Cover the wake already underway and one further early
alignment wake before new schedules use the larger interval. Change it
downward without rescheduling; at the retained later wake, let the first
response stop below the now-current Head, require its immediate continuation
request, and let the second response converge under the smaller interval. No
interval change may cancel or duplicate a registration.

Derive Live continuation from the nearest eligible `N(T, A)`, not solely from
raw revision distance. Cover an aligned cursor where comparison with `A`
directly selects all three continuations and the normalization exception
`T = 17`, `A = 8`, `H = 24`, which must continue immediately. Make a
body-bearing prefix end in `T < H < N(T, A)` and require it to carry `wakeup`,
the exact `KGI-Delta-Wake-At-Revision`, and no immediate follow-up request.
The empty `wait-for-wakeup` outcome follows the same boundary rule while
leaving both cursors unchanged.

Verify Stale preserves cache entries and permits existing and new jobs to
finish against the stalled Head. Publication replacement must allow a running
job and every attached request to finish against their captured immutable
history entries. Cancelling one request detaches only that waiter. Once the old
publication has no remaining runtime, slot, request, or SSE reference, its
cache and client registry are released; a detached job may complete its
remaining waiters, but its weak cache insertion must then fail harmlessly.
Terminal ApiService shutdown cancels and joins unfinished jobs. Retaining or
pruning a source history boundary must not itself retain or prune a completed
delta; history and response-cache lifetimes remain independent. Cover
source-ordered query-time pruning, source-slot replacement, and
history-owned replay-floor advancement through the focused cases above; no
Head-advance or target-level pruning path may remain. At the five-minute source
boundary, require a fresh window before starting a job. Historical database
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

Verify every atomic in-memory Head delta still contains each canonical block
and edge removal immediately before serialization, while its cached wire body
omits those removal entries and their now-unused dictionary hashes. Advance a
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
outside-PP sentinel edge; complete canonical direct-parent and merge-set hashes;
local `selected_parent_index`; Genesis without ORIGIN; current color and VSPC
state; and both construction-only alignment values. Verify that live
`BlockCommitted` projection and database reconstruction expose the same unique
child-parent relation set despite their permitted local vector ordering, create
one `GraphEdge` per relation, and count each retained edge once in
`usage_count`. Verify that the ID cut is the
maximum over the complete materialized block table, including an ID whose
block is outside the projected window. Recompute derived level usage from
edges. Exercise the separate capped API pool and prove its saturation or
generation loss cannot consume or retire a processing-pool connection.

Lifecycle cases cover ApiService `AwaitReset`, construction of a fresh private
runtime in `AwaitingCatchup`, waiting for the first-channel Catchup marker while
producer-side suppression remains active through processing PreSeal and
PostSeal,
`Constructing -> Aligning -> Active`, and the prohibition on installation
before alignment completes. Alignment covers block IDs at and below
the snapshot cut, a first greater ID with no retained effect, a first greater ID
that returns a delta, and the matching VSPC source. Exercise both crossing
orders. After one cut crosses, later updates from that source apply while the
other source continues its snapshot-relative filtering. Empty staging and
either single-cut state remain Aligning and publish no candidate, including an
indefinitely idle second source. Only after both cuts cross, apply all later
interleaved updates through a captured alignment frontier, require the recorded
gap generation still to be current, assign the publication identity, and
synchronously request installation without an intervening await or graph
receive. `Installed` enters Active; `Superseded` enters Quiescent. Updates queued
after the frontier apply through the Active path. Advance the gap generation
before the final fence and verify the candidate is abandoned for reconstruction
without an installation attempt. Verify the newly installed
publication initially has no encoded Head tier, its first applicable request
starts one shared tier job, and concurrent requests join it. Tier capture,
size, serialization, or compression failure must remain request-local and
must not change publication state or current-publication installation. The
first successful client catches up from the cached cursor through retained
ordinary deltas. Cover first publication revisions zero and above zero, plus
jointly atomic view and history visibility at every publication revision.

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

Pre-Catchup suppression produces no gap. Verify no database seed starts before
Catchup. The seed started after the marker covers every intentionally
suppressed commit, including block and VSPC commits racing with the marker
transition; an offer ordered after the transition enters the channel instead.
No second marker is required.

Exercise the runtime's universal intra-session reconstruction primitive from
`Constructing`, `Aligning`, and `Active`. The first two cancel any seed attempt
and abandon their candidate; `Active` first becomes terminally Stale. In every
case,
drain through the first observed channel Empty, discard graph mutations,
preserve sticky Live, capture the newest gap generation, and start a newer
seed. Updates after the empty frontier must be staged, while a later
gap-generation change restarts reconstruction again.
Cover staging overflow, query/generation/projection failure, alignment or
application failure, and a gap during each state. None may request processing
recovery. `Catchup` is not repeated during same-session reconstruction.
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
Exercise repeated publication and stale or repeated retirement against the
[API database generation binding](api-service.md#api-database-generation-binding--settled)
contract, including observation through the watch receiver.

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

Exercise API projection relationship handling through the accepted Rebuild
and ordinary materialization paths, and verify that the returned direct-parent
and merge-set values preserve the canonical persisted relationships. Per the
[Storage projection-read contract](storage.md#api-graph-projection-reads--settled),
projection verification does not require direct SQL mutation that bypasses
those paths in order to prove duplicate, own-hash, or Genesis-relationship
detection a second time. Continue to exercise read-local failures that prevent
resolution of a complete seed as `InconsistentProjection`.

For Rebuild, verify `reset(Rebuild)` atomically clears the locally bound API
client and disables public reads without changing the old client's
storage-owned validity. StorageService then closes replacement admission,
retires the old client, emits `ApiDbRetired(G1)`, creates a fresh `G2`, and
emits `ApiDbPublished(G2)` before mutating processing contents. Supervisor
forwards both events in order while Rebuild is running. `Published(G2)` makes
`G2` available to construction while public reads remain disabled, but its seed
read opens no transaction before definite commit opens the replacement gate.
The commit emits no second publication event. Replacement graph-publication
installation after alignment enables public reads. If the current
generation is lost after the seed has detached but before installation,
alignment may finish, installation still enables the gate, and
database-backed requests remain `503` until a later `Published(G3)`. Repeated
retirement/publication delivery is idempotent, and
neither generation turnover nor an absent current client invalidates an
otherwise advancing Active graph publication.

Verify [Reset and recovery-time availability](api-service.md#reset-and-recovery-time-availability--settled)
with ordinary Resync and Rebuild integration scenarios. Verify the fresh
`GraphUpdateReceiver` and exact recovery mode. From `AwaitingCatchup`, `Constructing`,
`Aligning`, and `Active`, start the fresh runtime before
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
Rebuild calls `reset` again with a fresh ingress. Rebuild reset locally unbinds
the old API client and disables public request admission; StorageService owns
closing database admission, conditionally retiring and draining an existing
old generation, and publishing the fresh gated generation. Verify the ordered
`ApiDbRetired(old), ApiDbPublished(fresh)` sequence when an old generation
exists and the sole `ApiDbPublished(fresh)` event when none exists. Supervisor
must forward exactly the emitted sequence without synthesizing the absent
retirement event.
Start an API database phase through `G1`, retire `G1` so no current API
generation remains, and begin Rebuild before that phase releases its
service-owned shared lease. Verify Rebuild emits no second retirement event,
publishes gated `G2`, cancels and boundedly drains the admitted `G1` phase, and
cannot acquire exclusive replacement access until its lease is released. Make
the lease release before `API_REPLACEMENT_DRAIN_TIMEOUT` and verify replacement
continues. Hold it through that timeout and require
`RebuildSetupFailed(ApiReadExclusion)` plus retirement of `G2`; absence of a
current generation must not make the drain vacuous.
Verify a detached old projection may complete delivery while
an undetached request returns 503, public reads reopen only after the
replacement completes alignment and becomes `Active`, and
`PpBoundarySealed` has no API generation role.
Cover the replacement `Published` event arriving while the Rebuild transaction
is still running, the runtime remaining `AwaitingCatchup` after that event, and
Catchup arriving before that event with construction waiting for the API
generation and replacement gate. A final definite replacement failure retires the gated generation
without making it readable; an ambiguous outcome retires both API and
processing generations. Cover fresh-pool validation failure as
`RebuildSetupFailed(ApiGenerationPreparation)` without publishing an
unvalidated client, and bounded old-generation drain failure as
`RebuildSetupFailed(ApiReadExclusion)` with retirement of the still-gated fresh
API generation. Neither starts the replacement transaction, mutates processing
contents or caches, or retires the processing generation. Cover setup
cancellation as ordinary cancellation without a fault. Break replacement
control before fresh API publication, after gated fresh API publication, and
while a phase admitted by a retired generation still holds a lease. Each
unexpected case returns
`RebuildFromPruningPointError::ReplacementControlUnavailable`, starts no replacement
transaction, retires the current API generation if one exists, leaves the
processing generation and persistent state unchanged, and reports
`Ownership(ManagedComponentUnavailable)` from StorageService with Fatal
disposition. Verify active database-phase cancellation is best effort and does
not delay the fault indefinitely, while the resulting global shutdown performs
ordinary bounded resource cleanup. No in-flight request
rebinds to a newly published storage generation,
and no request observes a partial or mixed generation, including with
PostgreSQL `TRUNCATE`. Verify ordinary sender teardown may close the installed
receiver. The runtime then becomes quiescent, does not reconstruct or change
publication state, and remains owned until its later `shutdown()`. A marker
worker with an accepted delivery retains no RPC or DB client. During session
replacement, run ProcessingSession shutdown and ApiService reset concurrently;
receiver replacement releases a blocked marker, ProcessingSession joins the
worker, and the session barrier completes before its successor starts.

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
waiter, serialized shorter and farther targets, shared success and shared
request-local failure, exact job removal, and weak insertion both
before and after the origin cache is dropped. Verify that one short cache
critical section elects exactly one builder per source after required pre-cache
checks and the immutable history/first-revision-timestamp capture. Concurrent
callers may duplicate that cheap capture but must join the elected source job
before encoding. No cache-state guard may survive into capture, extraction, an
`.await`, encoding, delivery, or waiter suspension. On success, require
completed insertion before the shared `watch` result; on ordinary
failure, require exact-job removal before the shared error so an immediate
retry starts new work. A cancelled receiver must not alter the job, and a
dropped origin cache must prevent insertion without preventing receiver
completion. Exercise completion after every waiter has detached and require
the one-way `Pending` transition to retain the terminal job value without
restoring a cache slot.

Exercise `DeltaCache` as one source-revision-ordered value under the enclosing
cache critical section. Exact lookup, running-job election, completed
replacement, serialized enlargement, completed-value reclassification,
expired-prefix pruning, and history-floor invalidation must
preserve one coherent slot map without storing an independent floor. Prove
that equal first-revision timestamps do not break prefix pruning, no excluded
target job can be installed, and no destination-heat or target-level side
index remains.

Verify all cache orchestration tasks are registered in the ApiService task
tracker. Include every demand-triggered tier-keyed Head snapshot job in the
same tracked cache and encoding infrastructure. For encoding, reserve
bounded historical capacity before database work,
release every database resource before submitting the detached projection,
and enforce the settled total and historical concurrency limits. Verify the
[process execution model](overview.md#process-execution-model--settled) and
[ApiService encoding scheduler](api-service.md#cache-and-encoding-jobs) with a
composition-root runtime assertion, a production-adapter assertion for
`spawn_blocking`, and an instrumented blocking executor.
Hold every active encode at a test barrier: additional jobs remain in the
scheduler queue while async status, SSE, cancellation, and processing work
remain pollable, and no database resource or component-state lock is held by a
blocked encode.
Scheduler shutdown rejects new work, cancels queued work, lets already active
blocking serialization or compression finish while discarding its output, and
joins the scheduler without a publication lock or database resource held.

Verify the [ApiService shutdown barrier](api-service.md#apiservice-shutdown--settled)
from `AwaitReset`, `AwaitingCatchup`, `Constructing`, `Aligning`, and `Active`,
including races with `reset`, marker delivery, gap reconstruction,
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
Hold one owned resource barrier open after ApiService has entered its terminal
shutdown state, cancel that shutdown waiter, and verify that a concurrent
`reset` is rejected with `ControlUnavailable` and releases its supplied graph
receiver. A later `shutdown` call must resume and complete the retained barrier
without leaking a runtime, task, publication, or database resource.
Supervisor signals Axum graceful shutdown, then starts ApiService shutdown. A
pending processing startup is cancelled and polled to its outcome while the
slot remains `Active`; only afterward does Supervisor move that session to
`Stopping` and start consuming shutdown. An `Active` session whose startup has
completed moves directly to `Stopping`; an existing `Stopping` future resumes;
and an `Empty` slot needs no processing barrier. Startup, processing shutdown,
API shutdown, and Axum completion are polled concurrently wherever they
coexist.
The listener accepts no new connection, existing connections remain
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
expose or consult persisted `DatabaseBinding.genesis_hash`.

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

Exercise `GET` and `HEAD` with absent, nonmatching, and matching validators.
Each `HEAD` response has the corresponding `GET` status and headers without a
body. A malformed `If-None-Match` on either supported method returns bodyless
`400` with `Cache-Control: no-store` and no content type, content encoding, or
ETag, without serializing or hashing the configuration. Every unsupported
method returns bodyless `405` with `Allow: GET, HEAD`, the `no-store` cache
directive, and none of those representation headers. Combine an unsupported
method with a malformed validator and require `405` to prove method-first
precedence.

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
that many waiters on one source job still produce one encode.
Track the transient encoding buffers, completed uncached delivery bodies, and
cache-owned encoded bytes independently while exercising these paths.

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
