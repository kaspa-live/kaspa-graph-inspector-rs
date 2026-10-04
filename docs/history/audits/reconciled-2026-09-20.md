# KGI v2 documentation corrections after the post-H1 review

This is the actionable list for the next documentation update. It compares
`docs/chats/initial_chat_post_handoff-2026-09-17.md` with the current repository
and incorporates later Architecture decisions. Line references below are to
that post-H1 transcript. The pre-H1 transcript is outside this pass. This
report changes no normative architecture. An **Open** item must be resolved
before implementation.

1. **Rolling sink Catchup trigger — Settled and closed.**
   Capture a sink marker at scan start and track it as
   `Unknown -> Present -> Removed` through the synthetic VSPC cursor and
   ordered responses. Cursor equality or occurrence in `added` establishes
   `Present`.
   Hold a GetBlocks page containing a `Present` marker, refresh the RPC sink,
   and enter Catchup before dispatch only when checked DAA-score distance is
   at most `max(30 * network_bps, mergeset_size_limit + 1)`. A larger gap
   rolls the marker forward and remains in Resync; removal, unknown VSPC
   membership, or a decreasing score cannot authorize the primary path. The
   normalized page's global maximum not being final, normalized length `< 3`,
   and an empty VSPC page remain independent fallbacks. The rejected local
   order-decrease trigger is registered in `docs/decisions/README.md`. No
   separate mixed-view recovery protocol is required: a material omission
   exposed by strict pre-Catchup processing uses the existing
   `Require(Resync)` path.
   **Update:** handoff §§11, 19; `processing-lifecycle.md` Catchup trigger;
   `decisions/README.md`; `open-questions.md`.
   **Source:** original exchange 5234–5252; post-H1 1498–1512; subsequent
   Architecture review of pinned rusty-kaspa GetBlocks snapshots and accepted
   rolling-marker decision.

2. **Boundary seal gates Catchup and Live — Settled.** Rebuild starts
   `PreSeal` and must commit the first qualifying block under strict policy
   and emit `PpBoundarySealed` before Catchup. A Catchup trigger before that
   milestone fails the recovery attempt. Live eligibility includes
   `PostSeal`, both overlap flags, and a fully dispatched GetBlocks page;
   the flags alone cannot authorize Live in `PreSeal`. Resync starts
   `PostSeal` only after successful reconciliation. **Update:** handoff
   §§7, 10–11, 19; `processing-lifecycle.md` Catchup/Live;
   `block-processing.md` phase rules. **Source:** post-H1 1474–1514,
   1578–1587, 1648, 5822–5885.

3. **VSPC V2 request and admitted change — Settled; upstream edge check
   Open.** Call `GetVirtualChainFromBlockV2` with
   `min_confirmation_count = Some(0)` (or `None`) and
   `data_verbosity_level = Some(RpcDataVerbosityLevel::None)`. Omitted
   verbosity defaults to `Full`; positive confirmations can remove the
   head from `added`. Pin and verify the chosen rusty-kaspa revision's
   acceptance-data length and incremental cursor behavior with these
   arguments. An empty V2 page is a pump hint, never an admitted
   `VspcChange`. Every admitted nonempty change has its destination at
   `added.last()`. A removed-only change is an unsupported invariant
   violation with a typed fault, not a reason to invent a fallback
   destination; its upstream reachability remains to verify. The wholly
   empty change case remains deferred. **Update:** handoff §§4, 9–10, 19;
   `vspc-processing.md` input; `processing-lifecycle.md` V2 pump;
   `open-questions.md` upstream verification. **Source:** post-H1
   2076–2164; later Architecture decision on exact RPC arguments.

4. **VSPC Catchup continuity and pending work — Settled.** Classify a
   resolved notification against committed synthetic sink `C` in order.
   First discard destination `D <= C` under the lower-bound/history rules;
   `D == C` may give eligible overlap credit but never creates an empty
   normalized change. For `D > C`, normalize a crossing only when `C` occurs
   in `added`: discard `removed` and the prefix through `C`, then apply the
   necessarily nonempty added-only suffix. Otherwise the original change is
   actionable only when its resolved source equals `C`. Neither an order
   comparison nor a sink in `removed` proves continuity. Keep an ordered
   synthetic FIFO, raw unresolved notifications by destination hash, and
   resolved candidates by destination order under one shared bound. Resolve
   both actionable endpoints before readiness; a pending old notification
   must not block a later actionable one or earn overlap credit merely from
   its destination hash. Distinct incompatible candidates require Resync.
   **Update:**
   handoff §§9, 11; `vspc-processing.md` normalization/pending/overlap;
   `processing-lifecycle.md` VSPC overlap. **Source:** post-H1
   3239–3421, 3423–3697.

5. **Database startup, ownership, and schema evolution — Settled;
   destructive administration Deferred.** Distinguish **Uninitialized** (no
   recognized KGI schema/binding), **Empty** (valid v2 schema and exact CLI
   network binding, but no PP/processing data), **Initialized**, and partial
   or incompatible layouts. `--initialize-db` is safe in persistent
   unattended startup configuration: initialize only Uninitialized; open
   compatible Empty/Initialized unchanged; reject network mismatch, v1,
   unknown, and partial schemas. Do not silently rebind an existing DB.
   Without that flag, first initialization needs interactive confirmation;
   noninteractive startup fails with an actionable confirmation error.
   `--clear-db` requests processing-data Rebuild under the compatible
   network. Empty processing state has no PP/sink for Resync, so
   reconciliation requests a distinct Rebuild run. A structurally valid
   schema with inconsistent processing contents also requires Rebuild,
   but is not classified as Empty; a partial schema is rejected. Storage
   holds a dedicated PostgreSQL session
   advisory lock before initialization, migration, or validation; losing
   that lock retires the validated DB client/session. Supported older v2
   schemas migrate forward transactionally under the lock;
   newer/unsupported schemas are rejected. Do not add a persistent
   destructive `--reinitialize-db --yes` startup flag; an explicit
   administrative reset remains a separate design. **Update:** handoff
   §§3, 5, 10; `storage.md` bootstrap/validation/migrations;
   `processing-lifecycle.md` first-run transition; CLI docs when added.
   **Source:** post-H1 5065–5218, 5284–5396, 5600–5627; later
   Architecture decision deferring destructive administration.

6. **Genesis bootstrap and Web marker — Settled.** Keep
   `blocks.selected_parent_id` non-null. PP bootstrap interns synthetic
   ORIGIN as a permanent outside-PP-boundary identity when it is the
   selected parent, including Genesis and a pruned PP with that selected
   parent. The bootstrap path is exempt from the ordinary rule that selected
   parent is a direct parent; ordinary `PersistedBlock` remains non-Genesis
   with a required selected parent. Web identifies a *present* Genesis by
   **zero actual direct parents**, regardless of how many parent edges are
   visible in the current graph window. The API must preserve direct-parent
   presence even for outside-boundary references. No persisted genesis-hash
   field or endpoint is required for this marker. **Update:** handoff
   §§6–7, 10, 13, 17; `storage.md` PP bootstrap/schema;
   `block-processing.md` PP boundary; API/Web response contract.
   **Source:** post-H1 5500–5550; later Architecture Genesis/Web decisions.

7. **Lifecycle ownership and reliable fault events — Settled.**
   NodeService, StorageService, and ResyncEngine own their own states;
   Supervisor owns orchestration and retains a run's first causal fault.
   Status is a lossy, eventually consistent observation, while
   fault/milestone events are reliable. Unexpected permanent-worker exit,
   panic, command-channel closure, or invalid forward command is a typed
   ownership/session or fatal fault, not ordinary DAG discontinuity. A
   dropped barrier-ack receiver does not undo worker teardown. Before
   `Start`, Supervisor must recheck that the *same acquired* RPC and DB
   clients are still published Ready, the engine is Idle, and desired
   recovery has not changed. Publish `EnteredLive` only after the
   synthetic pump is stopped/joined and both Live commands are enqueued; it
   says nothing about empty worker queues. **Update:** handoff §§1, 3,
   11–12; `overview.md` state ownership; `processing-lifecycle.md`
   event/fault and Start/Live contracts. **Source:** post-H1 334–386,
   390–550, 978–1002, 1626–1648.

8. **Resolver and VSPC reorg lookup — Settled.**
   `OrphanManager.resolution_pending` remains set after a
   successful resolver RPC until `AddOrphan` or `BlockPersisted` accounts
   for the block; RPC completion alone must not create a request gap.
   Deactivation drains child results while joining children so bounded
   channels cannot deadlock. For an actionable VSPC reorg,
   `ValidatedDbClient::resolve_materialized_ids` resolves `removed` then
   `added` in one ordered, cache-first batch: repeat inputs yield repeated
   IDs, one SQL read at most, no writes, interning, or promotion, and only
   materialized IDs succeed. Added-only changes use history and make no DB
   lookup. Missing versus identity-only members remain distinguishable
   typed errors. **Update:** handoff §§8–9, 12; `block-processing.md`
   resolver/pending/deactivation; `vspc-processing.md` reorg resolution;
   `storage.md` read API. **Source:** post-H1 3952–4166, 4204–4340,
   4435–4459.

9. **Direct Rebuild from child faults — Settled.** VspcProcessor requests
   `Require(Rebuild)` when an actionable VSPC change names a
   nonmaterialized hash directly in `added` or `removed`. A
   resolver-confirmed dependency unavailable from the node also requests
   `Require(Rebuild)`. In either case the DB contents can no longer be
   trusted against node state; these are explicit exceptions to the
   earlier broad statement that only failed ResyncEngine reconciliation
   requests Rebuild. A missing identity-only merge-set member outside the
   retained PP boundary is not this fault and remains ignorable for
   coloring. RPC/connection failure is not confirmation that a dependency
   is unavailable. A full bounded resolver work channel still requests
   Resync; a closed one is a session fault. **Update:** handoff §§3, 8–10;
   `processing-lifecycle.md` fault taxonomy; `block-processing.md`
   resolver; `vspc-processing.md` materiality; the “Conflicting Rebuild
   dispositions” row in `pre-handoff-2026-09-20-reconciliation.md`.
   **Source:** post-H1 4115–4170, 4379–4501; subsequent explicit
   Architecture decision. This supersedes the broad post-H1 wording at
   1658–1789 and the later mistaken recollection that neither direct
   Rebuild case had been discussed.

10. **Subscription enable ordering and activation drops — Settled;
    replay coverage to verify.**
    After both Catchup commands are queued, keep `NotificationRouter`
    Disabled while starting BlockAdded and VirtualChainChanged remotely.
    Enable local routing and publish `ValidatedRpcClient`'s subscription
    state as Enabled only after **both** remote starts succeed. On partial
    failure, stop any started subscription and retire the client if
    rollback is uncertain. This later Architecture decision supersedes the
    post-H1 proposal to enable local routing before `start_notify`.
    Callbacks received while the router is Disabled during activation are
    **intentionally dropped**; they do not count toward Catchup overlap and
    do not themselves request recovery. The synthetic GetBlocks/VSPC pumps
    continue until the usual overlap criteria authorize Live. Verify that
    this replay/overlap path covers activation-time drops, including a
    BlockAdded event outside the currently selected past; do not claim a
    no-gap guarantee without that evidence. Disabling remains
    an immediate local cutoff, not a transport quiescence fence.
    **Update:** handoff §§4, 10–11, 19; `node-service.md` enable sequence;
    `processing-lifecycle.md` Catchup subscription transition/coverage.
    **Source:** post-H1 4511–4675; subsequent explicit Architecture
    decisions on enable order and intentional activation drops.

11. **Focused storage/API wording — Settled.** `storage.md` still omits
    `levels.daa_score`: new levels use `i64::MAX` for no VSPC member, and
    the VSPC transaction updates removed and added levels to their final
    score. At most one current VSPC block occupies a level; a reorg may
    leave a VSPC-empty level. Its public DAA floor lookup selects greatest
    score `<= q`, then highest level on a tie, as handoff §§6, 16 already
    specify. Also resolve handoff §5's stale deferral of the node-server
    version relay: §16 already settles exposure of the **currently
    validated** version via API status/info without making persistence a
    prerequisite. Persistence of last-known observational node data is
    not part of the accepted v2 correctness contract. **Update:**
    `storage.md` levels/VSPC transaction; handoff §5 status wording.
    **Source:** post-H1 8847–8927, 9103–9189, 9810–9819.

12. **API load isolation and evidence — Settled.** Status/info is a
    separate memory-only admission lane so saturated graph work cannot
    prevent the Web tier from observing service state. Head delivery and
    historical DB reads have distinct budgets; all API paths reject or
    degrade within bounds while processing retains priority. Include v2
    metrics for per-endpoint requests/latency/bytes, SSE active/rejected
    clients, cache hits/misses/evictions, DB permit/query time, delta resets,
    slow-client disconnects, and processing commit latency. Load testing
    must show that API traffic up to configured rejection limits does not
    materially increase BlockProcessor or VspcProcessor commit latency.
    Traffic percentages and a numeric SSE cap are sizing hypotheses, not
    fixed architecture. **Update:** handoff §§13, 18–19; API status and
    operational/verification documents when created. **Source:** post-H1
    6115–6235, 6280–6380, 7870–7924.
