# Web architecture

## Scope and ownership

This document owns browser-side graph state, update behavior, view focus, and
public block identity. [The API protocol](api-protocol.md) owns the graph wire
contract, snapshots, deltas, SSE cursor delivery, and response-local hash
dictionaries.

## Delivery and runtime configuration — settled

The browser uses the same origin as KGI and calls the fixed relative
`/api/v1` surface. It derives the current site from `window.location.origin`
and has no compiled API-address or site-address setting.

The top crate's
[runtime Web configuration](overview.md#http-composition-and-runtime-web-configuration--settled)
may supply an external block-explorer URL template. The Web fetches that
configuration alongside its initial status and graph work without delaying
graph rendering. Until a valid optional template is available, or when its
value is absent, the external explorer action is hidden; graph behavior is
unchanged.

For the protocol-owned JSON representation, the Web converts public `u64`
decimal strings to `bigint` at its DTO boundary before constructing typed
client values. Graph logic never routes those values through JavaScript
`number`.

## Update acquisition — settled

The v1 Web's fast repeated polling is replaced by SSE cursor wakeups and HTTP
delta/snapshot catch-up for cached views. The Web keeps one in-flight catch-up
loop and coalesces desired cursors. Because SSE reconnection is not
exactly-once, each wakeup is a desired `(publication_id, revision)` cursor. The
paired `KGI-Publication-Id` and `KGI-Publication-State` headers describe the
publication context of HTTP DAG responses; graph state also arrives through
the dedicated SSE state message and never through a graph body.

Head-following stays prompt. On publication change, a head-following view
automatically reloads.

The Web adopts each API-owned `PublicationState` without requesting or
applying a graph delta merely because state changed. On `Stale`, it keeps the
coherent image visibly marked stale. If its cursor is behind the state
message's final Head revision, a Head-following or eligible fixed view may
first catch up through that publication's final stalled Head. It reloads or
freezes, according to its view policy, when a replacement publication ID
appears; until then it does not pretend that the old image remains Live.

A delta response may end at an intermediate revision below the Web's desired
cursor. Its body contains an ordered, gapless batch of retained history entries
and one response-wide hash dictionary. The Web validates the complete batch
before admission, including that its first boundary equals
`received_revision_id`, advances its received cursor to the final entry boundary,
and replays entries one at a time. It never assumes one response reaches the
original target.

Define the initial client replay bound:

```text
MAX_DELTA_REPLAY_BATCHES = 10
```

The replay state distinguishes:

```text
applied_revision_id / applied_revision_timestamp_us
    last entry already installed in the visible graph

received_revision_id
    final cursor of every admitted batch

observed_head_revision_id / observed_head_timestamp_us
    newest coherent Head pair received in delta-response headers

replay_deadline
    optional client-clock time by which the admitted queue should be applied

next_replay_at
    optional client-clock time for the next entry
```

A Head snapshot or Head-backed window initializes the applied and received
revision from its body and initializes the applied timestamp from that
revision's timestamp. The Web never compares timestamps from different
publication IDs.

At most one delta request is in flight. It always starts at
`received_revision_id`, while `applied_revision_id` continues to describe the
visible graph. A valid gapless response is admitted in full even if its arrival
fills the tenth batch slot; the Web never rejects it only to request the same
source-keyed cache entry again. Once ten batches are held, it starts no further
delta request. A partially replayed batch still counts. When the oldest batch
finishes and capacity becomes available, acquisition resumes from
`received_revision_id` if the latest continuation, coalesced SSE demand, or
observed desired cursor requires more graph progress. Each batch retains its
shared dictionary and remaining DTO storage until its final entry is consumed.

For a newly arrived batch with first timestamp `A` and last timestamp `B`, set
or replace:

```text
replay_deadline = client_now + (B - A)
```

Then cancel and recalculate any pending replay timer against the complete
admitted entry queue. When the queue is empty, both `replay_deadline` and
`next_replay_at` are absent and the applied and received cursors are equal.
When the queue is nonempty and `next_replay_at` is absent, apply its first entry
immediately. After applying entry `D`, let `P` be the next entry, `L` the final
queued entry, and calculate:

```text
SI = L.revision_timestamp_us - D.revision_timestamp_us
SC = replay_deadline - client_now

next_replay_at = client_now
    + (P.revision_timestamp_us - D.revision_timestamp_us) * SC / SI
```

Server timestamp subtraction is nonnegative within one validated publication
lineage. `SC` is the signed remaining client duration; if `SI == 0`, `SC <= 0`,
or quantization produces a zero delay, replay the next entry immediately. The
calculation uses the complete remaining queue, so one
batch replays at its observed server pace while batches arriving faster than
rendering move the deadline and accelerate catch-up. Applying one entry is
atomic from the browser graph's perspective.

Each successful delta response also supplies a coherent
`KGI-Head-Revision-Id` and `KGI-Head-Revision-Timestamp-Us` pair. The Web never
combines an SSE revision with an older HTTP timestamp. Its observable lag is:

```text
applied_revision_lag = observed_head_revision_id - applied_revision_id
applied_time_lag_us  = observed_head_timestamp_us
                     - applied_revision_timestamp_us
```

These values describe the visible graph rather than the newest downloaded
entry. They never influence graph correctness. The Web also observes admitted
batch and entry counts, queue occupancy, replay acceleration and missed
deadlines, plus Fixed-view level-lookup count and latency. Exact metric names
and export mechanics remain deferred.

For canonical Head-delta catch-up, the Web supplies its current opaque SSE
`client_id` and retains the newest response continuation while replay proceeds.
Each coordinated request captures the identifier with which it was issued.
`ContinueImmediately` requests again from `received_revision_id` as soon as
queue capacity permits. `ReachedHead` and `WaitForWakeup` wait for the next SSE
wakeup before requesting again; once it arrives, acquisition may proceed from
`received_revision_id` while earlier entries still replay whenever queue
capacity permits. An HTTP-only client follows the protocol-owned retry delay
under the same capacity rule. SSE wakeups received while acquisition is paused
coalesce to the newest desired cursor. A no-delta wait response leaves both
graph cursors unchanged.

Transport or request failure does not discard a valid admitted queue. The Web
retries from `received_revision_id` when its ordinary transport policy permits.
A malformed, nongapless, wrong-publication, or otherwise invalid batch is never
partially admitted and requires the applicable fresh-view recovery. Publication
replacement, explicit refresh, fixed-view destruction, and Web shutdown cancel
in-flight acquisition and replay timers and release every queued batch. A
replacement snapshot establishes the new publication's timestamp domain.

Adopting a fresh `client_id` for the same publication cancels an in-flight
delta request issued with the previous identifier but preserves every admitted
batch, both graph cursors, and replay timing. Acquisition retries from the
unchanged `received_revision_id` with the fresh identifier when queue capacity
and the newest desired cursor permit. A response associated with an identifier
that is no longer current is ignored in full, including its graph body,
continuation, and Head headers, even when transport cancellation lost the race
with response delivery.

The Web adopts each delta response's publication-context header pair. At a
Stale publication's final Head it replays every admitted entry and waits for the
replacement wakeup instead of polling that terminal cursor. The Web adopts each
dedicated `ClientRegistration`, then the API-owned `PublicationState`, before
processing the following `PublicationWakeup`. This replaces its opaque
identifier on initial connection, reconnection, and publication replacement
without embedding registration or publication state in the graph wakeup. Every
HTTP request continues to supply the Web's actual `received_revision_id` as its
authoritative acquisition cursor. `ClientRegistrationRequired` reconnects SSE
and retries from that unchanged cursor when the failed request identifier is
still current. If a newer identifier has already been adopted, it retries with
that identifier without reconnecting again.

Under the API-owned
[cached Head-delta wire contract](api-protocol.md#canonical-head-delta-responses--settled),
a Head-following view advances its local bounds from each replay entry's
`high_level` and its window depth, removes blocks below the resulting
`low_level`, and
removes an edge when its child leaves the window. A fixed view instead follows
the filtering contract below. These are browser reactions to the API contract
rather than independent wire rules.

The Web consumes serialized head, delta, and graph-window responses. It never
receives an internal `GraphView`, observes `TrackingPolicy`, or applies updates
to an ApiService subview. Internal subview extraction belongs exclusively to
the [API graph contract](api-graph.md#frozen-subview-extraction--settled).
Serialized graph-window contents follow the API-owned extraction contract.

## Head snapshots — settled

The Web requests the canonical Head endpoint with its presentation
`target_depth`. It retains that value locally; the response body does not echo
it. The API can return any cached tier whose nominal depth covers the target.
The Web trims a larger snapshot to exactly its presentation depth before
constructing the visible image.

For response `high_level = H` and target depth `D`, the desired nominal lower
bound is `max(1, H - D + 1)`. Retain blocks within that nominal extent. Remove
an edge when trimming removes its child; retain the parent endpoint and its
level for every retained edge, including an endpoint below the nominal lower
bound. Retain every nominal level and every referenced endpoint level, and
derive local edge-usage counters from the retained edges. These are the same
browser graph rules used when a moving Head later removes its lowest child
level.

Because the selected cache tier is never smaller than a valid
`target_depth`, caching requires no placeholder or progressive-fill UI. A
snapshot can still be naturally shorter at the pruning-point boundary. The Web
adopts the snapshot's exact `(publication_id, revision)` cursor,
`revision_timestamp_us`, and current publication-context headers, then uses
ordinary canonical delta catch-up toward its desired cursor.

## Fixed views — settled

A browser fixed view is presentation state built from serialized API responses;
it is not an internal `GraphView` and has no API `TrackingPolicy`. A
database-backed window has no public delta lineage and remains frozen until
explicit refresh. A window extracted from an addressable Head publication
follows that Head through the
[canonical delta endpoint](api-protocol.md#canonical-head-delta-responses--settled)
and filters each response locally to its fixed extent. It never requests or
applies a server-projected Fixed delta.

For every successful level, block-hash, or DAA window request, the Web retains
the original anchor and adopts the response's `GraphWindowResolution`.
`resolved_level` is the fixed focus for that image. For a Head-extracted
image, canonical Head deltas update the image without re-resolving the original
anchor. Explicit refresh resubmits that anchor and replaces the stored
resolution. In particular, if later reorgs change a DAA-resolved level's score,
keep focus on the resolved level rather than resolving the original DAA score
again.

An eligible fixed view retains its Head `(publication_id, revision)` source
cursor, that revision's timestamp, the source Head `high_level`, and the exact
effective level extent. It
uses the same optional SSE `client_id`, continuation headers, cache-backed
delta lane, and catch-up sequencing as any canonical Head-delta consumer. An
SSE wakeup updates only the desired cursor and publication state.

A Head-backed fixed view adds no distance-based delay or separate pacing
policy. It follows the same continuation and SSE-wakeup flow as any canonical
Head-delta consumer. The shared convergence interval and cached delta lane
already group work, while another scheduler would deliberately increase cursor
lag and exposure to Head-history expiry.

Replay a canonical batch entry by entry. Before filtering an entry, derive
its target Head lower bound from that entry's `high_level` and
`MAX_CACHE_DEPTH`. If the target Head no longer contains the complete fixed
extent, do not apply that entry or any later queued entry. Keep every earlier
successfully replayed entry, preserve the last coherent image and applied
cursor, and mark the view frozen. An already composed history entry remains
one indivisible replay unit; the Web does not reconstruct an intermediate
prefix hidden inside it.

While the complete extent remains contained, filter that entry as one atomic
candidate:

- retain added blocks whose coordinates lie in the fixed extent;
- retain added edges whose level spans intersect it under the
  [graph-owned edge predicate](api-graph.md#head-block-mutation--settled);
- retain changes for nominal levels and external endpoint levels required by
  retained edges;
- retain VSPC-membership and color changes only for blocks retained by the
  fixed image; and
- ignore all other mutations.

Canonical block and edge removals are absent from every wire entry and the
Fixed image does not infer them. Containment ensures that no object required by
the Fixed image has crossed the Head removal frontier. A filtered candidate
with no visible mutation still advances the fixed image's applied revision and
revision timestamp and adopts the entry's `high_level` as its known source Head
level; it never moves the fixed bounds.

A retained added edge can reference an endpoint level absent from both the
fixed image and the canonical delta because that level was unchanged in Head.
Atomic history entries do not duplicate unchanged source state. Detect such
requirements while preparing queued entries and use the API-owned
[Head-level lookup](api-protocol.md#fixed-window-reuse-of-canonical-head-deltas--settled).
The client may combine distinct missing levels from multiple queued entries in
one request up to `MAX_HEAD_LEVELS_PER_REQUEST`, and splits a larger set across
complete requests.

Stage each returned absolute value with the first dependent filtered entry and
apply that entry's complete browser update atomically. Preserve an existing
locally derived edge-usage counter while replacing the level's public `size`
and `daa_score`. A returned level may be newer than the entry; it does not
change the delta cursor, containment decision, timestamp, or mutation
selection. The canonical batch body itself carries no endpoint-level context.

Level lookup may proceed while earlier ready entries replay. If a dependent
entry reaches its scheduled time before its lookup completes, stop at that
entry. Keep the existing replay deadline; after the lookup completes,
recalculate the next replay time from the remaining queue, which accelerates
catch-up after the delay. If any required level is unavailable, the publication
is no longer addressable, the source history expires, or another request cannot
yield one complete applicable entry, apply nothing from that dependent entry.
Keep prior entries already applied and freeze the last coherent image. A
terminal `Stale` publication remains addressable: the Web may catch up through
its final stalled Head and obtain missing levels from it. Publication
replacement freezes the old fixed image. Only explicit refresh establishes
another window and possible Head lineage.

## Block identity and Genesis — settled

Web block identity is the hash. Decode HTTP-local integers through the
response dictionary immediately. Do not use storage `CompactId` values in
React state, URL identity, or cross-response comparisons. Level/slot
coordinates are display positions, not global canonical identities.

When a materialized Genesis is present, Web recognizes it from its empty
actual direct-parent list. A pruned non-Genesis PP may have no visible parent
edge but still has actual direct parents; ORIGIN as selected parent does not
by itself mark Genesis.
