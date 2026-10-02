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

## Update acquisition — settled

The v1 Web's fast repeated polling is replaced by SSE cursor wakeups and HTTP
delta/snapshot catch-up for cached views. The Web keeps one in-flight catch-up
loop and coalesces desired cursors. Because SSE reconnection is not
exactly-once, each wakeup is a desired `(publication_id, revision)` cursor; graph
state comes from HTTP delta or snapshot responses.

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
cursor. The single catch-up loop applies that complete interval, adopts its
`to` cursor, and requests the next interval until it reaches the desired cursor
or the API requires a snapshot. It never assumes one response reaches the
original target. It applies a delta only when the view's current revision equals
the delta's `from_revision_id`.

For canonical Head-delta catch-up, the Web supplies its current opaque SSE
`client_id` and consumes the response's `DeltaContinuation`. After
`ContinueImmediately`, it requests again from the returned `to_revision_id`;
after `ReachedHead` or `WaitForWakeup`, it waits for the next SSE wakeup. A
no-delta wait response leaves the graph cursor unchanged. The Web adopts each
delta response's captured publication state. At a Stale publication's final
Head it waits for the replacement wakeup instead of polling that terminal
cursor. The Web adopts each dedicated `ClientRegistration`, then the API-owned
`PublicationState`, before processing the following `PublicationWakeup`. This
replaces its opaque identifier on initial connection, reconnection, and
publication replacement without embedding registration or publication state
in the graph wakeup. Every HTTP request continues to supply the Web's actual
`from_revision_id` as its authoritative graph cursor.
`ClientRegistrationRequired` reconnects SSE and retries from that unchanged
cursor.
Under the API-owned
[cached Head-delta wire contract](api-protocol.md#canonical-head-delta-responses--settled),
a Head-following view advances its local bounds from `GraphDelta.high_level`
and its window depth, removes blocks below the resulting `low_level`, and
removes an edge when its child leaves the window. A fixed view instead follows
the filtering contract below. These are browser reactions to the API contract
rather than independent wire rules.

The Web consumes serialized head, delta, and graph-window responses. It never
receives an internal `GraphView`, observes `TrackingPolicy`, or applies updates
to an ApiService subview. Internal subview extraction belongs exclusively to
the [API graph contract](api-graph.md#frozen-subview-extraction--settled).
Serialized graph-window contents follow the API-owned extraction contract.

## Fixed views — settled behavior with deferred pacing

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
cursor, the source Head `high_level`, and the exact effective level extent. It
uses the same optional SSE `client_id`, continuation headers, cache-backed
delta lane, and catch-up sequencing as any canonical Head-delta consumer. An
SSE wakeup updates only the desired cursor and publication state.

Before filtering a response, derive its target Head lower bound from the
canonical `GraphDelta.high_level` and `MAX_CACHE_DEPTH`. If the target Head
no longer contains the complete fixed extent, do not apply any part of that
delta. Keep the previous coherent image and revision and mark it frozen. A
composed canonical response can hide an add-then-remove transition after the
extent leaves Head because the final removal is absent from the wire; the Web
does not reconstruct or approximate an intermediate prefix.

While the complete extent remains contained, filter the canonical delta as one
atomic candidate:

- retain added blocks whose coordinates lie in the fixed extent;
- retain added edges whose level spans intersect it under the
  [graph-owned edge predicate](api-graph.md#head-block-mutation--settled);
- retain changes for nominal levels and external endpoint levels required by
  retained edges;
- retain VSPC-membership and color changes only for blocks retained by the
  fixed image; and
- ignore all other mutations.

Canonical block and edge removals are absent from the wire and the Fixed image
does not infer them. Containment ensures that no object required by the Fixed
image has crossed the Head removal frontier. A filtered candidate with no
visible mutation still advances the fixed image's revision and adopts the
delta's `high_level` as its known source Head level; it never moves the fixed
bounds.

A retained added edge can reference an endpoint level absent from both the
fixed image and the canonical delta. Before applying anything, collect every
such level and call the API-owned
[Head-level lookup](api-protocol.md#fixed-window-reuse-of-canonical-head-deltas--settled).
Stage its absolute values with the filtered candidate and apply the complete
browser update atomically. Preserve each level's locally derived edge-usage
counter while replacing its public `size` and `daa_score`. A returned level
may be newer than the delta; it does not change the delta cursor, containment
decision, or mutation selection.

If any required level is unavailable, the publication is no longer
addressable, the source history expires, or another request cannot yield one
complete applicable canonical delta, apply nothing from that candidate and
freeze the last coherent image. A terminal `Stale` publication remains
addressable: the Web may catch up through its final stalled Head and obtain
missing levels from it. Publication replacement freezes the old fixed image.
Only explicit refresh establishes another window and possible Head lineage.

Fixed-view revision catch-up is distance-adaptive, not a blanket slow path.
Define:

```text
distance = max(0, head_level - visible_window_end_level)
```

If the head is visible or at most ten levels ahead, update without added
throttling. Beyond that, delay increasingly as distance grows, while
preserving contiguous canonical catch-up and explicit-refresh fallback if Head
history retention expires. The exact delay curve and cap remain deferred in the
[decision register](../decisions/deferred.md).

## Block identity and Genesis — settled

Web block identity is the hash. Decode HTTP-local integers through the
response dictionary immediately. Do not use storage `CompactId` values in
React state, URL identity, or cross-response comparisons. Level/slot
coordinates are display positions, not global canonical identities.

When a materialized Genesis is present, Web recognizes it from its empty
actual direct-parent list. A pruned non-Genesis PP may have no visible parent
edge but still has actual direct parents; ORIGIN as selected parent does not
by itself mark Genesis.
