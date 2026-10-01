# Web architecture

## Scope and ownership

This document owns browser-side graph state, update behavior, view focus, and
public block identity. [The API protocol](api-protocol.md) owns the graph wire
contract, snapshots, deltas, SSE cursor delivery, and response-local hash
dictionaries.

## Update acquisition — settled

The v1 Web's fast repeated polling is replaced by SSE cursor wakeups and HTTP
delta/snapshot catch-up for cached views. The Web keeps one in-flight catch-up
loop and coalesces desired cursors. Because SSE reconnection is not
exactly-once, each wakeup is a desired `(publication_id, revision)` cursor; graph
state comes from HTTP delta or snapshot responses.

Head-following stays prompt. On publication change, a head-following view
automatically reloads.

A state-only wakeup can repeat the current graph cursor. The Web adopts its
publication state without requesting or applying a graph delta. On a Stale
state, it keeps the coherent image visibly marked stale. If its cursor is
behind the Stale wakeup, a head-following view may first catch up through that
publication's final stalled Head. It reloads when a replacement publication ID
appears; until then it does not pretend that the old image remains Live.

A delta response may end at an intermediate revision below the Web's desired
cursor. The single catch-up loop applies that complete interval, adopts its
`to` cursor, and requests the next interval until it reaches the desired cursor
or the API requires a snapshot. It never assumes one response reaches the
original target. It applies a delta only when the view's current revision equals
the delta's `from_revision_id`.

For canonical Head catch-up, the Web consumes `KGI-More-Available`. After
applying a response marked `true`, it immediately requests again from the
returned `to_revision_id`; after `false`, it waits for the next SSE wakeup.
Under the API-owned
[cached Head-delta wire contract](api-protocol.md#canonical-head-delta-responses--settled),
the Web advances its local bounds from `GraphDelta.high_level` and its window
depth, removes blocks below the resulting `low_level`, and removes an edge when
its child leaves the window. This is the browser side of that contract rather
than an independent removal rule.

The Web consumes serialized head, delta, and graph-window responses. It never
receives an internal `GraphView`, observes `TrackingPolicy`, or applies updates
to an ApiService subview. Internal subview extraction belongs exclusively to
the [API graph contract](api-graph.md#frozen-subview-extraction--settled).
Serialized graph-window contents follow the API-owned extraction contract.

## Fixed views — settled behavior with deferred pacing

A browser fixed view is presentation state built from serialized API responses;
it is not an internal `GraphView` and has no API `TrackingPolicy`. When its
window was extracted from the active Head publication, it consumes the
API-owned Head-bounded projected deltas under the revision catch-up policy
below. A database-backed window has no public delta lineage and remains frozen
until explicit refresh. On publication change, a fixed view retains its current
image marked frozen/stale. Explicit refresh reruns the original anchor query.

For every successful level, block-hash, or DAA window request, the Web retains
the original anchor and adopts the response's `GraphWindowResolution`.
`resolved_level` is the fixed focus for that image. For an eligible
Head-extracted image, subsequent projected deltas update it without
re-resolving the original anchor. Explicit refresh resubmits that anchor and
replaces the stored resolution with the new response. In particular, if later
reorgs change a DAA-resolved level's score, keep focus on the resolved level
rather than resolving the original DAA score again.

An eligible fixed view retains its Head `(publication_id, revision)` source
cursor, the source Head `high_level` supplied with the initial extracted
window, and the exact effective level extent. Each catch-up request supplies
that extent to the API projection operation. The Web never applies a raw Head
delta. It applies any valid projected prefix, advances to its actual `to`
cursor, and adopts that projected `GraphDelta.high_level` as the Head level for
pacing. The projected value does not move the fixed window bounds. The Web
continues until the desired Head cursor is reached. An SSE wakeup updates the
desired cursor and publication state only; it does not itself change the
known Head level.

If the API reports that the extent left Head, required projection context is
unavailable, the publication became Stale or changed, or retained Head history
no longer covers the cursor, keep the last coherent image marked frozen/stale.
Do the same when no first complete projected history entry fits the response
budget: apply no part of that entry and end the projected lineage. Only
explicit refresh establishes another window and possible projected lineage.
Projected delta responses are `no-store`; the Web retains the updated graph
image but never caches a response for reuse.

Fixed-view revision catch-up is distance-adaptive, not a blanket slow path.
Define:

```text
distance = max(0, head_level - visible_window_end_level)
```

If the head is visible or at most ten levels ahead, update without added
throttling. Beyond that, delay increasingly as distance grows, while
preserving contiguous projected catch-up and explicit-refresh fallback if Head
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
