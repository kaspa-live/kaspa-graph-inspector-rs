# Web architecture

## Scope and ownership

This document owns browser-side graph state, update behavior, view focus, and
public block identity. [The API architecture](api.md) owns the graph wire
contract, snapshots, deltas, SSE cursor delivery, and response-local hash
dictionaries.

## Update acquisition — partially open

The v1 Web's fast repeated polling is replaced by SSE cursor wakeups and HTTP
delta/snapshot catch-up for cached views. The Web keeps one in-flight catch-up
loop and coalesces desired cursors. Because SSE reconnection is not
exactly-once, each wakeup is a desired `(publication_id, revision)` cursor; graph
state comes from HTTP delta or snapshot responses.

Head-following stays prompt. On publication change, a head-following view
automatically reloads.

A delta response may end at an intermediate revision below the Web's desired
cursor. The single catch-up loop applies that complete interval, adopts its
`to` cursor, and requests the next interval until it reaches the desired cursor
or the API requires a snapshot. It never assumes one response reaches the
original target. It applies a delta only when the view's current revision equals
the delta's `from_revision_id`.

The exact application of the new `GraphView`/`GraphDelta` model to browser
head and fixed views depends on the
[open subview-extraction contract](../decisions/open.md#api-graph-model-completion).
No Web implementation may invent a lower-bound rule while that item remains
open.

## Fixed views — partially open

The fixed-view extraction, update, and freeze boundary will be restored here
when the linked subview contract is settled. On publication change, a fixed
view retains its current image marked frozen/stale. Explicit refresh reruns
the original anchor query.

For every successful level, block-hash, or DAA window request, the Web retains
the original anchor and adopts the response's `GraphWindowResolution`.
`resolved_level` is the fixed focus for that image and subsequent deltas do not
re-resolve the original anchor. Explicit refresh resubmits that anchor and
replaces the stored resolution with the new response. In particular, if later
reorgs change a DAA-resolved level's score, keep focus on the resolved level
rather than resolving the original DAA score again.

Fixed-view revision catch-up is distance-adaptive, not a blanket slow path.
Define:

```text
distance = max(0, head_level - visible_window_end_level)
```

If the head is visible or at most ten levels ahead, update without added
throttling. Beyond that, delay increasingly as distance grows, while
preserving contiguous catch-up and snapshot fallback if delta retention
expires. The exact delay curve and cap remain deferred in the
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
