# Archived Head-bounded Fixed projection design

Status: frozen, superseded, non-normative historical snapshot.

This file preserves the server-side Head-bounded Fixed projection design as it
stood immediately before replacement by canonical Head-delta reuse and
browser-side extent filtering. The excerpts below retain their original
wording and links. They must not be updated to follow the current architecture.

## Public result types

Source: `docs/architecture/api-protocol.md`, `Public API values — settled`.

```rust
enum FixedProjectionEndReason {
    ExtentLeftHead,
    RequiredLevelUnavailable,
}

enum FixedDeltaResponseOutcome {
    UpToDate {
        revision: u64,
    },
    Complete(GraphDelta),
    Prefix(GraphDelta),
    Freeze {
        valid_prefix: Option<GraphDelta>,
        reason: FixedProjectionEndReason,
    },
    FreshViewRequired(FreshViewReason),
}
```

## Head-bounded Fixed delta projection — settled

Source: `docs/architecture/api-protocol.md`.

A public anchored window extracted from the active Head publication can follow
that publication without acquiring its own server-side `GraphPublication` or
`GraphHistory`. Its serialized response carries the Head `publication_id` and
source revision, the source Head `high_level` at that revision, and its fixed
effective level extent. The source Head level is lineage metadata, while the
effective extent describes the serialized window. A database-backed window has
no such lineage; the API returns it as a non-updating serialized snapshot.

The public projection operation is conceptually:

```rust
fn range_for_extent(
    publication_id: u64,
    from_revision_id: u64,
    requested_to_revision_id: u64,
    low_level: u64,
    high_level: u64,
) -> FixedDeltaResponseOutcome;
```

ApiService first applies the ordinary Head-history publication, availability,
boundary, and target selection rules to obtain eligible gapless entries. That
validation rejects a backward target before projection under the canonical
Head-delta protocol. ApiService does not apply the canonical Head response-byte
budget before projection. It
projects eligible complete entries in order to `[low_level, high_level]` and
applies the Fixed response budget to the final projected representation. The
fixed extent must remain inside the Head nominal extent throughout every
returned entry:

```text
head.low_level <= low_level
AND
high_level <= head.high_level
```

The initial Head extraction establishes the upper-bound condition at
`from_revision_id`. Because Head bounds never decrease, each selected entry's
`high_level` and the Head `max_depth` determine whether its resulting lower
bound still covers `low_level`. An aggregated history entry is indivisible. If
one would move the Head lower bound above `low_level`, do not project that
entry. Return earlier complete entries as `Freeze.valid_prefix`, when present,
and `ExtentLeftHead`; otherwise return that terminal reason without a prefix.
The current active Head view used for endpoint-level enrichment must also still
contain the fixed extent. Once it does not, return `ExtentLeftHead`; never use a
database read to reconstruct a departed extent.

Projection retains blocks whose coordinates lie in the fixed extent; edges
whose level spans intersect it under the settled edge predicate; nominal
levels in the extent; external endpoint levels required by retained edges; and
VSPC-membership or color changes for retained blocks. Head pruning cannot
remove a retained fixed block or edge while the containment predicate holds.
Its block and edge removals therefore lie outside the fixed projection and are
discarded by the extent filter.

A newly retained edge may require an endpoint `Level` that did not change in
Head and therefore is absent from the stored Head delta. In that case,
ApiService copies the complete current `Level` from the active Head view into
the projected result as:

```text
LevelChange {
    level,
    before: None,
    after: Some(current_level),
}
```

Within a Head-bounded projected response, this `before = None` means that the
receiver's previous value is unspecified; it does not assert that the level
was absent. Its wire meaning is an absolute upsert: `after` supplies the
resulting value and `before` is not an application precondition. Canonical and
internal deltas retain the ordinary absence meaning of `None`. ApiService adds
this presentation-only change after composing the canonical projected
interval, so it never participates in canonical delta composition or changes
its no-net-change rules. If that level is unavailable, return
`Freeze { reason: RequiredLevelUnavailable, .. }`. This lookup never consults
PostgreSQL.

The endpoint level may be newer than the returned `to_revision_id`; this is an
accepted presentation approximation. Block, edge, VSPC-membership, and color
mutations remain exact for the returned interval. A synthesized level never
affects cursor continuity, containment, or mutation selection, and later
absolute level changes supersede the approximated value.

Budget selection treats every stored Head-history entry as an indivisible
unit. For each candidate prefix, ApiService projects and composes its complete
entries, then adds every required endpoint `Level` to that candidate result and
measures the resulting complete response. The measured representation includes
the projected mutations, synthesized endpoint levels, response-local hash
dictionary, and response envelope. An entry is accepted only when that final
representation fits the Fixed response budget. Projection never splits an
aggregated entry, drops an endpoint level, or presents a truncated entry as
complete.

If all eligible entries fit, return `Complete`. If at least one fits but the
next complete projected entry does not, return the accepted entries as
`Prefix`, ending at the last accepted stored-entry boundary. If the first
complete projected entry does not fit, return
`FreshViewRequired(FirstStoredDeltaExceedsBudget)`. Existing projection
failures retain their `Freeze` outcomes. Exact encoded-size measurement and
whether the accepted interval is transmitted as entries or one composed patch
remain implementation choices under the wire-format decision, but the emitted
response must remain within the configured byte limit.

Every projected response uses a self-contained response-local hash dictionary
and carries the Head `(publication_id, revision)` source cursor. A projected
interval may contain no retained mutation and still advance that cursor. Every
returned projected `GraphDelta.high_level` is the source Head high level at the
returned `to_revision_id`; it does not move either bound of the serialized
fixed extent. This derived response is not a Head-history entry or an internal
Fixed lineage delta: do not append it to `GraphHistory`, apply it to
ApiService's internal `GraphView`, or use it as an input to canonical delta
composition. The internal updating-`Fixed`
[contract](api-graph.md#fixed-updates-through-the-head-cache--settled) remains
unchanged.

Projected Fixed delta responses are generated on demand and are never cached.
They carry `Cache-Control: no-store`, have no ETag, do not enter encoded-body
caches or cache single-flight construction, and have no immutable interval
cache identity.

Publication mismatch, terminal Stale state, and unavailable Head history use
the ordinary fresh-view outcomes. Every `FixedProjectionEndReason` is a
terminal server outcome for that projected lineage; no continuation is
available through a later range on the same lineage.

## Fixed views — settled behavior with deferred pacing

Source: `docs/architecture/web.md`.

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

## Verification obligations

Source: `docs/architecture/verification.md`.

Verify the settled
[Head-bounded Fixed projection](api-protocol.md#head-bounded-fixed-delta-projection--settled)
from a Head-extracted anchored window. Cover a fully contained extent, an empty
projected mutation that still advances the Head cursor, blocks inside the
extent, crossing edges with neither endpoint block present, nominal and
external endpoint levels, and retained VSPC-membership and color changes. A raw
Head delta must never be delivered as the projected response. Verify the
initial extracted response supplies the source Head high level, every projected
`GraphDelta.high_level` is the source Head high level at its returned `to`
revision, the browser adopts it for pacing, and neither value changes the fixed
effective bounds. An SSE wakeup alone must not change the browser's known Head
level.

Advance Head until the fixed lower bound leaves its nominal extent. Verify an
indivisible crossing history entry is not projected, any earlier complete
prefix is returned and applied, and the browser then freezes. Also cover no
valid prefix, publication replacement, Stale publication, and expired history.
When a retained edge needs an unchanged endpoint level, fetch it from the
current Head view; accept that this level can be newer than the returned
cursor, but never use it for coverage or cursor decisions. Encode the
synthesized absolute upsert as `before = None` and `after = Some(level)`, apply
it successfully whether the browser previously lacked or retained that level,
and do not interpret its `before` as an absence assertion. Verify enrichment
occurs after canonical projected composition. Absence of that required level
freezes only the browser fixed image. Head pruning removals outside the fixed
projection are discarded without affecting Head or the browser image.

Exercise the Fixed response budget after projection and endpoint-level
enrichment. Cover a complete interval that fits, a largest complete-entry
prefix when the next projected entry exceeds the budget, and a first projected
entry that exceeds the budget only after an endpoint level and the response
dictionary are added. Verify the last case applies no partial mutation, returns
the API-owned fresh-view outcome, and leaves the browser's last coherent image
frozen/stale until explicit refresh.

## Rejected-design entry

Source: `docs/decisions/rejected.md`.

| Rejected proposal | Concise reason | Current owner(s) |
|---|---|---|
| Apply a standalone Head-generated `GraphDelta` to a `Fixed` view. | Internal Fixed updates use the original event plus Head context; browser fixed views use the API's extent-projected, context-enriched response. | [API graph model](../architecture/api-graph.md), [API protocol](../architecture/api-protocol.md) |
