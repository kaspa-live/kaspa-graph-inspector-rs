# Archived adaptive fixed-view pacing design

Status: frozen, rejected, non-normative historical snapshot.

This file preserves the browser-side adaptive pacing design as it stood
immediately before its removal on 2 October 2026. It must not be updated to
follow the current architecture.

## Archived contract

Source: `docs/architecture/web.md`, `Fixed views`.

A Head-backed browser fixed view consumed canonical Head deltas and filtered
them locally to its fixed extent. Its revision catch-up was distance-adaptive
rather than a blanket slow path. Distance was defined as:

```text
distance = max(0, head_level - visible_window_end_level)
```

If the Head was visible or at most ten levels ahead, the Web updated without
added throttling. Beyond that point, it delayed increasingly as distance grew
while preserving contiguous canonical catch-up and explicit-refresh fallback
if Head history retention expired. The exact delay curve and cap remained a
deferred implementation choice.

The associated verification required no added throttling through distance ten,
activation of the increasing-delay policy at distance eleven, contiguous
canonical delta catch-up, and explicit-refresh fallback after retention
expiry.

## Original purpose

The policy attempted to reduce browser and network work for an older fixed
window because many later Head mutations would be discarded by the window's
local extent filter.

## Rejection and replacement

The later canonical Head-delta design already groups progress through its
revision convergence interval, shares publication-cached delta bodies, and
coordinates requests through continuation responses and SSE wakeups. A
Head-backed fixed window reuses that same lane, filters it locally, and fetches
only missing retained endpoint levels through the Head-level lookup.

An additional distance-based scheduler would intentionally leave the fixed
cursor behind, introduce a second client pacing policy, and increase exposure
to Head-history expiry. Its remaining bandwidth benefit did not justify that
complexity. Head-backed fixed windows therefore use the same continuation and
wakeup rules as other canonical Head-delta consumers. Database-backed windows
remain frozen until explicit refresh.
