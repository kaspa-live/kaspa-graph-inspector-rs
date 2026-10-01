# Head delta cache effectiveness estimate

Status: non-normative predictive analysis, recorded 1 October 2026.

This document preserves the initial effectiveness estimate and proposed
observability for the settled Head delta cache and convergence design. It is
not an architecture contract, accepted performance target, resource limit, or
verification requirement. The focused [API protocol](../architecture/api-protocol.md)
owns current cache and delivery behavior. Measurements from implementation may
invalidate these estimates and motivate later tuning or an architecture
change.

## Working assumptions

The initial estimate assumes:

```text
average graph revision rate               about 20 revisions/s
DELTA_CONVERGENCE_REVISION_INTERVAL       4 revisions
```

The normal shared lane would therefore produce approximately:

```text
20 / 4 = 5 CachedDelta entries/s
```

A continuously connected client's visible graph age would normally vary
between zero and four revisions. Before transport and processing time, this
corresponds to roughly:

```text
average batching delay    about 100 ms
maximum batching delay    about 200 ms
```

## Expected steady-state sharing

Suppose `N` clients have converged on the same source revision:

```text
F -- CachedDelta --> F+4
```

The first request starts construction. Concurrent requests join its
single-flight job, and later requests receive the completed cached value.

| Clients | Delta deliveries/s | Builds and encodes/s | Shared-work ratio |
|---:|---:|---:|---:|
| 1 | 5 | 5 | 0% |
| 10 | 50 | about 5 | about 90% |
| 100 | 500 | about 5 | about 99% |
| 1,000 | 5,000 | about 5 | about 99.9% |

The estimated shared-work ratio is:

```text
1 - encoded_delta_builds / delta_deliveries
```

This combines completed cache hits and single-flight waiters. Measure them
separately as well: perfectly synchronized clients may mostly join an active
job and produce few completed-cache hits even though nearly all expensive work
is shared.

## Expected request and CPU effects

Compared with requesting after every revision, the four-revision interval
would reduce normal delta-request and graph-wakeup frequency by approximately
75%:

```text
before: 20 requests/client/s
after:   5 requests/client/s
```

The expected cost order is:

```text
delta range selection
    < composition
    < serialization and dictionary construction
    < compression
```

The cache primarily eliminates repeated work from the expensive end. Each
client still incurs lightweight request handling and response-envelope
construction, but converged clients share composition, serialization,
hash-dictionary construction, and compression.

For 100 converged clients, the initial expectation is that these expensive
operations approach one execution per shared lane entry instead of one per
client response. HTTP handling and byte delivery remain proportional to the
number of clients.

## Convergence from dispersed cursors

A newly connected or delayed client may initially have a unique source
revision. Its first response may therefore require a unique build:

```text
unique F -- one-off bridge --> hot T --> shared lane
```

The heat policy is expected to direct subsequent work toward a popular
destination.
The initial cost is approximately one build per distinct starting cursor,
followed by shared cached entries. Destination concentration is therefore
expected to rise quickly after reconnect bursts.

Under the protocol's CPU-oriented rule, the estimate assumes the shortest
bridge to an existing cached source. The fresh-view distance gate is expected
to prevent expensive long-range composition and low-value cache entries for
much older clients.

## Expected cache extent

The dominant lane creates roughly one entry per four revisions. A useful
approximation is:

```text
dominant cached entries
    about retained revision count / 4
```

Expressed through graph levels:

```text
entries
    about MAX_CACHE_LEVEL_DISTANCE
        * average revisions per level
        / 4
```

Additional entries arise from unusual source cursors, response-budget
fallbacks, and reconnect bursts. Structural eviction removes them when their
source leaves retained history or their target becomes too far behind Head.

## Expected network effect

The cache does not eliminate the graph data each client must receive. Its
expected network benefits come from:

- fewer HTTP requests and SSE wakeups;
- one response envelope per several revisions;
- potentially better compression across a larger delta; and
- reuse of server-side encoded bytes.

Total delivered response bytes still grow approximately linearly with the
number of connected clients.

## Candidate implementation metrics

These are proposed measurements for validating the estimates. Their names and
exact instrumentation are implementation choices, not current contracts.

### Cache effectiveness

```text
head_delta_deliveries_total
head_delta_builds_total
head_delta_cache_hits_total
head_delta_singleflight_joins_total
```

Derived measurements:

```text
shared_work_ratio
    = 1 - head_delta_builds_total / head_delta_deliveries_total

completed_cache_hit_ratio
    = head_delta_cache_hits_total / head_delta_deliveries_total

singleflight_join_ratio
    = head_delta_singleflight_joins_total / head_delta_deliveries_total
```

`shared_work_ratio` is the primary effectiveness estimate because it captures
both completed-cache reuse and active-job sharing.

### Lane convergence

```text
destination_delivery_count{to_revision}
distinct_destinations_per_window
distinct_sources_per_window
catchup_hops_per_client
```

Useful derived measurements are:

```text
dominant_destination_share
    = deliveries to hottest destination / all deliveries

destination_fan_out
    = distinct to_revision values in a rolling interval
```

After warm-up, the expectation is a high dominant-destination share and low
destination fan-out.

### Construction cost

Measure latency and CPU time independently for:

```text
history selection
delta composition
hash-dictionary construction
serialization
compression
```

Also record distributions for:

```text
singleflight_waiters_per_job
encoded_delta_bytes
estimated_raw_bytes / encoded_delta_bytes
cached_delta_revision_span
cached_delta_level_span
```

### Wake scheduling

```text
client_registration_count
client_reconnect_count
revision_wakeups_sent
state_wakeups_sent
replacement_wakeups_sent
wait_for_wakeup_outcomes
continue_immediately_outcomes
```

Measure the elapsed time and revision distance between the client cursor, the
wake boundary being reached, the HTTP request arriving, and the response being
delivered. Under the working 10-BPS assumption, normal revision wake delay
is expected to cluster near four revisions and roughly 200 ms.

### Recovery and fragmentation

```text
size_limited_short_prefixes
cpu_bridge_responses
fresh_view_distance_rejections
first_entry_budget_rejections
client_registration_required_responses
requests_per_catchup
```

Frequent short prefixes would indicate that the response budget is fragmenting
the main lane. Frequent distance rejections would suggest that
`MAX_CACHE_LEVEL_DISTANCE` is too small for observed client behavior.

## Initial prediction

For multiple continuously connected clients, the initial prediction is:

- 90-99% shared construction work with 10-100 clients;
- approximately five dominant delta builds per second at 20 revisions/s;
- approximately 75% fewer graph wakeups and delta requests than per-revision
  delivery;
- one or a few dominant destination revisions per convergence interval;
- roughly 100 ms average batching delay and 200 ms maximum batching delay,
  excluding transport; and
- one-off additional builds for new, reconnecting, or delayed source cursors.

The recommended first implementation assessment prioritizes
`shared_work_ratio`.
Unlike completed cache-hit ratio alone, it captures the CPU benefit of both
completed cache reuse and single-flight joining.
