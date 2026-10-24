# Open architecture decisions

These items require an Architecture decision. They are not implementation
freedom and do not weaken any settled contract.

## Adaptive delta load controller

The [canonical Head-delta protocol](../architecture/api-protocol.md#canonical-head-delta-responses--settled)
fixes one ApiService-wide active interval, its power-of-two values from 1
through 128, its initial value of 4, its role in Live near-Head wake and target
selection, and the requirement that changing it does not reschedule existing
registrations. The controller that subsequently changes that value remains
unresolved.

Raw delta-request rate alone is insufficient because it combines cheap cache
hits with expensive construction. Candidate evidence includes:

- delta-lane permit wait ratio and request queue duration;
- encoding-scheduler occupancy and queue duration;
- active response-delivery occupancy;
- requests per delivered revision;
- cache hits, single-flight joins, builds, and enlargements;
- encoding CPU time and response bytes per revision; and
- client revision lag.

The current first draft would raise the interval quickly under sustained
delta-path pressure, lower it more slowly after sustained relief, change at
most one power-of-two step per evaluation, and use hysteresis. Measurements
must be partitionable by active interval so implementation evidence can show
whether larger batches actually reduce load. Architecture still needs to
select the input signal or composite, thresholds, evaluation period, and
hysteresis before ApiService may change the interval dynamically. The logical
delta operation can be implemented with the settled initial value of 4 and
must keep that value fixed until this decision is resolved.
