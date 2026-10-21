# API graph-update ingress

## Scope and ownership

This document owns the per-session graph-update channel, its producer and
receiver capabilities, the Catchup delivery gate, lifecycle-marker delivery,
and gap reporting. The processor owners define when committed values are
offered; the [processing lifecycle](processing-lifecycle.md#graph-lifecycle-marker-worker--settled)
owns marker-command timing and worker lifecycle. [API publication](api-publication.md)
owns how the private publication runtime consumes the ordered stream and
reconstructs its derived graph projection.

## In-process graph-update feed — settled

Define:

```text
GRAPH_UPDATE_CHANNEL_CAPACITY = 1024
```

Every processing session owns one fresh ordered graph-update channel with that
capacity. The
channel topology is the session boundary: graph updates carry no session ID,
or cross-session stale-message filter. Processors offer
`BlockCommitted`/`VspcCommitted` only after their respective DB commits. The
[BlockProcessor delivery contract](block-processing.md#committed-block-delivery)
and [VspcProcessor producer contract](vspc-processing.md#commit-and-graph-publication--settled)
establish causal order, so a VSPC update cannot reach this channel ahead of
blocks it depends on.

The ordered stream has this semantic shape:

```rust
enum GraphUpdate {
    Catchup,
    BlockCommitted(BlockCommitted),
    VspcCommitted(VspcCommitted),
    Live,
}

struct GraphUpdateProducer {
    tx: Sender<GraphUpdate>,
    gate: Arc<GraphUpdateGate>,
}

struct GraphUpdateReceiver {
    rx: Receiver<GraphUpdate>,
    gap: GraphUpdateGap,
}

struct GraphUpdateGate {
    state: Mutex<GraphUpdateGateState>,
    gap: GraphUpdateGapReporter,
}

enum GraphUpdateGateState {
    AwaitingCatchup,
    Open,
}

enum GraphUpdateOfferOutcome {
    SuppressedAwaitingCatchup,
    Enqueued,
    GapReported,
}

enum GraphUpdateProducerError {
    ReceiverClosed,
}

impl GraphUpdateProducer {
    fn offer_block_committed(
        &self,
        update: BlockCommitted,
    ) -> Result<GraphUpdateOfferOutcome, GraphUpdateProducerError>;

    fn offer_vspc_committed(
        &self,
        update: VspcCommitted,
    ) -> Result<GraphUpdateOfferOutcome, GraphUpdateProducerError>;

    fn publish_catchup(
        &self,
    ) -> Result<(), GraphUpdateProducerError>;

    async fn publish_live(
        &self,
    ) -> Result<(), GraphUpdateProducerError>;
}
```

`GraphUpdateProducer` is the cloneable session-scoped producer capability that
Supervisor supplies through ProcessingSession to both processors.
`GraphUpdateReceiver` is the corresponding single-consumer session capability
that Supervisor supplies through `ApiService::reset` to the session's private
publication runtime. Every producer clone shares one `GraphUpdateGate`,
initially `AwaitingCatchup`; the receiver does not expose that producer-side
gate.
`GraphUpdateGapReporter` and the consumer-side `GraphUpdateGap` refer to the
same session-local continuity state. These names fix the semantic capability
split. The mutex around `GraphUpdateGateState` is settled; concrete channel,
gap-counter, and wakeup types remain deferred.

The producer exposes distinct nonblocking operations for `BlockCommitted` and
`VspcCommitted`. Each operation holds the state mutex through classification
and `try_send`. A closed receiver returns `ReceiverClosed`. Otherwise, in
`AwaitingCatchup`, the operation discards the API projection update, returns
`SuppressedAwaitingCatchup`, and does not advance the gap. The underlying
database commit and processing-tier delivery remain valid. In `Open`, successful
delivery returns `Enqueued`; `Full` discards that delivery, advances the
session's reliable, coalescing gap generation, and returns `GapReported`.
Every producer operation returns `ReceiverClosed` without advancing the gap
when the receiver is closed. The
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns its active-run and expected-teardown dispositions; producer owners define
only the local work that follows the result.

The marker worker's `publish_catchup` operation holds the same state mutex,
requires `AwaitingCatchup`, enqueues `GraphUpdate::Catchup`, changes the state
to `Open`, and releases the mutex. No ordinary value can enter before the
marker or race between its enqueue and the state transition. Because ordinary
values are suppressed while awaiting Catchup, this marker is the first value
in the fresh positive-capacity channel and cannot encounter `Full`; receiver
closure leaves the gate in `AwaitingCatchup` and returns `ReceiverClosed`.

This atomic enqueue-and-open operation is the complete database/stream
boundary; marker delivery needs no completion acknowledgement. An ordinary
offer ordered before it is suppressed only after its database commit has
completed. Under the
[publication lifecycle](api-publication.md#head-publication-lifecycle-and-stream-alignment--settled),
the seed starts only after consuming the later Catchup marker, so that commit
is covered by the seed. An ordinary offer
ordered after it enters the stream behind the marker. A commit covered by both
the seed and the stream is handled by publication alignment. The
[processing lifecycle](processing-lifecycle.md#graph-lifecycle-marker-worker--settled)
owns command handoff and continuation without waiting for the marker to reach
PublicationRuntime.

The marker worker's `publish_live` operation requires `Open`, releases the
state mutex, and then uses lossless delivery, awaiting channel capacity instead
of reporting an ordinary-update gap. Its command FIFO preserves
`Catchup` before `Live`; unrelated ordinary graph updates may interleave before
`Live`. The runtime's reaction to a reported gap belongs to the
[publication reconstruction contract](api-publication.md#universal-api-reconstruction--settled).

The [processing lifecycle](processing-lifecycle.md#graph-lifecycle-marker-worker--settled)
owns the worker, reliable command handoffs, and their recovery-phase causality.
The publication runtime consumes the
resulting channel order without reconstructing those producer decisions. The
capacity provides about 51 seconds at an expected 20 updates per second and
about 10 seconds at 100 updates per second. Exact channel and wakeup primitives remain deferred in
the [decision register](../decisions/deferred.md).

Operational measurements include current occupancy, high-water mark, reported
gap count, and receiver-closure count for each session feed.
