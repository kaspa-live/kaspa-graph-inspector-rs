# API graph-update ingress

## Scope and ownership

This document owns the per-session graph-update channel, its producer and
receiver capabilities, the pre-seal delivery gate, lifecycle-marker delivery,
and gap reporting. The producer documents own when committed values and marker
commands are offered. [API publication](api-publication.md) owns how ApiService consumes
the ordered stream and reconstructs its derived graph projection.

## In-process graph-update feed — settled

Every processing session owns one fresh ordered, bounded graph-update channel. The
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
    PublishPostSeal,
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
    PreSeal,
    Open,
}

enum GraphUpdateOfferOutcome {
    SuppressedPreSeal,
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

    fn publish_post_seal(
        &self,
    ) -> Result<(), GraphUpdateProducerError>;

    async fn publish_live(
        &self,
    ) -> Result<(), GraphUpdateProducerError>;
}
```

`GraphUpdateProducer` is the cloneable session-scoped producer capability that
Supervisor supplies through ResyncEngine to both processors.
`GraphUpdateReceiver` is the corresponding single-consumer session capability
that Supervisor supplies through `ApiService::reset`. Every producer clone shares
one `GraphUpdateGate`, initially `PreSeal`; the receiver does not expose that
producer-side gate.
`GraphUpdateGapReporter` and the consumer-side `GraphUpdateGap` refer to the
same session-local continuity state. These names fix the semantic capability
split. The mutex around `GraphUpdateGateState` is settled; concrete channel,
gap-counter, and wakeup types remain deferred.

The producer exposes distinct nonblocking operations for `BlockCommitted` and
`VspcCommitted`. Each operation holds the state mutex through classification
and `try_send`. A closed receiver returns `ReceiverClosed`. Otherwise, in
`PreSeal`, the operation discards the API projection update, returns
`SuppressedPreSeal`, and does not advance the gap. The underlying database
commit and processing-tier delivery remain valid. In `Open`, successful
delivery returns `Enqueued`; `Full` discards that delivery, advances the
session's reliable, coalescing gap generation, and returns `GapReported`.

The marker worker's `publish_post_seal` operation holds the same state mutex,
requires `PreSeal`, enqueues `GraphUpdate::PublishPostSeal`, changes the state
to `Open`, and releases the mutex. No ordinary value can enter before the
marker or race between its enqueue and the state transition. Because pre-seal
ordinary values are suppressed, this marker is the first value in the fresh
positive-capacity channel and cannot encounter `Full`; receiver closure leaves
the gate in `PreSeal` and reports supersession.

The marker worker's `publish_live` operation requires `Open`, releases the
state mutex, and then uses lossless delivery, awaiting channel capacity instead
of reporting an ordinary-update gap. Its command FIFO preserves
`PublishPostSeal` before `Live`; unrelated ordinary graph updates may
interleave before `Live`. ApiService's reaction to a reported gap belongs to
the [publication reconstruction contract](api-publication.md#universal-api-reconstruction--settled).

The [BlockProcessor marker contract](block-processing.md#graph-lifecycle-marker-delivery--settled)
owns its worker and marker-command enqueue points. The
[processing lifecycle](processing-lifecycle.md#live-admission) owns the global
causality before the Live command. ApiService consumes the resulting channel
order without reconstructing those producer decisions. Exact channel and
wakeup primitives remain deferred in the
[decision register](../decisions/deferred.md).
