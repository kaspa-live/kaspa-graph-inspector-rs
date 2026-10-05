use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use kgi_model::graph_update::{BlockCommitted, GraphUpdate, VspcCommitted};
use thiserror::Error;
use tokio::sync::{mpsc, watch};

use crate::gap::{GraphUpdateGap, GraphUpdateGapReporter};

/// Number of graph updates retained by one processing-session ingress.
pub const GRAPH_UPDATE_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug)]
struct GraphUpdateGate {
    state: Mutex<GraphUpdateGateState>,
    gap: GraphUpdateGapReporter,
}

impl GraphUpdateGate {
    fn new(state: Mutex<GraphUpdateGateState>, gap: GraphUpdateGapReporter) -> Self {
        Self { state, gap }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphUpdateGateState {
    PreSeal,
    Open,
}

/// Result of offering an ordinary committed graph update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphUpdateOfferOutcome {
    SuppressedPreSeal,
    Enqueued,
    GapReported,
}

/// Failure to deliver through a session whose consumer no longer exists.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphUpdateProducerError {
    #[error("graph-update receiver is closed")]
    ReceiverClosed,
}

/// One consumer wakeup from the ordered stream or its continuity signal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphUpdateReceiveEvent {
    Update(GraphUpdate),
    Gap(u64),
}

/// Point-in-time operational measurements for one ingress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphUpdateIngressMetricsSnapshot {
    /// Number of graph updates currently queued for the receiver.
    pub current_occupancy: usize,
    /// Greatest queue occupancy observed during this session.
    pub high_water_mark: usize,
    /// Number of ordinary updates discarded because the open channel was full.
    pub reported_gap_count: u64,
    /// Number of times the session receiver transitioned to closed.
    pub receiver_closure_count: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct GraphUpdateIngressMetrics {
    inner: Arc<Mutex<GraphUpdateIngressMetricsInner>>,
}

#[derive(Debug, Default)]
struct GraphUpdateIngressMetricsInner {
    current_occupancy: usize,
    high_water_mark: usize,
    reported_gap_count: u64,
    receiver_closed: bool,
    receiver_closure_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphUpdateTrySendError {
    Full,
    Closed,
}

impl GraphUpdateIngressMetrics {
    fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(GraphUpdateIngressMetricsInner::default())) }
    }

    fn send_reserved(&self, permit: mpsc::Permit<'_, GraphUpdate>, update: GraphUpdate) -> Result<(), GraphUpdateProducerError> {
        let mut inner = self.inner.lock().expect("graph-update metrics mutex poisoned");
        if inner.receiver_closed {
            return Err(GraphUpdateProducerError::ReceiverClosed);
        }

        Self::record_enqueue(&mut inner);
        permit.send(update);
        Ok(())
    }

    fn try_send(&self, tx: &mpsc::Sender<GraphUpdate>, update: GraphUpdate) -> Result<(), GraphUpdateTrySendError> {
        let mut inner = self.inner.lock().expect("graph-update metrics mutex poisoned");
        if inner.receiver_closed {
            return Err(GraphUpdateTrySendError::Closed);
        }

        match tx.try_send(update) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => return Err(GraphUpdateTrySendError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(GraphUpdateTrySendError::Closed),
        }
        Self::record_enqueue(&mut inner);
        Ok(())
    }

    fn record_enqueue(inner: &mut GraphUpdateIngressMetricsInner) {
        inner.current_occupancy = inner.current_occupancy.checked_add(1).expect("graph-update occupancy overflow");
        inner.high_water_mark = inner.high_water_mark.max(inner.current_occupancy);
    }

    fn dequeued(&self) {
        let mut inner = self.inner.lock().expect("graph-update metrics mutex poisoned");
        inner.current_occupancy = inner.current_occupancy.checked_sub(1).expect("graph-update occupancy underflow");
    }

    pub(crate) fn advance_gap_generation(&self) -> Option<u64> {
        let mut inner = self.inner.lock().expect("graph-update metrics mutex poisoned");
        let generation = inner.reported_gap_count.checked_add(1)?;
        inner.reported_gap_count = generation;
        Some(generation)
    }

    fn close_receiver(&self, rx: &mut mpsc::Receiver<GraphUpdate>) {
        let mut inner = self.inner.lock().expect("graph-update metrics mutex poisoned");
        rx.close();
        if !inner.receiver_closed {
            inner.receiver_closed = true;
            inner.receiver_closure_count = inner.receiver_closure_count.checked_add(1).expect("receiver-closure count overflow");
        }
    }

    fn drop_receiver(&self, rx: &mut mpsc::Receiver<GraphUpdate>) {
        let mut inner = self.inner.lock().expect("graph-update metrics mutex poisoned");
        rx.close();
        if !inner.receiver_closed {
            inner.receiver_closed = true;
            inner.receiver_closure_count = inner.receiver_closure_count.checked_add(1).expect("receiver-closure count overflow");
        }
        inner.current_occupancy = 0;
    }

    #[must_use]
    fn snapshot(&self) -> GraphUpdateIngressMetricsSnapshot {
        let inner = self.inner.lock().expect("graph-update metrics mutex poisoned");
        GraphUpdateIngressMetricsSnapshot {
            current_occupancy: inner.current_occupancy,
            high_water_mark: inner.high_water_mark,
            reported_gap_count: inner.reported_gap_count,
            receiver_closure_count: inner.receiver_closure_count,
        }
    }
}

/// Cloneable producer capability for one processing session.
#[derive(Clone, Debug)]
pub struct GraphUpdateProducer {
    tx: mpsc::Sender<GraphUpdate>,
    gate: Arc<GraphUpdateGate>,
    metrics: GraphUpdateIngressMetrics,
}

impl GraphUpdateProducer {
    fn new(tx: mpsc::Sender<GraphUpdate>, gate: Arc<GraphUpdateGate>, metrics: GraphUpdateIngressMetrics) -> Self {
        Self { tx, gate, metrics }
    }

    /// Nonblockingly offers a committed block update to the session feed.
    ///
    /// # Errors
    ///
    /// Returns [`GraphUpdateProducerError::ReceiverClosed`] when the session
    /// receiver is closed.
    pub fn offer_block_committed(&self, update: BlockCommitted) -> Result<GraphUpdateOfferOutcome, GraphUpdateProducerError> {
        self.offer_ordinary(GraphUpdate::BlockCommitted(update))
    }

    /// Nonblockingly offers a committed VSPC update to the session feed.
    ///
    /// # Errors
    ///
    /// Returns [`GraphUpdateProducerError::ReceiverClosed`] when the session
    /// receiver is closed.
    pub fn offer_vspc_committed(&self, update: VspcCommitted) -> Result<GraphUpdateOfferOutcome, GraphUpdateProducerError> {
        self.offer_ordinary(GraphUpdate::VspcCommitted(update))
    }

    fn offer_ordinary(&self, update: GraphUpdate) -> Result<GraphUpdateOfferOutcome, GraphUpdateProducerError> {
        let state = self.gate.state.lock().expect("graph-update gate mutex poisoned");

        if self.tx.is_closed() {
            return Err(GraphUpdateProducerError::ReceiverClosed);
        }

        if *state == GraphUpdateGateState::PreSeal {
            return Ok(GraphUpdateOfferOutcome::SuppressedPreSeal);
        }

        match self.metrics.try_send(&self.tx, update) {
            Ok(()) => Ok(GraphUpdateOfferOutcome::Enqueued),
            Err(GraphUpdateTrySendError::Full) => {
                self.gate.gap.report();
                Ok(GraphUpdateOfferOutcome::GapReported)
            }
            Err(GraphUpdateTrySendError::Closed) => Err(GraphUpdateProducerError::ReceiverClosed),
        }
    }

    /// Publishes the first session marker and opens the shared producer gate.
    ///
    /// # Errors
    ///
    /// Returns [`GraphUpdateProducerError::ReceiverClosed`] when the session
    /// receiver is closed.
    ///
    /// # Panics
    ///
    /// Panics when the shared producer gate is not in its initial pre-seal
    /// state.
    pub fn publish_post_seal(&self) -> Result<(), GraphUpdateProducerError> {
        let mut state = self.gate.state.lock().expect("graph-update gate mutex poisoned");
        assert_eq!(*state, GraphUpdateGateState::PreSeal, "PublishPostSeal requires the PreSeal graph-update gate");

        match self.metrics.try_send(&self.tx, GraphUpdate::PublishPostSeal) {
            Ok(()) => {
                *state = GraphUpdateGateState::Open;
                Ok(())
            }
            Err(GraphUpdateTrySendError::Closed) => Err(GraphUpdateProducerError::ReceiverClosed),
            Err(GraphUpdateTrySendError::Full) => {
                unreachable!("PublishPostSeal must be the first value in a positive-capacity channel")
            }
        }
    }

    /// Waits for channel capacity and publishes the lossless live marker.
    ///
    /// # Errors
    ///
    /// Returns [`GraphUpdateProducerError::ReceiverClosed`] when the session
    /// receiver closes before delivery.
    ///
    /// # Panics
    ///
    /// Panics when the shared producer gate is not open.
    pub async fn publish_live(&self) -> Result<(), GraphUpdateProducerError> {
        self.publish_live_after_reservation(std::future::ready(())).await
    }

    async fn publish_live_after_reservation<F>(&self, after_reservation: F) -> Result<(), GraphUpdateProducerError>
    where
        F: Future<Output = ()>,
    {
        {
            let state = self.gate.state.lock().expect("graph-update gate mutex poisoned");
            assert_eq!(*state, GraphUpdateGateState::Open, "Live requires an open graph-update gate");
        }

        let permit = self.tx.reserve().await.map_err(|_| GraphUpdateProducerError::ReceiverClosed)?;
        after_reservation.await;
        self.metrics.send_reserved(permit, GraphUpdate::Live)
    }

    #[must_use]
    /// Returns the current operational measurements for this session feed.
    pub fn metrics(&self) -> GraphUpdateIngressMetricsSnapshot {
        self.metrics.snapshot()
    }
}

/// Single-consumer capability for one processing session.
#[derive(Debug)]
pub struct GraphUpdateReceiver {
    rx: mpsc::Receiver<GraphUpdate>,
    gap: GraphUpdateGap,
    metrics: GraphUpdateIngressMetrics,
}

impl GraphUpdateReceiver {
    fn new(rx: mpsc::Receiver<GraphUpdate>, gap: GraphUpdateGap, metrics: GraphUpdateIngressMetrics) -> Self {
        Self { rx, gap, metrics }
    }

    /// Waits for the next ordered channel value.
    pub async fn recv_update(&mut self) -> Option<GraphUpdate> {
        let update = self.rx.recv().await?;
        self.metrics.dequeued();
        Some(update)
    }

    /// Waits for the next unobserved continuity-gap generation.
    pub async fn wait_for_gap(&mut self) -> Option<u64> {
        loop {
            if let Some(generation) = self.gap.take_pending() {
                return Some(generation);
            }
            if let Some(generation) = self.gap.changed().await {
                return Some(generation);
            }
            if self.gap.reporter_closed() {
                return None;
            }
        }
    }

    /// Waits for either a continuity gap or the next ordered update.
    ///
    /// An already reported gap takes priority. Call [`Self::recv_update`]
    /// when channel order must be observed independently, including while
    /// awaiting the first `PublishPostSeal` marker.
    pub async fn next_event(&mut self) -> Option<GraphUpdateReceiveEvent> {
        loop {
            if let Some(generation) = self.gap.take_pending() {
                return Some(GraphUpdateReceiveEvent::Gap(generation));
            }

            tokio::select! {
                biased;
                generation = self.gap.changed(), if !self.gap.reporter_closed() => {
                    if let Some(generation) = generation {
                        return Some(GraphUpdateReceiveEvent::Gap(generation));
                    }
                }
                update = self.rx.recv() => {
                    let update = update?;
                    self.metrics.dequeued();
                    return Some(GraphUpdateReceiveEvent::Update(update));
                }
            }
        }
    }

    /// Attempts to receive only an ordered update without waiting.
    pub fn try_recv_update(&mut self) -> Result<GraphUpdate, mpsc::error::TryRecvError> {
        let update = self.rx.try_recv()?;
        self.metrics.dequeued();
        Ok(update)
    }

    /// Returns and marks the latest pending gap generation.
    pub fn take_pending_gap(&mut self) -> Option<u64> {
        self.gap.take_pending()
    }

    /// Returns the latest gap generation without marking it observed.
    #[must_use]
    pub fn gap_generation(&self) -> u64 {
        self.gap.generation()
    }

    /// Stops new producer delivery while retaining already queued updates.
    pub fn close(&mut self) {
        self.metrics.close_receiver(&mut self.rx);
    }

    #[must_use]
    pub fn metrics(&self) -> GraphUpdateIngressMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl Drop for GraphUpdateReceiver {
    fn drop(&mut self) {
        self.metrics.drop_receiver(&mut self.rx);
    }
}

/// Creates a fresh graph-update ingress for one processing session.
#[must_use]
pub fn graph_update_channel() -> (GraphUpdateProducer, GraphUpdateReceiver) {
    let (tx, rx) = mpsc::channel(GRAPH_UPDATE_CHANNEL_CAPACITY);
    let (gap_tx, gap_rx) = watch::channel(0);
    let metrics = GraphUpdateIngressMetrics::new();
    let gate = Arc::new(GraphUpdateGate::new(
        Mutex::new(GraphUpdateGateState::PreSeal),
        GraphUpdateGapReporter::new(gap_tx, metrics.clone()),
    ));

    (GraphUpdateProducer::new(tx, gate, metrics.clone()), GraphUpdateReceiver::new(rx, GraphUpdateGap::new(gap_rx), metrics))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kgi_model::{
        block::BlockHash,
        graph_update::{GraphUpdate, LevelCommitted, VspcCommitted},
    };

    use super::*;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn vspc_update(byte: u8) -> VspcCommitted {
        VspcCommitted {
            source: hash(byte),
            destination: hash(byte.wrapping_add(1)),
            removed: Arc::from([]),
            added: Arc::from([hash(byte.wrapping_add(1))]),
            level_snapshots: Arc::<[LevelCommitted]>::from([]),
        }
    }

    #[tokio::test]
    async fn pre_seal_offers_are_suppressed_without_a_gap() {
        let (producer, mut receiver) = graph_update_channel();

        assert_eq!(producer.offer_vspc_committed(vspc_update(1)), Ok(GraphUpdateOfferOutcome::SuppressedPreSeal));
        assert_eq!(receiver.gap_generation(), 0);
        assert_eq!(receiver.try_recv_update(), Err(mpsc::error::TryRecvError::Empty));
        assert_eq!(producer.metrics().current_occupancy, 0);
    }

    #[tokio::test]
    async fn post_seal_is_first_and_opens_every_producer_clone() {
        let (producer, mut receiver) = graph_update_channel();
        let clone = producer.clone();

        producer.publish_post_seal().expect("receiver is open");
        assert_eq!(clone.offer_vspc_committed(vspc_update(2)), Ok(GraphUpdateOfferOutcome::Enqueued));

        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::VspcCommitted(vspc_update(2))));
    }

    #[tokio::test]
    async fn racing_post_seal_and_ordinary_offer_preserve_the_gate_boundary() {
        let (producer, mut receiver) = graph_update_channel();
        let _keepalive = producer.clone();
        let ordinary_producer = producer.clone();
        let start = Arc::new(tokio::sync::Barrier::new(3));

        let marker_start = start.clone();
        let marker = tokio::spawn(async move {
            marker_start.wait().await;
            producer.publish_post_seal()
        });
        let ordinary_start = start.clone();
        let ordinary = tokio::spawn(async move {
            ordinary_start.wait().await;
            ordinary_producer.offer_vspc_committed(vspc_update(3))
        });
        start.wait().await;

        assert_eq!(marker.await.expect("marker task joins"), Ok(()));
        let ordinary_outcome = ordinary.await.expect("ordinary task joins").expect("receiver is open");
        assert_eq!(receiver.try_recv_update(), Ok(GraphUpdate::PublishPostSeal));
        match ordinary_outcome {
            GraphUpdateOfferOutcome::SuppressedPreSeal => {
                assert_eq!(receiver.try_recv_update(), Err(mpsc::error::TryRecvError::Empty));
            }
            GraphUpdateOfferOutcome::Enqueued => {
                assert_eq!(receiver.try_recv_update(), Ok(GraphUpdate::VspcCommitted(vspc_update(3))));
            }
            GraphUpdateOfferOutcome::GapReported => panic!("fresh ingress cannot be full"),
        }
    }

    #[tokio::test]
    async fn a_final_full_offer_reports_a_coalescing_gap() {
        let (producer, mut receiver) = graph_update_channel();
        producer.publish_post_seal().expect("receiver is open");

        for byte in 0..(GRAPH_UPDATE_CHANNEL_CAPACITY - 1) {
            assert_eq!(producer.offer_vspc_committed(vspc_update(byte as u8)), Ok(GraphUpdateOfferOutcome::Enqueued));
        }

        assert_eq!(producer.offer_vspc_committed(vspc_update(10)), Ok(GraphUpdateOfferOutcome::GapReported));
        assert_eq!(producer.offer_vspc_committed(vspc_update(11)), Ok(GraphUpdateOfferOutcome::GapReported));
        assert_eq!(receiver.gap_generation(), 2);
        assert_eq!(receiver.next_event().await, Some(GraphUpdateReceiveEvent::Gap(2)));
        assert_eq!(receiver.take_pending_gap(), None);
        assert_eq!(producer.offer_vspc_committed(vspc_update(12)), Ok(GraphUpdateOfferOutcome::GapReported));
        assert_eq!(receiver.wait_for_gap().await, Some(3));

        let metrics = producer.metrics();
        assert_eq!(metrics.current_occupancy, GRAPH_UPDATE_CHANNEL_CAPACITY);
        assert_eq!(metrics.high_water_mark, GRAPH_UPDATE_CHANNEL_CAPACITY);
        assert_eq!(metrics.reported_gap_count, 3);
    }

    #[tokio::test]
    async fn live_waits_for_capacity_and_preserves_channel_order() {
        let (producer, mut receiver) = graph_update_channel();
        producer.publish_post_seal().expect("receiver is open");
        for byte in 0..(GRAPH_UPDATE_CHANNEL_CAPACITY - 1) {
            assert_eq!(producer.offer_vspc_committed(vspc_update(byte as u8)), Ok(GraphUpdateOfferOutcome::Enqueued));
        }

        let live_producer = producer.clone();
        let live = tokio::spawn(async move { live_producer.publish_live().await });
        tokio::task::yield_now().await;
        assert!(!live.is_finished());

        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        assert_eq!(live.await.expect("task joins"), Ok(()));

        for _ in 0..(GRAPH_UPDATE_CHANNEL_CAPACITY - 1) {
            assert!(matches!(receiver.recv_update().await, Some(GraphUpdate::VspcCommitted(_))));
        }
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Live));
    }

    #[tokio::test]
    async fn live_reports_receiver_closed_when_receiver_drops_after_reservation() {
        let (producer, receiver) = graph_update_channel();
        producer.publish_post_seal().expect("receiver is open");

        let reserved = Arc::new(tokio::sync::Barrier::new(2));
        let continue_send = Arc::new(tokio::sync::Barrier::new(2));
        let producer_task = producer.clone();
        let reserved_task = reserved.clone();
        let continue_task = continue_send.clone();
        let live = tokio::spawn(async move {
            producer_task
                .publish_live_after_reservation(async move {
                    reserved_task.wait().await;
                    continue_task.wait().await;
                })
                .await
        });

        reserved.wait().await;
        drop(receiver);
        continue_send.wait().await;

        assert_eq!(live.await.expect("task joins"), Err(GraphUpdateProducerError::ReceiverClosed));
        let metrics = producer.metrics();
        assert_eq!(metrics.current_occupancy, 0);
        assert_eq!(metrics.receiver_closure_count, 1);
    }

    #[tokio::test]
    async fn receiver_closure_is_not_reported_as_a_gap() {
        let (producer, receiver) = graph_update_channel();
        producer.publish_post_seal().expect("receiver is open");
        drop(receiver);

        assert_eq!(producer.offer_block_committed(block_update()), Err(GraphUpdateProducerError::ReceiverClosed));
        assert_eq!(producer.offer_vspc_committed(vspc_update(7)), Err(GraphUpdateProducerError::ReceiverClosed));
        assert_eq!(producer.publish_live().await, Err(GraphUpdateProducerError::ReceiverClosed));

        let metrics = producer.metrics();
        assert_eq!(metrics.current_occupancy, 0);
        assert_eq!(metrics.reported_gap_count, 0);
        assert_eq!(metrics.receiver_closure_count, 1);
    }

    #[tokio::test]
    async fn closing_a_receiver_rejects_new_offers_and_drains_queued_updates() {
        let (producer, mut receiver) = graph_update_channel();
        producer.publish_post_seal().expect("receiver is open");
        assert_eq!(producer.offer_vspc_committed(vspc_update(8)), Ok(GraphUpdateOfferOutcome::Enqueued));

        receiver.close();
        assert_eq!(producer.offer_vspc_committed(vspc_update(9)), Err(GraphUpdateProducerError::ReceiverClosed));
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::VspcCommitted(vspc_update(8))));
        assert_eq!(receiver.recv_update().await, None);

        let metrics = receiver.metrics();
        assert_eq!(metrics.current_occupancy, 0);
        assert_eq!(metrics.reported_gap_count, 0);
        assert_eq!(metrics.receiver_closure_count, 1);
    }

    #[test]
    fn closed_receiver_leaves_a_failed_post_seal_gate_closed() {
        let (producer, receiver) = graph_update_channel();
        drop(receiver);

        assert_eq!(producer.publish_post_seal(), Err(GraphUpdateProducerError::ReceiverClosed));
        assert_eq!(*producer.gate.state.lock().expect("gate mutex is healthy"), GraphUpdateGateState::PreSeal);
        assert_eq!(producer.metrics().receiver_closure_count, 1);
    }

    fn block_update() -> BlockCommitted {
        use kgi_model::{
            block::{BlockColor, BlockCoordinate, CompactId},
            graph_update::ParentCommitted,
        };

        BlockCommitted {
            id: CompactId::new(1).expect("positive compact ID"),
            hash: hash(20),
            coordinate: BlockCoordinate::new(1, 0).expect("positive level"),
            timestamp: 0,
            daa_score: 0,
            selected_parent_index: None,
            direct_parents: Arc::<[ParentCommitted]>::from([]),
            blue_merge_set: Arc::from([]),
            red_merge_set: Arc::from([]),
            color: BlockColor::Gray,
            is_in_vspc: false,
            level_snapshots: Arc::from([]),
        }
    }

    #[tokio::test]
    #[should_panic(expected = "PublishPostSeal requires the PreSeal graph-update gate")]
    async fn duplicate_post_seal_marker_is_an_invariant_failure() {
        let (producer, _receiver) = graph_update_channel();
        producer.publish_post_seal().expect("receiver is open");
        producer.publish_post_seal().expect("invariant panic occurs first");
    }
}
