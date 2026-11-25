use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use kgi_api_ingress::channel::GraphUpdateProducer;
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle};

/// Failure to hand one lifecycle marker to the session-owned worker.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum MarkerHandoffError {
    /// The session graph-update consumer no longer exists.
    #[error("graph-update receiver is closed")]
    ReceiverClosed,

    /// The marker worker stopped accepting lifecycle commands unexpectedly.
    #[error("lifecycle-marker command path is closed")]
    CommandPathClosed,

    /// The marker was requested before launch, more than once, or out of order.
    #[error("lifecycle-marker command is invalid in the current state")]
    InvalidState,
}

/// Failure to join the session-owned marker worker.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum MarkerShutdownError {
    /// The marker task panicked or was cancelled.
    #[error("lifecycle-marker worker could not be joined")]
    WorkerJoinFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerCommand {
    Catchup,
    Live,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerState {
    Inert,
    AwaitingCatchup,
    AwaitingLive,
    Complete,
}

/// Owns serialized Catchup and Live delivery for one processing session.
pub(super) struct LifecycleMarker {
    commands: mpsc::UnboundedSender<MarkerCommand>,
    receiver_closed: Arc<AtomicBool>,
    state: MarkerState,
    task: Option<MarkerTask>,
    join: Option<JoinHandle<()>>,
}

impl LifecycleMarker {
    /// Constructs an inert worker retaining only its session producer.
    pub(super) fn new(producer: GraphUpdateProducer) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let receiver_closed = Arc::new(AtomicBool::new(false));
        let task = MarkerTask::new(producer, receiver, receiver_closed.clone());
        Self { commands, receiver_closed, state: MarkerState::Inert, task: Some(task), join: None }
    }

    /// Launches the worker exactly once with the initialized session tree.
    pub(super) fn start(&mut self) {
        let task = self.task.take().expect("lifecycle-marker worker starts exactly once");
        assert_eq!(self.state, MarkerState::Inert, "lifecycle-marker worker starts only from inert state");
        self.state = MarkerState::AwaitingCatchup;
        self.join = Some(tokio::spawn(task.run()));
    }

    /// Reliably accepts the session's exact-once Catchup command.
    pub(super) fn publish_catchup(&mut self) -> Result<(), MarkerHandoffError> {
        if self.state != MarkerState::AwaitingCatchup {
            return Err(MarkerHandoffError::InvalidState);
        }
        self.send(MarkerCommand::Catchup)?;
        self.state = MarkerState::AwaitingLive;
        Ok(())
    }

    /// Reliably accepts the session's exact-once Live command after Catchup.
    pub(super) fn publish_live(&mut self) -> Result<(), MarkerHandoffError> {
        if self.state != MarkerState::AwaitingLive {
            return Err(MarkerHandoffError::InvalidState);
        }
        self.send(MarkerCommand::Live)?;
        self.state = MarkerState::Complete;
        Ok(())
    }

    /// Consumes the worker, drains accepted commands, and joins its task.
    pub(super) async fn shutdown(mut self) -> Result<(), MarkerShutdownError> {
        if self.task.take().is_some() {
            return Ok(());
        }
        drop(self.commands);
        match self.join.take() {
            Some(join) => join.await.map_err(|_| MarkerShutdownError::WorkerJoinFailed),
            None => Ok(()),
        }
    }

    fn send(&self, command: MarkerCommand) -> Result<(), MarkerHandoffError> {
        self.commands.send(command).map_err(|_| {
            if self.receiver_closed.load(Ordering::Acquire) {
                MarkerHandoffError::ReceiverClosed
            } else {
                MarkerHandoffError::CommandPathClosed
            }
        })
    }
}

struct MarkerTask {
    producer: GraphUpdateProducer,
    commands: mpsc::UnboundedReceiver<MarkerCommand>,
    receiver_closed: Arc<AtomicBool>,
}

impl MarkerTask {
    fn new(producer: GraphUpdateProducer, commands: mpsc::UnboundedReceiver<MarkerCommand>, receiver_closed: Arc<AtomicBool>) -> Self {
        Self { producer, commands, receiver_closed }
    }

    async fn run(mut self) {
        while let Some(command) = self.commands.recv().await {
            let result = match command {
                MarkerCommand::Catchup => self.producer.publish_catchup(),
                MarkerCommand::Live => self.producer.publish_live().await,
            };
            if result.is_err() {
                self.receiver_closed.store(true, Ordering::Release);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kgi_api_ingress::channel::{GRAPH_UPDATE_CHANNEL_CAPACITY, GraphUpdateOfferOutcome, graph_update_channel};
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
    async fn inert_construction_accepts_no_marker_and_starts_no_task() {
        let (producer, mut receiver) = graph_update_channel();
        let mut marker = LifecycleMarker::new(producer);

        assert_eq!(marker.publish_catchup(), Err(MarkerHandoffError::InvalidState));
        assert_eq!(receiver.try_recv_update(), Err(mpsc::error::TryRecvError::Empty));
        marker.shutdown().await.expect("inert marker shutdown");
        assert_eq!(receiver.recv_update().await, None);
    }

    #[tokio::test]
    async fn publishes_catchup_and_live_in_command_order() {
        let (producer, mut receiver) = graph_update_channel();
        let mut marker = LifecycleMarker::new(producer);
        marker.start();

        marker.publish_catchup().expect("marker worker accepts Catchup");
        marker.publish_live().expect("marker worker accepts Live");
        marker.shutdown().await.expect("marker shutdown");

        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Catchup));
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Live));
        assert_eq!(receiver.recv_update().await, None);
    }

    #[tokio::test]
    async fn rejects_duplicate_and_out_of_order_commands() {
        let (producer, _receiver) = graph_update_channel();
        let mut marker = LifecycleMarker::new(producer);
        marker.start();

        assert_eq!(marker.publish_live(), Err(MarkerHandoffError::InvalidState));
        marker.publish_catchup().expect("marker worker accepts Catchup");
        assert_eq!(marker.publish_catchup(), Err(MarkerHandoffError::InvalidState));
        marker.publish_live().expect("marker worker accepts Live");
        assert_eq!(marker.publish_live(), Err(MarkerHandoffError::InvalidState));
        marker.shutdown().await.expect("marker shutdown");
    }

    #[tokio::test]
    async fn live_handoff_does_not_wait_for_graph_channel_capacity() {
        let (producer, mut receiver) = graph_update_channel();
        let mut marker = LifecycleMarker::new(producer.clone());
        marker.start();

        marker.publish_catchup().expect("marker worker accepts Catchup");
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Catchup));
        for byte in 0..GRAPH_UPDATE_CHANNEL_CAPACITY {
            assert_eq!(producer.offer_vspc_committed(vspc_update(byte as u8)), Ok(GraphUpdateOfferOutcome::Enqueued));
        }

        marker.publish_live().expect("full graph channel does not block local handoff");
        let shutdown = tokio::spawn(marker.shutdown());
        tokio::task::yield_now().await;
        assert_eq!(producer.metrics().current_occupancy, GRAPH_UPDATE_CHANNEL_CAPACITY);
        drop(producer);

        for _ in 0..GRAPH_UPDATE_CHANNEL_CAPACITY {
            assert!(matches!(receiver.recv_update().await, Some(GraphUpdate::VspcCommitted(_))));
        }
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Live));
        assert_eq!(receiver.recv_update().await, None);
        shutdown.await.expect("marker shutdown task joins").expect("marker shutdown");
    }

    #[tokio::test]
    async fn shutdown_drains_an_accepted_marker() {
        let (producer, mut receiver) = graph_update_channel();
        let mut marker = LifecycleMarker::new(producer);
        marker.start();

        marker.publish_catchup().expect("marker worker accepts Catchup");
        let shutdown = tokio::spawn(marker.shutdown());

        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Catchup));
        assert_eq!(receiver.recv_update().await, None);
        shutdown.await.expect("marker shutdown task joins").expect("marker shutdown");
    }

    #[tokio::test]
    async fn receiver_closure_ends_delivery_without_a_processing_fault() {
        let (producer, receiver) = graph_update_channel();
        let mut marker = LifecycleMarker::new(producer);
        marker.start();
        drop(receiver);

        marker.publish_catchup().expect("local handoff precedes receiver-closure observation");
        marker.commands.closed().await;
        assert_eq!(marker.publish_live(), Err(MarkerHandoffError::ReceiverClosed));
        marker.shutdown().await.expect("marker shutdown");
    }
}
