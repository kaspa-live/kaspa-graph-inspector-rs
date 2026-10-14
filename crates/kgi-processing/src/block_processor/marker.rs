use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use kgi_api_ingress::channel::GraphUpdateProducer;
use tokio::{sync::mpsc, task::JoinHandle};

use super::error::ChildControlError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MarkerHandoffError {
    ReceiverClosed,
    CommandPathClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerCommand {
    PublishPostSeal,
    Live,
}

pub(crate) struct LifecycleMarkerWorker {
    commands: mpsc::UnboundedSender<MarkerCommand>,
    receiver_closed: Arc<AtomicBool>,
    worker: Option<MarkerWorker>,
    join: Option<JoinHandle<()>>,
}

impl LifecycleMarkerWorker {
    pub(crate) fn new(producer: GraphUpdateProducer) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let receiver_closed = Arc::new(AtomicBool::new(false));
        let worker = MarkerWorker { producer, commands: receiver, receiver_closed: receiver_closed.clone() };
        Self { commands, receiver_closed, worker: Some(worker), join: None }
    }

    pub(crate) fn start(&mut self) {
        let worker = self.worker.take().expect("lifecycle-marker worker starts exactly once");
        self.join = Some(tokio::spawn(worker.run()));
    }

    pub(crate) fn publish_post_seal(&self) -> Result<(), MarkerHandoffError> {
        self.send(MarkerCommand::PublishPostSeal)
    }

    pub(crate) fn publish_live(&self) -> Result<(), MarkerHandoffError> {
        self.send(MarkerCommand::Live)
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), ChildControlError> {
        if self.worker.take().is_some() {
            return Ok(());
        }
        drop(self.commands);
        match self.join.take() {
            Some(join) => join.await.map_err(|_| ChildControlError::WorkerJoinFailed),
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

struct MarkerWorker {
    producer: GraphUpdateProducer,
    commands: mpsc::UnboundedReceiver<MarkerCommand>,
    receiver_closed: Arc<AtomicBool>,
}

impl MarkerWorker {
    async fn run(mut self) {
        while let Some(command) = self.commands.recv().await {
            let result = match command {
                MarkerCommand::PublishPostSeal => self.producer.publish_post_seal(),
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
    async fn publishes_markers_in_command_order() {
        let (producer, mut receiver) = graph_update_channel();
        let mut worker = LifecycleMarkerWorker::new(producer);
        worker.start();

        worker.publish_post_seal().expect("marker worker accepts PostSeal");
        worker.publish_live().expect("marker worker accepts Live");
        worker.shutdown().await.expect("marker shutdown");

        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Live));
        assert_eq!(receiver.recv_update().await, None);
    }

    #[tokio::test]
    async fn live_command_does_not_apply_graph_channel_backpressure() {
        let (producer, mut receiver) = graph_update_channel();
        let mut worker = LifecycleMarkerWorker::new(producer.clone());
        worker.start();

        worker.publish_post_seal().expect("marker worker accepts PostSeal");
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        for byte in 0..GRAPH_UPDATE_CHANNEL_CAPACITY {
            assert_eq!(producer.offer_vspc_committed(vspc_update(byte as u8)), Ok(GraphUpdateOfferOutcome::Enqueued));
        }

        worker.publish_live().expect("full graph channel does not block local handoff");
        let shutdown = tokio::spawn(worker.shutdown());
        tokio::task::yield_now().await;
        assert_eq!(producer.metrics().current_occupancy, GRAPH_UPDATE_CHANNEL_CAPACITY);
        drop(producer);

        for _ in 0..GRAPH_UPDATE_CHANNEL_CAPACITY {
            assert!(matches!(receiver.recv_update().await, Some(GraphUpdate::VspcCommitted(_))));
        }
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Live));
        assert_eq!(receiver.recv_update().await, None);
        shutdown.await.expect("marker shutdown task").expect("marker shutdown");
    }

    #[tokio::test]
    async fn shutdown_drains_an_already_accepted_marker() {
        let (producer, mut receiver) = graph_update_channel();
        let mut worker = LifecycleMarkerWorker::new(producer);
        worker.start();

        worker.publish_post_seal().expect("marker worker accepts PostSeal");
        let shutdown = tokio::spawn(worker.shutdown());

        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        assert_eq!(receiver.recv_update().await, None);
        shutdown.await.expect("marker shutdown task").expect("marker shutdown");
    }

    #[tokio::test]
    async fn receiver_closure_ends_marker_delivery_without_a_processing_fault() {
        let (producer, receiver) = graph_update_channel();
        let mut worker = LifecycleMarkerWorker::new(producer);
        worker.start();
        drop(receiver);

        worker.publish_post_seal().expect("local handoff precedes receiver-closure observation");
        worker.commands.closed().await;
        assert_eq!(worker.publish_live(), Err(MarkerHandoffError::ReceiverClosed));
        worker.shutdown().await.expect("marker shutdown");
    }
}
