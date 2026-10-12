use kgi_api_ingress::channel::GraphUpdateProducer;
use tokio::sync::mpsc;

use super::error::ChildControlError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerCommand {
    PublishPostSeal,
    Live,
}

pub(crate) struct LifecycleMarkerWorker {
    commands: mpsc::UnboundedSender<MarkerCommand>,
}

impl LifecycleMarkerWorker {
    pub(crate) fn start(producer: GraphUpdateProducer) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(producer, receiver));
        Self { commands }
    }

    pub(crate) fn publish_post_seal(&self) -> Result<(), ChildControlError> {
        self.send(MarkerCommand::PublishPostSeal)
    }

    pub(crate) fn publish_live(&self) -> Result<(), ChildControlError> {
        self.send(MarkerCommand::Live)
    }

    pub(crate) fn close(self) {
        drop(self.commands);
    }

    fn send(&self, command: MarkerCommand) -> Result<(), ChildControlError> {
        self.commands.send(command).map_err(|_| ChildControlError::CommandPathClosed)
    }
}

async fn run(producer: GraphUpdateProducer, mut commands: mpsc::UnboundedReceiver<MarkerCommand>) {
    while let Some(command) = commands.recv().await {
        let result = match command {
            MarkerCommand::PublishPostSeal => producer.publish_post_seal(),
            MarkerCommand::Live => producer.publish_live().await,
        };
        if result.is_err() {
            return;
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
        let worker = LifecycleMarkerWorker::start(producer);

        worker.publish_post_seal().expect("marker worker accepts PostSeal");
        worker.publish_live().expect("marker worker accepts Live");
        worker.close();

        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Live));
        assert_eq!(receiver.recv_update().await, None);
    }

    #[tokio::test]
    async fn live_command_does_not_apply_graph_channel_backpressure() {
        let (producer, mut receiver) = graph_update_channel();
        let worker = LifecycleMarkerWorker::start(producer.clone());

        worker.publish_post_seal().expect("marker worker accepts PostSeal");
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        for byte in 0..GRAPH_UPDATE_CHANNEL_CAPACITY {
            assert_eq!(producer.offer_vspc_committed(vspc_update(byte as u8)), Ok(GraphUpdateOfferOutcome::Enqueued));
        }

        worker.publish_live().expect("full graph channel does not block local handoff");
        worker.close();
        tokio::task::yield_now().await;
        assert_eq!(producer.metrics().current_occupancy, GRAPH_UPDATE_CHANNEL_CAPACITY);
        drop(producer);

        for _ in 0..GRAPH_UPDATE_CHANNEL_CAPACITY {
            assert!(matches!(receiver.recv_update().await, Some(GraphUpdate::VspcCommitted(_))));
        }
        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::Live));
        assert_eq!(receiver.recv_update().await, None);
    }

    #[tokio::test]
    async fn close_drains_an_already_accepted_marker() {
        let (producer, mut receiver) = graph_update_channel();
        let worker = LifecycleMarkerWorker::start(producer);

        worker.publish_post_seal().expect("marker worker accepts PostSeal");
        worker.close();

        assert_eq!(receiver.recv_update().await, Some(GraphUpdate::PublishPostSeal));
        assert_eq!(receiver.recv_update().await, None);
    }

    #[tokio::test]
    async fn receiver_closure_ends_marker_delivery_without_a_processing_fault() {
        let (producer, receiver) = graph_update_channel();
        let worker = LifecycleMarkerWorker::start(producer);
        drop(receiver);

        worker.publish_post_seal().expect("local handoff precedes receiver-closure observation");
        worker.commands.closed().await;
        assert_eq!(worker.publish_live(), Err(ChildControlError::CommandPathClosed));
    }
}
