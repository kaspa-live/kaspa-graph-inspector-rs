use std::sync::{Arc, Mutex};

use kaspa_notify::{
    error::{Error as NotifyError, Result as NotifyResult},
    notifier::Notify,
};
use kaspa_rpc_core::{Notification, VirtualChainChangedNotification};
use kgi_model::{
    lifecycle::{FaultKind, MalformedVspcNotificationReason, NotificationInputKind, OwnershipFault},
    vspc::VspcChange,
};
use tokio::sync::mpsc::error::TrySendError;

use crate::{
    normalization::{BlockNormalizationError, ResponseNormalizer},
    rpc::{NotificationChannels, NotificationFault},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotificationRouterState {
    Disabled,
    Enabled,
    Retired,
}

#[derive(Debug)]
pub(crate) struct NotificationRouter {
    normalizer: Arc<ResponseNormalizer>,
    inner: Mutex<NotificationRouterInner>,
}

#[derive(Debug)]
struct NotificationRouterInner {
    state: NotificationRouterState,
    destinations: Option<NotificationChannels>,
}

impl NotificationRouter {
    pub(crate) fn new(normalizer: Arc<ResponseNormalizer>) -> Self {
        Self {
            normalizer,
            inner: Mutex::new(NotificationRouterInner { state: NotificationRouterState::Disabled, destinations: None }),
        }
    }

    pub(crate) fn install(&self, channels: NotificationChannels) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.state != NotificationRouterState::Disabled {
            return false;
        }
        inner.destinations = Some(channels);
        true
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> NotificationRouterState {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).state
    }

    pub(crate) fn enable(&self) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.state != NotificationRouterState::Disabled || inner.destinations.is_none() {
            false
        } else {
            inner.state = NotificationRouterState::Enabled;
            true
        }
    }

    pub(crate) fn disable(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.state != NotificationRouterState::Retired {
            inner.state = NotificationRouterState::Disabled;
        }
    }

    pub(crate) fn clear(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.state == NotificationRouterState::Disabled {
            inner.destinations = None;
        }
    }

    pub(crate) fn retire(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.state = NotificationRouterState::Retired;
        inner.destinations = None;
    }

    fn route_block(
        &self,
        inner: &mut NotificationRouterInner,
        notification: kaspa_rpc_core::BlockAddedNotification,
    ) -> NotifyResult<()> {
        let block = match self.normalizer.block_added(Arc::unwrap_or_clone(notification.block)) {
            Ok(block) => block,
            Err(BlockNormalizationError::ScoreOutOfRange(fault)) => {
                return disable_and_report(
                    inner,
                    FaultKind::ScoreOutOfRange(fault),
                    format!("BlockAdded contains an out-of-range {fault:?}"),
                );
            }
            Err(error) => {
                return disable_and_report(
                    inner,
                    FaultKind::NotificationInputInvalid(NotificationInputKind::MalformedBlockAdded),
                    format!("malformed BlockAdded notification: {error:?}"),
                );
            }
        };

        let Some(destinations) = inner.destinations.as_ref() else {
            return Err(NotifyError::ChannelSendError);
        };
        match destinations.blocks().try_send(block) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                disable_and_report(inner, FaultKind::SessionContinuityLost, "BlockAdded destination is full")
            }
            Err(TrySendError::Closed(_)) => disable_and_report(
                inner,
                FaultKind::Ownership(OwnershipFault::SessionDataEndpointLost),
                "BlockAdded destination is closed",
            ),
        }
    }

    fn route_vspc(&self, inner: &mut NotificationRouterInner, notification: VirtualChainChangedNotification) -> NotifyResult<()> {
        let removed = notification.removed_chain_block_hashes;
        let added = notification.added_chain_block_hashes;
        if removed.is_empty() && added.is_empty() {
            return Ok(());
        }
        if !removed.is_empty() && added.is_empty() {
            return disable_and_report(
                inner,
                FaultKind::NotificationInputInvalid(NotificationInputKind::MalformedVspcChange(
                    MalformedVspcNotificationReason::RemovedChainWithoutAddedPath,
                )),
                "VirtualChainChanged removed blocks without an added path",
            );
        }

        let change = VspcChange { removed: Arc::from(removed.as_slice()), added: Arc::from(added.as_slice()) };
        let Some(destinations) = inner.destinations.as_ref() else {
            return Err(NotifyError::ChannelSendError);
        };
        match destinations.vspc().try_send(change) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                disable_and_report(inner, FaultKind::SessionContinuityLost, "VirtualChainChanged destination is full")
            }
            Err(TrySendError::Closed(_)) => disable_and_report(
                inner,
                FaultKind::Ownership(OwnershipFault::SessionDataEndpointLost),
                "VirtualChainChanged destination is closed",
            ),
        }
    }
}

impl Notify<Notification> for NotificationRouter {
    fn notify(&self, notification: Notification) -> NotifyResult<()> {
        let mut inner = self.inner.lock().map_err(|_| NotifyError::General("NotificationRouter mutex is poisoned".into()))?;
        if inner.state != NotificationRouterState::Enabled {
            return Ok(());
        }

        match notification {
            Notification::BlockAdded(notification) => self.route_block(&mut inner, notification),
            Notification::VirtualChainChanged(notification) => self.route_vspc(&mut inner, notification),
            _ => Ok(()),
        }
    }
}

fn disable_and_report(inner: &mut NotificationRouterInner, kind: FaultKind, diagnostic: impl Into<Arc<str>>) -> NotifyResult<()> {
    inner.state = NotificationRouterState::Disabled;
    inner
        .destinations
        .as_ref()
        .ok_or(NotifyError::ChannelSendError)?
        .faults()
        .send(NotificationFault::new(kind, diagnostic))
        .map_err(|_| NotifyError::ChannelSendError)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaspa_consensus_core::BlueWorkType;
    use kaspa_notify::notifier::Notify;
    use kaspa_rpc_core::{
        BlockAddedNotification, Notification, RpcBlock, RpcBlockVerboseData, RpcHeader, VirtualChainChangedNotification,
    };
    use kgi_model::{
        block::{BlockHash, MAX_DAA_SCORE, ValidatedNodeBlock},
        lifecycle::{FaultKind, MalformedVspcNotificationReason, NotificationInputKind, OwnershipFault, ScoreRangeFault},
        vspc::VspcChange,
    };
    use tokio::sync::mpsc;

    use crate::{
        normalization::ResponseNormalizer,
        rpc::{NotificationChannels, NotificationFault},
    };

    use super::{NotificationRouter, NotificationRouterState};

    #[test]
    fn disabled_and_retired_router_drop_without_consuming_capacity() {
        let (router, mut blocks, mut vspc, mut faults) = router(1);

        router.notify(block_added(rpc_block(hash(1), vec![hash(2)]))).expect("disabled callback");
        router.notify(vspc_changed(vec![], vec![hash(3)])).expect("disabled callback");
        assert!(blocks.try_recv().is_err());
        assert!(vspc.try_recv().is_err());
        assert!(faults.try_recv().is_err());

        assert!(router.enable());
        router.retire();
        assert!(!router.enable());
        router.notify(block_added(rpc_block(hash(4), vec![hash(2)]))).expect("retired callback");
        assert_eq!(router.state(), NotificationRouterState::Retired);
        assert!(blocks.try_recv().is_err());
    }

    #[test]
    fn enabled_router_normalizes_block_added() {
        let (router, mut blocks, _, mut faults) = router(1);
        assert!(router.enable());

        router.notify(block_added(rpc_block(hash(1), vec![hash(2)]))).expect("valid callback");

        let block = blocks.try_recv().expect("normalized block");
        assert_eq!(block.hash, hash(1));
        assert_eq!(block.selected_parent, hash(2));
        assert!(faults.try_recv().is_err());
        assert_eq!(router.state(), NotificationRouterState::Enabled);
    }

    #[test]
    fn malformed_block_disables_both_streams_and_reports_once() {
        let (router, mut blocks, mut vspc, mut faults) = router(2);
        assert!(router.enable());
        let mut malformed = rpc_block(hash(1), vec![hash(2)]);
        malformed.verbose_data = None;

        router.notify(block_added(malformed)).expect("malformed callback is classified");
        router.notify(vspc_changed(vec![], vec![hash(3)])).expect("disabled callback");

        assert_eq!(router.state(), NotificationRouterState::Disabled);
        assert_eq!(faults.try_recv().expect("fault").kind(), malformed_block_fault());
        assert!(faults.try_recv().is_err());
        assert!(blocks.try_recv().is_err());
        assert!(vspc.try_recv().is_err());
    }

    #[test]
    fn block_score_fault_retains_its_classification() {
        let (router, mut blocks, _, mut faults) = router(1);
        assert!(router.enable());
        let mut block = rpc_block(hash(1), vec![hash(2)]);
        block.header.daa_score = MAX_DAA_SCORE + 1;

        router.notify(block_added(block)).expect("range callback is classified");

        assert_eq!(faults.try_recv().expect("fault").kind(), FaultKind::ScoreOutOfRange(ScoreRangeFault::DaaScore));
        assert!(blocks.try_recv().is_err());
        assert_eq!(router.state(), NotificationRouterState::Disabled);
    }

    #[test]
    fn empty_vspc_is_a_noop_and_malformed_shape_disables_both_streams() {
        let (router, mut blocks, mut vspc, mut faults) = router(2);
        assert!(router.enable());

        router.notify(vspc_changed(vec![], vec![])).expect("empty no-op");
        assert!(vspc.try_recv().is_err());
        assert!(faults.try_recv().is_err());

        router.notify(vspc_changed(vec![hash(1)], vec![])).expect("malformed callback is classified");
        router.notify(block_added(rpc_block(hash(2), vec![hash(3)]))).expect("disabled callback");

        assert_eq!(
            faults.try_recv().expect("fault").kind(),
            FaultKind::NotificationInputInvalid(NotificationInputKind::MalformedVspcChange(
                MalformedVspcNotificationReason::RemovedChainWithoutAddedPath
            ))
        );
        assert!(blocks.try_recv().is_err());
        assert!(vspc.try_recv().is_err());
        assert_eq!(router.state(), NotificationRouterState::Disabled);
    }

    #[test]
    fn vspc_preserves_order_duplicates_and_intersections() {
        let (router, _, mut vspc, mut faults) = router(1);
        assert!(router.enable());
        let removed = vec![hash(1), hash(2), hash(2)];
        let added = vec![hash(2), hash(1), hash(3), hash(3)];

        router.notify(vspc_changed(removed.clone(), added.clone())).expect("valid callback");

        let change = vspc.try_recv().expect("routed change");
        assert_eq!(change.removed.as_ref(), removed);
        assert_eq!(change.added.as_ref(), added);
        assert!(faults.try_recv().is_err());
    }

    #[test]
    fn either_full_destination_disables_both_streams() {
        let (block_router, mut blocks, mut block_vspc, mut block_faults) = router(1);
        blocks.try_recv().expect_err("empty channel");
        block_router.enable();
        let block_sender = block_router.inner.lock().expect("router").destinations.as_ref().expect("destinations").blocks().clone();
        block_sender.try_send(validated_block(hash(9))).expect("fill block channel");
        block_router.notify(block_added(rpc_block(hash(1), vec![hash(2)]))).expect("full channel is classified");
        block_router.notify(vspc_changed(vec![], vec![hash(3)])).expect("disabled VSPC callback");
        assert_eq!(block_faults.try_recv().expect("fault").kind(), FaultKind::SessionContinuityLost);
        assert!(block_vspc.try_recv().is_err());

        let (vspc_router, mut later_blocks, mut filled_vspc, mut vspc_faults) = router(1);
        vspc_router.enable();
        let vspc_sender = vspc_router.inner.lock().expect("router").destinations.as_ref().expect("destinations").vspc().clone();
        vspc_sender.try_send(VspcChange { removed: Arc::from([]), added: Arc::from([hash(8)]) }).expect("fill VSPC channel");
        vspc_router.notify(vspc_changed(vec![], vec![hash(3)])).expect("full channel is classified");
        vspc_router.notify(block_added(rpc_block(hash(4), vec![hash(5)]))).expect("disabled block callback");
        assert_eq!(vspc_faults.try_recv().expect("fault").kind(), FaultKind::SessionContinuityLost);
        assert_eq!(filled_vspc.try_recv().expect("original queued change").added.as_ref(), &[hash(8)]);
        assert!(later_blocks.try_recv().is_err());
    }

    #[test]
    fn closed_enabled_destination_reports_endpoint_loss() {
        let (router, blocks, _, mut faults) = router(1);
        drop(blocks);
        assert!(router.enable());

        router.notify(block_added(rpc_block(hash(1), vec![hash(2)]))).expect("closed channel is classified");

        let fault = faults.try_recv().expect("fault");
        assert_eq!(fault.kind(), FaultKind::Ownership(OwnershipFault::SessionDataEndpointLost));
        assert!(fault.diagnostic().contains("closed"));
        assert_eq!(router.state(), NotificationRouterState::Disabled);
    }

    fn router(
        capacity: usize,
    ) -> (NotificationRouter, mpsc::Receiver<ValidatedNodeBlock>, mpsc::Receiver<VspcChange>, mpsc::UnboundedReceiver<NotificationFault>)
    {
        let (block_tx, block_rx) = mpsc::channel(capacity);
        let (vspc_tx, vspc_rx) = mpsc::channel(capacity);
        let (fault_tx, fault_rx) = mpsc::unbounded_channel();
        let router = NotificationRouter::new(Arc::new(ResponseNormalizer::new(hash(0))));
        assert!(router.install(NotificationChannels::new(block_tx, vspc_tx, fault_tx)));
        (router, block_rx, vspc_rx, fault_rx)
    }

    fn block_added(block: RpcBlock) -> Notification {
        Notification::BlockAdded(BlockAddedNotification { block: Arc::new(block) })
    }

    fn vspc_changed(removed: Vec<BlockHash>, added: Vec<BlockHash>) -> Notification {
        Notification::VirtualChainChanged(VirtualChainChangedNotification {
            removed_chain_block_hashes: Arc::new(removed),
            added_chain_block_hashes: Arc::new(added),
            accepted_transaction_ids: Arc::new(Vec::new()),
        })
    }

    fn malformed_block_fault() -> FaultKind {
        FaultKind::NotificationInputInvalid(NotificationInputKind::MalformedBlockAdded)
    }

    fn validated_block(hash_value: BlockHash) -> ValidatedNodeBlock {
        ValidatedNodeBlock {
            hash: hash_value,
            selected_parent: hash(0),
            direct_parents: Vec::new(),
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: 0,
            daa_score: 0,
            blue_score: 0,
            blue_work: BlueWorkType::from(0_u64),
        }
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn rpc_block(hash_value: BlockHash, direct_parents: Vec<BlockHash>) -> RpcBlock {
        let selected_parent = direct_parents.first().copied().unwrap_or_else(|| hash(99));
        RpcBlock {
            header: RpcHeader {
                hash: hash_value,
                version: 0,
                parents_by_level: vec![direct_parents],
                hash_merkle_root: hash(10),
                accepted_id_merkle_root: hash(11),
                utxo_commitment: hash(12),
                timestamp: 10,
                bits: 0,
                nonce: 0,
                daa_score: 11,
                blue_work: BlueWorkType::from(13_u64),
                blue_score: 12,
                pruning_point: hash(14),
            },
            transactions: Vec::new(),
            verbose_data: Some(RpcBlockVerboseData {
                hash: hash_value,
                difficulty: 1.0,
                selected_parent_hash: selected_parent,
                transaction_ids: Vec::new(),
                is_header_only: false,
                blue_score: 12,
                children_hashes: Vec::new(),
                merge_set_blues_hashes: Vec::new(),
                merge_set_reds_hashes: Vec::new(),
                is_chain_block: true,
            }),
        }
    }
}
