use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use kgi_model::{
    block::{BlockHash, ValidatedNodeBlock},
    lifecycle::{Component, OwnershipFault},
};
use kgi_node::rpc::ValidatedRpcClient;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use super::{
    BlockResolutionEvent, PROCESSING_DATA_CHANNEL_CAPACITY,
    dependency::{DependencyResolver, DependencyResolverMetrics, DependencyResolverMetricsSnapshot, DependencyRpc},
    error::{ChildControlError, ChildFault, ChildFaultKind, SessionDataPath},
};

pub(crate) const MAX_ORPHAN_BLOCKS: usize = 1024;
pub(crate) const ORPHAN_RESOLUTION_THRESHOLD: usize = 256;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OrphanManagerMetricsSnapshot {
    pub current_occupancy: usize,
    pub high_water_mark: usize,
    pub resolution_pending: usize,
    pub threshold_crossings: u64,
}

#[derive(Clone, Debug, Default)]
struct OrphanManagerMetrics {
    inner: Arc<Mutex<OrphanManagerMetricsSnapshot>>,
}

impl OrphanManagerMetrics {
    fn update(&self, topology: &OrphanTopology, threshold_crossed: bool) {
        let mut inner = self.inner.lock().expect("orphan metrics mutex poisoned");
        inner.current_occupancy = topology.orphans.len();
        inner.high_water_mark = inner.high_water_mark.max(inner.current_occupancy);
        inner.resolution_pending = topology.resolution_pending.len();
        if threshold_crossed {
            inner.threshold_crossings = inner.threshold_crossings.checked_add(1).expect("orphan threshold counter overflow");
        }
    }

    fn reset_current(&self) {
        let mut inner = self.inner.lock().expect("orphan metrics mutex poisoned");
        inner.current_occupancy = 0;
        inner.resolution_pending = 0;
    }

    fn snapshot(&self) -> OrphanManagerMetricsSnapshot {
        *self.inner.lock().expect("orphan metrics mutex poisoned")
    }
}

enum OrphanCommand {
    Shutdown(oneshot::Sender<Result<(), ChildControlError>>),
}

enum OrphanMessage {
    AddOrphan { block: ValidatedNodeBlock, missing: Arc<[BlockHash]> },
    BlockPersisted(BlockHash),
}

pub(crate) struct OrphanManager<R: DependencyRpc = ValidatedRpcClient> {
    commands: mpsc::UnboundedSender<OrphanCommand>,
    messages: mpsc::Sender<OrphanMessage>,
    metrics: OrphanManagerMetrics,
    resolver_metrics: Arc<DependencyResolverMetrics>,
    worker: Option<OrphanManagerWorker<R>>,
    join: Option<JoinHandle<()>>,
}

impl<R: DependencyRpc> OrphanManager<R> {
    pub(crate) fn new(rpc: Arc<R>, resolution: mpsc::Sender<BlockResolutionEvent>, faults: mpsc::UnboundedSender<ChildFault>) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (message_tx, message_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (resolver_fault_tx, resolver_fault_rx) = mpsc::unbounded_channel();
        let metrics = OrphanManagerMetrics::default();
        let resolver = DependencyResolver::new(rpc, resolution.clone(), resolver_fault_tx);
        let resolver_metrics = resolver.metrics_source();
        let worker = OrphanManagerWorker {
            commands: command_rx,
            messages: message_rx,
            resolver: Some(resolver),
            resolution,
            resolver_faults: resolver_fault_rx,
            faults,
            metrics: metrics.clone(),
            topology: OrphanTopology::default(),
            state: OrphanManagerState::Active,
            messages_open: true,
            resolver_faults_open: true,
        };
        Self { commands: command_tx, messages: message_tx, metrics, resolver_metrics, worker: Some(worker), join: None }
    }

    pub(crate) fn start(&mut self) {
        let mut worker = self.worker.take().expect("OrphanManager starts exactly once");
        worker.resolver.as_mut().expect("inert OrphanManager owns its resolver").start();
        self.join = Some(tokio::spawn(worker.run()));
    }

    pub(crate) fn add_orphan(
        &self,
        block: ValidatedNodeBlock,
        missing: Arc<[BlockHash]>,
    ) -> Result<(), mpsc::error::TrySendError<()>> {
        self.messages.try_send(OrphanMessage::AddOrphan { block, missing }).map_err(map_try_send_error)
    }

    pub(crate) fn block_persisted(&self, hash: BlockHash) -> Result<(), mpsc::error::TrySendError<()>> {
        self.messages.try_send(OrphanMessage::BlockPersisted(hash)).map_err(map_try_send_error)
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), ChildControlError> {
        if self.worker.take().is_some() {
            return Ok(());
        }
        let (completion, acknowledgement) = oneshot::channel();
        let barrier = match self.commands.send(OrphanCommand::Shutdown(completion)) {
            Ok(()) => acknowledgement.await.map_err(|_| ChildControlError::AcknowledgementLost)?,
            Err(_) => Err(ChildControlError::CommandPathClosed),
        };
        let joined = match self.join.take() {
            Some(join) => join.await.map_err(|_| ChildControlError::WorkerJoinFailed),
            None => Ok(()),
        };
        joined?;
        barrier
    }

    pub(crate) fn metrics(&self) -> OrphanManagerMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub(crate) fn resolver_metrics(&self) -> DependencyResolverMetricsSnapshot {
        self.resolver_metrics.snapshot()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrphanManagerState {
    Active,
    Faulted,
}

struct OrphanManagerWorker<R: DependencyRpc> {
    commands: mpsc::UnboundedReceiver<OrphanCommand>,
    messages: mpsc::Receiver<OrphanMessage>,
    resolver: Option<DependencyResolver<R>>,
    resolution: mpsc::Sender<BlockResolutionEvent>,
    resolver_faults: mpsc::UnboundedReceiver<ChildFault>,
    faults: mpsc::UnboundedSender<ChildFault>,
    metrics: OrphanManagerMetrics,
    topology: OrphanTopology,
    state: OrphanManagerState,
    messages_open: bool,
    resolver_faults_open: bool,
}

impl<R: DependencyRpc> OrphanManagerWorker<R> {
    async fn run(mut self) {
        loop {
            while let Ok(command) = self.commands.try_recv() {
                if self.handle_command(command).await {
                    return;
                }
            }

            tokio::select! {
                biased;
                command = self.commands.recv() => {
                    match command {
                        Some(command) => {
                            if self.handle_command(command).await {
                                return;
                            }
                        }
                        None => return,
                    }
                }
                fault = self.resolver_faults.recv(), if self.resolver_faults_open => {
                    match fault {
                        Some(fault) => self.enter_fault(fault).await,
                        None => {
                            self.resolver_faults_open = false;
                            self.enter_own_fault(ChildFaultKind::Ownership(OwnershipFault::UnexpectedWorkerTermination)).await;
                        }
                    }
                }
                message = self.messages.recv(), if self.state == OrphanManagerState::Active && self.messages_open => {
                    match message {
                        Some(message) => self.handle_message(message).await,
                        None => {
                            self.messages_open = false;
                            self.enter_own_fault(ChildFaultKind::SessionDataEndpointLost(SessionDataPath::OrphanInput)).await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, command: OrphanCommand) -> bool {
        match command {
            OrphanCommand::Shutdown(completion) => {
                self.state = OrphanManagerState::Faulted;
                self.clear_state();
                self.drain_messages();
                let result = match self.resolver.take() {
                    Some(resolver) => resolver.shutdown().await,
                    None => Ok(()),
                };
                let _ = completion.send(result);
                true
            }
        }
    }

    async fn handle_message(&mut self, message: OrphanMessage) {
        let result = match message {
            OrphanMessage::AddOrphan { block, missing } => self.topology.add_orphan(block, &missing),
            OrphanMessage::BlockPersisted(hash) => Ok(self.topology.block_persisted(hash)),
        };

        let mutation = match result {
            Ok(mutation) => mutation,
            Err(TopologyError::CapacityExhausted) => {
                self.enter_own_fault(ChildFaultKind::OrphanCapacityExhausted).await;
                return;
            }
            Err(TopologyError::EmptyMissingSet) => {
                self.enter_own_fault(ChildFaultKind::InvalidProtocol(Arc::from(
                    "orphan admission requires at least one missing reference",
                )))
                .await;
                return;
            }
        };

        self.metrics.update(&self.topology, mutation.threshold_crossed);
        for hash in mutation.cancel {
            if !self.cancel_resolution(hash).await {
                return;
            }
        }
        for block in mutation.ready {
            match self.resolution.try_send(BlockResolutionEvent::OrphanReady(block)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.enter_own_fault(ChildFaultKind::SessionDataFull(SessionDataPath::Resolution)).await;
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.enter_own_fault(ChildFaultKind::SessionDataEndpointLost(SessionDataPath::Resolution)).await;
                    return;
                }
            }
        }
        for hash in self.topology.take_resolution_candidates() {
            self.metrics.update(&self.topology, false);
            if !self.request_resolution(hash).await {
                return;
            }
        }
    }

    async fn request_resolution(&mut self, hash: BlockHash) -> bool {
        let Some(resolver) = self.resolver.as_ref() else {
            return false;
        };
        match resolver.resolve(hash) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.enter_own_fault(ChildFaultKind::SessionDataFull(SessionDataPath::ResolverWork)).await;
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.enter_own_fault(ChildFaultKind::SessionDataEndpointLost(SessionDataPath::ResolverWork)).await;
                false
            }
        }
    }

    async fn cancel_resolution(&mut self, hash: BlockHash) -> bool {
        let Some(resolver) = self.resolver.as_ref() else {
            return false;
        };
        match resolver.cancel(hash) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.enter_own_fault(ChildFaultKind::SessionDataFull(SessionDataPath::ResolverWork)).await;
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.enter_own_fault(ChildFaultKind::SessionDataEndpointLost(SessionDataPath::ResolverWork)).await;
                false
            }
        }
    }

    async fn enter_fault(&mut self, fault: ChildFault) {
        if self.state == OrphanManagerState::Faulted {
            return;
        }
        self.state = OrphanManagerState::Faulted;
        self.report_fault(fault);
        if let Some(resolver) = self.resolver.take()
            && let Err(error) = resolver.shutdown().await
        {
            self.report_fault(error.into());
        }
    }

    async fn enter_own_fault(&mut self, kind: ChildFaultKind) {
        self.enter_fault(ChildFault::new(Component::OrphanManager, kind)).await;
    }

    fn clear_state(&mut self) {
        self.topology = OrphanTopology::default();
        self.metrics.reset_current();
    }

    fn drain_messages(&mut self) {
        while self.messages.try_recv().is_ok() {}
    }

    fn report_fault(&self, fault: ChildFault) {
        let _ = self.faults.send(fault);
    }
}

fn map_try_send_error<T>(error: mpsc::error::TrySendError<T>) -> mpsc::error::TrySendError<()> {
    match error {
        mpsc::error::TrySendError::Full(_) => mpsc::error::TrySendError::Full(()),
        mpsc::error::TrySendError::Closed(_) => mpsc::error::TrySendError::Closed(()),
    }
}

struct OrphanEntry {
    block: ValidatedNodeBlock,
    missing: HashSet<BlockHash>,
}

#[derive(Default)]
struct OrphanTopology {
    orphans: HashMap<BlockHash, OrphanEntry>,
    dependents: HashMap<BlockHash, HashSet<BlockHash>>,
    resolution_pending: HashSet<BlockHash>,
    resolution_active: bool,
}

#[derive(Debug, Eq, PartialEq)]
enum TopologyError {
    CapacityExhausted,
    EmptyMissingSet,
}

#[derive(Default)]
struct TopologyMutation {
    ready: Vec<ValidatedNodeBlock>,
    cancel: Vec<BlockHash>,
    threshold_crossed: bool,
}

impl OrphanTopology {
    fn add_orphan(&mut self, block: ValidatedNodeBlock, missing: &[BlockHash]) -> Result<TopologyMutation, TopologyError> {
        if self.orphans.contains_key(&block.hash) {
            return Ok(TopologyMutation::default());
        }
        if self.orphans.len() == MAX_ORPHAN_BLOCKS {
            return Err(TopologyError::CapacityExhausted);
        }

        let missing: HashSet<_> = missing.iter().copied().collect();
        if missing.is_empty() {
            return Err(TopologyError::EmptyMissingSet);
        }
        let block_hash = block.hash;
        for dependency in &missing {
            self.dependents.entry(*dependency).or_default().insert(block_hash);
        }
        self.orphans.insert(block_hash, OrphanEntry { block, missing });

        let mut mutation = TopologyMutation::default();
        if self.resolution_pending.remove(&block_hash) {
            mutation.cancel.push(block_hash);
        }
        mutation.cancel.extend(self.remove_unneeded_pending());
        if !self.resolution_active && self.orphans.len() >= ORPHAN_RESOLUTION_THRESHOLD {
            self.resolution_active = true;
            mutation.threshold_crossed = true;
        }
        Ok(mutation)
    }

    fn block_persisted(&mut self, hash: BlockHash) -> TopologyMutation {
        let mut mutation = TopologyMutation::default();
        if self.resolution_pending.remove(&hash) {
            mutation.cancel.push(hash);
        }
        self.remove_orphan(hash);

        let mut affected: Vec<_> = self.dependents.remove(&hash).unwrap_or_default().into_iter().collect();
        affected.sort_unstable();
        for orphan_hash in affected {
            let Some(entry) = self.orphans.get_mut(&orphan_hash) else {
                continue;
            };
            entry.missing.remove(&hash);
            if entry.missing.is_empty() {
                let entry = self.orphans.remove(&orphan_hash).expect("ready orphan remains stored");
                mutation.ready.push(entry.block);
            }
        }

        mutation.cancel.extend(self.remove_unneeded_pending());
        mutation.cancel.sort_unstable();
        mutation.cancel.dedup();
        if self.orphans.is_empty() {
            self.resolution_active = false;
        }
        mutation
    }

    fn remove_orphan(&mut self, hash: BlockHash) {
        let Some(entry) = self.orphans.remove(&hash) else {
            return;
        };
        for dependency in entry.missing {
            let remove_index = if let Some(dependents) = self.dependents.get_mut(&dependency) {
                dependents.remove(&hash);
                dependents.is_empty()
            } else {
                false
            };
            if remove_index {
                self.dependents.remove(&dependency);
            }
        }
    }

    fn remove_unneeded_pending(&mut self) -> Vec<BlockHash> {
        let mut unneeded: Vec<_> =
            self.resolution_pending.iter().filter(|hash| !self.dependents.contains_key(hash)).copied().collect();
        unneeded.sort_unstable();
        for hash in &unneeded {
            self.resolution_pending.remove(hash);
        }
        unneeded
    }

    fn take_resolution_candidates(&mut self) -> Vec<BlockHash> {
        if !self.resolution_active {
            return Vec::new();
        }
        // Exact ranking traverses each candidate's reachable orphan subgraph.
        // The orphan-count bound and pending-set exclusion keep this small for
        // expected sparse topology, but total reference edges are not bounded
        // here. If profiling exposes this as a hotspot, preserve the same
        // distinct-descendant ranking with stable orphan indices and a reusable
        // visitation bitmap instead of allocating a HashSet per candidate.
        let mut candidates: Vec<_> = self
            .dependents
            .keys()
            .filter(|hash| !self.orphans.contains_key(hash) && !self.resolution_pending.contains(hash))
            .map(|hash| (*hash, self.transitive_impact(*hash)))
            .collect();
        candidates.sort_unstable_by(|(left_hash, left_impact), (right_hash, right_impact)| {
            right_impact.cmp(left_impact).then_with(|| left_hash.cmp(right_hash))
        });
        let hashes: Vec<_> = candidates.into_iter().map(|(hash, _)| hash).collect();
        self.resolution_pending.extend(hashes.iter().copied());
        hashes
    }

    fn transitive_impact(&self, dependency: BlockHash) -> usize {
        let mut visited = HashSet::new();
        let mut stack = vec![dependency];
        while let Some(hash) = stack.pop() {
            let Some(children) = self.dependents.get(&hash) else {
                continue;
            };
            for child in children {
                if visited.insert(*child) {
                    stack.push(*child);
                }
            }
        }
        visited.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use kgi_model::block::BlueWork;
    use tokio::sync::Semaphore;

    use super::*;

    fn hash(value: usize) -> BlockHash {
        let bytes = value.to_le_bytes();
        let mut hash = [0_u8; 32];
        hash[..bytes.len()].copy_from_slice(&bytes);
        BlockHash::from_bytes(hash)
    }

    fn block(value: usize) -> ValidatedNodeBlock {
        ValidatedNodeBlock {
            hash: hash(value),
            selected_parent: hash(value + 10_000),
            direct_parents: Vec::new(),
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: value as u64,
            daa_score: value as u64,
            blue_score: value as u64,
            blue_work: BlueWork::from_u64(value as u64),
        }
    }

    fn block_with_hash(hash: BlockHash) -> ValidatedNodeBlock {
        ValidatedNodeBlock {
            hash,
            selected_parent: BlockHash::from_bytes([255; 32]),
            direct_parents: Vec::new(),
            blue_merge_set: Vec::new(),
            red_merge_set: Vec::new(),
            timestamp: 1,
            daa_score: 1,
            blue_score: 1,
            blue_work: BlueWork::from_u64(1),
        }
    }

    #[derive(Clone)]
    struct GatedRpc {
        started: mpsc::UnboundedSender<BlockHash>,
        releases: Arc<Semaphore>,
    }

    impl DependencyRpc for GatedRpc {
        async fn full_block(&self, hash: BlockHash) -> Result<ValidatedNodeBlock, kgi_node::error::NodeError> {
            let _ = self.started.send(hash);
            self.releases.acquire().await.expect("test release semaphore remains open").forget();
            Ok(block_with_hash(hash))
        }
    }

    struct ManagerFixture {
        manager: OrphanManager<GatedRpc>,
        resolution: mpsc::Receiver<BlockResolutionEvent>,
        faults: mpsc::UnboundedReceiver<ChildFault>,
        started: mpsc::UnboundedReceiver<BlockHash>,
        releases: Arc<Semaphore>,
        weak_rpc: Weak<GatedRpc>,
    }

    fn manager_fixture() -> ManagerFixture {
        let (resolution_tx, resolution_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (fault_tx, fault_rx) = mpsc::unbounded_channel();
        let (started_tx, started_rx) = mpsc::unbounded_channel();
        let releases = Arc::new(Semaphore::new(0));
        let rpc = Arc::new(GatedRpc { started: started_tx, releases: releases.clone() });
        let weak_rpc = Arc::downgrade(&rpc);
        let mut manager = OrphanManager::new(rpc, resolution_tx, fault_tx);
        manager.start();
        ManagerFixture { manager, resolution: resolution_rx, faults: fault_rx, started: started_rx, releases, weak_rpc }
    }

    #[test]
    fn persisted_dependencies_release_each_child_once_in_induced_topological_order() {
        let mut topology = OrphanTopology::default();
        topology.add_orphan(block(2), &[hash(1)]).expect("parent orphan");
        topology.add_orphan(block(3), &[hash(2)]).expect("child orphan");
        topology.add_orphan(block(4), &[hash(2)]).expect("second child orphan");

        let first = topology.block_persisted(hash(1));
        assert_eq!(first.ready.iter().map(|block| block.hash).collect::<Vec<_>>(), vec![hash(2)]);
        assert_eq!(topology.orphans.len(), 2);

        let second = topology.block_persisted(hash(2));
        assert_eq!(second.ready.iter().map(|block| block.hash).collect::<Vec<_>>(), vec![hash(3), hash(4)]);
        assert!(topology.orphans.is_empty());

        assert!(topology.block_persisted(hash(2)).ready.is_empty());
    }

    #[test]
    fn threshold_activates_at_exactly_one_quarter_capacity_and_stays_active() {
        let mut topology = OrphanTopology::default();
        let dependency = hash(20_000);
        for value in 0..ORPHAN_RESOLUTION_THRESHOLD - 1 {
            let mutation = topology.add_orphan(block(value), &[dependency]).expect("below-threshold orphan");
            assert!(!mutation.threshold_crossed);
        }
        assert!(topology.take_resolution_candidates().is_empty());

        let mutation = topology.add_orphan(block(ORPHAN_RESOLUTION_THRESHOLD - 1), &[dependency]).expect("threshold orphan");
        assert!(mutation.threshold_crossed);
        assert_eq!(topology.take_resolution_candidates(), vec![dependency]);

        topology.block_persisted(dependency);
        assert!(topology.orphans.is_empty());
        assert!(!topology.resolution_active);
    }

    #[test]
    fn frontier_selection_maximizes_transitive_release_with_hash_tiebreaking() {
        let mut topology = OrphanTopology::default();
        let high_impact = hash(100);
        let low_impact = hash(101);
        topology.add_orphan(block(1), &[high_impact]).expect("root orphan");
        topology.add_orphan(block(2), &[hash(1)]).expect("first descendant");
        topology.add_orphan(block(3), &[hash(1)]).expect("second descendant");
        topology.add_orphan(block(4), &[low_impact]).expect("independent orphan");
        topology.resolution_active = true;

        assert_eq!(topology.take_resolution_candidates(), vec![high_impact, low_impact]);
    }

    #[test]
    fn resolved_body_remains_pending_until_orphan_admission_and_then_cancels() {
        let mut topology = OrphanTopology::default();
        let requested = hash(100);
        topology.add_orphan(block(1), &[requested]).expect("blocked orphan");
        topology.resolution_active = true;
        assert_eq!(topology.take_resolution_candidates(), vec![requested]);

        assert!(topology.resolution_pending.contains(&requested));
        let mutation = topology.add_orphan(block(100), &[hash(99)]).expect("resolved block is itself orphaned");
        assert_eq!(mutation.cancel, vec![requested]);
        assert!(!topology.resolution_pending.contains(&requested));
        assert_eq!(topology.take_resolution_candidates(), vec![hash(99)]);
    }

    #[test]
    fn capacity_rejects_a_new_hash_without_mutation_but_accepts_a_duplicate() {
        let mut topology = OrphanTopology::default();
        let dependency = hash(20_000);
        for value in 0..MAX_ORPHAN_BLOCKS {
            topology.add_orphan(block(value), &[dependency]).expect("orphan within capacity");
        }
        let pending_before = topology.resolution_pending.clone();
        let dependents_before = topology.dependents.get(&dependency).map(HashSet::len);

        assert!(topology.add_orphan(block(0), &[dependency]).expect("duplicate remains admissible").ready.is_empty());
        assert!(matches!(topology.add_orphan(block(MAX_ORPHAN_BLOCKS), &[dependency]), Err(TopologyError::CapacityExhausted)));
        assert_eq!(topology.orphans.len(), MAX_ORPHAN_BLOCKS);
        assert_eq!(topology.resolution_pending, pending_before);
        assert_eq!(topology.dependents.get(&dependency).map(HashSet::len), dependents_before);
    }

    #[tokio::test]
    async fn worker_emits_resolution_only_at_the_exact_threshold() {
        let ManagerFixture { manager, resolution: _resolution, mut faults, mut started, .. } = manager_fixture();
        let dependency = hash(20_000);

        for value in 0..ORPHAN_RESOLUTION_THRESHOLD - 1 {
            manager.add_orphan(block(value), Arc::from([dependency])).expect("orphan input");
        }
        for _ in 0..10_000 {
            if manager.metrics().current_occupancy == ORPHAN_RESOLUTION_THRESHOLD - 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(manager.metrics().current_occupancy, ORPHAN_RESOLUTION_THRESHOLD - 1);
        assert!(started.try_recv().is_err());

        manager.add_orphan(block(ORPHAN_RESOLUTION_THRESHOLD - 1), Arc::from([dependency])).expect("threshold orphan input");
        assert_eq!(started.recv().await, Some(dependency));
        assert_eq!(manager.metrics().threshold_crossings, 1);
        assert!(faults.try_recv().is_err());

        manager.shutdown().await.expect("shutdown barrier");
    }

    #[tokio::test]
    async fn natural_body_admission_orders_cancel_before_replacement_resolution() {
        let ManagerFixture { manager, mut resolution, mut faults, mut started, releases, .. } = manager_fixture();
        let dependency = hash(20_000);
        let replacement = hash(20_001);
        for value in 0..ORPHAN_RESOLUTION_THRESHOLD {
            manager.add_orphan(block(value), Arc::from([dependency])).expect("orphan input");
        }
        assert_eq!(started.recv().await, Some(dependency));

        manager.add_orphan(block(20_000), Arc::from([replacement])).expect("natural dependency body");
        assert_eq!(started.recv().await, Some(replacement));
        releases.add_permits(2);
        assert!(matches!(
            resolution.recv().await,
            Some(BlockResolutionEvent::DependencyResolved { requested_hash, result: Ok(_) }) if requested_hash == replacement
        ));
        wait_until(|| manager.resolver_metrics() == DependencyResolverMetricsSnapshot { active_operations: 0, waiting_operations: 0 })
            .await;
        assert!(resolution.try_recv().is_err());
        assert!(faults.try_recv().is_err());

        manager.shutdown().await.expect("shutdown barrier");
    }

    #[tokio::test]
    async fn shutdown_preempts_and_discards_a_queued_data_backlog() {
        let ManagerFixture { manager, resolution: _resolution, mut faults, .. } = manager_fixture();
        let metrics = manager.metrics.clone();
        let resolver_metrics = manager.resolver_metrics.clone();
        for value in 0..PROCESSING_DATA_CHANNEL_CAPACITY {
            manager.add_orphan(block(value), Arc::from([hash(20_000)])).expect("queued orphan");
        }

        manager.shutdown().await.expect("shutdown barrier");
        assert_eq!(metrics.snapshot().current_occupancy, 0);
        assert_eq!(metrics.snapshot().resolution_pending, 0);
        assert_eq!(resolver_metrics.snapshot(), DependencyResolverMetricsSnapshot { active_operations: 0, waiting_operations: 0 });
        assert!(faults.try_recv().is_err());
    }

    #[tokio::test]
    async fn one_session_manager_accepts_its_first_work_after_launch() {
        let ManagerFixture { manager, resolution: _resolution, mut faults, .. } = manager_fixture();
        manager.add_orphan(block(30_000), Arc::from([hash(30_001)])).expect("new-session orphan");
        for _ in 0..10_000 {
            if manager.metrics().current_occupancy == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(manager.metrics().current_occupancy, 1);
        assert!(faults.try_recv().is_err());
        manager.shutdown().await.expect("shutdown barrier");
    }

    #[tokio::test]
    async fn transitive_shutdown_releases_the_exact_rpc_generation() {
        let ManagerFixture { manager, weak_rpc, .. } = manager_fixture();
        assert!(weak_rpc.upgrade().is_some());
        manager.shutdown().await.expect("shutdown barrier");
        assert!(weak_rpc.upgrade().is_none());
    }

    #[tokio::test]
    async fn orphan_capacity_fault_stops_the_resolver_and_releases_its_rpc_generation() {
        let ManagerFixture { manager, resolution: _resolution, mut faults, mut started, weak_rpc, .. } = manager_fixture();
        let dependency = hash(20_000);
        for value in 0..MAX_ORPHAN_BLOCKS {
            manager.add_orphan(block(value), Arc::from([dependency])).expect("orphan within capacity");
        }
        wait_until(|| manager.metrics().current_occupancy == MAX_ORPHAN_BLOCKS).await;
        assert_eq!(started.recv().await, Some(dependency));

        manager.add_orphan(block(MAX_ORPHAN_BLOCKS), Arc::from([dependency])).expect("capacity-triggering input");
        assert_eq!(faults.recv().await, Some(ChildFault::new(Component::OrphanManager, ChildFaultKind::OrphanCapacityExhausted)));
        wait_until(|| manager.resolver_metrics() == DependencyResolverMetricsSnapshot { active_operations: 0, waiting_operations: 0 })
            .await;
        assert!(weak_rpc.upgrade().is_none());

        manager.shutdown().await.expect("shutdown barrier");
    }

    #[tokio::test]
    async fn resolver_fault_propagates_through_orphan_manager_and_stops_the_child() {
        let (resolution_tx, _resolution_rx) = mpsc::channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        for value in 0..PROCESSING_DATA_CHANNEL_CAPACITY {
            resolution_tx.try_send(BlockResolutionEvent::OrphanReady(block(value))).expect("fill resolution channel");
        }
        let (fault_tx, mut fault_rx) = mpsc::unbounded_channel();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let releases = Arc::new(Semaphore::new(0));
        let rpc = Arc::new(GatedRpc { started: started_tx, releases: releases.clone() });
        let weak_rpc = Arc::downgrade(&rpc);
        let mut manager = OrphanManager::new(rpc, resolution_tx, fault_tx);
        manager.start();

        let dependency = hash(20_000);
        for value in 0..ORPHAN_RESOLUTION_THRESHOLD {
            manager.add_orphan(block(value), Arc::from([dependency])).expect("orphan input");
        }
        assert_eq!(started_rx.recv().await, Some(dependency));
        releases.add_permits(1);
        assert_eq!(
            fault_rx.recv().await,
            Some(ChildFault::new(Component::DependencyResolver, ChildFaultKind::SessionDataFull(SessionDataPath::Resolution)))
        );
        wait_until(|| manager.resolver_metrics() == DependencyResolverMetricsSnapshot { active_operations: 0, waiting_operations: 0 })
            .await;
        assert!(weak_rpc.upgrade().is_none());

        manager.shutdown().await.expect("shutdown barrier");
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        for _ in 0..10_000 {
            if condition() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition did not become true");
    }
}
