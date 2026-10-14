use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use kgi_core::channel::{BoundedChannelMetrics, BoundedChannelMetricsSnapshot, BoundedSender, bounded_channel};
use kgi_model::{
    block::{BlockHash, ValidatedNodeBlock},
    lifecycle::{Component, OwnershipFault},
};
use kgi_node::{error::NodeError, rpc::ValidatedRpcClient};
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::{AbortHandle, Id, JoinHandle, JoinSet},
};

use super::{
    BlockResolutionEvent,
    error::{ChildControlError, ChildFault, ChildFaultKind, SessionDataPath},
};
use crate::PROCESSING_DATA_CHANNEL_CAPACITY;

pub(crate) const MAX_DEPENDENCY_RPC_CONCURRENCY: usize = 8;

pub(crate) trait DependencyRpc: Send + Sync + 'static {
    fn full_block(&self, hash: BlockHash) -> impl Future<Output = Result<ValidatedNodeBlock, NodeError>> + Send;
}

impl DependencyRpc for ValidatedRpcClient {
    async fn full_block(&self, hash: BlockHash) -> Result<ValidatedNodeBlock, NodeError> {
        self.full_block(hash).await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolverWork {
    Resolve(BlockHash),
    Cancel(BlockHash),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DependencyResolverMetricsSnapshot {
    pub active_operations: usize,
    pub waiting_operations: usize,
    pub work_channel: BoundedChannelMetricsSnapshot,
}

#[derive(Debug)]
pub(super) struct DependencyResolverMetrics {
    active: Arc<AtomicUsize>,
    waiting: Arc<AtomicUsize>,
    work_channel: BoundedChannelMetrics,
}

impl DependencyResolverMetrics {
    pub(super) fn snapshot(&self) -> DependencyResolverMetricsSnapshot {
        DependencyResolverMetricsSnapshot {
            active_operations: self.active.load(Ordering::Relaxed),
            waiting_operations: self.waiting.load(Ordering::Relaxed),
            work_channel: self.work_channel.snapshot(),
        }
    }
}

struct CounterGuard {
    counter: Arc<AtomicUsize>,
}

impl CounterGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for CounterGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

enum ResolverCommand {
    Shutdown(oneshot::Sender<()>),
}

pub(crate) struct DependencyResolver<R: DependencyRpc = ValidatedRpcClient> {
    commands: mpsc::UnboundedSender<ResolverCommand>,
    work: BoundedSender<ResolverWork>,
    metrics: Arc<DependencyResolverMetrics>,
    worker: Option<DependencyResolverWorker<R>>,
    join: Option<JoinHandle<()>>,
}

impl<R: DependencyRpc> DependencyResolver<R> {
    pub(crate) fn new(
        rpc: Arc<R>,
        resolution: BoundedSender<BlockResolutionEvent>,
        faults: mpsc::UnboundedSender<ChildFault>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (work_tx, work_rx) = bounded_channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let metrics = Arc::new(DependencyResolverMetrics {
            active: Arc::new(AtomicUsize::new(0)),
            waiting: Arc::new(AtomicUsize::new(0)),
            work_channel: work_tx.metrics_source(),
        });
        let worker = DependencyResolverWorker {
            commands: command_rx,
            work: work_rx,
            resolution,
            faults,
            metrics: metrics.clone(),
            permits: Arc::new(Semaphore::new(MAX_DEPENDENCY_RPC_CONCURRENCY)),
            rpc,
            tasks: JoinSet::new(),
            tasks_by_hash: HashMap::new(),
            hashes_by_task: HashMap::new(),
            cancelled_tasks: HashSet::new(),
            faulted: false,
            work_open: true,
        };
        Self { commands: command_tx, work: work_tx, metrics, worker: Some(worker), join: None }
    }

    pub(crate) fn start(&mut self) {
        let worker = self.worker.take().expect("DependencyResolver starts exactly once");
        self.join = Some(tokio::spawn(worker.run()));
    }

    pub(super) fn resolve(&self, hash: BlockHash) -> Result<(), mpsc::error::TrySendError<()>> {
        self.work.try_send(ResolverWork::Resolve(hash)).map_err(map_work_send_error)
    }

    pub(super) fn cancel(&self, hash: BlockHash) -> Result<(), mpsc::error::TrySendError<()>> {
        self.work.try_send(ResolverWork::Cancel(hash)).map_err(map_work_send_error)
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), ChildControlError> {
        if self.worker.take().is_some() {
            return Ok(());
        }
        let (completion, acknowledgement) = oneshot::channel();
        let barrier = match self.commands.send(ResolverCommand::Shutdown(completion)) {
            Ok(()) => acknowledgement.await.map_err(|_| ChildControlError::AcknowledgementLost),
            Err(_) => Err(ChildControlError::CommandPathClosed),
        };
        let joined = match self.join.take() {
            Some(join) => join.await.map_err(|_| ChildControlError::WorkerJoinFailed),
            None => Ok(()),
        };
        joined?;
        barrier
    }

    pub(crate) fn metrics(&self) -> DependencyResolverMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub(super) fn metrics_source(&self) -> Arc<DependencyResolverMetrics> {
        self.metrics.clone()
    }
}

fn map_work_send_error(error: mpsc::error::TrySendError<ResolverWork>) -> mpsc::error::TrySendError<()> {
    match error {
        mpsc::error::TrySendError::Full(_) => mpsc::error::TrySendError::Full(()),
        mpsc::error::TrySendError::Closed(_) => mpsc::error::TrySendError::Closed(()),
    }
}

struct DependencyResolverWorker<R> {
    commands: mpsc::UnboundedReceiver<ResolverCommand>,
    work: kgi_core::channel::BoundedReceiver<ResolverWork>,
    resolution: BoundedSender<BlockResolutionEvent>,
    faults: mpsc::UnboundedSender<ChildFault>,
    metrics: Arc<DependencyResolverMetrics>,
    permits: Arc<Semaphore>,
    rpc: Arc<R>,
    tasks: JoinSet<Result<ValidatedNodeBlock, NodeError>>,
    tasks_by_hash: HashMap<BlockHash, (Id, AbortHandle)>,
    hashes_by_task: HashMap<Id, BlockHash>,
    cancelled_tasks: HashSet<Id>,
    faulted: bool,
    work_open: bool,
}

impl<R: DependencyRpc> DependencyResolverWorker<R> {
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
                        None => {
                            if !self.faulted {
                                self.report_fault(ChildFaultKind::Ownership(OwnershipFault::InternalControlPathLost));
                            }
                            self.abort_and_join().await;
                            return;
                        }
                    }
                }
                work = self.work.recv(), if !self.faulted && self.work_open => {
                    match work {
                        Some(work) => self.handle_work(work).await,
                        None => {
                            self.work_open = false;
                            self.report_fault(ChildFaultKind::SessionDataEndpointLost(SessionDataPath::ResolverWork));
                            self.faulted = true;
                            self.tasks.abort_all();
                        }
                    }
                }
                result = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    if let Some(result) = result {
                        self.handle_task_result(result);
                    }
                }
            }
        }
    }

    /// Applies one prioritized lifecycle command.
    ///
    /// Returns `true` only when the worker must terminate after completing the
    /// command's acknowledgement barrier.
    async fn handle_command(&mut self, command: ResolverCommand) -> bool {
        match command {
            ResolverCommand::Shutdown(completion) => {
                self.abort_and_join().await;
                self.drain_work();
                let _ = completion.send(());
                true
            }
        }
    }

    async fn handle_work(&mut self, work: ResolverWork) {
        match work {
            ResolverWork::Resolve(hash) => self.start_resolution(hash),
            ResolverWork::Cancel(hash) => {
                if let Some((task_id, abort)) = self.tasks_by_hash.get(&hash) {
                    self.cancelled_tasks.insert(*task_id);
                    abort.abort();
                }
            }
        }
    }

    fn start_resolution(&mut self, hash: BlockHash) {
        if self.tasks_by_hash.contains_key(&hash) {
            self.report_fault(ChildFaultKind::InvalidProtocol(Arc::from("duplicate dependency resolution request")));
            self.faulted = true;
            self.tasks.abort_all();
            return;
        }

        let rpc = self.rpc.clone();
        let permits = self.permits.clone();
        let waiting = CounterGuard::new(self.metrics.waiting.clone());
        let active_counter = self.metrics.active.clone();
        let abort = self.tasks.spawn(async move {
            let _permit = permits.acquire_owned().await.expect("dependency resolver semaphore remains open");
            drop(waiting);
            let _active = CounterGuard::new(active_counter);
            rpc.full_block(hash).await
        });
        let task_id = abort.id();
        self.tasks_by_hash.insert(hash, (task_id, abort));
        self.hashes_by_task.insert(task_id, hash);
    }

    fn handle_task_result(&mut self, result: Result<(Id, Result<ValidatedNodeBlock, NodeError>), tokio::task::JoinError>) {
        let task_id = match &result {
            Ok((task_id, _)) => *task_id,
            Err(error) => error.id(),
        };
        let cancelled = self.cancelled_tasks.remove(&task_id);
        let Some(hash) = self.hashes_by_task.remove(&task_id) else {
            self.report_fault(ChildFaultKind::InvalidProtocol(Arc::from("dependency task completed without tracked hash")));
            self.faulted = true;
            return;
        };
        self.tasks_by_hash.remove(&hash);
        if cancelled {
            return;
        }

        match result {
            Ok((_, result)) if !self.faulted => {
                match self.resolution.try_send(BlockResolutionEvent::DependencyResolved { requested_hash: hash, result }) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.report_fault(ChildFaultKind::SessionDataFull(SessionDataPath::Resolution));
                        self.faulted = true;
                        self.tasks.abort_all();
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        self.report_fault(ChildFaultKind::SessionDataEndpointLost(SessionDataPath::Resolution));
                        self.faulted = true;
                        self.tasks.abort_all();
                    }
                }
            }
            Ok(_) => {}
            Err(error) if error.is_cancelled() => {}
            Err(_) => {
                self.report_fault(ChildFaultKind::Ownership(OwnershipFault::UnexpectedWorkerTermination));
                self.faulted = true;
                self.tasks.abort_all();
            }
        }
    }

    async fn abort_and_join(&mut self) {
        self.tasks.abort_all();
        while let Some(result) = self.tasks.join_next_with_id().await {
            let task_id = match &result {
                Ok((task_id, _)) => *task_id,
                Err(error) => error.id(),
            };
            if let Some(hash) = self.hashes_by_task.remove(&task_id) {
                self.tasks_by_hash.remove(&hash);
            }
        }
        self.tasks_by_hash.clear();
        self.hashes_by_task.clear();
        self.cancelled_tasks.clear();
    }

    fn drain_work(&mut self) {
        while self.work.try_recv().is_ok() {}
    }

    fn report_fault(&self, kind: ChildFaultKind) {
        let _ = self.faults.send(ChildFault::new(Component::DependencyResolver, kind));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Weak};

    use kgi_model::block::BlueWork;

    use super::*;

    #[derive(Clone)]
    struct GatedRpc {
        started: mpsc::UnboundedSender<BlockHash>,
        releases: Arc<Semaphore>,
    }

    struct FailingRpc;

    impl DependencyRpc for FailingRpc {
        async fn full_block(&self, requested: BlockHash) -> Result<ValidatedNodeBlock, NodeError> {
            if requested == hash(1) {
                Err(NodeError::BlockNotFound { hash: requested })
            } else {
                Err(NodeError::RpcRequestFailed { diagnostic: Arc::from("opaque test failure") })
            }
        }
    }

    impl DependencyRpc for GatedRpc {
        async fn full_block(&self, hash: BlockHash) -> Result<ValidatedNodeBlock, NodeError> {
            let _ = self.started.send(hash);
            self.releases.acquire().await.expect("test release semaphore remains open").forget();
            Ok(block(hash))
        }
    }

    fn hash(value: usize) -> BlockHash {
        let bytes = value.to_le_bytes();
        let mut hash = [0_u8; 32];
        hash[..bytes.len()].copy_from_slice(&bytes);
        BlockHash::from_bytes(hash)
    }

    fn block(hash: BlockHash) -> ValidatedNodeBlock {
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

    struct ResolverFixture {
        resolver: DependencyResolver<GatedRpc>,
        resolution: kgi_core::channel::BoundedReceiver<BlockResolutionEvent>,
        faults: mpsc::UnboundedReceiver<ChildFault>,
        started: mpsc::UnboundedReceiver<BlockHash>,
        releases: Arc<Semaphore>,
        rpc: Weak<GatedRpc>,
    }

    async fn resolver() -> ResolverFixture {
        let (resolution_tx, resolution_rx) = bounded_channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (fault_tx, fault_rx) = mpsc::unbounded_channel();
        let (started_tx, started_rx) = mpsc::unbounded_channel();
        let releases = Arc::new(Semaphore::new(0));
        let rpc = Arc::new(GatedRpc { started: started_tx, releases: releases.clone() });
        let weak = Arc::downgrade(&rpc);
        let mut resolver = DependencyResolver::new(rpc, resolution_tx, fault_tx);
        resolver.start();
        ResolverFixture { resolver, resolution: resolution_rx, faults: fault_rx, started: started_rx, releases, rpc: weak }
    }

    #[tokio::test]
    async fn local_rpc_limit_tracks_active_and_waiting_operations() {
        let ResolverFixture { resolver, mut resolution, mut faults, mut started, releases, .. } = resolver().await;
        for value in 0..=MAX_DEPENDENCY_RPC_CONCURRENCY {
            resolver.resolve(hash(value)).expect("resolver work");
        }

        let mut started_hashes = Vec::new();
        for _ in 0..MAX_DEPENDENCY_RPC_CONCURRENCY {
            started_hashes.push(started.recv().await.expect("active RPC"));
        }
        assert_eq!(started_hashes.len(), MAX_DEPENDENCY_RPC_CONCURRENCY);
        assert!(started.try_recv().is_err());
        wait_until(|| resolver.metrics().active_operations == MAX_DEPENDENCY_RPC_CONCURRENCY).await;
        assert_eq!(resolver.metrics().waiting_operations, 1);

        releases.add_permits(MAX_DEPENDENCY_RPC_CONCURRENCY);
        for _ in 0..MAX_DEPENDENCY_RPC_CONCURRENCY {
            assert!(matches!(resolution.recv().await, Some(BlockResolutionEvent::DependencyResolved { result: Ok(_), .. })));
        }
        assert_eq!(started.recv().await, Some(hash(MAX_DEPENDENCY_RPC_CONCURRENCY)));
        releases.add_permits(1);
        assert!(matches!(resolution.recv().await, Some(BlockResolutionEvent::DependencyResolved { result: Ok(_), .. })));
        wait_until(|| {
            let snapshot = resolver.metrics();
            snapshot.active_operations == 0 && snapshot.waiting_operations == 0
        })
        .await;
        assert!(faults.try_recv().is_err());
        resolver.shutdown().await.expect("shutdown barrier");
    }

    #[tokio::test]
    async fn natural_arrival_cancels_waiting_or_active_work_without_a_result() {
        let ResolverFixture { resolver, mut resolution, mut faults, mut started, .. } = resolver().await;
        let requested = hash(7);
        resolver.resolve(requested).expect("resolve request");
        assert_eq!(started.recv().await, Some(requested));
        resolver.cancel(requested).expect("cancel request");

        wait_until(|| {
            let snapshot = resolver.metrics();
            snapshot.active_operations == 0 && snapshot.waiting_operations == 0
        })
        .await;
        assert!(resolution.try_recv().is_err());
        assert!(faults.try_recv().is_err());
        resolver.shutdown().await.expect("shutdown barrier");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_cancellation_wins_an_already_ready_rpc_result() {
        let ResolverFixture { resolver, mut resolution, mut faults, mut started, releases, .. } = resolver().await;
        let requested = hash(8);
        resolver.resolve(requested).expect("resolve request");
        assert_eq!(started.recv().await, Some(requested));

        // Both wakeups become ready before this current-thread test yields back
        // to the resolver. Work priority must record the task-ID cancellation
        // before the completed RPC result is joined.
        releases.add_permits(1);
        resolver.cancel(requested).expect("cancel request");

        wait_until(|| {
            let snapshot = resolver.metrics();
            snapshot.active_operations == 0 && snapshot.waiting_operations == 0
        })
        .await;
        tokio::task::yield_now().await;
        assert!(resolution.try_recv().is_err());
        assert!(faults.try_recv().is_err());
        resolver.shutdown().await.expect("shutdown barrier");
    }

    #[tokio::test]
    async fn full_parent_result_channel_faults_without_blocking_shutdown() {
        let (resolution_tx, _resolution_rx) = bounded_channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let resolution_metrics = resolution_tx.metrics_source();
        for value in 0..PROCESSING_DATA_CHANNEL_CAPACITY {
            resolution_tx.try_send(BlockResolutionEvent::OrphanReady(block(hash(value)))).expect("fill resolution channel");
        }
        assert_eq!(
            resolution_metrics.snapshot(),
            BoundedChannelMetricsSnapshot {
                current_occupancy: PROCESSING_DATA_CHANNEL_CAPACITY,
                high_water_mark: PROCESSING_DATA_CHANNEL_CAPACITY,
            }
        );
        assert!(matches!(
            resolution_tx.try_send(BlockResolutionEvent::OrphanReady(block(hash(PROCESSING_DATA_CHANNEL_CAPACITY)))),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        let (fault_tx, mut fault_rx) = mpsc::unbounded_channel();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let releases = Arc::new(Semaphore::new(0));
        let rpc = Arc::new(GatedRpc { started: started_tx, releases: releases.clone() });
        let mut resolver = DependencyResolver::new(rpc, resolution_tx, fault_tx);
        resolver.start();
        resolver.resolve(hash(5_000)).expect("resolve request");
        assert_eq!(started_rx.recv().await, Some(hash(5_000)));
        releases.add_permits(1);
        assert_eq!(
            fault_rx.recv().await,
            Some(ChildFault::new(Component::DependencyResolver, ChildFaultKind::SessionDataFull(SessionDataPath::Resolution)))
        );

        resolver.shutdown().await.expect("shutdown barrier");
    }

    #[tokio::test]
    async fn work_channel_reports_exact_capacity_and_high_water() {
        let (resolution_tx, _resolution_rx) = bounded_channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (fault_tx, _fault_rx) = mpsc::unbounded_channel();
        let (started_tx, _started_rx) = mpsc::unbounded_channel();
        let rpc = Arc::new(GatedRpc { started: started_tx, releases: Arc::new(Semaphore::new(0)) });
        let resolver = DependencyResolver::new(rpc, resolution_tx, fault_tx);

        for value in 0..PROCESSING_DATA_CHANNEL_CAPACITY {
            resolver.resolve(hash(value)).expect("work within channel capacity");
        }
        assert!(matches!(resolver.resolve(hash(PROCESSING_DATA_CHANNEL_CAPACITY)), Err(mpsc::error::TrySendError::Full(()))));
        assert_eq!(
            resolver.metrics().work_channel,
            BoundedChannelMetricsSnapshot {
                current_occupancy: PROCESSING_DATA_CHANNEL_CAPACITY,
                high_water_mark: PROCESSING_DATA_CHANNEL_CAPACITY,
            }
        );

        resolver.shutdown().await.expect("inert shutdown");
    }

    #[tokio::test]
    async fn consuming_shutdown_releases_the_exact_rpc_generation() {
        let ResolverFixture { resolver, rpc, .. } = resolver().await;
        assert!(rpc.upgrade().is_some());
        resolver.shutdown().await.expect("shutdown barrier");
        assert!(rpc.upgrade().is_none());
    }

    #[tokio::test]
    async fn dropping_an_active_owner_reports_command_path_loss() {
        let ResolverFixture { resolver, mut faults, .. } = resolver().await;

        drop(resolver);

        assert_eq!(
            faults.recv().await,
            Some(ChildFault::new(Component::DependencyResolver, ChildFaultKind::Ownership(OwnershipFault::InternalControlPathLost),))
        );
        assert!(faults.recv().await.is_none());
    }

    #[tokio::test]
    async fn resolver_preserves_unavailable_and_opaque_node_outcomes() {
        let (resolution_tx, mut resolution_rx) = bounded_channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (fault_tx, mut fault_rx) = mpsc::unbounded_channel();
        let mut resolver = DependencyResolver::new(Arc::new(FailingRpc), resolution_tx, fault_tx);
        resolver.start();
        resolver.resolve(hash(1)).expect("not-found request");
        resolver.resolve(hash(2)).expect("opaque-failure request");

        let mut not_found = false;
        let mut opaque = false;
        for _ in 0..2 {
            match resolution_rx.recv().await.expect("resolver result") {
                BlockResolutionEvent::DependencyResolved { requested_hash, result: Err(NodeError::BlockNotFound { hash }) } => {
                    assert_eq!(requested_hash, hash);
                    not_found = true;
                }
                BlockResolutionEvent::DependencyResolved { requested_hash, result: Err(NodeError::RpcRequestFailed { .. }) } => {
                    assert_eq!(requested_hash, hash(2));
                    opaque = true;
                }
                other => panic!("unexpected resolver result: {other:?}"),
            }
        }
        assert!(not_found && opaque);
        assert!(fault_rx.try_recv().is_err());
        resolver.shutdown().await.expect("shutdown barrier");
    }

    #[tokio::test]
    async fn shutdown_preempts_and_discards_a_queued_work_backlog() {
        let ResolverFixture { resolver, mut resolution, mut faults, mut started, .. } = resolver().await;
        let metrics = resolver.metrics_source();
        for value in 0..PROCESSING_DATA_CHANNEL_CAPACITY {
            resolver.resolve(hash(value)).expect("queued resolution");
        }

        resolver.shutdown().await.expect("shutdown barrier");
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.active_operations, 0);
        assert_eq!(snapshot.waiting_operations, 0);
        assert_eq!(snapshot.work_channel.current_occupancy, 0);
        assert!(started.try_recv().is_err());
        assert!(resolution.try_recv().is_err());
        assert!(faults.try_recv().is_err());
    }

    #[tokio::test]
    async fn inert_construction_starts_no_rpc_before_single_launch() {
        let (resolution_tx, mut resolution_rx) = bounded_channel(PROCESSING_DATA_CHANNEL_CAPACITY);
        let (fault_tx, mut fault_rx) = mpsc::unbounded_channel();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let releases = Arc::new(Semaphore::new(0));
        let rpc = Arc::new(GatedRpc { started: started_tx, releases: releases.clone() });
        let mut resolver = DependencyResolver::new(rpc, resolution_tx, fault_tx);
        assert!(started_rx.try_recv().is_err());
        resolver.start();

        let requested = hash(30_000);
        resolver.resolve(requested).expect("session work");
        assert_eq!(started_rx.recv().await, Some(requested));
        releases.add_permits(1);
        assert!(
            matches!(resolution_rx.recv().await, Some(BlockResolutionEvent::DependencyResolved { requested_hash, result: Ok(_) }) if requested_hash == requested)
        );
        assert!(fault_rx.try_recv().is_err());
        resolver.shutdown().await.expect("shutdown barrier");
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
