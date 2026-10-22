use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
};

use kgi_api_ingress::channel::{GraphUpdateObservation, GraphUpdateReceiver};
use kgi_model::graph_update::{BlockCommitted, GraphUpdate, VspcCommitted};
use thiserror::Error;
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// Maximum number of ordinary committed updates retained during construction.
pub(crate) const API_STAGING_UPDATE_CAPACITY: usize = 4096;

/// Point-in-time operational measurements for one publication runtime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PublicationRuntimeMetricsSnapshot {
    /// Number of ordinary committed updates currently staged.
    pub staging_occupancy: usize,
    /// Greatest staging occupancy observed during this runtime.
    pub staging_high_water_mark: usize,
    /// Number of updates rejected at the staging-capacity boundary.
    pub staging_overflow_count: u64,
    /// Number of intra-session reconstruction cuts observed by this runtime.
    pub reconstruction_count: u64,
}

/// Terminal failure of the private publication-runtime worker.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum PublicationRuntimeError {
    /// The session stream violated its lifecycle-marker order.
    #[error("graph-update stream violated publication marker order")]
    InvalidMarkerOrder,
    /// The runtime worker panicked or its Tokio task was otherwise lost.
    #[error("publication-runtime worker could not be joined")]
    WorkerJoinFailed,
}

/// One session-bound graph-publication worker and its completed shutdown barrier.
pub(crate) struct PublicationRuntime {
    cancellation: CancellationToken,
    completion: PublicationRuntimeCompletion,
    shared: Arc<RuntimeSharedState>,
}

impl PublicationRuntime {
    /// Starts the single worker for one fresh processing-session ingress.
    pub(crate) fn new(graph_updates: GraphUpdateReceiver) -> Self {
        let cancellation = CancellationToken::new();
        let shared = Arc::new(RuntimeSharedState::new());
        let worker = PublicationRuntimeWorker { graph_updates, staging: StagingBuffer::new(shared.clone()), shared: shared.clone() };
        let join = tokio::spawn(worker.run(cancellation.clone()));
        Self { cancellation, completion: PublicationRuntimeCompletion::new(join), shared }
    }

    /// Cancels and joins the runtime worker, returning its retained result.
    pub(crate) async fn shutdown(&self) -> Result<(), PublicationRuntimeError> {
        self.cancellation.cancel();
        self.completion.wait().await
    }

    /// Returns this runtime's current operational measurements.
    #[must_use]
    pub(crate) fn metrics(&self) -> PublicationRuntimeMetricsSnapshot {
        self.shared.lock().metrics
    }

    #[cfg(test)]
    fn snapshot(&self) -> RuntimeSnapshot {
        *self.shared.lock()
    }
}

impl Drop for PublicationRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[derive(Clone)]
struct PublicationRuntimeCompletion {
    inner: Arc<PublicationRuntimeCompletionInner>,
}

struct PublicationRuntimeCompletionInner {
    join: AsyncMutex<Option<JoinHandle<Result<(), PublicationRuntimeError>>>>,
    result: watch::Sender<Option<Result<(), PublicationRuntimeError>>>,
}

impl PublicationRuntimeCompletion {
    fn new(join: JoinHandle<Result<(), PublicationRuntimeError>>) -> Self {
        let (result, _) = watch::channel(None);
        Self { inner: Arc::new(PublicationRuntimeCompletionInner { join: AsyncMutex::new(Some(join)), result }) }
    }

    async fn wait(&self) -> Result<(), PublicationRuntimeError> {
        if let Some(result) = *self.inner.result.borrow() {
            return result;
        }

        let mut join = self.inner.join.lock().await;
        if let Some(worker) = join.as_mut() {
            let result = worker.await.unwrap_or(Err(PublicationRuntimeError::WorkerJoinFailed));
            *join = None;
            self.inner.result.send_replace(Some(result));
            return result;
        }

        self.inner.result.borrow().as_ref().copied().expect("a consumed publication-runtime join stores its result")
    }
}

struct PublicationRuntimeWorker {
    graph_updates: GraphUpdateReceiver,
    staging: StagingBuffer,
    shared: Arc<RuntimeSharedState>,
}

impl PublicationRuntimeWorker {
    async fn run(mut self, cancellation: CancellationToken) -> Result<(), PublicationRuntimeError> {
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    self.finish();
                    return Ok(());
                }
                observation = self.graph_updates.next_observation() => match observation {
                    Some(observation) => self.handle_observation(observation)?,
                    None => {
                        self.shared.set_phase(PublicationRuntimePhase::Quiescent);
                        cancellation.cancelled().await;
                        self.finish();
                        return Ok(());
                    }
                }
            }
        }
    }

    fn handle_observation(&mut self, observation: GraphUpdateObservation) -> Result<(), PublicationRuntimeError> {
        let phase = self.shared.lock().phase;
        match (phase, observation) {
            (PublicationRuntimePhase::AwaitingCatchup, GraphUpdateObservation::Update(GraphUpdate::Catchup)) => {
                self.shared.set_phase(PublicationRuntimePhase::Constructing);
                Ok(())
            }
            (PublicationRuntimePhase::AwaitingCatchup, _) => Err(PublicationRuntimeError::InvalidMarkerOrder),
            (PublicationRuntimePhase::Constructing, GraphUpdateObservation::Update(GraphUpdate::BlockCommitted(update))) => {
                self.stage(StagedGraphUpdate::Block(update));
                Ok(())
            }
            (PublicationRuntimePhase::Constructing, GraphUpdateObservation::Update(GraphUpdate::VspcCommitted(update))) => {
                self.stage(StagedGraphUpdate::Vspc(update));
                Ok(())
            }
            (PublicationRuntimePhase::Constructing, GraphUpdateObservation::Update(GraphUpdate::Live)) => {
                self.shared.set_target_live();
                Ok(())
            }
            (PublicationRuntimePhase::Constructing, GraphUpdateObservation::Gap(_)) => {
                self.restart_staging();
                Ok(())
            }
            (PublicationRuntimePhase::Constructing, GraphUpdateObservation::Update(GraphUpdate::Catchup)) => {
                Err(PublicationRuntimeError::InvalidMarkerOrder)
            }
            (PublicationRuntimePhase::Quiescent | PublicationRuntimePhase::Stopped, _) => {
                unreachable!("a quiescent or stopped publication runtime does not consume graph updates")
            }
        }
    }

    fn stage(&mut self, update: StagedGraphUpdate) {
        if self.staging.push(update).is_err() {
            self.restart_staging();
        }
    }

    fn restart_staging(&mut self) {
        self.staging.clear();
        self.shared.record_reconstruction();
    }

    fn finish(&mut self) {
        self.staging.clear();
        self.shared.set_phase(PublicationRuntimePhase::Stopped);
    }
}

enum StagedGraphUpdate {
    Block(BlockCommitted),
    Vspc(VspcCommitted),
}

struct StagingBuffer {
    entries: VecDeque<StagedGraphUpdate>,
    shared: Arc<RuntimeSharedState>,
}

impl StagingBuffer {
    fn new(shared: Arc<RuntimeSharedState>) -> Self {
        Self { entries: VecDeque::with_capacity(API_STAGING_UPDATE_CAPACITY), shared }
    }

    fn push(&mut self, update: StagedGraphUpdate) -> Result<(), StagingOverflow> {
        if self.entries.len() == API_STAGING_UPDATE_CAPACITY {
            self.shared.record_staging_overflow();
            return Err(StagingOverflow);
        }
        self.entries.push_back(update);
        self.shared.record_staging_occupancy(self.entries.len());
        Ok(())
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.shared.record_staging_occupancy(0);
    }
}

impl Drop for StagingBuffer {
    fn drop(&mut self) {
        self.shared.record_staging_occupancy(0);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StagingOverflow;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationRuntimePhase {
    AwaitingCatchup,
    Constructing,
    Quiescent,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeSnapshot {
    phase: PublicationRuntimePhase,
    target_live: bool,
    metrics: PublicationRuntimeMetricsSnapshot,
}

struct RuntimeSharedState {
    inner: Mutex<RuntimeSnapshot>,
}

impl RuntimeSharedState {
    fn new() -> Self {
        Self {
            inner: Mutex::new(RuntimeSnapshot {
                phase: PublicationRuntimePhase::AwaitingCatchup,
                target_live: false,
                metrics: PublicationRuntimeMetricsSnapshot::default(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, RuntimeSnapshot> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn set_phase(&self, phase: PublicationRuntimePhase) {
        self.lock().phase = phase;
    }

    fn set_target_live(&self) {
        self.lock().target_live = true;
    }

    fn record_staging_occupancy(&self, occupancy: usize) {
        let mut state = self.lock();
        state.metrics.staging_occupancy = occupancy;
        state.metrics.staging_high_water_mark = state.metrics.staging_high_water_mark.max(occupancy);
    }

    fn record_staging_overflow(&self) {
        let mut state = self.lock();
        state.metrics.staging_overflow_count =
            state.metrics.staging_overflow_count.checked_add(1).expect("publication-runtime staging-overflow count fits u64");
    }

    fn record_reconstruction(&self) {
        let mut state = self.lock();
        state.metrics.reconstruction_count =
            state.metrics.reconstruction_count.checked_add(1).expect("publication-runtime reconstruction count fits u64");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kgi_api_ingress::channel::{GraphUpdateOfferOutcome, graph_update_channel};
    use kgi_model::{
        block::BlockHash,
        graph_update::{GraphUpdate, VspcCommitted},
    };
    use tokio::sync::Semaphore;

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
            level_snapshots: Arc::from([]),
        }
    }

    async fn wait_for(runtime: &PublicationRuntime, predicate: impl Fn(RuntimeSnapshot) -> bool) -> RuntimeSnapshot {
        for _ in 0..100 {
            let snapshot = runtime.snapshot();
            if predicate(snapshot) {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
        panic!("publication runtime did not reach the expected state")
    }

    #[tokio::test]
    async fn catchup_starts_staging_and_live_is_sticky_without_consuming_capacity() {
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver);

        assert_eq!(producer.offer_vspc_committed(vspc_update(1)), Ok(GraphUpdateOfferOutcome::SuppressedAwaitingCatchup));
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::AwaitingCatchup);
        assert_eq!(runtime.metrics().staging_occupancy, 0);

        producer.publish_catchup().expect("runtime receiver is open");
        wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Constructing).await;
        assert_eq!(producer.offer_vspc_committed(vspc_update(2)), Ok(GraphUpdateOfferOutcome::Enqueued));
        let staged = wait_for(&runtime, |snapshot| snapshot.metrics.staging_occupancy == 1).await;
        assert_eq!(staged.metrics.staging_high_water_mark, 1);

        producer.publish_live().await.expect("runtime receiver is open");
        let live = wait_for(&runtime, |snapshot| snapshot.target_live).await;
        assert_eq!(live.metrics.staging_occupancy, 1);

        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
        assert_eq!(runtime.metrics().staging_occupancy, 0);
    }

    #[tokio::test]
    async fn receiver_closure_is_quiescent_until_explicit_shutdown() {
        let (producer, receiver) = graph_update_channel();
        let runtime = PublicationRuntime::new(receiver);
        producer.publish_catchup().expect("runtime receiver is open");
        assert_eq!(producer.offer_vspc_committed(vspc_update(3)), Ok(GraphUpdateOfferOutcome::Enqueued));
        wait_for(&runtime, |snapshot| snapshot.metrics.staging_occupancy == 1).await;

        drop(producer);
        let quiescent = wait_for(&runtime, |snapshot| snapshot.phase == PublicationRuntimePhase::Quiescent).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(runtime.snapshot(), quiescent);

        assert_eq!(runtime.shutdown().await, Ok(()));
        assert_eq!(runtime.snapshot().phase, PublicationRuntimePhase::Stopped);
        assert_eq!(runtime.metrics().staging_occupancy, 0);
    }

    #[test]
    fn staging_capacity_is_exact_and_overflow_preserves_sticky_measurements() {
        let shared = Arc::new(RuntimeSharedState::new());
        let mut staging = StagingBuffer::new(shared.clone());
        for byte in 0..API_STAGING_UPDATE_CAPACITY {
            staging.push(StagedGraphUpdate::Vspc(vspc_update(byte as u8))).expect("entry is within staging capacity");
        }
        assert_eq!(staging.push(StagedGraphUpdate::Vspc(vspc_update(0))), Err(StagingOverflow));
        staging.clear();

        assert_eq!(
            shared.lock().metrics,
            PublicationRuntimeMetricsSnapshot {
                staging_occupancy: 0,
                staging_high_water_mark: API_STAGING_UPDATE_CAPACITY,
                staging_overflow_count: 1,
                reconstruction_count: 0,
            }
        );
    }

    #[tokio::test]
    async fn completion_join_ownership_survives_a_cancelled_waiter() {
        let release = Arc::new(Semaphore::new(0));
        let worker_release = release.clone();
        let join = tokio::spawn(async move {
            worker_release.acquire().await.expect("test semaphore remains open").forget();
            Ok(())
        });
        let completion = PublicationRuntimeCompletion::new(join);

        let first_completion = completion.clone();
        let first = tokio::spawn(async move { first_completion.wait().await });
        tokio::task::yield_now().await;
        first.abort();
        assert!(first.await.expect_err("first waiter is cancelled").is_cancelled());

        let second_completion = completion.clone();
        let second = tokio::spawn(async move { second_completion.wait().await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        release.add_permits(1);
        assert_eq!(second.await.expect("second waiter joins"), Ok(()));
        assert_eq!(completion.wait().await, Ok(()));
    }

    #[tokio::test]
    async fn invalid_first_marker_is_a_typed_worker_failure() {
        let (_producer, receiver) = graph_update_channel();
        let shared = Arc::new(RuntimeSharedState::new());
        let mut worker = PublicationRuntimeWorker { graph_updates: receiver, staging: StagingBuffer::new(shared.clone()), shared };

        assert_eq!(
            worker.handle_observation(GraphUpdateObservation::Update(GraphUpdate::Live)),
            Err(PublicationRuntimeError::InvalidMarkerOrder)
        );
    }
}
