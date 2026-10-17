#![allow(dead_code, reason = "the cache kernel is integrated by later API increments")]

use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use bytes::Bytes;
use kgi_api_model::protocol::{HOT_DESTINATION_LEVEL_DISTANCE, MAX_CACHE_LEVEL_DISTANCE};
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{codec::HeadEtagInput, encoding::scheduler::EncodingSchedulerError};

/// One immutable source-keyed canonical-delta response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CachedDelta {
    from_revision_id: u64,
    to_revision_id: u64,
    high_level: u32,
    uncompressed_json_bytes: u64,
    gzip_body: Bytes,
}

impl CachedDelta {
    /// Constructs one completely encoded canonical-delta cache value.
    pub(crate) const fn new(
        from_revision_id: u64,
        to_revision_id: u64,
        high_level: u32,
        uncompressed_json_bytes: u64,
        gzip_body: Bytes,
    ) -> Self {
        Self { from_revision_id, to_revision_id, high_level, uncompressed_json_bytes, gzip_body }
    }

    /// Returns the exact source revision represented by this entry.
    pub(crate) const fn source_revision_id(&self) -> u64 {
        self.from_revision_id
    }

    /// Returns the fixed target revision represented by this entry.
    pub(crate) const fn to_revision_id(&self) -> u64 {
        self.to_revision_id
    }

    /// Returns the last entry's target Head level.
    pub(crate) const fn high_level(&self) -> u32 {
        self.high_level
    }

    /// Returns the complete uncompressed JSON byte count.
    pub(crate) const fn uncompressed_json_bytes(&self) -> u64 {
        self.uncompressed_json_bytes
    }

    /// Returns the shared final gzip body.
    pub(crate) const fn gzip_body(&self) -> &Bytes {
        &self.gzip_body
    }
}

/// One immutable depth-tiered Head snapshot response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CachedHeadSnapshot {
    depth: u32,
    revision: u64,
    revision_timestamp_ms: u64,
    low_level: u32,
    high_level: u32,
    uncompressed_json_bytes: u64,
    etag: HeadEtagInput,
    gzip_body: Bytes,
}

impl CachedHeadSnapshot {
    /// Constructs one completely encoded Head-tier cache value.
    #[allow(clippy::too_many_arguments, reason = "the fields are the settled complete cached Head value")]
    pub(crate) const fn new(
        depth: u32,
        revision: u64,
        revision_timestamp_ms: u64,
        low_level: u32,
        high_level: u32,
        uncompressed_json_bytes: u64,
        etag: HeadEtagInput,
        gzip_body: Bytes,
    ) -> Self {
        Self { depth, revision, revision_timestamp_ms, low_level, high_level, uncompressed_json_bytes, etag, gzip_body }
    }

    /// Returns this entry's derived cache depth.
    pub(crate) const fn depth(&self) -> u32 {
        self.depth
    }

    /// Returns the immutable snapshot revision.
    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the publication-local timestamp of the snapshot revision.
    pub(crate) const fn revision_timestamp_ms(&self) -> u64 {
        self.revision_timestamp_ms
    }

    /// Returns the actual inclusive lower level encoded in the response.
    pub(crate) const fn low_level(&self) -> u32 {
        self.low_level
    }

    /// Returns the actual inclusive upper level encoded in the response.
    pub(crate) const fn high_level(&self) -> u32 {
        self.high_level
    }

    /// Returns the complete uncompressed JSON byte count.
    pub(crate) const fn uncompressed_json_bytes(&self) -> u64 {
        self.uncompressed_json_bytes
    }

    /// Returns the semantic weak-validator input for this response.
    pub(crate) const fn etag(&self) -> HeadEtagInput {
        self.etag
    }

    /// Returns the shared final gzip body.
    pub(crate) const fn gzip_body(&self) -> &Bytes {
        &self.gzip_body
    }
}

/// Shared failure produced by one cache construction job.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum CacheBuildError {
    /// The service-level cache-job cancellation domain was cancelled or closed.
    #[error("cache construction was cancelled")]
    Cancelled,
    /// Detached response encoding failed.
    #[error(transparent)]
    Encoding(#[from] EncodingSchedulerError),
}

/// Request-local result of waiting for shared cache construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum CacheWaitError {
    /// This waiter detached without changing the shared job.
    #[error("cache wait was cancelled")]
    Cancelled,
    /// The shared cache construction failed.
    #[error(transparent)]
    Build(#[from] CacheBuildError),
}

/// One service-level owner for dynamically spawned cache orchestration tasks.
pub(crate) struct CacheJobTracker {
    registration: Mutex<bool>,
    cancellation: CancellationToken,
    tasks: TaskTracker,
}

impl CacheJobTracker {
    /// Creates an open tracker and cache-job cancellation root.
    pub(crate) fn new() -> Self {
        Self { registration: Mutex::new(true), cancellation: CancellationToken::new(), tasks: TaskTracker::new() }
    }

    /// Closes registration, cancels unfinished jobs, and joins every tracked task.
    pub(crate) async fn shutdown(&self) {
        {
            let mut accepting = self.registration.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if *accepting {
                *accepting = false;
                self.tasks.close();
                self.cancellation.cancel();
            }
        }
        self.tasks.wait().await;
    }

    fn spawn<F, Fut>(&self, task: F) -> bool
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let accepting = self.registration.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !*accepting {
            return false;
        }
        let cancellation = self.cancellation.child_token();
        self.tasks.spawn(task(cancellation));
        true
    }
}

/// Publication-local completed responses, shared jobs, and destination heat.
pub(crate) struct GraphCache {
    state: Mutex<GraphCacheState>,
}

impl GraphCache {
    /// Creates an empty cache for one graph publication.
    pub(crate) fn new() -> Self {
        Self { state: Mutex::new(GraphCacheState::default()) }
    }

    /// Reuses or starts the single immutable delta response for one exact source revision.
    ///
    /// The elected builder samples the then-current Head only after construction
    /// so an aged result can complete its waiters without entering the cache.
    pub(crate) fn delta<F, Fut, H, HeadFut>(
        self: &Arc<Self>,
        jobs: &CacheJobTracker,
        source_revision: u64,
        builder: F,
        current_head_high_level: H,
    ) -> CacheAccess<CachedDelta>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedDelta>, CacheBuildError>> + Send + 'static,
        H: FnOnce() -> HeadFut + Send + 'static,
        HeadFut: Future<Output = u32> + Send + 'static,
    {
        let (access, builder_job) = self.lock_state().deltas.access(source_revision);

        if let Some(job) = builder_job {
            self.spawn_delta_builder(jobs, source_revision, job, builder, current_head_high_level);
        }
        access
    }

    /// Records one successfully delivered canonical delta at its actual destination.
    pub(crate) fn record_delta_delivery(&self, destination_revision: u64, destination_high_level: u32) {
        self.lock_state().deltas.record_delivery(destination_revision, destination_high_level);
    }

    /// Prunes complete delta and destination-heat buckets behind their structural bounds.
    pub(crate) fn prune_deltas(&self, head_high_level: u32) {
        self.lock_state().deltas.prune(head_high_level);
    }

    fn spawn_delta_builder<F, Fut, H, HeadFut>(
        self: &Arc<Self>,
        jobs: &CacheJobTracker,
        source_revision: u64,
        job: Arc<CacheJob<CachedDelta>>,
        builder: F,
        current_head_high_level: H,
    ) where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedDelta>, CacheBuildError>> + Send + 'static,
        H: FnOnce() -> HeadFut + Send + 'static,
        HeadFut: Future<Output = u32> + Send + 'static,
    {
        let cache = Arc::downgrade(self);
        let task_job = job.clone();
        let spawned = jobs.spawn(move |cancellation| async move {
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(CacheBuildError::Cancelled),
                result = async {
                    let value = builder(cancellation.clone()).await?;
                    let head_high_level = current_head_high_level().await;
                    Ok((value, head_high_level))
                } => result,
            };
            finish_delta_job(&cache, source_revision, &task_job, result);
        });
        if !spawned {
            finish_delta_job(&Arc::downgrade(self), source_revision, &job, Err(CacheBuildError::Cancelled));
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, GraphCacheState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Default)]
struct GraphCacheState {
    deltas: DeltaCache,
    head_slots: HashMap<u32, CacheSlot<CachedHeadSnapshot>>,
}

#[derive(Default)]
struct DeltaCache {
    slots_by_source: HashMap<u64, CacheSlot<CachedDelta>>,
    sources_by_target_level: BTreeMap<u32, Vec<u64>>,
    destination_heat: DestinationHeat,
}

impl DeltaCache {
    fn access(&mut self, source_revision: u64) -> (CacheAccess<CachedDelta>, Option<Arc<CacheJob<CachedDelta>>>) {
        let slot = self.slots_by_source.entry(source_revision).or_default();
        if let Some(completed) = &slot.completed {
            (CacheAccess::Completed(completed.clone()), None)
        } else if let Some(running) = &slot.running {
            (CacheAccess::Job(running.clone()), None)
        } else {
            let job = Arc::new(CacheJob::pending());
            slot.running = Some(job.clone());
            (CacheAccess::Job(job.clone()), Some(job))
        }
    }

    fn finish(
        &mut self,
        source_revision: u64,
        job: &Arc<CacheJob<CachedDelta>>,
        result: &Result<(Arc<CachedDelta>, u32), CacheBuildError>,
    ) {
        let mut remove_slot = false;
        if let Some(slot) = self.slots_by_source.get_mut(&source_revision)
            && slot.running.as_ref().is_some_and(|running| Arc::ptr_eq(running, job))
        {
            if let Ok((value, head_high_level)) = result {
                assert_eq!(value.source_revision_id(), source_revision, "a completed delta remains keyed by its captured source");
                if head_high_level.saturating_sub(value.high_level()) <= MAX_CACHE_LEVEL_DISTANCE {
                    slot.completed = Some(value.clone());
                    self.sources_by_target_level.entry(value.high_level()).or_default().push(source_revision);
                }
            }
            slot.running = None;
            remove_slot = slot.completed.is_none();
        }
        if remove_slot {
            self.slots_by_source.remove(&source_revision);
        }
    }

    fn record_delivery(&mut self, destination_revision: u64, destination_high_level: u32) {
        self.destination_heat.record(destination_revision, destination_high_level);
    }

    fn prune(&mut self, head_high_level: u32) {
        let retained_target_level = head_high_level.saturating_sub(MAX_CACHE_LEVEL_DISTANCE);
        let retained = self.sources_by_target_level.split_off(&retained_target_level);
        let expired = std::mem::replace(&mut self.sources_by_target_level, retained);
        for source_revisions in expired.into_values() {
            for source_revision in source_revisions {
                self.slots_by_source.remove(&source_revision);
            }
        }
        self.destination_heat.prune(head_high_level);
    }
}

#[derive(Default)]
struct DestinationHeat {
    counts_by_revision: HashMap<u64, u64>,
    revisions_by_level: BTreeMap<u32, Vec<u64>>,
}

impl DestinationHeat {
    fn record(&mut self, destination_revision: u64, destination_high_level: u32) {
        if let Some(count) = self.counts_by_revision.get_mut(&destination_revision) {
            *count = count.saturating_add(1);
        } else {
            self.counts_by_revision.insert(destination_revision, 1);
            self.revisions_by_level.entry(destination_high_level).or_default().push(destination_revision);
        }
    }

    fn prune(&mut self, head_high_level: u32) {
        let retained_level = head_high_level.saturating_sub(HOT_DESTINATION_LEVEL_DISTANCE);
        let retained = self.revisions_by_level.split_off(&retained_level);
        let expired = std::mem::replace(&mut self.revisions_by_level, retained);
        for destination_revisions in expired.into_values() {
            for destination_revision in destination_revisions {
                self.counts_by_revision.remove(&destination_revision);
            }
        }
    }
}

struct CacheSlot<T> {
    completed: Option<Arc<T>>,
    running: Option<Arc<CacheJob<T>>>,
}

impl<T> Default for CacheSlot<T> {
    fn default() -> Self {
        Self { completed: None, running: None }
    }
}

/// Existing value or shared construction job selected by one cache access.
pub(crate) enum CacheAccess<T> {
    /// A completed immutable value is available immediately.
    Completed(Arc<T>),
    /// Construction remains in progress and may be awaited independently.
    Job(Arc<CacheJob<T>>),
}

enum CacheJobState<T> {
    Pending,
    Complete(Arc<Result<Arc<T>, CacheBuildError>>),
}

impl<T> Clone for CacheJobState<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Pending => Self::Pending,
            Self::Complete(result) => Self::Complete(result.clone()),
        }
    }
}

/// One shared pending-or-terminal cache construction.
pub(crate) struct CacheJob<T> {
    state: watch::Sender<CacheJobState<T>>,
}

impl<T> CacheJob<T> {
    fn pending() -> Self {
        let (state, _) = watch::channel(CacheJobState::Pending);
        Self { state }
    }

    /// Waits for the shared result while allowing this waiter to detach independently.
    pub(crate) async fn wait(&self, cancellation: CancellationToken) -> Result<Arc<T>, CacheWaitError> {
        let mut state = self.state.subscribe();
        loop {
            let terminal = match &*state.borrow() {
                CacheJobState::Pending => None,
                CacheJobState::Complete(result) => Some(result.clone()),
            };
            if let Some(result) = terminal {
                return match result.as_ref() {
                    Ok(value) => Ok(value.clone()),
                    Err(error) => Err((*error).into()),
                };
            }

            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(CacheWaitError::Cancelled),
                changed = state.changed() => {
                    assert!(changed.is_ok(), "a cache job owns its watch sender through terminal completion");
                }
            }
        }
    }

    fn complete(&self, result: Result<Arc<T>, CacheBuildError>) {
        self.state.send_replace(CacheJobState::Complete(Arc::new(result)));
    }
}

fn finish_delta_job(
    cache: &Weak<GraphCache>,
    source_revision: u64,
    job: &Arc<CacheJob<CachedDelta>>,
    result: Result<(Arc<CachedDelta>, u32), CacheBuildError>,
) {
    if let Some(cache) = cache.upgrade() {
        cache.lock_state().deltas.finish(source_revision, job, &result);
    }
    job.complete(result.map(|(value, _)| value));
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicU32, AtomicUsize, Ordering},
        },
    };

    use bytes::Bytes;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    use super::{CacheAccess, CacheBuildError, CacheJobTracker, CacheWaitError, CachedDelta, EncodingSchedulerError, GraphCache};

    fn cached_delta(source: u64, target: u64) -> Arc<CachedDelta> {
        cached_delta_at(source, target, 17)
    }

    fn cached_delta_at(source: u64, target: u64, high_level: u32) -> Arc<CachedDelta> {
        Arc::new(CachedDelta::new(source, target, high_level, 23, Bytes::from_static(b"gzip")))
    }

    fn delta<F, Fut>(
        cache: &Arc<GraphCache>,
        jobs: &CacheJobTracker,
        source_revision: u64,
        completion_head_high_level: u32,
        builder: F,
    ) -> CacheAccess<CachedDelta>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedDelta>, CacheBuildError>> + Send + 'static,
    {
        cache.delta(jobs, source_revision, builder, move || async move { completion_head_high_level })
    }

    fn job(access: CacheAccess<CachedDelta>) -> Arc<super::CacheJob<CachedDelta>> {
        match access {
            CacheAccess::Job(job) => job,
            CacheAccess::Completed(_) => panic!("expected shared cache job"),
        }
    }

    #[tokio::test]
    async fn concurrent_misses_install_one_builder_and_cancel_only_one_waiter() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let builds = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = oneshot::channel();
        let builds_for_job = builds.clone();

        let first = job(delta(&cache, &jobs, 7, 17, move |_| async move {
            builds_for_job.fetch_add(1, Ordering::SeqCst);
            release_rx.await.expect("builder is released");
            Ok(cached_delta(7, 11))
        }));
        let second = job(delta(&cache, &jobs, 7, 17, |_| async { panic!("a joined miss cannot become a second builder") }));
        assert!(Arc::ptr_eq(&first, &second));

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert_eq!(first.wait(cancelled).await, Err(CacheWaitError::Cancelled));

        release_tx.send(()).expect("builder release is observed");
        let completed = second.wait(CancellationToken::new()).await.expect("remaining waiter receives success");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        match delta(&cache, &jobs, 7, 17, |_| async { panic!("a completed hit cannot rebuild") }) {
            CacheAccess::Completed(hit) => assert!(Arc::ptr_eq(&hit, &completed)),
            CacheAccess::Job(_) => panic!("completed cache entry is reused"),
        }

        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn failure_removes_the_exact_running_job_before_notifying_waiters() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let failed = job(delta(&cache, &jobs, 5, 17, |_| async {
            Err(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed))
        }));
        assert_eq!(
            failed.wait(CancellationToken::new()).await,
            Err(CacheWaitError::Build(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed)))
        );

        let replacement = job(delta(&cache, &jobs, 5, 17, |_| async { Ok(cached_delta(5, 8)) }));
        assert!(!Arc::ptr_eq(&failed, &replacement));
        replacement.wait(CancellationToken::new()).await.expect("immediate retry succeeds");
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn dropped_origin_cache_skips_insertion_but_preserves_waiter_completion() {
        let cache = Arc::new(GraphCache::new());
        let weak_cache = Arc::downgrade(&cache);
        let jobs = CacheJobTracker::new();
        let (release_tx, release_rx) = oneshot::channel();
        let running = job(delta(&cache, &jobs, 3, 17, move |_| async move {
            release_rx.await.expect("detached builder is released");
            Ok(cached_delta(3, 4))
        }));

        drop(cache);
        assert!(weak_cache.upgrade().is_none(), "the task retains only a weak cache reference");
        release_tx.send(()).expect("detached builder release is observed");
        assert_eq!(running.wait(CancellationToken::new()).await.expect("existing waiter still completes").to_revision_id(), 4);
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn terminal_watch_value_survives_when_every_initial_waiter_detaches() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let (release_tx, release_rx) = oneshot::channel();
        let running = job(delta(&cache, &jobs, 13, 17, move |_| async move {
            release_rx.await.expect("builder is released");
            Ok(cached_delta(13, 21))
        }));

        release_tx.send(()).expect("builder release is observed");
        tokio::task::yield_now().await;
        assert_eq!(
            running.wait(CancellationToken::new()).await.expect("late subscriber sees send_replace value").to_revision_id(),
            21
        );
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn tracker_shutdown_cancels_jobs_clears_slots_and_rejects_new_registration() {
        let cache = Arc::new(GraphCache::new());
        let jobs = Arc::new(CacheJobTracker::new());
        let running =
            job(delta(&cache, &jobs, 34, 17, |_| async { std::future::pending::<Result<Arc<CachedDelta>, CacheBuildError>>().await }));

        jobs.shutdown().await;
        assert_eq!(running.wait(CancellationToken::new()).await, Err(CacheWaitError::Build(CacheBuildError::Cancelled)));

        let rejected = job(delta(&cache, &jobs, 34, 17, |_| async { Ok(cached_delta(34, 35)) }));
        assert!(!Arc::ptr_eq(&running, &rejected));
        assert_eq!(rejected.wait(CancellationToken::new()).await, Err(CacheWaitError::Build(CacheBuildError::Cancelled)));
    }

    #[tokio::test]
    async fn completed_delta_remains_immutable_at_its_exact_source_key() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let first = job(delta(&cache, &jobs, 7, 17, |_| async { Ok(cached_delta(7, 11)) }));
        let completed = first.wait(CancellationToken::new()).await.expect("initial build completes");

        match delta(&cache, &jobs, 7, 17, |_| async { Ok(cached_delta(7, 19)) }) {
            CacheAccess::Completed(hit) => {
                assert!(Arc::ptr_eq(&hit, &completed));
                assert_eq!(hit.to_revision_id(), 11, "Head advancement cannot promote a source-keyed entry");
            }
            CacheAccess::Job(_) => panic!("the exact source reuses its immutable value"),
        }

        let distinct = job(delta(&cache, &jobs, 8, 17, |_| async { Ok(cached_delta(8, 19)) }));
        assert_eq!(distinct.wait(CancellationToken::new()).await.expect("distinct source builds independently").to_revision_id(), 19);
        jobs.shutdown().await;
    }

    #[test]
    fn destination_heat_counts_saturates_and_prunes_complete_level_buckets() {
        let cache = GraphCache::new();
        cache.record_delta_delivery(21, 20);
        cache.record_delta_delivery(21, 20);
        cache.record_delta_delivery(34, 20);
        cache.record_delta_delivery(55, 21);
        {
            let state = cache.lock_state();
            assert_eq!(state.deltas.destination_heat.counts_by_revision.get(&21), Some(&2));
            assert_eq!(state.deltas.destination_heat.counts_by_revision.get(&34), Some(&1));
            assert_eq!(state.deltas.destination_heat.revisions_by_level.get(&20), Some(&vec![21, 34]));
            assert_eq!(state.deltas.destination_heat.revisions_by_level.get(&21), Some(&vec![55]));
        }

        cache.lock_state().deltas.destination_heat.counts_by_revision.insert(21, u64::MAX);
        cache.record_delta_delivery(21, 20);
        assert_eq!(cache.lock_state().deltas.destination_heat.counts_by_revision.get(&21), Some(&u64::MAX));

        cache.prune_deltas(270);
        assert_eq!(cache.lock_state().deltas.destination_heat.counts_by_revision.len(), 3, "distance 250 remains retained");

        cache.prune_deltas(271);
        let state = cache.lock_state();
        assert_eq!(state.deltas.destination_heat.counts_by_revision, HashMap::from([(55, 1)]));
        assert_eq!(state.deltas.destination_heat.revisions_by_level, BTreeMap::from([(21, vec![55])]));
    }

    #[tokio::test]
    async fn target_level_index_prunes_complete_expired_buckets_only() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        for (source, target, high_level) in [(5, 8, 20), (13, 16, 20), (21, 24, 21)] {
            let completed =
                job(delta(&cache, &jobs, source, 520, move |_| async move { Ok(cached_delta_at(source, target, high_level)) }));
            completed.wait(CancellationToken::new()).await.expect("fixture entry completes");
        }

        cache.prune_deltas(520);
        {
            let state = cache.lock_state();
            assert_eq!(state.deltas.slots_by_source.len(), 3, "distance 500 and younger buckets remain retained");
            assert_eq!(state.deltas.sources_by_target_level.keys().copied().collect::<Vec<_>>(), [20, 21]);
        }

        cache.prune_deltas(521);
        {
            let state = cache.lock_state();
            assert_eq!(state.deltas.slots_by_source.keys().copied().collect::<Vec<_>>(), [21]);
            assert_eq!(state.deltas.sources_by_target_level.keys().copied().collect::<Vec<_>>(), [21]);
        }
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn completion_serves_but_does_not_index_a_candidate_that_aged_past_the_bound() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let (release_tx, release_rx) = oneshot::channel();
        let head_high_level = Arc::new(AtomicU32::new(520));
        let completion_head = Arc::clone(&head_high_level);
        let aged = job(cache.delta(
            &jobs,
            3,
            move |_| async move {
                release_rx.await.expect("builder is released");
                Ok(cached_delta_at(3, 4, 20))
            },
            move || {
                let completion_head = Arc::clone(&completion_head);
                async move { completion_head.load(Ordering::SeqCst) }
            },
        ));

        head_high_level.store(521, Ordering::SeqCst);
        release_tx.send(()).expect("builder release is observed");
        assert_eq!(aged.wait(CancellationToken::new()).await.expect("existing waiter receives successful result").to_revision_id(), 4);
        {
            let state = cache.lock_state();
            assert!(!state.deltas.slots_by_source.contains_key(&3));
            assert!(!state.deltas.sources_by_target_level.contains_key(&20));
        }

        let replacement = job(delta(&cache, &jobs, 3, 521, |_| async { Ok(cached_delta_at(3, 7, 521)) }));
        assert_eq!(replacement.wait(CancellationToken::new()).await.expect("uncached source can retry").to_revision_id(), 7);
        jobs.shutdown().await;
    }
}
