#![allow(dead_code, reason = "the cache kernel is integrated by later API increments")]

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use bytes::Bytes;
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

    fn delta<F, Fut>(self: &Arc<Self>, jobs: &CacheJobTracker, source_revision: u64, builder: F) -> CacheAccess<CachedDelta>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedDelta>, CacheBuildError>> + Send + 'static,
    {
        let access = {
            let mut state = self.lock_state();
            let slot = state.delta_slots.entry(source_revision).or_default();
            if let Some(completed) = &slot.completed {
                CacheAccess::Completed(completed.clone())
            } else if let Some(running) = &slot.running {
                CacheAccess::Job(running.clone())
            } else {
                let job = Arc::new(CacheJob::pending());
                slot.running = Some(job.clone());
                CacheAccess::Builder(job)
            }
        };

        if let CacheAccess::Builder(job) = access {
            self.spawn_delta_builder(jobs, source_revision, job.clone(), builder);
            CacheAccess::Job(job)
        } else {
            access
        }
    }

    fn spawn_delta_builder<F, Fut>(
        self: &Arc<Self>,
        jobs: &CacheJobTracker,
        source_revision: u64,
        job: Arc<CacheJob<CachedDelta>>,
        builder: F,
    ) where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedDelta>, CacheBuildError>> + Send + 'static,
    {
        let cache = Arc::downgrade(self);
        let task_job = job.clone();
        let spawned = jobs.spawn(move |cancellation| async move {
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(CacheBuildError::Cancelled),
                result = builder(cancellation.clone()) => result,
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
    delta_slots: HashMap<u64, CacheSlot<CachedDelta>>,
    head_slots: HashMap<u32, CacheSlot<CachedHeadSnapshot>>,
    destination_heat: HashMap<u64, u64>,
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

enum CacheAccess<T> {
    Completed(Arc<T>),
    Job(Arc<CacheJob<T>>),
    Builder(Arc<CacheJob<T>>),
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

struct CacheJob<T> {
    state: watch::Sender<CacheJobState<T>>,
}

impl<T> CacheJob<T> {
    fn pending() -> Self {
        let (state, _) = watch::channel(CacheJobState::Pending);
        Self { state }
    }

    async fn wait(&self, cancellation: CancellationToken) -> Result<Arc<T>, CacheWaitError> {
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
    result: Result<Arc<CachedDelta>, CacheBuildError>,
) {
    if let Some(cache) = cache.upgrade() {
        let mut state = cache.lock_state();
        let mut remove_slot = false;
        if let Some(slot) = state.delta_slots.get_mut(&source_revision)
            && slot.running.as_ref().is_some_and(|running| Arc::ptr_eq(running, job))
        {
            if let Ok(value) = &result {
                slot.completed = Some(value.clone());
            }
            slot.running = None;
            remove_slot = slot.completed.is_none();
        }
        if remove_slot {
            state.delta_slots.remove(&source_revision);
        }
    }
    job.complete(result);
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    use super::{CacheAccess, CacheBuildError, CacheJobTracker, CacheWaitError, CachedDelta, EncodingSchedulerError, GraphCache};

    fn cached_delta(source: u64, target: u64) -> Arc<CachedDelta> {
        Arc::new(CachedDelta::new(source, target, 17, 23, Bytes::from_static(b"gzip")))
    }

    fn job(access: CacheAccess<CachedDelta>) -> Arc<super::CacheJob<CachedDelta>> {
        match access {
            CacheAccess::Job(job) => job,
            CacheAccess::Completed(_) | CacheAccess::Builder(_) => panic!("expected shared cache job"),
        }
    }

    #[tokio::test]
    async fn concurrent_misses_install_one_builder_and_cancel_only_one_waiter() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let builds = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = oneshot::channel();
        let builds_for_job = builds.clone();

        let first = job(cache.delta(&jobs, 7, move |_| async move {
            builds_for_job.fetch_add(1, Ordering::SeqCst);
            release_rx.await.expect("builder is released");
            Ok(cached_delta(7, 11))
        }));
        let second = job(cache.delta(&jobs, 7, |_| async { panic!("a joined miss cannot become a second builder") }));
        assert!(Arc::ptr_eq(&first, &second));

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert_eq!(first.wait(cancelled).await, Err(CacheWaitError::Cancelled));

        release_tx.send(()).expect("builder release is observed");
        let completed = second.wait(CancellationToken::new()).await.expect("remaining waiter receives success");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        match cache.delta(&jobs, 7, |_| async { panic!("a completed hit cannot rebuild") }) {
            CacheAccess::Completed(hit) => assert!(Arc::ptr_eq(&hit, &completed)),
            CacheAccess::Job(_) | CacheAccess::Builder(_) => panic!("completed cache entry is reused"),
        }

        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn failure_removes_the_exact_running_job_before_notifying_waiters() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let failed =
            job(cache.delta(&jobs, 5, |_| async { Err(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed)) }));
        assert_eq!(
            failed.wait(CancellationToken::new()).await,
            Err(CacheWaitError::Build(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed)))
        );

        let replacement = job(cache.delta(&jobs, 5, |_| async { Ok(cached_delta(5, 8)) }));
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
        let running = job(cache.delta(&jobs, 3, move |_| async move {
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
        let running = job(cache.delta(&jobs, 13, move |_| async move {
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
            job(cache.delta(&jobs, 34, |_| async { std::future::pending::<Result<Arc<CachedDelta>, CacheBuildError>>().await }));

        jobs.shutdown().await;
        assert_eq!(running.wait(CancellationToken::new()).await, Err(CacheWaitError::Build(CacheBuildError::Cancelled)));

        let rejected = job(cache.delta(&jobs, 34, |_| async { Ok(cached_delta(34, 35)) }));
        assert!(!Arc::ptr_eq(&running, &rejected));
        assert_eq!(rejected.wait(CancellationToken::new()).await, Err(CacheWaitError::Build(CacheBuildError::Cancelled)));
    }
}
