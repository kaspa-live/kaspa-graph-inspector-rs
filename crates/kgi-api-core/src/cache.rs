#![allow(dead_code, reason = "the cache kernel is integrated by later API increments")]

use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use bytes::Bytes;
use kgi_api_model::protocol::{CACHED_HEAD_SNAPSHOT_MAX_AGE_MS, CACHED_HEAD_WINDOW_DEPTHS, DELTA_CACHE_MAX_AGE_MS};
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{codec::HeadEtagInput, encoding::scheduler::EncodingSchedulerError};

/// One immutable source-keyed canonical-delta response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CachedDelta {
    from_revision_id: u64,
    to_revision_id: u64,
    size_limited: bool,
    uncompressed_json_bytes: u64,
    gzip_body: Bytes,
}

impl CachedDelta {
    /// Constructs one completely encoded canonical-delta cache value.
    pub(crate) const fn new(
        from_revision_id: u64,
        to_revision_id: u64,
        size_limited: bool,
        uncompressed_json_bytes: u64,
        gzip_body: Bytes,
    ) -> Self {
        Self { from_revision_id, to_revision_id, size_limited, uncompressed_json_bytes, gzip_body }
    }

    /// Returns the exact source revision represented by this entry.
    pub(crate) const fn source_revision_id(&self) -> u64 {
        self.from_revision_id
    }

    /// Returns the fixed target revision represented by this entry.
    pub(crate) const fn to_revision_id(&self) -> u64 {
        self.to_revision_id
    }

    /// Returns whether soft-size descent selected this cached target.
    pub(crate) const fn is_size_limited(&self) -> bool {
        self.size_limited
    }

    /// Returns whether this response satisfies a request for `target_revision`.
    pub(crate) const fn is_adequate_for(&self, target_revision: u64) -> bool {
        self.to_revision_id >= target_revision || self.size_limited
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

/// One coherent publication boundary for cached Head-snapshot eligibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HeadEligibilityBoundary {
    head_revision_timestamp_ms: u64,
    replay_floor_revision: u64,
    oldest_available_revision: u64,
}

impl HeadEligibilityBoundary {
    /// Constructs the eligibility boundary captured from one publication core.
    pub(crate) const fn new(head_revision_timestamp_ms: u64, replay_floor_revision: u64, oldest_available_revision: u64) -> Self {
        Self { head_revision_timestamp_ms, replay_floor_revision, oldest_available_revision }
    }

    /// Returns whether one cached snapshot remains eligible at this boundary.
    pub(crate) fn admits(self, snapshot: &CachedHeadSnapshot) -> bool {
        let age_ms = self
            .head_revision_timestamp_ms
            .checked_sub(snapshot.revision_timestamp_ms())
            .expect("a cached Head snapshot does not postdate its publication Head");
        snapshot.revision() >= self.replay_floor_revision
            && snapshot.revision() >= self.oldest_available_revision
            && age_ms < CACHED_HEAD_SNAPSHOT_MAX_AGE_MS
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
    /// Replay-floor advancement detached this job before it could complete.
    #[error("cache construction was invalidated by replay floor {replay_floor_revision}")]
    InvalidatedByReplayFloor {
        /// The inclusive replay floor which invalidated the job.
        replay_floor_revision: u64,
    },
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
        Self::with_cancellation(CancellationToken::new())
    }

    /// Creates an open tracker using its ApiService-owned cancellation root.
    pub(crate) fn with_cancellation(cancellation: CancellationToken) -> Self {
        Self { registration: Mutex::new(true), cancellation, tasks: TaskTracker::new() }
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

    fn new_job_cancellation(&self) -> CancellationToken {
        self.cancellation.child_token()
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

/// Publication-local completed responses and shared construction jobs.
pub(crate) struct GraphCache {
    state: Mutex<GraphCacheState>,
}

impl GraphCache {
    /// Creates an empty cache for one graph publication.
    pub(crate) fn new() -> Self {
        Self { state: Mutex::new(GraphCacheState::default()) }
    }

    /// Prunes expired sources and inspects one exact delta source without history work.
    pub(crate) fn lookup_delta(
        &self,
        source_revision: u64,
        target_revision: u64,
        head_revision_timestamp_ms: u64,
    ) -> DeltaCacheLookup {
        self.lock_state().deltas.lookup(source_revision, target_revision, head_revision_timestamp_ms)
    }

    /// Re-evaluates one captured delta source and elects at most one builder.
    pub(crate) fn elect_delta<F, Fut>(
        self: &Arc<Self>,
        jobs: &CacheJobTracker,
        source_revision: u64,
        first_revision_timestamp_ms: u64,
        target_revision: u64,
        builder: F,
    ) -> CacheAccess<CachedDelta>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedDelta>, CacheBuildError>> + Send + 'static,
    {
        let (access, builder_job) =
            self.lock_state().deltas.elect(source_revision, first_revision_timestamp_ms, target_revision, jobs.new_job_cancellation());

        if let Some(job) = builder_job {
            self.spawn_delta_builder(jobs, source_revision, job, builder);
        }
        access
    }

    /// Reuses or starts cache work for one exact configured Head tier.
    pub(crate) fn head_tier<F, Fut>(
        self: &Arc<Self>,
        jobs: &CacheJobTracker,
        depth: u32,
        boundary: HeadEligibilityBoundary,
        builder: F,
    ) -> CacheAccess<CachedHeadSnapshot>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedHeadSnapshot>, CacheBuildError>> + Send + 'static,
    {
        assert!(CACHED_HEAD_WINDOW_DEPTHS.contains(&depth), "a Head cache key is one configured tier");
        let (access, builder_job) = self.lock_state().heads.access(depth, boundary, jobs.new_job_cancellation());

        if let Some(job) = builder_job {
            self.spawn_head_builder(jobs, depth, job, builder);
        }
        access
    }

    /// Detaches entries invalidated by a newer replay floor and terminalizes
    /// their running jobs before returning cancellation ownership.
    pub(crate) fn invalidate_replay_floor(&self, replay_floor_revision: u64) -> ReplayFloorInvalidation {
        self.invalidate_replay_floor_with(replay_floor_revision, || {})
    }

    fn invalidate_replay_floor_with(&self, replay_floor_revision: u64, after_detach: impl FnOnce()) -> ReplayFloorInvalidation {
        let mut state = self.lock_state();
        let invalidation = state.detach_before_replay_floor(replay_floor_revision);
        after_detach();
        invalidation.terminalize(replay_floor_revision);
        invalidation
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
            let job_cancellation = task_job.cancellation.clone();
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(CacheBuildError::Cancelled),
                () = task_job.work_cancelled() => Err(CacheBuildError::Cancelled),
                result = builder(job_cancellation) => result,
            };
            finish_delta_job(&cache, source_revision, &task_job, result);
        });
        if !spawned {
            finish_delta_job(&Arc::downgrade(self), source_revision, &job, Err(CacheBuildError::Cancelled));
        }
    }

    fn spawn_head_builder<F, Fut>(
        self: &Arc<Self>,
        jobs: &CacheJobTracker,
        depth: u32,
        job: Arc<CacheJob<CachedHeadSnapshot>>,
        builder: F,
    ) where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedHeadSnapshot>, CacheBuildError>> + Send + 'static,
    {
        let cache = Arc::downgrade(self);
        let task_job = job.clone();
        let spawned = jobs.spawn(move |cancellation| async move {
            let job_cancellation = task_job.cancellation.clone();
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(CacheBuildError::Cancelled),
                () = task_job.work_cancelled() => Err(CacheBuildError::Cancelled),
                result = async {
                    let value = builder(job_cancellation).await?;
                    assert_eq!(value.depth(), depth, "a Head-tier builder returns its selected depth");
                    Ok(value)
                } => result,
            };
            finish_head_job(&cache, depth, &task_job, result);
        });
        if !spawned {
            finish_head_job(&Arc::downgrade(self), depth, &job, Err(CacheBuildError::Cancelled));
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, GraphCacheState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Default)]
struct GraphCacheState {
    deltas: DeltaCache,
    heads: HeadCache,
}

impl GraphCacheState {
    fn detach_before_replay_floor(&mut self, replay_floor_revision: u64) -> ReplayFloorInvalidation {
        let delta_jobs = self.deltas.detach_before_replay_floor(replay_floor_revision);
        let head_jobs = self.heads.detach_before_replay_floor(replay_floor_revision);

        ReplayFloorInvalidation { delta_jobs, head_jobs }
    }
}

#[derive(Default)]
struct HeadCache {
    slots_by_depth: HashMap<u32, HeadCacheSlot>,
}

impl HeadCache {
    fn detach_before_replay_floor(&mut self, replay_floor_revision: u64) -> Vec<Arc<CacheJob<CachedHeadSnapshot>>> {
        let mut jobs = Vec::new();

        self.slots_by_depth.retain(|_, slot| {
            if slot.completed.as_ref().is_some_and(|value| value.revision() < replay_floor_revision) {
                slot.completed = None;
            }
            if let Some(job) = slot.running.take() {
                jobs.push(job);
            }
            slot.completed.is_some()
        });

        jobs
    }

    fn access(
        &mut self,
        depth: u32,
        boundary: HeadEligibilityBoundary,
        cancellation: CancellationToken,
    ) -> (CacheAccess<CachedHeadSnapshot>, Option<Arc<CacheJob<CachedHeadSnapshot>>>) {
        let slot = self.slots_by_depth.entry(depth).or_default();
        if let Some(completed) = &slot.completed {
            if boundary.admits(completed) {
                return (CacheAccess::Completed(completed.clone()), None);
            }
            slot.completed = None;
        }
        if let Some(running) = &slot.running {
            return (CacheAccess::Job(running.clone()), None);
        }
        let job = Arc::new(CacheJob::pending(cancellation));
        slot.running = Some(job.clone());
        (CacheAccess::Job(job.clone()), Some(job))
    }

    fn finish(
        &mut self,
        depth: u32,
        job: &Arc<CacheJob<CachedHeadSnapshot>>,
        result: &Result<Arc<CachedHeadSnapshot>, CacheBuildError>,
    ) {
        let mut remove_slot = false;
        if let Some(slot) = self.slots_by_depth.get_mut(&depth)
            && slot.running.as_ref().is_some_and(|running| Arc::ptr_eq(running, job))
        {
            if let Ok(value) = result {
                assert_eq!(value.depth(), depth, "a completed Head snapshot remains keyed by its derived tier");
                slot.completed = Some(value.clone());
            }
            slot.running = None;
            remove_slot = slot.completed.is_none();
        }
        if remove_slot {
            self.slots_by_depth.remove(&depth);
        }
    }
}

#[derive(Default)]
struct DeltaCache {
    slots_by_source: BTreeMap<u64, DeltaCacheSlot>,
}

impl DeltaCache {
    fn detach_before_replay_floor(&mut self, replay_floor_revision: u64) -> Vec<Arc<CacheJob<CachedDelta>>> {
        let retained = self.slots_by_source.split_off(&replay_floor_revision);
        let expired = std::mem::replace(&mut self.slots_by_source, retained);
        expired.into_values().filter_map(|slot| slot.running.map(|running| running.job)).collect()
    }

    fn lookup(&mut self, source_revision: u64, target_revision: u64, head_revision_timestamp_ms: u64) -> DeltaCacheLookup {
        self.prune_expired(head_revision_timestamp_ms);
        let Some(slot) = self.slots_by_source.get(&source_revision) else {
            return DeltaCacheLookup::ConstructionRequired;
        };
        if let Some(completed) = &slot.completed
            && completed.is_adequate_for(target_revision)
        {
            DeltaCacheLookup::Completed(completed.clone())
        } else if let Some(running) = &slot.running {
            DeltaCacheLookup::Running(running.job.clone())
        } else {
            DeltaCacheLookup::ConstructionRequired
        }
    }

    fn elect(
        &mut self,
        source_revision: u64,
        first_revision_timestamp_ms: u64,
        target_revision: u64,
        cancellation: CancellationToken,
    ) -> (CacheAccess<CachedDelta>, Option<Arc<CacheJob<CachedDelta>>>) {
        if let Some(slot) = self.slots_by_source.get_mut(&source_revision) {
            if let Some(completed) = &slot.completed
                && completed.is_adequate_for(target_revision)
            {
                return (CacheAccess::Completed(completed.clone()), None);
            }
            if let Some(running) = &slot.running {
                return (CacheAccess::Job(running.job.clone()), None);
            }

            let job = Arc::new(CacheJob::pending(cancellation));
            slot.running = Some(RunningDeltaJob { target_revision, job: job.clone() });
            return (CacheAccess::Job(job.clone()), Some(job));
        }

        let job = Arc::new(CacheJob::pending(cancellation));
        self.slots_by_source.insert(
            source_revision,
            DeltaCacheSlot {
                first_revision_timestamp_ms,
                size_ceiling_revision: None,
                completed: None,
                running: Some(RunningDeltaJob { target_revision, job: job.clone() }),
            },
        );
        (CacheAccess::Job(job.clone()), Some(job))
    }

    fn finish(&mut self, source_revision: u64, job: &Arc<CacheJob<CachedDelta>>, result: &Result<Arc<CachedDelta>, CacheBuildError>) {
        let mut remove_slot = false;
        if let Some(slot) = self.slots_by_source.get_mut(&source_revision)
            && slot.running.as_ref().is_some_and(|running| Arc::ptr_eq(&running.job, job))
        {
            if let Ok(value) = result {
                assert_eq!(value.source_revision_id(), source_revision, "a completed delta remains keyed by its captured source");
                assert_eq!(
                    Some(value.to_revision_id()),
                    slot.running.as_ref().map(|running| running.target_revision),
                    "a completed delta reaches its elected target"
                );
                if slot.completed.as_ref().is_none_or(|completed| value.to_revision_id() > completed.to_revision_id()) {
                    slot.completed = Some(value.clone());
                }
            }
            slot.running = None;
            remove_slot = slot.completed.is_none();
        }
        if remove_slot {
            self.slots_by_source.remove(&source_revision);
        }
    }

    fn prune_expired(&mut self, head_revision_timestamp_ms: u64) {
        let Some((&first_source, first_slot)) = self.slots_by_source.first_key_value() else {
            return;
        };
        if !Self::is_expired(first_slot, head_revision_timestamp_ms) {
            return;
        }

        let (&last_source, last_slot) = self.slots_by_source.last_key_value().expect("a cache with a first slot has a last slot");
        if Self::is_expired(last_slot, head_revision_timestamp_ms) {
            self.slots_by_source.clear();
            return;
        }

        // Search the numeric source-revision domain. For sparse cache keys,
        // range(midpoint..) supplies the first actual source at or after the
        // midpoint. The first delivered revision is source + 1, so stored
        // timestamps preserve this ordering.
        let mut expired = first_source;
        let mut retained = last_source;
        while retained - expired > 1 {
            let midpoint = expired + (retained - expired) / 2;
            let (_, candidate) =
                self.slots_by_source.range(midpoint..).next().expect("the binary-search midpoint does not exceed the last source");
            if Self::is_expired(candidate, head_revision_timestamp_ms) {
                expired = midpoint;
            } else {
                retained = midpoint;
            }
        }

        let first_retained_source = *self.slots_by_source.range(retained..).next().expect("an unexpired suffix has a first source").0;
        self.slots_by_source = self.slots_by_source.split_off(&first_retained_source);
    }

    fn is_expired(slot: &DeltaCacheSlot, head_revision_timestamp_ms: u64) -> bool {
        head_revision_timestamp_ms
            .checked_sub(slot.first_revision_timestamp_ms)
            .expect("a delta source timestamp does not postdate its publication Head")
            >= DELTA_CACHE_MAX_AGE_MS
    }
}

struct DeltaCacheSlot {
    first_revision_timestamp_ms: u64,
    size_ceiling_revision: Option<u64>,
    completed: Option<Arc<CachedDelta>>,
    running: Option<RunningDeltaJob>,
}

struct RunningDeltaJob {
    target_revision: u64,
    job: Arc<CacheJob<CachedDelta>>,
}

#[derive(Default)]
struct HeadCacheSlot {
    completed: Option<Arc<CachedHeadSnapshot>>,
    running: Option<Arc<CacheJob<CachedHeadSnapshot>>>,
}

/// Result of one pruned exact-source delta-cache lookup.
pub(crate) enum DeltaCacheLookup {
    /// A completed immutable value is available at this source.
    Completed(Arc<CachedDelta>),
    /// Construction for this source is already running.
    Running(Arc<CacheJob<CachedDelta>>),
    /// The caller must consult history before the second cache election.
    ConstructionRequired,
}

/// Existing value or shared construction job selected by one cache access.
pub(crate) enum CacheAccess<T> {
    /// A completed immutable value is available immediately.
    Completed(Arc<T>),
    /// Construction remains in progress and may be awaited independently.
    Job(Arc<CacheJob<T>>),
}

impl<T> CacheAccess<T> {
    /// Resolves the completed value or shared construction job.
    pub(crate) async fn resolve(self, cancellation: CancellationToken) -> Result<Arc<T>, CacheWaitError> {
        match self {
            Self::Completed(value) => Ok(value),
            Self::Job(job) => job.wait(cancellation).await,
        }
    }
}

enum CacheJobState<T> {
    Pending,
    Complete(Arc<Result<Arc<T>, CacheBuildError>>),
    InvalidatedByReplayFloor { replay_floor_revision: u64 },
}

impl<T> Clone for CacheJobState<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Pending => Self::Pending,
            Self::Complete(result) => Self::Complete(result.clone()),
            Self::InvalidatedByReplayFloor { replay_floor_revision } => {
                Self::InvalidatedByReplayFloor { replay_floor_revision: *replay_floor_revision }
            }
        }
    }
}

/// One shared pending-or-terminal cache construction.
pub(crate) struct CacheJob<T> {
    cancellation: CancellationToken,
    state: watch::Sender<CacheJobState<T>>,
}

impl<T> CacheJob<T> {
    fn pending(cancellation: CancellationToken) -> Self {
        let (state, _) = watch::channel(CacheJobState::Pending);
        Self { cancellation, state }
    }

    /// Waits for the shared result while allowing this waiter to detach independently.
    pub(crate) async fn wait(&self, cancellation: CancellationToken) -> Result<Arc<T>, CacheWaitError> {
        let mut state = self.state.subscribe();
        loop {
            let terminal = match &*state.borrow() {
                CacheJobState::Pending => None,
                CacheJobState::Complete(result) => Some(Ok(result.clone())),
                CacheJobState::InvalidatedByReplayFloor { replay_floor_revision } => Some(Err(*replay_floor_revision)),
            };
            if let Some(terminal) = terminal {
                return match terminal {
                    Ok(result) => match result.as_ref() {
                        Ok(value) => Ok(value.clone()),
                        Err(error) => Err((*error).into()),
                    },
                    Err(replay_floor_revision) => Err(CacheWaitError::InvalidatedByReplayFloor { replay_floor_revision }),
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
        let mut result = Some(Arc::new(result));
        self.state.send_if_modified(|state| match state {
            CacheJobState::Pending => {
                *state = CacheJobState::Complete(result.take().expect("one completion result is installed once"));
                true
            }
            CacheJobState::InvalidatedByReplayFloor { .. } => false,
            CacheJobState::Complete(_) => panic!("a cache job has exactly one ordinary completion"),
        });
    }

    fn invalidate_by_replay_floor(&self, replay_floor_revision: u64) {
        let changed = self.state.send_if_modified(|state| match state {
            CacheJobState::Pending => {
                *state = CacheJobState::InvalidatedByReplayFloor { replay_floor_revision };
                true
            }
            CacheJobState::Complete(_) | CacheJobState::InvalidatedByReplayFloor { .. } => false,
        });
        assert!(changed, "a running cache slot contains a pending job during replay-floor invalidation");
    }

    fn cancel_work(&self) {
        self.cancellation.cancel();
    }

    async fn work_cancelled(&self) {
        self.cancellation.cancelled().await;
    }
}

/// Cache jobs made terminal by one replay-floor invalidation.
///
/// The graph-cache mutex is no longer held when the caller receives this
/// value, so cancelling the detached construction work cannot lengthen the
/// cache critical section.
pub(crate) struct ReplayFloorInvalidation {
    delta_jobs: Vec<Arc<CacheJob<CachedDelta>>>,
    head_jobs: Vec<Arc<CacheJob<CachedHeadSnapshot>>>,
}

impl ReplayFloorInvalidation {
    fn terminalize(&self, replay_floor_revision: u64) {
        for job in &self.delta_jobs {
            job.invalidate_by_replay_floor(replay_floor_revision);
        }
        for job in &self.head_jobs {
            job.invalidate_by_replay_floor(replay_floor_revision);
        }
    }

    /// Cancels every detached construction without waiting for task exit.
    pub(crate) fn cancel(self) {
        for job in self.delta_jobs {
            job.cancel_work();
        }
        for job in self.head_jobs {
            job.cancel_work();
        }
    }
}

fn finish_delta_job(
    cache: &Weak<GraphCache>,
    source_revision: u64,
    job: &Arc<CacheJob<CachedDelta>>,
    result: Result<Arc<CachedDelta>, CacheBuildError>,
) {
    if let Some(cache) = cache.upgrade() {
        cache.lock_state().deltas.finish(source_revision, job, &result);
    }
    job.complete(result);
}

fn finish_head_job(
    cache: &Weak<GraphCache>,
    depth: u32,
    job: &Arc<CacheJob<CachedHeadSnapshot>>,
    result: Result<Arc<CachedHeadSnapshot>, CacheBuildError>,
) {
    if let Some(cache) = cache.upgrade() {
        cache.lock_state().heads.finish(depth, job, &result);
    }
    job.complete(result);
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{
            Arc, Barrier,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::Bytes;
    use kgi_api_model::{
        graph::LevelExtent,
        protocol::{CACHED_HEAD_SNAPSHOT_MAX_AGE_MS, CACHED_HEAD_WINDOW_DEPTHS, DELTA_CACHE_MAX_AGE_MS},
    };
    use tokio::sync::{Notify, oneshot};
    use tokio_util::sync::CancellationToken;

    use crate::codec::HeadEtagInput;

    use super::{
        CacheAccess, CacheBuildError, CacheJobTracker, CacheWaitError, CachedDelta, CachedHeadSnapshot, DeltaCache, DeltaCacheLookup,
        DeltaCacheSlot, EncodingSchedulerError, GraphCache, HeadEligibilityBoundary, RunningDeltaJob,
    };

    fn cached_delta(source: u64, target: u64) -> Arc<CachedDelta> {
        Arc::new(CachedDelta::new(source, target, false, 23, Bytes::from_static(b"gzip")))
    }

    fn cached_head(depth: u32, revision: u64, high_level: u32) -> Arc<CachedHeadSnapshot> {
        cached_head_with_body(depth, revision, high_level, Bytes::from(vec![u8::try_from(revision).unwrap_or(u8::MAX)]))
    }

    fn cached_head_with_body(depth: u32, revision: u64, high_level: u32, gzip_body: Bytes) -> Arc<CachedHeadSnapshot> {
        let low_level = high_level.saturating_sub(depth - 1).max(1);
        let extent = LevelExtent::new(low_level, high_level).expect("cached Head extent is valid");
        Arc::new(CachedHeadSnapshot::new(
            depth,
            revision,
            revision * 10,
            low_level,
            high_level,
            23,
            HeadEtagInput::new(7, revision, extent),
            gzip_body,
        ))
    }

    fn seed_head(cache: &GraphCache, value: Arc<CachedHeadSnapshot>) {
        let depth = value.depth();
        cache.lock_state().heads.slots_by_depth.entry(depth).or_default().completed = Some(value);
    }

    fn seed_delta(cache: &GraphCache, value: Arc<CachedDelta>) {
        let source_revision = value.source_revision_id();
        cache.lock_state().deltas.slots_by_source.insert(
            source_revision,
            DeltaCacheSlot {
                first_revision_timestamp_ms: source_revision + 1,
                size_ceiling_revision: None,
                completed: Some(value),
                running: None,
            },
        );
    }

    fn running_delta_slot(source_revision: u64, target_revision: u64, job: Arc<super::CacheJob<CachedDelta>>) -> DeltaCacheSlot {
        DeltaCacheSlot {
            first_revision_timestamp_ms: source_revision + 1,
            size_ceiling_revision: None,
            completed: None,
            running: Some(RunningDeltaJob { target_revision, job }),
        }
    }

    fn head_boundary() -> HeadEligibilityBoundary {
        HeadEligibilityBoundary::new(1_000, 0, 0)
    }

    fn delta<F, Fut>(
        cache: &Arc<GraphCache>,
        jobs: &CacheJobTracker,
        source_revision: u64,
        target_revision: u64,
        builder: F,
    ) -> CacheAccess<CachedDelta>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedDelta>, CacheBuildError>> + Send + 'static,
    {
        match cache.lookup_delta(source_revision, target_revision, 1_000) {
            DeltaCacheLookup::Completed(value) => CacheAccess::Completed(value),
            DeltaCacheLookup::Running(job) => CacheAccess::Job(job),
            DeltaCacheLookup::ConstructionRequired => {
                cache.elect_delta(jobs, source_revision, source_revision + 1, target_revision, builder)
            }
        }
    }

    fn job<T>(access: CacheAccess<T>) -> Arc<super::CacheJob<T>> {
        match access {
            CacheAccess::Job(job) => job,
            CacheAccess::Completed(_) => panic!("expected shared cache job"),
        }
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn response_cache_constants_match_the_configured_bounds() {
        assert_eq!(CACHED_HEAD_WINDOW_DEPTHS, [20, 50, 100, 150, 200, 250]);
        assert_eq!(CACHED_HEAD_SNAPSHOT_MAX_AGE_MS, 90_000);
        assert_eq!(DELTA_CACHE_MAX_AGE_MS, 300_000);
    }

    #[test]
    fn delta_age_pruning_binary_searches_sparse_sources_and_preserves_the_unexpired_suffix() {
        let mut cache = DeltaCache::default();
        for (source_revision, first_revision_timestamp_ms) in [(1, 0), (10, 0), (1_000_000, 1), (u64::MAX - 1, 1)] {
            cache.slots_by_source.insert(
                source_revision,
                DeltaCacheSlot {
                    first_revision_timestamp_ms,
                    size_ceiling_revision: None,
                    completed: Some(cached_delta(source_revision, source_revision + 1)),
                    running: None,
                },
            );
        }

        cache.prune_expired(DELTA_CACHE_MAX_AGE_MS - 1);
        assert_eq!(cache.slots_by_source.keys().copied().collect::<Vec<_>>(), [1, 10, 1_000_000, u64::MAX - 1]);

        cache.prune_expired(DELTA_CACHE_MAX_AGE_MS);
        assert_eq!(cache.slots_by_source.keys().copied().collect::<Vec<_>>(), [1_000_000, u64::MAX - 1]);
    }

    #[test]
    #[should_panic(expected = "a delta source timestamp does not postdate its publication Head")]
    fn delta_age_pruning_rejects_a_source_timestamp_from_the_future() {
        let mut cache = DeltaCache::default();
        cache.slots_by_source.insert(
            10,
            DeltaCacheSlot {
                first_revision_timestamp_ms: 11,
                size_ceiling_revision: None,
                completed: Some(cached_delta(10, 11)),
                running: None,
            },
        );

        cache.prune_expired(10);
    }

    #[tokio::test]
    async fn expired_running_delta_detaches_without_cancellation_or_cache_resurrection() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let started = Arc::new(Notify::new());
        let started_builder = started.clone();
        let (release_tx, release_rx) = oneshot::channel();
        let running = job(cache.elect_delta(&jobs, 3, 0, 4, move |_| async move {
            started_builder.notify_one();
            release_rx.await.expect("expired delta builder is released");
            Ok(cached_delta(3, 4))
        }));
        started.notified().await;

        assert!(matches!(cache.lookup_delta(3, 4, DELTA_CACHE_MAX_AGE_MS), DeltaCacheLookup::ConstructionRequired));
        assert!(!running.cancellation.is_cancelled(), "age pruning detaches rather than cancelling the running job");

        release_tx.send(()).expect("expired delta builder release is observed");
        assert_eq!(
            running.wait(CancellationToken::new()).await.expect("the admitted waiter receives the detached result").to_revision_id(),
            4
        );
        assert!(matches!(cache.lookup_delta(3, 4, DELTA_CACHE_MAX_AGE_MS), DeltaCacheLookup::ConstructionRequired));
        jobs.shutdown().await;
    }

    #[test]
    fn head_eligibility_boundary_applies_age_and_both_revision_floors() {
        let snapshot = cached_head(50, 1, 100);

        assert!(HeadEligibilityBoundary::new(90_009, 1, 1).admits(&snapshot));
        assert!(!HeadEligibilityBoundary::new(90_010, 1, 1).admits(&snapshot));
        assert!(!HeadEligibilityBoundary::new(10, 2, 1).admits(&snapshot));
        assert!(!HeadEligibilityBoundary::new(10, 1, 2).admits(&snapshot));
    }

    #[test]
    #[should_panic(expected = "a cached Head snapshot does not postdate its publication Head")]
    fn head_eligibility_rejects_a_snapshot_from_the_future() {
        let _ = HeadEligibilityBoundary::new(9, 0, 0).admits(&cached_head(50, 1, 100));
    }

    #[test]
    fn cached_delta_carries_only_its_canonical_response_identity_and_body() {
        let body = Bytes::from_static(b"gzip");
        let value = CachedDelta::new(7, 11, true, 23, body.clone());

        assert_eq!(value.source_revision_id(), 7);
        assert_eq!(value.to_revision_id(), 11);
        assert!(value.is_size_limited());
        assert_eq!(value.uncompressed_json_bytes(), 23);
        assert_eq!(value.gzip_body(), &body);
    }

    #[test]
    fn head_tier_access_uses_only_the_exact_configured_slot() {
        let cache = GraphCache::new();
        let exact = cached_head(50, 1, 100);
        let larger = cached_head(100, 2, 100);
        seed_head(&cache, exact.clone());
        seed_head(&cache, larger.clone());
        let (selection, builder_job) = cache.lock_state().heads.access(50, head_boundary(), CancellationToken::new());
        assert!(matches!(selection, CacheAccess::Completed(value) if Arc::ptr_eq(&value, &exact)));
        assert!(builder_job.is_none());

        cache.lock_state().heads.slots_by_depth.get_mut(&50).expect("exact slot exists").completed = None;
        let (selection, builder_job) = cache.lock_state().heads.access(50, head_boundary(), CancellationToken::new());
        assert!(matches!(&selection, CacheAccess::Job(job) if builder_job.as_ref().is_some_and(|builder| Arc::ptr_eq(job, builder))));
        assert!(cache.lock_state().heads.slots_by_depth.get(&100).is_some_and(|slot| {
            slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &larger)) && slot.running.is_none()
        }));
    }

    #[test]
    fn obsolete_head_completion_is_removed_before_joining_or_installing_exact_work() {
        let cache = GraphCache::new();
        let obsolete = cached_head(50, 1, 100);
        seed_head(&cache, obsolete.clone());

        let (first, builder_job) =
            cache.lock_state().heads.access(50, HeadEligibilityBoundary::new(1_000, 0, 2), CancellationToken::new());
        let first = job(first);
        assert!(builder_job.as_ref().is_some_and(|builder| Arc::ptr_eq(builder, &first)));
        assert!(cache.lock_state().heads.slots_by_depth.get(&50).is_some_and(|slot| slot.completed.is_none()));

        let (joined, builder_job) = cache.lock_state().heads.access(50, head_boundary(), CancellationToken::new());
        assert!(matches!(joined, CacheAccess::Job(job) if Arc::ptr_eq(&job, &first)));
        assert!(builder_job.is_none());
    }

    #[tokio::test]
    async fn exact_head_tier_misses_share_one_job_without_consulting_a_larger_tier() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let larger = cached_head(100, 1, 100);
        seed_head(&cache, larger.clone());
        let (release_tx, release_rx) = oneshot::channel();

        let first = job(cache.head_tier(&jobs, 50, head_boundary(), move |_| async move {
            release_rx.await.expect("Head builder is released");
            Ok(cached_head(50, 2, 100))
        }));
        let second =
            job(cache
                .head_tier(&jobs, 50, head_boundary(), |_| async { panic!("an exact-tier waiter cannot become a second builder") }));
        assert!(Arc::ptr_eq(&first, &second));
        assert!(cache.lock_state().heads.slots_by_depth.get(&100).is_some_and(|slot| {
            slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &larger)) && slot.running.is_none()
        }));

        release_tx.send(()).expect("Head builder release is observed");
        let completed = first.wait(CancellationToken::new()).await.expect("exact-tier construction completes");
        assert!(Arc::ptr_eq(&second.wait(CancellationToken::new()).await.expect("joined waiter receives the same value"), &completed));
        assert!(matches!(
            cache.head_tier(&jobs, 50, head_boundary(), |_| async { panic!("an eligible exact hit cannot rebuild") }),
            CacheAccess::Completed(value) if Arc::ptr_eq(&value, &completed)
        ));
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn head_tier_job_retains_only_a_weak_cache_reference() {
        let cache = Arc::new(GraphCache::new());
        let weak_cache = Arc::downgrade(&cache);
        let jobs = CacheJobTracker::new();
        let (release_tx, release_rx) = oneshot::channel();
        let running = job(cache.head_tier(&jobs, 20, head_boundary(), move |_| async move {
            release_rx.await.expect("detached Head builder is released");
            Ok(cached_head(20, 1, 20))
        }));

        drop(cache);
        assert!(weak_cache.upgrade().is_none());
        release_tx.send(()).expect("detached Head builder release is observed");
        assert_eq!(running.wait(CancellationToken::new()).await.expect("existing waiter completes").revision(), 1);
        jobs.shutdown().await;
    }

    #[test]
    #[should_panic(expected = "a Head cache key is one configured tier")]
    fn unconfigured_head_tier_is_an_internal_invariant_failure() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let _ = cache.head_tier(&jobs, 21, head_boundary(), |_| async { Ok(cached_head(21, 1, 21)) });
    }

    #[tokio::test]
    async fn concurrent_misses_install_one_builder_and_cancel_only_one_waiter() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let builds = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = oneshot::channel();
        let builds_for_job = builds.clone();

        let first = job(delta(&cache, &jobs, 7, 11, move |_| async move {
            builds_for_job.fetch_add(1, Ordering::SeqCst);
            release_rx.await.expect("builder is released");
            Ok(cached_delta(7, 11))
        }));
        let second = job(delta(&cache, &jobs, 7, 11, |_| async { panic!("a joined miss cannot become a second builder") }));
        assert!(Arc::ptr_eq(&first, &second));

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert_eq!(first.wait(cancelled).await, Err(CacheWaitError::Cancelled));

        release_tx.send(()).expect("builder release is observed");
        let completed = second.wait(CancellationToken::new()).await.expect("remaining waiter receives success");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        match delta(&cache, &jobs, 7, 11, |_| async { panic!("a completed hit cannot rebuild") }) {
            CacheAccess::Completed(hit) => assert!(Arc::ptr_eq(&hit, &completed)),
            CacheAccess::Job(_) => panic!("completed cache entry is reused"),
        }

        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn second_delta_election_discards_a_redundant_capture_after_an_adequate_completion() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        assert!(matches!(cache.lookup_delta(7, 11, 1_000), DeltaCacheLookup::ConstructionRequired));

        let completed = cache
            .elect_delta(&jobs, 7, 8, 11, |_| async { Ok(cached_delta(7, 11)) })
            .resolve(CancellationToken::new())
            .await
            .expect("competing construction completes");

        match cache.elect_delta(&jobs, 7, 8, 11, |_| async { panic!("the redundant capture cannot become a builder") }) {
            CacheAccess::Completed(value) => assert!(Arc::ptr_eq(&value, &completed)),
            CacheAccess::Job(_) => panic!("the second election observes the adequate completion"),
        }
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn farther_waiter_joins_the_current_job_then_reenters_for_enlargement() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let (release_tx, release_rx) = oneshot::channel();
        let shorter = job(delta(&cache, &jobs, 7, 11, move |_| async move {
            release_rx.await.expect("shorter construction is released");
            Ok(cached_delta(7, 11))
        }));

        let farther_waiter = job(delta(&cache, &jobs, 7, 19, |_| async { panic!("the farther waiter joins current work") }));
        assert!(Arc::ptr_eq(&shorter, &farther_waiter));
        release_tx.send(()).expect("shorter construction release is observed");
        assert_eq!(farther_waiter.wait(CancellationToken::new()).await.expect("joined work completes").to_revision_id(), 11);

        let enlargement = job(delta(&cache, &jobs, 7, 19, |_| async { Ok(cached_delta(7, 19)) }));
        assert!(!Arc::ptr_eq(&shorter, &enlargement));
        assert_eq!(enlargement.wait(CancellationToken::new()).await.expect("re-elected enlargement completes").to_revision_id(), 19);
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn failed_enlargement_preserves_the_previous_completed_value() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let completed = delta(&cache, &jobs, 7, 11, |_| async { Ok(cached_delta(7, 11)) })
            .resolve(CancellationToken::new())
            .await
            .expect("initial construction completes");

        let failed = job(delta(&cache, &jobs, 7, 19, |_| async {
            Err(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed))
        }));
        assert_eq!(
            failed.wait(CancellationToken::new()).await,
            Err(CacheWaitError::Build(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed)))
        );

        match delta(&cache, &jobs, 7, 11, |_| async { panic!("the preserved completion remains available") }) {
            CacheAccess::Completed(value) => assert!(Arc::ptr_eq(&value, &completed)),
            CacheAccess::Job(_) => panic!("ordinary enlargement failure preserves the completed value"),
        }
        let retry = job(delta(&cache, &jobs, 7, 19, |_| async { Ok(cached_delta(7, 19)) }));
        assert!(!Arc::ptr_eq(&failed, &retry));
        retry.wait(CancellationToken::new()).await.expect("enlargement can be retried");
        jobs.shutdown().await;
    }

    #[test]
    fn size_limited_delta_is_adequate_for_every_farther_target() {
        let cache = GraphCache::new();
        let value = Arc::new(CachedDelta::new(7, 11, true, 23, Bytes::from_static(b"gzip")));
        seed_delta(&cache, value.clone());

        assert!(matches!(cache.lookup_delta(7, u64::MAX, 1_000), DeltaCacheLookup::Completed(hit) if Arc::ptr_eq(&hit, &value)));
    }

    #[tokio::test]
    async fn failure_removes_the_exact_running_job_before_notifying_waiters() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let failed = job(delta(&cache, &jobs, 5, 8, |_| async {
            Err(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed))
        }));
        assert_eq!(
            failed.wait(CancellationToken::new()).await,
            Err(CacheWaitError::Build(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed)))
        );

        let replacement = job(delta(&cache, &jobs, 5, 8, |_| async { Ok(cached_delta(5, 8)) }));
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
        let running = job(delta(&cache, &jobs, 3, 4, move |_| async move {
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
        let running = job(delta(&cache, &jobs, 13, 21, move |_| async move {
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
            job(delta(&cache, &jobs, 34, 35, |_| async { std::future::pending::<Result<Arc<CachedDelta>, CacheBuildError>>().await }));

        jobs.shutdown().await;
        assert_eq!(running.wait(CancellationToken::new()).await, Err(CacheWaitError::Build(CacheBuildError::Cancelled)));

        let rejected = job(delta(&cache, &jobs, 34, 35, |_| async { Ok(cached_delta(34, 35)) }));
        assert!(!Arc::ptr_eq(&running, &rejected));
        assert_eq!(rejected.wait(CancellationToken::new()).await, Err(CacheWaitError::Build(CacheBuildError::Cancelled)));
    }

    #[tokio::test]
    async fn completed_delta_and_head_hits_share_body_allocations_past_cache_release() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();

        let delta_body = Bytes::from(vec![1, 2, 3, 4]);
        let builder_delta_body = delta_body.clone();
        let completed_delta =
            job(delta(
                &cache,
                &jobs,
                7,
                11,
                move |_| async move { Ok(Arc::new(CachedDelta::new(7, 11, false, 23, builder_delta_body))) },
            ))
            .wait(CancellationToken::new())
            .await
            .expect("delta construction completes");
        let delta_hit = match delta(&cache, &jobs, 7, 11, |_| async { panic!("a completed delta cannot rebuild") }) {
            CacheAccess::Completed(value) => value,
            CacheAccess::Job(_) => panic!("the completed delta is reused"),
        };
        assert!(Arc::ptr_eq(&completed_delta, &delta_hit));
        assert_eq!(completed_delta.gzip_body().as_ptr(), delta_hit.gzip_body().as_ptr());
        let retained_delta_body = delta_hit.gzip_body().clone();

        let head_body = Bytes::from(vec![5, 6, 7, 8]);
        let builder_head_body = head_body.clone();
        let completed_head = cache
            .head_tier(&jobs, 50, head_boundary(), move |_| {
                let body = builder_head_body.clone();
                async move { Ok(cached_head_with_body(50, 13, 17, body)) }
            })
            .resolve(CancellationToken::new())
            .await
            .expect("Head construction completes");
        let head_hit = cache
            .head_tier(&jobs, 50, head_boundary(), |_| async { panic!("a completed Head tier cannot rebuild") })
            .resolve(CancellationToken::new())
            .await
            .expect("Head hit completes");
        assert!(Arc::ptr_eq(&completed_head, &head_hit));
        assert_eq!(completed_head.gzip_body().as_ptr(), head_hit.gzip_body().as_ptr());
        let retained_head_body = head_hit.gzip_body().clone();

        jobs.shutdown().await;
        drop(completed_delta);
        drop(delta_hit);
        drop(delta_body);
        drop(completed_head);
        drop(head_hit);
        drop(head_body);
        drop(cache);

        assert_eq!(retained_delta_body.as_ref(), [1, 2, 3, 4]);
        assert_eq!(retained_head_body.as_ref(), [5, 6, 7, 8]);
    }

    #[tokio::test]
    async fn inadequate_completed_delta_is_preserved_while_one_enlargement_runs() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let first = job(delta(&cache, &jobs, 7, 11, |_| async { Ok(cached_delta(7, 11)) }));
        let completed = first.wait(CancellationToken::new()).await.expect("initial build completes");

        let (release_tx, release_rx) = oneshot::channel();
        let enlargement = job(delta(&cache, &jobs, 7, 19, move |_| async move {
            release_rx.await.expect("enlargement is released");
            Ok(cached_delta(7, 19))
        }));

        match delta(&cache, &jobs, 7, 11, |_| async { panic!("the shorter request reuses the preserved completion") }) {
            CacheAccess::Completed(hit) => {
                assert!(Arc::ptr_eq(&hit, &completed));
                assert_eq!(hit.to_revision_id(), 11);
            }
            CacheAccess::Job(_) => panic!("the completed value remains adequate for the shorter request"),
        }

        let joined = job(delta(&cache, &jobs, 7, 23, |_| async { panic!("one source has at most one running job") }));
        assert!(Arc::ptr_eq(&enlargement, &joined));

        release_tx.send(()).expect("enlargement release is observed");
        let enlarged = enlargement.wait(CancellationToken::new()).await.expect("enlargement completes");
        assert_eq!(enlarged.to_revision_id(), 19);
        match delta(&cache, &jobs, 7, 19, |_| async { panic!("the enlarged completion is reused") }) {
            CacheAccess::Completed(hit) => assert!(Arc::ptr_eq(&hit, &enlarged)),
            CacheAccess::Job(_) => panic!("the enlarged completion is adequate"),
        }
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn replay_floor_invalidation_removes_the_exact_cache_set_and_terminalizes_detached_jobs() {
        let cache = GraphCache::new();
        seed_delta(&cache, cached_delta(3, 4));
        seed_delta(&cache, cached_delta(10, 11));
        seed_delta(&cache, cached_delta(12, 13));

        let old_delta_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        let floor_delta_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        {
            let mut state = cache.lock_state();
            state.deltas.slots_by_source.insert(8, running_delta_slot(8, 9, old_delta_job.clone()));
            state.deltas.slots_by_source.insert(10, running_delta_slot(10, 11, floor_delta_job.clone()));
        }

        let old_head_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        let floor_head_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        let head_only_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        {
            let mut state = cache.lock_state();
            state
                .heads
                .slots_by_depth
                .insert(50, super::HeadCacheSlot { completed: Some(cached_head(50, 9, 100)), running: Some(old_head_job.clone()) });
            state.heads.slots_by_depth.insert(
                100,
                super::HeadCacheSlot { completed: Some(cached_head(100, 10, 100)), running: Some(floor_head_job.clone()) },
            );
            state.heads.slots_by_depth.insert(150, super::HeadCacheSlot { completed: Some(cached_head(150, 11, 100)), running: None });
            state.heads.slots_by_depth.insert(200, super::HeadCacheSlot { completed: None, running: Some(head_only_job.clone()) });
        }

        let invalidation = cache.invalidate_replay_floor(10);

        {
            let state = cache.lock_state();
            assert_eq!(state.deltas.slots_by_source.keys().copied().collect::<Vec<_>>(), [10, 12]);
            assert!(!state.heads.slots_by_depth.contains_key(&50));
            assert!(!state.heads.slots_by_depth.contains_key(&200));
            assert!(state.heads.slots_by_depth.get(&100).is_some_and(|slot| {
                slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| value.revision() == 10)
            }));
            assert!(state.heads.slots_by_depth.get(&150).is_some_and(|slot| {
                slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| value.revision() == 11)
            }));
        }

        let invalidated = CacheWaitError::InvalidatedByReplayFloor { replay_floor_revision: 10 };
        assert_eq!(old_delta_job.wait(CancellationToken::new()).await, Err(invalidated));
        assert_eq!(old_head_job.wait(CancellationToken::new()).await, Err(invalidated));
        assert_eq!(floor_head_job.wait(CancellationToken::new()).await, Err(invalidated));
        assert_eq!(head_only_job.wait(CancellationToken::new()).await, Err(invalidated));
        assert!(matches!(&*floor_delta_job.state.borrow(), super::CacheJobState::Pending));

        assert!(!old_delta_job.cancellation.is_cancelled());
        assert!(!old_head_job.cancellation.is_cancelled());
        invalidation.cancel();
        assert!(old_delta_job.cancellation.is_cancelled());
        assert!(old_head_job.cancellation.is_cancelled());
        assert!(!floor_delta_job.cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn replay_floor_cancellation_stops_detached_work_releases_resources_and_preserves_terminal_state() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let started = Arc::new(Notify::new());
        let started_builder = started.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_builder = dropped.clone();
        let resource = Arc::new(());
        let weak_resource = Arc::downgrade(&resource);
        let builder_resource = resource.clone();
        let running = job(delta(&cache, &jobs, 3, 4, move |_| async move {
            let _drop_signal = DropSignal(dropped_builder);
            let _resource = builder_resource;
            started_builder.notify_one();
            std::future::pending::<Result<Arc<CachedDelta>, CacheBuildError>>().await
        }));
        drop(resource);
        started.notified().await;

        let invalidation = cache.invalidate_replay_floor(4);
        assert!(!dropped.load(Ordering::SeqCst), "terminalization does not cancel work while the cache operation runs");
        invalidation.cancel();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached builder is cancelled");

        assert_eq!(
            running.wait(CancellationToken::new()).await,
            Err(CacheWaitError::InvalidatedByReplayFloor { replay_floor_revision: 4 })
        );
        jobs.shutdown().await;
        assert!(weak_resource.upgrade().is_none(), "joined cancellation releases resources retained by detached work");
    }

    #[test]
    fn delta_replay_floor_invalidation_wins_before_contending_completion_and_cannot_be_overwritten() {
        let cache = Arc::new(GraphCache::new());
        let job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        cache.lock_state().deltas.slots_by_source.insert(3, running_delta_slot(3, 4, job.clone()));

        let detached = Arc::new(Barrier::new(2));
        let release_invalidator = Arc::new(Barrier::new(2));
        let invalidation_cache = cache.clone();
        let invalidation_detached = detached.clone();
        let invalidation_release = release_invalidator.clone();
        let invalidator = std::thread::spawn(move || {
            invalidation_cache.invalidate_replay_floor_with(4, || {
                invalidation_detached.wait();
                invalidation_release.wait();
            })
        });
        detached.wait();

        let completion_started = Arc::new(Barrier::new(2));
        let completion_cache = Arc::downgrade(&cache);
        let completion_job = job.clone();
        let completion_barrier = completion_started.clone();
        let completion = std::thread::spawn(move || {
            completion_barrier.wait();
            super::finish_delta_job(&completion_cache, 3, &completion_job, Ok(cached_delta(3, 4)));
        });
        completion_started.wait();
        release_invalidator.wait();

        let invalidation = invalidator.join().expect("invalidation thread succeeds");
        completion.join().expect("completion thread succeeds");
        assert!(!cache.lock_state().deltas.slots_by_source.contains_key(&3));
        assert!(matches!(&*job.state.borrow(), super::CacheJobState::InvalidatedByReplayFloor { replay_floor_revision: 4 }));

        job.complete(Err(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed)));
        job.complete(Err(CacheBuildError::Cancelled));
        assert!(matches!(&*job.state.borrow(), super::CacheJobState::InvalidatedByReplayFloor { replay_floor_revision: 4 }));
        invalidation.cancel();
    }

    #[test]
    fn head_replay_floor_invalidation_wins_before_contending_completion_and_cannot_be_overwritten() {
        let cache = Arc::new(GraphCache::new());
        let depth = CACHED_HEAD_WINDOW_DEPTHS[0];
        let job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        cache.lock_state().heads.slots_by_depth.insert(depth, super::HeadCacheSlot { completed: None, running: Some(job.clone()) });

        let detached = Arc::new(Barrier::new(2));
        let release_invalidator = Arc::new(Barrier::new(2));
        let invalidation_cache = cache.clone();
        let invalidation_detached = detached.clone();
        let invalidation_release = release_invalidator.clone();
        let invalidator = std::thread::spawn(move || {
            invalidation_cache.invalidate_replay_floor_with(4, || {
                invalidation_detached.wait();
                invalidation_release.wait();
            })
        });
        detached.wait();

        let completion_started = Arc::new(Barrier::new(2));
        let completion_cache = Arc::downgrade(&cache);
        let completion_job = job.clone();
        let completion_barrier = completion_started.clone();
        let completion = std::thread::spawn(move || {
            completion_barrier.wait();
            super::finish_head_job(&completion_cache, depth, &completion_job, Ok(cached_head(depth, 3, 20)));
        });
        completion_started.wait();
        release_invalidator.wait();

        let invalidation = invalidator.join().expect("invalidation thread succeeds");
        completion.join().expect("completion thread succeeds");
        assert!(!cache.lock_state().heads.slots_by_depth.contains_key(&depth));
        assert!(matches!(&*job.state.borrow(), super::CacheJobState::InvalidatedByReplayFloor { replay_floor_revision: 4 }));

        job.complete(Err(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed)));
        job.complete(Err(CacheBuildError::Cancelled));
        assert!(matches!(&*job.state.borrow(), super::CacheJobState::InvalidatedByReplayFloor { replay_floor_revision: 4 }));
        invalidation.cancel();
    }

    #[tokio::test]
    async fn delta_ordinary_completion_wins_before_replay_floor_invalidation() {
        let cache = Arc::new(GraphCache::new());
        let job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        cache.lock_state().deltas.slots_by_source.insert(3, running_delta_slot(3, 4, job.clone()));

        let completed = cached_delta(3, 4);
        super::finish_delta_job(&Arc::downgrade(&cache), 3, &job, Ok(completed.clone()));
        assert!(cache.lock_state().deltas.slots_by_source.get(&3).is_some_and(|slot| {
            slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &completed))
        }));

        let invalidation = cache.invalidate_replay_floor(4);
        assert!(invalidation.delta_jobs.is_empty());
        assert!(invalidation.head_jobs.is_empty());
        assert!(!cache.lock_state().deltas.slots_by_source.contains_key(&3));
        let observed = job.wait(CancellationToken::new()).await.expect("ordinary completion remains terminal");
        assert!(Arc::ptr_eq(&observed, &completed));
    }

    #[tokio::test]
    async fn head_ordinary_completion_wins_before_replay_floor_invalidation() {
        let cache = Arc::new(GraphCache::new());
        let depth = CACHED_HEAD_WINDOW_DEPTHS[0];
        let job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        cache.lock_state().heads.slots_by_depth.insert(depth, super::HeadCacheSlot { completed: None, running: Some(job.clone()) });

        let completed = cached_head(depth, 3, 20);
        super::finish_head_job(&Arc::downgrade(&cache), depth, &job, Ok(completed.clone()));
        assert!(cache.lock_state().heads.slots_by_depth.get(&depth).is_some_and(|slot| {
            slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &completed))
        }));

        let invalidation = cache.invalidate_replay_floor(4);
        assert!(invalidation.delta_jobs.is_empty());
        assert!(invalidation.head_jobs.is_empty());
        assert!(!cache.lock_state().heads.slots_by_depth.contains_key(&depth));
        let observed = job.wait(CancellationToken::new()).await.expect("ordinary completion remains terminal");
        assert!(Arc::ptr_eq(&observed, &completed));
    }
}
