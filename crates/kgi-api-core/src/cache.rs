#![allow(dead_code, reason = "the cache kernel is integrated by later API increments")]

use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use bytes::Bytes;
use kgi_api_model::{
    projection::MAX_WINDOW_DEPTH,
    protocol::{
        CACHED_HEAD_WINDOW_DEPTH_STEP, CACHED_WINDOW_MAX_LEVEL_DISTANCE, CACHED_WINDOW_REFRESH_LEVEL_DISTANCE,
        HOT_DESTINATION_LEVEL_DISTANCE, MAX_CACHE_LEVEL_DISTANCE,
    },
};
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{codec::HeadEtagInput, encoding::scheduler::EncodingSchedulerError};

/// One immutable source-keyed canonical-delta response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CachedDelta {
    from_revision_id: u64,
    to_revision_id: u64,
    head_level: u32,
    uncompressed_json_bytes: u64,
    gzip_body: Bytes,
}

impl CachedDelta {
    /// Constructs one completely encoded canonical-delta cache value.
    pub(crate) const fn new(
        from_revision_id: u64,
        to_revision_id: u64,
        head_level: u32,
        uncompressed_json_bytes: u64,
        gzip_body: Bytes,
    ) -> Self {
        Self { from_revision_id, to_revision_id, head_level, uncompressed_json_bytes, gzip_body }
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
    pub(crate) const fn head_level(&self) -> u32 {
        self.head_level
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
        let (access, builder_job) = self.lock_state().deltas.access(source_revision, jobs.new_job_cancellation());

        if let Some(job) = builder_job {
            self.spawn_delta_builder(jobs, source_revision, job, builder, current_head_high_level);
        }
        access
    }

    /// Selects or starts the cache work for one valid requested Head depth.
    ///
    /// The current Head is sampled before the cache critical section. The
    /// returned access retains the request's selected tier even when a larger
    /// completed value or running job supplies the response.
    pub(crate) async fn head<'a, F, Fut, H, HeadFut>(
        self: &Arc<Self>,
        jobs: &'a CacheJobTracker,
        target_depth: u32,
        builder: F,
        current_head_high_level: H,
    ) -> HeadCacheAccess<'a, F, H>
    where
        F: Fn(u32, CancellationToken) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<CachedHeadSnapshot>, CacheBuildError>> + Send + 'static,
        H: Fn() -> HeadFut + Clone + Send + Sync + 'static,
        HeadFut: Future<Output = u32> + Send + 'static,
    {
        let selected_depth = selected_head_depth(target_depth);
        let head_high_level = current_head_high_level().await;
        let selection = self.lock_state().select_head(selected_depth, head_high_level, jobs.new_job_cancellation());

        if let Some(job) = &selection.builder_job {
            self.spawn_head_builder(jobs, selected_depth, job.clone(), builder.clone(), current_head_high_level.clone());
        }

        HeadCacheAccess {
            cache: self.clone(),
            jobs,
            selected_depth,
            response_depth: selection.response_depth,
            access: selection.access,
            builder,
            current_head_high_level,
        }
    }

    /// Records one successfully delivered canonical delta at its actual destination.
    pub(crate) fn record_delta_delivery(&self, destination_revision: u64, destination_head_level: u32) {
        self.lock_state().deltas.record_delivery(destination_revision, destination_head_level);
    }

    /// Prunes complete delta and destination-heat buckets behind their structural bounds.
    pub(crate) fn prune_deltas(&self, head_high_level: u32) {
        self.lock_state().deltas.prune(head_high_level);
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
            let job_cancellation = task_job.cancellation.clone();
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(CacheBuildError::Cancelled),
                () = task_job.work_cancelled() => Err(CacheBuildError::Cancelled),
                result = async {
                    let value = builder(job_cancellation).await?;
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

    fn spawn_head_builder<F, Fut, H, HeadFut>(
        self: &Arc<Self>,
        jobs: &CacheJobTracker,
        depth: u32,
        job: Arc<CacheJob<CachedHeadSnapshot>>,
        builder: F,
        current_head_high_level: H,
    ) where
        F: Fn(u32, CancellationToken) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<CachedHeadSnapshot>, CacheBuildError>> + Send + 'static,
        H: Fn() -> HeadFut + Clone + Send + Sync + 'static,
        HeadFut: Future<Output = u32> + Send + 'static,
    {
        let cache = Arc::downgrade(self);
        let task_job = job.clone();
        let spawned = jobs.spawn(move |cancellation| async move {
            let job_cancellation = task_job.cancellation.clone();
            loop {
                let attempt = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => Err(CacheBuildError::Cancelled),
                    () = task_job.work_cancelled() => Err(CacheBuildError::Cancelled),
                    result = async {
                        let value = builder(depth, job_cancellation.clone()).await?;
                        assert_eq!(value.depth(), depth, "a Head-tier builder returns its selected depth");
                        let head_high_level = current_head_high_level().await;
                        Ok((value, head_high_level))
                    } => result,
                };

                match attempt {
                    Ok((value, head_high_level))
                        if head_snapshot_eligibility(&value, head_high_level) == HeadSnapshotEligibility::Ineligible =>
                    {
                        continue;
                    }
                    result => {
                        finish_head_job(&cache, depth, &task_job, result);
                        break;
                    }
                }
            }
        });
        if !spawned {
            finish_head_job(&Arc::downgrade(self), depth, &job, Err(CacheBuildError::Cancelled));
        }
    }

    fn ensure_head_refresh<F, Fut, H, HeadFut>(
        self: &Arc<Self>,
        jobs: &CacheJobTracker,
        selected_depth: u32,
        value: &Arc<CachedHeadSnapshot>,
        head_high_level: u32,
        builder: F,
        current_head_high_level: H,
    ) where
        F: Fn(u32, CancellationToken) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<CachedHeadSnapshot>, CacheBuildError>> + Send + 'static,
        H: Fn() -> HeadFut + Clone + Send + Sync + 'static,
        HeadFut: Future<Output = u32> + Send + 'static,
    {
        let builder_job = self.lock_state().ensure_head_refresh(selected_depth, value, head_high_level, jobs.new_job_cancellation());
        if let Some(job) = builder_job {
            self.spawn_head_builder(jobs, selected_depth, job, builder, current_head_high_level);
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

impl GraphCacheState {
    fn detach_before_replay_floor(&mut self, replay_floor_revision: u64) -> ReplayFloorInvalidation {
        let delta_jobs = self.deltas.detach_before_replay_floor(replay_floor_revision);
        let mut head_jobs = Vec::new();

        self.head_slots.retain(|_, slot| {
            if slot.completed.as_ref().is_some_and(|value| value.revision() < replay_floor_revision) {
                slot.completed = None;
            }
            if let Some(job) = slot.running.take() {
                head_jobs.push(job);
            }
            slot.completed.is_some()
        });

        ReplayFloorInvalidation { delta_jobs, head_jobs }
    }

    fn select_head(&mut self, selected_depth: u32, head_high_level: u32, cancellation: CancellationToken) -> HeadSelection {
        if let Some(completed) = self
            .head_slots
            .get(&selected_depth)
            .and_then(|slot| slot.completed.as_ref())
            .filter(|value| head_snapshot_eligibility(value, head_high_level) != HeadSnapshotEligibility::Ineligible)
            .cloned()
        {
            let builder_job = (head_snapshot_eligibility(&completed, head_high_level) == HeadSnapshotEligibility::Refresh)
                .then(|| self.install_head_job(selected_depth, cancellation.clone()))
                .flatten();
            return HeadSelection { response_depth: selected_depth, access: CacheAccess::Completed(completed), builder_job };
        }

        for depth in head_tiers().filter(|&depth| depth > selected_depth) {
            if let Some(completed) = self
                .head_slots
                .get(&depth)
                .and_then(|slot| slot.completed.as_ref())
                .filter(|value| head_snapshot_eligibility(value, head_high_level) != HeadSnapshotEligibility::Ineligible)
                .cloned()
            {
                return HeadSelection {
                    response_depth: depth,
                    access: CacheAccess::Completed(completed),
                    builder_job: self.install_head_job(selected_depth, cancellation.clone()),
                };
            }
        }

        if let Some(running) = self.head_slots.get(&selected_depth).and_then(|slot| slot.running.clone()) {
            return HeadSelection { response_depth: selected_depth, access: CacheAccess::Job(running), builder_job: None };
        }

        for depth in head_tiers().filter(|&depth| depth > selected_depth) {
            if let Some(running) = self.head_slots.get(&depth).and_then(|slot| slot.running.clone()) {
                return HeadSelection { response_depth: depth, access: CacheAccess::Job(running), builder_job: None };
            }
        }

        let job = self.install_head_job(selected_depth, cancellation).expect("a selected tier without running work installs one job");
        HeadSelection { response_depth: selected_depth, access: CacheAccess::Job(job.clone()), builder_job: Some(job) }
    }

    fn ensure_head_refresh(
        &mut self,
        selected_depth: u32,
        value: &Arc<CachedHeadSnapshot>,
        head_high_level: u32,
        cancellation: CancellationToken,
    ) -> Option<Arc<CacheJob<CachedHeadSnapshot>>> {
        if head_snapshot_eligibility(value, head_high_level) == HeadSnapshotEligibility::Fresh {
            return None;
        }
        let slot = self.head_slots.get(&selected_depth)?;
        if !slot.completed.as_ref().is_some_and(|completed| Arc::ptr_eq(completed, value)) {
            return None;
        }
        self.install_head_job(selected_depth, cancellation)
    }

    fn install_head_job(&mut self, depth: u32, cancellation: CancellationToken) -> Option<Arc<CacheJob<CachedHeadSnapshot>>> {
        let slot = self.head_slots.entry(depth).or_default();
        if slot.running.is_some() {
            return None;
        }
        let job = Arc::new(CacheJob::pending(cancellation));
        slot.running = Some(job.clone());
        Some(job)
    }

    fn finish_head(
        &mut self,
        depth: u32,
        job: &Arc<CacheJob<CachedHeadSnapshot>>,
        result: &Result<(Arc<CachedHeadSnapshot>, u32), CacheBuildError>,
    ) {
        let mut remove_slot = false;
        if let Some(slot) = self.head_slots.get_mut(&depth)
            && slot.running.as_ref().is_some_and(|running| Arc::ptr_eq(running, job))
        {
            if let Ok((value, _)) = result {
                assert_eq!(value.depth(), depth, "a completed Head snapshot remains keyed by its derived tier");
                slot.completed = Some(value.clone());
            }
            slot.running = None;
            remove_slot = slot.completed.is_none();
        }
        if remove_slot {
            self.head_slots.remove(&depth);
        }
    }
}

struct HeadSelection {
    response_depth: u32,
    access: CacheAccess<CachedHeadSnapshot>,
    builder_job: Option<Arc<CacheJob<CachedHeadSnapshot>>>,
}

#[derive(Default)]
struct DeltaCache {
    slots_by_source: HashMap<u64, CacheSlot<CachedDelta>>,
    sources_by_head_level: BTreeMap<u32, Vec<u64>>,
    destination_heat: DestinationHeat,
}

impl DeltaCache {
    fn detach_before_replay_floor(&mut self, replay_floor_revision: u64) -> Vec<Arc<CacheJob<CachedDelta>>> {
        let mut jobs = Vec::new();
        self.slots_by_source.retain(|source_revision, slot| {
            if *source_revision >= replay_floor_revision {
                return true;
            }
            if let Some(job) = slot.running.take() {
                jobs.push(job);
            }
            false
        });
        self.sources_by_head_level.retain(|_, source_revisions| {
            source_revisions.retain(|source_revision| *source_revision >= replay_floor_revision);
            !source_revisions.is_empty()
        });
        jobs
    }

    fn access(
        &mut self,
        source_revision: u64,
        cancellation: CancellationToken,
    ) -> (CacheAccess<CachedDelta>, Option<Arc<CacheJob<CachedDelta>>>) {
        let slot = self.slots_by_source.entry(source_revision).or_default();
        if let Some(completed) = &slot.completed {
            (CacheAccess::Completed(completed.clone()), None)
        } else if let Some(running) = &slot.running {
            (CacheAccess::Job(running.clone()), None)
        } else {
            let job = Arc::new(CacheJob::pending(cancellation));
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
                if head_high_level.saturating_sub(value.head_level()) <= MAX_CACHE_LEVEL_DISTANCE {
                    slot.completed = Some(value.clone());
                    self.sources_by_head_level.entry(value.head_level()).or_default().push(source_revision);
                }
            }
            slot.running = None;
            remove_slot = slot.completed.is_none();
        }
        if remove_slot {
            self.slots_by_source.remove(&source_revision);
        }
    }

    fn record_delivery(&mut self, destination_revision: u64, destination_head_level: u32) {
        self.destination_heat.record(destination_revision, destination_head_level);
    }

    fn prune(&mut self, head_high_level: u32) {
        let retained_head_level = head_high_level.saturating_sub(MAX_CACHE_LEVEL_DISTANCE);
        let retained = self.sources_by_head_level.split_off(&retained_head_level);
        let expired = std::mem::replace(&mut self.sources_by_head_level, retained);
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
    fn record(&mut self, destination_revision: u64, destination_head_level: u32) {
        if let Some(count) = self.counts_by_revision.get_mut(&destination_revision) {
            *count = count.saturating_add(1);
        } else {
            self.counts_by_revision.insert(destination_revision, 1);
            self.revisions_by_level.entry(destination_head_level).or_default().push(destination_revision);
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

/// Selected Head tier and the completed value or shared job supplying it.
pub(crate) struct HeadCacheAccess<'a, F, H> {
    cache: Arc<GraphCache>,
    jobs: &'a CacheJobTracker,
    selected_depth: u32,
    response_depth: u32,
    access: CacheAccess<CachedHeadSnapshot>,
    builder: F,
    current_head_high_level: H,
}

impl<'a, F, Fut, H, HeadFut> HeadCacheAccess<'a, F, H>
where
    F: Fn(u32, CancellationToken) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<Arc<CachedHeadSnapshot>, CacheBuildError>> + Send + 'static,
    H: Fn() -> HeadFut + Clone + Send + Sync + 'static,
    HeadFut: Future<Output = u32> + Send + 'static,
{
    /// Returns the tier derived from the original request depth.
    pub(crate) const fn selected_depth(&self) -> u32 {
        self.selected_depth
    }

    /// Returns the selected or smallest covering tier supplying this access.
    pub(crate) const fn response_depth(&self) -> u32 {
        self.response_depth
    }

    /// Resolves the access and demand-refreshes a selected soft-aged result.
    pub(crate) async fn resolve(self, cancellation: CancellationToken) -> Result<Arc<CachedHeadSnapshot>, CacheWaitError> {
        let joined_job = matches!(self.access, CacheAccess::Job(_));
        let refresh_cancellation = cancellation.clone();
        let value = match self.access {
            CacheAccess::Completed(value) => value,
            CacheAccess::Job(job) => job.wait(cancellation).await?,
        };

        if joined_job && self.response_depth == self.selected_depth {
            let head_high_level = tokio::select! {
                biased;
                () = refresh_cancellation.cancelled() => return Err(CacheWaitError::Cancelled),
                head_high_level = (self.current_head_high_level)() => head_high_level,
            };
            self.cache.ensure_head_refresh(
                self.jobs,
                self.selected_depth,
                &value,
                head_high_level,
                self.builder,
                self.current_head_high_level,
            );
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeadSnapshotEligibility {
    Fresh,
    Refresh,
    Ineligible,
}

fn selected_head_depth(target_depth: u32) -> u32 {
    assert!((1..=MAX_WINDOW_DEPTH).contains(&target_depth), "Head target depth is validated before cache selection");
    target_depth.div_ceil(CACHED_HEAD_WINDOW_DEPTH_STEP) * CACHED_HEAD_WINDOW_DEPTH_STEP
}

fn head_tiers() -> impl Iterator<Item = u32> {
    assert_eq!(MAX_WINDOW_DEPTH % CACHED_HEAD_WINDOW_DEPTH_STEP, 0, "Head cache tiers exactly cover the public depth bound");
    (CACHED_HEAD_WINDOW_DEPTH_STEP..=MAX_WINDOW_DEPTH).step_by(CACHED_HEAD_WINDOW_DEPTH_STEP as usize)
}

fn head_snapshot_eligibility(value: &CachedHeadSnapshot, head_high_level: u32) -> HeadSnapshotEligibility {
    let distance =
        head_high_level.checked_sub(value.high_level()).expect("a publication Head does not precede one of its captured snapshots");
    if distance < CACHED_WINDOW_REFRESH_LEVEL_DISTANCE {
        HeadSnapshotEligibility::Fresh
    } else if distance < CACHED_WINDOW_MAX_LEVEL_DISTANCE {
        HeadSnapshotEligibility::Refresh
    } else {
        HeadSnapshotEligibility::Ineligible
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
    result: Result<(Arc<CachedDelta>, u32), CacheBuildError>,
) {
    if let Some(cache) = cache.upgrade() {
        cache.lock_state().deltas.finish(source_revision, job, &result);
    }
    job.complete(result.map(|(value, _)| value));
}

fn finish_head_job(
    cache: &Weak<GraphCache>,
    depth: u32,
    job: &Arc<CacheJob<CachedHeadSnapshot>>,
    result: Result<(Arc<CachedHeadSnapshot>, u32), CacheBuildError>,
) {
    if let Some(cache) = cache.upgrade() {
        cache.lock_state().finish_head(depth, job, &result);
    }
    job.complete(result.map(|(value, _)| value));
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        future::Future,
        sync::{
            Arc, Barrier,
            atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::Bytes;
    use kgi_api_model::{
        graph::LevelExtent,
        protocol::{CACHED_HEAD_WINDOW_DEPTH_STEP, CACHED_WINDOW_MAX_LEVEL_DISTANCE, CACHED_WINDOW_REFRESH_LEVEL_DISTANCE},
    };
    use tokio::sync::{Notify, oneshot};
    use tokio_util::sync::CancellationToken;

    use crate::codec::HeadEtagInput;

    use super::{
        CacheAccess, CacheBuildError, CacheJobTracker, CacheWaitError, CachedDelta, CachedHeadSnapshot, EncodingSchedulerError,
        GraphCache, HeadSnapshotEligibility, head_snapshot_eligibility, selected_head_depth,
    };

    fn cached_delta(source: u64, target: u64) -> Arc<CachedDelta> {
        cached_delta_at(source, target, 17)
    }

    fn cached_delta_at(source: u64, target: u64, head_level: u32) -> Arc<CachedDelta> {
        Arc::new(CachedDelta::new(source, target, head_level, 23, Bytes::from_static(b"gzip")))
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
        cache.lock_state().head_slots.entry(depth).or_default().completed = Some(value);
    }

    fn seed_delta(cache: &GraphCache, value: Arc<CachedDelta>) {
        let source_revision = value.source_revision_id();
        let head_level = value.head_level();
        let mut state = cache.lock_state();
        state.deltas.slots_by_source.entry(source_revision).or_default().completed = Some(value);
        state.deltas.sources_by_head_level.entry(head_level).or_default().push(source_revision);
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

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn requested_head_depths_derive_the_five_settled_tiers() {
        for (target_depth, expected) in [(1, 50), (50, 50), (51, 100), (149, 150), (200, 200), (201, 250), (250, 250)] {
            assert_eq!(selected_head_depth(target_depth), expected);
        }
        assert_eq!(selected_head_depth(CACHED_HEAD_WINDOW_DEPTH_STEP), CACHED_HEAD_WINDOW_DEPTH_STEP);
    }

    #[test]
    fn head_selection_uses_the_exact_completed_running_and_covering_order() {
        let exact_cache = GraphCache::new();
        let exact = cached_head(50, 1, 100);
        let larger = cached_head(100, 2, 100);
        seed_head(&exact_cache, exact.clone());
        seed_head(&exact_cache, larger);
        let selection = exact_cache.lock_state().select_head(50, 100, CancellationToken::new());
        assert_eq!(selection.response_depth, 50);
        assert!(matches!(selection.access, CacheAccess::Completed(value) if Arc::ptr_eq(&value, &exact)));
        assert!(selection.builder_job.is_none());

        let completed_cover_cache = GraphCache::new();
        let completed_cover = cached_head(100, 3, 100);
        seed_head(&completed_cover_cache, completed_cover.clone());
        let selection = completed_cover_cache.lock_state().select_head(50, 100, CancellationToken::new());
        assert_eq!(selection.response_depth, 100);
        assert!(matches!(selection.access, CacheAccess::Completed(value) if Arc::ptr_eq(&value, &completed_cover)));
        assert!(selection.builder_job.is_some(), "covering reuse starts the missing selected tier");

        let selected_job_cache = GraphCache::new();
        let selected_job =
            selected_job_cache.lock_state().install_head_job(50, CancellationToken::new()).expect("selected job installs");
        let selected_job_selection = selected_job_cache.lock_state().select_head(50, 100, CancellationToken::new());
        assert_eq!(selected_job_selection.response_depth, 50);
        assert!(matches!(selected_job_selection.access, CacheAccess::Job(job) if Arc::ptr_eq(&job, &selected_job)));
        assert!(selected_job_selection.builder_job.is_none());

        let covering_job_cache = GraphCache::new();
        let covering_job =
            covering_job_cache.lock_state().install_head_job(100, CancellationToken::new()).expect("covering job installs");
        let covering_job_selection = covering_job_cache.lock_state().select_head(50, 100, CancellationToken::new());
        assert_eq!(covering_job_selection.response_depth, 100);
        assert!(matches!(covering_job_selection.access, CacheAccess::Job(job) if Arc::ptr_eq(&job, &covering_job)));
        assert!(covering_job_selection.builder_job.is_none());
        assert!(!covering_job_cache.lock_state().head_slots.contains_key(&50));

        let empty_cache = GraphCache::new();
        let new_selection = empty_cache.lock_state().select_head(50, 100, CancellationToken::new());
        assert_eq!(new_selection.response_depth, 50);
        assert!(
            matches!(&new_selection.access, CacheAccess::Job(job) if new_selection.builder_job.as_ref().is_some_and(|builder| Arc::ptr_eq(job, builder)))
        );
    }

    #[test]
    fn head_snapshot_age_boundaries_are_exact() {
        let value = cached_head(50, 1, 100);
        assert_eq!(head_snapshot_eligibility(&value, 139), HeadSnapshotEligibility::Fresh);
        assert_eq!(head_snapshot_eligibility(&value, 140), HeadSnapshotEligibility::Refresh);
        assert_eq!(head_snapshot_eligibility(&value, 199), HeadSnapshotEligibility::Refresh);
        assert_eq!(head_snapshot_eligibility(&value, 200), HeadSnapshotEligibility::Ineligible);
        assert_eq!(CACHED_WINDOW_REFRESH_LEVEL_DISTANCE, 40);
        assert_eq!(CACHED_WINDOW_MAX_LEVEL_DISTANCE, 100);
    }

    #[tokio::test]
    async fn soft_aged_selected_hit_serves_old_value_and_refreshes_only_its_tier() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let old_selected = cached_head(50, 1, 100);
        let untouched_cover = cached_head(100, 2, 100);
        seed_head(&cache, old_selected.clone());
        seed_head(&cache, untouched_cover.clone());

        let release = Arc::new(Notify::new());
        let release_builder = release.clone();
        let access = cache
            .head(
                &jobs,
                25,
                move |depth, _| {
                    let release = release_builder.clone();
                    async move {
                        release.notified().await;
                        Ok(cached_head(depth, 3, 140))
                    }
                },
                || async { 140 },
            )
            .await;
        assert_eq!(access.selected_depth(), 50);
        assert_eq!(access.response_depth(), 50);
        assert!(Arc::ptr_eq(
            &access.resolve(CancellationToken::new()).await.expect("soft-aged completed value remains eligible"),
            &old_selected
        ));

        let refresh = cache
            .lock_state()
            .head_slots
            .get(&50)
            .and_then(|slot| slot.running.clone())
            .expect("selected soft-aged hit starts one refresh");
        assert!(cache.lock_state().head_slots.get(&100).is_some_and(|slot| {
            slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &untouched_cover))
        }));

        release.notify_one();
        let refreshed = refresh.wait(CancellationToken::new()).await.expect("refresh succeeds");
        assert_eq!(refreshed.revision(), 3);
        assert!(cache.lock_state().head_slots.get(&50).is_some_and(|slot| {
            slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &refreshed))
        }));
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn hard_aged_candidate_retries_inside_one_pending_tier_job() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_builder = attempts.clone();
        let access = cache
            .head(
                &jobs,
                CACHED_HEAD_WINDOW_DEPTH_STEP,
                move |depth, _| {
                    let attempt = attempts_builder.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let (revision, high_level) = if attempt == 0 { (1, 20) } else { (2, 120) };
                        Ok(cached_head(depth, revision, high_level))
                    }
                },
                || async { 120 },
            )
            .await;
        let shared_job = match &access.access {
            CacheAccess::Job(job) => job.clone(),
            CacheAccess::Completed(_) => panic!("the first tier request starts one job"),
        };
        let concurrent = cache
            .head(
                &jobs,
                CACHED_HEAD_WINDOW_DEPTH_STEP,
                |_, _| async { panic!("a concurrent tier waiter cannot build") },
                || async { 120 },
            )
            .await;
        assert!(matches!(&concurrent.access, CacheAccess::Job(job) if Arc::ptr_eq(job, &shared_job)));

        let completed = access.resolve(CancellationToken::new()).await.expect("newer retry completes every waiter");
        assert_eq!(completed.revision(), 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(Arc::ptr_eq(
            &concurrent.resolve(CancellationToken::new()).await.expect("concurrent waiter receives the same result"),
            &completed
        ));
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn hard_aged_completed_value_is_preserved_but_excluded_during_reconstruction() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let previous = cached_head(50, 1, 100);
        seed_head(&cache, previous.clone());
        let release = Arc::new(Notify::new());
        let release_builder = release.clone();

        let access = cache
            .head(
                &jobs,
                50,
                move |depth, _| {
                    let release = release_builder.clone();
                    async move {
                        release.notified().await;
                        Ok(cached_head(depth, 2, 200))
                    }
                },
                || async { 200 },
            )
            .await;
        assert_eq!(access.response_depth(), 50);
        assert!(matches!(&access.access, CacheAccess::Job(_)), "distance 100 excludes the previous completed value");
        assert!(cache.lock_state().head_slots.get(&50).is_some_and(|slot| {
            slot.running.is_some() && slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &previous))
        }));

        release.notify_one();
        let replacement = access.resolve(CancellationToken::new()).await.expect("shared reconstruction completes");
        assert_eq!(replacement.revision(), 2);
        assert!(cache.lock_state().head_slots.get(&50).is_some_and(|slot| {
            slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &replacement))
        }));
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn selected_soft_aged_job_starts_one_successor_but_covering_use_does_not() {
        let selected_cache = Arc::new(GraphCache::new());
        let selected_jobs = CacheJobTracker::new();
        let selected_attempts = Arc::new(AtomicUsize::new(0));
        let selected_attempts_builder = selected_attempts.clone();
        let successor_gate = Arc::new(Notify::new());
        let successor_builder_gate = successor_gate.clone();
        let selected = selected_cache
            .head(
                &selected_jobs,
                50,
                move |depth, _| {
                    let attempt = selected_attempts_builder.fetch_add(1, Ordering::SeqCst);
                    let successor_gate = successor_builder_gate.clone();
                    async move {
                        if attempt > 0 {
                            successor_gate.notified().await;
                        }
                        Ok(cached_head(depth, u64::try_from(attempt + 1).expect("small attempt"), if attempt == 0 { 60 } else { 100 }))
                    }
                },
                || async { 100 },
            )
            .await;
        let first = selected.resolve(CancellationToken::new()).await.expect("soft-aged selected candidate is delivered");
        assert_eq!(first.high_level(), 60);
        tokio::task::yield_now().await;
        assert_eq!(selected_attempts.load(Ordering::SeqCst), 2, "selected delivery starts one successor refresh");
        let successor = selected_cache
            .lock_state()
            .head_slots
            .get(&50)
            .and_then(|slot| slot.running.clone())
            .expect("successor remains the tier's running job");
        successor_gate.notify_one();
        successor.wait(CancellationToken::new()).await.expect("successor completes");
        selected_jobs.shutdown().await;

        let covering_cache = Arc::new(GraphCache::new());
        let covering_jobs = CacheJobTracker::new();
        let covering_attempts = Arc::new(AtomicUsize::new(0));
        let covering_attempts_builder = covering_attempts.clone();
        let release = Arc::new(Notify::new());
        let release_builder = release.clone();
        let original = covering_cache
            .head(
                &covering_jobs,
                100,
                move |depth, _| {
                    covering_attempts_builder.fetch_add(1, Ordering::SeqCst);
                    let release = release_builder.clone();
                    async move {
                        release.notified().await;
                        Ok(cached_head(depth, 1, 60))
                    }
                },
                || async { 100 },
            )
            .await;
        drop(original);
        let covering = covering_cache
            .head(&covering_jobs, 50, |_, _| async { panic!("a covering join cannot create selected-tier work") }, || async { 100 })
            .await;
        assert_eq!(covering.selected_depth(), 50);
        assert_eq!(covering.response_depth(), 100);
        release.notify_one();
        covering.resolve(CancellationToken::new()).await.expect("covering job completes");
        tokio::task::yield_now().await;
        assert_eq!(covering_attempts.load(Ordering::SeqCst), 1);
        assert!(covering_cache.lock_state().head_slots.get(&100).is_some_and(|slot| slot.running.is_none()));
        assert!(!covering_cache.lock_state().head_slots.contains_key(&50));
        covering_jobs.shutdown().await;
    }

    #[tokio::test]
    async fn failed_refresh_preserves_previous_value_and_allows_retry() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let previous = cached_head(50, 1, 100);
        seed_head(&cache, previous.clone());

        let fail_gate = Arc::new(Notify::new());
        let fail_builder_gate = fail_gate.clone();

        let served = cache
            .head(
                &jobs,
                50,
                move |_, _| {
                    let fail_gate = fail_builder_gate.clone();
                    async move {
                        fail_gate.notified().await;
                        Err(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed))
                    }
                },
                || async { 140 },
            )
            .await;
        assert!(Arc::ptr_eq(&served.resolve(CancellationToken::new()).await.expect("previous value remains serviceable"), &previous));
        let failed = cache
            .lock_state()
            .head_slots
            .get(&50)
            .and_then(|slot| slot.running.clone())
            .expect("refresh failure is observed through its shared job");
        fail_gate.notify_one();
        assert_eq!(
            failed.wait(CancellationToken::new()).await,
            Err(CacheWaitError::Build(CacheBuildError::Encoding(EncodingSchedulerError::BlockingExecutorFailed)))
        );
        assert!(cache.lock_state().head_slots.get(&50).is_some_and(|slot| {
            slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &previous))
        }));

        let retry_gate = Arc::new(Notify::new());
        let retry_builder_gate = retry_gate.clone();
        let retried = cache
            .head(
                &jobs,
                50,
                move |depth, _| {
                    let retry_gate = retry_builder_gate.clone();
                    async move {
                        retry_gate.notified().await;
                        Ok(cached_head(depth, 2, 140))
                    }
                },
                || async { 140 },
            )
            .await;
        assert!(Arc::ptr_eq(&retried.resolve(CancellationToken::new()).await.expect("retry still serves previous value"), &previous));
        let replacement =
            cache.lock_state().head_slots.get(&50).and_then(|slot| slot.running.clone()).expect("retry installs new refresh work");
        retry_gate.notify_one();
        assert_eq!(replacement.wait(CancellationToken::new()).await.expect("retry succeeds").revision(), 2);
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn head_advancement_without_demand_starts_no_refresh() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let head_high_level = Arc::new(AtomicU32::new(100));
        let sampled_head = head_high_level.clone();
        let builds = Arc::new(AtomicUsize::new(0));
        let builder_builds = builds.clone();
        cache
            .head(
                &jobs,
                50,
                move |depth, _| {
                    builder_builds.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(cached_head(depth, 1, 100)) }
                },
                move || {
                    let sampled_head = sampled_head.clone();
                    async move { sampled_head.load(Ordering::SeqCst) }
                },
            )
            .await
            .resolve(CancellationToken::new())
            .await
            .expect("initial demand constructs the tier");

        head_high_level.store(140, Ordering::SeqCst);
        tokio::task::yield_now().await;
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert!(cache.lock_state().head_slots.get(&50).is_some_and(|slot| slot.running.is_none()));
        jobs.shutdown().await;
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
    async fn tracker_shutdown_cancels_unobserved_head_refresh_preserves_the_old_value_and_rejects_replacement() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let previous = cached_head(CACHED_HEAD_WINDOW_DEPTH_STEP, 1, 100);
        seed_head(&cache, previous.clone());

        let access = cache
            .head(
                &jobs,
                CACHED_HEAD_WINDOW_DEPTH_STEP,
                |_, _| async { std::future::pending::<Result<Arc<CachedHeadSnapshot>, CacheBuildError>>().await },
                || async { 140 },
            )
            .await;
        assert!(matches!(&access.access, CacheAccess::Completed(value) if Arc::ptr_eq(value, &previous)));
        drop(access);
        let running = cache
            .lock_state()
            .head_slots
            .get(&CACHED_HEAD_WINDOW_DEPTH_STEP)
            .and_then(|slot| slot.running.clone())
            .expect("demand starts one tracked Head refresh");

        jobs.shutdown().await;
        assert_eq!(running.wait(CancellationToken::new()).await, Err(CacheWaitError::Build(CacheBuildError::Cancelled)));
        assert!(cache.lock_state().head_slots.get(&CACHED_HEAD_WINDOW_DEPTH_STEP).is_some_and(|slot| {
            slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &previous))
        }));

        let rejected = cache
            .head(
                &jobs,
                CACHED_HEAD_WINDOW_DEPTH_STEP,
                |_, _| async { panic!("a closed tracker cannot run new Head work") },
                || async { 200 },
            )
            .await;
        assert_eq!(rejected.resolve(CancellationToken::new()).await, Err(CacheWaitError::Build(CacheBuildError::Cancelled)));
        assert!(cache.lock_state().head_slots.get(&CACHED_HEAD_WINDOW_DEPTH_STEP).is_some_and(|slot| {
            slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| Arc::ptr_eq(value, &previous))
        }));
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
                17,
                move |_| async move { Ok(Arc::new(CachedDelta::new(7, 11, 17, 23, builder_delta_body))) },
            ))
            .wait(CancellationToken::new())
            .await
            .expect("delta construction completes");
        let delta_hit = match delta(&cache, &jobs, 7, 17, |_| async { panic!("a completed delta cannot rebuild") }) {
            CacheAccess::Completed(value) => value,
            CacheAccess::Job(_) => panic!("the completed delta is reused"),
        };
        assert!(Arc::ptr_eq(&completed_delta, &delta_hit));
        assert_eq!(completed_delta.gzip_body().as_ptr(), delta_hit.gzip_body().as_ptr());
        let retained_delta_body = delta_hit.gzip_body().clone();

        let head_body = Bytes::from(vec![5, 6, 7, 8]);
        let builder_head_body = head_body.clone();
        let completed_head = cache
            .head(
                &jobs,
                50,
                move |depth, _| {
                    let body = builder_head_body.clone();
                    async move { Ok(cached_head_with_body(depth, 13, 17, body)) }
                },
                || async { 17 },
            )
            .await
            .resolve(CancellationToken::new())
            .await
            .expect("Head construction completes");
        let head_hit = cache
            .head(&jobs, 50, |_, _| async { panic!("a completed Head tier cannot rebuild") }, || async { 17 })
            .await
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
    async fn head_level_index_prunes_complete_expired_buckets_only() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        for (source, target, head_level) in [(5, 8, 20), (13, 16, 20), (21, 24, 21)] {
            let completed =
                job(delta(&cache, &jobs, source, 520, move |_| async move { Ok(cached_delta_at(source, target, head_level)) }));
            completed.wait(CancellationToken::new()).await.expect("fixture entry completes");
        }

        cache.prune_deltas(520);
        {
            let state = cache.lock_state();
            assert_eq!(state.deltas.slots_by_source.len(), 3, "distance 500 and younger buckets remain retained");
            assert_eq!(state.deltas.sources_by_head_level.keys().copied().collect::<Vec<_>>(), [20, 21]);
        }

        cache.prune_deltas(521);
        {
            let state = cache.lock_state();
            assert_eq!(state.deltas.slots_by_source.keys().copied().collect::<Vec<_>>(), [21]);
            assert_eq!(state.deltas.sources_by_head_level.keys().copied().collect::<Vec<_>>(), [21]);
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
            assert!(!state.deltas.sources_by_head_level.contains_key(&20));
        }

        let replacement = job(delta(&cache, &jobs, 3, 521, |_| async { Ok(cached_delta_at(3, 7, 521)) }));
        assert_eq!(replacement.wait(CancellationToken::new()).await.expect("uncached source can retry").to_revision_id(), 7);
        jobs.shutdown().await;
    }

    #[tokio::test]
    async fn replay_floor_invalidation_removes_the_exact_cache_set_and_terminalizes_detached_jobs() {
        let cache = GraphCache::new();
        seed_delta(&cache, cached_delta_at(3, 4, 20));
        seed_delta(&cache, cached_delta_at(10, 11, 21));
        seed_delta(&cache, cached_delta_at(12, 13, 22));

        let old_delta_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        let floor_delta_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        {
            let mut state = cache.lock_state();
            state.deltas.slots_by_source.insert(8, super::CacheSlot { completed: None, running: Some(old_delta_job.clone()) });
            state.deltas.slots_by_source.insert(10, super::CacheSlot { completed: None, running: Some(floor_delta_job.clone()) });
        }

        let old_head_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        let floor_head_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        let head_only_job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        {
            let mut state = cache.lock_state();
            state
                .head_slots
                .insert(50, super::CacheSlot { completed: Some(cached_head(50, 9, 100)), running: Some(old_head_job.clone()) });
            state
                .head_slots
                .insert(100, super::CacheSlot { completed: Some(cached_head(100, 10, 100)), running: Some(floor_head_job.clone()) });
            state.head_slots.insert(150, super::CacheSlot { completed: Some(cached_head(150, 11, 100)), running: None });
            state.head_slots.insert(200, super::CacheSlot { completed: None, running: Some(head_only_job.clone()) });
        }

        let invalidation = cache.invalidate_replay_floor(10);

        {
            let state = cache.lock_state();
            assert_eq!(state.deltas.slots_by_source.keys().copied().collect::<std::collections::HashSet<_>>(), [10, 12].into());
            assert_eq!(state.deltas.sources_by_head_level, BTreeMap::from([(21, vec![10]), (22, vec![12])]));
            assert!(!state.head_slots.contains_key(&50));
            assert!(!state.head_slots.contains_key(&200));
            assert!(state.head_slots.get(&100).is_some_and(|slot| {
                slot.running.is_none() && slot.completed.as_ref().is_some_and(|value| value.revision() == 10)
            }));
            assert!(state.head_slots.get(&150).is_some_and(|slot| {
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
    async fn replay_floor_cancellation_stops_detached_work_without_replacing_its_terminal_state() {
        let cache = Arc::new(GraphCache::new());
        let jobs = CacheJobTracker::new();
        let started = Arc::new(Notify::new());
        let started_builder = started.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_builder = dropped.clone();
        let running = job(delta(&cache, &jobs, 3, 20, move |_| async move {
            let _drop_signal = DropSignal(dropped_builder);
            started_builder.notify_one();
            std::future::pending::<Result<Arc<CachedDelta>, CacheBuildError>>().await
        }));
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
    }

    #[test]
    fn replay_floor_invalidation_wins_before_contending_completion_and_cannot_be_overwritten() {
        let cache = Arc::new(GraphCache::new());
        let job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        cache.lock_state().deltas.slots_by_source.insert(3, super::CacheSlot { completed: None, running: Some(job.clone()) });

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
            super::finish_delta_job(&completion_cache, 3, &completion_job, Ok((cached_delta(3, 4), 20)));
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

    #[tokio::test]
    async fn ordinary_completion_wins_before_replay_floor_invalidation() {
        let cache = Arc::new(GraphCache::new());
        let job = Arc::new(super::CacheJob::pending(CancellationToken::new()));
        cache.lock_state().deltas.slots_by_source.insert(3, super::CacheSlot { completed: None, running: Some(job.clone()) });

        let completed = cached_delta(3, 4);
        super::finish_delta_job(&Arc::downgrade(&cache), 3, &job, Ok((completed.clone(), 20)));
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
}
