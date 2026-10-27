#![allow(dead_code, reason = "the isolated publication kernel is integrated by later API increments")]

mod error;
mod estimate;
pub(crate) mod runtime;

use std::{sync::Arc, time::Instant};

use kgi_api_model::{
    delta::GraphDelta,
    graph::{GraphView, GraphViewExtractError, LevelExtent, TrackingPolicy},
    history::{GraphHistory, GraphHistoryRange, GraphHistoryRangeError, GraphHistoryReplayability},
    protocol::{HeadLevelLookupOutcome, HeadLevelValue},
};
use kgi_model::graph_update::{BlockCommitted, VspcCommitted};
use rand::{RngCore, rngs::OsRng};
use tokio::sync::{Mutex, RwLock};

use crate::{
    cache::{
        CacheAccess, CacheBuildError, CacheJobTracker, CachedDelta, CachedHeadSnapshot, GraphCache, HeadCacheLookup,
        HeadEligibilityBoundary,
    },
    client::SseClientRegistry,
    codec::{HeadEtag, HeadEtagInput, graph::HeadSnapshotResponseDto},
    encoding::scheduler::EncodingScheduler,
};

pub(crate) use self::error::PublicationProjectionError;
use self::estimate::{classify_replayability, estimate_raw_delta_bytes};

/// Externally meaningful lifecycle state of one graph publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GraphPublicationState {
    /// The aligned graph is available while processing catches up to Live.
    Synchronizing,
    /// The graph follows the processing session's Live stream.
    Live,
    /// The graph is terminally detached from further mutation.
    Stale,
}

/// Exact-tier result selected while one publication-core capture is retained.
pub(crate) enum HeadTierSelection {
    /// A completed value or shared construction job must supply the response.
    Access(CacheAccess<CachedHeadSnapshot>),
    /// The prospective current-tier identity matched before construction.
    ConditionMatched(HeadEtag),
}

/// Atomically observed lifecycle state, canonical Head view, and history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphPublicationCore {
    state: GraphPublicationState,
    view: GraphView,
    history: GraphHistory,
}

impl GraphPublicationCore {
    fn head_eligibility_boundary(&self) -> HeadEligibilityBoundary {
        HeadEligibilityBoundary::new(
            self.view.current_revision_timestamp_ms(),
            self.history.replay_floor_revision(),
            self.history.oldest_available_revision(),
        )
    }

    fn head_tier_extent(&self, depth: u32) -> LevelExtent {
        let high_level = self.view.high_level();
        let low_level = high_level.saturating_sub(depth - 1).max(self.view.low_level());
        LevelExtent::new(low_level, high_level).expect("a positive Head tier produces a valid contained extent")
    }

    fn capture_head_tier(&self, publication_id: u64, depth: u32) -> GraphViewCapture {
        let extent = self.head_tier_extent(depth);
        let view = self.view.extract_subview(extent).expect("a selected Head tier is contained in the complete Head");
        GraphViewCapture {
            publication_id,
            replay_floor_revision: self.history.replay_floor_revision(),
            oldest_available_revision: self.history.oldest_available_revision(),
            view,
        }
    }

    fn capture_levels(&self, publication_id: u64, requested_levels: &[u32]) -> Result<GraphLevelCapture, u32> {
        let mut levels = Vec::with_capacity(requested_levels.len());
        for &level in requested_levels {
            let Some(&value) = self.view.level(level) else {
                return Err(level);
            };
            levels.push(HeadLevelValue { level, value });
        }
        Ok(GraphLevelCapture { publication_id, revision: self.view.current_revision_id(), levels: levels.into() })
    }
}

/// One detached immutable graph view from a specific publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphViewCapture {
    publication_id: u64,
    replay_floor_revision: u64,
    oldest_available_revision: u64,
    view: GraphView,
}

impl GraphViewCapture {
    /// Returns the publication that supplied this view.
    pub(crate) const fn publication_id(&self) -> u64 {
        self.publication_id
    }

    /// Returns the inclusive replay floor observed with this view.
    pub(crate) const fn replay_floor_revision(&self) -> u64 {
        self.replay_floor_revision
    }

    /// Returns the oldest retained history boundary observed with this view.
    pub(crate) const fn oldest_available_revision(&self) -> u64 {
        self.oldest_available_revision
    }

    /// Returns the captured frozen graph view.
    pub(crate) const fn view(&self) -> &GraphView {
        &self.view
    }

    /// Consumes the capture and returns its frozen graph view.
    pub(crate) fn into_view(self) -> GraphView {
        self.view
    }
}

/// One all-or-nothing retained-level capture from a specific publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphLevelCapture {
    publication_id: u64,
    revision: u64,
    levels: Arc<[HeadLevelValue]>,
}

impl GraphLevelCapture {
    fn into_outcome(self) -> HeadLevelLookupOutcome {
        HeadLevelLookupOutcome::Complete { publication_id: self.publication_id, revision: self.revision, levels: self.levels }
    }
}

/// One coherent target sampled before selecting retained delta history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GraphDeltaTargetCapture {
    publication_id: u64,
    state: GraphPublicationState,
    revision: u64,
    revision_timestamp_ms: u64,
    head_level: u32,
    replay_floor_revision: u64,
}

impl GraphDeltaTargetCapture {
    /// Returns the publication whose Head supplied this target.
    pub(crate) const fn publication_id(self) -> u64 {
        self.publication_id
    }

    /// Returns the lifecycle state observed with the target Head.
    pub(crate) const fn state(self) -> GraphPublicationState {
        self.state
    }

    /// Returns the captured Head revision.
    pub(crate) const fn revision(self) -> u64 {
        self.revision
    }

    /// Returns the captured revision's publication-local timestamp.
    pub(crate) const fn revision_timestamp_ms(self) -> u64 {
        self.revision_timestamp_ms
    }

    /// Returns the captured Head level.
    pub(crate) const fn head_level(self) -> u32 {
        self.head_level
    }

    /// Returns the inclusive replay floor observed with the target Head.
    pub(crate) const fn replay_floor_revision(self) -> u64 {
        self.replay_floor_revision
    }
}

/// Result of linearizing one immutable cached response against the current
/// history replay floor.
pub(crate) enum ReplayFloorAdmission<T> {
    /// The immutable response may proceed even if the floor advances later.
    Admitted(Arc<T>),
    /// The response revision already precedes the inclusive replay floor.
    Rejected {
        /// Floor observed while holding the publication core read lock.
        replay_floor_revision: u64,
    },
}

/// One publication-local graph lineage and its synchronization boundary.
pub(crate) struct GraphPublication {
    publication_id: u64,
    revision_clock_origin: Instant,
    mutation: Mutex<()>,
    core: RwLock<GraphPublicationCore>,
    cache: Arc<GraphCache>,
    clients: SseClientRegistry,
    #[cfg(test)]
    atomic_mutation_test_hook: Option<std::sync::Arc<AtomicMutationTestHook>>,
    #[cfg(test)]
    delta_estimate_test_override: Option<u64>,
    #[cfg(test)]
    replay_floor_admission_test_hook: Option<std::sync::Arc<ReplayFloorAdmissionTestHook>>,
    #[cfg(test)]
    head_second_election_test_hook: Option<std::sync::Arc<HeadSecondElectionTestHook>>,
}

#[cfg(test)]
/// Deterministic pause inside one atomic publication mutation.
pub(crate) struct AtomicMutationTestHook {
    armed: std::sync::atomic::AtomicBool,
    view_updated: std::sync::Barrier,
    finish_core_update: std::sync::Barrier,
}

#[cfg(test)]
impl AtomicMutationTestHook {
    fn new() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(true),
            view_updated: std::sync::Barrier::new(2),
            finish_core_update: std::sync::Barrier::new(2),
        }
    }

    fn pause_after_view_update(&self) {
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.view_updated.wait();
            self.finish_core_update.wait();
        }
    }

    /// Waits until the graph view has changed while the core write lock remains held.
    pub(crate) fn wait_for_view_update(&self) {
        self.view_updated.wait();
    }

    /// Allows the paused mutation to finish its core update.
    pub(crate) fn resume_core_update(&self) {
        self.finish_core_update.wait();
    }
}

#[cfg(test)]
/// Deterministic pause while one replay-floor admission holds the core read lock.
pub(crate) struct ReplayFloorAdmissionTestHook {
    core_locked: std::sync::Barrier,
    finish_admission: std::sync::Barrier,
}

#[cfg(test)]
impl ReplayFloorAdmissionTestHook {
    fn new() -> Self {
        Self { core_locked: std::sync::Barrier::new(2), finish_admission: std::sync::Barrier::new(2) }
    }

    fn pause_with_core_lock(&self) {
        self.core_locked.wait();
        self.finish_admission.wait();
    }

    /// Waits until admission has acquired the publication-core read lock.
    pub(crate) fn wait_for_core_lock(&self) {
        self.core_locked.wait();
    }

    /// Allows the paused admission to return its result.
    pub(crate) fn resume_admission(&self) {
        self.finish_admission.wait();
    }
}

#[cfg(test)]
/// Deterministic pause between prospective evaluation and the second Head-cache election.
pub(crate) struct HeadSecondElectionTestHook {
    armed: std::sync::atomic::AtomicBool,
    prospective_evaluated: std::sync::Barrier,
    continue_election: std::sync::Barrier,
}

#[cfg(test)]
impl HeadSecondElectionTestHook {
    fn new() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(true),
            prospective_evaluated: std::sync::Barrier::new(2),
            continue_election: std::sync::Barrier::new(2),
        }
    }

    fn pause_before_second_election(&self) {
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.prospective_evaluated.wait();
            self.continue_election.wait();
        }
    }

    /// Waits until a nonmatching prospective identity has been evaluated.
    pub(crate) fn wait_for_prospective_evaluation(&self) {
        self.prospective_evaluated.wait();
    }

    /// Allows the paused request to perform its second cache election.
    pub(crate) fn resume_second_election(&self) {
        self.continue_election.wait();
    }
}

impl GraphPublication {
    /// Creates an aligned unpublished publication from one construction candidate.
    pub(crate) fn new(
        initial_state: GraphPublicationState,
        revision_clock_origin: Instant,
        view: GraphView,
        history: GraphHistory,
    ) -> Result<Self, PublicationProjectionError> {
        assert_ne!(initial_state, GraphPublicationState::Stale, "a fresh publication is Synchronizing or Live");
        assert_eq!(view.tracking_policy(), TrackingPolicy::Head, "a publication owns a Head graph view");

        let view_revision = view.current_revision_id();
        let history_revision = history.current_revision_id();
        if view_revision != history_revision {
            return Err(PublicationProjectionError::RevisionMismatch { view_revision, history_revision });
        }
        assert!(
            view.current_revision_timestamp_ms() <= revision_timestamp_ms_at(revision_clock_origin, Instant::now()),
            "candidate revision timestamp belongs to the publication clock"
        );

        let publication_id = random_nonzero_publication_id();
        Ok(Self {
            publication_id,
            revision_clock_origin,
            mutation: Mutex::new(()),
            core: RwLock::new(GraphPublicationCore { state: initial_state, view, history }),
            cache: Arc::new(GraphCache::new()),
            clients: SseClientRegistry::new(publication_id, initial_state, view_revision),
            #[cfg(test)]
            atomic_mutation_test_hook: None,
            #[cfg(test)]
            delta_estimate_test_override: None,
            #[cfg(test)]
            replay_floor_admission_test_hook: None,
            #[cfg(test)]
            head_second_election_test_hook: None,
        })
    }

    /// Returns this publication's immutable random identity.
    pub(crate) const fn publication_id(&self) -> u64 {
        self.publication_id
    }

    /// Returns this publication's response cache.
    pub(crate) fn cache(&self) -> Arc<GraphCache> {
        self.cache.clone()
    }

    /// Forces every test update to cross the replay-barrier estimate.
    #[cfg(test)]
    pub(crate) fn force_replay_barrier_estimate(&mut self) {
        self.delta_estimate_test_override = Some(estimate::SOFT_DELTA_ESTIMATED_BYTES);
    }

    /// Installs a test-only pause after the graph view mutates under the core write lock.
    #[cfg(test)]
    pub(crate) fn pause_atomic_mutation_after_view_update(&mut self) -> std::sync::Arc<AtomicMutationTestHook> {
        let hook = std::sync::Arc::new(AtomicMutationTestHook::new());
        self.atomic_mutation_test_hook = Some(hook.clone());
        hook
    }

    /// Installs a test-only pause while replay-floor admission holds the core read lock.
    #[cfg(test)]
    pub(crate) fn pause_replay_floor_admission(&mut self) -> std::sync::Arc<ReplayFloorAdmissionTestHook> {
        let hook = std::sync::Arc::new(ReplayFloorAdmissionTestHook::new());
        self.replay_floor_admission_test_hook = Some(hook.clone());
        hook
    }

    /// Installs a test-only pause after prospective evaluation and before the second cache election.
    #[cfg(test)]
    pub(crate) fn pause_before_head_second_election(&mut self) -> std::sync::Arc<HeadSecondElectionTestHook> {
        let hook = std::sync::Arc::new(HeadSecondElectionTestHook::new());
        self.head_second_election_test_hook = Some(hook.clone());
        hook
    }

    /// Returns this publication's client-registration and wake registry.
    pub(crate) const fn client_registry(&self) -> &SseClientRegistry {
        &self.clients
    }

    /// Samples whole milliseconds elapsed in this publication's clock domain.
    pub(crate) fn revision_timestamp_ms(&self) -> u64 {
        self.revision_timestamp_ms_at(Instant::now())
    }

    /// Transitions a visible Synchronizing publication to Live.
    pub(crate) async fn transition_live(&self) {
        let _mutation = self.mutation.lock().await;
        let mut core = self.core.write().await;
        assert_eq!(core.state, GraphPublicationState::Synchronizing, "only a Synchronizing publication becomes Live");
        core.state = GraphPublicationState::Live;
        let revision = core.view.current_revision_id();
        drop(core);
        self.clients.publish_state(GraphPublicationState::Live, revision);
    }

    /// Makes a visible publication terminally Stale.
    ///
    /// Returns whether this call performed the transition. Repeating the
    /// terminal transition has no additional effect.
    pub(crate) async fn transition_stale(&self) -> bool {
        let _mutation = self.mutation.lock().await;
        let mut core = self.core.write().await;
        if core.state == GraphPublicationState::Stale {
            return false;
        }
        core.state = GraphPublicationState::Stale;
        let revision = core.view.current_revision_id();
        drop(core);
        self.clients.publish_state(GraphPublicationState::Stale, revision);
        true
    }

    /// Captures the complete current Head as an owned frozen view.
    pub(crate) async fn capture_view(&self) -> GraphViewCapture {
        let core = self.core.read().await;
        let view = core.view.extract_subview(core.view.extent()).expect("a graph view contains its own extent");
        GraphViewCapture {
            publication_id: self.publication_id,
            replay_floor_revision: core.history.replay_floor_revision(),
            oldest_available_revision: core.history.oldest_available_revision(),
            view,
        }
    }

    /// Captures one contained extent from the current Head as an owned frozen view.
    pub(crate) async fn capture_subview(&self, extent: LevelExtent) -> Result<GraphViewCapture, GraphViewExtractError> {
        let core = self.core.read().await;
        let view = core.view.extract_subview(extent)?;
        Ok(GraphViewCapture {
            publication_id: self.publication_id,
            replay_floor_revision: core.history.replay_floor_revision(),
            oldest_available_revision: core.history.oldest_available_revision(),
            view,
        })
    }

    /// Captures one coherent canonical-delta target from the current core.
    pub(crate) async fn capture_delta_target(&self) -> GraphDeltaTargetCapture {
        let core = self.core.read().await;
        GraphDeltaTargetCapture {
            publication_id: self.publication_id,
            state: core.state,
            revision: core.view.current_revision_id(),
            revision_timestamp_ms: core.view.current_revision_timestamp_ms(),
            head_level: core.view.high_level(),
            replay_floor_revision: core.history.replay_floor_revision(),
        }
    }

    /// Captures every requested retained level, or identifies one unavailable level.
    pub(crate) async fn capture_levels(&self, requested_levels: &[u32]) -> HeadLevelLookupOutcome {
        match self.core.read().await.capture_levels(self.publication_id, requested_levels) {
            Ok(capture) => capture.into_outcome(),
            Err(requested_level) => HeadLevelLookupOutcome::LevelUnavailable { requested_level },
        }
    }

    /// Captures immutable retained history through a previously sampled target.
    pub(crate) async fn capture_history_range(
        &self,
        from_revision_id: u64,
        target: GraphDeltaTargetCapture,
    ) -> Result<GraphHistoryRange, GraphHistoryRangeError> {
        assert_eq!(target.publication_id, self.publication_id, "delta target belongs to this publication");
        self.core.read().await.history.range(from_revision_id, target.revision)
    }

    /// Selects one exact configured Head tier, admitting a matching identity before construction.
    pub(crate) async fn head_tier(
        self: &Arc<Self>,
        jobs: &CacheJobTracker,
        scheduler: &EncodingScheduler,
        depth: u32,
        condition_matches: impl FnOnce(&HeadEtag) -> bool,
    ) -> HeadTierSelection {
        let core = self.core.read().await;
        let boundary = core.head_eligibility_boundary();
        match self.cache.lookup_head(depth, boundary) {
            HeadCacheLookup::Completed(completed) => {
                return HeadTierSelection::Access(CacheAccess::Completed(completed));
            }
            HeadCacheLookup::Running(running) => return HeadTierSelection::Access(CacheAccess::Job(running)),
            HeadCacheLookup::ConstructionRequired => {}
        }

        let extent = core.head_tier_extent(depth);
        let etag = HeadEtagInput::new(self.publication_id, core.view.current_revision_id(), extent).etag();
        if condition_matches(&etag) {
            #[cfg(test)]
            if let Some(hook) = &self.replay_floor_admission_test_hook {
                hook.pause_with_core_lock();
            }
            return HeadTierSelection::ConditionMatched(etag);
        }

        #[cfg(test)]
        if let Some(hook) = &self.head_second_election_test_hook {
            hook.pause_before_second_election();
        }

        let publication = self.clone();
        let scheduler = scheduler.clone();
        HeadTierSelection::Access(self.cache.elect_head(
            jobs,
            depth,
            boundary,
            || core.capture_head_tier(self.publication_id, depth),
            move |capture, cancellation| async move { publication.build_head_tier(depth, capture, scheduler, cancellation).await },
        ))
    }

    /// Linearizes one completed cached delta against the current replay floor.
    pub(crate) async fn admit_cached_delta_replay_floor(&self, candidate: Arc<CachedDelta>) -> ReplayFloorAdmission<CachedDelta> {
        let source_revision = candidate.source_revision_id();
        self.admit_replay_floor(source_revision, candidate).await
    }

    /// Linearizes one completed cached Head snapshot against the current replay floor.
    pub(crate) async fn admit_cached_head_replay_floor(
        &self,
        candidate: Arc<CachedHeadSnapshot>,
    ) -> ReplayFloorAdmission<CachedHeadSnapshot> {
        let revision = candidate.revision();
        self.admit_replay_floor(revision, candidate).await
    }

    /// Applies one committed block and publishes any resulting history entry.
    ///
    /// Returns the newly published target revision, or `None` when the update
    /// has no retained effect and advances neither core graph value.
    pub(crate) async fn apply_block_committed(&self, committed: &BlockCommitted) -> Result<Option<u64>, PublicationProjectionError> {
        self.apply_original_update(OriginalUpdate::Block(committed)).await
    }

    /// Applies one committed VSPC transition and publishes any resulting history entry.
    ///
    /// Returns the newly published target revision, or `None` when the update
    /// has no retained effect and advances neither core graph value.
    pub(crate) async fn apply_vspc_committed(&self, committed: &VspcCommitted) -> Result<Option<u64>, PublicationProjectionError> {
        self.apply_original_update(OriginalUpdate::Vspc(committed)).await
    }

    async fn apply_original_update(&self, update: OriginalUpdate<'_>) -> Result<Option<u64>, PublicationProjectionError> {
        let mutation = self.mutation.lock().await;
        let mut core = self.core.write().await;
        assert_ne!(core.state, GraphPublicationState::Stale, "a Stale publication is not mutated");
        let GraphPublicationCore { view, history, .. } = &mut *core;
        let applied = AppliedLineageUpdate::apply(
            view,
            history,
            update,
            self.revision_timestamp_ms(),
            |delta| {
                #[cfg(test)]
                if let Some(estimated_raw_bytes) = self.delta_estimate_test_override {
                    return estimated_raw_bytes;
                }
                estimate_raw_delta_bytes(delta)
            },
            || {
                #[cfg(test)]
                if let Some(hook) = &self.atomic_mutation_test_hook {
                    hook.pause_after_view_update();
                }
            },
        )?;
        let Some(applied) = applied else {
            return Ok(None);
        };
        let invalidation =
            applied.replay_floor_advance.map(|replay_floor_revision| self.cache.invalidate_replay_floor(replay_floor_revision));
        drop(core);
        if let Some(invalidation) = invalidation {
            invalidation.cancel();
        }
        self.clients.advance_head(applied.target_revision);
        drop(mutation);
        Ok(Some(applied.target_revision))
    }

    async fn admit_replay_floor<T>(&self, candidate_revision: u64, candidate: Arc<T>) -> ReplayFloorAdmission<T> {
        let core = self.core.read().await;
        #[cfg(test)]
        if let Some(hook) = &self.replay_floor_admission_test_hook {
            hook.pause_with_core_lock();
        }
        let replay_floor_revision = core.history.replay_floor_revision();
        if candidate_revision < replay_floor_revision {
            ReplayFloorAdmission::Rejected { replay_floor_revision }
        } else {
            ReplayFloorAdmission::Admitted(candidate)
        }
    }

    async fn build_head_tier(
        self: Arc<Self>,
        depth: u32,
        mut capture: GraphViewCapture,
        scheduler: EncodingScheduler,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<Arc<CachedHeadSnapshot>, CacheBuildError> {
        loop {
            let view = capture.view();
            let revision = view.current_revision_id();
            let revision_timestamp_ms = view.current_revision_timestamp_ms();
            let low_level = view.low_level();
            let high_level = view.high_level();
            let response = HeadSnapshotResponseDto::from_capture(&capture);
            let etag = response.etag_input().etag();
            let encoded = scheduler.encode_priority(response, cancellation.clone()).await?;
            let candidate = Arc::new(CachedHeadSnapshot::new(
                depth,
                revision,
                revision_timestamp_ms,
                low_level,
                high_level,
                encoded.uncompressed_json_bytes(),
                etag,
                encoded.into_gzip_body(),
            ));

            if self.head_candidate_is_eligible(&candidate).await {
                return Ok(candidate);
            }
            capture = self.capture_head_tier(depth).await;
        }
    }

    async fn capture_head_tier(&self, depth: u32) -> GraphViewCapture {
        let core = self.core.read().await;
        core.capture_head_tier(self.publication_id, depth)
    }

    async fn head_candidate_is_eligible(&self, candidate: &CachedHeadSnapshot) -> bool {
        let core = self.core.read().await;
        core.head_eligibility_boundary().admits(candidate)
    }

    fn revision_timestamp_ms_at(&self, now: Instant) -> u64 {
        revision_timestamp_ms_at(self.revision_clock_origin, now)
    }
}

enum OriginalUpdate<'a> {
    Block(&'a BlockCommitted),
    Vspc(&'a VspcCommitted),
}

impl OriginalUpdate<'_> {
    fn apply(self, view: &mut GraphView, revision_timestamp_ms: u64) -> Result<Option<GraphDelta>, PublicationProjectionError> {
        match self {
            Self::Block(committed) => Ok(view.apply_head_block_committed(committed, revision_timestamp_ms)?),
            Self::Vspc(committed) => Ok(view.apply_head_vspc_committed(committed, revision_timestamp_ms)?),
        }
    }
}

struct AppliedLineageUpdate {
    target_revision: u64,
    replay_floor_advance: Option<u64>,
}

impl AppliedLineageUpdate {
    fn apply(
        view: &mut GraphView,
        history: &mut GraphHistory,
        update: OriginalUpdate<'_>,
        revision_timestamp_ms: u64,
        estimate_delta: impl FnOnce(&GraphDelta) -> u64,
        after_view_update: impl FnOnce(),
    ) -> Result<Option<Self>, PublicationProjectionError> {
        let view_revision = view.current_revision_id();
        let history_revision = history.current_revision_id();
        if view_revision != history_revision {
            return Err(PublicationProjectionError::RevisionMismatch { view_revision, history_revision });
        }

        let Some(delta) = update.apply(view, revision_timestamp_ms)? else {
            return Ok(None);
        };
        after_view_update();
        let target_revision = delta.to_revision_id();
        let estimated_raw_bytes = estimate_delta(&delta);
        let replayability = classify_replayability(estimated_raw_bytes);
        let replay_floor_advance = (replayability == GraphHistoryReplayability::ReplayBarrier).then_some(target_revision);
        history.append(delta, estimated_raw_bytes, replayability).expect("a successful Head update extends its matching history");
        history.prune(view.low_level());
        Ok(Some(Self { target_revision, replay_floor_advance }))
    }
}

fn revision_timestamp_ms_at(origin: Instant, now: Instant) -> u64 {
    let elapsed = now.checked_duration_since(origin).expect("publication clock does not precede its origin");
    u64::try_from(elapsed.as_millis()).expect("publication revision timestamp fits u64")
}

fn random_nonzero_publication_id() -> u64 {
    let mut rng = OsRng;
    sample_nonzero_publication_id(|| rng.next_u64())
}

fn sample_nonzero_publication_id(mut sample: impl FnMut() -> u64) -> u64 {
    loop {
        let candidate = sample();
        if candidate != 0 {
            return candidate;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        future::Future,
        sync::Arc,
        time::Duration,
    };

    use bytes::Bytes;
    use kgi_api_model::{
        delta::GraphDelta,
        graph::{GraphViewUpdateError, Level, LevelExtent},
        history::{GraphHistoryRange, GraphHistoryRangeError, GraphHistoryReplayability},
    };
    use kgi_model::{
        block::{BlockColor, BlockCoordinate, BlockHash, CompactId},
        graph_update::{BlockCommitted, LevelCommitted, ParentCommitted, VspcCommitted},
    };

    use super::*;
    use crate::{
        cache::{
            CacheAccess, CacheBuildError, CacheJobTracker, CacheWaitError, CachedDelta, CachedHeadSnapshot, DeltaBuildContext,
            DeltaCacheLookup, GraphCache, HeadCacheLookup, HeadEligibilityBoundary,
        },
        client::{DeltaCompletionResult, DeltaScheduleAction, SseMailbox, SseMessage},
        codec::{
            HeadEtagInput,
            sse::{PublicationStateDto, PublicationWakeupDto},
        },
    };

    fn head_view() -> GraphView {
        head_view_with_depth(1)
    }

    fn head_view_with_depth(max_depth: u32) -> GraphView {
        GraphView::from_database_projection(
            max_depth,
            LevelExtent::new(1, 1).expect("valid extent"),
            TrackingPolicy::Head,
            BTreeMap::from([(1, Level { size: 1, daa_score: None })]),
            HashMap::new(),
            HashMap::new(),
        )
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn committed_block(byte: u8) -> BlockCommitted {
        let level = u32::from(byte);
        let parent_level = level - 1;
        BlockCommitted {
            id: CompactId::new(i64::from(byte)).expect("positive compact ID"),
            hash: hash(byte),
            coordinate: BlockCoordinate::new(level, 0).expect("positive coordinate"),
            timestamp: u64::from(level) * 10,
            daa_score: u64::from(level) * 100,
            selected_parent_index: Some(0),
            direct_parents: Arc::from([ParentCommitted {
                hash: hash(byte - 1),
                coordinate: Some(BlockCoordinate::new(parent_level, 0).expect("positive parent coordinate")),
            }]),
            blue_merge_set: Arc::from([]),
            red_merge_set: Arc::from([]),
            color: BlockColor::Gray,
            is_in_vspc: false,
            level_snapshots: Arc::from([
                LevelCommitted { level: parent_level, size: 1, daa_score: None },
                LevelCommitted { level, size: 1, daa_score: None },
            ]),
        }
    }

    fn vspc_added(byte: u8, daa_score: Option<u64>) -> VspcCommitted {
        VspcCommitted {
            source: hash(byte - 1),
            destination: hash(byte),
            removed: Arc::from([]),
            added: Arc::from([hash(byte)]),
            level_snapshots: Arc::from([LevelCommitted { level: u32::from(byte), size: 1, daa_score }]),
        }
    }

    fn advanced_delta() -> GraphDelta {
        GraphDelta::new(0, 1, 123, 1, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new())
            .expect("valid advancing delta")
    }

    fn cached_head_candidate(publication_id: u64, revision: u64) -> Arc<CachedHeadSnapshot> {
        cached_head_candidate_at_depth(publication_id, revision, 50)
    }

    fn cached_head_candidate_at_depth(publication_id: u64, revision: u64, depth: u32) -> Arc<CachedHeadSnapshot> {
        let extent = LevelExtent::new(1, 1).expect("valid cached Head extent");
        Arc::new(CachedHeadSnapshot::new(
            depth,
            revision,
            revision * 10,
            1,
            1,
            4,
            HeadEtagInput::new(publication_id, revision, extent).etag(),
            Bytes::from_static(b"head"),
        ))
    }

    fn delta_access<F, Fut>(
        cache: &Arc<GraphCache>,
        jobs: &CacheJobTracker,
        source_revision: u64,
        target_revision: u64,
        builder: F,
    ) -> CacheAccess<CachedDelta>
    where
        F: FnOnce(DeltaBuildContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<CachedDelta>, CacheBuildError>> + Send + 'static,
    {
        match cache.lookup_delta(source_revision, target_revision, 0) {
            DeltaCacheLookup::Completed(value) => CacheAccess::Completed(value),
            DeltaCacheLookup::Running(job) => CacheAccess::Job(job),
            DeltaCacheLookup::ConstructionRequired => cache.elect_delta(jobs, source_revision, 0, target_revision, false, builder),
        }
    }

    fn publication_with_atomic_mutation_hook(max_depth: u32) -> (Arc<GraphPublication>, Arc<AtomicMutationTestHook>) {
        let hook = Arc::new(AtomicMutationTestHook::new());
        let mut publication = GraphPublication::new(
            GraphPublicationState::Synchronizing,
            Instant::now(),
            head_view_with_depth(max_depth),
            GraphHistory::new(),
        )
        .expect("matching candidate");
        publication.atomic_mutation_test_hook = Some(Arc::clone(&hook));
        (Arc::new(publication), hook)
    }

    #[tokio::test]
    async fn constructs_synchronizing_and_direct_live_publications() {
        for state in [GraphPublicationState::Synchronizing, GraphPublicationState::Live] {
            let publication =
                GraphPublication::new(state, Instant::now(), head_view(), GraphHistory::new()).expect("matching candidate");

            assert_ne!(publication.publication_id(), 0);
            let core = publication.core.read().await;
            assert_eq!(core.state, state);
            assert_eq!(core.view.current_revision_id(), 0);
            assert_eq!(core.view.current_revision_timestamp_ms(), 0);
            assert_eq!(core.history.current_revision_id(), 0);
            drop(core);
            assert!(publication.mutation.try_lock().is_ok());

            let mailbox = Arc::new(SseMailbox::new());
            let identifier = publication
                .client_registry()
                .register(0, &mailbox, tokio_util::sync::CancellationToken::new())
                .expect("initial registration succeeds");
            assert_eq!(mailbox.recv().await, Some(SseMessage::ClientRegistration(identifier.registration())));
            assert_eq!(
                mailbox.recv().await,
                Some(SseMessage::PublicationState(PublicationStateDto::new(publication.publication_id(), 0, state)))
            );
            assert_eq!(
                mailbox.recv().await,
                Some(SseMessage::PublicationWakeup(PublicationWakeupDto::new(publication.publication_id(), 0)))
            );
        }
    }

    #[tokio::test]
    async fn matching_prospective_head_identity_leaves_the_exact_tier_vacant() {
        let publication = Arc::new(
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("coherent publication"),
        );
        let cache = publication.cache();
        let jobs = CacheJobTracker::new();
        let scheduler = EncodingScheduler::production();
        let expected =
            HeadEtagInput::new(publication.publication_id(), 0, LevelExtent::new(1, 1).expect("valid prospective extent")).etag();

        let selection = publication.head_tier(&jobs, &scheduler, 20, |etag| etag == &expected).await;
        assert!(matches!(selection, HeadTierSelection::ConditionMatched(etag) if etag == expected));
        assert!(matches!(cache.lookup_head(20, HeadEligibilityBoundary::new(0, 0, 0)), HeadCacheLookup::ConstructionRequired));

        jobs.shutdown().await;
        scheduler.shutdown().await.expect("scheduler shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn second_head_election_joins_a_job_installed_after_the_initial_miss() {
        let publication = Arc::new(
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("coherent publication"),
        );
        let cache = publication.cache();
        let jobs = Arc::new(CacheJobTracker::new());
        let scheduler = EncodingScheduler::production();
        let condition_entered = Arc::new(std::sync::Barrier::new(2));
        let resume_condition = Arc::new(std::sync::Barrier::new(2));

        let request_publication = publication.clone();
        let request_jobs = jobs.clone();
        let request_scheduler = scheduler.clone();
        let request_entered = condition_entered.clone();
        let request_resume = resume_condition.clone();
        let request = tokio::spawn(async move {
            request_publication
                .head_tier(&request_jobs, &request_scheduler, 20, |_| {
                    request_entered.wait();
                    request_resume.wait();
                    false
                })
                .await
        });
        condition_entered.wait();

        let competing = match cache.elect_head(
            &jobs,
            20,
            HeadEligibilityBoundary::new(0, 0, 0),
            || (),
            |(), _| async { std::future::pending::<Result<Arc<CachedHeadSnapshot>, CacheBuildError>>().await },
        ) {
            CacheAccess::Job(job) => job,
            CacheAccess::Completed(_) => panic!("the competing caller installs a running job"),
        };
        resume_condition.wait();

        let HeadTierSelection::Access(CacheAccess::Job(joined)) = request.await.expect("request task completes") else {
            panic!("the second election joins the competing job")
        };
        assert!(Arc::ptr_eq(&joined, &competing));

        jobs.shutdown().await;
        scheduler.shutdown().await.expect("scheduler shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn second_head_election_reuses_a_completion_installed_after_the_initial_miss() {
        let publication = Arc::new(
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("coherent publication"),
        );
        let cache = publication.cache();
        let jobs = Arc::new(CacheJobTracker::new());
        let scheduler = EncodingScheduler::production();
        let condition_entered = Arc::new(std::sync::Barrier::new(2));
        let resume_condition = Arc::new(std::sync::Barrier::new(2));

        let request_publication = publication.clone();
        let request_jobs = jobs.clone();
        let request_scheduler = scheduler.clone();
        let request_entered = condition_entered.clone();
        let request_resume = resume_condition.clone();
        let request = tokio::spawn(async move {
            request_publication
                .head_tier(&request_jobs, &request_scheduler, 20, |_| {
                    request_entered.wait();
                    request_resume.wait();
                    false
                })
                .await
        });
        condition_entered.wait();

        let expected = cached_head_candidate_at_depth(publication.publication_id(), 0, 20);
        let competing = cache.elect_head(
            &jobs,
            20,
            HeadEligibilityBoundary::new(0, 0, 0),
            || expected.clone(),
            |candidate, _| async move { Ok(candidate) },
        );
        let completed =
            competing.resolve(tokio_util::sync::CancellationToken::new()).await.expect("competing completion is installed");
        resume_condition.wait();

        let HeadTierSelection::Access(CacheAccess::Completed(reused)) = request.await.expect("request task completes") else {
            panic!("the second election reuses the competing completion")
        };
        assert!(Arc::ptr_eq(&reused, &completed));

        jobs.shutdown().await;
        scheduler.shutdown().await.expect("scheduler shuts down");
    }

    #[tokio::test]
    async fn accepts_an_already_advanced_matching_candidate() {
        let delta = advanced_delta();
        let mut view = head_view();
        view.apply_delta(&delta).expect("matching Head delta");
        let mut history = GraphHistory::new();
        history.append(delta, 1, GraphHistoryReplayability::Replayable).expect("matching history delta");

        let origin = Instant::now().checked_sub(Duration::from_millis(123)).expect("valid fixture origin");
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, origin, view, history).expect("matching advanced candidate");

        let core = publication.core.read().await;
        assert_eq!(core.view.current_revision_id(), 1);
        assert_eq!(core.history.current_revision_id(), 1);
        drop(core);

        let mailbox = Arc::new(SseMailbox::new());
        publication
            .client_registry()
            .register(0, &mailbox, tokio_util::sync::CancellationToken::new())
            .expect("registration succeeds");
        mailbox.recv().await.expect("client-registration message");
        assert_eq!(
            mailbox.recv().await,
            Some(SseMessage::PublicationState(PublicationStateDto::new(
                publication.publication_id(),
                1,
                GraphPublicationState::Synchronizing,
            )))
        );
        assert_eq!(
            mailbox.recv().await,
            Some(SseMessage::PublicationWakeup(PublicationWakeupDto::new(publication.publication_id(), 1)))
        );
    }

    #[tokio::test]
    async fn original_updates_advance_core_and_noops_advance_neither_graph_value() {
        let origin = Instant::now().checked_sub(Duration::from_millis(10)).expect("valid fixture origin");
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, origin, head_view_with_depth(2), GraphHistory::new())
                .expect("matching candidate");
        let mailbox = Arc::new(SseMailbox::new());
        let identifier = publication
            .client_registry()
            .register(0, &mailbox, tokio_util::sync::CancellationToken::new())
            .expect("registration succeeds");
        for _ in 0..3 {
            mailbox.recv().await.expect("initial registration message");
        }
        assert_eq!(
            publication
                .client_registry()
                .complete_delta(identifier, DeltaScheduleAction::Replace { wake_at_revision_id: 1, wake_sent: false }),
            DeltaCompletionResult::Accepted
        );

        assert_eq!(publication.apply_block_committed(&committed_block(2)).await.expect("block update"), Some(1));
        assert_eq!(
            mailbox.recv().await,
            Some(SseMessage::PublicationWakeup(PublicationWakeupDto::new(publication.publication_id(), 1)))
        );
        {
            let core = publication.core.read().await;
            assert_eq!(core.view.current_revision_id(), 1);
            assert!(core.view.current_revision_timestamp_ms() >= 10);
            assert_eq!(core.view.block(&hash(2)).map(|block| block.is_in_vspc), Some(false));
            assert_eq!(core.history.current_revision_id(), 1);
            assert_eq!(core.history.len(), 1);
            assert_eq!(core.history.entries().next().expect("block entry").estimated_raw_bytes(), 828);
        }

        let vspc = vspc_added(2, Some(200));
        assert_eq!(
            publication
                .client_registry()
                .complete_delta(identifier, DeltaScheduleAction::Replace { wake_at_revision_id: 2, wake_sent: false }),
            DeltaCompletionResult::Accepted
        );
        assert_eq!(publication.apply_vspc_committed(&vspc).await.expect("VSPC update"), Some(2));
        assert_eq!(
            mailbox.recv().await,
            Some(SseMessage::PublicationWakeup(PublicationWakeupDto::new(publication.publication_id(), 2)))
        );
        let core = publication.core.read().await;
        assert_eq!(core.view.block(&hash(2)).map(|block| block.is_in_vspc), Some(true));
        assert_eq!(core.history.current_revision_id(), 2);
        drop(core);

        assert_eq!(publication.apply_vspc_committed(&vspc).await.expect("VSPC no-op"), None);
        let core = publication.core.read().await;
        assert_eq!(core.view.current_revision_id(), 2);
        assert_eq!(core.history.len(), 2);
    }

    #[tokio::test]
    async fn original_update_classifies_the_exact_soft_estimate_boundary() {
        for (estimated_raw_bytes, expected_replayability) in [
            (estimate::SOFT_DELTA_ESTIMATED_BYTES - 1, GraphHistoryReplayability::Replayable),
            (estimate::SOFT_DELTA_ESTIMATED_BYTES, GraphHistoryReplayability::ReplayBarrier),
            (estimate::SOFT_DELTA_ESTIMATED_BYTES + 1, GraphHistoryReplayability::ReplayBarrier),
        ] {
            let expected_replay_floor = match expected_replayability {
                GraphHistoryReplayability::Replayable => 0,
                GraphHistoryReplayability::ReplayBarrier => 1,
            };
            let mut publication =
                GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                    .expect("matching candidate");
            publication.delta_estimate_test_override = Some(estimated_raw_bytes);

            assert_eq!(publication.apply_block_committed(&committed_block(2)).await.expect("block update"), Some(1));

            let core = publication.core.read().await;
            assert_eq!(core.view.current_revision_id(), 1);
            assert_eq!(core.history.current_revision_id(), 1);
            assert_eq!(core.history.current_cumulative_estimated_bytes(), estimated_raw_bytes);
            match expected_replayability {
                GraphHistoryReplayability::Replayable => {
                    assert_eq!(core.history.replay_floor_revision(), 0);
                    assert_eq!(core.history.len(), 1);
                }
                GraphHistoryReplayability::ReplayBarrier => {
                    assert_eq!(core.history.replay_floor_revision(), 1);
                    assert!(core.history.is_empty());
                }
            }
            drop(core);

            let view = publication.capture_view().await;
            let target = publication.capture_delta_target().await;
            assert_eq!(view.replay_floor_revision(), expected_replay_floor);
            assert_eq!(view.oldest_available_revision(), expected_replay_floor);
            assert_eq!(target.replay_floor_revision(), expected_replay_floor);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replay_barrier_invalidates_cache_before_publishing_core_and_sse_head() {
        let (mut publication, hook) = publication_with_atomic_mutation_hook(2);
        Arc::get_mut(&mut publication).expect("fixture publication is not yet shared").delta_estimate_test_override =
            Some(estimate::SOFT_DELTA_ESTIMATED_BYTES);
        let cache = publication.cache();
        let jobs = CacheJobTracker::new();

        let CacheAccess::Job(initial_delta) = delta_access(&cache, &jobs, 0, 1, |_| async {
            Ok(Arc::new(CachedDelta::new(0, 1, false, 4, Bytes::from_static(b"old-delta"))))
        }) else {
            panic!("fixture delta installs one cache job");
        };
        initial_delta.wait(tokio_util::sync::CancellationToken::new()).await.expect("fixture delta completes");

        let head_access = cache.elect_head(
            &jobs,
            50,
            HeadEligibilityBoundary::new(0, 0, 0),
            || (),
            |(), _| async { std::future::pending::<Result<Arc<CachedHeadSnapshot>, CacheBuildError>>().await },
        );

        let mailbox = Arc::new(SseMailbox::new());
        let identifier = publication
            .client_registry()
            .register(0, &mailbox, tokio_util::sync::CancellationToken::new())
            .expect("registration succeeds");
        for _ in 0..3 {
            mailbox.recv().await.expect("initial registration message");
        }
        assert_eq!(
            publication
                .client_registry()
                .complete_delta(identifier, DeltaScheduleAction::Replace { wake_at_revision_id: 1, wake_sent: false }),
            DeltaCompletionResult::Accepted
        );

        let update_publication = publication.clone();
        let update = tokio::spawn(async move { update_publication.apply_block_committed(&committed_block(2)).await });
        hook.wait_for_view_update();

        let capture_publication = publication.clone();
        let capture = tokio::spawn(async move { capture_publication.capture_delta_target().await });
        tokio::task::yield_now().await;
        assert!(!capture.is_finished(), "the new core remains hidden before replay-floor invalidation");
        assert!(matches!(
            delta_access(&cache, &jobs, 0, 1, |_| async { panic!("the pre-floor completed value still exists before invalidation") }),
            CacheAccess::Completed(_)
        ));

        hook.resume_core_update();
        assert_eq!(update.await.expect("update task").expect("replay-barrier update"), Some(1));
        let captured = capture.await.expect("capture task");
        assert_eq!(captured.revision(), 1);
        assert_eq!(captured.replay_floor_revision(), 1);
        assert_eq!(
            mailbox.recv().await,
            Some(SseMessage::PublicationWakeup(PublicationWakeupDto::new(publication.publication_id(), 1)))
        );
        assert_eq!(
            head_access.resolve(tokio_util::sync::CancellationToken::new()).await,
            Err(CacheWaitError::InvalidatedByReplayFloor { replay_floor_revision: 1 })
        );
        assert!(matches!(delta_access(&cache, &jobs, 0, 1, |_| async { Err(CacheBuildError::Cancelled) }), CacheAccess::Job(_)));

        jobs.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replay_floor_advance_wins_before_final_admission() {
        let (mut publication, hook) = publication_with_atomic_mutation_hook(2);
        Arc::get_mut(&mut publication).expect("fixture publication is not yet shared").delta_estimate_test_override =
            Some(estimate::SOFT_DELTA_ESTIMATED_BYTES);
        let old_delta = Arc::new(CachedDelta::new(0, 1, false, 4, Bytes::from_static(b"delta")));
        let old_head = cached_head_candidate(publication.publication_id(), 0);

        let update_publication = publication.clone();
        let update = tokio::spawn(async move { update_publication.apply_block_committed(&committed_block(2)).await });
        hook.wait_for_view_update();

        let admission_publication = publication.clone();
        let admission_delta = old_delta.clone();
        let admission = tokio::spawn(async move { admission_publication.admit_cached_delta_replay_floor(admission_delta).await });
        tokio::task::yield_now().await;
        assert!(!admission.is_finished(), "final admission waits behind replay-floor advancement");

        hook.resume_core_update();
        assert_eq!(update.await.expect("update task").expect("replay-barrier update"), Some(1));
        assert!(matches!(admission.await.expect("admission task"), ReplayFloorAdmission::Rejected { replay_floor_revision: 1 }));
        assert!(matches!(
            publication.admit_cached_head_replay_floor(old_head).await,
            ReplayFloorAdmission::Rejected { replay_floor_revision: 1 }
        ));

        let floor_delta = Arc::new(CachedDelta::new(1, 2, false, 4, Bytes::from_static(b"floor-delta")));
        assert!(matches!(publication.admit_cached_delta_replay_floor(floor_delta).await, ReplayFloorAdmission::Admitted(_)));
        let floor_head = cached_head_candidate(publication.publication_id(), 1);
        assert!(matches!(publication.admit_cached_head_replay_floor(floor_head).await, ReplayFloorAdmission::Admitted(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn final_admission_wins_before_later_replay_floor_advance() {
        let mut publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view_with_depth(2), GraphHistory::new())
                .expect("matching candidate");
        publication.delta_estimate_test_override = Some(estimate::SOFT_DELTA_ESTIMATED_BYTES);
        let delta = Arc::new(CachedDelta::new(0, 1, false, 4, Bytes::from_static(b"delta")));
        let admitted_delta = publication.admit_cached_delta_replay_floor(delta.clone()).await;

        let hook = Arc::new(ReplayFloorAdmissionTestHook::new());
        publication.replay_floor_admission_test_hook = Some(hook.clone());
        let publication = Arc::new(publication);
        let head = cached_head_candidate(publication.publication_id(), 0);
        let admission_publication = publication.clone();
        let admission_head = head.clone();
        let admission = tokio::spawn(async move { admission_publication.admit_cached_head_replay_floor(admission_head).await });
        hook.wait_for_core_lock();

        let update_publication = publication.clone();
        let update = tokio::spawn(async move { update_publication.apply_block_committed(&committed_block(2)).await });
        tokio::task::yield_now().await;
        assert!(!update.is_finished(), "floor advancement waits for final admission's core read lock");

        hook.resume_admission();
        let admitted_head = admission.await.expect("admission task");
        assert_eq!(update.await.expect("update task").expect("replay-barrier update"), Some(1));
        assert_eq!(publication.core.read().await.history.replay_floor_revision(), 1);

        match admitted_delta {
            ReplayFloorAdmission::Admitted(candidate) => assert!(Arc::ptr_eq(&candidate, &delta)),
            ReplayFloorAdmission::Rejected { .. } => panic!("delta admission precedes the floor advance"),
        }
        match admitted_head {
            ReplayFloorAdmission::Admitted(candidate) => assert!(Arc::ptr_eq(&candidate, &head)),
            ReplayFloorAdmission::Rejected { .. } => panic!("Head admission precedes the floor advance"),
        }
    }

    #[tokio::test]
    async fn starting_revision_mismatch_refuses_before_applying_the_update() {
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("matching candidate");
        publication
            .core
            .write()
            .await
            .history
            .append(advanced_delta(), 1, GraphHistoryReplayability::Replayable)
            .expect("fixture history drift");

        assert_eq!(
            publication.apply_vspc_committed(&vspc_added(2, None)).await,
            Err(PublicationProjectionError::RevisionMismatch { view_revision: 0, history_revision: 1 })
        );
        let core = publication.core.read().await;
        assert_eq!(core.view.current_revision_id(), 0);
        assert!(core.view.block(&hash(2)).is_none());
    }

    #[tokio::test]
    async fn history_prunes_only_after_a_delta_head_level_leaves_the_head_window() {
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view_with_depth(2), GraphHistory::new())
                .expect("matching candidate");

        for byte in 2..=4 {
            assert_eq!(
                publication.apply_block_committed(&committed_block(byte)).await.expect("block update"),
                Some(u64::from(byte - 1))
            );
        }

        let core = publication.core.read().await;
        assert_eq!(core.history.current_revision_id(), 3);
        assert_eq!(core.history.len(), 2);
        assert_eq!(core.history.oldest_available_revision(), 1);
        assert_eq!(core.history.entries().next().expect("oldest retained entry").delta().head_level(), 3);
    }

    #[tokio::test]
    async fn history_pruning_preserves_a_structurally_eligible_source_keyed_delta() {
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view_with_depth(2), GraphHistory::new())
                .expect("matching candidate");
        assert_eq!(publication.apply_block_committed(&committed_block(2)).await.expect("block update"), Some(1));

        let cache = publication.cache();
        let jobs = CacheJobTracker::new();
        let CacheAccess::Job(initial) = delta_access(&cache, &jobs, 0, 1, |_| async {
            Ok(Arc::new(CachedDelta::new(0, 1, false, 4, Bytes::from_static(b"gzip"))))
        }) else {
            panic!("the first source access starts a job");
        };
        initial.wait(tokio_util::sync::CancellationToken::new()).await.expect("fixture cache entry completes");
        assert!(matches!(
            delta_access(&cache, &jobs, 0, 1, |_| async { panic!("retained source must hit") }),
            CacheAccess::Completed(_)
        ));

        assert_eq!(publication.apply_block_committed(&committed_block(3)).await.expect("block update"), Some(2));
        assert_eq!(publication.apply_block_committed(&committed_block(4)).await.expect("block update"), Some(3));

        match delta_access(&cache, &jobs, 0, 1, |_| async { panic!("source-history pruning does not prune a completed delta") }) {
            CacheAccess::Completed(hit) => assert_eq!(hit.to_revision_id(), 1),
            CacheAccess::Job(_) => panic!("the completed source remains cached until target-level pruning"),
        }
        jobs.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn atomic_core_hides_partial_updates_and_orders_following_writers() {
        let (publication, hook) = publication_with_atomic_mutation_hook(3);
        let first_publication = Arc::clone(&publication);
        let first = tokio::spawn(async move { first_publication.apply_block_committed(&committed_block(2)).await });
        hook.wait_for_view_update();

        let capture_publication = Arc::clone(&publication);
        let capture = tokio::spawn(async move {
            let core = capture_publication.core.read().await;
            (core.view.current_revision_id(), core.history.current_revision_id())
        });
        tokio::task::yield_now().await;
        let second_publication = Arc::clone(&publication);
        let second = tokio::spawn(async move { second_publication.apply_block_committed(&committed_block(3)).await });
        tokio::task::yield_now().await;
        let stale_publication = Arc::clone(&publication);
        let stale = tokio::spawn(async move { stale_publication.transition_stale().await });
        tokio::task::yield_now().await;
        assert!(!capture.is_finished());
        assert!(!second.is_finished());
        assert!(!stale.is_finished());

        first.abort();
        hook.resume_core_update();
        match first.await {
            Ok(result) => assert_eq!(result.expect("first mutation"), Some(1)),
            Err(error) => assert!(error.is_cancelled()),
        }
        let (view_revision, history_revision) = capture.await.expect("core capture task");
        assert_eq!(view_revision, history_revision);
        assert_eq!(second.await.expect("second mutation task").expect("second mutation"), Some(2));
        assert!(stale.await.expect("Stale transition task"));

        let core = publication.core.read().await;
        assert_eq!(core.state, GraphPublicationState::Stale);
        assert_eq!(core.view.current_revision_id(), 2);
        assert_eq!(core.history.current_revision_id(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_before_atomic_mutation_leaves_no_state_or_owned_work() {
        let publication = Arc::new(
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view_with_depth(2), GraphHistory::new())
                .expect("matching candidate"),
        );

        let mutation_guard = publication.mutation.lock().await;
        let waiting_publication = Arc::clone(&publication);
        let waiting = tokio::spawn(async move { waiting_publication.apply_block_committed(&committed_block(2)).await });
        tokio::task::yield_now().await;
        waiting.abort();
        assert!(waiting.await.expect_err("writer-gate waiter is cancelled").is_cancelled());
        drop(mutation_guard);
        {
            let core = publication.core.read().await;
            assert_eq!(core.view.current_revision_id(), 0);
            assert_eq!(core.history.current_revision_id(), 0);
        }

        let core_guard = publication.core.read().await;
        let waiting_publication = Arc::clone(&publication);
        let waiting = tokio::spawn(async move { waiting_publication.apply_block_committed(&committed_block(2)).await });
        tokio::task::yield_now().await;
        assert!(publication.mutation.try_lock().is_err());
        waiting.abort();
        assert!(waiting.await.expect_err("core waiter is cancelled").is_cancelled());
        drop(core_guard);

        let weak = Arc::downgrade(&publication);
        drop(publication);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn lifecycle_transitions_preserve_the_graph_lineage_and_stale_is_idempotent() {
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("matching candidate");
        let initial = publication.capture_delta_target().await;
        let mailbox = Arc::new(SseMailbox::new());
        publication
            .client_registry()
            .register(initial.revision(), &mailbox, tokio_util::sync::CancellationToken::new())
            .expect("registration succeeds");
        for _ in 0..3 {
            mailbox.recv().await.expect("initial registration message");
        }

        publication.transition_live().await;
        assert_eq!(
            mailbox.recv().await,
            Some(SseMessage::PublicationState(PublicationStateDto::new(
                publication.publication_id(),
                initial.revision(),
                GraphPublicationState::Live,
            )))
        );
        let live = publication.capture_delta_target().await;
        assert_eq!(live.state(), GraphPublicationState::Live);
        assert_eq!(live.revision(), initial.revision());
        assert_eq!(live.revision_timestamp_ms(), initial.revision_timestamp_ms());
        assert_eq!(live.head_level(), initial.head_level());
        assert_eq!(publication.core.read().await.history.current_revision_id(), initial.revision());

        assert!(publication.transition_stale().await);
        assert_eq!(
            mailbox.recv().await,
            Some(SseMessage::PublicationState(PublicationStateDto::new(
                publication.publication_id(),
                initial.revision(),
                GraphPublicationState::Stale,
            )))
        );
        assert!(!publication.transition_stale().await);
        let stale = publication.capture_delta_target().await;
        assert_eq!(stale.state(), GraphPublicationState::Stale);
        assert_eq!(stale.revision(), initial.revision());
        assert_eq!(stale.revision_timestamp_ms(), initial.revision_timestamp_ms());
        assert_eq!(stale.head_level(), initial.head_level());
        assert_eq!(publication.core.read().await.history.current_revision_id(), initial.revision());

        let direct_live = GraphPublication::new(GraphPublicationState::Live, Instant::now(), head_view(), GraphHistory::new())
            .expect("matching direct-Live candidate");
        let direct_initial = direct_live.capture_delta_target().await;
        assert!(direct_live.transition_stale().await);
        let direct_stale = direct_live.capture_delta_target().await;
        assert_eq!(direct_initial.state(), GraphPublicationState::Live);
        assert_eq!(direct_stale.state(), GraphPublicationState::Stale);
        assert_eq!(direct_stale.revision(), direct_initial.revision());
        assert_eq!(direct_stale.revision_timestamp_ms(), direct_initial.revision_timestamp_ms());
        assert_eq!(direct_live.core.read().await.history.current_revision_id(), direct_initial.revision());
    }

    #[tokio::test]
    #[should_panic(expected = "only a Synchronizing publication becomes Live")]
    async fn rejects_a_live_transition_from_direct_initial_live() {
        let publication = GraphPublication::new(GraphPublicationState::Live, Instant::now(), head_view(), GraphHistory::new())
            .expect("matching candidate");

        publication.transition_live().await;
    }

    #[tokio::test]
    async fn captures_are_detached_coherent_and_survive_later_pruning() {
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view_with_depth(2), GraphHistory::new())
                .expect("matching candidate");
        publication.apply_block_committed(&committed_block(2)).await.expect("first block update");

        let full = publication.capture_view().await;
        assert_eq!(full.publication_id(), publication.publication_id());
        assert_eq!(full.replay_floor_revision(), 0);
        assert_eq!(full.oldest_available_revision(), 0);
        assert_eq!(full.view().tracking_policy(), TrackingPolicy::Frozen);
        assert_eq!(full.view().current_revision_id(), 1);
        assert!(full.view().block(&hash(2)).is_some());
        let subview = publication.capture_subview(LevelExtent::new(1, 1).expect("valid extent")).await.expect("contained extent");
        assert_eq!(subview.replay_floor_revision(), full.replay_floor_revision());
        assert_eq!(subview.oldest_available_revision(), full.oldest_available_revision());
        assert_eq!(subview.view().extent(), LevelExtent::new(1, 1).expect("valid extent"));
        assert_eq!(
            publication.capture_subview(LevelExtent::new(2, 3).expect("valid extent")).await,
            Err(GraphViewExtractError::OutsideSourceExtent)
        );

        publication.apply_block_committed(&committed_block(3)).await.expect("second block update");
        let target = publication.capture_delta_target().await;
        assert_eq!(target.publication_id(), publication.publication_id());
        assert_eq!(target.replay_floor_revision(), 0);
        let retained = publication.capture_history_range(0, target).await.expect("retained history");
        let GraphHistoryRange::Deltas { actual_target, entries, .. } = retained else {
            panic!("two updates produce retained deltas");
        };
        assert_eq!(actual_target, 2);
        assert_eq!(entries.len(), 2);

        publication.apply_block_committed(&committed_block(4)).await.expect("third block update");
        assert_eq!(full.view().current_revision_id(), 1);
        assert!(full.view().block(&hash(3)).is_none());
        assert_eq!(target.revision(), 2);
        assert_eq!(entries[0].delta().from_revision_id(), 0);
        assert_eq!(entries[1].delta().to_revision_id(), 2);

        let current = publication.capture_delta_target().await;
        assert_eq!(
            publication.capture_history_range(0, current).await,
            Err(GraphHistoryRangeError::StartPruned { requested: 0, oldest_available: 1 })
        );
        assert_eq!(full.into_view().tracking_policy(), TrackingPolicy::Frozen);
    }

    #[tokio::test]
    async fn view_projection_failure_remains_typed_and_leaves_the_core_unchanged() {
        let frozen_publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("matching candidate");
        {
            let mut core = frozen_publication.core.write().await;
            core.view = core.view.extract_subview(core.view.extent()).expect("own extent");
        }
        assert_eq!(
            frozen_publication.apply_vspc_committed(&vspc_added(2, None)).await,
            Err(PublicationProjectionError::ViewUpdate(GraphViewUpdateError::Frozen))
        );
        let core = frozen_publication.core.read().await;
        assert_eq!(core.view.current_revision_id(), 0);
        assert_eq!(core.history.current_revision_id(), 0);
    }

    #[test]
    fn independently_constructed_publications_receive_fresh_identities() {
        let first = GraphPublication::new(GraphPublicationState::Live, Instant::now(), head_view(), GraphHistory::new())
            .expect("first publication");
        let second = GraphPublication::new(GraphPublicationState::Live, Instant::now(), head_view(), GraphHistory::new())
            .expect("second publication");

        assert_ne!(first.publication_id(), second.publication_id());
    }

    #[test]
    fn publication_is_the_durable_owner_of_its_response_cache() {
        let publication =
            GraphPublication::new(GraphPublicationState::Live, Instant::now(), head_view(), GraphHistory::new()).expect("publication");
        let cache = Arc::downgrade(&publication.cache);
        assert_eq!(Arc::strong_count(&publication.cache), 1);

        let temporary = publication.cache();
        assert_eq!(Arc::strong_count(&publication.cache), 2);
        drop(temporary);
        assert_eq!(Arc::strong_count(&publication.cache), 1);

        drop(publication);
        assert!(cache.upgrade().is_none());
    }

    #[test]
    fn rejects_a_mismatched_construction_candidate() {
        let mut view = head_view();
        view.apply_delta(&advanced_delta()).expect("advancing delta");

        let result = GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), view, GraphHistory::new());

        assert!(matches!(result, Err(PublicationProjectionError::RevisionMismatch { view_revision: 1, history_revision: 0 })));
    }

    #[test]
    #[should_panic(expected = "a fresh publication is Synchronizing or Live")]
    fn rejects_stale_as_an_initial_state() {
        let _ = GraphPublication::new(GraphPublicationState::Stale, Instant::now(), head_view(), GraphHistory::new());
    }

    #[test]
    fn publication_clock_uses_whole_milliseconds_from_the_candidate_origin() {
        let origin = Instant::now();
        let publication = GraphPublication::new(GraphPublicationState::Synchronizing, origin, head_view(), GraphHistory::new())
            .expect("matching candidate");

        assert_eq!(publication.revision_timestamp_ms_at(origin), 0);
        assert_eq!(publication.revision_timestamp_ms_at(origin + Duration::from_micros(999)), 0);
        assert_eq!(publication.revision_timestamp_ms_at(origin + Duration::from_millis(1)), 1);
        assert_eq!(publication.revision_timestamp_ms_at(origin + Duration::from_millis(1_234) + Duration::from_micros(999)), 1_234);
        assert!(publication.revision_timestamp_ms() >= publication.revision_timestamp_ms_at(origin));
    }

    #[test]
    fn publication_identity_retries_the_reserved_zero_value() {
        let mut candidates = [0, 0, 47].into_iter();
        assert_eq!(sample_nonzero_publication_id(|| candidates.next().expect("fixture candidate")), 47);
    }
}
