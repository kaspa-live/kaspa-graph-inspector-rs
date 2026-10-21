#![allow(dead_code, reason = "the isolated publication kernel is integrated by later API increments")]

mod error;
mod estimate;
pub(crate) mod runtime;

use std::{sync::Arc, time::Instant};

use kgi_api_model::{
    delta::GraphDelta,
    graph::{GraphView, GraphViewExtractError, LevelExtent, TrackingPolicy},
    history::{GraphHistory, GraphHistoryRange, GraphHistoryRangeError},
};
use kgi_model::graph_update::{BlockCommitted, VspcCommitted};
use rand::{RngCore, rngs::OsRng};
use tokio::sync::{Mutex, RwLock};

use crate::{cache::GraphCache, client::SseClientRegistry};

pub(crate) use self::error::PublicationProjectionError;
use self::estimate::estimate_raw_delta_bytes;

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

/// Atomically observed lifecycle state, canonical Head view, and history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphPublicationCore {
    state: GraphPublicationState,
    view: GraphView,
    history: GraphHistory,
}

/// One detached immutable graph view from a specific publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphViewCapture {
    publication_id: u64,
    view: GraphView,
}

impl GraphViewCapture {
    /// Returns the publication that supplied this view.
    pub(crate) const fn publication_id(&self) -> u64 {
        self.publication_id
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

/// One coherent target sampled before selecting retained delta history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GraphDeltaTargetCapture {
    publication_id: u64,
    state: GraphPublicationState,
    revision: u64,
    revision_timestamp_ms: u64,
    head_level: u32,
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
}

#[cfg(test)]
struct AtomicMutationTestHook {
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

    fn wait_for_view_update(&self) {
        self.view_updated.wait();
    }

    fn resume_core_update(&self) {
        self.finish_core_update.wait();
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
        GraphViewCapture { publication_id: self.publication_id, view }
    }

    /// Captures one contained extent from the current Head as an owned frozen view.
    pub(crate) async fn capture_subview(&self, extent: LevelExtent) -> Result<GraphViewCapture, GraphViewExtractError> {
        let core = self.core.read().await;
        let view = core.view.extract_subview(extent)?;
        Ok(GraphViewCapture { publication_id: self.publication_id, view })
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
        let view_revision = core.view.current_revision_id();
        let history_revision = core.history.current_revision_id();
        if view_revision != history_revision {
            return Err(PublicationProjectionError::RevisionMismatch { view_revision, history_revision });
        }

        assert_ne!(core.state, GraphPublicationState::Stale, "a Stale publication is not mutated");
        let delta = update.apply(&mut core.view, self.revision_timestamp_ms())?;
        let Some(delta) = delta else {
            return Ok(None);
        };
        #[cfg(test)]
        if let Some(hook) = &self.atomic_mutation_test_hook {
            hook.pause_after_view_update();
        }
        let target_revision = delta.to_revision_id();
        let head_low_level = core.view.low_level();
        let estimated_raw_bytes = estimate_raw_delta_bytes(&delta);
        core.history.append(delta, estimated_raw_bytes).expect("a successful Head update extends the locked history");
        core.history.prune(head_low_level);
        let head_high_level = core.view.high_level();
        drop(core);
        self.clients.advance_head(target_revision);
        drop(mutation);
        self.cache.prune_deltas(head_high_level);
        Ok(Some(target_revision))
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
        sync::Arc,
        time::Duration,
    };

    use bytes::Bytes;
    use kgi_api_model::{
        delta::GraphDelta,
        graph::{GraphViewUpdateError, Level, LevelExtent},
        history::{GraphHistoryRange, GraphHistoryRangeError},
    };
    use kgi_model::{
        block::{BlockColor, BlockCoordinate, BlockHash, CompactId},
        graph_update::{BlockCommitted, LevelCommitted, ParentCommitted, VspcCommitted},
    };

    use super::*;
    use crate::{
        cache::{CacheAccess, CacheJobTracker, CachedDelta},
        client::{DeltaCompletionResult, DeltaScheduleAction, SseMailbox, SseMessage},
        codec::sse::{PublicationStateDto, PublicationWakeupDto},
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
    async fn accepts_an_already_advanced_matching_candidate() {
        let delta = advanced_delta();
        let mut view = head_view();
        view.apply_delta(&delta).expect("matching Head delta");
        let mut history = GraphHistory::new();
        history.append(delta, 1).expect("matching history delta");

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
    async fn starting_revision_mismatch_refuses_before_applying_the_update() {
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("matching candidate");
        publication.core.write().await.history.append(advanced_delta(), 1).expect("fixture history drift");

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
        let CacheAccess::Job(initial) = cache.delta(
            &jobs,
            0,
            |_| async { Ok(Arc::new(CachedDelta::new(0, 1, 2, 4, Bytes::from_static(b"gzip")))) },
            || async { 2 },
        ) else {
            panic!("the first source access starts a job");
        };
        initial.wait(tokio_util::sync::CancellationToken::new()).await.expect("fixture cache entry completes");
        assert!(matches!(
            cache.delta(&jobs, 0, |_| async { panic!("retained source must hit") }, || async { 2 }),
            CacheAccess::Completed(_)
        ));

        assert_eq!(publication.apply_block_committed(&committed_block(3)).await.expect("block update"), Some(2));
        assert_eq!(publication.apply_block_committed(&committed_block(4)).await.expect("block update"), Some(3));

        match cache.delta(&jobs, 0, |_| async { panic!("source-history pruning does not prune a completed delta") }, || async { 4 }) {
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
        assert_eq!(full.view().tracking_policy(), TrackingPolicy::Frozen);
        assert_eq!(full.view().current_revision_id(), 1);
        assert!(full.view().block(&hash(2)).is_some());
        let subview = publication.capture_subview(LevelExtent::new(1, 1).expect("valid extent")).await.expect("contained extent");
        assert_eq!(subview.view().extent(), LevelExtent::new(1, 1).expect("valid extent"));
        assert_eq!(
            publication.capture_subview(LevelExtent::new(2, 3).expect("valid extent")).await,
            Err(GraphViewExtractError::OutsideSourceExtent)
        );

        publication.apply_block_committed(&committed_block(3)).await.expect("second block update");
        let target = publication.capture_delta_target().await;
        assert_eq!(target.publication_id(), publication.publication_id());
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
