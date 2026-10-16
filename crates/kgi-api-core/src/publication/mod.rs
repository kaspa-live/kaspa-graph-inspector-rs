#![allow(dead_code, reason = "the isolated publication kernel is integrated by later API increments")]

mod error;
mod estimate;

use std::time::Instant;

use kgi_api_model::{
    delta::GraphDelta,
    graph::{GraphView, GraphViewExtractError, LevelExtent, TrackingPolicy},
    history::{GraphHistory, GraphHistoryRange, GraphHistoryRangeError},
};
use kgi_model::graph_update::{BlockCommitted, VspcCommitted};
use rand::{RngCore, rngs::OsRng};
use tokio::sync::{Mutex, RwLock};

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

/// Atomically observed lifecycle state and complete canonical Head view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphPublicationImage {
    state: GraphPublicationState,
    view: GraphView,
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
    high_level: u64,
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

    /// Returns the captured Head's nominal upper level.
    pub(crate) const fn high_level(self) -> u64 {
        self.high_level
    }
}

/// One publication-local graph lineage and its synchronization boundary.
pub(crate) struct GraphPublication {
    publication_id: u64,
    revision_clock_origin: Instant,
    mutation: Mutex<()>,
    image: RwLock<GraphPublicationImage>,
    history: RwLock<GraphHistory>,
    #[cfg(test)]
    mutation_test_hook: Option<std::sync::Arc<MutationTestHook>>,
}

#[cfg(test)]
struct MutationTestHook {
    image_published: tokio::sync::Barrier,
    resume_history_append: tokio::sync::Barrier,
}

#[cfg(test)]
impl MutationTestHook {
    fn new() -> Self {
        Self { image_published: tokio::sync::Barrier::new(2), resume_history_append: tokio::sync::Barrier::new(2) }
    }

    async fn pause_after_image_publication(&self) {
        self.image_published.wait().await;
        self.resume_history_append.wait().await;
    }

    async fn wait_for_image_publication(&self) {
        self.image_published.wait().await;
    }

    async fn resume_history_append(&self) {
        self.resume_history_append.wait().await;
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

        let image_revision = view.current_revision_id();
        let history_revision = history.current_revision_id();
        if image_revision != history_revision {
            return Err(PublicationProjectionError::RevisionMismatch { image_revision, history_revision });
        }
        assert!(
            view.current_revision_timestamp_ms() <= revision_timestamp_ms_at(revision_clock_origin, Instant::now()),
            "candidate revision timestamp belongs to the publication clock"
        );

        Ok(Self {
            publication_id: random_nonzero_publication_id(),
            revision_clock_origin,
            mutation: Mutex::new(()),
            image: RwLock::new(GraphPublicationImage { state: initial_state, view }),
            history: RwLock::new(history),
            #[cfg(test)]
            mutation_test_hook: None,
        })
    }

    /// Returns this publication's immutable random identity.
    pub(crate) const fn publication_id(&self) -> u64 {
        self.publication_id
    }

    /// Samples whole milliseconds elapsed in this publication's clock domain.
    pub(crate) fn revision_timestamp_ms(&self) -> u64 {
        self.revision_timestamp_ms_at(Instant::now())
    }

    /// Transitions a visible Synchronizing publication to Live.
    pub(crate) async fn transition_live(&self) {
        let _mutation = self.mutation.lock().await;
        let mut image = self.image.write().await;
        assert_eq!(image.state, GraphPublicationState::Synchronizing, "only a Synchronizing publication becomes Live");
        image.state = GraphPublicationState::Live;
    }

    /// Makes a visible publication terminally Stale.
    ///
    /// Returns whether this call performed the transition. Repeating the
    /// terminal transition has no additional effect.
    pub(crate) async fn transition_stale(&self) -> bool {
        let _mutation = self.mutation.lock().await;
        let mut image = self.image.write().await;
        if image.state == GraphPublicationState::Stale {
            return false;
        }
        image.state = GraphPublicationState::Stale;
        true
    }

    /// Captures the complete current Head as an owned frozen view.
    pub(crate) async fn capture_view(&self) -> GraphViewCapture {
        let image = self.image.read().await;
        let view = image.view.extract_subview(image.view.extent()).expect("a graph view contains its own extent");
        GraphViewCapture { publication_id: self.publication_id, view }
    }

    /// Captures one contained extent from the current Head as an owned frozen view.
    pub(crate) async fn capture_subview(&self, extent: LevelExtent) -> Result<GraphViewCapture, GraphViewExtractError> {
        let image = self.image.read().await;
        let view = image.view.extract_subview(extent)?;
        Ok(GraphViewCapture { publication_id: self.publication_id, view })
    }

    /// Captures one coherent canonical-delta target from the current image.
    pub(crate) async fn capture_delta_target(&self) -> GraphDeltaTargetCapture {
        let image = self.image.read().await;
        GraphDeltaTargetCapture {
            publication_id: self.publication_id,
            state: image.state,
            revision: image.view.current_revision_id(),
            revision_timestamp_ms: image.view.current_revision_timestamp_ms(),
            high_level: image.view.high_level(),
        }
    }

    /// Captures immutable retained history through a previously sampled target.
    pub(crate) async fn capture_history_range(
        &self,
        from_revision_id: u64,
        target: GraphDeltaTargetCapture,
    ) -> Result<GraphHistoryRange, GraphHistoryRangeError> {
        assert_eq!(target.publication_id, self.publication_id, "delta target belongs to this publication");
        self.history.read().await.range(from_revision_id, target.revision)
    }

    /// Applies one committed block and publishes any resulting history entry.
    ///
    /// Returns the newly published target revision, or `None` when the update
    /// has no retained effect and advances neither the image nor its history.
    pub(crate) async fn apply_block_committed(&self, committed: &BlockCommitted) -> Result<Option<u64>, PublicationProjectionError> {
        self.apply_original_update(OriginalUpdate::Block(committed)).await
    }

    /// Applies one committed VSPC transition and publishes any resulting history entry.
    ///
    /// Returns the newly published target revision, or `None` when the update
    /// has no retained effect and advances neither the image nor its history.
    pub(crate) async fn apply_vspc_committed(&self, committed: &VspcCommitted) -> Result<Option<u64>, PublicationProjectionError> {
        self.apply_original_update(OriginalUpdate::Vspc(committed)).await
    }

    async fn apply_original_update(&self, update: OriginalUpdate<'_>) -> Result<Option<u64>, PublicationProjectionError> {
        let _mutation = self.mutation.lock().await;

        let image_revision = self.image.read().await.view.current_revision_id();
        let history_revision = self.history.read().await.current_revision_id();
        if image_revision != history_revision {
            return Err(PublicationProjectionError::RevisionMismatch { image_revision, history_revision });
        }

        let mut image = self.image.write().await;
        assert_ne!(image.state, GraphPublicationState::Stale, "a Stale publication is not mutated");
        let delta = update.apply(&mut image.view, self.revision_timestamp_ms())?;
        let Some(delta) = delta else {
            return Ok(None);
        };
        let target_revision = delta.to_revision_id();
        let head_low_level = image.view.low_level();
        drop(image);

        #[cfg(test)]
        if let Some(hook) = &self.mutation_test_hook {
            hook.pause_after_image_publication().await;
        }

        let estimated_raw_bytes = estimate_raw_delta_bytes(&delta);
        let mut history = self.history.write().await;
        history.append(delta, estimated_raw_bytes)?;
        history.prune(head_low_level);
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

    use kgi_api_model::{
        delta::GraphDelta,
        graph::{GraphViewUpdateError, Level, LevelExtent},
        history::{GraphHistoryAppendError, GraphHistoryRange, GraphHistoryRangeError},
    };
    use kgi_model::{
        block::{BlockColor, BlockCoordinate, BlockHash, CompactId},
        graph_update::{BlockCommitted, LevelCommitted, ParentCommitted, VspcCommitted},
    };

    use super::*;

    fn head_view() -> GraphView {
        head_view_with_depth(1)
    }

    fn head_view_with_depth(max_depth: u64) -> GraphView {
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
        let level = u64::from(byte);
        let parent_level = level - 1;
        BlockCommitted {
            id: CompactId::new(i64::from(byte)).expect("positive compact ID"),
            hash: hash(byte),
            coordinate: BlockCoordinate::new(level, 0).expect("positive coordinate"),
            timestamp: level * 10,
            daa_score: level * 100,
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
            level_snapshots: Arc::from([LevelCommitted { level: u64::from(byte), size: 1, daa_score }]),
        }
    }

    fn advanced_delta() -> GraphDelta {
        GraphDelta::new(0, 1, 123, 1, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new())
            .expect("valid advancing delta")
    }

    fn publication_with_mutation_hook(max_depth: u64) -> (Arc<GraphPublication>, Arc<MutationTestHook>) {
        let hook = Arc::new(MutationTestHook::new());
        let mut publication = GraphPublication::new(
            GraphPublicationState::Synchronizing,
            Instant::now(),
            head_view_with_depth(max_depth),
            GraphHistory::new(),
        )
        .expect("matching candidate");
        publication.mutation_test_hook = Some(Arc::clone(&hook));
        (Arc::new(publication), hook)
    }

    #[tokio::test]
    async fn constructs_synchronizing_and_direct_live_publications() {
        for state in [GraphPublicationState::Synchronizing, GraphPublicationState::Live] {
            let publication =
                GraphPublication::new(state, Instant::now(), head_view(), GraphHistory::new()).expect("matching candidate");

            assert_ne!(publication.publication_id(), 0);
            let image = publication.image.read().await;
            assert_eq!(image.state, state);
            assert_eq!(image.view.current_revision_id(), 0);
            assert_eq!(image.view.current_revision_timestamp_ms(), 0);
            drop(image);
            assert_eq!(publication.history.read().await.current_revision_id(), 0);
            assert!(publication.mutation.try_lock().is_ok());
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

        assert_eq!(publication.image.read().await.view.current_revision_id(), 1);
        assert_eq!(publication.history.read().await.current_revision_id(), 1);
    }

    #[tokio::test]
    async fn original_updates_advance_image_and_history_and_noops_advance_neither() {
        let origin = Instant::now().checked_sub(Duration::from_millis(10)).expect("valid fixture origin");
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, origin, head_view_with_depth(2), GraphHistory::new())
                .expect("matching candidate");

        assert_eq!(publication.apply_block_committed(&committed_block(2)).await.expect("block update"), Some(1));
        {
            let image = publication.image.read().await;
            assert_eq!(image.view.current_revision_id(), 1);
            assert!(image.view.current_revision_timestamp_ms() >= 10);
            assert_eq!(image.view.block(&hash(2)).map(|block| block.is_in_vspc), Some(false));
        }
        {
            let history = publication.history.read().await;
            assert_eq!(history.current_revision_id(), 1);
            assert_eq!(history.len(), 1);
            assert_eq!(history.entries().next().expect("block entry").estimated_raw_bytes(), 846);
        }

        let vspc = vspc_added(2, Some(200));
        assert_eq!(publication.apply_vspc_committed(&vspc).await.expect("VSPC update"), Some(2));
        assert_eq!(publication.image.read().await.view.block(&hash(2)).map(|block| block.is_in_vspc), Some(true));
        assert_eq!(publication.history.read().await.current_revision_id(), 2);

        assert_eq!(publication.apply_vspc_committed(&vspc).await.expect("VSPC no-op"), None);
        assert_eq!(publication.image.read().await.view.current_revision_id(), 2);
        assert_eq!(publication.history.read().await.len(), 2);
    }

    #[tokio::test]
    async fn starting_revision_mismatch_refuses_before_applying_the_update() {
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("matching candidate");
        publication.history.write().await.append(advanced_delta(), 1).expect("fixture history drift");

        assert_eq!(
            publication.apply_vspc_committed(&vspc_added(2, None)).await,
            Err(PublicationProjectionError::RevisionMismatch { image_revision: 0, history_revision: 1 })
        );
        assert_eq!(publication.image.read().await.view.current_revision_id(), 0);
        assert!(publication.image.read().await.view.block(&hash(2)).is_none());
    }

    #[tokio::test]
    async fn history_prunes_only_after_a_delta_high_level_leaves_the_head_window() {
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view_with_depth(2), GraphHistory::new())
                .expect("matching candidate");

        for byte in 2..=4 {
            assert_eq!(
                publication.apply_block_committed(&committed_block(byte)).await.expect("block update"),
                Some(u64::from(byte - 1))
            );
        }

        let history = publication.history.read().await;
        assert_eq!(history.current_revision_id(), 3);
        assert_eq!(history.len(), 2);
        assert_eq!(history.oldest_available_revision(), 1);
        assert_eq!(history.entries().next().expect("oldest retained entry").delta().high_level(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn writer_gate_orders_mutations_and_stale_after_the_image_history_interval() {
        let (publication, hook) = publication_with_mutation_hook(3);

        let first_publication = Arc::clone(&publication);
        let first = tokio::spawn(async move { first_publication.apply_block_committed(&committed_block(2)).await });
        hook.wait_for_image_publication().await;

        assert_eq!(publication.image.read().await.view.current_revision_id(), 1);
        assert_eq!(publication.history.read().await.current_revision_id(), 0);
        let first_target = publication.capture_delta_target().await;
        assert_eq!(first_target.revision(), 1);
        assert_eq!(
            publication.capture_history_range(0, first_target).await.expect("complete available history"),
            GraphHistoryRange::UpToDate { revision: 0 }
        );

        let second_publication = Arc::clone(&publication);
        let second = tokio::spawn(async move { second_publication.apply_block_committed(&committed_block(3)).await });
        tokio::task::yield_now().await;
        let stale_publication = Arc::clone(&publication);
        let stale = tokio::spawn(async move { stale_publication.transition_stale().await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        assert!(!stale.is_finished());
        assert_eq!(publication.capture_delta_target().await.state(), GraphPublicationState::Synchronizing);

        hook.resume_history_append().await;
        assert_eq!(first.await.expect("first mutation task").expect("first mutation"), Some(1));
        hook.wait_for_image_publication().await;
        assert_eq!(publication.image.read().await.view.current_revision_id(), 2);
        assert_eq!(publication.history.read().await.current_revision_id(), 1);
        assert!(!stale.is_finished());

        hook.resume_history_append().await;
        assert_eq!(second.await.expect("second mutation task").expect("second mutation"), Some(2));
        assert!(stale.await.expect("Stale transition task"));
        let final_target = publication.capture_delta_target().await;
        assert_eq!(final_target.state(), GraphPublicationState::Stale);
        assert_eq!(final_target.revision(), 2);
        assert_eq!(publication.history.read().await.current_revision_id(), 2);
    }

    #[tokio::test]
    async fn lifecycle_transitions_preserve_the_graph_lineage_and_stale_is_idempotent() {
        let publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("matching candidate");
        let initial = publication.capture_delta_target().await;

        publication.transition_live().await;
        let live = publication.capture_delta_target().await;
        assert_eq!(live.state(), GraphPublicationState::Live);
        assert_eq!(live.revision(), initial.revision());
        assert_eq!(live.revision_timestamp_ms(), initial.revision_timestamp_ms());
        assert_eq!(live.high_level(), initial.high_level());
        assert_eq!(publication.history.read().await.current_revision_id(), initial.revision());

        assert!(publication.transition_stale().await);
        assert!(!publication.transition_stale().await);
        let stale = publication.capture_delta_target().await;
        assert_eq!(stale.state(), GraphPublicationState::Stale);
        assert_eq!(stale.revision(), initial.revision());
        assert_eq!(stale.revision_timestamp_ms(), initial.revision_timestamp_ms());
        assert_eq!(stale.high_level(), initial.high_level());
        assert_eq!(publication.history.read().await.current_revision_id(), initial.revision());

        let direct_live = GraphPublication::new(GraphPublicationState::Live, Instant::now(), head_view(), GraphHistory::new())
            .expect("matching direct-Live candidate");
        let direct_initial = direct_live.capture_delta_target().await;
        assert!(direct_live.transition_stale().await);
        let direct_stale = direct_live.capture_delta_target().await;
        assert_eq!(direct_initial.state(), GraphPublicationState::Live);
        assert_eq!(direct_stale.state(), GraphPublicationState::Stale);
        assert_eq!(direct_stale.revision(), direct_initial.revision());
        assert_eq!(direct_stale.revision_timestamp_ms(), direct_initial.revision_timestamp_ms());
        assert_eq!(direct_live.history.read().await.current_revision_id(), direct_initial.revision());
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
    async fn projection_failures_remain_typed_and_do_not_partially_apply_the_failing_phase() {
        let frozen_publication =
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), head_view(), GraphHistory::new())
                .expect("matching candidate");
        {
            let mut image = frozen_publication.image.write().await;
            image.view = image.view.extract_subview(image.view.extent()).expect("own extent");
        }
        assert_eq!(
            frozen_publication.apply_vspc_committed(&vspc_added(2, None)).await,
            Err(PublicationProjectionError::ViewUpdate(GraphViewUpdateError::Frozen))
        );
        assert_eq!(frozen_publication.image.read().await.view.current_revision_id(), 0);
        assert_eq!(frozen_publication.history.read().await.current_revision_id(), 0);

        let (publication, hook) = publication_with_mutation_hook(2);
        let mutation_publication = Arc::clone(&publication);
        let mutation = tokio::spawn(async move { mutation_publication.apply_block_committed(&committed_block(2)).await });
        hook.wait_for_image_publication().await;
        publication.history.write().await.append(advanced_delta(), 1).expect("fixture history drift");
        hook.resume_history_append().await;

        assert_eq!(
            mutation.await.expect("mutation task"),
            Err(PublicationProjectionError::HistoryAppend(GraphHistoryAppendError::RevisionMismatch {
                history_revision: 1,
                delta_from: 0,
            }))
        );
        assert_eq!(publication.image.read().await.view.current_revision_id(), 1);
        assert_eq!(publication.history.read().await.current_revision_id(), 1);
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
    fn rejects_a_mismatched_construction_candidate() {
        let mut view = head_view();
        view.apply_delta(&advanced_delta()).expect("advancing delta");

        let result = GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), view, GraphHistory::new());

        assert!(matches!(result, Err(PublicationProjectionError::RevisionMismatch { image_revision: 1, history_revision: 0 })));
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
