#![allow(dead_code, reason = "the isolated publication kernel is integrated by later API increments")]

mod error;

use std::time::Instant;

use kgi_api_model::{
    graph::{GraphView, TrackingPolicy},
    history::GraphHistory,
};
use rand::{RngCore, rngs::OsRng};
use tokio::sync::{Mutex, RwLock};

pub(crate) use self::error::PublicationProjectionError;

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

/// One publication-local graph lineage and its synchronization boundary.
pub(crate) struct GraphPublication {
    publication_id: u64,
    revision_clock_origin: Instant,
    mutation: Mutex<()>,
    image: RwLock<GraphPublicationImage>,
    history: RwLock<GraphHistory>,
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

    fn revision_timestamp_ms_at(&self, now: Instant) -> u64 {
        revision_timestamp_ms_at(self.revision_clock_origin, now)
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
        time::Duration,
    };

    use kgi_api_model::{
        delta::GraphDelta,
        graph::{Level, LevelExtent},
    };

    use super::*;

    fn head_view() -> GraphView {
        GraphView::from_database_projection(
            1,
            LevelExtent::new(1, 1).expect("valid extent"),
            TrackingPolicy::Head,
            BTreeMap::from([(1, Level { size: 1, daa_score: None })]),
            HashMap::new(),
            HashMap::new(),
        )
    }

    fn advanced_delta() -> GraphDelta {
        GraphDelta::new(0, 1, 123, 1, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new())
            .expect("valid advancing delta")
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
