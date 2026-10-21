#![allow(dead_code, reason = "the API database binding is consumed by later ApiService increments")]

use std::sync::Arc;

use kgi_storage::generation::ValidatedApiDbClient;
use tokio::sync::watch;

/// Generation behavior observed by the API database binding.
pub(crate) trait ApiDbGeneration: Send + Sync + 'static {
    /// Reports whether StorageService still considers this exact generation usable.
    fn is_valid(&self) -> bool;
}

impl ApiDbGeneration for ValidatedApiDbClient {
    fn is_valid(&self) -> bool {
        ValidatedApiDbClient::is_valid(self)
    }
}

/// One ordered API database-generation transition forwarded to ApiService.
#[derive(Clone, Debug)]
pub(crate) enum ApiDbGenerationEvent<G = ValidatedApiDbClient> {
    /// Removes the exact generation when it remains current.
    Retired(Arc<G>),
    /// Makes one validated generation current.
    Published(Arc<G>),
}

/// Complete latest-value API database binding observed by consumers.
#[derive(Clone, Debug)]
pub(crate) enum ApiDbSnapshot<G = ValidatedApiDbClient> {
    /// The binding remains available for generation updates and reads.
    Running {
        /// Latest exact generation forwarded by Supervisor, when present.
        current: Option<Arc<G>>,
        /// Whether public database-backed request admission is open.
        public_reads_enabled: bool,
    },
    /// The binding is terminal and contains no database generation.
    Stopped,
}

/// Rejection of one API database-generation update.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ApiDbUpdateError {
    /// The supplied publication is already terminally invalid.
    #[error("cannot publish an invalid API database generation")]
    InvalidGeneration,
    /// The binding has crossed its terminal update cutoff.
    #[error("API database binding is stopped")]
    Stopped,
}

/// Watch-backed latest-value API database binding.
pub(crate) struct ApiDbState<G = ValidatedApiDbClient> {
    tx: watch::Sender<ApiDbSnapshot<G>>,
}

impl<G: ApiDbGeneration> ApiDbState<G> {
    /// Creates a running binding with no current generation and public reads enabled.
    pub(crate) fn new() -> Self {
        let (tx, _) = watch::channel(ApiDbSnapshot::Running { current: None, public_reads_enabled: true });
        Self { tx }
    }

    /// Applies one ordered exact-generation event and publishes its complete snapshot.
    pub(crate) fn update(&self, event: ApiDbGenerationEvent<G>) -> Result<(), ApiDbUpdateError> {
        let mut result = Ok(());
        self.tx.send_if_modified(|snapshot| match snapshot {
            ApiDbSnapshot::Stopped => {
                result = Err(ApiDbUpdateError::Stopped);
                false
            }
            ApiDbSnapshot::Running { current, .. } => match &event {
                ApiDbGenerationEvent::Retired(retired) => {
                    let exact = current.as_ref().is_some_and(|bound| Arc::ptr_eq(bound, retired));
                    if exact {
                        *current = None;
                    }
                    exact
                }
                ApiDbGenerationEvent::Published(published) => {
                    if current.as_ref().is_some_and(|bound| Arc::ptr_eq(bound, published)) {
                        return false;
                    }
                    if !published.is_valid() {
                        result = Err(ApiDbUpdateError::InvalidGeneration);
                        return false;
                    }
                    *current = Some(published.clone());
                    true
                }
            },
        });
        result
    }

    /// Returns the current valid generation when public database reads are enabled.
    pub(crate) fn public_read_client(&self) -> Option<Arc<G>> {
        match &*self.tx.borrow() {
            ApiDbSnapshot::Running { current: Some(current), public_reads_enabled: true } if current.is_valid() => {
                Some(current.clone())
            }
            ApiDbSnapshot::Running { .. } | ApiDbSnapshot::Stopped => None,
        }
    }

    /// Waits for and returns the next valid current generation, or `None` after stop.
    pub(crate) async fn current(&self) -> Option<Arc<G>> {
        let mut snapshots = self.tx.subscribe();
        loop {
            {
                let snapshot = snapshots.borrow_and_update();
                match &*snapshot {
                    ApiDbSnapshot::Running { current: Some(current), .. } if current.is_valid() => {
                        return Some(current.clone());
                    }
                    ApiDbSnapshot::Running { .. } => {}
                    ApiDbSnapshot::Stopped => return None,
                }
            }
            snapshots.changed().await.expect("ApiDbState retains its sender while current() is waiting");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::{ApiDbGeneration, ApiDbGenerationEvent, ApiDbSnapshot, ApiDbState, ApiDbUpdateError};

    struct TestGeneration {
        valid: AtomicBool,
    }

    impl TestGeneration {
        fn valid() -> Arc<Self> {
            Arc::new(Self { valid: AtomicBool::new(true) })
        }

        fn invalid() -> Arc<Self> {
            Arc::new(Self { valid: AtomicBool::new(false) })
        }

        fn retire(&self) {
            self.valid.store(false, Ordering::Release);
        }
    }

    impl ApiDbGeneration for TestGeneration {
        fn is_valid(&self) -> bool {
            self.valid.load(Ordering::Acquire)
        }
    }

    fn current(state: &ApiDbState<TestGeneration>) -> Option<Arc<TestGeneration>> {
        match &*state.tx.borrow() {
            ApiDbSnapshot::Running { current, .. } => current.clone(),
            ApiDbSnapshot::Stopped => None,
        }
    }

    #[test]
    fn binding_starts_running_without_a_generation_and_with_public_reads_enabled() {
        let state = ApiDbState::<TestGeneration>::new();
        let snapshot = state.tx.borrow();

        assert!(matches!(&*snapshot, ApiDbSnapshot::Running { current: None, public_reads_enabled: true }));
    }

    #[test]
    fn publication_and_retirement_follow_exact_arc_identity() {
        let state = ApiDbState::<TestGeneration>::new();
        let first = TestGeneration::valid();
        let replacement = TestGeneration::valid();

        state.update(ApiDbGenerationEvent::Published(first.clone())).expect("first generation is accepted");
        assert!(current(&state).is_some_and(|current| Arc::ptr_eq(&current, &first)));

        state.update(ApiDbGenerationEvent::Published(replacement.clone())).expect("replacement generation is accepted");
        state.update(ApiDbGenerationEvent::Retired(first)).expect("stale retirement is idempotent");
        assert!(current(&state).is_some_and(|current| Arc::ptr_eq(&current, &replacement)));

        state.update(ApiDbGenerationEvent::Retired(replacement)).expect("exact retirement is accepted");
        assert!(current(&state).is_none());
    }

    #[test]
    fn only_state_changing_events_publish_a_complete_snapshot() {
        let state = ApiDbState::<TestGeneration>::new();
        let generation = TestGeneration::valid();
        let mut snapshots = state.tx.subscribe();

        state.update(ApiDbGenerationEvent::Published(generation.clone())).expect("publication succeeds");
        assert!(snapshots.has_changed().expect("binding remains open"));
        snapshots.borrow_and_update();

        state.update(ApiDbGenerationEvent::Published(generation.clone())).expect("repeated publication is idempotent");
        assert!(!snapshots.has_changed().expect("binding remains open"));

        let stale = TestGeneration::valid();
        state.update(ApiDbGenerationEvent::Retired(stale)).expect("stale retirement is idempotent");
        assert!(!snapshots.has_changed().expect("binding remains open"));
        assert!(current(&state).is_some_and(|current| Arc::ptr_eq(&current, &generation)));

        state.update(ApiDbGenerationEvent::Retired(generation)).expect("exact retirement succeeds");
        assert!(snapshots.has_changed().expect("binding remains open"));
    }

    #[test]
    fn a_distinct_invalid_publication_is_rejected_without_changing_or_publishing_state() {
        let state = ApiDbState::<TestGeneration>::new();
        let current_generation = TestGeneration::valid();
        state.update(ApiDbGenerationEvent::Published(current_generation.clone())).expect("valid generation is accepted");
        let snapshots = state.tx.subscribe();
        let invalid = TestGeneration::invalid();

        assert_eq!(state.update(ApiDbGenerationEvent::Published(invalid)), Err(ApiDbUpdateError::InvalidGeneration));

        assert!(!snapshots.has_changed().expect("binding remains open"));
        assert!(current(&state).is_some_and(|current| Arc::ptr_eq(&current, &current_generation)));
    }

    #[test]
    fn stopped_binding_rejects_every_later_generation_event() {
        let state = ApiDbState::<TestGeneration>::new();
        state.tx.send_replace(ApiDbSnapshot::Stopped);
        let snapshots = state.tx.subscribe();

        assert_eq!(state.update(ApiDbGenerationEvent::Published(TestGeneration::valid())), Err(ApiDbUpdateError::Stopped));
        assert_eq!(state.update(ApiDbGenerationEvent::Retired(TestGeneration::valid())), Err(ApiDbUpdateError::Stopped));
        assert!(!snapshots.has_changed().expect("binding remains observable"));
    }

    #[test]
    fn repeating_the_bound_publication_remains_idempotent_after_external_invalidation() {
        let state = ApiDbState::<TestGeneration>::new();
        let generation = TestGeneration::valid();
        state.update(ApiDbGenerationEvent::Published(generation.clone())).expect("publication succeeds");
        generation.retire();
        let snapshots = state.tx.subscribe();

        state
            .update(ApiDbGenerationEvent::Published(generation.clone()))
            .expect("same-Arc delivery is idempotent rather than a republication");

        assert!(current(&state).is_some_and(|current| Arc::ptr_eq(&current, &generation)));
        assert!(!snapshots.has_changed().expect("binding remains open"));
    }

    #[test]
    fn public_read_admission_is_immediate_and_requires_an_enabled_valid_current_generation() {
        let state = ApiDbState::<TestGeneration>::new();
        assert!(state.public_read_client().is_none());

        let generation = TestGeneration::valid();
        state.update(ApiDbGenerationEvent::Published(generation.clone())).expect("publication succeeds");
        assert!(state.public_read_client().is_some_and(|current| Arc::ptr_eq(&current, &generation)));

        generation.retire();
        assert!(state.public_read_client().is_none());

        let replacement = TestGeneration::valid();
        state.update(ApiDbGenerationEvent::Published(replacement.clone())).expect("replacement succeeds");
        state.tx.send_modify(|snapshot| {
            let ApiDbSnapshot::Running { public_reads_enabled, .. } = snapshot else {
                panic!("test binding remains running");
            };
            *public_reads_enabled = false;
        });
        assert!(state.public_read_client().is_none());
    }

    #[tokio::test]
    async fn construction_waits_through_absence_and_wakes_for_a_valid_publication() {
        let state = Arc::new(ApiDbState::<TestGeneration>::new());
        let waiter = {
            let state = state.clone();
            tokio::spawn(async move { state.current().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        let generation = TestGeneration::valid();
        state.update(ApiDbGenerationEvent::Published(generation.clone())).expect("publication succeeds");

        let observed = waiter.await.expect("waiter task succeeds").expect("publication satisfies the waiter");
        assert!(Arc::ptr_eq(&observed, &generation));
    }

    #[tokio::test]
    async fn construction_ignores_the_public_gate_and_waits_past_an_invalid_current_generation() {
        let state = Arc::new(ApiDbState::<TestGeneration>::new());
        let invalid = TestGeneration::valid();
        state.update(ApiDbGenerationEvent::Published(invalid.clone())).expect("publication succeeds");
        invalid.retire();
        state.tx.send_modify(|snapshot| {
            let ApiDbSnapshot::Running { public_reads_enabled, .. } = snapshot else {
                panic!("test binding remains running");
            };
            *public_reads_enabled = false;
        });

        let waiter = {
            let state = state.clone();
            tokio::spawn(async move { state.current().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        let replacement = TestGeneration::valid();
        state.update(ApiDbGenerationEvent::Published(replacement.clone())).expect("replacement succeeds");

        let observed = waiter.await.expect("waiter task succeeds").expect("replacement satisfies the waiter");
        assert!(Arc::ptr_eq(&observed, &replacement));
    }
}
