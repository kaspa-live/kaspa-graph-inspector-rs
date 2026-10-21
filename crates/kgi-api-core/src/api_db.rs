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
        self.is_valid()
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
}

#[cfg(test)]
mod tests {
    use super::{ApiDbGeneration, ApiDbSnapshot, ApiDbState};

    struct TestGeneration;

    impl ApiDbGeneration for TestGeneration {
        fn is_valid(&self) -> bool {
            true
        }
    }

    #[test]
    fn binding_starts_running_without_a_generation_and_with_public_reads_enabled() {
        let state = ApiDbState::<TestGeneration>::new();
        let snapshot = state.tx.borrow();

        assert!(matches!(&*snapshot, ApiDbSnapshot::Running { current: None, public_reads_enabled: true }));
    }
}
