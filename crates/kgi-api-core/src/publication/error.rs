use kgi_api_model::graph::GraphViewUpdateError;
use thiserror::Error;

/// Failure to preserve one publication's projected graph lineage.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum PublicationProjectionError {
    /// The candidate core does not contain one coherent graph revision.
    #[error("publication view revision {view_revision} does not match history revision {history_revision}")]
    RevisionMismatch { view_revision: u64, history_revision: u64 },

    /// The canonical Head view refused the original committed update.
    #[error("publication graph update failed: {0}")]
    ViewUpdate(#[from] GraphViewUpdateError),
}
