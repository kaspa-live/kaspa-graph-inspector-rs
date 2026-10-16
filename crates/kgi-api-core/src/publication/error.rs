use kgi_api_model::{graph::GraphViewUpdateError, history::GraphHistoryAppendError};
use thiserror::Error;

/// Failure to preserve one publication's projected graph lineage.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum PublicationProjectionError {
    /// The independently published image and history do not share one revision.
    #[error("publication image revision {image_revision} does not match history revision {history_revision}")]
    RevisionMismatch { image_revision: u64, history_revision: u64 },

    /// The canonical Head view refused the original committed update.
    #[error("publication graph update failed: {0}")]
    ViewUpdate(#[from] GraphViewUpdateError),

    /// The resulting canonical delta did not extend retained history.
    #[error("publication history append failed: {0}")]
    HistoryAppend(#[from] GraphHistoryAppendError),
}
