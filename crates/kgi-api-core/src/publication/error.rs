use thiserror::Error;

/// Failure to preserve one publication's projected graph lineage.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum PublicationProjectionError {
    /// The independently published image and history do not share one revision.
    #[error("publication image revision {image_revision} does not match history revision {history_revision}")]
    RevisionMismatch { image_revision: u64, history_revision: u64 },
}
