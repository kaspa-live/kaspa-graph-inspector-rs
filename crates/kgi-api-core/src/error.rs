use std::sync::Arc;

use thiserror::Error;

/// Failure of the ApiService control lifecycle or one permanent owned worker.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ApiServiceError {
    /// The service has crossed its terminal control boundary.
    #[error("ApiService control is unavailable")]
    ControlUnavailable,

    /// The permanent publication-runtime worker returned or panicked unexpectedly.
    #[error("ApiService publication runtime failed: {diagnostic}")]
    PublicationRuntimeFailed {
        /// Non-semantic diagnostic context for operators.
        diagnostic: Arc<str>,
    },

    /// The service-owned encoding scheduler returned or panicked unexpectedly.
    #[error("ApiService encoding scheduler failed: {diagnostic}")]
    EncodingSchedulerFailed {
        /// Non-semantic diagnostic context for operators.
        diagnostic: Arc<str>,
    },
}
