use std::{io, path::PathBuf, sync::Arc};

use kaspa_consensus_core::network::NetworkId;
use kgi_model::{
    block::BlockHash,
    lifecycle::{RecoveryInputKind, ScoreRangeFault},
};
use thiserror::Error;

/// Consensus parameter whose configured or derived value is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsensusParameter {
    TargetTimePerBlock,
    MergeSetSizeLimit,
    GetBlocksCoreBudget,
    VspcV2AddedBatchSize,
    AnticoneFinalizationDepth,
    CatchupMaxDaaGap,
}

/// Reason a consensus parameter cannot be used by KGI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsensusParameterReason {
    BelowMinimum,
    AboveMaximum,
    ArithmeticOverflow,
}

/// Typed failure produced before unsafe consensus-derived arithmetic is used.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid {parameter:?}: {reason:?} ({diagnostic})")]
pub struct InvalidConsensusParameters {
    pub parameter: ConsensusParameter,
    pub reason: ConsensusParameterReason,
    diagnostic: Arc<str>,
}

impl InvalidConsensusParameters {
    pub(crate) fn new(parameter: ConsensusParameter, reason: ConsensusParameterReason, diagnostic: impl Into<Arc<str>>) -> Self {
        Self { parameter, reason, diagnostic: diagnostic.into() }
    }

    /// Returns non-semantic diagnostic context for operators.
    #[must_use]
    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }
}

/// Startup failure while resolving KGI's local consensus assumptions.
#[derive(Debug, Error)]
pub enum ConsensusResolutionError {
    #[error("override parameters are unsupported for {network_id}")]
    OverrideUnsupported { network_id: NetworkId },

    #[error("failed to read override parameters from {path}: {source}")]
    ReadOverride {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to parse override parameters from {path}: {source}")]
    ParseOverride {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error(transparent)]
    InvalidParameters(#[from] InvalidConsensusParameters),
}

/// Permanent reason a physical node connection cannot be published.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NodeRejection {
    #[error("node network mismatch: expected {expected}, observed {observed}")]
    NetworkMismatch { expected: NetworkId, observed: NetworkId },

    #[error(
        "incompatible RPC API: required {required_version}.{minimum_revision}+, observed {observed_version:?}.{observed_revision:?}"
    )]
    IncompatibleRpcApi { required_version: u16, minimum_revision: u16, observed_version: Option<u16>, observed_revision: Option<u16> },

    #[error(
        "required notification capabilities are unavailable: handle_stop_notify={handle_stop_notify}, handle_message_id={handle_message_id}"
    )]
    MissingNotificationCapabilities { handle_stop_notify: bool, handle_message_id: bool },
}

/// Transient reason NodeService currently has no validated RPC generation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NodeUnavailableReason {
    #[error("node connection failed: {diagnostic}")]
    ConnectionFailed { diagnostic: Arc<str> },

    #[error("node validation failed: {diagnostic}")]
    ValidationFailed { diagnostic: Arc<str> },

    #[error("node is performing initial block download")]
    InitialBlockDownload,

    #[error("validated node connection was lost: {diagnostic}")]
    ConnectionLost { diagnostic: Arc<str> },
}

/// Failure of the permanent NodeService control lifecycle.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NodeServiceError {
    #[error("NodeService control path is unavailable")]
    ControlUnavailable,

    #[error("NodeService event path closed while the service was running")]
    EventPathClosed,

    #[error("NodeService worker failed: {diagnostic}")]
    WorkerFailed { diagnostic: Arc<str> },
}

/// Failure returned by one operation on a validated RPC generation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NodeError {
    #[error("validated RPC generation was lost")]
    GenerationLost,

    #[error("RPC operation was cancelled")]
    Cancelled,

    #[error("block {hash} was definitively not found")]
    BlockNotFound { hash: BlockHash },

    #[error("RPC request failed: {diagnostic}")]
    RpcRequestFailed { diagnostic: Arc<str> },

    #[error("notification subscription control failed: {diagnostic}")]
    SubscriptionControlFailed { diagnostic: Arc<str> },

    #[error("notification subscription state does not permit this operation")]
    InvalidSubscriptionState,

    #[error("recovery input is invalid: {0:?}")]
    RecoveryInputInvalid(RecoveryInputKind),

    #[error("node score is outside KGI's range: {0:?}")]
    ScoreOutOfRange(ScoreRangeFault),

    #[error("NodeService retirement control path is unavailable")]
    RetirementControlUnavailable,
}
