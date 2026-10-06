use std::{io, path::PathBuf, sync::Arc};

use kaspa_consensus_core::network::NetworkId;
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
