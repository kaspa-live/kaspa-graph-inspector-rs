//! Representation-independent values shared by public API operations.

use std::sync::Arc;

use kgi_model::lifecycle::{NodeServiceStatus, ProcessingStatus, StorageServiceStatus, SupervisorStatus};

use crate::{graph::Level, history::GraphHistoryEntryList};

/// Maximum number of Head levels a cached delta target may trail its publication.
pub const MAX_CACHE_LEVEL_DISTANCE: u32 = 500;

/// Maximum number of Head levels a heated delta destination may trail its publication.
pub const HOT_DESTINATION_LEVEL_DISTANCE: u32 = 250;

/// Reason a request must abandon its current graph lineage or source cursor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FreshViewReason {
    /// The requested publication is not the addressed retained publication.
    PublicationMismatch,
    /// The requested source revision has left retained history.
    StartPruned,
    /// The requested source revision is not a retained history boundary.
    StartUnavailable,
    /// The requested delta distance exceeds the supported bound.
    DeltaDistanceExceeded,
    /// The first indivisible retained delta exceeds the response budget.
    FirstStoredDeltaExceedsBudget,
}

/// Semantic result of one canonical Head-delta request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeltaResponseOutcome {
    /// The request cursor already equals the selected target.
    UpToDate,
    /// The request requires a live-client registration before it can wait.
    ClientRegistrationRequired,
    /// No delta is available and the registered client should wait for this revision.
    WaitForWakeup {
        /// First revision whose availability should wake the client.
        wake_at_revision_id: u64,
    },
    /// The complete selected delta range fits the response budget.
    Complete(GraphHistoryEntryList),
    /// A complete prefix of the selected delta range fits the response budget.
    Prefix(GraphHistoryEntryList),
    /// The request cannot continue from its current graph lineage or source cursor.
    FreshViewRequired(FreshViewReason),
}

/// Required client action after one successful canonical Head-delta response.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeltaContinuation {
    /// The response reached the captured Head target.
    ReachedHead,
    /// Another delta request should follow immediately.
    ContinueImmediately,
    /// The registered client should wait for its next wakeup.
    WaitForWakeup,
}

/// One complete retained level returned by a Head-level lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeadLevelValue {
    /// Retained level number.
    pub level: u32,
    /// Complete public value at that level.
    pub value: Level,
}

/// Semantic result of one all-or-nothing retained Head-level lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeadLevelLookupOutcome {
    /// Every requested level was captured from one retained publication.
    Complete {
        /// Publication that supplied the immutable capture.
        publication_id: u64,
        /// Revision of the immutable capture.
        revision: u64,
        /// Complete values for the distinct requested levels.
        levels: Arc<[HeadLevelValue]>,
    },
    /// At least one requested level is unavailable in the retained Head image.
    LevelUnavailable {
        /// Unavailable level that prevents an all-or-nothing result.
        requested_level: u32,
    },
    /// The lookup requires a fresh view for the supplied reason.
    FreshViewRequired(FreshViewReason),
}

/// Eventually consistent public observation of the running KGI process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemStatus {
    /// Compile-time package version of the running KGI executable.
    pub kgi_version: String,
    /// Latest Supervisor observation.
    pub supervisor: SupervisorStatus,
    /// Latest NodeService observation.
    pub node: NodeServiceStatus,
    /// Latest StorageService observation.
    pub storage: StorageServiceStatus,
    /// Latest processing observation.
    pub processing: ProcessingStatus,
}
