//! Representation-independent values shared by public API operations.

use std::sync::Arc;

use kgi_model::lifecycle::{NodeServiceStatus, ProcessingStatus, StorageServiceStatus, SupervisorStatus};

use crate::{graph::Level, history::GraphHistoryEntryList};

/// Exact ordered depth tiers retained by the publication-local Head cache.
pub const CACHED_HEAD_WINDOW_DEPTHS: [u32; 6] = [20, 50, 100, 150, 200, 250];

/// Maximum age of a cached Head snapshot in publication-local milliseconds.
pub const CACHED_HEAD_SNAPSHOT_MAX_AGE_MS: u64 = 90_000;

/// Maximum age of a source-keyed canonical delta in publication-local milliseconds.
pub const DELTA_CACHE_MAX_AGE_MS: u64 = 300_000;

/// Fixed revision span used to enter the canonical-delta dyadic ladder.
pub const DELTA_BASE_REVISION_SPAN: u64 = 4;

/// Largest ordinary revision span in the canonical-delta dyadic ladder.
pub const MAX_DELTA_REVISION_SPAN: u64 = 128;

/// Permitted process-wide canonical-delta intervals.
pub const ADAPTIVE_DELTA_INTERVALS: [u64; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// Initial process-wide canonical-delta interval.
pub const INITIAL_ADAPTIVE_DELTA_INTERVAL: u64 = 4;

/// Reason a request must abandon its current graph lineage or source cursor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FreshViewReason {
    /// The requested publication is not the addressed retained publication.
    PublicationMismatch,
    /// The requested source revision has left retained history.
    StartPruned,
    /// The requested source revision is not a retained history boundary.
    StartUnavailable,
    /// The source is too old to recreate after its cache entry was pruned.
    DeltaSourceExpired,
    /// An uncrossable retained-history barrier follows the source cursor.
    DeltaReplayBarrier,
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
