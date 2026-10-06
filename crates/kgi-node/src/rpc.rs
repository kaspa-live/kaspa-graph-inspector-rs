use kaspa_rpc_core::api::ops::{RPC_API_REVISION, RPC_API_VERSION};
use kgi_model::block::BlockHash;

/// Minimal normalized marker used by Catchup sink tracking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatchupSinkSample {
    pub hash: BlockHash,
    pub daa_score: u64,
}

/// Returns whether a remote node satisfies KGI's compiled RPC API floor.
#[must_use]
#[allow(
    clippy::absurd_extreme_comparisons,
    reason = "the compiled revision is currently zero, but the architecture requires a revision floor"
)]
pub const fn is_rpc_api_compatible(remote_version: u16, remote_revision: u16) -> bool {
    remote_version == RPC_API_VERSION && remote_revision >= RPC_API_REVISION
}

#[cfg(test)]
mod tests {
    use kaspa_rpc_core::api::ops::{RPC_API_REVISION, RPC_API_VERSION};

    use super::is_rpc_api_compatible;

    #[test]
    fn api_version_is_exact_and_revision_is_a_floor() {
        assert!(is_rpc_api_compatible(RPC_API_VERSION, RPC_API_REVISION));
        assert!(is_rpc_api_compatible(RPC_API_VERSION, RPC_API_REVISION.saturating_add(1)));
        assert!(!is_rpc_api_compatible(RPC_API_VERSION.saturating_add(1), RPC_API_REVISION));
        if RPC_API_VERSION > 0 {
            assert!(!is_rpc_api_compatible(RPC_API_VERSION - 1, RPC_API_REVISION));
        }
        if let Some(lower_revision) = std::hint::black_box(RPC_API_REVISION).checked_sub(1) {
            assert!(!is_rpc_api_compatible(RPC_API_VERSION, lower_revision));
        }
    }
}
