use kaspa_consensus_core::network::NetworkId;
use kgi_model::block::BlockHash;

/// Immutable network binding and pruning-point score of one database generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMetadata {
    network_id: NetworkId,
    genesis_hash: BlockHash,
    db_pp_blue_score: u64,
}

impl NodeMetadata {
    #[allow(dead_code, reason = "constructed by the private schema validator")]
    pub(crate) const fn new(network_id: NetworkId, genesis_hash: BlockHash, db_pp_blue_score: u64) -> Self {
        Self { network_id, genesis_hash, db_pp_blue_score }
    }

    /// Returns the exact network type and suffix bound to the database.
    #[must_use]
    pub const fn network_id(&self) -> NetworkId {
        self.network_id
    }

    /// Returns the immutable Genesis hash bound to the database.
    #[must_use]
    pub const fn genesis_hash(&self) -> BlockHash {
        self.genesis_hash
    }

    /// Returns the persisted pruning-point blue score.
    #[must_use]
    pub const fn db_pp_blue_score(&self) -> u64 {
        self.db_pp_blue_score
    }
}
