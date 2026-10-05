use std::fmt;
use thiserror::Error;

/// Immutable Kaspa block identity.
pub type BlockHash = kaspa_hashes::Hash;

/// Kaspa blue-work value used for deterministic consensus ordering.
pub type BlueWork = kaspa_math::Uint192;

/// Whole milliseconds since the Unix epoch, as reported by a Kaspa header.
pub type Timestamp = u64;

/// Highest DAA score representable by KGI.
pub const MAX_DAA_SCORE: u64 = i64::MAX as u64 - 1;

/// Highest blue score representable by KGI.
pub const MAX_BLUE_SCORE: u64 = i64::MAX as u64;

/// Database-local positive block identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct CompactId(i64);

impl CompactId {
    /// Creates an identifier when `value` is positive.
    #[must_use]
    pub const fn new(value: i64) -> Option<Self> {
        if value > 0 { Some(Self(value)) } else { None }
    }

    /// Returns the signed database representation.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for CompactId {
    type Error = InvalidCompactId;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(InvalidCompactId(value))
    }
}

impl From<CompactId> for i64 {
    fn from(value: CompactId) -> Self {
        value.get()
    }
}

impl fmt::Display for CompactId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A non-positive database value cannot be a KGI compact ID.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("compact ID must be positive, got {0}")]
pub struct InvalidCompactId(pub i64);

/// Persisted or projected block color.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BlockColor {
    Gray = 0,
    Blue,
    Red,
}

/// A normalized full node block admitted across the NodeService boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedNodeBlock {
    pub hash: BlockHash,
    pub selected_parent: BlockHash,
    pub direct_parents: Vec<BlockHash>,
    pub blue_merge_set: Vec<BlockHash>,
    pub red_merge_set: Vec<BlockHash>,
    pub timestamp: Timestamp,
    pub daa_score: u64,
    pub blue_score: u64,
    pub blue_work: BlueWork,
}

/// Deterministic order by `(blue_work, hash)`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsensusOrder {
    blue_work: BlueWork,
    hash: BlockHash,
}

impl ConsensusOrder {
    #[must_use]
    pub const fn new(blue_work: BlueWork, hash: BlockHash) -> Self {
        Self { blue_work, hash }
    }

    #[must_use]
    pub const fn blue_work(&self) -> &BlueWork {
        &self.blue_work
    }

    #[must_use]
    pub const fn hash(&self) -> BlockHash {
        self.hash
    }
}

/// A materialized VSPC position in one database generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VspcPoint {
    consensus_order: ConsensusOrder,
    id: CompactId,
}

impl VspcPoint {
    #[must_use]
    pub const fn new(consensus_order: ConsensusOrder, id: CompactId) -> Self {
        Self { consensus_order, id }
    }

    #[must_use]
    pub const fn hash(&self) -> BlockHash {
        self.consensus_order.hash()
    }

    #[must_use]
    pub const fn order(&self) -> &ConsensusOrder {
        &self.consensus_order
    }

    #[must_use]
    pub const fn id(&self) -> CompactId {
        self.id
    }
}

/// Database-local display position of a materialized block.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BlockCoordinate {
    level: u64,
    slot: u64,
}

impl BlockCoordinate {
    /// Creates a coordinate when `level` is positive.
    #[must_use]
    pub const fn new(level: u64, slot: u64) -> Option<Self> {
        if level > 0 { Some(Self { level, slot }) } else { None }
    }

    #[must_use]
    pub const fn level(self) -> u64 {
        self.level
    }

    #[must_use]
    pub const fn slot(self) -> u64 {
        self.slot
    }
}

/// Complete committed starting point for a recovery session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaterializedSyncAnchor {
    pub point: VspcPoint,
    pub selected_parent: BlockHash,
    pub blue_score: u64,
}

/// Persistent identity and materiality state for a block hash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockPresence {
    Absent,
    BoundaryIdentity { id: CompactId },
    Materialized { id: CompactId, coordinate: BlockCoordinate },
}

/// Materialized block identity delivered to processing consumers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistedBlock {
    pub point: VspcPoint,
    pub selected_parent: BlockHash,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    #[test]
    fn compact_id_requires_a_positive_signed_value() {
        assert_eq!(CompactId::new(-1), None);
        assert_eq!(CompactId::new(0), None);
        assert_eq!(CompactId::new(1).map(CompactId::get), Some(1));
        assert_eq!(CompactId::new(i64::MAX).map(CompactId::get), Some(i64::MAX));
    }

    #[test]
    fn materialized_coordinate_requires_a_positive_level() {
        assert_eq!(BlockCoordinate::new(0, 0), None);
        let coordinate = BlockCoordinate::new(1, 0).expect("level one is valid");
        assert_eq!(coordinate.level(), 1);
        assert_eq!(coordinate.slot(), 0);
    }

    #[test]
    fn consensus_order_is_lexicographic_by_work_then_hash() {
        let lower_work = ConsensusOrder::new(BlueWork::from_u64(1), hash(255));
        let higher_work = ConsensusOrder::new(BlueWork::from_u64(2), hash(0));
        assert!(lower_work < higher_work);

        let lower_hash = ConsensusOrder::new(BlueWork::from_u64(2), hash(0));
        let higher_hash = ConsensusOrder::new(BlueWork::from_u64(2), hash(1));
        assert!(lower_hash < higher_hash);
    }

    #[test]
    fn vspc_point_exposes_order_identity_without_duplicating_hash() {
        let order = ConsensusOrder::new(BlueWork::from_u64(42), hash(7));
        let id = CompactId::new(9).expect("positive compact ID");
        let point = VspcPoint::new(order, id);

        assert_eq!(point.hash(), hash(7));
        assert_eq!(point.order(), &order);
        assert_eq!(point.id(), id);
    }

    #[test]
    fn score_ranges_reserve_only_the_daa_sentinel() {
        assert_eq!(MAX_DAA_SCORE + 1, MAX_BLUE_SCORE);
        assert_eq!(MAX_BLUE_SCORE, i64::MAX as u64);
    }
}
