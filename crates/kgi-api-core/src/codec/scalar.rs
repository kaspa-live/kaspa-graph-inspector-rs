use kgi_model::block::{BlockColor, BlockHash};
use serde::{Serialize, Serializer};

/// Serialization-only response-local block-hash reference.
pub(crate) type HashRef = u32;

/// Public `u64` encoded as unsigned decimal JSON text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct U64Dto(u64);

impl From<u64> for U64Dto {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Serialize for U64Dto {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&self.0)
    }
}

/// Canonical hexadecimal block hash used at the JSON boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlockHashDto(BlockHash);

impl From<BlockHash> for BlockHashDto {
    fn from(value: BlockHash) -> Self {
        Self(value)
    }
}

impl Serialize for BlockHashDto {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&self.0)
    }
}

/// Numeric graph-body representation of one block color.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct BlockColorDto(u8);

impl From<BlockColor> for BlockColorDto {
    fn from(value: BlockColor) -> Self {
        Self(match value {
            BlockColor::Gray => 0,
            BlockColor::Blue => 1,
            BlockColor::Red => 2,
        })
    }
}

#[cfg(test)]
mod tests {
    use kgi_model::block::{BlockColor, BlockHash};

    use super::{BlockColorDto, BlockHashDto, U64Dto};

    #[test]
    fn public_scalars_use_exact_wire_representations() {
        assert_eq!(serde_json::to_string(&U64Dto::from(u64::MAX)).expect("u64 JSON"), format!("\"{}\"", u64::MAX));
        assert_eq!(
            serde_json::to_string(&BlockHashDto::from(BlockHash::from_bytes([0xab; 32]))).expect("hash JSON"),
            format!("\"{}\"", "ab".repeat(32))
        );
        assert_eq!(serde_json::to_string(&BlockColorDto::from(BlockColor::Gray)).expect("gray JSON"), "0");
        assert_eq!(serde_json::to_string(&BlockColorDto::from(BlockColor::Blue)).expect("blue JSON"), "1");
        assert_eq!(serde_json::to_string(&BlockColorDto::from(BlockColor::Red)).expect("red JSON"), "2");
    }
}
