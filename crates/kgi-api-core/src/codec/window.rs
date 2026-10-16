use serde::Serialize;

use super::{scalar::U64Dto, to_json_bytes};

/// Public lineage source of one anchored graph window.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum GraphWindowSourceDto {
    /// Window extracted from an immutable retained Head capture.
    Head { publication_id: U64Dto, revision: U64Dto, revision_timestamp_ms: U64Dto, head_high_level: U64Dto },
    /// Request-local database projection without public delta lineage.
    Database,
}

impl GraphWindowSourceDto {
    /// Constructs a Head-backed window source.
    pub(crate) fn head(publication_id: u64, revision: u64, revision_timestamp_ms: u64, head_high_level: u64) -> Self {
        Self::Head {
            publication_id: publication_id.into(),
            revision: revision.into(),
            revision_timestamp_ms: revision_timestamp_ms.into(),
            head_high_level: head_high_level.into(),
        }
    }

    /// Constructs a database-backed window source.
    pub(crate) const fn database() -> Self {
        Self::Database
    }

    /// Serializes this source discriminator as compact JSON.
    pub(crate) fn encode(self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::GraphWindowSourceDto;

    #[test]
    fn window_sources_use_exact_discriminated_shapes() {
        assert_eq!(
            GraphWindowSourceDto::head(42, 123, 456_789, 900).encode().expect("Head source JSON"),
            br#"{"type":"head","publication_id":"42","revision":"123","revision_timestamp_ms":"456789","head_high_level":"900"}"#
        );
        assert_eq!(GraphWindowSourceDto::database().encode().expect("database source JSON"), br#"{"type":"database"}"#);
    }
}
