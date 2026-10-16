#![allow(dead_code, reason = "the isolated codec foundation is integrated by later API increments")]

pub(crate) mod error;
pub(crate) mod lifecycle;
pub(crate) mod scalar;
pub(crate) mod sse;
pub(crate) mod status;
pub(crate) mod window;

use kgi_api_model::graph::LevelExtent;
use serde::Serialize;

fn to_json_bytes<T>(value: &T) -> Result<Vec<u8>, serde_json::Error>
where
    T: Serialize,
{
    serde_json::to_vec(value)
}

/// Current internal graph-payload representation version.
pub(crate) const GRAPH_REPRESENTATION_VERSION: u32 = 1;

/// Semantic identity input for one cacheable Head snapshot validator.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct HeadEtagInput {
    representation_version: u32,
    publication_id: u64,
    revision: u64,
    extent: LevelExtent,
}

impl HeadEtagInput {
    /// Constructs the semantic identity of one Head snapshot.
    pub(crate) const fn new(publication_id: u64, revision: u64, extent: LevelExtent) -> Self {
        Self { representation_version: GRAPH_REPRESENTATION_VERSION, publication_id, revision, extent }
    }

    /// Returns the graph-payload representation version.
    pub(crate) const fn representation_version(self) -> u32 {
        self.representation_version
    }

    /// Returns the publication that supplied the snapshot.
    pub(crate) const fn publication_id(self) -> u64 {
        self.publication_id
    }

    /// Returns the snapshot revision.
    pub(crate) const fn revision(self) -> u64 {
        self.revision
    }

    /// Returns the snapshot's actual nominal level extent.
    pub(crate) const fn extent(self) -> LevelExtent {
        self.extent
    }
}

#[cfg(test)]
mod tests {
    use super::{GRAPH_REPRESENTATION_VERSION, HeadEtagInput};
    use kgi_api_model::graph::LevelExtent;

    fn extent(low_level: u64, high_level: u64) -> LevelExtent {
        LevelExtent::new(low_level, high_level).expect("test extent is valid")
    }

    #[test]
    fn head_etag_input_contains_exact_semantic_identity() {
        let input = HeadEtagInput::new(7, 11, extent(13, 17));

        assert_eq!(input.representation_version(), GRAPH_REPRESENTATION_VERSION);
        assert_eq!(input.publication_id(), 7);
        assert_eq!(input.revision(), 11);
        assert_eq!(input.extent(), extent(13, 17));
    }

    #[test]
    fn every_head_etag_semantic_field_participates_in_equality() {
        let input = HeadEtagInput::new(7, 11, extent(13, 17));

        assert_ne!(input, HeadEtagInput::new(8, 11, extent(13, 17)));
        assert_ne!(input, HeadEtagInput::new(7, 12, extent(13, 17)));
        assert_ne!(input, HeadEtagInput::new(7, 11, extent(12, 17)));
        assert_ne!(input, HeadEtagInput::new(7, 11, extent(13, 18)));
        assert_ne!(input, HeadEtagInput { representation_version: GRAPH_REPRESENTATION_VERSION + 1, ..input });
    }
}
