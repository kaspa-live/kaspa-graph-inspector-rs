#![allow(dead_code, reason = "the isolated codec foundation is integrated by later API increments")]

pub(crate) mod delta;
pub(crate) mod error;
pub(crate) mod graph;
pub(crate) mod lifecycle;
pub(crate) mod scalar;
pub(crate) mod sse;
pub(crate) mod status;
pub(crate) mod window;

mod dictionary;

use std::sync::Arc;

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

/// Stable opaque value carried by the weak Head response validator.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct HeadEtag {
    opaque: Arc<str>,
}

impl HeadEtag {
    /// Returns the opaque validator text without HTTP quotes or the weak prefix.
    pub(crate) fn opaque(&self) -> &str {
        &self.opaque
    }

    /// Formats the complete weak HTTP validator value.
    pub(crate) fn weak_header_value(&self) -> String {
        format!("W/\"{}\"", self.opaque)
    }
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

    /// Encodes this semantic identity as one stable opaque Head validator.
    pub(crate) fn etag(self) -> HeadEtag {
        HeadEtag {
            opaque: format!(
                "kgi-head-v{}-p{}-r{}-l{}-h{}",
                self.representation_version,
                self.publication_id,
                self.revision,
                self.extent.low_level(),
                self.extent.high_level()
            )
            .into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GRAPH_REPRESENTATION_VERSION, HeadEtagInput};
    use kgi_api_model::graph::LevelExtent;

    fn extent(low_level: u32, high_level: u32) -> LevelExtent {
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

    #[test]
    fn head_etag_has_one_stable_weak_http_encoding() {
        let etag = HeadEtagInput::new(7, 11, extent(13, 17)).etag();

        assert_eq!(etag.opaque(), "kgi-head-v1-p7-r11-l13-h17");
        assert_eq!(etag.weak_header_value(), "W/\"kgi-head-v1-p7-r11-l13-h17\"");
    }
}
