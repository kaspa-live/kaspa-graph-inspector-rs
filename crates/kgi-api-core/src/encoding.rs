#![allow(dead_code, reason = "the encoding boundary is submitted by the scheduler in the next increment point")]

use std::io::{self, Write};

use bytes::Bytes;
use flate2::{Compression, GzBuilder};
use serde::Serialize;
use thiserror::Error;

/// Maximum complete uncompressed JSON body accepted for a graph response.
pub(crate) const MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

const GZIP_COMPRESSION_LEVEL: u32 = 6;

/// One completely serialized, size-checked, and compressed graph response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedGraphBody {
    uncompressed_json_bytes: u64,
    gzip_body: Bytes,
}

impl EncodedGraphBody {
    /// Returns the complete uncompressed JSON byte count checked before gzip.
    pub(crate) const fn uncompressed_json_bytes(&self) -> u64 {
        self.uncompressed_json_bytes
    }

    /// Returns the immutable final gzip representation.
    pub(crate) const fn gzip_body(&self) -> &Bytes {
        &self.gzip_body
    }

    /// Consumes the value and returns its immutable final gzip representation.
    pub(crate) fn into_gzip_body(self) -> Bytes {
        self.gzip_body
    }
}

/// Request-local failure while constructing one graph response representation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum GraphEncodingError {
    /// The detached response value could not be serialized as JSON.
    #[error("graph JSON serialization failed")]
    Serialization,
    /// The complete uncompressed JSON representation exceeds its hard limit.
    #[error("uncompressed graph JSON exceeds its hard response limit")]
    UncompressedResponseTooLarge,
    /// The size-checked JSON representation could not be compressed.
    #[error("graph gzip compression failed")]
    Compression,
}

/// Serializes and gzip-compresses one detached graph response value.
pub(crate) fn encode_graph<T>(value: &T) -> Result<EncodedGraphBody, GraphEncodingError>
where
    T: Serialize,
{
    encode_graph_with(value, gzip)
}

fn encode_graph_with<T, C>(value: &T, compressor: C) -> Result<EncodedGraphBody, GraphEncodingError>
where
    T: Serialize,
    C: FnOnce(&[u8]) -> io::Result<Vec<u8>>,
{
    let mut json = LimitedJsonBuffer::new(MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES);
    if serde_json::to_writer(&mut json, value).is_err() {
        return Err(if json.limit_exceeded {
            GraphEncodingError::UncompressedResponseTooLarge
        } else {
            GraphEncodingError::Serialization
        });
    }

    let json = json.into_inner();
    let uncompressed_json_bytes = u64::try_from(json.len()).expect("a bounded graph response length fits u64");
    let gzip_body = compressor(&json).map(Bytes::from).map_err(|_| GraphEncodingError::Compression)?;

    Ok(EncodedGraphBody { uncompressed_json_bytes, gzip_body })
}

fn gzip(json: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = GzBuilder::new().mtime(0).write(Vec::new(), Compression::new(GZIP_COMPRESSION_LEVEL));
    encoder.write_all(json)?;
    encoder.finish()
}

struct LimitedJsonBuffer {
    bytes: Vec<u8>,
    limit: usize,
    limit_exceeded: bool,
}

impl LimitedJsonBuffer {
    fn new(limit: usize) -> Self {
        Self { bytes: Vec::new(), limit, limit_exceeded: false }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for LimitedJsonBuffer {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let Some(new_len) = self.bytes.len().checked_add(input.len()) else {
            self.limit_exceeded = true;
            return Err(io::Error::other("graph JSON size overflow"));
        };
        if new_len > self.limit {
            self.limit_exceeded = true;
            return Err(io::Error::other("graph JSON response limit exceeded"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};

    use flate2::read::GzDecoder;
    use serde::{Serialize, Serializer};

    use super::{GZIP_COMPRESSION_LEVEL, GraphEncodingError, MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES, encode_graph, encode_graph_with};

    #[test]
    fn encodes_exact_limit_as_deterministic_gzip() {
        let value = "a".repeat(MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES - 2);

        let first = encode_graph(&value).expect("the exact limit is accepted");
        let second = encode_graph(&value).expect("repeated encoding succeeds");

        assert_eq!(first.uncompressed_json_bytes(), MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES as u64);
        assert_eq!(first.gzip_body(), second.gzip_body());

        let mut decoded = Vec::new();
        GzDecoder::new(first.gzip_body().as_ref()).read_to_end(&mut decoded).expect("gzip body decodes");
        assert_eq!(decoded.len(), MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES);
        assert_eq!(decoded.first(), Some(&b'"'));
        assert_eq!(decoded.last(), Some(&b'"'));
        assert!(decoded[1..decoded.len() - 1].iter().all(|byte| *byte == b'a'));
    }

    #[test]
    fn rejects_the_first_byte_above_the_uncompressed_limit_without_compression() {
        let value = "a".repeat(MAX_UNCOMPRESSED_GRAPH_RESPONSE_BYTES - 1);
        let mut compression_called = false;

        let error = encode_graph_with(&value, |_| {
            compression_called = true;
            Ok(Vec::new())
        })
        .expect_err("the first oversized byte is rejected");

        assert_eq!(error, GraphEncodingError::UncompressedResponseTooLarge);
        assert!(!compression_called);
    }

    #[test]
    fn distinguishes_serialization_failure_from_the_size_limit() {
        struct FailingValue;

        impl Serialize for FailingValue {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                Err(serde::ser::Error::custom("injected serialization failure"))
            }
        }

        assert_eq!(encode_graph(&FailingValue), Err(GraphEncodingError::Serialization));
    }

    #[test]
    fn reports_compression_failure_after_successful_size_check() {
        let error = encode_graph_with(&"graph", |_| Err(io::Error::other("injected compression failure")))
            .expect_err("compression failure is request-local");

        assert_eq!(error, GraphEncodingError::Compression);
    }

    #[test]
    fn records_the_selected_gzip_quality() {
        assert_eq!(GZIP_COMPRESSION_LEVEL, 6);
    }
}
