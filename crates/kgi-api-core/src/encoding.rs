#![allow(dead_code, reason = "the encoding boundary is submitted by the scheduler in the next increment point")]

use std::io::{self, Write};

use bytes::Bytes;
use flate2::{Compression, GzBuilder};
use serde::Serialize;
use thiserror::Error;

pub(crate) mod scheduler;

/// Maximum complete uncompressed JSON body accepted for a non-delta graph response.
pub(crate) const MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

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

/// Diagnostic evidence that an admitted canonical delta exceeded the expected hard bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeltaEstimatorOverrun {
    estimated_raw_bytes: u64,
    observed_uncompressed_json_bytes: u64,
}

impl DeltaEstimatorOverrun {
    /// Returns the pre-election additive estimate that admitted the candidate.
    pub(crate) const fn estimated_raw_bytes(self) -> u64 {
        self.estimated_raw_bytes
    }

    /// Returns the complete measured canonical-delta JSON size.
    pub(crate) const fn observed_uncompressed_json_bytes(self) -> u64 {
        self.observed_uncompressed_json_bytes
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
    let mut json = LimitedJsonBuffer::new(MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES);
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

/// Serializes and gzip-compresses one estimator-admitted canonical delta.
///
/// An unexpected hard-bound overrun is reported after complete measurement
/// and before compression, but does not reject or truncate the response.
pub(crate) fn encode_delta_graph<T>(
    value: &T,
    estimated_raw_bytes: u64,
    observe_overrun: impl FnOnce(DeltaEstimatorOverrun),
) -> Result<EncodedGraphBody, GraphEncodingError>
where
    T: Serialize,
{
    encode_delta_graph_with(value, estimated_raw_bytes, observe_overrun, gzip)
}

fn encode_delta_graph_with<T, O, C>(
    value: &T,
    estimated_raw_bytes: u64,
    observe_overrun: O,
    compressor: C,
) -> Result<EncodedGraphBody, GraphEncodingError>
where
    T: Serialize,
    O: FnOnce(DeltaEstimatorOverrun),
    C: FnOnce(&[u8]) -> io::Result<Vec<u8>>,
{
    let json = serde_json::to_vec(value).map_err(|_| GraphEncodingError::Serialization)?;
    let uncompressed_json_bytes = u64::try_from(json.len()).expect("a canonical-delta JSON length fits u64");
    if json.len() > MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES {
        observe_overrun(DeltaEstimatorOverrun { estimated_raw_bytes, observed_uncompressed_json_bytes: uncompressed_json_bytes });
    }
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
    use std::{
        cell::Cell,
        io::{self, Read},
    };

    use flate2::read::GzDecoder;
    use serde::{Serialize, Serializer};

    use super::{
        DeltaEstimatorOverrun, GZIP_COMPRESSION_LEVEL, GraphEncodingError, MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES,
        encode_delta_graph_with, encode_graph, encode_graph_with,
    };

    #[test]
    fn encodes_exact_limit_as_deterministic_gzip() {
        let value = "a".repeat(MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES - 2);

        let first = encode_graph(&value).expect("the exact limit is accepted");
        let second = encode_graph(&value).expect("repeated encoding succeeds");

        assert_eq!(first.uncompressed_json_bytes(), MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES as u64);
        assert_eq!(first.gzip_body(), second.gzip_body());

        let mut decoded = Vec::new();
        GzDecoder::new(first.gzip_body().as_ref()).read_to_end(&mut decoded).expect("gzip body decodes");
        assert_eq!(decoded.len(), MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES);
        assert_eq!(decoded.first(), Some(&b'"'));
        assert_eq!(decoded.last(), Some(&b'"'));
        assert!(decoded[1..decoded.len() - 1].iter().all(|byte| *byte == b'a'));
    }

    #[test]
    fn rejects_the_first_byte_above_the_uncompressed_limit_without_compression() {
        let value = "a".repeat(MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES - 1);
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
    fn canonical_delta_reports_an_overrun_before_compressing_the_complete_body() {
        let value = "a".repeat(MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES);
        let observation = Cell::new(None);
        let encoded = encode_delta_graph_with(
            &value,
            17,
            |overrun| observation.set(Some(overrun)),
            |json| {
                assert_eq!(
                    observation.get(),
                    Some(DeltaEstimatorOverrun {
                        estimated_raw_bytes: 17,
                        observed_uncompressed_json_bytes: u64::try_from(json.len()).expect("test JSON length fits u64"),
                    }),
                    "the diagnostic observation precedes compression"
                );
                Ok(json.to_vec())
            },
        )
        .expect("an estimator overrun remains a successful complete encoding");

        assert_eq!(encoded.uncompressed_json_bytes(), (MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES + 2) as u64);
        assert_eq!(encoded.gzip_body().len(), MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES + 2);
    }

    #[test]
    fn canonical_delta_within_the_hard_bound_emits_no_overrun() {
        let mut observation = None;
        let encoded = encode_delta_graph_with(&"delta", 5, |overrun| observation = Some(overrun), |json| Ok(json.to_vec()))
            .expect("ordinary canonical delta encodes");

        assert_eq!(encoded.uncompressed_json_bytes(), 7);
        assert_eq!(observation, None);
    }

    #[test]
    fn records_the_selected_gzip_quality() {
        assert_eq!(GZIP_COMPRESSION_LEVEL, 6);
    }
}
