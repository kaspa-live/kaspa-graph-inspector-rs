use kgi_api_model::{projection::GraphWindowAnchorUnavailable, protocol::FreshViewReason};
use serde::Serialize;

use super::{scalar::BlockHashDto, scalar::U64Dto, to_json_bytes};

/// Common direct JSON error body.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ApiErrorDto {
    category: ApiErrorCategoryDto,
    code: ApiErrorCodeDto,
    message: &'static str,
    details: Option<ApiErrorDetailsDto>,
}

impl ApiErrorDto {
    /// Constructs an invalid-request response body.
    pub(crate) const fn invalid_request() -> Self {
        Self::without_details(ApiErrorCategoryDto::InvalidRequest, ApiErrorCodeDto::InvalidRequest, "The request is invalid.")
    }

    /// Constructs a method-not-allowed response body.
    pub(crate) const fn method_not_allowed() -> Self {
        Self::without_details(
            ApiErrorCategoryDto::InvalidRequest,
            ApiErrorCodeDto::MethodNotAllowed,
            "The requested method is not allowed.",
        )
    }

    /// Constructs a gzip-required response body.
    pub(crate) const fn gzip_required() -> Self {
        Self::without_details(
            ApiErrorCategoryDto::NotAcceptable,
            ApiErrorCodeDto::GzipRequired,
            "Graph requests require gzip support.",
        )
    }

    /// Constructs a route-not-found response body.
    pub(crate) const fn route_not_found() -> Self {
        Self::without_details(ApiErrorCategoryDto::NotFound, ApiErrorCodeDto::RouteNotFound, "The requested route was not found.")
    }

    /// Constructs a typed unavailable-window-anchor response body.
    pub(crate) fn window_anchor_unavailable(reason: GraphWindowAnchorUnavailable) -> Self {
        Self {
            category: ApiErrorCategoryDto::NotFound,
            code: ApiErrorCodeDto::WindowAnchorUnavailable,
            message: "The graph window anchor is unavailable.",
            details: Some(ApiErrorDetailsDto::WindowAnchorUnavailable(reason.into())),
        }
    }

    /// Constructs a typed unavailable-Head-level response body.
    pub(crate) fn head_level_unavailable(requested_level: u64) -> Self {
        Self {
            category: ApiErrorCategoryDto::NotFound,
            code: ApiErrorCodeDto::HeadLevelUnavailable,
            message: "A requested Head level is unavailable.",
            details: Some(ApiErrorDetailsDto::HeadLevelUnavailable { requested_level: requested_level.into() }),
        }
    }

    /// Constructs a client-registration-required response body.
    pub(crate) const fn client_registration_required() -> Self {
        Self::without_details(
            ApiErrorCategoryDto::Conflict,
            ApiErrorCodeDto::ClientRegistrationRequired,
            "A current client registration is required.",
        )
    }

    /// Constructs a typed fresh-view-required response body.
    pub(crate) fn fresh_view_required(reason: FreshViewReason) -> Self {
        Self {
            category: ApiErrorCategoryDto::Conflict,
            code: ApiErrorCodeDto::FreshViewRequired,
            message: "A fresh graph view is required.",
            details: Some(ApiErrorDetailsDto::FreshViewRequired { reason: reason.into() }),
        }
    }

    /// Constructs a graph-response-too-large response body.
    pub(crate) fn graph_response_too_large(max_uncompressed_bytes: u64, actual_uncompressed_bytes: u64) -> Self {
        Self {
            category: ApiErrorCategoryDto::Unprocessable,
            code: ApiErrorCodeDto::GraphResponseTooLarge,
            message: "The graph response exceeds the size limit.",
            details: Some(ApiErrorDetailsDto::GraphResponseTooLarge {
                max_uncompressed_bytes: max_uncompressed_bytes.into(),
                actual_uncompressed_bytes: actual_uncompressed_bytes.into(),
            }),
        }
    }

    /// Constructs a capacity-exhausted response body.
    pub(crate) const fn capacity_exhausted() -> Self {
        Self::without_details(ApiErrorCategoryDto::Busy, ApiErrorCodeDto::CapacityExhausted, "Request capacity is exhausted.")
    }

    /// Constructs a service-unavailable response body.
    pub(crate) const fn service_unavailable() -> Self {
        Self::without_details(
            ApiErrorCategoryDto::Unavailable,
            ApiErrorCodeDto::ServiceUnavailable,
            "The service is temporarily unavailable.",
        )
    }

    /// Constructs an encoding-timeout response body.
    pub(crate) const fn encoding_timeout() -> Self {
        Self::without_details(ApiErrorCategoryDto::Unavailable, ApiErrorCodeDto::EncodingTimeout, "Graph encoding timed out.")
    }

    /// Constructs an internal-error response body without internal diagnostics.
    pub(crate) const fn internal_error() -> Self {
        Self::without_details(ApiErrorCategoryDto::Internal, ApiErrorCodeDto::InternalError, "An internal error occurred.")
    }

    /// Serializes this error body as compact JSON.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(self)
    }

    const fn without_details(category: ApiErrorCategoryDto, code: ApiErrorCodeDto, message: &'static str) -> Self {
        Self { category, code, message, details: None }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ApiErrorCategoryDto {
    InvalidRequest,
    NotAcceptable,
    NotFound,
    Conflict,
    Unprocessable,
    Busy,
    Unavailable,
    Internal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ApiErrorCodeDto {
    InvalidRequest,
    MethodNotAllowed,
    GzipRequired,
    RouteNotFound,
    WindowAnchorUnavailable,
    HeadLevelUnavailable,
    ClientRegistrationRequired,
    FreshViewRequired,
    GraphResponseTooLarge,
    CapacityExhausted,
    ServiceUnavailable,
    EncodingTimeout,
    InternalError,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
enum ApiErrorDetailsDto {
    WindowAnchorUnavailable(WindowAnchorUnavailableDetailsDto),
    HeadLevelUnavailable { requested_level: U64Dto },
    FreshViewRequired { reason: FreshViewReasonDto },
    GraphResponseTooLarge { max_uncompressed_bytes: U64Dto, actual_uncompressed_bytes: U64Dto },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
enum WindowAnchorUnavailableDetailsDto {
    LevelNotRetained { requested_level: U64Dto },
    BlockNotMaterialized { requested_hash: BlockHashDto },
    NoRetainedDaaMatch { requested_score: U64Dto },
}

impl From<GraphWindowAnchorUnavailable> for WindowAnchorUnavailableDetailsDto {
    fn from(value: GraphWindowAnchorUnavailable) -> Self {
        match value {
            GraphWindowAnchorUnavailable::LevelNotRetained { requested_level } => {
                Self::LevelNotRetained { requested_level: requested_level.into() }
            }
            GraphWindowAnchorUnavailable::BlockNotMaterialized { requested_hash } => {
                Self::BlockNotMaterialized { requested_hash: requested_hash.into() }
            }
            GraphWindowAnchorUnavailable::NoRetainedDaaMatch { requested_score } => {
                Self::NoRetainedDaaMatch { requested_score: requested_score.into() }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum FreshViewReasonDto {
    PublicationMismatch,
    StartPruned,
    StartUnavailable,
    DeltaDistanceExceeded,
    FirstStoredDeltaExceedsBudget,
}

impl From<FreshViewReason> for FreshViewReasonDto {
    fn from(value: FreshViewReason) -> Self {
        match value {
            FreshViewReason::PublicationMismatch => Self::PublicationMismatch,
            FreshViewReason::StartPruned => Self::StartPruned,
            FreshViewReason::StartUnavailable => Self::StartUnavailable,
            FreshViewReason::DeltaDistanceExceeded => Self::DeltaDistanceExceeded,
            FreshViewReason::FirstStoredDeltaExceedsBudget => Self::FirstStoredDeltaExceedsBudget,
        }
    }
}

#[cfg(test)]
mod tests {
    use kgi_api_model::{projection::GraphWindowAnchorUnavailable, protocol::FreshViewReason};
    use kgi_model::block::BlockHash;
    use serde_json::{Value, json};

    use super::ApiErrorDto;

    fn value(error: &ApiErrorDto) -> Value {
        let encoded = error.encode().expect("error JSON");
        let text = std::str::from_utf8(&encoded).expect("JSON is UTF-8");
        assert!(!text.contains(": "));
        assert!(!text.contains(", "));
        serde_json::from_slice(&encoded).expect("error value")
    }

    #[test]
    fn errors_without_details_emit_explicit_null_and_safe_messages() {
        let cases = [
            (ApiErrorDto::invalid_request(), "invalid-request", "invalid-request"),
            (ApiErrorDto::method_not_allowed(), "invalid-request", "method-not-allowed"),
            (ApiErrorDto::gzip_required(), "not-acceptable", "gzip-required"),
            (ApiErrorDto::route_not_found(), "not-found", "route-not-found"),
            (ApiErrorDto::client_registration_required(), "conflict", "client-registration-required"),
            (ApiErrorDto::capacity_exhausted(), "busy", "capacity-exhausted"),
            (ApiErrorDto::service_unavailable(), "unavailable", "service-unavailable"),
            (ApiErrorDto::encoding_timeout(), "unavailable", "encoding-timeout"),
            (ApiErrorDto::internal_error(), "internal", "internal-error"),
        ];

        for (error, category, code) in cases {
            let encoded = value(&error);
            assert_eq!(encoded["category"], category);
            assert_eq!(encoded["code"], code);
            assert!(encoded["message"].as_str().is_some_and(|message| !message.is_empty()));
            assert_eq!(encoded["details"], Value::Null);
        }
    }

    #[test]
    fn structured_errors_use_exact_detail_shapes() {
        assert_eq!(
            value(&ApiErrorDto::window_anchor_unavailable(GraphWindowAnchorUnavailable::LevelNotRetained {
                requested_level: u64::MAX,
            }))["details"],
            json!({ "reason": "level-not-retained", "requested_level": u64::MAX.to_string() })
        );
        assert_eq!(
            value(&ApiErrorDto::window_anchor_unavailable(GraphWindowAnchorUnavailable::BlockNotMaterialized {
                requested_hash: BlockHash::from_bytes([0xab; 32]),
            }))["details"],
            json!({ "reason": "block-not-materialized", "requested_hash": "ab".repeat(32) })
        );
        assert_eq!(
            value(&ApiErrorDto::window_anchor_unavailable(GraphWindowAnchorUnavailable::NoRetainedDaaMatch { requested_score: 17 }))["details"],
            json!({ "reason": "no-retained-daa-match", "requested_score": "17" })
        );
        assert_eq!(value(&ApiErrorDto::head_level_unavailable(19))["details"], json!({ "requested_level": "19" }));
        assert_eq!(
            value(&ApiErrorDto::graph_response_too_large(23, 29))["details"],
            json!({ "max_uncompressed_bytes": "23", "actual_uncompressed_bytes": "29" })
        );
    }

    #[test]
    fn every_fresh_view_reason_uses_its_exact_public_spelling() {
        let cases = [
            (FreshViewReason::PublicationMismatch, "publication-mismatch"),
            (FreshViewReason::StartPruned, "start-pruned"),
            (FreshViewReason::StartUnavailable, "start-unavailable"),
            (FreshViewReason::DeltaDistanceExceeded, "delta-distance-exceeded"),
            (FreshViewReason::FirstStoredDeltaExceedsBudget, "first-stored-delta-exceeds-budget"),
        ];

        for (reason, expected) in cases {
            let encoded = value(&ApiErrorDto::fresh_view_required(reason));
            assert_eq!(encoded["category"], "conflict");
            assert_eq!(encoded["code"], "fresh-view-required");
            assert_eq!(encoded["details"], json!({ "reason": expected }));
        }
    }
}
