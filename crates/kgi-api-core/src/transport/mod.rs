mod body;
mod query;
mod sse;

use std::sync::Arc;

use axum::{
    Router,
    body::{Body, Bytes, HttpBody},
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode, header},
    routing::{MethodFilter, on},
};
use kgi_api_model::protocol::DeltaContinuation;
use kgi_storage::error::ApiReadError;

use crate::{
    admission::{ApiRequestAdmissionError, ApiRequestGuard},
    cache::{CacheBuildError, CacheWaitError},
    codec::{HeadEtag, error::ApiErrorDto, status::SystemStatusDto},
    encoding::{GraphEncodingError, MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES, scheduler::EncodingSchedulerError},
    publication::{GraphPublication, GraphPublicationState},
    service::{
        AnchoredWindowError, AnchoredWindowResponse, ApiService, CanonicalDeltaResponse, CanonicalDeltaSuccess,
        HeadLevelLookupResponse, HeadSnapshotCondition, HeadSnapshotOutcome, SseConnectionOpenError,
    },
};

use self::{
    body::GuardedBody,
    query::{
        AnchoredWindowQuery, CanonicalDeltaQuery, EmptyQuery, HeadLevelsQuery, HeadSnapshotQuery, PublicationWakeupQuery, accepts_gzip,
    },
};

const KGI_PUBLICATION_ID: HeaderName = HeaderName::from_static("kgi-publication-id");
const KGI_PUBLICATION_STATE: HeaderName = HeaderName::from_static("kgi-publication-state");
const KGI_HEAD_REVISION_ID: HeaderName = HeaderName::from_static("kgi-head-revision-id");
const KGI_HEAD_REVISION_TIMESTAMP_MS: HeaderName = HeaderName::from_static("kgi-head-revision-timestamp-ms");
const KGI_DELTA_OUTCOME: HeaderName = HeaderName::from_static("kgi-delta-outcome");
const KGI_DELTA_CONTINUATION: HeaderName = HeaderName::from_static("kgi-delta-continuation");
const KGI_DELTA_WAKE_AT_REVISION: HeaderName = HeaderName::from_static("kgi-delta-wake-at-revision");
const KGI_DELTA_RETRY_AFTER_MS: HeaderName = HeaderName::from_static("kgi-delta-retry-after-ms");

pub(super) enum ParsedHeadCondition {
    Unconditional,
    Any,
    Tags(Vec<HeadEtag>),
}

impl ParsedHeadCondition {
    fn into_service_condition(self) -> HeadSnapshotCondition {
        match self {
            Self::Unconditional => HeadSnapshotCondition::Unconditional,
            Self::Any => HeadSnapshotCondition::Any,
            Self::Tags(tags) => HeadSnapshotCondition::Tags(tags.into()),
        }
    }
}

pub(super) fn router() -> Router<Arc<ApiService>> {
    Router::new()
        .route(
            "/graph/head",
            on(MethodFilter::GET, head_snapshot).on(MethodFilter::HEAD, graph_method_not_allowed).fallback(graph_method_not_allowed),
        )
        .route(
            "/graph/deltas",
            on(MethodFilter::GET, canonical_delta).on(MethodFilter::HEAD, graph_method_not_allowed).fallback(graph_method_not_allowed),
        )
        .route(
            "/graph/levels",
            on(MethodFilter::GET, head_levels).on(MethodFilter::HEAD, graph_method_not_allowed).fallback(graph_method_not_allowed),
        )
        .route(
            "/graph/window",
            on(MethodFilter::GET, anchored_window).on(MethodFilter::HEAD, graph_method_not_allowed).fallback(graph_method_not_allowed),
        )
        .route(
            "/graph/wakeups",
            on(MethodFilter::GET, publication_wakeups).on(MethodFilter::HEAD, method_not_allowed).fallback(method_not_allowed),
        )
        .route("/status", on(MethodFilter::GET, status).on(MethodFilter::HEAD, method_not_allowed).fallback(method_not_allowed))
        .fallback(route_not_found)
}

async fn status(State(service): State<Arc<ApiService>>, request: Request<Body>) -> Response<Body> {
    if EmptyQuery::parse(request.uri().query()).is_none() || request_has_body(&request) {
        return invalid_request(false);
    }
    let request = match service.admit_status_request() {
        Ok(request) => request,
        Err(error) => return admission_error(error, false),
    };
    match SystemStatusDto::encode(&service.system_status(&request)) {
        Ok(body) => response(StatusCode::OK, Bytes::from(body), Some("application/json"), "no-store", false, false, Some(request)),
        Err(_) => error_response(StatusCode::INTERNAL_SERVER_ERROR, ApiErrorDto::internal_error(), false, Some(request)),
    }
}

async fn head_snapshot(State(service): State<Arc<ApiService>>, request: Request<Body>) -> Response<Body> {
    let Some(query) = HeadSnapshotQuery::parse(request.uri().query()) else { return invalid_request(true) };
    if request_has_body(&request) {
        return invalid_request(true);
    }
    let Some(condition) = ParsedHeadCondition::parse(request.headers()) else { return invalid_request(true) };
    if !accepts_gzip(request.headers()) {
        return gzip_required();
    }
    let admitted = match service.admit_head_request() {
        Ok(request) => request,
        Err(error) => return admission_error(error, true),
    };
    match service.head_snapshot(query.target_depth, &condition.into_service_condition(), &admitted).await {
        Ok(HeadSnapshotOutcome::Unavailable) => unavailable(true, Some(admitted)),
        Ok(HeadSnapshotOutcome::NotModified { publication, etag }) => {
            let mut response = response(StatusCode::NOT_MODIFIED, Bytes::new(), None, "no-cache", true, false, Some(admitted));
            insert_header(response.headers_mut(), header::ETAG, &etag.weak_header_value());
            add_publication_headers(&mut response, &publication).await;
            response
        }
        Ok(HeadSnapshotOutcome::Complete { publication, snapshot }) => {
            let mut response = response(
                StatusCode::OK,
                snapshot.gzip_body().clone(),
                Some("application/json"),
                "no-cache",
                true,
                true,
                Some(admitted),
            );
            insert_header(response.headers_mut(), header::ETAG, &snapshot.etag().weak_header_value());
            add_publication_headers(&mut response, &publication).await;
            response
        }
        Err(error) => cache_wait_error(error, admitted),
    }
}

async fn canonical_delta(State(service): State<Arc<ApiService>>, request: Request<Body>) -> Response<Body> {
    let Some(query) = CanonicalDeltaQuery::parse(request.uri().query()) else { return invalid_request(true) };
    if request_has_body(&request) {
        return invalid_request(true);
    }
    if !accepts_gzip(request.headers()) {
        return gzip_required();
    }
    let admitted = match service.admit_head_request() {
        Ok(request) => request,
        Err(error) => return admission_error(error, true),
    };
    match service.canonical_delta(query.publication_id, query.from_revision_id, query.client, &admitted).await {
        Ok(CanonicalDeltaResponse::FreshViewRequired { publication, reason }) => {
            let mut response = error_response(StatusCode::CONFLICT, ApiErrorDto::fresh_view_required(reason), true, Some(admitted));
            if let Some(publication) = publication {
                add_publication_headers(&mut response, &publication).await;
            }
            response
        }
        Ok(CanonicalDeltaResponse::InvalidFutureCursor { publication }) => {
            let mut response = invalid_request_guarded(admitted);
            add_publication_headers(&mut response, &publication).await;
            response
        }
        Ok(CanonicalDeltaResponse::ClientRegistrationRequired { publication }) => {
            let mut response = error_response(StatusCode::CONFLICT, ApiErrorDto::client_registration_required(), true, Some(admitted));
            add_publication_headers(&mut response, &publication).await;
            response
        }
        Ok(CanonicalDeltaResponse::Success { publication, head, outcome, continuation, wake_at_revision_id, retry_after_ms }) => {
            let (body, outcome_name) = match outcome {
                CanonicalDeltaSuccess::UpToDate => (Bytes::new(), "up-to-date"),
                CanonicalDeltaSuccess::WaitForWakeup => (Bytes::new(), "wait-for-wakeup"),
                CanonicalDeltaSuccess::Complete(candidate) => (candidate.gzip_body().clone(), "complete"),
                CanonicalDeltaSuccess::Prefix(candidate) => (candidate.gzip_body().clone(), "prefix"),
            };
            let body_bearing = !body.is_empty();
            let mut response = response(
                StatusCode::OK,
                body,
                body_bearing.then_some("application/json"),
                "no-cache",
                true,
                body_bearing,
                Some(admitted),
            );
            insert_header(response.headers_mut(), KGI_HEAD_REVISION_ID, &head.revision().to_string());
            insert_header(response.headers_mut(), KGI_HEAD_REVISION_TIMESTAMP_MS, &head.revision_timestamp_ms().to_string());
            insert_header(response.headers_mut(), KGI_DELTA_OUTCOME, outcome_name);
            insert_header(
                response.headers_mut(),
                KGI_DELTA_CONTINUATION,
                match continuation {
                    DeltaContinuation::ReachedHead => "head",
                    DeltaContinuation::ContinueImmediately => "continue",
                    DeltaContinuation::WaitForWakeup => "wakeup",
                },
            );
            if let Some(wake_at_revision_id) = wake_at_revision_id {
                insert_header(response.headers_mut(), KGI_DELTA_WAKE_AT_REVISION, &wake_at_revision_id.to_string());
            }
            if let Some(retry_after_ms) = retry_after_ms {
                insert_header(response.headers_mut(), KGI_DELTA_RETRY_AFTER_MS, &retry_after_ms.to_string());
            }
            add_publication_headers(&mut response, &publication).await;
            response
        }
        Err(error) => cache_wait_error(error, admitted),
    }
}

async fn head_levels(State(service): State<Arc<ApiService>>, request: Request<Body>) -> Response<Body> {
    let Some(query) = HeadLevelsQuery::parse(request.uri().query()) else { return invalid_request(true) };
    if request_has_body(&request) {
        return invalid_request(true);
    }
    if !accepts_gzip(request.headers()) {
        return gzip_required();
    }
    let admitted = match service.admit_head_request() {
        Ok(request) => request,
        Err(error) => return admission_error(error, true),
    };
    match service.head_levels(query.publication_id, &query.levels, &admitted).await {
        Ok(HeadLevelLookupResponse::Complete { publication, body }) => {
            let mut response = gzip_graph_response(body.into_gzip_body(), "no-store", admitted);
            add_publication_headers(&mut response, &publication).await;
            response
        }
        Ok(HeadLevelLookupResponse::LevelUnavailable { publication, requested_level }) => {
            let mut response =
                error_response(StatusCode::NOT_FOUND, ApiErrorDto::head_level_unavailable(requested_level), true, Some(admitted));
            add_publication_headers(&mut response, &publication).await;
            response
        }
        Ok(HeadLevelLookupResponse::FreshViewRequired { publication, reason }) => {
            let mut response = error_response(StatusCode::CONFLICT, ApiErrorDto::fresh_view_required(reason), true, Some(admitted));
            if let Some(publication) = publication {
                add_publication_headers(&mut response, &publication).await;
            }
            response
        }
        Err(error) => encoding_error(error, admitted),
    }
}

async fn anchored_window(State(service): State<Arc<ApiService>>, request: Request<Body>) -> Response<Body> {
    let Some(query) = AnchoredWindowQuery::parse(request.uri().query()) else { return invalid_request(true) };
    if request_has_body(&request) {
        return invalid_request(true);
    }
    if !accepts_gzip(request.headers()) {
        return gzip_required();
    }
    let admitted = match service.admit_head_request() {
        Ok(request) => request,
        Err(error) => return admission_error(error, true),
    };
    match service.anchored_window(&admitted, query.anchor, query.max_depth).await {
        Ok(AnchoredWindowResponse::Unavailable) => unavailable(true, Some(admitted)),
        Ok(AnchoredWindowResponse::Head { publication, body }) | Ok(AnchoredWindowResponse::Database { publication, body }) => {
            let mut response = gzip_graph_response(body.into_gzip_body(), "no-store", admitted);
            add_publication_headers(&mut response, &publication).await;
            response
        }
        Ok(AnchoredWindowResponse::AnchorUnavailable { publication, unavailable }) => {
            let mut response =
                error_response(StatusCode::NOT_FOUND, ApiErrorDto::window_anchor_unavailable(unavailable), true, Some(admitted));
            add_publication_headers(&mut response, &publication).await;
            response
        }
        Err(error) => anchored_window_error(error, admitted),
    }
}

async fn publication_wakeups(State(service): State<Arc<ApiService>>, request: Request<Body>) -> Response<Body> {
    let Some(_query) = PublicationWakeupQuery::parse(request.uri().query()) else { return invalid_request(false) };
    if request_has_body(&request) {
        return invalid_request(false);
    }
    match service.open_sse_connection() {
        Ok(connection) => sse::response(connection),
        Err(SseConnectionOpenError::Admission(error)) => admission_error(error, false),
        Err(SseConnectionOpenError::Unavailable) => unavailable(false, None),
        Err(SseConnectionOpenError::RegistrationFailed) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, ApiErrorDto::internal_error(), false, None)
        }
    }
}

async fn add_publication_headers(response: &mut Response<Body>, publication: &GraphPublication) {
    insert_header(response.headers_mut(), KGI_PUBLICATION_ID, &publication.publication_id().to_string());
    insert_header(
        response.headers_mut(),
        KGI_PUBLICATION_STATE,
        match publication.state().await {
            GraphPublicationState::Synchronizing => "synchronizing",
            GraphPublicationState::Live => "live",
            GraphPublicationState::Stale => "stale",
        },
    );
}

fn request_has_body(request: &Request<Body>) -> bool {
    request.body().size_hint().exact() != Some(0)
}

fn gzip_graph_response(body: Bytes, cache_control: &'static str, request: ApiRequestGuard) -> Response<Body> {
    response(StatusCode::OK, body, Some("application/json"), cache_control, true, true, Some(request))
}

fn invalid_request(graph: bool) -> Response<Body> {
    error_response(StatusCode::BAD_REQUEST, ApiErrorDto::invalid_request(), graph, None)
}

fn invalid_request_guarded(request: ApiRequestGuard) -> Response<Body> {
    error_response(StatusCode::BAD_REQUEST, ApiErrorDto::invalid_request(), true, Some(request))
}

fn gzip_required() -> Response<Body> {
    error_response(StatusCode::NOT_ACCEPTABLE, ApiErrorDto::gzip_required(), true, None)
}

fn unavailable(graph: bool, request: Option<ApiRequestGuard>) -> Response<Body> {
    let mut response = error_response(StatusCode::SERVICE_UNAVAILABLE, ApiErrorDto::service_unavailable(), graph, request);
    response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

fn admission_error(error: ApiRequestAdmissionError, graph: bool) -> Response<Body> {
    match error {
        ApiRequestAdmissionError::Saturated => {
            let mut response = error_response(StatusCode::TOO_MANY_REQUESTS, ApiErrorDto::capacity_exhausted(), graph, None);
            response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        ApiRequestAdmissionError::Unavailable | ApiRequestAdmissionError::Cancelled => unavailable(graph, None),
    }
}

fn cache_wait_error(error: CacheWaitError, request: ApiRequestGuard) -> Response<Body> {
    match error {
        CacheWaitError::Cancelled
        | CacheWaitError::InvalidatedByReplayFloor { .. }
        | CacheWaitError::Build(CacheBuildError::Cancelled) => unavailable(true, Some(request)),
        CacheWaitError::Build(CacheBuildError::Encoding(error)) => encoding_error(error, request),
    }
}

fn encoding_error(error: EncodingSchedulerError, request: ApiRequestGuard) -> Response<Body> {
    match error {
        EncodingSchedulerError::AdmissionSaturated => {
            let mut response = error_response(StatusCode::TOO_MANY_REQUESTS, ApiErrorDto::capacity_exhausted(), true, Some(request));
            response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        EncodingSchedulerError::QueueTimeout => {
            let mut response = error_response(StatusCode::SERVICE_UNAVAILABLE, ApiErrorDto::encoding_timeout(), true, Some(request));
            response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        EncodingSchedulerError::Cancelled | EncodingSchedulerError::ShuttingDown => unavailable(true, Some(request)),
        EncodingSchedulerError::Encoding(GraphEncodingError::UncompressedResponseTooLarge { actual_uncompressed_bytes }) => {
            error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                ApiErrorDto::graph_response_too_large(
                    MAX_UNCOMPRESSED_NON_DELTA_GRAPH_RESPONSE_BYTES as u64,
                    actual_uncompressed_bytes,
                ),
                true,
                Some(request),
            )
        }
        EncodingSchedulerError::Encoding(GraphEncodingError::Serialization | GraphEncodingError::Compression)
        | EncodingSchedulerError::BlockingExecutorFailed
        | EncodingSchedulerError::WorkerFailed => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, ApiErrorDto::internal_error(), true, Some(request))
        }
    }
}

fn anchored_window_error(error: AnchoredWindowError, request: ApiRequestGuard) -> Response<Body> {
    match error {
        AnchoredWindowError::Admission(error) => match error {
            ApiRequestAdmissionError::Saturated => {
                let mut response =
                    error_response(StatusCode::TOO_MANY_REQUESTS, ApiErrorDto::capacity_exhausted(), true, Some(request));
                response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
                response
            }
            ApiRequestAdmissionError::Unavailable | ApiRequestAdmissionError::Cancelled => unavailable(true, Some(request)),
        },
        AnchoredWindowError::DatabaseAdmissionSaturated => {
            let mut response = error_response(StatusCode::TOO_MANY_REQUESTS, ApiErrorDto::capacity_exhausted(), true, Some(request));
            response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        AnchoredWindowError::Cancelled => unavailable(true, Some(request)),
        AnchoredWindowError::Database(ApiReadError::GenerationLost) => unavailable(true, Some(request)),
        AnchoredWindowError::Database(ApiReadError::Cancelled) => unavailable(true, Some(request)),
        AnchoredWindowError::Database(ApiReadError::QueryFailed | ApiReadError::InconsistentProjection)
        | AnchoredWindowError::DatabaseTimeout
        | AnchoredWindowError::DatabaseWaiterFailed => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, ApiErrorDto::internal_error(), true, Some(request))
        }
        AnchoredWindowError::Encoding(error) => encoding_error(error, request),
    }
}

fn error_response(status: StatusCode, error: ApiErrorDto, graph: bool, request: Option<ApiRequestGuard>) -> Response<Body> {
    let body = error.encode().expect("the closed API error DTO serializes");
    response(status, Bytes::from(body), Some("application/json"), "no-store", graph, false, request)
}

fn response(
    status: StatusCode,
    bytes: Bytes,
    content_type: Option<&'static str>,
    cache_control: &'static str,
    graph: bool,
    gzip: bool,
    request: Option<ApiRequestGuard>,
) -> Response<Body> {
    let body = match request {
        Some(request) => Body::new(GuardedBody::new(bytes, request, gzip)),
        None => Body::from(bytes),
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache_control));
    if let Some(content_type) = content_type {
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    }
    if graph {
        headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    }
    if gzip {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    response
}

async fn graph_method_not_allowed() -> Response<Body> {
    let mut response = error_response(StatusCode::METHOD_NOT_ALLOWED, ApiErrorDto::method_not_allowed(), true, None);
    response.headers_mut().insert(header::ALLOW, HeaderValue::from_static("GET"));
    response
}

async fn method_not_allowed() -> Response<Body> {
    let mut response = error_response(StatusCode::METHOD_NOT_ALLOWED, ApiErrorDto::method_not_allowed(), false, None);
    response.headers_mut().insert(header::ALLOW, HeaderValue::from_static("GET"));
    response
}

async fn route_not_found() -> Response<Body> {
    error_response(StatusCode::NOT_FOUND, ApiErrorDto::route_not_found(), false, None)
}

fn insert_header(headers: &mut HeaderMap, name: HeaderName, value: &str) {
    headers.insert(name, HeaderValue::from_str(value).expect("protocol header values are valid ASCII"));
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        future::poll_fn,
        io::Read,
        pin::Pin,
        sync::Arc,
        time::Instant,
    };

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use flate2::read::GzDecoder;
    use http_body::Body as _;
    use kgi_api_model::{
        graph::{GraphView, Level, LevelExtent, TrackingPolicy},
        history::GraphHistory,
    };
    use kgi_model::lifecycle::{
        NodeServiceStatus, NodeServiceStatusState, ProcessingState, ProcessingStatus, StorageServiceStatus, StorageServiceStatusState,
        SupervisorLifecycle, SupervisorStatus,
    };
    use rand::rngs::mock::StepRng;
    use serde_json::Value;
    use tokio::sync::watch;
    use tower::ServiceExt;

    use crate::{
        client::{ClientIdentifier, DeltaCompletionResult, DeltaScheduleAction},
        publication::{GraphPublication, GraphPublicationState},
        service::ApiService,
    };

    use super::{KGI_DELTA_CONTINUATION, KGI_DELTA_OUTCOME, KGI_PUBLICATION_ID, KGI_PUBLICATION_STATE, router};

    fn service() -> Arc<ApiService> {
        let (_, supervisor) = watch::channel(SupervisorStatus {
            lifecycle: SupervisorLifecycle::Running,
            desired_recovery: None,
            active_recovery: None,
        });
        let (_, node) = watch::channel(NodeServiceStatus { state: NodeServiceStatusState::Connecting, last_validated: None });
        let (_, storage) = watch::channel(StorageServiceStatus { state: StorageServiceStatusState::Connecting });
        let (_, processing) = watch::channel(ProcessingStatus { state: ProcessingState::Idle });
        ApiService::new(supervisor, node, storage, processing).0
    }

    fn publication() -> Arc<GraphPublication> {
        let view = GraphView::from_database_projection(
            250,
            LevelExtent::new(1, 2).expect("valid extent"),
            TrackingPolicy::Head,
            BTreeMap::from([(1, Level { size: 1, daa_score: Some(10) }), (2, Level { size: 2, daa_score: Some(20) })]),
            HashMap::new(),
            HashMap::new(),
        );
        Arc::new(
            GraphPublication::new(GraphPublicationState::Synchronizing, Instant::now(), view, GraphHistory::new())
                .expect("coherent publication"),
        )
    }

    fn request(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).header(header::ACCEPT_ENCODING, "gzip").body(Body::empty()).expect("valid request")
    }

    async fn json_body(response: axum::response::Response) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.expect("response body completes");
        serde_json::from_slice(&body).expect("response is JSON")
    }

    async fn gzip_json_body(response: axum::response::Response) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.expect("response body completes");
        let mut json = Vec::new();
        GzDecoder::new(body.as_ref()).read_to_end(&mut json).expect("gzip body decodes");
        serde_json::from_slice(&json).expect("response is JSON")
    }

    async fn next_sse_frame(body: &mut Body) -> Option<String> {
        let frame = poll_fn(|context| Pin::new(&mut *body).poll_frame(context)).await?;
        let data = frame.expect("SSE body frame succeeds").into_data().expect("SSE emits data frames");
        Some(String::from_utf8(data.to_vec()).expect("SSE frames are UTF-8"))
    }

    fn sse_payload(frame: &str, event: &str) -> Value {
        let prefix = format!("event: {event}\ndata: ");
        let data = frame.strip_prefix(&prefix).and_then(|frame| frame.strip_suffix("\n\n")).expect("exact SSE frame shape");
        serde_json::from_str(data).expect("SSE data is compact JSON")
    }

    #[tokio::test]
    async fn status_route_is_independent_and_api_fallbacks_remain_typed() {
        let application = router().with_state(service());

        let status = application.clone().oneshot(request("/status")).await.expect("status routed");
        assert_eq!(status.status(), StatusCode::OK);
        assert_eq!(status.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(json_body(status).await["supervisor"]["lifecycle"], "running");

        let unknown = application.clone().oneshot(request("/unknown")).await.expect("fallback routed");
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        assert_eq!(json_body(unknown).await["code"], "route-not-found");

        let method = application
            .oneshot(Request::builder().method("POST").uri("/status").body(Body::empty()).expect("valid request"))
            .await
            .expect("method fallback routed");
        assert_eq!(method.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(method.headers()[header::ALLOW], "GET");

        let head_method = router()
            .with_state(service())
            .oneshot(Request::builder().method("HEAD").uri("/status").body(Body::empty()).expect("valid request"))
            .await
            .expect("HEAD method fallback routed");
        assert_eq!(head_method.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(head_method.headers()[header::ALLOW], "GET");
    }

    #[tokio::test]
    async fn graph_validation_precedes_gzip_and_graph_work() {
        let application = router().with_state(service());

        let malformed = application
            .clone()
            .oneshot(Request::builder().uri("/graph/head?target_depth=0").body(Body::empty()).expect("valid request"))
            .await
            .expect("request routed");
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(malformed.headers()[header::VARY], "Accept-Encoding");

        let missing_gzip = application
            .clone()
            .oneshot(Request::builder().uri("/graph/head?target_depth=20").body(Body::empty()).expect("valid request"))
            .await
            .expect("request routed");
        assert_eq!(missing_gzip.status(), StatusCode::NOT_ACCEPTABLE);
        assert_eq!(json_body(missing_gzip).await["code"], "gzip-required");

        let method = application
            .oneshot(Request::builder().method("POST").uri("/graph/head?target_depth=20").body(Body::empty()).expect("valid request"))
            .await
            .expect("method routed");
        assert_eq!(method.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(method.headers()[header::ALLOW], "GET");
    }

    #[tokio::test]
    async fn ordinary_graph_routes_map_complete_logical_operations() {
        let service = service();
        let publication = publication();
        let publication_id = publication.publication_id();
        service.install_test_publication(publication);
        let application = router().with_state(service);

        let head = application.clone().oneshot(request("/graph/head?target_depth=20")).await.expect("Head routed");
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(head.headers()[header::CONTENT_ENCODING], "gzip");
        assert_eq!(head.headers()[KGI_PUBLICATION_ID], publication_id.to_string());
        assert_eq!(head.headers()[KGI_PUBLICATION_STATE], "synchronizing");
        let etag = head.headers()[header::ETAG].clone();
        assert_eq!(gzip_json_body(head).await["revision"], "0");

        let not_modified = application
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/graph/head?target_depth=20")
                    .header(header::ACCEPT_ENCODING, "gzip")
                    .header(header::IF_NONE_MATCH, etag)
                    .body(Body::empty())
                    .expect("valid conditional request"),
            )
            .await
            .expect("conditional Head routed");
        assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(not_modified.headers()[header::CACHE_CONTROL], "no-cache");
        assert!(!not_modified.headers().contains_key(header::CONTENT_ENCODING));
        assert!(to_bytes(not_modified.into_body(), usize::MAX).await.expect("empty 304 completes").is_empty());

        let delta = application
            .clone()
            .oneshot(request(&format!("/graph/deltas?publication_id={publication_id}&from_revision_id=0")))
            .await
            .expect("delta routed");
        assert_eq!(delta.status(), StatusCode::OK);
        assert_eq!(delta.headers()[KGI_DELTA_OUTCOME], "up-to-date");
        assert_eq!(delta.headers()[KGI_DELTA_CONTINUATION], "head");
        assert!(!delta.headers().contains_key(header::CONTENT_ENCODING));
        assert!(to_bytes(delta.into_body(), usize::MAX).await.expect("empty delta completes").is_empty());

        let levels = application
            .clone()
            .oneshot(request(&format!("/graph/levels?publication_id={publication_id}&level=2&level=1&level=1")))
            .await
            .expect("levels routed");
        assert_eq!(levels.status(), StatusCode::OK);
        assert_eq!(gzip_json_body(levels).await["levels"].as_array().expect("levels array").len(), 2);

        let window = application.oneshot(request("/graph/window?max_depth=2&level=1")).await.expect("window routed");
        assert_eq!(window.status(), StatusCode::OK);
        assert_eq!(gzip_json_body(window).await["resolution"]["resolved_level"], 1);
    }

    #[tokio::test]
    async fn duplicate_scalars_unknown_parameters_and_request_bodies_are_rejected() {
        let application = router().with_state(service());
        for request in [
            request("/graph/head?target_depth=20&target_depth=20"),
            request("/graph/head?target_depth=20&depth=20"),
            Request::builder().uri("/status").body(Body::from("unexpected")).expect("valid request"),
        ] {
            let response = application.clone().oneshot(request).await.expect("request routed");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn typed_graph_failures_preserve_headers_and_publication_context() {
        let unavailable_application = router().with_state(service());
        let unavailable =
            unavailable_application.oneshot(request("/graph/head?target_depth=20")).await.expect("unavailable Head routed");
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable.headers()[header::RETRY_AFTER], "1");
        assert!(!unavailable.headers().contains_key(KGI_PUBLICATION_ID));

        let service = service();
        let publication = publication();
        let publication_id = publication.publication_id();
        service.install_test_publication(publication);
        let application = router().with_state(service);

        let mismatch = application
            .clone()
            .oneshot(request(&format!("/graph/deltas?publication_id={}&from_revision_id=0", publication_id.wrapping_add(1))))
            .await
            .expect("mismatched delta routed");
        assert_eq!(mismatch.status(), StatusCode::CONFLICT);
        assert_eq!(mismatch.headers()[KGI_PUBLICATION_ID], publication_id.to_string());
        assert_eq!(json_body(mismatch).await["details"]["reason"], "publication-mismatch");

        let future = application
            .clone()
            .oneshot(request(&format!("/graph/deltas?publication_id={publication_id}&from_revision_id=1")))
            .await
            .expect("future delta routed");
        assert_eq!(future.status(), StatusCode::BAD_REQUEST);
        assert_eq!(future.headers()[KGI_PUBLICATION_ID], publication_id.to_string());

        let mut random = StepRng::new(7, 0);
        let absent_client = ClientIdentifier::generate(&mut random).encode();
        let registration = application
            .clone()
            .oneshot(request(&format!("/graph/deltas?publication_id={publication_id}&from_revision_id=0&client_id={absent_client}")))
            .await
            .expect("registration failure routed");
        assert_eq!(registration.status(), StatusCode::CONFLICT);
        assert_eq!(json_body(registration).await["code"], "client-registration-required");

        let missing_level = application
            .oneshot(request(&format!("/graph/levels?publication_id={publication_id}&level=3")))
            .await
            .expect("missing level routed");
        assert_eq!(missing_level.status(), StatusCode::NOT_FOUND);
        assert_eq!(json_body(missing_level).await["details"]["requested_level"], 3);
    }

    #[tokio::test]
    async fn sse_stream_reports_initial_and_replacement_publications_in_order() {
        let service = service();
        let initial = publication();
        let initial_id = initial.publication_id();
        service.install_test_publication(initial.clone());
        let application = router().with_state(service.clone());

        let response = application
            .oneshot(
                Request::builder()
                    .uri("/graph/wakeups?publication_id=999&from_revision_id=123")
                    .header("last-event-id", "not-a-graph-cursor")
                    .body(Body::empty())
                    .expect("valid SSE request"),
            )
            .await
            .expect("SSE request routed");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "text/event-stream");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert!(!response.headers().contains_key(header::CONTENT_ENCODING));
        assert!(!response.headers().contains_key(header::ETAG));
        assert!(!response.headers().contains_key(header::VARY));

        let mut body = response.into_body();
        let registration_frame = next_sse_frame(&mut body).await.expect("initial registration frame");
        let initial_registration = sse_payload(&registration_frame, "client-registration");
        let initial_client = initial_registration["client_id"].as_str().expect("client identifier string");
        assert_eq!(initial_client.len(), 11);
        let initial_identifier = ClientIdentifier::parse(initial_client).expect("canonical client identifier");

        let state = sse_payload(&next_sse_frame(&mut body).await.expect("initial state frame"), "publication-state");
        assert_eq!(
            state,
            serde_json::json!({
                "publication_id": initial_id.to_string(),
                "revision": "0",
                "state": "synchronizing",
            })
        );
        let wakeup = sse_payload(&next_sse_frame(&mut body).await.expect("initial wakeup frame"), "publication-wakeup");
        assert_eq!(wakeup, serde_json::json!({ "publication_id": initial_id.to_string(), "revision": "0" }));

        assert_eq!(
            initial
                .client_registry()
                .complete_delta(initial_identifier, DeltaScheduleAction::Replace { wake_at_revision_id: 1, wake_sent: false },),
            DeltaCompletionResult::Accepted
        );
        initial.client_registry().advance_head(1);

        let replacement = publication();
        let replacement_id = replacement.publication_id();
        assert_ne!(replacement_id, initial_id);
        service.install_test_publication(replacement.clone());

        let registration_frame = next_sse_frame(&mut body).await.expect("replacement registration frame");
        let replacement_registration = sse_payload(&registration_frame, "client-registration");
        let replacement_client = replacement_registration["client_id"].as_str().expect("replacement identifier string");
        assert_ne!(replacement_client, initial_client);
        let replacement_identifier = ClientIdentifier::parse(replacement_client).expect("canonical replacement identifier");
        let state = sse_payload(&next_sse_frame(&mut body).await.expect("replacement state frame"), "publication-state");
        assert_eq!(state["publication_id"], replacement_id.to_string());
        let wakeup = sse_payload(&next_sse_frame(&mut body).await.expect("replacement wakeup frame"), "publication-wakeup");
        assert_eq!(wakeup["publication_id"], replacement_id.to_string());
        assert!(!initial.client_registry().validate(initial_identifier));
        assert!(replacement.client_registry().validate(replacement_identifier));

        drop(body);
        assert!(!replacement.client_registry().validate(replacement_identifier));
    }

    #[tokio::test]
    async fn sse_state_delivery_slow_client_disconnect_and_shutdown_are_bounded() {
        let active_service = service();
        let active_publication = publication();
        active_service.install_test_publication(active_publication.clone());
        let application = router().with_state(active_service.clone());
        let response = application
            .oneshot(
                Request::builder()
                    .uri("/graph/wakeups?publication_id=1&from_revision_id=0")
                    .body(Body::empty())
                    .expect("valid SSE request"),
            )
            .await
            .expect("SSE request routed");
        let mut body = response.into_body();
        for _ in 0..3 {
            next_sse_frame(&mut body).await.expect("initial sequence frame");
        }

        active_publication.transition_live().await;
        let live = sse_payload(&next_sse_frame(&mut body).await.expect("Live state frame"), "publication-state");
        assert_eq!(live["state"], "live");
        assert!(active_publication.transition_stale().await);
        let stale = sse_payload(&next_sse_frame(&mut body).await.expect("Stale state frame"), "publication-state");
        assert_eq!(stale["state"], "stale");

        let shutdown_service = active_service.clone();
        let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished(), "shutdown retains the live SSE completion guard");
        assert!(next_sse_frame(&mut body).await.is_none());
        assert_eq!(shutdown.await.expect("shutdown task completes"), Ok(()));

        let slow_service = service();
        let slow_publication = publication();
        slow_service.install_test_publication(slow_publication.clone());
        let slow_response = router()
            .with_state(slow_service)
            .oneshot(
                Request::builder()
                    .uri("/graph/wakeups?publication_id=1&from_revision_id=0")
                    .body(Body::empty())
                    .expect("valid slow-client request"),
            )
            .await
            .expect("slow SSE request routed");
        let mut slow_body = slow_response.into_body();
        for _ in 0..=crate::client::SSE_CLIENT_BUFFER_CAPACITY {
            slow_publication.client_registry().publish_state(GraphPublicationState::Synchronizing, 0);
        }
        assert!(next_sse_frame(&mut slow_body).await.is_none(), "a saturated required-message mailbox disconnects the client");
    }

    #[tokio::test]
    async fn sse_request_validation_precedes_connection_work() {
        let application = router().with_state(service());
        for request in [
            Request::builder().uri("/graph/wakeups?publication_id=1").body(Body::empty()).expect("missing cursor request"),
            Request::builder()
                .uri("/graph/wakeups?publication_id=1&from_revision_id=0&client_id=bad")
                .body(Body::empty())
                .expect("extra parameter request"),
            Request::builder()
                .uri("/graph/wakeups?publication_id=1&from_revision_id=0")
                .body(Body::from("unexpected"))
                .expect("body-bearing request"),
        ] {
            let response = application.clone().oneshot(request).await.expect("invalid SSE request routed");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(!response.headers().contains_key(header::VARY));
        }

        let unavailable = application
            .oneshot(
                Request::builder()
                    .uri("/graph/wakeups?publication_id=1&from_revision_id=0")
                    .body(Body::empty())
                    .expect("well-formed unavailable request"),
            )
            .await
            .expect("unavailable SSE request routed");
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable.headers()[header::RETRY_AFTER], "1");

        let method = router()
            .with_state(service())
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/graph/wakeups?publication_id=1&from_revision_id=0")
                    .body(Body::empty())
                    .expect("valid method request"),
            )
            .await
            .expect("SSE method fallback routed");
        assert_eq!(method.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(method.headers()[header::ALLOW], "GET");
        assert!(!method.headers().contains_key(header::VARY));
    }

    #[tokio::test]
    async fn sse_connection_lane_rejects_only_after_its_exact_bound() {
        let service = service();
        service.install_test_publication(publication());
        let application = router().with_state(service);
        let mut bodies = Vec::with_capacity(crate::client::MAX_SSE_CLIENTS);
        for cursor in 0..crate::client::MAX_SSE_CLIENTS {
            let response = application
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/graph/wakeups?publication_id=1&from_revision_id={cursor}"))
                        .body(Body::empty())
                        .expect("valid bounded SSE request"),
                )
                .await
                .expect("bounded SSE request routed");
            assert_eq!(response.status(), StatusCode::OK);
            bodies.push(response.into_body());
        }

        let saturated = application
            .oneshot(
                Request::builder()
                    .uri("/graph/wakeups?publication_id=1&from_revision_id=0")
                    .body(Body::empty())
                    .expect("valid saturated SSE request"),
            )
            .await
            .expect("saturated SSE request routed");
        assert_eq!(saturated.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(saturated.headers()[header::RETRY_AFTER], "1");
        drop(bodies);
    }
}
