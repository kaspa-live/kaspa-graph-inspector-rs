use std::{
    convert::Infallible,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker, ready},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, HttpBody},
    http::{Method, Request, Response},
};
use http_body::Frame;
use hyper::{body::Incoming, server::conn::http1};
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use pin_project_lite::pin_project;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::oneshot,
    time::sleep,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower::Service;

use crate::admission::ApiRequestGuard;

/// Complete delivery deadline for one ordinary graph HTTP response.
pub(super) const GRAPH_HTTP_DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct PendingDeliverySlot(Arc<Mutex<Option<PendingDelivery>>>);

enum PendingDelivery {
    Request { guard: ApiRequestGuard, timeout: Option<Duration> },
    Sse { cancellation: CancellationToken },
}

impl PendingDeliverySlot {
    fn new(delivery: PendingDelivery) -> Self {
        Self(Arc::new(Mutex::new(Some(delivery))))
    }

    fn take(self) -> Option<PendingDelivery> {
        self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take()
    }
}

/// Marks one admitted ordinary response for socket-boundary delivery tracking.
pub(super) fn track_request(response: &mut Response<Body>, guard: ApiRequestGuard, delivery_timeout: bool) {
    let timeout = delivery_timeout.then_some(GRAPH_HTTP_DELIVERY_TIMEOUT);
    track_request_with_timeout(response, guard, timeout);
}

fn track_request_with_timeout(response: &mut Response<Body>, guard: ApiRequestGuard, timeout: Option<Duration>) {
    response.extensions_mut().insert(PendingDeliverySlot::new(PendingDelivery::Request { guard, timeout }));
}

/// Marks one SSE response for connection-observed cancellation.
pub(super) fn track_sse(response: &mut Response<Body>, cancellation: CancellationToken) {
    response.extensions_mut().insert(PendingDeliverySlot::new(PendingDelivery::Sse { cancellation }));
}

struct DeliveryState {
    inner: Mutex<DeliveryStateInner>,
    abort: CancellationToken,
    tasks: TaskTracker,
}

struct DeliveryStateInner {
    occupied: bool,
    closed: bool,
    next_id: u64,
    flush_generation: u64,
    active: Option<ActiveDelivery>,
    ready_wakers: Vec<Waker>,
}

struct ActiveDelivery {
    id: u64,
    body_complete: bool,
    expected_body_bytes: Option<u64>,
    written_body_bytes: u64,
    header_match: usize,
    headers_complete: bool,
    required_flush_generation: Option<u64>,
    result: Option<oneshot::Sender<DeliveryResult>>,
}

#[derive(Clone, Copy)]
enum DeliveryResult {
    Flushed,
    Failed,
}

impl DeliveryState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(DeliveryStateInner {
                occupied: false,
                closed: false,
                next_id: 0,
                flush_generation: 0,
                active: None,
                ready_wakers: Vec::new(),
            }),
            abort: CancellationToken::new(),
            tasks: TaskTracker::new(),
        })
    }

    fn poll_reserve(&self, context: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.closed {
            return Poll::Ready(());
        }
        if !inner.occupied {
            inner.occupied = true;
            return Poll::Ready(());
        }
        if !inner.ready_wakers.iter().any(|waker| waker.will_wake(context.waker())) {
            inner.ready_wakers.push(context.waker().clone());
        }
        Poll::Pending
    }

    fn install_response(self: &Arc<Self>, mut response: Response<Body>, suppress_body: bool) -> Response<Body> {
        let pending = response.extensions_mut().remove::<PendingDeliverySlot>().and_then(PendingDeliverySlot::take);
        let body_complete = suppress_body || response.body().is_end_stream();
        let expected_body_bytes = suppress_body.then_some(0).or_else(|| response.body().size_hint().exact());
        let (result_sender, result_receiver) = if pending.is_some() {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };

        let id = {
            let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(inner.occupied, "a response is installed only after reserving its connection boundary");
            assert!(inner.active.is_none(), "HTTP/1.1 response delivery remains serialized per connection");
            inner.next_id = inner.next_id.checked_add(1).expect("HTTP/1.1 response delivery identifiers fit u64");
            let id = inner.next_id;
            inner.active = Some(ActiveDelivery {
                id,
                body_complete,
                expected_body_bytes,
                written_body_bytes: 0,
                header_match: 0,
                headers_complete: false,
                required_flush_generation: None,
                result: result_sender,
            });
            id
        };

        if let (Some(pending), Some(result)) = (pending, result_receiver) {
            let state = self.clone();
            let _task = self.tasks.spawn(async move {
                match pending {
                    PendingDelivery::Request { guard, timeout } => watch_request_delivery(state, result, guard, timeout).await,
                    PendingDelivery::Sse { cancellation } => watch_sse_delivery(state, result, cancellation).await,
                }
            });
        }

        let (parts, body) = response.into_parts();
        Response::from_parts(parts, Body::new(DeliveryBody::new(body, self.clone(), id, body_complete)))
    }

    fn body_complete(&self, id: u64) {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let next_flush_generation = inner.flush_generation.checked_add(1).expect("HTTP/1.1 socket flush generations fit u64");
        if let Some(active) = inner.active.as_mut()
            && active.id == id
        {
            active.body_complete = true;
            if active.headers_complete && active.expected_body_bytes.is_some_and(|expected| active.written_body_bytes >= expected) {
                active.required_flush_generation = Some(next_flush_generation);
            } else if active.expected_body_bytes.is_none() {
                drop(inner);
                self.request_abort();
            }
        }
    }

    fn bytes_written(&self, bytes: &[u8]) {
        const HEADER_BOUNDARY: &[u8; 4] = b"\r\n\r\n";

        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let next_flush_generation = inner.flush_generation.checked_add(1).expect("HTTP/1.1 socket flush generations fit u64");
        let Some(active) = inner.active.as_mut() else { return };
        let mut offset = 0;
        if !active.headers_complete {
            while let Some(&byte) = bytes.get(offset) {
                if byte == HEADER_BOUNDARY[active.header_match] {
                    active.header_match += 1;
                    if active.header_match == HEADER_BOUNDARY.len() {
                        active.headers_complete = true;
                        offset += 1;
                        break;
                    }
                } else {
                    active.header_match = usize::from(byte == HEADER_BOUNDARY[0]);
                }
                offset += 1;
            }
        }
        if active.headers_complete {
            active.written_body_bytes = active
                .written_body_bytes
                .checked_add(u64::try_from(bytes.len() - offset).expect("accepted write length fits u64"))
                .expect("HTTP response body byte count fits u64");
            if active.body_complete && active.expected_body_bytes.is_some_and(|expected| active.written_body_bytes >= expected) {
                active.required_flush_generation = Some(next_flush_generation);
            }
        }
    }

    fn body_failed(&self, id: u64) {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.active.as_ref().is_some_and(|active| active.id == id) {
            drop(inner);
            self.request_abort();
        }
    }

    fn flush_completed(&self) {
        let (result, wakers) = {
            let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.flush_generation = inner.flush_generation.checked_add(1).expect("HTTP/1.1 socket flush generations fit u64");
            if !inner
                .active
                .as_ref()
                .and_then(|active| active.required_flush_generation)
                .is_some_and(|required| required <= inner.flush_generation)
            {
                return;
            }
            let active = inner.active.take().expect("the completed active delivery exists");
            inner.occupied = false;
            (active.result, std::mem::take(&mut inner.ready_wakers))
        };
        if let Some(result) = result {
            let _ = result.send(DeliveryResult::Flushed);
        }
        for waker in wakers {
            waker.wake();
        }
    }

    fn request_abort(&self) {
        self.abort.cancel();
    }

    fn connection_closed(&self) {
        let (result, wakers) = {
            let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.closed = true;
            inner.occupied = false;
            (inner.active.take().and_then(|active| active.result), std::mem::take(&mut inner.ready_wakers))
        };
        if let Some(result) = result {
            let _ = result.send(DeliveryResult::Failed);
        }
        for waker in wakers {
            waker.wake();
        }
    }
}

async fn watch_request_delivery(
    state: Arc<DeliveryState>,
    mut result: oneshot::Receiver<DeliveryResult>,
    guard: ApiRequestGuard,
    timeout: Option<Duration>,
) {
    let cancellation = guard.cancellation().cancelled_owned();
    match timeout {
        Some(timeout) => {
            tokio::select! {
                biased;
                _ = &mut result => {}
                _ = cancellation => {
                    state.request_abort();
                    let _ = result.await;
                }
                _ = sleep(timeout) => {
                    state.request_abort();
                    let _ = result.await;
                }
            }
        }
        None => {
            tokio::select! {
                biased;
                _ = &mut result => {}
                _ = cancellation => {
                    state.request_abort();
                    let _ = result.await;
                }
            }
        }
    }
    drop(guard);
}

async fn watch_sse_delivery(
    state: Arc<DeliveryState>,
    mut result: oneshot::Receiver<DeliveryResult>,
    cancellation: CancellationToken,
) {
    tokio::select! {
        biased;
        _ = &mut result => {}
        _ = cancellation.cancelled() => {
            state.request_abort();
            let _ = result.await;
        }
    }
}

struct ResponseReservation {
    state: Arc<DeliveryState>,
    suppress_body: bool,
    active: bool,
}

impl ResponseReservation {
    fn new(state: Arc<DeliveryState>, suppress_body: bool) -> Self {
        Self { state, suppress_body, active: true }
    }

    fn install(mut self, response: Response<Body>) -> Response<Body> {
        self.active = false;
        self.state.install_response(response, self.suppress_body)
    }
}

impl Drop for ResponseReservation {
    fn drop(&mut self) {
        if self.active {
            self.state.request_abort();
        }
    }
}

struct ConnectionService {
    application: Router,
    state: Arc<DeliveryState>,
    reserved: bool,
}

impl ConnectionService {
    fn new(application: Router, state: Arc<DeliveryState>) -> Self {
        Self { application, state, reserved: false }
    }
}

impl Clone for ConnectionService {
    fn clone(&self) -> Self {
        Self { application: self.application.clone(), state: self.state.clone(), reserved: false }
    }
}

impl Service<Request<Incoming>> for ConnectionService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(<Router as Service<Request<Body>>>::poll_ready(&mut self.application, context))?;
        if !self.reserved {
            ready!(self.state.poll_reserve(context));
            self.reserved = true;
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        assert!(self.reserved, "Tower calls a connection service only after readiness");
        self.reserved = false;
        let reservation = ResponseReservation::new(self.state.clone(), request.method() == Method::HEAD);
        let future = self.application.call(request.map(Body::new));
        Box::pin(async move { future.await.map(|response| reservation.install(response)) })
    }
}

pin_project! {
    struct DeliveryBody {
        #[pin]
        inner: Body,
        state: Arc<DeliveryState>,
        id: u64,
        terminal: bool,
        remaining_data_bytes: Option<u64>,
    }

    impl PinnedDrop for DeliveryBody {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if !*this.terminal {
                this.state.body_failed(*this.id);
            }
        }
    }
}

impl DeliveryBody {
    fn new(inner: Body, state: Arc<DeliveryState>, id: u64, terminal: bool) -> Self {
        let remaining_data_bytes = inner.size_hint().exact();
        Self { inner, state, id, terminal, remaining_data_bytes }
    }
}

impl HttpBody for DeliveryBody {
    type Data = <Body as HttpBody>::Data;
    type Error = <Body as HttpBody>::Error;

    fn poll_frame(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        match this.inner.as_mut().poll_frame(context) {
            Poll::Ready(None) => {
                if !*this.terminal {
                    *this.terminal = true;
                    this.state.body_complete(*this.id);
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                *this.terminal = true;
                this.state.body_failed(*this.id);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(frame))) => {
                if let (Some(remaining), Some(data)) = (this.remaining_data_bytes.as_mut(), frame.data_ref()) {
                    *remaining = remaining
                        .checked_sub(u64::try_from(data.len()).expect("HTTP body frame length fits u64"))
                        .expect("an exact-size HTTP body does not emit more data than its size hint");
                    if *remaining == 0 {
                        *this.terminal = true;
                        this.state.body_complete(*this.id);
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.terminal || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

pin_project! {
    struct DeliveryIo<I> {
        #[pin]
        inner: I,
        state: Arc<DeliveryState>,
    }
}

impl<I> DeliveryIo<I> {
    fn new(inner: I, state: Arc<DeliveryState>) -> Self {
        Self { inner, state }
    }
}

impl<I: AsyncRead> AsyncRead for DeliveryIo<I> {
    fn poll_read(self: Pin<&mut Self>, context: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_read(context, buffer)
    }
}

impl<I: AsyncWrite> AsyncWrite for DeliveryIo<I> {
    fn poll_write(self: Pin<&mut Self>, context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.project();
        let result = this.inner.poll_write(context, buffer);
        match &result {
            Poll::Ready(Ok(written)) => this.state.bytes_written(&buffer[..*written]),
            Poll::Ready(Err(_)) => this.state.request_abort(),
            Poll::Pending => {}
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.project();
        let result = this.inner.poll_flush(context);
        match &result {
            Poll::Ready(Ok(())) => this.state.flush_completed(),
            Poll::Ready(Err(_)) => this.state.request_abort(),
            Poll::Pending => {}
        }
        result
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.project();
        let result = this.inner.poll_shutdown(context);
        if matches!(result, Poll::Ready(Err(_))) {
            this.state.request_abort();
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(self: Pin<&mut Self>, context: &mut Context<'_>, buffers: &[io::IoSlice<'_>]) -> Poll<io::Result<usize>> {
        let this = self.project();
        let result = this.inner.poll_write_vectored(context, buffers);
        match &result {
            Poll::Ready(Ok(written)) => {
                let mut written = *written;
                for buffer in buffers {
                    if written == 0 {
                        break;
                    }
                    let accepted = written.min(buffer.len());
                    this.state.bytes_written(&buffer[..accepted]);
                    written -= accepted;
                }
            }
            Poll::Ready(Err(_)) => this.state.request_abort(),
            Poll::Pending => {}
        }
        result
    }
}

struct ConnectionLifetime {
    state: Arc<DeliveryState>,
    closed: bool,
}

impl ConnectionLifetime {
    fn new(state: Arc<DeliveryState>) -> Self {
        Self { state, closed: false }
    }

    fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.state.connection_closed();
        self.state.tasks.close();
    }
}

impl Drop for ConnectionLifetime {
    fn drop(&mut self) {
        self.close();
    }
}

/// Serves one accepted socket through the reusable HTTP/1.1 delivery adapter.
pub(crate) async fn serve<I>(io: I, application: Router) -> Result<(), hyper::Error>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let state = DeliveryState::new();
    let mut lifetime = ConnectionLifetime::new(state.clone());
    let service = TowerToHyperService::new(ConnectionService::new(application, state.clone()));
    let io = TokioIo::new(DeliveryIo::new(io, state.clone()));
    let connection_result = {
        let connection = http1::Builder::new().serve_connection(io, service);
        tokio::pin!(connection);
        tokio::select! {
            biased;
            result = &mut connection => Some(result),
            _ = state.abort.cancelled() => None,
        }
    };

    lifetime.close();
    state.tasks.wait().await;
    connection_result.unwrap_or(Ok(()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
        time::Instant,
    };

    use axum::{
        Router,
        body::{Body, Bytes},
        http::{Response, StatusCode},
        routing::get,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpSocket, TcpStream},
        sync::Notify,
        task::JoinHandle,
        time::{Duration, timeout},
    };
    use tokio_util::sync::CancellationToken;

    use kgi_api_model::{
        graph::{GraphView, Level, LevelExtent, TrackingPolicy},
        history::GraphHistory,
    };
    use kgi_model::lifecycle::{
        NodeServiceStatus, NodeServiceStatusState, ProcessingState, ProcessingStatus, StorageServiceStatus, StorageServiceStatusState,
        SupervisorLifecycle, SupervisorStatus,
    };

    use crate::{
        admission::{ApiRequestAdmissionError, ApiRequestAdmissions},
        client::{ClientIdentifier, MAX_SSE_CLIENTS},
        publication::{GraphPublication, GraphPublicationState},
        service::{ApiService, SseConnectionOpenError},
    };

    use super::{serve, track_request_with_timeout};

    fn api_service() -> (Arc<ApiService>, crate::service::ApiServiceEventReceiver) {
        let (_, supervisor) = tokio::sync::watch::channel(SupervisorStatus {
            lifecycle: SupervisorLifecycle::Running,
            desired_recovery: None,
            active_recovery: None,
        });
        let (_, node) =
            tokio::sync::watch::channel(NodeServiceStatus { state: NodeServiceStatusState::Connecting, last_validated: None });
        let (_, storage) = tokio::sync::watch::channel(StorageServiceStatus { state: StorageServiceStatusState::Connecting });
        let (_, processing) = tokio::sync::watch::channel(ProcessingStatus { state: ProcessingState::Idle });
        ApiService::new(supervisor, node, storage, processing)
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

    async fn spawn_server(application: Router) -> (std::net::SocketAddr, JoinHandle<Result<(), hyper::Error>>) {
        let socket = TcpSocket::new_v4().expect("test listener socket created");
        socket.set_reuseaddr(true).expect("test listener address reuse configured");
        socket.set_send_buffer_size(1024).expect("small test send buffer configured");
        socket.bind((std::net::Ipv4Addr::LOCALHOST, 0).into()).expect("test listener binds");
        let listener = socket.listen(16).expect("test listener listens");
        let address = listener.local_addr().expect("test listener has an address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("test connection accepted");
            serve(stream, application).await
        });
        (address, server)
    }

    async fn request(stream: &mut TcpStream) {
        stream.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await.expect("test request written");
    }

    async fn request_sse(stream: &mut TcpStream) {
        stream
            .write_all(
                b"GET /graph/wakeups?publication_id=1&from_revision_id=0 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .expect("SSE request written");
    }

    async fn read_client_registration(stream: &mut TcpStream) -> ClientIdentifier {
        const PREFIX: &[u8] = b"\"client_id\":\"";

        timeout(Duration::from_secs(2), async {
            let mut response = Vec::new();
            let mut buffer = [0_u8; 1_024];
            loop {
                let read = stream.read(&mut buffer).await.expect("SSE response remains readable");
                assert_ne!(read, 0, "SSE response includes its initial registration before EOF");
                response.extend_from_slice(&buffer[..read]);
                let Some(start) = response.windows(PREFIX.len()).position(|window| window == PREFIX) else {
                    continue;
                };
                let value_start = start + PREFIX.len();
                let Some(value) = response.get(value_start..value_start + 11) else {
                    continue;
                };
                let value = std::str::from_utf8(value).expect("client identifier is UTF-8");
                return ClientIdentifier::parse(value).expect("client identifier is canonical");
            }
        })
        .await
        .expect("initial SSE registration arrives")
    }

    #[tokio::test]
    async fn successful_final_flush_releases_admission() {
        let admissions = Arc::new(ApiRequestAdmissions::new(CancellationToken::new()));
        let application = Router::new().route(
            "/",
            get({
                let admissions = admissions.clone();
                move || {
                    let request = admissions.admit_status().expect("status request admitted");
                    async move {
                        let mut response = Response::new(Body::from("complete"));
                        track_request_with_timeout(&mut response, request, None);
                        response
                    }
                }
            }),
        );
        let (address, server) = spawn_server(application).await;
        let mut client = TcpStream::connect(address).await.expect("test client connects");
        request(&mut client).await;

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("complete response read");
        assert!(response.windows(b"complete".len()).any(|window| window == b"complete"));
        timeout(Duration::from_secs(1), admissions.wait()).await.expect("final flush releases admission");
        timeout(Duration::from_secs(1), server)
            .await
            .expect("server completes")
            .expect("server task joins")
            .expect("connection succeeds");
    }

    #[tokio::test]
    async fn header_only_response_uses_the_final_flush_boundary() {
        let admissions = Arc::new(ApiRequestAdmissions::new(CancellationToken::new()));
        let application = Router::new().route(
            "/",
            get({
                let admissions = admissions.clone();
                move || {
                    let request = admissions.admit_status().expect("status request admitted");
                    async move {
                        let mut response = Response::new(Body::empty());
                        *response.status_mut() = StatusCode::NO_CONTENT;
                        track_request_with_timeout(&mut response, request, None);
                        response
                    }
                }
            }),
        );
        let (address, server) = spawn_server(application).await;
        let mut client = TcpStream::connect(address).await.expect("test client connects");
        request(&mut client).await;

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("header-only response read");
        assert!(response.starts_with(b"HTTP/1.1 204"));
        timeout(Duration::from_secs(1), admissions.wait()).await.expect("header flush releases admission");
        timeout(Duration::from_secs(1), server)
            .await
            .expect("server completes")
            .expect("server task joins")
            .expect("connection succeeds");
    }

    #[tokio::test]
    async fn stopped_reader_is_aborted_without_another_body_poll() {
        const RESPONSE_BYTES: usize = 16 * 1024 * 1024;

        let admissions = Arc::new(ApiRequestAdmissions::new(CancellationToken::new()));
        let response_started = Arc::new(Notify::new());
        let application = Router::new().route(
            "/",
            get({
                let admissions = admissions.clone();
                let response_started = response_started.clone();
                move || {
                    let request = admissions.admit_head().expect("Head request admitted");
                    let response_started = response_started.clone();
                    async move {
                        let mut response = Response::new(Body::from(Bytes::from(vec![b'x'; RESPONSE_BYTES])));
                        track_request_with_timeout(&mut response, request, Some(Duration::from_millis(100)));
                        response_started.notify_one();
                        response
                    }
                }
            }),
        );
        let (address, server) = spawn_server(application).await;
        let socket = TcpSocket::new_v4().expect("test socket created");
        socket.set_recv_buffer_size(1024).expect("small receive buffer configured");
        let mut client = socket.connect(address).await.expect("test client connects");
        request(&mut client).await;
        response_started.notified().await;

        let released_before_deadline = timeout(Duration::from_millis(20), admissions.wait()).await.is_ok();
        timeout(Duration::from_secs(2), admissions.wait()).await.expect("delivery deadline releases admission");
        timeout(Duration::from_secs(2), server)
            .await
            .expect("connection abort completes server")
            .expect("server task joins")
            .expect("delivery abort is an expected connection outcome");

        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("aborted socket reaches EOF")
            .expect("buffered response bytes remain readable");
        assert!(!released_before_deadline, "body production alone retains admission; received {} bytes", response.len());
        assert!(response.len() < RESPONSE_BYTES, "the delivery deadline truncates the response");
    }

    #[tokio::test]
    async fn shutdown_cancellation_aborts_backpressure_and_joins_delivery() {
        const RESPONSE_BYTES: usize = 16 * 1024 * 1024;

        let (service, _events) = api_service();
        let response_started = Arc::new(Notify::new());
        let application = Router::new().route(
            "/",
            get({
                let service = service.clone();
                let response_started = response_started.clone();
                move || {
                    let request = service.admit_head_request().expect("Head request admitted");
                    let response_started = response_started.clone();
                    async move {
                        let mut response = Response::new(Body::from(Bytes::from(vec![b'x'; RESPONSE_BYTES])));
                        track_request_with_timeout(&mut response, request, None);
                        response_started.notify_one();
                        response
                    }
                }
            }),
        );
        let (address, server) = spawn_server(application).await;
        let socket = TcpSocket::new_v4().expect("test socket created");
        socket.set_recv_buffer_size(1024).expect("small receive buffer configured");
        let mut client = socket.connect(address).await.expect("test client connects");
        request(&mut client).await;
        response_started.notified().await;

        let shutdown = tokio::spawn({
            let service = service.clone();
            async move { service.shutdown().await }
        });
        timeout(Duration::from_secs(2), shutdown)
            .await
            .expect("shutdown is unblocked by connection abort")
            .expect("shutdown task joins")
            .expect("shutdown succeeds");
        timeout(Duration::from_secs(2), server)
            .await
            .expect("connection abort completes server")
            .expect("server task joins")
            .expect("shutdown cancellation is an expected connection outcome");

        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("cancelled socket reaches EOF")
            .expect("buffered response bytes remain readable");
        assert!(response.len() < RESPONSE_BYTES, "shutdown truncates the backpressured response");
    }

    #[tokio::test]
    async fn peer_disconnect_releases_the_exact_delivery_guard() {
        const RESPONSE_BYTES: usize = 16 * 1024 * 1024;

        let admissions = Arc::new(ApiRequestAdmissions::new(CancellationToken::new()));
        let response_started = Arc::new(Notify::new());
        let application = Router::new().route(
            "/",
            get({
                let admissions = admissions.clone();
                let response_started = response_started.clone();
                move || {
                    let request = admissions.admit_head().expect("Head request admitted");
                    let response_started = response_started.clone();
                    async move {
                        let mut response = Response::new(Body::from(Bytes::from(vec![b'x'; RESPONSE_BYTES])));
                        track_request_with_timeout(&mut response, request, None);
                        response_started.notify_one();
                        response
                    }
                }
            }),
        );
        let (address, server) = spawn_server(application).await;
        let socket = TcpSocket::new_v4().expect("test socket created");
        socket.set_recv_buffer_size(1024).expect("small receive buffer configured");
        let mut client = socket.connect(address).await.expect("test client connects");
        request(&mut client).await;
        response_started.notified().await;
        drop(client);

        timeout(Duration::from_secs(2), admissions.wait()).await.expect("peer disconnect releases admission");
        let _ = timeout(Duration::from_secs(2), server).await.expect("server observes peer disconnect").expect("server task joins");
    }

    #[tokio::test]
    async fn abandoned_connection_adapter_releases_delivery_ownership() {
        const RESPONSE_BYTES: usize = 16 * 1024 * 1024;

        let admissions = Arc::new(ApiRequestAdmissions::new(CancellationToken::new()));
        let response_started = Arc::new(Notify::new());
        let application = Router::new().route(
            "/",
            get({
                let admissions = admissions.clone();
                let response_started = response_started.clone();
                move || {
                    let request = admissions.admit_head().expect("Head request admitted");
                    let response_started = response_started.clone();
                    async move {
                        let mut response = Response::new(Body::from(Bytes::from(vec![b'x'; RESPONSE_BYTES])));
                        track_request_with_timeout(&mut response, request, None);
                        response_started.notify_one();
                        response
                    }
                }
            }),
        );
        let (address, server) = spawn_server(application).await;
        let socket = TcpSocket::new_v4().expect("test socket created");
        socket.set_recv_buffer_size(1024).expect("small receive buffer configured");
        let mut client = socket.connect(address).await.expect("test client connects");
        request(&mut client).await;
        response_started.notified().await;
        server.abort();
        assert!(server.await.expect_err("aborted server task reports cancellation").is_cancelled());

        timeout(Duration::from_secs(2), admissions.wait()).await.expect("adapter abandonment releases admission");
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("abandoned adapter closes its socket")
            .expect("buffered response bytes remain readable");
    }

    #[tokio::test]
    async fn real_sse_backpressure_disconnects_registration_releases_admission_and_allows_shutdown() {
        let (service, _events) = api_service();
        let publication = publication();
        service.install_test_publication(publication.clone());
        let application = crate::router().with_state(service.clone());
        let (address, server) = spawn_server(application).await;
        let socket = TcpSocket::new_v4().expect("test socket created");
        socket.set_recv_buffer_size(1024).expect("small receive buffer configured");
        let mut client = socket.connect(address).await.expect("test client connects");
        request_sse(&mut client).await;
        let identifier = read_client_registration(&mut client).await;
        assert!(publication.client_registry().validate(identifier));

        let mut backpressured = false;
        for _ in 0..10_000 {
            publication.client_registry().publish_state(GraphPublicationState::Synchronizing, 0);
            let drained = timeout(Duration::from_millis(20), async {
                loop {
                    if publication.client_registry().pending_messages_for_test(identifier) != Some(0) {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    break;
                }
            })
            .await;
            if drained.is_err() {
                backpressured = true;
                break;
            }
        }
        assert!(backpressured, "stopped peer reading eventually prevents Hyper from polling the real SSE mailbox");

        for _ in 0..10_000 {
            if !publication.client_registry().validate(identifier) {
                break;
            }
            publication.client_registry().publish_state(GraphPublicationState::Synchronizing, 0);
        }
        assert!(!publication.client_registry().validate(identifier), "mailbox saturation removes the exact registration");

        timeout(Duration::from_secs(2), server)
            .await
            .expect("slow-client cancellation closes the connection")
            .expect("server task joins")
            .expect("slow-client cancellation is an expected connection outcome");
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("slow-client SSE socket reaches EOF")
            .expect("buffered SSE bytes remain readable");

        let mut admitted = Vec::with_capacity(MAX_SSE_CLIENTS);
        for _ in 0..MAX_SSE_CLIENTS {
            admitted.push(service.open_sse_connection().expect("the released SSE permit restores the exact full capacity"));
        }
        assert!(matches!(service.open_sse_connection(), Err(SseConnectionOpenError::Admission(ApiRequestAdmissionError::Saturated))));
        drop(admitted);
        timeout(Duration::from_secs(2), service.shutdown())
            .await
            .expect("released SSE delivery permits shutdown to complete")
            .expect("service shuts down cleanly");
    }

    #[tokio::test]
    async fn prior_knowledge_http2_is_not_served() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let application = Router::new().fallback({
            let calls = calls.clone();
            move || {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { "unexpected" }
            }
        });
        let (address, server) = spawn_server(application).await;
        let mut client = TcpStream::connect(address).await.expect("test client connects");
        client.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await.expect("HTTP/2 preface written");

        let mut response = Vec::new();
        timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("HTTP/1.1 adapter rejects the preface")
            .expect("rejection bytes read");
        let _ = timeout(Duration::from_secs(1), server).await.expect("server completes").expect("server task joins");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
