use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use axum::body::{Body, Bytes, HttpBody};
use http_body::{Frame, SizeHint};
use pin_project_lite::pin_project;
use tokio::time::{Sleep, sleep};
use tokio_util::sync::WaitForCancellationFutureOwned;

use crate::admission::ApiRequestGuard;

/// Complete delivery deadline for one ordinary graph HTTP response.
pub(super) const GRAPH_HTTP_DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);

pin_project! {
    /// Response body retaining one admitted request through terminal delivery.
    pub(super) struct GuardedBody {
        #[pin]
        inner: Body,
        #[pin]
        cancellation: WaitForCancellationFutureOwned,
        deadline: Option<Pin<Box<Sleep>>>,
        request: Option<ApiRequestGuard>,
        terminated: bool,
    }
}

impl GuardedBody {
    pub(super) fn new(body: Bytes, request: ApiRequestGuard, delivery_timeout: bool) -> Self {
        let timeout = delivery_timeout.then_some(GRAPH_HTTP_DELIVERY_TIMEOUT);
        Self::with_inner(Body::from(body), request, timeout)
    }

    fn with_inner(inner: Body, request: ApiRequestGuard, timeout: Option<Duration>) -> Self {
        let cancellation = request.cancellation().cancelled_owned();
        Self {
            inner,
            cancellation,
            deadline: timeout.map(|timeout| Box::pin(sleep(timeout))),
            request: Some(request),
            terminated: false,
        }
    }

    fn terminate(mut self: Pin<&mut Self>) {
        let projected = self.as_mut().project();
        projected.request.take();
        *projected.terminated = true;
    }
}

impl HttpBody for GuardedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self;
        if *this.as_mut().project().terminated {
            return Poll::Ready(None);
        }
        if this.as_mut().project().cancellation.poll(context).is_ready() {
            this.as_mut().terminate();
            return Poll::Ready(Some(Err(axum::Error::new(io::Error::new(
                io::ErrorKind::Interrupted,
                "API response delivery was cancelled",
            )))));
        }
        if let Some(deadline) = this.as_mut().project().deadline.as_mut()
            && deadline.as_mut().poll(context).is_ready()
        {
            this.as_mut().terminate();
            return Poll::Ready(Some(Err(axum::Error::new(io::Error::new(
                io::ErrorKind::TimedOut,
                "API response delivery timed out",
            )))));
        }

        match this.as_mut().project().inner.poll_frame(context) {
            Poll::Ready(None) => {
                this.as_mut().terminate();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.as_mut().terminate();
                Poll::Ready(Some(Err(error)))
            }
            result => result,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.terminated
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    };

    use axum::body::{Body, HttpBody, to_bytes};
    use bytes::Bytes;
    use http_body::Frame;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::admission::ApiRequestAdmissions;

    use super::GuardedBody;

    struct PendingBody;

    impl HttpBody for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn guarded_body_releases_completion_after_terminal_delivery() {
        let admissions = Arc::new(ApiRequestAdmissions::new(CancellationToken::new()));
        let request = admissions.admit_head().expect("request admitted");
        let body = Body::new(GuardedBody::new(Bytes::from_static(b"body"), request, true));
        let waiting = {
            let admissions = admissions.clone();
            tokio::spawn(async move { admissions.wait().await })
        };

        assert_eq!(to_bytes(body, usize::MAX).await.expect("body delivered"), Bytes::from_static(b"body"));
        timeout(Duration::from_secs(1), waiting).await.expect("completion released").expect("waiter joins");
    }

    #[tokio::test]
    async fn guarded_body_timeout_releases_completion_and_terminates_delivery() {
        let admissions = Arc::new(ApiRequestAdmissions::new(CancellationToken::new()));
        let request = admissions.admit_head().expect("request admitted");
        let body = Body::new(GuardedBody::with_inner(Body::new(PendingBody), request, Some(Duration::ZERO)));

        assert!(to_bytes(body, usize::MAX).await.is_err());
        timeout(Duration::from_secs(1), admissions.wait()).await.expect("completion released");
    }
}
