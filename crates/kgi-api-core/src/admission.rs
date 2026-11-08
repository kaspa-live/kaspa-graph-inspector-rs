#![allow(dead_code, reason = "request guards become transport-owned in increment 20")]

use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::{CancellationToken, DropGuard};

/// Maximum concurrently admitted Head-lane HTTP requests.
pub(crate) const MAX_HEAD_HTTP_REQUESTS: usize = 64;
/// Maximum concurrently admitted historical HTTP requests.
pub(crate) const MAX_HISTORICAL_HTTP_REQUESTS: usize = 8;
/// Maximum concurrently admitted memory-only status requests.
pub(crate) const MAX_STATUS_HTTP_REQUESTS: usize = 16;

const MAX_HTTP_REQUESTS: usize = MAX_HEAD_HTTP_REQUESTS + MAX_HISTORICAL_HTTP_REQUESTS + MAX_STATUS_HTTP_REQUESTS;

/// Reason one nonblocking HTTP request admission failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ApiRequestAdmissionError {
    /// The applicable independent request lane is full.
    #[error("API request lane is saturated")]
    Saturated,
    /// ApiService has closed terminal request admission.
    #[error("API request admission is unavailable")]
    Unavailable,
    /// The request was cancelled while changing its applicable lane.
    #[error("API request was cancelled")]
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApiRequestLane {
    Head,
    Historical,
    Status,
}

struct ApiRequestGuardInner {
    cancellation: CancellationToken,
    _cancellation_on_completion: DropGuard,
    lane: Mutex<Option<(ApiRequestLane, OwnedSemaphorePermit)>>,
    _completion: OwnedSemaphorePermit,
}

/// One admitted request's lane, cancellation, and shutdown-completion ownership.
pub(crate) struct ApiRequestGuard {
    inner: Arc<ApiRequestGuardInner>,
    cancel_on_drop: bool,
}

impl ApiRequestGuard {
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    pub(crate) fn lane(&self) -> Option<ApiRequestLane> {
        self.inner.lane.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_ref().map(|(lane, _)| *lane)
    }

    pub(crate) fn cancel(&self) {
        self.inner.cancellation.cancel();
    }

    /// Retains all request resources for independently owned cleanup without
    /// making cleanup-task completion cancel a still-live request handler.
    pub(crate) fn cleanup_owner(&self) -> Self {
        Self { inner: self.inner.clone(), cancel_on_drop: false }
    }
}

impl Drop for ApiRequestGuard {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.inner.cancellation.cancel();
        }
    }
}

/// Independent bounded request lanes plus the common terminal completion barrier.
pub(crate) struct ApiRequestAdmissions {
    accepting: Mutex<bool>,
    head: Arc<Semaphore>,
    historical: Arc<Semaphore>,
    status: Arc<Semaphore>,
    completion: Arc<Semaphore>,
    http_cancellation: CancellationToken,
}

impl ApiRequestAdmissions {
    pub(crate) fn new(http_cancellation: CancellationToken) -> Self {
        Self {
            accepting: Mutex::new(true),
            head: Arc::new(Semaphore::new(MAX_HEAD_HTTP_REQUESTS)),
            historical: Arc::new(Semaphore::new(MAX_HISTORICAL_HTTP_REQUESTS)),
            status: Arc::new(Semaphore::new(MAX_STATUS_HTTP_REQUESTS)),
            completion: Arc::new(Semaphore::new(MAX_HTTP_REQUESTS)),
            http_cancellation,
        }
    }

    pub(crate) fn admit_head(&self) -> Result<ApiRequestGuard, ApiRequestAdmissionError> {
        self.admit(ApiRequestLane::Head, &self.head)
    }

    pub(crate) fn admit_status(&self) -> Result<ApiRequestGuard, ApiRequestAdmissionError> {
        self.admit(ApiRequestLane::Status, &self.status)
    }

    fn admit(&self, lane: ApiRequestLane, semaphore: &Arc<Semaphore>) -> Result<ApiRequestGuard, ApiRequestAdmissionError> {
        let accepting = self.accepting.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !*accepting {
            return Err(ApiRequestAdmissionError::Unavailable);
        }
        let permit = semaphore.clone().try_acquire_owned().map_err(admission_error)?;
        let completion =
            self.completion.clone().try_acquire_owned().expect("the completion bound equals the sum of independently admitted lanes");
        let cancellation = self.http_cancellation.child_token();
        let cancellation_on_completion = cancellation.clone().drop_guard();
        let request = ApiRequestGuard {
            inner: Arc::new(ApiRequestGuardInner {
                cancellation,
                _cancellation_on_completion: cancellation_on_completion,
                lane: Mutex::new(Some((lane, permit))),
                _completion: completion,
            }),
            cancel_on_drop: true,
        };
        drop(accepting);
        Ok(request)
    }

    /// Releases the Head permit and immediately attempts historical admission.
    pub(crate) fn handoff_to_historical(&self, request: &ApiRequestGuard) -> Result<(), ApiRequestAdmissionError> {
        let mut lane = request.inner.lane.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (current, permit) = lane.take().expect("a Head request hands off at most once");
        assert_eq!(current, ApiRequestLane::Head, "only the anchored Head phase enters the historical lane");
        drop(permit);

        if request.inner.cancellation.is_cancelled() {
            return Err(ApiRequestAdmissionError::Cancelled);
        }
        let permit = self.historical.clone().try_acquire_owned().map_err(admission_error)?;
        *lane = Some((ApiRequestLane::Historical, permit));
        Ok(())
    }

    pub(crate) fn close(&self) {
        let mut accepting = self.accepting.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !*accepting {
            return;
        }
        *accepting = false;
        self.head.close();
        self.historical.close();
        self.status.close();
    }

    pub(crate) async fn wait(&self) {
        let _all_requests = self
            .completion
            .clone()
            .acquire_many_owned(u32::try_from(MAX_HTTP_REQUESTS).expect("request bound fits u32"))
            .await
            .expect("the request-completion semaphore is never closed");
    }
}

fn admission_error(error: TryAcquireError) -> ApiRequestAdmissionError {
    match error {
        TryAcquireError::NoPermits => ApiRequestAdmissionError::Saturated,
        TryAcquireError::Closed => ApiRequestAdmissionError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, timeout};
    use tokio_util::sync::CancellationToken;

    use super::{
        ApiRequestAdmissionError, ApiRequestAdmissions, ApiRequestLane, MAX_HEAD_HTTP_REQUESTS, MAX_HISTORICAL_HTTP_REQUESTS,
        MAX_STATUS_HTTP_REQUESTS,
    };

    #[test]
    fn request_lanes_saturate_independently_and_handoff_never_holds_both() {
        let admissions = ApiRequestAdmissions::new(CancellationToken::new());
        let heads: Vec<_> = (0..MAX_HEAD_HTTP_REQUESTS).map(|_| admissions.admit_head().expect("Head admission")).collect();
        assert!(matches!(admissions.admit_head(), Err(ApiRequestAdmissionError::Saturated)));

        let statuses: Vec<_> = (0..MAX_STATUS_HTTP_REQUESTS).map(|_| admissions.admit_status().expect("status admission")).collect();
        assert!(matches!(admissions.admit_status(), Err(ApiRequestAdmissionError::Saturated)));

        drop(heads);
        let historical: Vec<_> = (0..MAX_HISTORICAL_HTTP_REQUESTS)
            .map(|_| {
                let request = admissions.admit_head().expect("Head admission resumes independently");
                admissions.handoff_to_historical(&request).expect("historical handoff");
                request
            })
            .collect();
        let rejected = admissions.admit_head().expect("the released Head permit remains available");
        assert!(matches!(admissions.handoff_to_historical(&rejected), Err(ApiRequestAdmissionError::Saturated)));
        assert_eq!(historical[0].lane(), Some(ApiRequestLane::Historical));
        assert_eq!(statuses[0].lane(), Some(ApiRequestLane::Status));
        assert_eq!(rejected.lane(), None);
    }

    #[tokio::test]
    async fn terminal_wait_includes_independently_owned_cleanup() {
        let root = CancellationToken::new();
        let admissions = ApiRequestAdmissions::new(root.clone());
        let request = admissions.admit_head().expect("request admission");
        let cleanup = request.cleanup_owner();
        admissions.close();
        root.cancel();
        drop(request);

        assert!(timeout(Duration::from_millis(20), admissions.wait()).await.is_err());
        drop(cleanup);
        timeout(Duration::from_secs(1), admissions.wait()).await.expect("cleanup release completes the barrier");
        assert!(matches!(admissions.admit_status(), Err(ApiRequestAdmissionError::Unavailable)));
    }
}
