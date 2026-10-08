//! Generic retirement requests shared by permanent service workers.

use tokio::sync::oneshot;

/// A service-owned retirement target paired with a completion barrier.
pub struct RetirementRequest<T, R = (), E = ()> {
    target: T,
    reason: R,
    completion: oneshot::Sender<Result<(), E>>,
}

impl<T, E> RetirementRequest<T, (), E> {
    /// Creates a retirement request without a reason payload.
    pub fn new(target: T, completion: oneshot::Sender<Result<(), E>>) -> Self {
        Self { target, reason: (), completion }
    }
}

impl<T, R, E> RetirementRequest<T, R, E> {
    /// Creates a retirement request with a service-specific reason.
    pub fn with_reason(target: T, reason: R, completion: oneshot::Sender<Result<(), E>>) -> Self {
        Self { target, reason, completion }
    }

    /// Returns the exact service-owned retirement target.
    #[must_use]
    pub const fn target(&self) -> &T {
        &self.target
    }

    /// Returns the service-specific retirement reason.
    #[must_use]
    pub const fn reason(&self) -> &R {
        &self.reason
    }

    /// Completes the request's acknowledgement barrier with the service result.
    pub fn complete(self, result: Result<(), E>) {
        let _ = self.completion.send(result);
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::RetirementRequest;

    #[test]
    fn reasonless_request_preserves_target_and_completes_result_barrier() {
        let (completion, mut acknowledgement) = oneshot::channel();
        let request = RetirementRequest::new(7_u8, completion);

        assert_eq!(*request.target(), 7);
        assert_eq!(*request.reason(), ());
        request.complete(Err("event path closed"));
        assert_eq!(acknowledgement.try_recv(), Ok(Err("event path closed")));
    }

    #[test]
    fn reasoned_request_preserves_target_and_reason() {
        let (completion, _acknowledgement) = oneshot::channel::<Result<(), ()>>();
        let request = RetirementRequest::with_reason("generation", "malformed response", completion);

        assert_eq!(*request.target(), "generation");
        assert_eq!(*request.reason(), "malformed response");
    }
}
