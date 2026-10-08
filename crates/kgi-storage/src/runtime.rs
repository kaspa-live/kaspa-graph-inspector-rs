use std::sync::Weak;

use kgi_core::retirement::RetirementRequest as CoreRetirementRequest;
use tokio::sync::mpsc;

use crate::generation::{ValidatedApiDbClient, ValidatedDbClient};

pub(crate) type RetirementSender = mpsc::UnboundedSender<RetirementRequest>;
pub(crate) type RetirementReceiver = mpsc::UnboundedReceiver<RetirementRequest>;

pub(crate) enum RetirementTarget {
    Processing(Weak<ValidatedDbClient>),
    #[allow(dead_code, reason = "used by API projection operations in the persistence increment")]
    Api(Weak<ValidatedApiDbClient>),
}

pub(crate) type RetirementRequest = CoreRetirementRequest<RetirementTarget>;

pub(crate) fn retirement_channel() -> (RetirementSender, RetirementReceiver) {
    mpsc::unbounded_channel()
}
