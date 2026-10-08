use std::sync::Weak;

use kgi_core::retirement::RetirementRequest as CoreRetirementRequest;
use kgi_model::lifecycle::RecoveryInputKind;
use tokio::sync::mpsc;

use crate::rpc::ValidatedRpcClient;

pub(crate) type RetirementSender = mpsc::UnboundedSender<RetirementRequest>;
pub(crate) type RetirementReceiver = mpsc::UnboundedReceiver<RetirementRequest>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetirementReason {
    MalformedRecoveryInput(RecoveryInputKind),
    SubscriptionControlFailure,
}

pub(crate) type RetirementRequest = CoreRetirementRequest<Weak<ValidatedRpcClient>, RetirementReason>;

pub(crate) fn retirement_channel() -> (RetirementSender, RetirementReceiver) {
    mpsc::unbounded_channel()
}
