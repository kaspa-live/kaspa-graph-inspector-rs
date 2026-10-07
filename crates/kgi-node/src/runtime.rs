use std::sync::Weak;

use kgi_model::lifecycle::RecoveryInputKind;
use tokio::sync::{mpsc, oneshot};

use crate::rpc::ValidatedRpcClient;

pub(crate) type RetirementSender = mpsc::UnboundedSender<RetirementRequest>;
pub(crate) type RetirementReceiver = mpsc::UnboundedReceiver<RetirementRequest>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetirementReason {
    MalformedRecoveryInput(RecoveryInputKind),
    SubscriptionControlFailure,
}

pub(crate) struct RetirementRequest {
    generation: Weak<ValidatedRpcClient>,
    reason: RetirementReason,
    completion: oneshot::Sender<()>,
}

impl RetirementRequest {
    pub(crate) fn new(generation: Weak<ValidatedRpcClient>, reason: RetirementReason, completion: oneshot::Sender<()>) -> Self {
        Self { generation, reason, completion }
    }

    pub(crate) fn generation(&self) -> &Weak<ValidatedRpcClient> {
        &self.generation
    }

    pub(crate) const fn reason(&self) -> RetirementReason {
        self.reason
    }

    pub(crate) fn complete(self) {
        let _ = self.completion.send(());
    }
}

pub(crate) fn retirement_channel() -> (RetirementSender, RetirementReceiver) {
    mpsc::unbounded_channel()
}
