use std::sync::{Arc, Weak};

use kgi_core::retirement::RetirementRequest as CoreRetirementRequest;
use kgi_model::block::ValidatedNodeBlock;
use tokio::sync::{mpsc, oneshot};

use crate::{
    error::StorageError,
    generation::{ValidatedApiDbClient, ValidatedDbClient},
    operation::RebuildFromPruningPointOutcome,
};
use tokio_util::sync::CancellationToken;

pub(crate) type RetirementSender = mpsc::UnboundedSender<RetirementRequest>;
pub(crate) type RetirementReceiver = mpsc::UnboundedReceiver<RetirementRequest>;

pub(crate) enum RetirementTarget {
    Processing(Weak<ValidatedDbClient>),
    Api(Weak<ValidatedApiDbClient>),
}

pub(crate) type RetirementRequest = CoreRetirementRequest<RetirementTarget, (), StorageError>;

pub(crate) type RebuildSender = mpsc::UnboundedSender<RebuildRequest>;
pub(crate) type RebuildReceiver = mpsc::UnboundedReceiver<RebuildRequest>;

pub(crate) struct RebuildRequest {
    target: Weak<ValidatedDbClient>,
    pruning_point: ValidatedNodeBlock,
    cancellation: CancellationToken,
    completion: oneshot::Sender<Result<RebuildFromPruningPointOutcome, StorageError>>,
}

impl RebuildRequest {
    pub(crate) fn new(
        target: Weak<ValidatedDbClient>,
        pruning_point: ValidatedNodeBlock,
        cancellation: CancellationToken,
        completion: oneshot::Sender<Result<RebuildFromPruningPointOutcome, StorageError>>,
    ) -> Self {
        Self { target, pruning_point, cancellation, completion }
    }

    pub(crate) fn targets(&self, generation: &Arc<ValidatedDbClient>) -> bool {
        self.target.upgrade().is_some_and(|target| Arc::ptr_eq(&target, generation))
    }

    pub(crate) fn into_parts(
        self,
    ) -> (ValidatedNodeBlock, CancellationToken, oneshot::Sender<Result<RebuildFromPruningPointOutcome, StorageError>>) {
        (self.pruning_point, self.cancellation, self.completion)
    }

    pub(crate) fn complete(self, result: Result<RebuildFromPruningPointOutcome, StorageError>) {
        let _ = self.completion.send(result);
    }
}

pub(crate) fn retirement_channel() -> (RetirementSender, RetirementReceiver) {
    mpsc::unbounded_channel()
}

pub(crate) fn rebuild_channel() -> (RebuildSender, RebuildReceiver) {
    mpsc::unbounded_channel()
}
