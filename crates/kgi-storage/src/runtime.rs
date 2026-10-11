use std::sync::{Arc, Weak};

use kgi_core::retirement::RetirementRequest as CoreRetirementRequest;
use kgi_model::block::{MaterializedSyncAnchor, ValidatedNodeBlock};
use tokio::sync::{mpsc, oneshot};

use crate::{
    error::StorageError,
    generation::{ValidatedApiDbClient, ValidatedDbClient},
};

pub(crate) type RetirementSender = mpsc::UnboundedSender<RetirementRequest>;
pub(crate) type RetirementReceiver = mpsc::UnboundedReceiver<RetirementRequest>;

pub(crate) enum RetirementTarget {
    Processing(Weak<ValidatedDbClient>),
    #[allow(dead_code, reason = "used by API projection operations in the persistence increment")]
    Api(Weak<ValidatedApiDbClient>),
}

pub(crate) type RetirementRequest = CoreRetirementRequest<RetirementTarget, (), StorageError>;

pub(crate) type RebuildSender = mpsc::UnboundedSender<RebuildRequest>;
pub(crate) type RebuildReceiver = mpsc::UnboundedReceiver<RebuildRequest>;

pub(crate) struct RebuildRequest {
    target: Weak<ValidatedDbClient>,
    pruning_point: ValidatedNodeBlock,
    completion: oneshot::Sender<Result<MaterializedSyncAnchor, StorageError>>,
}

impl RebuildRequest {
    pub(crate) fn new(
        target: Weak<ValidatedDbClient>,
        pruning_point: ValidatedNodeBlock,
        completion: oneshot::Sender<Result<MaterializedSyncAnchor, StorageError>>,
    ) -> Self {
        Self { target, pruning_point, completion }
    }

    pub(crate) fn targets(&self, generation: &Arc<ValidatedDbClient>) -> bool {
        self.target.upgrade().is_some_and(|target| Arc::ptr_eq(&target, generation))
    }

    pub(crate) fn into_parts(self) -> (ValidatedNodeBlock, oneshot::Sender<Result<MaterializedSyncAnchor, StorageError>>) {
        (self.pruning_point, self.completion)
    }

    pub(crate) fn complete(self, result: Result<MaterializedSyncAnchor, StorageError>) {
        let _ = self.completion.send(result);
    }
}

pub(crate) fn retirement_channel() -> (RetirementSender, RetirementReceiver) {
    mpsc::unbounded_channel()
}

pub(crate) fn rebuild_channel() -> (RebuildSender, RebuildReceiver) {
    mpsc::unbounded_channel()
}
