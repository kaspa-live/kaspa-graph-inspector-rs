use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use kgi_core::timing::Timing;
use tokio::sync::watch;

use crate::error::{RebuildSetupFault, StorageError};

pub(crate) const API_REPLACEMENT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Admission {
    Open,
    Closed { epoch: u64 },
}

struct GateState {
    admission: Admission,
    control_available: bool,
    next_epoch: u64,
    next_lease_id: u64,
    active_leases: HashMap<u64, watch::Sender<bool>>,
}

/// StorageService-owned admission and cancellation registry for API database read leases.
pub(crate) struct ApiReplacementGate {
    state: Mutex<GateState>,
    // Wakeup revision only; lease identity and replacement ownership use separate values.
    change_revision: watch::Sender<u64>,
}

impl ApiReplacementGate {
    pub(crate) fn new() -> Arc<Self> {
        let (change_revision, _) = watch::channel(0);
        Arc::new(Self {
            state: Mutex::new(GateState {
                admission: Admission::Open,
                control_available: true,
                next_epoch: 1,
                next_lease_id: 1,
                active_leases: HashMap::new(),
            }),
            change_revision,
        })
    }

    pub(crate) fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.change_revision.subscribe()
    }

    pub(crate) fn try_acquire_lease(self: &Arc<Self>) -> Result<Option<ApiDbReadLease>, StorageError> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        ensure_control_available(&state)?;
        if matches!(state.admission, Admission::Closed { .. }) {
            return Ok(None);
        }
        let lease_id = state.next_lease_id;
        state.next_lease_id = state
            .next_lease_id
            .checked_add(1)
            .ok_or_else(|| StorageError::replacement_control("API database read-lease identifier overflow"))?;
        let (cancellation, cancellation_rx) = watch::channel(false);
        state.active_leases.insert(lease_id, cancellation);
        Ok(Some(ApiDbReadLease { gate: self.clone(), lease_id, cancellation: cancellation_rx }))
    }

    pub(crate) fn begin_replacement(self: &Arc<Self>) -> Result<ApiReplacementAttempt, StorageError> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        ensure_control_available(&state)?;
        let epoch = state.next_epoch;
        state.next_epoch =
            state.next_epoch.checked_add(1).ok_or_else(|| StorageError::replacement_control("API replacement epoch overflow"))?;
        state.admission = Admission::Closed { epoch };
        for cancellation in state.active_leases.values() {
            cancellation.send_replace(true);
        }
        drop(state);
        self.announce_change();
        Ok(ApiReplacementAttempt { gate: self.clone(), epoch })
    }

    fn release(&self, lease_id: u64) {
        let removed = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).active_leases.remove(&lease_id);
        if removed.is_some() {
            self.announce_change();
        }
    }

    fn announce_change(&self) {
        let revision = self.change_revision.borrow().wrapping_add(1);
        self.change_revision.send_replace(revision);
    }

    fn active_is_empty(&self, epoch: u64) -> Result<bool, StorageError> {
        let state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        ensure_current_attempt(&state, epoch)?;
        Ok(state.active_leases.is_empty())
    }

    fn ensure_current(&self, epoch: u64) -> Result<(), StorageError> {
        let state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        ensure_current_attempt(&state, epoch)
    }

    fn open(&self, epoch: u64) -> Result<(), StorageError> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        ensure_control_available(&state)?;
        if state.admission != (Admission::Closed { epoch }) || !state.active_leases.is_empty() {
            return Err(StorageError::replacement_control("API replacement attempt cannot open admission"));
        }
        state.admission = Admission::Open;
        drop(state);
        self.announce_change();
        Ok(())
    }

    #[cfg(test)]
    fn active_lease_count(&self) -> usize {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).active_leases.len()
    }

    #[cfg(test)]
    pub(crate) fn break_control(&self) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.control_available = false;
        if matches!(state.admission, Admission::Open) {
            state.admission = Admission::Closed { epoch: state.next_epoch };
        }
        for cancellation in state.active_leases.values() {
            cancellation.send_replace(true);
        }
        drop(state);
        self.announce_change();
    }
}

fn ensure_control_available(state: &GateState) -> Result<(), StorageError> {
    if state.control_available { Ok(()) } else { Err(StorageError::replacement_control("API replacement gate is unavailable")) }
}

fn ensure_current_attempt(state: &GateState, epoch: u64) -> Result<(), StorageError> {
    ensure_control_available(state)?;
    if state.admission == (Admission::Closed { epoch }) {
        Ok(())
    } else {
        Err(StorageError::replacement_control("stale API replacement attempt"))
    }
}

/// One admitted API database read lease, released synchronously on drop.
pub(crate) struct ApiDbReadLease {
    gate: Arc<ApiReplacementGate>,
    lease_id: u64,
    #[allow(dead_code, reason = "consumed by API database operations in the API projection increment")]
    cancellation: watch::Receiver<bool>,
}

impl ApiDbReadLease {
    #[allow(dead_code, reason = "consumed by API database operations in the API projection increment")]
    pub(crate) async fn cancelled(&mut self) {
        if *self.cancellation.borrow() {
            return;
        }
        let _ = self.cancellation.changed().await;
    }

    #[cfg(test)]
    pub(crate) fn is_cancelled(&self) -> bool {
        *self.cancellation.borrow()
    }
}

impl Drop for ApiDbReadLease {
    fn drop(&mut self) {
        self.gate.release(self.lease_id);
    }
}

/// Epoch-fenced ownership of one closed-gate replacement attempt.
pub(crate) struct ApiReplacementAttempt {
    gate: Arc<ApiReplacementGate>,
    epoch: u64,
}

impl ApiReplacementAttempt {
    pub(crate) fn ensure_current(&self) -> Result<(), StorageError> {
        self.gate.ensure_current(self.epoch)
    }

    pub(crate) async fn acquire_exclusive(&self, timing: &Timing) -> Result<ApiReplacementExclusive, StorageError> {
        let mut changes = self.gate.subscribe_changes();
        let timeout = timing.sleep(API_REPLACEMENT_DRAIN_TIMEOUT);
        tokio::pin!(timeout);
        loop {
            if self.gate.active_is_empty(self.epoch)? {
                return Ok(ApiReplacementExclusive { gate: self.gate.clone(), epoch: self.epoch });
            }
            tokio::select! {
                () = &mut timeout => return Err(StorageError::RebuildSetupFailed(RebuildSetupFault::ApiReadExclusion)),
                result = changes.changed() => {
                    result.map_err(|_| StorageError::replacement_control("API replacement notification path closed"))?;
                }
            }
        }
    }
}

/// Proof that no admitted API database read lease overlaps replacement.
pub(crate) struct ApiReplacementExclusive {
    gate: Arc<ApiReplacementGate>,
    epoch: u64,
}

impl ApiReplacementExclusive {
    pub(crate) fn ensure_current(&self) -> Result<(), StorageError> {
        self.gate.ensure_current(self.epoch)
    }

    pub(crate) fn open(self) -> Result<(), StorageError> {
        self.gate.open(self.epoch)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use kgi_core::timing::{Clock, Jitter, Timing};
    use tokio::sync::{mpsc, oneshot};

    use super::{API_REPLACEMENT_DRAIN_TIMEOUT, ApiReplacementGate};
    use crate::error::{RebuildSetupFault, StorageError};

    struct IdentityJitter;

    impl Jitter for IdentityJitter {
        fn apply(&self, nominal: Duration) -> Duration {
            nominal
        }
    }

    struct ManualClock {
        sleeps: mpsc::UnboundedSender<(Duration, oneshot::Sender<()>)>,
    }

    #[async_trait]
    impl Clock for ManualClock {
        async fn sleep(&self, duration: Duration) {
            let (completion, receiver) = oneshot::channel();
            self.sleeps.send((duration, completion)).expect("manual clock receiver");
            let _ = receiver.await;
        }
    }

    fn timing() -> (Timing, mpsc::UnboundedReceiver<(Duration, oneshot::Sender<()>)>) {
        let (sleep_tx, sleep_rx) = mpsc::unbounded_channel();
        (Timing::new(Arc::new(ManualClock { sleeps: sleep_tx }), Arc::new(IdentityJitter)), sleep_rx)
    }

    #[tokio::test]
    async fn replacement_cancels_and_drains_every_admitted_lease_before_opening() {
        let gate = ApiReplacementGate::new();
        let mut first = gate.try_acquire_lease().expect("admission succeeds").expect("gate is open");
        let second = gate.try_acquire_lease().expect("admission succeeds").expect("gate is open");
        let attempt = gate.begin_replacement().expect("replacement closes admission");

        assert!(first.is_cancelled());
        first.cancelled().await;
        assert!(second.is_cancelled());
        assert!(gate.try_acquire_lease().expect("closed gate is healthy").is_none());

        let (timing, mut sleeps) = timing();
        let exclusive = attempt.acquire_exclusive(&timing);
        tokio::pin!(exclusive);
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut exclusive).await.is_err());
        let (duration, _timeout) = sleeps.recv().await.expect("drain timeout starts");
        assert_eq!(duration, API_REPLACEMENT_DRAIN_TIMEOUT);

        drop(first);
        assert_eq!(gate.active_lease_count(), 1);
        drop(second);
        let exclusive = exclusive.await.expect("last lease release establishes exclusion");
        exclusive.open().expect("exclusive attempt opens admission");
        assert!(gate.try_acquire_lease().expect("opened gate remains healthy").is_some());
    }

    #[tokio::test]
    async fn replacement_drain_expires_at_the_settled_timeout() {
        let gate = ApiReplacementGate::new();
        let lease = gate.try_acquire_lease().expect("admission succeeds").expect("gate is open");
        let attempt = gate.begin_replacement().expect("replacement closes admission");
        let (timing, mut sleeps) = timing();

        let drain = tokio::spawn(async move { attempt.acquire_exclusive(&timing).await });
        let (duration, completion) = sleeps.recv().await.expect("drain timeout starts");
        assert_eq!(duration, API_REPLACEMENT_DRAIN_TIMEOUT);
        completion.send(()).expect("advance drain timeout");

        assert!(matches!(
            drain.await.expect("drain task completes"),
            Err(StorageError::RebuildSetupFailed(RebuildSetupFault::ApiReadExclusion))
        ));
        assert!(lease.is_cancelled());
        assert!(gate.try_acquire_lease().expect("timed-out gate remains healthy").is_none());
    }
}
