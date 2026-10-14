//! Exact occupancy metrics for bounded Tokio channels.

use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{
    self,
    error::{SendError, TryRecvError, TrySendError},
};

/// Current and historical occupancy of one bounded channel.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BoundedChannelMetricsSnapshot {
    pub current_occupancy: usize,
    pub high_water_mark: usize,
}

#[derive(Debug)]
struct BoundedChannelState {
    metrics: BoundedChannelMetricsSnapshot,
    receiver_open: bool,
}

/// Cloneable read-only metrics handle for one bounded channel.
#[derive(Clone, Debug)]
pub struct BoundedChannelMetrics(Arc<Mutex<BoundedChannelState>>);

impl BoundedChannelMetrics {
    /// Returns one coherent metrics observation.
    #[must_use]
    pub fn snapshot(&self) -> BoundedChannelMetricsSnapshot {
        self.0.lock().expect("bounded channel metrics mutex poisoned").metrics
    }

    fn enqueue(&self) -> bool {
        let mut state = self.0.lock().expect("bounded channel metrics mutex poisoned");
        if !state.receiver_open {
            return false;
        }
        state.metrics.current_occupancy += 1;
        state.metrics.high_water_mark = state.metrics.high_water_mark.max(state.metrics.current_occupancy);
        true
    }

    fn dequeue(&self) {
        let mut state = self.0.lock().expect("bounded channel metrics mutex poisoned");
        state.metrics.current_occupancy = state.metrics.current_occupancy.saturating_sub(1);
    }

    fn close_receiver(&self) {
        let mut state = self.0.lock().expect("bounded channel metrics mutex poisoned");
        state.receiver_open = false;
        state.metrics.current_occupancy = 0;
    }
}

/// Sender half of a bounded Tokio channel with exact occupancy metrics.
#[derive(Debug)]
pub struct BoundedSender<T> {
    inner: mpsc::Sender<T>,
    metrics: BoundedChannelMetrics,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), metrics: self.metrics.clone() }
    }
}

impl<T> BoundedSender<T> {
    /// Sends one value, waiting for capacity while the receiver remains open.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let permit = match self.inner.reserve().await {
            Ok(permit) => permit,
            Err(_) => return Err(SendError(value)),
        };
        if !self.metrics.enqueue() {
            return Err(SendError(value));
        }
        permit.send(value);
        Ok(())
    }

    /// Attempts to send one value without waiting for capacity.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let permit = match self.inner.try_reserve() {
            Ok(permit) => permit,
            Err(TrySendError::Full(_)) => return Err(TrySendError::Full(value)),
            Err(TrySendError::Closed(_)) => return Err(TrySendError::Closed(value)),
        };
        if !self.metrics.enqueue() {
            return Err(TrySendError::Closed(value));
        }
        permit.send(value);
        Ok(())
    }

    /// Returns a cloneable read-only metrics handle.
    #[must_use]
    pub fn metrics_source(&self) -> BoundedChannelMetrics {
        self.metrics.clone()
    }
}

/// Receiver half of a bounded Tokio channel with exact occupancy metrics.
#[derive(Debug)]
pub struct BoundedReceiver<T> {
    inner: mpsc::Receiver<T>,
    metrics: BoundedChannelMetrics,
}

impl<T> BoundedReceiver<T> {
    /// Receives the next value, or `None` after every sender is gone.
    pub async fn recv(&mut self) -> Option<T> {
        let value = self.inner.recv().await;
        if value.is_some() {
            self.metrics.dequeue();
        }
        value
    }

    /// Attempts to receive the next value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let value = self.inner.try_recv()?;
        self.metrics.dequeue();
        Ok(value)
    }

    /// Returns a cloneable read-only metrics handle.
    #[must_use]
    pub fn metrics_source(&self) -> BoundedChannelMetrics {
        self.metrics.clone()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        self.metrics.close_receiver();
    }
}

/// Creates one bounded Tokio channel with exact occupancy instrumentation.
#[must_use]
pub fn bounded_channel<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    let (sender, receiver) = mpsc::channel(capacity);
    let metrics = BoundedChannelMetrics(Arc::new(Mutex::new(BoundedChannelState {
        metrics: BoundedChannelMetricsSnapshot::default(),
        receiver_open: true,
    })));
    (BoundedSender { inner: sender, metrics: metrics.clone() }, BoundedReceiver { inner: receiver, metrics })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn occupancy_and_high_water_follow_successful_queue_mutations() {
        let (sender, mut receiver) = bounded_channel(2);
        let metrics = sender.metrics_source();

        sender.send(1).await.expect("first send");
        sender.try_send(2).expect("second send");
        assert_eq!(metrics.snapshot(), BoundedChannelMetricsSnapshot { current_occupancy: 2, high_water_mark: 2 });
        assert!(matches!(sender.try_send(3), Err(TrySendError::Full(3))));

        assert_eq!(receiver.recv().await, Some(1));
        assert_eq!(metrics.snapshot(), BoundedChannelMetricsSnapshot { current_occupancy: 1, high_water_mark: 2 });
        assert_eq!(receiver.try_recv(), Ok(2));
        assert_eq!(metrics.snapshot(), BoundedChannelMetricsSnapshot { current_occupancy: 0, high_water_mark: 2 });
    }

    #[test]
    fn dropping_the_receiver_clears_occupancy_and_closes_senders() {
        let (sender, receiver) = bounded_channel(1);
        let metrics = sender.metrics_source();
        sender.try_send(1).expect("queued value");

        drop(receiver);

        assert_eq!(metrics.snapshot(), BoundedChannelMetricsSnapshot { current_occupancy: 0, high_water_mark: 1 });
        assert!(matches!(sender.try_send(2), Err(TrySendError::Closed(2))));
    }
}
