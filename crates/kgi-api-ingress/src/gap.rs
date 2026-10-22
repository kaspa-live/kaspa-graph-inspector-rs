use crate::channel::GraphUpdateChannelState;
use tokio::sync::watch;

/// Producer-side reporter for a lost ordinary graph update.
#[derive(Clone, Debug)]
pub(crate) struct GraphUpdateGapReporter {
    generation: watch::Sender<u64>,
    channel_state: GraphUpdateChannelState,
}

impl GraphUpdateGapReporter {
    pub(crate) fn new(generation: watch::Sender<u64>, channel_state: GraphUpdateChannelState) -> Self {
        Self { generation, channel_state }
    }

    /// Advances the session gap generation and publishes its newest value.
    ///
    /// Consecutive reports may coalesce into one receiver wakeup, while the
    /// published generation still records every lost ordinary update.
    ///
    /// # Panics
    ///
    /// Panics if the generation exceeds `u64::MAX`.
    pub(crate) fn report(&self) {
        let next = self.channel_state.advance_gap_generation().expect("graph-update gap generation overflow");
        self.generation.send_replace(next);
    }
}

/// Consumer-side observation of the session-local gap generation.
#[derive(Debug)]
pub struct GraphUpdateGap {
    generation: watch::Receiver<u64>,
    observed_generation: u64,
    reporter_closed: bool,
}

impl GraphUpdateGap {
    pub(crate) fn new(generation: watch::Receiver<u64>) -> Self {
        Self { generation, observed_generation: 0, reporter_closed: false }
    }

    /// Returns the latest published generation without marking it observed.
    #[must_use]
    pub fn generation(&self) -> u64 {
        *self.generation.borrow()
    }

    /// Returns the latest published generation and marks it observed.
    pub(crate) fn acknowledge_current(&mut self) -> u64 {
        let generation = *self.generation.borrow_and_update();
        self.observed_generation = generation;
        generation
    }

    /// Marks and returns the latest unobserved generation, if one exists.
    pub fn take_pending(&mut self) -> Option<u64> {
        let observed_generation = self.observed_generation;
        let generation = self.acknowledge_current();
        if generation == observed_generation { None } else { Some(generation) }
    }

    pub(crate) async fn changed(&mut self) -> Option<u64> {
        if self.reporter_closed {
            return None;
        }

        match self.generation.changed().await {
            Ok(()) => self.take_pending(),
            Err(_) => {
                self.reporter_closed = true;
                self.take_pending()
            }
        }
    }

    pub(crate) const fn reporter_closed(&self) -> bool {
        self.reporter_closed
    }
}
