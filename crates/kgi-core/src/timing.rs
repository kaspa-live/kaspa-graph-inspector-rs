//! Injectable timing primitives shared by permanent service workers.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use rand::{Rng, SeedableRng, rngs::SmallRng};

/// Asynchronous monotonic delay source.
#[async_trait]
pub trait Clock: Send + Sync {
    /// Waits until the requested duration has elapsed.
    async fn sleep(&self, duration: Duration);
}

/// Production clock backed by Tokio's monotonic timer.
pub struct TokioClock;

#[async_trait]
impl Clock for TokioClock {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Transforms a nominal retry delay into its actual delay.
pub trait Jitter: Send + Sync {
    /// Applies the source's jitter policy to one nominal duration.
    fn apply(&self, nominal: Duration) -> Duration;
}

/// Equal-jitter source selecting from 50% through 100% of a nominal delay.
pub struct EqualJitter {
    random: Mutex<SmallRng>,
}

impl EqualJitter {
    /// Creates an independently seeded production jitter source.
    #[must_use]
    pub fn from_entropy() -> Self {
        Self { random: Mutex::new(SmallRng::from_entropy()) }
    }
}

impl Jitter for EqualJitter {
    fn apply(&self, nominal: Duration) -> Duration {
        let upper = u64::try_from(nominal.as_nanos()).expect("KGI delay fits u64 nanoseconds");
        let lower = upper.div_ceil(2);
        let nanos = self.random.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).gen_range(lower..=upper);
        Duration::from_nanos(nanos)
    }
}

/// Shared clock and jitter dependencies for runtime delay policies.
#[derive(Clone)]
pub struct Timing {
    clock: Arc<dyn Clock>,
    jitter: Arc<dyn Jitter>,
}

impl Timing {
    /// Combines injectable clock and jitter implementations.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, jitter: Arc<dyn Jitter>) -> Self {
        Self { clock, jitter }
    }

    /// Creates production timing backed by Tokio and entropy-seeded equal jitter.
    #[must_use]
    pub fn production() -> Self {
        Self::new(Arc::new(TokioClock), Arc::new(EqualJitter::from_entropy()))
    }

    /// Waits for an exact duration without applying jitter.
    pub async fn sleep(&self, duration: Duration) {
        self.clock.sleep(duration).await;
    }

    /// Applies the configured jitter policy and waits for the resulting delay.
    pub async fn sleep_jittered(&self, nominal: Duration) {
        self.clock.sleep(self.jitter.apply(nominal)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;

    use super::{Clock, EqualJitter, Jitter, Timing};

    #[derive(Default)]
    struct RecordingClock(std::sync::Mutex<Vec<Duration>>);

    #[async_trait]
    impl Clock for RecordingClock {
        async fn sleep(&self, duration: Duration) {
            self.0.lock().expect("recording clock lock").push(duration);
        }
    }

    struct HalfJitter;

    impl Jitter for HalfJitter {
        fn apply(&self, nominal: Duration) -> Duration {
            nominal / 2
        }
    }

    #[test]
    fn equal_jitter_stays_within_inclusive_half_to_full_range() {
        let jitter = EqualJitter::from_entropy();
        let nominal = Duration::from_secs(30);
        for _ in 0..256 {
            let actual = jitter.apply(nominal);
            assert!(actual >= Duration::from_secs(15));
            assert!(actual <= nominal);
        }
    }

    #[tokio::test]
    async fn timing_routes_exact_and_jittered_delays_through_one_clock() {
        let clock = Arc::new(RecordingClock::default());
        let timing = Timing::new(clock.clone(), Arc::new(HalfJitter));

        timing.sleep(Duration::from_secs(8)).await;
        timing.sleep_jittered(Duration::from_secs(8)).await;

        assert_eq!(*clock.0.lock().expect("recording clock lock"), [Duration::from_secs(8), Duration::from_secs(4)]);
    }
}
