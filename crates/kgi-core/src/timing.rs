//! Injectable timing primitives shared by permanent service workers.

use std::{sync::Mutex, time::Duration};

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{EqualJitter, Jitter};

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
}
