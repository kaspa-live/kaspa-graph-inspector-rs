use std::{sync::Mutex, time::Duration};

use async_trait::async_trait;
use rand::{Rng, SeedableRng, rngs::SmallRng};

#[async_trait]
pub(crate) trait Clock: Send + Sync {
    async fn sleep(&self, duration: Duration);
}

pub(crate) struct TokioClock;

#[async_trait]
impl Clock for TokioClock {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

pub(crate) trait Jitter: Send + Sync {
    fn apply(&self, nominal: Duration) -> Duration;
}

pub(crate) struct EqualJitter {
    random: Mutex<SmallRng>,
}

impl EqualJitter {
    pub(crate) fn from_entropy() -> Self {
        Self { random: Mutex::new(SmallRng::from_entropy()) }
    }
}

impl Jitter for EqualJitter {
    fn apply(&self, nominal: Duration) -> Duration {
        let upper = u64::try_from(nominal.as_nanos()).expect("NodeService delay fits u64 nanoseconds");
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
