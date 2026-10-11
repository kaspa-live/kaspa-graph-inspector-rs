use std::{future::Future, time::Duration};

use kgi_core::timing::Timing;
use kgi_model::lifecycle::PersistenceFault;

use crate::error::{StorageError, sqlx_connection_lost};

const RETRY_DELAYS: [Duration; 3] = [Duration::from_millis(10), Duration::from_millis(50), Duration::from_millis(250)];

#[derive(Clone, Copy)]
pub(crate) enum DatabasePhase {
    BeforeCommit,
    Commit,
}

pub(crate) enum TransactionAttemptError<S> {
    Semantic(S),
    Storage(StorageError),
    Database { operation: &'static str, phase: DatabasePhase, error: sqlx::Error },
}

impl<S> TransactionAttemptError<S> {
    pub(crate) fn database(operation: &'static str, error: sqlx::Error) -> Self {
        Self::Database { operation, phase: DatabasePhase::BeforeCommit, error }
    }

    pub(crate) fn commit(operation: &'static str, error: sqlx::Error) -> Self {
        Self::Database { operation, phase: DatabasePhase::Commit, error }
    }
}

pub(crate) async fn run<T, S, F, Fut>(timing: &Timing, mut attempt: F) -> Result<Result<T, S>, StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, TransactionAttemptError<S>>>,
{
    let mut retry_index = 0;
    loop {
        match attempt().await {
            Ok(value) => return Ok(Ok(value)),
            Err(TransactionAttemptError::Semantic(error)) => return Ok(Err(error)),
            Err(TransactionAttemptError::Storage(error)) => return Err(error),
            Err(TransactionAttemptError::Database { error, .. }) if retryable(&error) => {
                let Some(nominal) = RETRY_DELAYS.get(retry_index).copied() else {
                    return Err(StorageError::Persistence(PersistenceFault::RetryExhausted));
                };
                retry_index += 1;
                timing.sleep_jittered(nominal).await;
            }
            Err(TransactionAttemptError::Database { operation: _, phase: DatabasePhase::Commit, error })
                if sqlx_connection_lost(&error) =>
            {
                return Err(StorageError::Persistence(PersistenceFault::AmbiguousCommit));
            }
            Err(TransactionAttemptError::Database { operation, phase: DatabasePhase::BeforeCommit, error })
                if sqlx_connection_lost(&error) =>
            {
                return Err(StorageError::database(operation, error));
            }
            Err(TransactionAttemptError::Database { .. }) => {
                return Err(StorageError::Persistence(PersistenceFault::DefiniteFailure));
            }
        }
    }
}

fn retryable(error: &sqlx::Error) -> bool {
    matches!(error.as_database_error().and_then(|database| database.code()).as_deref(), Some("40001" | "40P01"))
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::Cow,
        error::Error,
        fmt,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use kgi_core::timing::{Clock, Jitter, Timing};
    use kgi_model::lifecycle::PersistenceFault;
    use sqlx::error::{DatabaseError, ErrorKind};

    use super::{DatabasePhase, TransactionAttemptError, run};
    use crate::error::StorageError;

    struct RecordingClock(Mutex<Vec<Duration>>);

    #[async_trait]
    impl Clock for RecordingClock {
        async fn sleep(&self, duration: Duration) {
            self.0.lock().expect("recording clock lock").push(duration);
        }
    }

    struct IdentityJitter;

    impl Jitter for IdentityJitter {
        fn apply(&self, nominal: Duration) -> Duration {
            nominal
        }
    }

    #[derive(Debug)]
    struct CodedDatabaseError(&'static str);

    impl fmt::Display for CodedDatabaseError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "injected PostgreSQL error {}", self.0)
        }
    }

    impl Error for CodedDatabaseError {}

    impl DatabaseError for CodedDatabaseError {
        fn message(&self) -> &str {
            "injected PostgreSQL error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.0))
        }

        fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> ErrorKind {
            ErrorKind::Other
        }
    }

    fn database_error(code: &'static str) -> sqlx::Error {
        sqlx::Error::database(CodedDatabaseError(code))
    }

    #[tokio::test]
    async fn retryable_transactions_use_the_complete_nominal_sequence() {
        let clock = Arc::new(RecordingClock(Mutex::new(Vec::new())));
        let timing = Timing::new(clock.clone(), Arc::new(IdentityJitter));
        let attempts = AtomicUsize::new(0);
        let result = run(&timing, || {
            let attempt = attempts.fetch_add(1, Ordering::Relaxed);
            async move {
                match attempt {
                    0 => Err(TransactionAttemptError::<()>::database("attempt", database_error("40001"))),
                    1 => Err(TransactionAttemptError::<()>::database("attempt", database_error("40P01"))),
                    _ => Ok(7),
                }
            }
        })
        .await;

        assert_eq!(result, Ok(Ok(7)));
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        assert_eq!(*clock.0.lock().expect("recording clock lock"), [Duration::from_millis(10), Duration::from_millis(50)]);
    }

    #[tokio::test]
    async fn retry_exhaustion_and_commit_uncertainty_remain_distinct() {
        let clock = Arc::new(RecordingClock(Mutex::new(Vec::new())));
        let timing = Timing::new(clock.clone(), Arc::new(IdentityJitter));
        let attempts = AtomicUsize::new(0);
        let exhausted = run(&timing, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            async { Err::<(), _>(TransactionAttemptError::<()>::database("attempt", database_error("40001"))) }
        })
        .await;
        assert_eq!(exhausted, Err(StorageError::Persistence(PersistenceFault::RetryExhausted)));
        assert_eq!(attempts.load(Ordering::Relaxed), 4);
        assert_eq!(
            *clock.0.lock().expect("recording clock lock"),
            [Duration::from_millis(10), Duration::from_millis(50), Duration::from_millis(250)]
        );

        let ambiguous = run(&timing, || async {
            Err::<(), _>(TransactionAttemptError::<()>::Database {
                operation: "commit",
                phase: DatabasePhase::Commit,
                error: sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "lost acknowledgement")),
            })
        })
        .await;
        assert_eq!(ambiguous, Err(StorageError::Persistence(PersistenceFault::AmbiguousCommit)));
    }
}
