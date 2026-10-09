use std::sync::Arc;

use sqlx::{
    PgConnection,
    migrate::{MigrateError, Migrator},
};

use crate::error::{StorageError, StorageRejection, sqlx_connection_lost};

pub(crate) static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub(crate) fn current_version() -> i64 {
    MIGRATOR.iter().map(|migration| migration.version).max().unwrap_or(0)
}

pub(crate) async fn migrate(connection: &mut PgConnection) -> Result<(), StorageError> {
    // SQLx's public `run` wrapper cannot satisfy the spawned worker's higher-ranked
    // `Send` requirement for `&mut PgConnection`; `run_direct` performs the same
    // latest-version, non-skipping migration on this already acquired connection.
    MIGRATOR.run_direct(None, connection, false).await.map_err(classify_error)
}

pub(crate) fn classify_error(error: MigrateError) -> StorageError {
    let diagnostic = Arc::from(error.to_string());
    match execution_error(&error) {
        Some(error) if sqlx_connection_lost(error) => StorageError::ConnectionLost { operation: "migration execution", diagnostic },
        Some(error) if retryable_sqlstate(error) => StorageError::Database { operation: "migration execution", diagnostic },
        _ => StorageRejection::MigrationFailed { diagnostic }.into(),
    }
}

fn execution_error(error: &MigrateError) -> Option<&sqlx::Error> {
    match error {
        MigrateError::Execute(error) | MigrateError::ExecuteMigration(error, _) => Some(error),
        _ => None,
    }
}

fn retryable_sqlstate(error: &sqlx::Error) -> bool {
    matches!(error.as_database_error().and_then(|database| database.code()).as_deref(), Some("40001" | "40P01"))
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, error::Error, fmt};

    use sqlx::error::{DatabaseError, ErrorKind};

    use super::{MigrateError, classify_error};
    use crate::error::{StorageError, StorageRejection};

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

    fn execution_with_code(code: &'static str) -> MigrateError {
        MigrateError::Execute(sqlx::Error::database(CodedDatabaseError(code)))
    }

    #[test]
    fn only_retryable_migration_sqlstates_remain_transient() {
        let retryable =
            [execution_with_code("40001"), MigrateError::ExecuteMigration(sqlx::Error::database(CodedDatabaseError("40P01")), 1)];
        for error in retryable {
            assert!(matches!(classify_error(error), StorageError::Database { operation: "migration execution", .. }));
        }

        assert!(matches!(
            classify_error(execution_with_code("23505")),
            StorageError::Rejected(StorageRejection::MigrationFailed { .. })
        ));
    }

    #[test]
    fn migration_connection_loss_remains_transient() {
        let error = MigrateError::ExecuteMigration(
            sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "injected connection loss")),
            1,
        );
        assert!(matches!(classify_error(error), StorageError::ConnectionLost { operation: "migration execution", .. }));
    }

    #[test]
    fn migration_framework_failures_are_terminal() {
        assert!(matches!(classify_error(MigrateError::Dirty(1)), StorageError::Rejected(StorageRejection::MigrationFailed { .. })));
        assert!(matches!(
            classify_error(MigrateError::VersionMismatch(1)),
            StorageError::Rejected(StorageRejection::MigrationFailed { .. })
        ));
    }
}
