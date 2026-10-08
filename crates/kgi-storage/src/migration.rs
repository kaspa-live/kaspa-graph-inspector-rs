use sqlx::{PgConnection, migrate::Migrator};

use crate::error::StorageError;

pub(crate) static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub(crate) fn current_version() -> i64 {
    MIGRATOR.iter().map(|migration| migration.version).max().unwrap_or(0)
}

pub(crate) async fn migrate(connection: &mut PgConnection) -> Result<(), StorageError> {
    // SQLx's public `run` wrapper cannot satisfy the spawned worker's higher-ranked
    // `Send` requirement for `&mut PgConnection`; `run_direct` performs the same
    // latest-version, non-skipping migration on this already acquired connection.
    MIGRATOR.run_direct(None, connection, false).await.map_err(StorageError::migration)
}
