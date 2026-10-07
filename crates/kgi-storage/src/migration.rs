use sqlx::{PgConnection, migrate::Migrator};

use crate::error::StorageError;

pub(crate) static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub(crate) fn current_version() -> i64 {
    MIGRATOR.iter().map(|migration| migration.version).max().unwrap_or(0)
}

pub(crate) async fn migrate(connection: &mut PgConnection) -> Result<(), StorageError> {
    MIGRATOR.run(connection).await.map_err(StorageError::migration)
}
