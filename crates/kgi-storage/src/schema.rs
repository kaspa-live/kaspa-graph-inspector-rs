use std::{collections::BTreeSet, future::Future, pin::Pin, sync::Arc};

use sqlx::{PgConnection, Row};

use crate::{
    error::{StorageError, StorageRejection},
    migration,
};

const MIGRATION_TABLE: &str = "_sqlx_migrations";
const KGI_TABLES: [&str; 7] =
    ["administrative_metadata", "block_identifiers", "blocks", "levels", "network_metadata", "parents", "processing_metadata"];
const KGI_COLUMNS: [&str; 28] = [
    "administrative_metadata.singleton:boolean:bool:NO:NO",
    "administrative_metadata.last_reinitialization_token:text:text:YES:NO",
    "block_identifiers.id:bigint:int8:NO:YES",
    "block_identifiers.hash:bytea:bytea:NO:NO",
    "blocks.id:bigint:int8:NO:NO",
    "blocks.timestamp:bigint:int8:NO:NO",
    "blocks.daa_score:bigint:int8:NO:NO",
    "blocks.level:bigint:int8:NO:NO",
    "blocks.slot:bigint:int8:NO:NO",
    "blocks.selected_parent_id:bigint:int8:NO:NO",
    "blocks.color:smallint:int2:NO:NO",
    "blocks.is_in_vspc:boolean:bool:NO:NO",
    "blocks.blue_merge_set:ARRAY:_int8:NO:NO",
    "blocks.red_merge_set:ARRAY:_int8:NO:NO",
    "levels.level:bigint:int8:NO:NO",
    "levels.size:bigint:int8:NO:NO",
    "levels.daa_score:bigint:int8:NO:NO",
    "network_metadata.singleton:boolean:bool:NO:NO",
    "network_metadata.network_id:text:text:NO:NO",
    "network_metadata.genesis_hash:bytea:bytea:NO:NO",
    "parents.child_id:bigint:int8:NO:NO",
    "parents.parent_id:bigint:int8:NO:NO",
    "parents.child_level:bigint:int8:NO:NO",
    "parents.child_slot:bigint:int8:NO:NO",
    "parents.parent_level:bigint:int8:NO:NO",
    "parents.parent_slot:bigint:int8:NO:NO",
    "processing_metadata.singleton:boolean:bool:NO:NO",
    "processing_metadata.db_pp_blue_score:bigint:int8:NO:NO",
];

pub(crate) fn prepare(connection: &mut PgConnection) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + '_>> {
    Box::pin(async move {
        validate_migratable_layout(connection).await?;
        reject_newer_schema(connection).await?;
        migration::migrate(connection).await?;
        validate_current_layout(connection).await
    })
}

async fn validate_migratable_layout(connection: &mut PgConnection) -> Result<(), StorageError> {
    let tables = table_names(connection).await?;
    if tables.is_empty() {
        return Ok(());
    }

    let known: BTreeSet<&str> = KGI_TABLES.iter().copied().chain([MIGRATION_TABLE]).collect();
    let unknown = tables.iter().filter(|table| !known.contains(table.as_str())).cloned().collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(
            StorageRejection::UnsupportedSchema { diagnostic: Arc::from(format!("unknown tables: {}", unknown.join(", "))) }.into()
        );
    }

    if !tables.contains(MIGRATION_TABLE) {
        return Err(StorageRejection::UnsupportedSchema {
            diagnostic: Arc::from("KGI-like tables exist without SQLx migration history"),
        }
        .into());
    }

    Ok(())
}

async fn reject_newer_schema(connection: &mut PgConnection) -> Result<(), StorageError> {
    if !table_names(connection).await?.contains(MIGRATION_TABLE) {
        return Ok(());
    }

    let observed: Option<i64> = sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| StorageError::database("schema-version inspection", error))?;
    let supported = migration::current_version();
    if observed.is_some_and(|version| version > supported) {
        return Err(StorageRejection::SchemaTooNew { observed: observed.expect("checked Some"), supported }.into());
    }
    Ok(())
}

async fn validate_current_layout(connection: &mut PgConnection) -> Result<(), StorageError> {
    let actual = table_names(connection).await?;
    let expected = KGI_TABLES.iter().copied().chain(std::iter::once(MIGRATION_TABLE)).map(str::to_owned).collect::<BTreeSet<_>>();
    if actual != expected {
        let missing = expected.difference(&actual).cloned().collect::<Vec<_>>();
        let unexpected = actual.difference(&expected).cloned().collect::<Vec<_>>();
        return Err(StorageRejection::UnsupportedSchema {
            diagnostic: Arc::from(format!(
                "current schema layout mismatch (missing: {}; unexpected: {})",
                display_names(&missing),
                display_names(&unexpected)
            )),
        }
        .into());
    }

    let actual_columns = column_signatures(connection).await?;
    let expected_columns = KGI_COLUMNS.iter().copied().map(str::to_owned).collect::<BTreeSet<_>>();
    if actual_columns != expected_columns {
        let missing = expected_columns.difference(&actual_columns).cloned().collect::<Vec<_>>();
        let unexpected = actual_columns.difference(&expected_columns).cloned().collect::<Vec<_>>();
        return Err(StorageRejection::UnsupportedSchema {
            diagnostic: Arc::from(format!(
                "current schema column mismatch (missing: {}; unexpected: {})",
                display_names(&missing),
                display_names(&unexpected)
            )),
        }
        .into());
    }
    Ok(())
}

async fn table_names(connection: &mut PgConnection) -> Result<BTreeSet<String>, StorageError> {
    sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
         ORDER BY table_name",
    )
    .fetch_all(connection)
    .await
    .map(|tables| tables.into_iter().collect())
    .map_err(|error| StorageError::database("schema inspection", error))
}

async fn column_signatures(connection: &mut PgConnection) -> Result<BTreeSet<String>, StorageError> {
    let rows = sqlx::query(
        "SELECT table_name, column_name, data_type, udt_name, is_nullable, is_identity
         FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = ANY($1)
         ORDER BY table_name, ordinal_position",
    )
    .bind(KGI_TABLES.as_slice())
    .fetch_all(connection)
    .await
    .map_err(|error| StorageError::database("schema-column inspection", error))?;
    rows.into_iter()
        .map(|row| {
            let table: String = row.try_get("table_name").map_err(|error| StorageError::database("table-name decode", error))?;
            let column: String = row.try_get("column_name").map_err(|error| StorageError::database("column-name decode", error))?;
            let data_type: String = row.try_get("data_type").map_err(|error| StorageError::database("column-type decode", error))?;
            let udt_name: String = row.try_get("udt_name").map_err(|error| StorageError::database("column-UDT decode", error))?;
            let nullable: String =
                row.try_get("is_nullable").map_err(|error| StorageError::database("column-nullability decode", error))?;
            let identity: String =
                row.try_get("is_identity").map_err(|error| StorageError::database("column-identity decode", error))?;
            Ok(format!("{table}.{column}:{data_type}:{udt_name}:{nullable}:{identity}"))
        })
        .collect()
}

fn display_names(names: &[String]) -> String {
    if names.is_empty() { "none".to_owned() } else { names.join(", ") }
}
