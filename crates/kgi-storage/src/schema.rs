use std::{collections::BTreeSet, sync::Arc};

use sqlx::{PgConnection, Row};

use crate::{
    error::{StorageError, StorageRejection},
    migration,
};

const MIGRATION_TABLE: &str = "_sqlx_migrations";
const KGI_TABLES: [&str; 7] =
    ["administrative_metadata", "block_identifiers", "blocks", "levels", "network_metadata", "parents", "processing_metadata"];
const KGI_COLUMNS: [&str; 28] = [
    "administrative_metadata.singleton:boolean:bool:NO:NO:<none>:true",
    "administrative_metadata.last_reinitialization_token:text:text:YES:NO:<none>:<none>",
    "block_identifiers.id:bigint:int8:NO:YES:BY DEFAULT:<none>",
    "block_identifiers.hash:bytea:bytea:NO:NO:<none>:<none>",
    "blocks.id:bigint:int8:NO:NO:<none>:<none>",
    "blocks.timestamp:bigint:int8:NO:NO:<none>:<none>",
    "blocks.daa_score:bigint:int8:NO:NO:<none>:<none>",
    "blocks.level:bigint:int8:NO:NO:<none>:<none>",
    "blocks.slot:bigint:int8:NO:NO:<none>:<none>",
    "blocks.selected_parent_id:bigint:int8:NO:NO:<none>:<none>",
    "blocks.color:smallint:int2:NO:NO:<none>:<none>",
    "blocks.is_in_vspc:boolean:bool:NO:NO:<none>:<none>",
    "blocks.blue_merge_set:ARRAY:_int8:NO:NO:<none>:<none>",
    "blocks.red_merge_set:ARRAY:_int8:NO:NO:<none>:<none>",
    "levels.level:bigint:int8:NO:NO:<none>:<none>",
    "levels.size:bigint:int8:NO:NO:<none>:<none>",
    "levels.daa_score:bigint:int8:NO:NO:<none>:'9223372036854775807'::bigint",
    "network_metadata.singleton:boolean:bool:NO:NO:<none>:true",
    "network_metadata.network_id:text:text:NO:NO:<none>:<none>",
    "network_metadata.genesis_hash:bytea:bytea:NO:NO:<none>:<none>",
    "parents.child_id:bigint:int8:NO:NO:<none>:<none>",
    "parents.parent_id:bigint:int8:NO:NO:<none>:<none>",
    "parents.child_level:bigint:int8:NO:NO:<none>:<none>",
    "parents.child_slot:bigint:int8:NO:NO:<none>:<none>",
    "parents.parent_level:bigint:int8:NO:NO:<none>:<none>",
    "parents.parent_slot:bigint:int8:NO:NO:<none>:<none>",
    "processing_metadata.singleton:boolean:bool:NO:NO:<none>:true",
    "processing_metadata.db_pp_blue_score:bigint:int8:NO:NO:<none>:<none>",
];
const KGI_CONSTRAINTS: [&str; 29] = [
    "administrative_metadata.administrative_metadata_pkey:p:true:false:false:PRIMARY KEY (singleton)",
    "administrative_metadata.administrative_metadata_singleton_check:c:true:false:false:CHECK (singleton)",
    "block_identifiers.block_identifiers_hash_check:c:true:false:false:CHECK (octet_length(hash) = 32)",
    "block_identifiers.block_identifiers_hash_key:u:true:false:false:UNIQUE (hash)",
    "block_identifiers.block_identifiers_pkey:p:true:false:false:PRIMARY KEY (id)",
    "blocks.blocks_color_check:c:true:false:false:CHECK (color >= 0 AND color <= 2)",
    "blocks.blocks_daa_score_check:c:true:false:false:CHECK (daa_score >= 0 AND daa_score < '9223372036854775807'::bigint)",
    "blocks.blocks_id_fkey:f:true:false:false:FOREIGN KEY (id) REFERENCES block_identifiers(id)",
    "blocks.blocks_level_check:c:true:false:false:CHECK (level > 0)",
    "blocks.blocks_level_slot_key:u:true:false:false:UNIQUE (level, slot)",
    "blocks.blocks_pkey:p:true:false:false:PRIMARY KEY (id)",
    "blocks.blocks_selected_parent_id_fkey:f:true:false:false:FOREIGN KEY (selected_parent_id) REFERENCES block_identifiers(id)",
    "blocks.blocks_slot_check:c:true:false:false:CHECK (slot >= 0)",
    "levels.levels_daa_score_check:c:true:false:false:CHECK (daa_score >= 0)",
    "levels.levels_level_check:c:true:false:false:CHECK (level > 0)",
    "levels.levels_pkey:p:true:false:false:PRIMARY KEY (level)",
    "levels.levels_size_check:c:true:false:false:CHECK (size > 0)",
    "network_metadata.network_metadata_genesis_hash_check:c:true:false:false:CHECK (octet_length(genesis_hash) = 32)",
    "network_metadata.network_metadata_pkey:p:true:false:false:PRIMARY KEY (singleton)",
    "network_metadata.network_metadata_singleton_check:c:true:false:false:CHECK (singleton)",
    "parents.parents_child_level_check:c:true:false:false:CHECK (child_level > 0)",
    "parents.parents_child_slot_check:c:true:false:false:CHECK (child_slot >= 0)",
    "parents.parents_parent_level_check:c:true:false:false:CHECK (parent_level >= 0)",
    "parents.parents_parent_slot_check:c:true:false:false:CHECK (parent_slot >= 0)",
    "parents.parents_check:c:true:false:false:CHECK (parent_level > 0 OR parent_slot = 0)",
    "parents.parents_pkey:p:true:false:false:PRIMARY KEY (child_id, parent_id)",
    "processing_metadata.processing_metadata_db_pp_blue_score_check:c:true:false:false:CHECK (db_pp_blue_score >= 0)",
    "processing_metadata.processing_metadata_pkey:p:true:false:false:PRIMARY KEY (singleton)",
    "processing_metadata.processing_metadata_singleton_check:c:true:false:false:CHECK (singleton)",
];
const KGI_INDEXES: [&str; 13] = [
    "administrative_metadata.administrative_metadata_pkey:btree:true:true:true:true:1:1:singleton#0:<none>",
    "block_identifiers.block_identifiers_hash_key:btree:true:false:true:true:1:1:hash#0:<none>",
    "block_identifiers.block_identifiers_pkey:btree:true:true:true:true:1:1:id#0:<none>",
    "blocks.blocks_level_slot_key:btree:true:false:true:true:2:2:level#0,slot#0:<none>",
    "blocks.blocks_pkey:btree:true:true:true:true:1:1:id#0:<none>",
    "blocks.blocks_vspc_sink_idx:btree:false:false:true:true:1:1:id#3:is_in_vspc",
    "levels.levels_daa_score_level_idx:btree:false:false:true:true:2:2:daa_score#3,level#3:<none>",
    "levels.levels_pkey:btree:true:true:true:true:1:1:level#0:<none>",
    "network_metadata.network_metadata_pkey:btree:true:true:true:true:1:1:singleton#0:<none>",
    "parents.parents_child_coordinate_idx:btree:false:false:true:true:2:2:child_level#0,child_slot#0:<none>",
    "parents.parents_parent_coordinate_idx:btree:false:false:true:true:2:2:parent_level#0,parent_slot#0:parent_level > 0",
    "parents.parents_pkey:btree:true:true:true:true:2:2:child_id#0,parent_id#0:<none>",
    "processing_metadata.processing_metadata_pkey:btree:true:true:true:true:1:1:singleton#0:<none>",
];

pub(crate) async fn prepare(connection: &mut PgConnection) -> Result<(), StorageError> {
    validate_migratable_layout(connection).await?;
    reject_newer_schema(connection).await?;
    migration::migrate(connection).await?;
    validate_current_layout(connection).await
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

    let actual_constraints = constraint_signatures(connection).await?;
    let expected_constraints = KGI_CONSTRAINTS.iter().copied().map(str::to_owned).collect::<BTreeSet<_>>();
    if actual_constraints != expected_constraints {
        let missing = expected_constraints.difference(&actual_constraints).cloned().collect::<Vec<_>>();
        let unexpected = actual_constraints.difference(&expected_constraints).cloned().collect::<Vec<_>>();
        return Err(StorageRejection::UnsupportedSchema {
            diagnostic: Arc::from(format!(
                "current schema constraint mismatch (missing: {}; unexpected: {})",
                display_names(&missing),
                display_names(&unexpected)
            )),
        }
        .into());
    }

    let actual_indexes = index_signatures(connection).await?;
    let expected_indexes = KGI_INDEXES.iter().copied().map(str::to_owned).collect::<BTreeSet<_>>();
    if !expected_indexes.is_subset(&actual_indexes) {
        let missing = expected_indexes.difference(&actual_indexes).cloned().collect::<Vec<_>>();
        return Err(StorageRejection::UnsupportedSchema {
            diagnostic: Arc::from(format!("current schema index mismatch (missing: {})", display_names(&missing))),
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
        "SELECT table_name, column_name, data_type, udt_name, is_nullable,
                is_identity, identity_generation, column_default
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
            let identity_generation: Option<String> = row
                .try_get("identity_generation")
                .map_err(|error| StorageError::database("column-identity-generation decode", error))?;
            let default: Option<String> =
                row.try_get("column_default").map_err(|error| StorageError::database("column-default decode", error))?;
            Ok(format!(
                "{table}.{column}:{data_type}:{udt_name}:{nullable}:{identity}:{}:{}",
                identity_generation.as_deref().unwrap_or("<none>"),
                default.as_deref().unwrap_or("<none>")
            ))
        })
        .collect()
}

async fn constraint_signatures(connection: &mut PgConnection) -> Result<BTreeSet<String>, StorageError> {
    let rows = sqlx::query(
        "SELECT table_class.relname AS table_name,
                constraint_record.conname AS constraint_name,
                constraint_record.contype::text AS constraint_type,
                constraint_record.convalidated,
                constraint_record.condeferrable,
                constraint_record.condeferred,
                pg_get_constraintdef(constraint_record.oid, true) AS definition
         FROM pg_catalog.pg_constraint AS constraint_record
         JOIN pg_catalog.pg_class AS table_class
           ON table_class.oid = constraint_record.conrelid
         JOIN pg_catalog.pg_namespace AS table_namespace
           ON table_namespace.oid = table_class.relnamespace
         WHERE table_namespace.nspname = current_schema()
           AND table_class.relname = ANY($1)
         ORDER BY table_class.relname, constraint_record.conname",
    )
    .bind(KGI_TABLES.as_slice())
    .fetch_all(connection)
    .await
    .map_err(|error| StorageError::database("schema-constraint inspection", error))?;
    rows.into_iter()
        .map(|row| {
            let table: String = row.try_get("table_name").map_err(|error| StorageError::database("table-name decode", error))?;
            let name: String =
                row.try_get("constraint_name").map_err(|error| StorageError::database("constraint-name decode", error))?;
            let constraint_type: String =
                row.try_get("constraint_type").map_err(|error| StorageError::database("constraint-type decode", error))?;
            let validated: bool =
                row.try_get("convalidated").map_err(|error| StorageError::database("constraint-validation decode", error))?;
            let deferrable: bool =
                row.try_get("condeferrable").map_err(|error| StorageError::database("constraint-deferrability decode", error))?;
            let deferred: bool =
                row.try_get("condeferred").map_err(|error| StorageError::database("constraint-deferred decode", error))?;
            let definition: String =
                row.try_get("definition").map_err(|error| StorageError::database("constraint-definition decode", error))?;
            Ok(format!("{table}.{name}:{constraint_type}:{validated}:{deferrable}:{deferred}:{definition}"))
        })
        .collect()
}

async fn index_signatures(connection: &mut PgConnection) -> Result<BTreeSet<String>, StorageError> {
    let rows = sqlx::query(
        "SELECT table_class.relname AS table_name,
                index_class.relname AS index_name,
                access_method.amname AS access_method,
                index_record.indisunique,
                index_record.indisprimary,
                index_record.indisvalid,
                index_record.indisready,
                index_record.indnkeyatts,
                index_record.indnatts,
                ARRAY(
                    SELECT pg_get_indexdef(index_record.indexrelid, key_position, true)
                           || '#' || index_record.indoption[key_position - 1]::text
                    FROM generate_series(1, index_record.indnkeyatts) AS key_position
                    ORDER BY key_position
                ) AS keys,
                pg_get_expr(index_record.indpred, index_record.indrelid, true) AS predicate
         FROM pg_catalog.pg_index AS index_record
         JOIN pg_catalog.pg_class AS table_class
           ON table_class.oid = index_record.indrelid
         JOIN pg_catalog.pg_namespace AS table_namespace
           ON table_namespace.oid = table_class.relnamespace
         JOIN pg_catalog.pg_class AS index_class
           ON index_class.oid = index_record.indexrelid
         JOIN pg_catalog.pg_am AS access_method
           ON access_method.oid = index_class.relam
         WHERE table_namespace.nspname = current_schema()
           AND table_class.relname = ANY($1)
         ORDER BY table_class.relname, index_class.relname",
    )
    .bind(KGI_TABLES.as_slice())
    .fetch_all(connection)
    .await
    .map_err(|error| StorageError::database("schema-index inspection", error))?;
    rows.into_iter()
        .map(|row| {
            let table: String = row.try_get("table_name").map_err(|error| StorageError::database("table-name decode", error))?;
            let name: String = row.try_get("index_name").map_err(|error| StorageError::database("index-name decode", error))?;
            let access_method: String =
                row.try_get("access_method").map_err(|error| StorageError::database("index-access-method decode", error))?;
            let unique: bool = row.try_get("indisunique").map_err(|error| StorageError::database("index-uniqueness decode", error))?;
            let primary: bool = row.try_get("indisprimary").map_err(|error| StorageError::database("index-primary decode", error))?;
            let valid: bool = row.try_get("indisvalid").map_err(|error| StorageError::database("index-validity decode", error))?;
            let ready: bool = row.try_get("indisready").map_err(|error| StorageError::database("index-readiness decode", error))?;
            let key_count: i16 =
                row.try_get("indnkeyatts").map_err(|error| StorageError::database("index-key-count decode", error))?;
            let attribute_count: i16 =
                row.try_get("indnatts").map_err(|error| StorageError::database("index-attribute-count decode", error))?;
            let keys: Vec<String> = row.try_get("keys").map_err(|error| StorageError::database("index-key decode", error))?;
            let predicate: Option<String> =
                row.try_get("predicate").map_err(|error| StorageError::database("index-predicate decode", error))?;
            Ok(format!(
                "{table}.{name}:{access_method}:{unique}:{primary}:{valid}:{ready}:{key_count}:{attribute_count}:{}:{}",
                keys.join(","),
                predicate.as_deref().unwrap_or("<none>")
            ))
        })
        .collect()
}

fn display_names(names: &[String]) -> String {
    if names.is_empty() { "none".to_owned() } else { names.join(", ") }
}
