use std::{collections::BTreeSet, str::FromStr, sync::Arc};

use kaspa_consensus_core::network::NetworkId;
use kgi_model::block::BlockHash;
use sqlx::{PgConnection, Row};

use crate::{
    error::{StorageError, StorageRejection},
    generation::NodeMetadata,
    migration,
};

const MIGRATION_TABLE: &str = "_sqlx_migrations";
const KGI_TABLES: [&str; 6] = ["administrative_metadata", "block_identifiers", "blocks", "levels", "node_metadata", "parents"];
const KGI_COLUMNS: [&str; 27] = [
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
    "node_metadata.singleton:boolean:bool:NO:NO",
    "node_metadata.network_id:text:text:NO:NO",
    "node_metadata.genesis_hash:bytea:bytea:NO:NO",
    "node_metadata.db_pp_blue_score:bigint:int8:NO:NO",
    "parents.child_id:bigint:int8:NO:NO",
    "parents.parent_id:bigint:int8:NO:NO",
    "parents.child_level:bigint:int8:NO:NO",
    "parents.child_slot:bigint:int8:NO:NO",
    "parents.parent_level:bigint:int8:NO:NO",
    "parents.parent_slot:bigint:int8:NO:NO",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DatabaseState {
    Uninitialized,
    Empty(NodeMetadata),
    Initialized(NodeMetadata),
    Inconsistent { binding: DatabaseBinding, metadata: Option<NodeMetadata> },
}

impl DatabaseState {
    pub(crate) fn binding(&self) -> Option<DatabaseBinding> {
        match self {
            Self::Uninitialized => None,
            Self::Empty(metadata) | Self::Initialized(metadata) => Some(DatabaseBinding::from(metadata)),
            Self::Inconsistent { binding, .. } => Some(*binding),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DatabaseBinding {
    pub(crate) network_id: NetworkId,
    pub(crate) genesis_hash: BlockHash,
}

impl From<&NodeMetadata> for DatabaseBinding {
    fn from(metadata: &NodeMetadata) -> Self {
        Self { network_id: metadata.network_id(), genesis_hash: metadata.genesis_hash() }
    }
}

struct StoredMetadata {
    binding: DatabaseBinding,
    db_pp_blue_score: Option<u64>,
}

impl StoredMetadata {
    fn validated(&self) -> Option<NodeMetadata> {
        self.db_pp_blue_score.map(|score| NodeMetadata::new(self.binding.network_id, self.binding.genesis_hash, score))
    }
}

pub(crate) async fn prepare(connection: &mut PgConnection) -> Result<DatabaseState, StorageError> {
    validate_migratable_layout(connection).await?;
    reject_newer_schema(connection).await?;
    migration::migrate(connection).await?;
    validate_current_layout(connection).await?;
    classify(connection).await
}

pub(crate) async fn classify(connection: &mut PgConnection) -> Result<DatabaseState, StorageError> {
    validate_administrative_metadata(connection).await?;
    let metadata = read_metadata(connection).await?;
    let counts = processing_counts(connection).await?;

    let Some(stored) = metadata else {
        if counts.is_empty() {
            return Ok(DatabaseState::Uninitialized);
        }
        return Err(StorageRejection::UnsupportedSchema {
            diagnostic: Arc::from("processing data exists without complete node metadata"),
        }
        .into());
    };

    let Some(metadata) = stored.validated() else {
        return Ok(DatabaseState::Inconsistent { binding: stored.binding, metadata: None });
    };

    if counts.is_empty() {
        return if metadata.db_pp_blue_score() == 0 {
            Ok(DatabaseState::Empty(metadata))
        } else {
            Ok(DatabaseState::Inconsistent { binding: stored.binding, metadata: Some(metadata) })
        };
    }

    if processing_contents_are_coherent(connection, &metadata).await? {
        Ok(DatabaseState::Initialized(metadata))
    } else {
        Ok(DatabaseState::Inconsistent { binding: stored.binding, metadata: Some(metadata) })
    }
}

async fn validate_migratable_layout(connection: &mut PgConnection) -> Result<(), StorageError> {
    let tables = table_names(connection).await?;
    if tables.is_empty() {
        return Ok(());
    }

    let known: BTreeSet<&str> = KGI_TABLES.iter().copied().chain(std::iter::once(MIGRATION_TABLE)).collect();
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

async fn validate_administrative_metadata(connection: &mut PgConnection) -> Result<(), StorageError> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM administrative_metadata")
        .fetch_one(connection)
        .await
        .map_err(|error| StorageError::database("administrative-metadata inspection", error))?;
    if count == 1 {
        Ok(())
    } else {
        Err(StorageRejection::UnsupportedSchema {
            diagnostic: Arc::from(format!("expected one administrative-metadata row, observed {count}")),
        }
        .into())
    }
}

async fn read_metadata(connection: &mut PgConnection) -> Result<Option<StoredMetadata>, StorageError> {
    let rows = sqlx::query("SELECT network_id, genesis_hash, db_pp_blue_score FROM node_metadata")
        .fetch_all(connection)
        .await
        .map_err(|error| StorageError::database("node-metadata read", error))?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => {
            let network_text: String = row.try_get("network_id").map_err(|error| StorageError::invalid_metadata(error.to_string()))?;
            let network_id = NetworkId::from_str(&network_text)
                .map_err(|error| StorageError::invalid_metadata(format!("invalid network ID {network_text:?}: {error}")))?;
            let genesis_bytes: Vec<u8> =
                row.try_get("genesis_hash").map_err(|error| StorageError::invalid_metadata(error.to_string()))?;
            let genesis_hash = BlockHash::try_from(genesis_bytes.as_slice())
                .map_err(|error| StorageError::invalid_metadata(format!("invalid Genesis hash: {error}")))?;
            let score: i64 = row.try_get("db_pp_blue_score").map_err(|error| StorageError::invalid_metadata(error.to_string()))?;
            Ok(Some(StoredMetadata {
                binding: DatabaseBinding { network_id, genesis_hash },
                db_pp_blue_score: u64::try_from(score).ok(),
            }))
        }
        _ => Err(StorageError::invalid_metadata("multiple node-metadata rows")),
    }
}

#[derive(Clone, Copy)]
struct ProcessingCounts {
    identifiers: i64,
    blocks: i64,
    levels: i64,
    parents: i64,
}

impl ProcessingCounts {
    fn is_empty(self) -> bool {
        self.identifiers == 0 && self.blocks == 0 && self.levels == 0 && self.parents == 0
    }
}

async fn processing_counts(connection: &mut PgConnection) -> Result<ProcessingCounts, StorageError> {
    let row = sqlx::query(
        "SELECT
            (SELECT COUNT(*) FROM block_identifiers) AS identifiers,
            (SELECT COUNT(*) FROM blocks) AS blocks,
            (SELECT COUNT(*) FROM levels) AS levels,
            (SELECT COUNT(*) FROM parents) AS parents",
    )
    .fetch_one(connection)
    .await
    .map_err(|error| StorageError::database("processing-state inspection", error))?;
    Ok(ProcessingCounts {
        identifiers: row.try_get("identifiers").map_err(|error| StorageError::database("identifier count decode", error))?,
        blocks: row.try_get("blocks").map_err(|error| StorageError::database("block count decode", error))?,
        levels: row.try_get("levels").map_err(|error| StorageError::database("level count decode", error))?,
        parents: row.try_get("parents").map_err(|error| StorageError::database("parent count decode", error))?,
    })
}

async fn processing_contents_are_coherent(connection: &mut PgConnection, metadata: &NodeMetadata) -> Result<bool, StorageError> {
    let structural_fault: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1
            FROM blocks b
            LEFT JOIN levels l ON l.level = b.level
            WHERE l.level IS NULL OR b.slot >= l.size
        ) OR EXISTS (
            SELECT 1
            FROM levels l
            LEFT JOIN LATERAL (
                SELECT COUNT(*) AS count, MIN(slot) AS min_slot, MAX(slot) AS max_slot
                FROM blocks b WHERE b.level = l.level
            ) actual ON TRUE
            WHERE actual.count <> l.size OR actual.min_slot <> 0 OR actual.max_slot <> l.size - 1
        ) OR EXISTS (
            SELECT 1
            FROM blocks b
            CROSS JOIN LATERAL unnest(b.blue_merge_set || b.red_merge_set) AS merged(id)
            LEFT JOIN block_identifiers i ON i.id = merged.id
            WHERE i.id IS NULL
        ) OR EXISTS (
            SELECT 1
            FROM parents p
            LEFT JOIN blocks child ON child.id = p.child_id
            LEFT JOIN block_identifiers parent_identity ON parent_identity.id = p.parent_id
            LEFT JOIN blocks parent ON parent.id = p.parent_id
            WHERE child.id IS NULL
               OR parent_identity.id IS NULL
               OR child.level <> p.child_level
               OR child.slot <> p.child_slot
               OR (parent.id IS NULL AND (p.parent_level <> 0 OR p.parent_slot <> 0))
               OR (parent.id IS NOT NULL AND (parent.level <> p.parent_level OR parent.slot <> p.parent_slot))
        )",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|error| StorageError::database("processing-invariant inspection", error))?;
    if structural_fault {
        return Ok(false);
    }

    let anchor = sqlx::query(
        "SELECT identity.hash, block.id, block.selected_parent_id, block.is_in_vspc
         FROM blocks block
         JOIN block_identifiers identity ON identity.id = block.id
         WHERE block.level = 1 AND block.slot = 0",
    )
    .fetch_optional(&mut *connection)
    .await
    .map_err(|error| StorageError::database("pruning-point inspection", error))?;
    let Some(anchor) = anchor else {
        return Ok(false);
    };

    let sink_exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM blocks WHERE is_in_vspc)")
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| StorageError::database("VSPC-sink inspection", error))?;
    if !sink_exists {
        return Ok(false);
    }

    let anchor_hash: Vec<u8> = anchor.try_get("hash").map_err(|error| StorageError::database("pruning-point hash decode", error))?;
    if anchor_hash.as_slice() != metadata.genesis_hash().as_bytes() {
        return Ok(true);
    }

    let anchor_id: i64 = anchor.try_get("id").map_err(|error| StorageError::database("pruning-point ID decode", error))?;
    let selected_parent_id: i64 =
        anchor.try_get("selected_parent_id").map_err(|error| StorageError::database("selected-parent decode", error))?;
    let is_in_vspc: bool = anchor.try_get("is_in_vspc").map_err(|error| StorageError::database("pruning-point VSPC decode", error))?;
    let genesis_shape_is_valid: bool = sqlx::query_scalar(
        "SELECT
            $1::BIGINT = 0
            AND $2::BOOLEAN
            AND NOT EXISTS (SELECT 1 FROM parents WHERE child_id = $3)
            AND NOT EXISTS (SELECT 1 FROM blocks WHERE id = $4)
            AND (SELECT COUNT(*) FROM block_identifiers identity
                 LEFT JOIN blocks block ON block.id = identity.id
                 WHERE block.id IS NULL) = 1",
    )
    .bind(i64::try_from(metadata.db_pp_blue_score()).unwrap_or(i64::MAX))
    .bind(is_in_vspc)
    .bind(anchor_id)
    .bind(selected_parent_id)
    .fetch_one(connection)
    .await
    .map_err(|error| StorageError::database("Genesis-anchor inspection", error))?;
    Ok(genesis_shape_is_valid)
}

fn display_names(names: &[String]) -> String {
    if names.is_empty() { "none".to_owned() } else { names.join(", ") }
}
