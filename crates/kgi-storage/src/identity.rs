use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use kgi_model::block::{BlockCoordinate, BlockHash, BlockPresence, CompactId};
use sqlx::{PgPool, Row};

use crate::{
    cache::{CachedIdentity, CachedMaterializedBlock, ProcessingCaches},
    error::StorageError,
};

struct StoredIdentity {
    identity: CachedIdentity,
    block: Option<CachedMaterializedBlock>,
}

pub(crate) enum MaterializedIdResolution {
    Resolved(Box<[CompactId]>),
    NonMaterialized { missing: Arc<[BlockHash]>, identity_only: Arc<[BlockHash]> },
}

pub(crate) async fn block_presence(pool: &PgPool, caches: &ProcessingCaches, hash: BlockHash) -> Result<BlockPresence, StorageError> {
    if let Some(identity) = caches.identity(hash) {
        if !identity.materialized {
            return Ok(BlockPresence::BoundaryIdentity { id: identity.id });
        }
        if let Some(block) = caches.materialized_block(identity.id) {
            return Ok(BlockPresence::Materialized { id: identity.id, coordinate: block.coordinate });
        }
    }

    let row = sqlx::query(
        "SELECT identity.id, block.level, block.slot, block.selected_parent_id
         FROM block_identifiers identity
         LEFT JOIN blocks block ON block.id = identity.id
         WHERE identity.hash = $1",
    )
    .bind(hash.as_bytes().as_slice())
    .fetch_optional(pool)
    .await
    .map_err(|error| StorageError::database("block-presence lookup", error))?;

    let Some(row) = row else {
        return Ok(BlockPresence::Absent);
    };
    let stored = decode_identity(&row)?;
    publish_stored_identity(caches, hash, &stored);
    Ok(match stored.block {
        Some(block) => BlockPresence::Materialized { id: stored.identity.id, coordinate: block.coordinate },
        None => BlockPresence::BoundaryIdentity { id: stored.identity.id },
    })
}

pub(crate) async fn resolve_materialized_ids(
    pool: &PgPool,
    caches: &ProcessingCaches,
    hashes: &[BlockHash],
) -> Result<MaterializedIdResolution, StorageError> {
    let mut seen = HashSet::with_capacity(hashes.len());
    let distinct: Vec<_> = hashes.iter().copied().filter(|hash| seen.insert(*hash)).collect();
    let mut resolved = HashMap::with_capacity(distinct.len());
    let mut misses = Vec::new();

    for hash in &distinct {
        match caches.identity(*hash) {
            Some(identity) if !identity.materialized || caches.materialized_block(identity.id).is_some() => {
                resolved.insert(*hash, identity);
            }
            Some(_) | None => misses.push(*hash),
        }
    }

    if !misses.is_empty() {
        let encoded: Vec<Vec<u8>> = misses.iter().map(|hash| hash.as_bytes().to_vec()).collect();
        let rows = sqlx::query(
            "SELECT identity.hash, identity.id, block.level, block.slot, block.selected_parent_id
             FROM block_identifiers identity
             LEFT JOIN blocks block ON block.id = identity.id
             WHERE identity.hash = ANY($1::BYTEA[])",
        )
        .bind(encoded)
        .fetch_all(pool)
        .await
        .map_err(|error| StorageError::database("materialized-ID batch lookup", error))?;

        for row in rows {
            let encoded_hash: Vec<u8> =
                row.try_get("hash").map_err(|error| StorageError::database("materialized-ID hash decode", error))?;
            let hash = BlockHash::try_from(encoded_hash.as_slice())
                .map_err(|error| StorageError::invalid_metadata(format!("invalid stored block hash: {error}")))?;
            let stored = decode_identity(&row)?;
            publish_stored_identity(caches, hash, &stored);
            resolved.insert(hash, stored.identity);
        }
    }

    let mut missing = Vec::new();
    let mut identity_only = Vec::new();
    for hash in distinct {
        match resolved.get(&hash) {
            None => missing.push(hash),
            Some(identity) if !identity.materialized => identity_only.push(hash),
            Some(_) => {}
        }
    }
    if !missing.is_empty() || !identity_only.is_empty() {
        return Ok(MaterializedIdResolution::NonMaterialized { missing: Arc::from(missing), identity_only: Arc::from(identity_only) });
    }

    Ok(MaterializedIdResolution::Resolved(
        hashes.iter().map(|hash| resolved.get(hash).expect("every requested hash was classified").id).collect(),
    ))
}

fn decode_identity(row: &sqlx::postgres::PgRow) -> Result<StoredIdentity, StorageError> {
    let raw_id: i64 = row.try_get("id").map_err(|error| StorageError::database("block identity decode", error))?;
    let id = CompactId::new(raw_id)
        .ok_or_else(|| StorageError::invalid_metadata(format!("stored compact ID is not positive: {raw_id}")))?;
    let level: Option<i64> = row.try_get("level").map_err(|error| StorageError::database("block level decode", error))?;
    let slot: Option<i64> = row.try_get("slot").map_err(|error| StorageError::database("block slot decode", error))?;
    let raw_selected_parent_id: Option<i64> =
        row.try_get("selected_parent_id").map_err(|error| StorageError::database("selected-parent ID decode", error))?;
    let block = match (level, slot, raw_selected_parent_id) {
        (None, None, None) => None,
        (Some(level), Some(slot), Some(raw_selected_parent_id)) => {
            let coordinate = u64::try_from(level)
                .ok()
                .and_then(|level| BlockCoordinate::new(level, u64::try_from(slot).ok()?))
                .ok_or_else(|| StorageError::invalid_metadata(format!("invalid block coordinate ({level}, {slot})")))?;
            let selected_parent_id = CompactId::new(raw_selected_parent_id).ok_or_else(|| {
                StorageError::invalid_metadata(format!("stored selected-parent ID is not positive: {raw_selected_parent_id}"))
            })?;
            Some(CachedMaterializedBlock { coordinate, selected_parent_id })
        }
        _ => return Err(StorageError::invalid_metadata("stored block has partial immutable metadata")),
    };
    Ok(StoredIdentity { identity: CachedIdentity { id, materialized: block.is_some() }, block })
}

fn publish_stored_identity(caches: &ProcessingCaches, hash: BlockHash, stored: &StoredIdentity) {
    caches.publish_identity(hash, stored.identity);
    if let Some(block) = stored.block {
        caches.publish_materialized_block(stored.identity.id, block);
    }
}

#[cfg(test)]
mod tests {
    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_model::block::{BlockCoordinate, BlockHash, BlockPresence, CompactId};
    use sqlx::Row;
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    };

    use crate::{
        database::{LockedDatabase, PreparedDatabase, open_processing_generation},
        generation::ValidatedDbClient,
        operation::ResolveMaterializedIdsError,
        runtime::{RetirementReceiver, retirement_channel},
        state::DatabaseState,
    };

    const POSTGRES_PORT: u16 = 5432;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn mainnet() -> NetworkId {
        NetworkId::new(NetworkType::Mainnet)
    }

    async fn fixture() -> (ContainerAsync<Postgres>, String) {
        let container = Postgres::default().with_tag("17-alpine").start().await.expect("PostgreSQL fixture must start");
        let host = container.get_host().await.expect("fixture host must resolve");
        let port = container.get_host_port_ipv4(POSTGRES_PORT).await.expect("fixture PostgreSQL port must resolve");
        let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
        (container, database_url)
    }

    async fn processing_client(database_url: &str) -> (PreparedDatabase, std::sync::Arc<ValidatedDbClient>, RetirementReceiver) {
        let (mut database, state) =
            LockedDatabase::connect(database_url).await.expect("database lock").prepare().await.expect("schema preparation");
        assert_eq!(state, DatabaseState::Uninitialized);
        let state = database.initialize_if_uninitialized(mainnet(), hash(0), None).await.expect("database initialization");
        let binding = state.binding().expect("initialized database binding");
        let (retirement_tx, retirement_rx) = retirement_channel();
        let client = open_processing_generation(database_url.to_owned(), binding, retirement_tx).await.expect("processing generation");
        (database, client, retirement_rx)
    }

    async fn insert_identity(database: &mut PreparedDatabase, hash: BlockHash) -> CompactId {
        let id: i64 = sqlx::query_scalar("INSERT INTO block_identifiers (hash) VALUES ($1) RETURNING id")
            .bind(hash.as_bytes().as_slice())
            .fetch_one(database.connection_mut())
            .await
            .expect("identity insertion");
        CompactId::new(id).expect("positive generated ID")
    }

    #[tokio::test]
    async fn identity_reads_distinguish_materiality_preserve_order_and_never_cache_absence() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, _retirements) = processing_client(&database_url).await;
        let boundary_hash = hash(1);
        let materialized_hash = hash(2);
        let initially_absent_hash = hash(3);
        let missing_hash = hash(4);
        let second_materialized_hash = hash(5);
        let boundary_id = insert_identity(&mut database, boundary_hash).await;
        let materialized_id = insert_identity(&mut database, materialized_hash).await;
        let second_materialized_id = insert_identity(&mut database, second_materialized_hash).await;

        sqlx::query("INSERT INTO levels (level, size) VALUES (2, 2)")
            .execute(database.connection_mut())
            .await
            .expect("level insertion");
        sqlx::query(
            "INSERT INTO blocks (
                id, timestamp, daa_score, level, slot, selected_parent_id,
                color, is_in_vspc, blue_merge_set, red_merge_set
             ) VALUES ($1, 0, 0, 2, 0, $2, 0, FALSE, '{}', '{}')",
        )
        .bind(materialized_id.get())
        .bind(boundary_id.get())
        .execute(database.connection_mut())
        .await
        .expect("materialized block insertion");
        sqlx::query(
            "INSERT INTO blocks (
                id, timestamp, daa_score, level, slot, selected_parent_id,
                color, is_in_vspc, blue_merge_set, red_merge_set
             ) VALUES ($1, 0, 0, 2, 1, $2, 0, FALSE, '{}', '{}')",
        )
        .bind(second_materialized_id.get())
        .bind(boundary_id.get())
        .execute(database.connection_mut())
        .await
        .expect("second materialized block insertion");

        assert_eq!(client.block_presence(hash(99)).await, Ok(BlockPresence::Absent));
        assert_eq!(client.block_presence(boundary_hash).await, Ok(BlockPresence::BoundaryIdentity { id: boundary_id }));
        assert_eq!(
            client.block_presence(materialized_hash).await,
            Ok(BlockPresence::Materialized { id: materialized_id, coordinate: BlockCoordinate::new(2, 0).expect("valid coordinate") })
        );
        assert_eq!(
            client.cached_materialized_block(materialized_id),
            Some(crate::cache::CachedMaterializedBlock {
                coordinate: BlockCoordinate::new(2, 0).expect("valid coordinate"),
                selected_parent_id: boundary_id,
            })
        );

        assert_eq!(client.block_presence(initially_absent_hash).await, Ok(BlockPresence::Absent));
        let newly_inserted_id = insert_identity(&mut database, initially_absent_hash).await;
        assert_eq!(client.block_presence(initially_absent_hash).await, Ok(BlockPresence::BoundaryIdentity { id: newly_inserted_id }));

        assert_eq!(
            client.resolve_materialized_ids(&[second_materialized_hash, materialized_hash, second_materialized_hash]).await,
            Ok(vec![second_materialized_id, materialized_id, second_materialized_id].into_boxed_slice())
        );
        assert_eq!(client.resolve_materialized_ids(&[]).await, Ok(Vec::new().into_boxed_slice()));
        let before_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM block_identifiers")
            .fetch_one(database.connection_mut())
            .await
            .expect("identity count")
            .try_get("count")
            .expect("identity count decode");
        assert_eq!(before_count, 4, "identity reads must not create or promote identities");
        assert_eq!(
            client.resolve_materialized_ids(&[materialized_hash, missing_hash, boundary_hash, missing_hash, boundary_hash,]).await,
            Err(ResolveMaterializedIdsError::NonMaterialized {
                missing: vec![missing_hash].into(),
                identity_only: vec![boundary_hash].into(),
            })
        );
        let after_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM block_identifiers")
            .fetch_one(database.connection_mut())
            .await
            .expect("identity count after failed resolution");
        assert_eq!(after_count, before_count);
    }
}
