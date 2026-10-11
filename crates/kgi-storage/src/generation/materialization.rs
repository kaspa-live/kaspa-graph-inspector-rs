use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use kgi_model::{
    block::{BlockColor, BlockCoordinate, BlockHash, CompactId, Timestamp, ValidatedNodeBlock},
    graph_update::{BlockCommitted, LevelCommitted, ParentCommitted},
};
use sqlx::{PgPool, Postgres, Row, Transaction};

use super::{ValidatedDbClient, interning};
use crate::{
    cache::{CachedIdentity, CachedMaterializedBlock, CachedMergeSets, ProcessingCacheDomains},
    database::{NO_VSPC_DAA_SCORE, daa_score_to_sql, timestamp_to_sql},
    error::StorageError,
    operation::{MaterializeBlockError, MaterializeBlockOutcome, ReferencePolicy},
    transaction::{self, TransactionAttemptError},
};

#[derive(Clone, Copy)]
pub(super) enum CommitBehavior {
    Normal,
    #[cfg(test)]
    CommitThenLoseAcknowledgement,
    #[cfg(test)]
    RollbackThenLoseAcknowledgement,
}

enum MaterializationResult {
    Outcome(MaterializeBlockOutcome),
    IncomingBoundaryIdentity { hash: BlockHash },
    NonMaterializedReferences { missing: Arc<[BlockHash]>, identity_only: Arc<[BlockHash]> },
}

struct PreparedBlock {
    hash: BlockHash,
    selected_parent: BlockHash,
    direct_parents: Vec<BlockHash>,
    selected_parent_index: u32,
    universal: Vec<BlockHash>,
    blue_merge_set: Arc<[BlockHash]>,
    red_merge_set: Arc<[BlockHash]>,
    timestamp: Timestamp,
    daa_score: u64,
}

#[derive(Clone, Copy)]
struct StoredIdentity {
    id: CompactId,
    block: Option<CachedMaterializedBlock>,
}

impl StoredIdentity {
    const fn coordinate(self) -> Option<BlockCoordinate> {
        match self.block {
            Some(block) => Some(block.coordinate),
            None => None,
        }
    }
}

struct AttemptSuccess {
    outcome: MaterializeBlockOutcome,
    cache_updates: Vec<(BlockHash, StoredIdentity)>,
    merge_sets: Option<(CompactId, CachedMergeSets)>,
}

struct LoadedIdentities {
    values: HashMap<BlockHash, StoredIdentity>,
    cache_misses: Vec<BlockHash>,
}

enum SemanticError {
    IncomingBoundaryIdentity { hash: BlockHash },
    NonMaterializedReferences { missing: Arc<[BlockHash]>, identity_only: Arc<[BlockHash]> },
}

impl PreparedBlock {
    fn new(block: ValidatedNodeBlock) -> Self {
        let ValidatedNodeBlock {
            hash,
            selected_parent,
            direct_parents,
            blue_merge_set,
            red_merge_set,
            timestamp,
            daa_score,
            blue_score: _,
            blue_work: _,
        } = block;
        let selected_parent_index = direct_parents
            .iter()
            .position(|hash| *hash == selected_parent)
            .and_then(|index| u32::try_from(index).ok())
            .expect("ValidatedNodeBlock selected parent has a representable canonical index");

        let mut seen = HashSet::new();
        let mut universal = Vec::new();
        for hash in
            red_merge_set.iter().chain(&blue_merge_set).chain(&direct_parents).chain(std::iter::once(&selected_parent)).copied()
        {
            if seen.insert(hash) {
                universal.push(hash);
            }
        }
        universal.push(hash);

        Self {
            hash,
            selected_parent,
            direct_parents,
            selected_parent_index,
            universal,
            blue_merge_set: Arc::from(blue_merge_set),
            red_merge_set: Arc::from(red_merge_set),
            timestamp,
            daa_score,
        }
    }

    fn references(&self) -> &[BlockHash] {
        self.universal.split_last().expect("the own hash is always appended").1
    }
}

impl ValidatedDbClient {
    /// Materializes one validated non-Genesis block under the selected reference policy.
    pub async fn materialize_block(
        &self,
        block: ValidatedNodeBlock,
        policy: ReferencePolicy,
    ) -> Result<MaterializeBlockOutcome, MaterializeBlockError> {
        let commit_behavior = self.next_materialization_commit_behavior();
        let result = self
            .run_operation(|| async {
                let _mutation = self.mutation_guard.read().await;
                let _lane = self.materialization_lane.lock().await;
                let caches = self.caches.load_full();
                let prepared = PreparedBlock::new(block);
                let daa_score = daa_score_to_sql(prepared.daa_score)?;
                let result = transaction::run(&self.transaction_timing, || {
                    attempt(self.runtime.pool(), &caches, &prepared, daa_score, policy, commit_behavior)
                })
                .await?;
                let success = match result {
                    Ok(success) => success,
                    Err(SemanticError::IncomingBoundaryIdentity { hash }) => {
                        return Ok(MaterializationResult::IncomingBoundaryIdentity { hash });
                    }
                    Err(SemanticError::NonMaterializedReferences { missing, identity_only }) => {
                        return Ok(MaterializationResult::NonMaterializedReferences { missing, identity_only });
                    }
                };

                for (hash, stored) in success.cache_updates {
                    caches.publish_identity(hash, CachedIdentity { id: stored.id, materialized: stored.block.is_some() });
                    if let Some(block) = stored.block {
                        caches.publish_materialized_block(stored.id, block);
                    }
                }
                if let Some((id, merge_sets)) = success.merge_sets {
                    caches.publish_merge_sets(id, merge_sets);
                }
                Ok(MaterializationResult::Outcome(success.outcome))
            })
            .await?;
        match result {
            MaterializationResult::Outcome(outcome) => Ok(outcome),
            MaterializationResult::IncomingBoundaryIdentity { hash } => Err(MaterializeBlockError::IncomingBoundaryIdentity { hash }),
            MaterializationResult::NonMaterializedReferences { missing, identity_only } => {
                Err(MaterializeBlockError::NonMaterializedReferences { missing, identity_only })
            }
        }
    }

    fn next_materialization_commit_behavior(&self) -> CommitBehavior {
        #[cfg(test)]
        {
            return self
                .materialization_commit_behavior
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .unwrap_or(CommitBehavior::Normal);
        }
        #[cfg(not(test))]
        CommitBehavior::Normal
    }

    #[cfg(test)]
    fn inject_materialization_commit_behavior(&self, behavior: CommitBehavior) {
        *self.materialization_commit_behavior.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(behavior);
    }
}

async fn attempt(
    pool: &PgPool,
    caches: &ProcessingCacheDomains,
    prepared: &PreparedBlock,
    daa_score: i64,
    policy: ReferencePolicy,
    commit_behavior: CommitBehavior,
) -> Result<AttemptSuccess, TransactionAttemptError<SemanticError>> {
    let mut transaction =
        pool.begin().await.map_err(|error| TransactionAttemptError::database("block-materialization transaction start", error))?;
    let LoadedIdentities { values: mut identities, cache_misses } =
        load_identities(&mut transaction, caches, &prepared.universal).await?;

    if let Some(own) = identities.get(&prepared.hash).copied() {
        if let Some(coordinate) = own.coordinate() {
            transaction
                .rollback()
                .await
                .map_err(|error| TransactionAttemptError::database("materialized-block dedup transaction rollback", error))?;
            return Ok(AttemptSuccess {
                outcome: MaterializeBlockOutcome::AlreadyMaterialized { id: own.id, coordinate },
                cache_updates: cache_misses.iter().filter(|hash| **hash == prepared.hash).map(|hash| (*hash, own)).collect(),
                merge_sets: None,
            });
        }
        transaction
            .rollback()
            .await
            .map_err(|error| TransactionAttemptError::database("boundary-identity transaction rollback", error))?;
        return Err(TransactionAttemptError::Semantic(SemanticError::IncomingBoundaryIdentity { hash: prepared.hash }));
    }

    if policy == ReferencePolicy::RequireMaterialized {
        let mut missing = Vec::new();
        let mut identity_only = Vec::new();
        for hash in prepared.references() {
            match identities.get(hash) {
                None => missing.push(*hash),
                Some(identity) if identity.block.is_none() => identity_only.push(*hash),
                Some(_) => {}
            }
        }
        if !missing.is_empty() || !identity_only.is_empty() {
            transaction
                .rollback()
                .await
                .map_err(|error| TransactionAttemptError::database("nonmaterialized-reference transaction rollback", error))?;
            return Err(TransactionAttemptError::Semantic(SemanticError::NonMaterializedReferences {
                missing: Arc::from(missing),
                identity_only: Arc::from(identity_only),
            }));
        }
    }

    intern_missing_identities(&mut transaction, &mut identities, &cache_misses).await?;

    let direct_parents: Vec<_> = prepared
        .direct_parents
        .iter()
        .map(|hash| (*hash, identities.get(hash).copied().expect("every direct parent was resolved")))
        .collect();
    let level = direct_parents
        .iter()
        .filter_map(|(_, identity)| identity.coordinate().map(BlockCoordinate::level))
        .max()
        .map_or(Some(1), |level| level.checked_add(1))
        .ok_or_else(|| TransactionAttemptError::Storage(StorageError::invalid_metadata("derived block level overflow")))?;
    let raw_level = i64::try_from(level)
        .map_err(|_| TransactionAttemptError::Storage(StorageError::invalid_metadata("derived block level exceeds BIGINT")))?;
    let raw_slot: i64 = sqlx::query_scalar(
        "INSERT INTO levels (level, size, daa_score) VALUES ($1, 1, $2)
         ON CONFLICT (level) DO UPDATE SET size = levels.size + 1
         RETURNING levels.size - 1",
    )
    .bind(raw_level)
    .bind(NO_VSPC_DAA_SCORE)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("block-coordinate allocation", error))?;
    let slot = u64::try_from(raw_slot)
        .map_err(|_| TransactionAttemptError::Storage(StorageError::invalid_metadata("allocated block slot is negative")))?;
    let coordinate = BlockCoordinate::new(level, slot)
        .ok_or_else(|| TransactionAttemptError::Storage(StorageError::invalid_metadata("allocated block coordinate is invalid")))?;

    let selected_parent_id = identities.get(&prepared.selected_parent).expect("selected parent was resolved").id;
    let own = identities.get_mut(&prepared.hash).expect("the incoming block identity was interned");
    own.block = Some(CachedMaterializedBlock { coordinate, selected_parent_id });
    let own_id = own.id;
    let blue_ids: Vec<i64> = prepared
        .blue_merge_set
        .iter()
        .map(|hash| identities.get(hash).expect("blue merge-set identity was resolved").id.get())
        .collect();
    let red_ids: Vec<i64> = prepared
        .red_merge_set
        .iter()
        .map(|hash| identities.get(hash).expect("red merge-set identity was resolved").id.get())
        .collect();
    sqlx::query(
        "INSERT INTO blocks (
             id, timestamp, daa_score, level, slot, selected_parent_id,
             color, is_in_vspc, blue_merge_set, red_merge_set
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, $8, $9)",
    )
    .bind(own_id.get())
    .bind(timestamp_to_sql(prepared.timestamp))
    .bind(daa_score)
    .bind(raw_level)
    .bind(raw_slot)
    .bind(selected_parent_id.get())
    .bind(BlockColor::Gray as i16)
    .bind(&blue_ids)
    .bind(&red_ids)
    .execute(&mut *transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("block materialization", error))?;

    let mut parent_ids = Vec::with_capacity(direct_parents.len());
    let mut parent_levels = Vec::with_capacity(direct_parents.len());
    let mut parent_slots = Vec::with_capacity(direct_parents.len());
    for (_, parent) in &direct_parents {
        parent_ids.push(parent.id.get());
        parent_levels.push(
            parent.coordinate().map_or(0, |coordinate| i64::try_from(coordinate.level()).expect("stored parent level fits BIGINT")),
        );
        parent_slots.push(
            parent.coordinate().map_or(0, |coordinate| i64::try_from(coordinate.slot()).expect("stored parent slot fits BIGINT")),
        );
    }
    sqlx::query(
        "INSERT INTO parents (
             child_id, parent_id, child_level, child_slot, parent_level, parent_slot
         )
         SELECT $1, relation.parent_id, $2, $3, relation.parent_level, relation.parent_slot
         FROM UNNEST($4::BIGINT[], $5::BIGINT[], $6::BIGINT[])
             AS relation(parent_id, parent_level, parent_slot)",
    )
    .bind(own_id.get())
    .bind(raw_level)
    .bind(raw_slot)
    .bind(&parent_ids)
    .bind(&parent_levels)
    .bind(&parent_slots)
    .execute(&mut *transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("direct-parent materialization", error))?;

    let level_snapshots = load_level_snapshots(
        &mut transaction,
        level,
        direct_parents.iter().filter_map(|(_, identity)| identity.coordinate().map(BlockCoordinate::level)),
    )
    .await?;
    let committed = BlockCommitted {
        id: own_id,
        hash: prepared.hash,
        coordinate,
        timestamp: prepared.timestamp,
        daa_score: prepared.daa_score,
        selected_parent_index: Some(prepared.selected_parent_index),
        direct_parents: direct_parents
            .iter()
            .map(|(hash, identity)| ParentCommitted { hash: *hash, coordinate: identity.coordinate() })
            .collect(),
        blue_merge_set: Arc::clone(&prepared.blue_merge_set),
        red_merge_set: Arc::clone(&prepared.red_merge_set),
        color: BlockColor::Gray,
        is_in_vspc: false,
        level_snapshots,
    };

    commit(transaction, commit_behavior)
        .await
        .map_err(|error| TransactionAttemptError::commit("block-materialization transaction commit", error))?;
    Ok(AttemptSuccess {
        outcome: MaterializeBlockOutcome::Inserted { committed },
        cache_updates: cache_misses
            .iter()
            .map(|hash| (*hash, *identities.get(hash).expect("every universal identity was resolved")))
            .collect(),
        merge_sets: Some((
            own_id,
            CachedMergeSets {
                blue: blue_ids.iter().map(|id| CompactId::new(*id).expect("resolved blue merge-set IDs are positive")).collect(),
                red: red_ids.iter().map(|id| CompactId::new(*id).expect("resolved red merge-set IDs are positive")).collect(),
            },
        )),
    })
}

async fn commit(transaction: Transaction<'_, Postgres>, behavior: CommitBehavior) -> Result<(), sqlx::Error> {
    match behavior {
        CommitBehavior::Normal => transaction.commit().await,
        #[cfg(test)]
        CommitBehavior::CommitThenLoseAcknowledgement => {
            transaction.commit().await?;
            Err(sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected materialization COMMIT acknowledgement loss",
            )))
        }
        #[cfg(test)]
        CommitBehavior::RollbackThenLoseAcknowledgement => {
            transaction.rollback().await?;
            Err(sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected materialization COMMIT uncertainty after rollback",
            )))
        }
    }
}

async fn load_identities(
    transaction: &mut Transaction<'_, Postgres>,
    caches: &ProcessingCacheDomains,
    hashes: &[BlockHash],
) -> Result<LoadedIdentities, TransactionAttemptError<SemanticError>> {
    let (mut identities, misses) = seed_identities_from_cache(caches, hashes);
    if misses.is_empty() {
        return Ok(LoadedIdentities { values: identities, cache_misses: misses });
    }

    let encoded: Vec<Vec<u8>> = misses.iter().map(|hash| hash.as_bytes().to_vec()).collect();
    let rows = sqlx::query(
        "SELECT identity.hash, identity.id, block.level, block.slot, block.selected_parent_id
         FROM block_identifiers identity
         LEFT JOIN blocks block ON block.id = identity.id
         WHERE identity.hash = ANY($1::BYTEA[])",
    )
    .bind(encoded)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("block-reference resolution", error))?;
    for row in rows {
        let encoded_hash: Vec<u8> =
            row.try_get("hash").map_err(|error| TransactionAttemptError::database("block-reference hash decode", error))?;
        let hash = BlockHash::try_from(encoded_hash.as_slice()).map_err(|error| {
            TransactionAttemptError::Storage(StorageError::invalid_metadata(format!("invalid stored block hash: {error}")))
        })?;
        let raw_id: i64 = row.try_get("id").map_err(|error| TransactionAttemptError::database("block-reference ID decode", error))?;
        let id = CompactId::new(raw_id).ok_or_else(|| {
            TransactionAttemptError::Storage(StorageError::invalid_metadata(format!("stored compact ID is not positive: {raw_id}")))
        })?;
        let level: Option<i64> =
            row.try_get("level").map_err(|error| TransactionAttemptError::database("block-reference level decode", error))?;
        let slot: Option<i64> =
            row.try_get("slot").map_err(|error| TransactionAttemptError::database("block-reference slot decode", error))?;
        let coordinate = decode_coordinate(level, slot).map_err(TransactionAttemptError::Storage)?;
        let raw_selected_parent_id: Option<i64> = row
            .try_get("selected_parent_id")
            .map_err(|error| TransactionAttemptError::database("block-reference selected-parent ID decode", error))?;
        let block = decode_materialized_block(coordinate, raw_selected_parent_id).map_err(TransactionAttemptError::Storage)?;
        identities.insert(hash, StoredIdentity { id, block });
    }
    Ok(LoadedIdentities { values: identities, cache_misses: misses })
}

async fn intern_missing_identities(
    transaction: &mut Transaction<'_, Postgres>,
    identities: &mut HashMap<BlockHash, StoredIdentity>,
    cache_misses: &[BlockHash],
) -> Result<(), TransactionAttemptError<SemanticError>> {
    let missing: Vec<_> = cache_misses.iter().filter(|&hash| !identities.contains_key(hash)).copied().collect();
    for (hash, id) in interning::intern_identities(transaction, &missing).await? {
        identities.insert(hash, StoredIdentity { id, block: None });
    }
    Ok(())
}

fn seed_identities_from_cache(
    caches: &ProcessingCacheDomains,
    hashes: &[BlockHash],
) -> (HashMap<BlockHash, StoredIdentity>, Vec<BlockHash>) {
    let mut identities = HashMap::with_capacity(hashes.len());
    let mut misses = Vec::new();
    for hash in hashes {
        let Some(identity) = caches.identity(*hash) else {
            misses.push(*hash);
            continue;
        };
        let block = if identity.materialized {
            let Some(block) = caches.materialized_block(identity.id) else {
                misses.push(*hash);
                continue;
            };
            Some(block)
        } else {
            None
        };
        identities.insert(*hash, StoredIdentity { id: identity.id, block });
    }
    (identities, misses)
}

async fn load_level_snapshots(
    transaction: &mut Transaction<'_, Postgres>,
    block_level: u64,
    parent_levels: impl Iterator<Item = u64>,
) -> Result<Arc<[LevelCommitted]>, TransactionAttemptError<SemanticError>> {
    let mut levels = BTreeSet::from([block_level]);
    levels.extend(parent_levels);
    let encoded: Vec<i64> = levels
        .iter()
        .copied()
        .map(|level| {
            i64::try_from(level)
                .map_err(|_| TransactionAttemptError::Storage(StorageError::invalid_metadata("snapshot level exceeds BIGINT")))
        })
        .collect::<Result<_, _>>()?;
    let rows = sqlx::query("SELECT level, size, daa_score FROM levels WHERE level = ANY($1::BIGINT[]) ORDER BY level")
        .bind(encoded)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| TransactionAttemptError::database("materialized level-snapshot load", error))?;
    if rows.len() != levels.len() {
        return Err(TransactionAttemptError::Storage(StorageError::invalid_metadata(
            "materialized parent level has no level-state row",
        )));
    }
    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_level: i64 =
            row.try_get("level").map_err(|error| TransactionAttemptError::database("level-snapshot level decode", error))?;
        let raw_size: i64 =
            row.try_get("size").map_err(|error| TransactionAttemptError::database("level-snapshot size decode", error))?;
        let raw_daa: i64 =
            row.try_get("daa_score").map_err(|error| TransactionAttemptError::database("level-snapshot DAA decode", error))?;
        let level = u64::try_from(raw_level)
            .map_err(|_| TransactionAttemptError::Storage(StorageError::invalid_metadata("stored snapshot level is negative")))?;
        let size = u64::try_from(raw_size)
            .map_err(|_| TransactionAttemptError::Storage(StorageError::invalid_metadata("stored level size is negative")))?;
        let daa_score =
            if raw_daa == NO_VSPC_DAA_SCORE {
                None
            } else {
                Some(u64::try_from(raw_daa).map_err(|_| {
                    TransactionAttemptError::Storage(StorageError::invalid_metadata("stored level DAA score is negative"))
                })?)
            };
        snapshots.push(LevelCommitted { level, size, daa_score });
    }
    Ok(Arc::from(snapshots))
}

fn decode_coordinate(level: Option<i64>, slot: Option<i64>) -> Result<Option<BlockCoordinate>, StorageError> {
    match (level, slot) {
        (None, None) => Ok(None),
        (Some(level), Some(slot)) => {
            let coordinate = u64::try_from(level)
                .ok()
                .and_then(|level| BlockCoordinate::new(level, u64::try_from(slot).ok()?))
                .ok_or_else(|| StorageError::invalid_metadata(format!("invalid stored block coordinate ({level}, {slot})")))?;
            Ok(Some(coordinate))
        }
        _ => Err(StorageError::invalid_metadata("stored block has a partial coordinate")),
    }
}

fn decode_materialized_block(
    coordinate: Option<BlockCoordinate>,
    raw_selected_parent_id: Option<i64>,
) -> Result<Option<CachedMaterializedBlock>, StorageError> {
    match (coordinate, raw_selected_parent_id) {
        (None, None) => Ok(None),
        (Some(coordinate), Some(raw_selected_parent_id)) => {
            let selected_parent_id = CompactId::new(raw_selected_parent_id).ok_or_else(|| {
                StorageError::invalid_metadata(format!("stored selected-parent ID is not positive: {raw_selected_parent_id}"))
            })?;
            Ok(Some(CachedMaterializedBlock { coordinate, selected_parent_id }))
        }
        _ => Err(StorageError::invalid_metadata("stored block has partial immutable metadata")),
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_model::{
        block::{BlockColor, BlockCoordinate, BlockHash, BlockPresence, CompactId, ValidatedNodeBlock},
        graph_update::{LevelCommitted, ParentCommitted},
        lifecycle::PersistenceFault,
    };
    use sqlx::Row;
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    };
    use tokio::time::timeout;

    use super::{PreparedBlock, seed_identities_from_cache};
    use crate::{
        cache::{CachedIdentity, CachedMaterializedBlock, ProcessingCacheDomains},
        database::{LockedDatabase, PreparedDatabase, open_processing_generation},
        error::StorageError,
        generation::ValidatedDbClient,
        operation::{MaterializeBlockError, MaterializeBlockOutcome, ReferencePolicy},
        runtime::{RetirementReceiver, RetirementTarget, retirement_channel},
        state::DatabaseState,
    };

    const POSTGRES_PORT: u16 = 5432;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn mainnet() -> NetworkId {
        NetworkId::new(NetworkType::Mainnet)
    }

    #[test]
    fn identity_cache_seeding_requires_complete_materialized_metadata() {
        let caches = ProcessingCacheDomains::new();
        let materialized_id = CompactId::new(1).expect("positive ID");
        let boundary_id = CompactId::new(2).expect("positive ID");
        let coordinate_missing_id = CompactId::new(3).expect("positive ID");
        let coordinate = BlockCoordinate::new(1, 0).expect("materialized coordinate");
        caches.publish_identity(hash(1), CachedIdentity { id: materialized_id, materialized: true });
        caches.publish_materialized_block(materialized_id, CachedMaterializedBlock { coordinate, selected_parent_id: boundary_id });
        caches.publish_identity(hash(2), CachedIdentity { id: boundary_id, materialized: false });
        caches.publish_identity(hash(3), CachedIdentity { id: coordinate_missing_id, materialized: true });

        let (identities, misses) = seed_identities_from_cache(&caches, &[hash(1), hash(2), hash(3), hash(4)]);

        assert_eq!(
            identities.get(&hash(1)).map(|identity| (identity.id, identity.coordinate())),
            Some((materialized_id, Some(coordinate)))
        );
        assert_eq!(identities.get(&hash(2)).map(|identity| (identity.id, identity.coordinate())), Some((boundary_id, None)));
        assert!(!identities.contains_key(&hash(3)));
        assert!(!identities.contains_key(&hash(4)));
        assert_eq!(misses, [hash(3), hash(4)]);
    }

    fn block(
        hash: BlockHash,
        selected_parent: BlockHash,
        direct_parents: Vec<BlockHash>,
        blue_merge_set: Vec<BlockHash>,
        red_merge_set: Vec<BlockHash>,
    ) -> ValidatedNodeBlock {
        ValidatedNodeBlock {
            hash,
            selected_parent,
            direct_parents,
            blue_merge_set,
            red_merge_set,
            timestamp: u64::MAX,
            daa_score: 42,
            blue_score: 43,
            blue_work: Default::default(),
        }
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

    async fn insert_level(database: &mut PreparedDatabase, level: u64, size: u64, daa_score: Option<u64>) {
        sqlx::query("INSERT INTO levels (level, size, daa_score) VALUES ($1, $2, $3)")
            .bind(i64::try_from(level).expect("test level fits BIGINT"))
            .bind(i64::try_from(size).expect("test size fits BIGINT"))
            .bind(daa_score.map_or(i64::MAX, |score| i64::try_from(score).expect("test DAA score fits BIGINT")))
            .execute(database.connection_mut())
            .await
            .expect("level insertion");
    }

    async fn insert_materialized(
        database: &mut PreparedDatabase,
        hash: BlockHash,
        selected_parent_id: CompactId,
        coordinate: BlockCoordinate,
    ) -> CompactId {
        let id = insert_identity(database, hash).await;
        sqlx::query(
            "INSERT INTO blocks (
                 id, timestamp, daa_score, level, slot, selected_parent_id,
                 color, is_in_vspc, blue_merge_set, red_merge_set
             ) VALUES ($1, 0, 1, $2, $3, $4, 0, FALSE, '{}', '{}')",
        )
        .bind(id.get())
        .bind(i64::try_from(coordinate.level()).expect("test level fits BIGINT"))
        .bind(i64::try_from(coordinate.slot()).expect("test slot fits BIGINT"))
        .bind(selected_parent_id.get())
        .execute(database.connection_mut())
        .await
        .expect("materialized block insertion");
        id
    }

    async fn identity_count(database: &mut PreparedDatabase) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM block_identifiers")
            .fetch_one(database.connection_mut())
            .await
            .expect("identity count")
    }

    async fn complete_processing_retirement(retirements: &mut RetirementReceiver, expected: &Arc<ValidatedDbClient>) {
        let request = retirements.recv().await.expect("processing retirement request");
        let retired = match request.target() {
            RetirementTarget::Processing(generation) => generation.upgrade().expect("processing generation remains alive"),
            RetirementTarget::Api(_) => panic!("materialization requested API retirement"),
        };
        assert!(Arc::ptr_eq(&retired, expected));
        assert!(retired.retire());
        request.complete(Ok(()));
    }

    async fn install_retry_trigger(database: &mut PreparedDatabase) {
        sqlx::query("CREATE SEQUENCE materialization_attempts")
            .execute(database.connection_mut())
            .await
            .expect("retry sequence creation");
        sqlx::query(
            "CREATE FUNCTION inject_materialization_retry() RETURNS trigger LANGUAGE plpgsql AS $$
             DECLARE attempt BIGINT := nextval('materialization_attempts');
             BEGIN
                 IF attempt = 1 THEN
                     RAISE EXCEPTION 'injected serialization failure' USING ERRCODE = '40001';
                 ELSIF attempt = 2 THEN
                     RAISE EXCEPTION 'injected deadlock' USING ERRCODE = '40P01';
                 END IF;
                 RETURN NEW;
             END
             $$;",
        )
        .execute(database.connection_mut())
        .await
        .expect("retry function creation");
        sqlx::query(
            "CREATE TRIGGER inject_materialization_retry
                 BEFORE INSERT ON blocks
                 FOR EACH ROW EXECUTE FUNCTION inject_materialization_retry()",
        )
        .execute(database.connection_mut())
        .await
        .expect("retry trigger creation");
    }

    async fn replace_retry_trigger(database: &mut PreparedDatabase, sqlstate: &str) {
        sqlx::query("ALTER SEQUENCE materialization_attempts RESTART WITH 1")
            .execute(database.connection_mut())
            .await
            .expect("retry sequence reset");
        let statement = match sqlstate {
            "40001" => {
                "CREATE OR REPLACE FUNCTION inject_materialization_retry() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                     PERFORM nextval('materialization_attempts');
                     RAISE EXCEPTION 'injected transaction failure' USING ERRCODE = '40001';
                 END
                 $$;"
            }
            "23505" => {
                "CREATE OR REPLACE FUNCTION inject_materialization_retry() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                     PERFORM nextval('materialization_attempts');
                     RAISE EXCEPTION 'injected transaction failure' USING ERRCODE = '23505';
                 END
                 $$;"
            }
            _ => panic!("unsupported injected SQLSTATE"),
        };
        sqlx::query(statement).execute(database.connection_mut()).await.expect("retry trigger replacement");
    }

    async fn install_connection_loss_trigger(database: &mut PreparedDatabase) {
        sqlx::query(
            "CREATE FUNCTION inject_materialization_connection_loss() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;",
        )
        .execute(database.connection_mut())
        .await
        .expect("connection-loss function creation");
        sqlx::query(
            "CREATE TRIGGER inject_materialization_connection_loss
                 BEFORE INSERT ON blocks
                 FOR EACH ROW EXECUTE FUNCTION inject_materialization_connection_loss()",
        )
        .execute(database.connection_mut())
        .await
        .expect("connection-loss trigger creation");
    }

    #[tokio::test]
    async fn materialization_preserves_canonical_relationships_universal_order_and_level_snapshots() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, _retirements) = processing_client(&database_url).await;
        let origin_id = insert_identity(&mut database, hash(1)).await;
        insert_level(&mut database, 1, 1, Some(10)).await;
        let parent_a = hash(2);
        let parent_a_id =
            insert_materialized(&mut database, parent_a, origin_id, BlockCoordinate::new(1, 0).expect("coordinate")).await;
        client.caches.load().publish_identity(parent_a, CachedIdentity { id: parent_a_id, materialized: true });
        insert_level(&mut database, 2, 1, None).await;
        let parent_b = hash(3);
        let parent_b_id =
            insert_materialized(&mut database, parent_b, parent_a_id, BlockCoordinate::new(2, 0).expect("coordinate")).await;
        let boundary_parent = hash(4);
        let boundary_parent_id = insert_identity(&mut database, boundary_parent).await;
        let red_missing = hash(5);
        let blue_missing = hash(6);
        let own_hash = hash(7);
        let input = block(
            own_hash,
            parent_a,
            vec![parent_a, parent_b, boundary_parent],
            vec![parent_b, parent_a, blue_missing],
            vec![red_missing, parent_a],
        );
        let prepared = PreparedBlock::new(input.clone());
        assert_eq!(prepared.references(), [red_missing, parent_a, parent_b, blue_missing, boundary_parent]);
        assert_eq!(prepared.universal, [red_missing, parent_a, parent_b, blue_missing, boundary_parent, own_hash]);

        let outcome = client
            .materialize_block(input.clone(), ReferencePolicy::AllowBoundaryIdentities)
            .await
            .expect("boundary-permitting materialization");
        let MaterializeBlockOutcome::Inserted { committed } = outcome else {
            panic!("new block must be inserted");
        };
        assert_eq!(committed.hash, own_hash);
        assert_eq!(committed.coordinate, BlockCoordinate::new(3, 0).expect("coordinate"));
        assert_eq!(committed.timestamp, u64::MAX);
        assert_eq!(committed.daa_score, 42);
        assert_eq!(committed.selected_parent_index, Some(0));
        assert_eq!(
            committed.direct_parents.as_ref(),
            [
                ParentCommitted { hash: parent_a, coordinate: BlockCoordinate::new(1, 0) },
                ParentCommitted { hash: parent_b, coordinate: BlockCoordinate::new(2, 0) },
                ParentCommitted { hash: boundary_parent, coordinate: None },
            ]
        );
        assert_eq!(committed.blue_merge_set.as_ref(), [parent_b, parent_a, blue_missing]);
        assert_eq!(committed.red_merge_set.as_ref(), [red_missing, parent_a]);
        assert_eq!(committed.color, BlockColor::Gray);
        assert!(!committed.is_in_vspc);
        assert_eq!(
            committed.level_snapshots.as_ref(),
            [
                LevelCommitted { level: 1, size: 1, daa_score: Some(10) },
                LevelCommitted { level: 2, size: 1, daa_score: None },
                LevelCommitted { level: 3, size: 1, daa_score: None },
            ]
        );

        let rows = sqlx::query(
            "SELECT parent_id, parent_level, parent_slot
             FROM parents WHERE child_id = $1 ORDER BY parent_id",
        )
        .bind(committed.id.get())
        .fetch_all(database.connection_mut())
        .await
        .expect("parent rows");
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(|row| row.get::<i64, _>("parent_id") == boundary_parent_id.get()
            && row.get::<i64, _>("parent_level") == 0
            && row.get::<i64, _>("parent_slot") == 0));

        let (blue_ids, red_ids, color, in_vspc): (Vec<i64>, Vec<i64>, i16, bool) =
            sqlx::query_as("SELECT blue_merge_set, red_merge_set, color, is_in_vspc FROM blocks WHERE id = $1")
                .bind(committed.id.get())
                .fetch_one(database.connection_mut())
                .await
                .expect("stored block");
        let red_missing_id: i64 = sqlx::query_scalar("SELECT id FROM block_identifiers WHERE hash = $1")
            .bind(red_missing.as_bytes().as_slice())
            .fetch_one(database.connection_mut())
            .await
            .expect("red identity");
        let blue_missing_id: i64 = sqlx::query_scalar("SELECT id FROM block_identifiers WHERE hash = $1")
            .bind(blue_missing.as_bytes().as_slice())
            .fetch_one(database.connection_mut())
            .await
            .expect("blue identity");
        assert!(red_missing_id < blue_missing_id && blue_missing_id < committed.id.get());
        assert_eq!(blue_ids, [parent_b_id.get(), parent_a_id.get(), blue_missing_id]);
        assert_eq!(red_ids, [red_missing_id, parent_a_id.get()]);
        assert_eq!((color, in_vspc), (BlockColor::Gray as i16, false));
        assert!(client.has_cached_identity(own_hash));
        assert!(client.has_cached_identity(red_missing));
        assert!(client.has_cached_identity(blue_missing));
        assert!(client.has_cached_merge_sets(committed.id));
        assert_eq!(
            client.cached_materialized_block(committed.id),
            Some(CachedMaterializedBlock { coordinate: committed.coordinate, selected_parent_id: parent_a_id })
        );
        assert_eq!(client.cached_materialized_block(boundary_parent_id), None);
        assert_eq!(client.caches.load().materialized_block(parent_a_id).map(|block| block.coordinate), BlockCoordinate::new(1, 0));
        assert_eq!(
            client.block_presence(own_hash).await,
            Ok(BlockPresence::Materialized { id: committed.id, coordinate: committed.coordinate })
        );

        let second_hash = hash(8);
        let second = client
            .materialize_block(
                block(second_hash, parent_a, vec![parent_a, parent_b], vec![parent_a], vec![parent_b]),
                ReferencePolicy::RequireMaterialized,
            )
            .await
            .expect("strict materialization from materialized references");
        let MaterializeBlockOutcome::Inserted { committed: second } = second else {
            panic!("second new block must be inserted");
        };
        assert_eq!(second.coordinate, BlockCoordinate::new(3, 1).expect("coordinate"));
        assert_eq!(
            second.level_snapshots.as_ref(),
            [
                LevelCommitted { level: 1, size: 1, daa_score: Some(10) },
                LevelCommitted { level: 2, size: 1, daa_score: None },
                LevelCommitted { level: 3, size: 2, daa_score: None },
            ]
        );

        let anticone_root_hash = hash(9);
        let anticone_root = client
            .materialize_block(
                block(anticone_root_hash, boundary_parent, vec![boundary_parent], vec![boundary_parent], vec![boundary_parent]),
                ReferencePolicy::AllowBoundaryIdentities,
            )
            .await
            .expect("outside-boundary anticone root materialization");
        let MaterializeBlockOutcome::Inserted { committed: anticone_root } = anticone_root else {
            panic!("anticone root must be inserted");
        };
        assert_eq!(anticone_root.coordinate, BlockCoordinate::new(1, 1).expect("coordinate"));
        assert_eq!(anticone_root.level_snapshots.as_ref(), [LevelCommitted { level: 1, size: 2, daa_score: Some(10) }]);
        assert_eq!(anticone_root.direct_parents.as_ref(), [ParentCommitted { hash: boundary_parent, coordinate: None }]);

        let dedup = client
            .materialize_block(
                block(own_hash, hash(99), vec![hash(99)], vec![hash(98)], vec![hash(97)]),
                ReferencePolicy::RequireMaterialized,
            )
            .await;
        assert_eq!(dedup, Ok(MaterializeBlockOutcome::AlreadyMaterialized { id: committed.id, coordinate: committed.coordinate }));
    }

    #[tokio::test]
    async fn strict_materialization_classifies_all_references_before_mutation_and_own_boundary_wins() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, _retirements) = processing_client(&database_url).await;
        let origin_id = insert_identity(&mut database, hash(1)).await;
        insert_level(&mut database, 1, 1, Some(10)).await;
        let selected_parent = hash(2);
        insert_materialized(&mut database, selected_parent, origin_id, BlockCoordinate::new(1, 0).expect("coordinate")).await;
        let boundary = hash(3);
        insert_identity(&mut database, boundary).await;
        let first_missing = hash(4);
        let second_missing = hash(5);
        let own_hash = hash(6);
        let input = block(
            own_hash,
            selected_parent,
            vec![selected_parent, boundary],
            vec![boundary, second_missing],
            vec![first_missing, boundary],
        );
        let identities_before = identity_count(&mut database).await;

        let strict = client.materialize_block(input.clone(), ReferencePolicy::RequireMaterialized).await;
        assert_eq!(
            strict,
            Err(MaterializeBlockError::NonMaterializedReferences {
                missing: vec![first_missing, second_missing].into(),
                identity_only: vec![boundary].into(),
            })
        );
        assert_eq!(identity_count(&mut database).await, identities_before);
        assert!(!client.has_cached_identity(own_hash));
        assert!(!client.has_cached_identity(first_missing));
        assert!(!client.has_cached_identity(second_missing));
        assert_eq!(client.block_presence(own_hash).await, Ok(BlockPresence::Absent));

        insert_identity(&mut database, own_hash).await;
        let boundary_own = client.materialize_block(input, ReferencePolicy::AllowBoundaryIdentities).await;
        assert_eq!(boundary_own, Err(MaterializeBlockError::IncomingBoundaryIdentity { hash: own_hash }));
        assert_eq!(identity_count(&mut database).await, identities_before + 1);
    }

    #[tokio::test]
    async fn materialization_retries_only_authorized_postgres_failures() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, _retirements) = processing_client(&database_url).await;
        let origin_id = insert_identity(&mut database, hash(1)).await;
        insert_level(&mut database, 1, 1, Some(10)).await;
        let selected_parent = hash(2);
        insert_materialized(&mut database, selected_parent, origin_id, BlockCoordinate::new(1, 0).expect("coordinate")).await;
        install_retry_trigger(&mut database).await;

        let retried_hash = hash(3);
        let retried = client
            .materialize_block(
                block(retried_hash, selected_parent, vec![selected_parent], Vec::new(), Vec::new()),
                ReferencePolicy::RequireMaterialized,
            )
            .await;
        assert!(matches!(retried, Ok(MaterializeBlockOutcome::Inserted { .. })));
        let attempts: i64 = sqlx::query_scalar("SELECT last_value FROM materialization_attempts")
            .fetch_one(database.connection_mut())
            .await
            .expect("retry attempt count");
        assert_eq!(attempts, 3);

        replace_retry_trigger(&mut database, "40001").await;
        let exhausted_hash = hash(4);
        let exhausted = client
            .materialize_block(
                block(exhausted_hash, selected_parent, vec![selected_parent], Vec::new(), Vec::new()),
                ReferencePolicy::RequireMaterialized,
            )
            .await;
        assert_eq!(exhausted, Err(MaterializeBlockError::Storage(StorageError::Persistence(PersistenceFault::RetryExhausted))));
        assert!(client.is_valid());
        assert!(!client.has_cached_identity(exhausted_hash));
        assert_eq!(client.block_presence(exhausted_hash).await, Ok(BlockPresence::Absent));
        let attempts: i64 = sqlx::query_scalar("SELECT last_value FROM materialization_attempts")
            .fetch_one(database.connection_mut())
            .await
            .expect("exhausted attempt count");
        assert_eq!(attempts, 4);

        replace_retry_trigger(&mut database, "23505").await;
        let definite_hash = hash(5);
        let definite = client
            .materialize_block(
                block(definite_hash, selected_parent, vec![selected_parent], Vec::new(), Vec::new()),
                ReferencePolicy::RequireMaterialized,
            )
            .await;
        assert_eq!(definite, Err(MaterializeBlockError::Storage(StorageError::Persistence(PersistenceFault::DefiniteFailure))));
        assert!(client.is_valid());
        assert!(!client.has_cached_identity(definite_hash));
        assert_eq!(client.block_presence(definite_hash).await, Ok(BlockPresence::Absent));
    }

    #[tokio::test]
    async fn ambiguous_materialization_retires_and_publishes_no_cache_for_either_database_truth() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, mut retirements) = processing_client(&database_url).await;
        let origin_id = insert_identity(&mut database, hash(1)).await;
        insert_level(&mut database, 1, 1, Some(10)).await;
        let selected_parent = hash(2);
        insert_materialized(&mut database, selected_parent, origin_id, BlockCoordinate::new(1, 0).expect("coordinate")).await;

        let committed_hash = hash(3);
        client.inject_materialization_commit_behavior(super::CommitBehavior::CommitThenLoseAcknowledgement);
        let operation_client = Arc::clone(&client);
        let operation = tokio::spawn(async move {
            operation_client
                .materialize_block(
                    block(committed_hash, selected_parent, vec![selected_parent], Vec::new(), Vec::new()),
                    ReferencePolicy::RequireMaterialized,
                )
                .await
        });
        complete_processing_retirement(&mut retirements, &client).await;
        assert_eq!(
            operation.await.expect("committed ambiguous operation"),
            Err(MaterializeBlockError::Storage(StorageError::Persistence(PersistenceFault::AmbiguousCommit)))
        );
        assert!(!client.has_cached_identity(committed_hash));
        let committed: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM blocks block
                 JOIN block_identifiers identity ON identity.id = block.id
                 WHERE identity.hash = $1
             )",
        )
        .bind(committed_hash.as_bytes().as_slice())
        .fetch_one(database.connection_mut())
        .await
        .expect("committed ambiguous truth");
        assert!(committed);

        let (replacement_tx, mut replacement_retirements) = retirement_channel();
        let replacement = open_processing_generation(database_url.clone(), client.binding(), replacement_tx)
            .await
            .expect("replacement processing generation");
        let rolled_back_hash = hash(4);
        replacement.inject_materialization_commit_behavior(super::CommitBehavior::RollbackThenLoseAcknowledgement);
        let operation_client = Arc::clone(&replacement);
        let operation = tokio::spawn(async move {
            operation_client
                .materialize_block(
                    block(rolled_back_hash, selected_parent, vec![selected_parent], Vec::new(), Vec::new()),
                    ReferencePolicy::RequireMaterialized,
                )
                .await
        });
        complete_processing_retirement(&mut replacement_retirements, &replacement).await;
        assert_eq!(
            operation.await.expect("rolled-back ambiguous operation"),
            Err(MaterializeBlockError::Storage(StorageError::Persistence(PersistenceFault::AmbiguousCommit)))
        );
        assert!(!replacement.has_cached_identity(rolled_back_hash));
        let rolled_back: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM block_identifiers WHERE hash = $1)")
            .bind(rolled_back_hash.as_bytes().as_slice())
            .fetch_one(database.connection_mut())
            .await
            .expect("rolled-back ambiguous truth");
        assert!(!rolled_back);
    }

    #[tokio::test]
    async fn precommit_connection_loss_rolls_back_and_retires_the_generation() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, mut retirements) = processing_client(&database_url).await;
        let origin_id = insert_identity(&mut database, hash(1)).await;
        insert_level(&mut database, 1, 1, Some(10)).await;
        let selected_parent = hash(2);
        insert_materialized(&mut database, selected_parent, origin_id, BlockCoordinate::new(1, 0).expect("coordinate")).await;
        install_connection_loss_trigger(&mut database).await;
        let own_hash = hash(3);
        let operation_client = Arc::clone(&client);
        let operation = tokio::spawn(async move {
            operation_client
                .materialize_block(
                    block(own_hash, selected_parent, vec![selected_parent], Vec::new(), Vec::new()),
                    ReferencePolicy::RequireMaterialized,
                )
                .await
        });

        timeout(Duration::from_secs(5), complete_processing_retirement(&mut retirements, &client))
            .await
            .expect("connection-loss retirement barrier");
        assert_eq!(
            operation.await.expect("connection-loss operation"),
            Err(MaterializeBlockError::Storage(StorageError::GenerationLost))
        );
        assert!(!client.has_cached_identity(own_hash));
        let persisted: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM block_identifiers WHERE hash = $1)")
            .bind(own_hash.as_bytes().as_slice())
            .fetch_one(database.connection_mut())
            .await
            .expect("connection-loss database truth");
        assert!(!persisted);
    }
}
