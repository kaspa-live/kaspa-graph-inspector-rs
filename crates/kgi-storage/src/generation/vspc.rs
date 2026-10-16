use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use kgi_model::{
    block::{BlockColor, BlockCoordinate, BlockHash, CompactId},
    graph_update::LevelCommitted,
    vspc::ReadyVspcChange,
};
use sqlx::{PgPool, Postgres, Row, Transaction};

use super::ValidatedDbClient;
use crate::{
    cache::{CachedMaterializedBlock, CachedMergeSets, CachedVspcSink, ProcessingCacheDomains},
    database::NO_VSPC_DAA_SCORE,
    error::{ProcessingDbError, StorageError},
    operation::{ApplyVspcChangeError, VspcCommitOutcome, VspcPathConflict},
    transaction::{self, TransactionAttemptError},
};

#[derive(Clone, Copy)]
pub(super) enum CommitBehavior {
    Normal,
    #[cfg(test)]
    CommitThenLoseAcknowledgement,
}

struct AttemptSuccess {
    outcome: VspcCommitOutcome,
    member_cache_updates: Vec<(CompactId, CachedMaterializedBlock)>,
    merge_set_cache_updates: Vec<(CompactId, CachedMergeSets)>,
}

#[derive(Clone, Copy)]
struct VspcPathConflictIds {
    child: CompactId,
    expected_parent: CompactId,
    stored_parent: CompactId,
}

enum SemanticError {
    SourceDiscontinuity,
    MemberNotMaterialized { id: CompactId },
    PathDiscontinuity(VspcPathConflict),
}

impl ValidatedDbClient {
    /// Atomically applies one materialized, endpoint-resolved VSPC transition.
    pub async fn apply_vspc_change(&self, change: ReadyVspcChange) -> Result<VspcCommitOutcome, ApplyVspcChangeError> {
        if change.added.is_empty() {
            return Err(ApplyVspcChangeError::Database(ProcessingDbError::ServiceUnavailable {
                diagnostic: Arc::from("a ready VSPC change must contain an added path"),
            }));
        }
        let commit_behavior = self.next_vspc_commit_behavior();
        self.run_operation(|| async {
            let _mutation = self.mutation_guard.read().await;
            let _lane = self.vspc_lane.lock().await;
            let caches = self.caches.load_full();
            let result =
                transaction::run(&self.transaction_timing, || attempt(self.runtime.pool(), &caches, &change, commit_behavior)).await?;
            let success = match result {
                Ok(success) => success,
                Err(SemanticError::SourceDiscontinuity) => return Err(StorageError::VspcSourceDiscontinuity),
                Err(SemanticError::MemberNotMaterialized { id }) => {
                    return Err(StorageError::VspcMemberNotMaterialized { id });
                }
                Err(SemanticError::PathDiscontinuity(conflict)) => {
                    return Err(StorageError::VspcPathDiscontinuity(conflict));
                }
            };

            for (id, block) in success.member_cache_updates {
                caches.publish_materialized_block(id, block);
            }
            for (id, merge_sets) in success.merge_set_cache_updates {
                caches.publish_merge_sets(id, merge_sets);
            }
            caches
                .publish_vspc_sink(CachedVspcSink { id: success.outcome.destination.id(), hash: success.outcome.destination.hash() });
            Ok(success.outcome)
        })
        .await
        .map_err(|error| match error {
            StorageError::VspcSourceDiscontinuity => ApplyVspcChangeError::VspcSourceDiscontinuity,
            StorageError::VspcMemberNotMaterialized { id } => ApplyVspcChangeError::VspcMemberNotMaterialized { id },
            StorageError::VspcPathDiscontinuity(conflict) => ApplyVspcChangeError::VspcPathDiscontinuity(conflict),
            error => ApplyVspcChangeError::Database(ProcessingDbError::mutation(error)),
        })
    }

    fn next_vspc_commit_behavior(&self) -> CommitBehavior {
        #[cfg(test)]
        {
            return self
                .vspc_commit_behavior
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .unwrap_or(CommitBehavior::Normal);
        }
        #[cfg(not(test))]
        CommitBehavior::Normal
    }

    #[cfg(test)]
    fn inject_vspc_commit_behavior(&self, behavior: CommitBehavior) {
        *self.vspc_commit_behavior.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(behavior);
    }
}

async fn attempt(
    pool: &PgPool,
    caches: &ProcessingCacheDomains,
    change: &ReadyVspcChange,
    commit_behavior: CommitBehavior,
) -> Result<AttemptSuccess, TransactionAttemptError<SemanticError>> {
    let mut transaction = pool.begin().await.map_err(|error| TransactionAttemptError::database("VSPC transaction start", error))?;

    let current_sink = load_current_sink(&mut transaction, caches).await?;
    if current_sink != (change.source.id(), change.source.hash()) {
        return rollback_semantic(transaction, SemanticError::SourceDiscontinuity, "VSPC source validation rollback").await;
    }

    let member_ids = distinct_member_ids(change);
    let (members, member_cache_updates) = load_members(&mut transaction, caches, &member_ids).await?;
    if let Some(id) = change.removed.iter().chain(change.added.iter()).find(|id| !members.contains_key(id)).copied() {
        return rollback_semantic(transaction, SemanticError::MemberNotMaterialized { id }, "VSPC materiality validation rollback")
            .await;
    }
    if change.removed.first().is_some_and(|id| *id != change.source.id()) {
        return rollback_semantic(transaction, SemanticError::SourceDiscontinuity, "VSPC removed-source validation rollback").await;
    }
    if let Some(conflict_ids) = first_path_conflict(change, &members) {
        let conflict = resolve_path_conflict(&mut transaction, conflict_ids).await?;
        return rollback_semantic(transaction, SemanticError::PathDiscontinuity(conflict), "VSPC selected-parent validation rollback")
            .await;
    }

    let (merge_sets, merge_set_cache_updates) = load_merge_sets(&mut transaction, caches, &change.added).await?;
    apply_membership(&mut transaction, &change.removed, &change.added).await?;
    apply_merge_set_colors(&mut transaction, &change.added, &merge_sets).await?;
    let affected_levels: BTreeSet<_> =
        member_ids.iter().map(|id| members.get(id).expect("every VSPC member was proven materialized").coordinate.level()).collect();
    let level_snapshots = update_level_scores(&mut transaction, &affected_levels).await?;

    commit(transaction, commit_behavior).await.map_err(|error| TransactionAttemptError::commit("VSPC transaction commit", error))?;
    Ok(AttemptSuccess {
        outcome: VspcCommitOutcome { destination: change.destination, level_snapshots },
        member_cache_updates,
        merge_set_cache_updates,
    })
}

async fn rollback_semantic<T>(
    transaction: Transaction<'_, Postgres>,
    error: SemanticError,
    operation: &'static str,
) -> Result<T, TransactionAttemptError<SemanticError>> {
    transaction.rollback().await.map_err(|error| TransactionAttemptError::database(operation, error))?;
    Err(TransactionAttemptError::Semantic(error))
}

async fn load_current_sink(
    transaction: &mut Transaction<'_, Postgres>,
    caches: &ProcessingCacheDomains,
) -> Result<(CompactId, BlockHash), TransactionAttemptError<SemanticError>> {
    if let Some(sink) = caches.vspc_sink() {
        return Ok((sink.id, sink.hash));
    }
    let row = sqlx::query(
        "SELECT block.id, identity.hash
         FROM blocks block
         JOIN block_identifiers identity ON identity.id = block.id
         WHERE block.is_in_vspc
         ORDER BY block.id DESC
         LIMIT 1",
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("committed VSPC sink load", error))?
    .ok_or_else(|| TransactionAttemptError::Storage(StorageError::invalid_metadata("database has no committed VSPC sink")))?;
    let raw_id: i64 = row.try_get("id").map_err(|error| TransactionAttemptError::database("VSPC sink ID decode", error))?;
    let id = decode_id(raw_id, "committed VSPC sink")?;
    let encoded_hash: Vec<u8> =
        row.try_get("hash").map_err(|error| TransactionAttemptError::database("VSPC sink hash decode", error))?;
    let hash = decode_hash(&encoded_hash, "committed VSPC sink")?;
    Ok((id, hash))
}

fn distinct_member_ids(change: &ReadyVspcChange) -> Vec<CompactId> {
    let mut seen = HashSet::new();
    change.removed.iter().chain(change.added.iter()).copied().filter(|id| seen.insert(*id)).collect()
}

async fn load_members(
    transaction: &mut Transaction<'_, Postgres>,
    caches: &ProcessingCacheDomains,
    ids: &[CompactId],
) -> Result<
    (HashMap<CompactId, CachedMaterializedBlock>, Vec<(CompactId, CachedMaterializedBlock)>),
    TransactionAttemptError<SemanticError>,
> {
    let (mut members, misses) = seed_members_from_cache(caches, ids);
    if misses.is_empty() {
        return Ok((members, Vec::new()));
    }

    let encoded: Vec<i64> = misses.iter().map(|id| id.get()).collect();
    let rows = sqlx::query(
        "SELECT block.id, block.level, block.slot, block.selected_parent_id
         FROM blocks block
         WHERE block.id = ANY($1::BIGINT[])",
    )
    .bind(encoded)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("VSPC member load", error))?;

    let mut cache_updates = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_id: i64 = row.try_get("id").map_err(|error| TransactionAttemptError::database("VSPC member ID decode", error))?;
        let id = decode_id(raw_id, "VSPC member")?;
        let raw_level: i64 =
            row.try_get("level").map_err(|error| TransactionAttemptError::database("VSPC member level decode", error))?;
        let raw_slot: i32 =
            row.try_get("slot").map_err(|error| TransactionAttemptError::database("VSPC member slot decode", error))?;
        let coordinate =
            u32::try_from(raw_level).ok().and_then(|level| BlockCoordinate::new(level, u16::try_from(raw_slot).ok()?)).ok_or_else(
                || TransactionAttemptError::Storage(StorageError::invalid_metadata("stored VSPC member coordinate is invalid")),
            )?;
        let raw_selected_parent_id: i64 = row
            .try_get("selected_parent_id")
            .map_err(|error| TransactionAttemptError::database("VSPC member selected-parent ID decode", error))?;
        let selected_parent_id = decode_id(raw_selected_parent_id, "VSPC member selected parent")?;
        let block = CachedMaterializedBlock { coordinate, selected_parent_id };
        members.insert(id, block);
        cache_updates.push((id, block));
    }
    Ok((members, cache_updates))
}

fn seed_members_from_cache(
    caches: &ProcessingCacheDomains,
    ids: &[CompactId],
) -> (HashMap<CompactId, CachedMaterializedBlock>, Vec<CompactId>) {
    let mut members = HashMap::with_capacity(ids.len());
    let mut misses = Vec::new();
    for id in ids {
        let Some(block) = caches.materialized_block(*id) else {
            misses.push(*id);
            continue;
        };
        members.insert(*id, block);
    }
    (members, misses)
}

fn first_path_conflict(
    change: &ReadyVspcChange,
    members: &HashMap<CompactId, CachedMaterializedBlock>,
) -> Option<VspcPathConflictIds> {
    let stored = |id: CompactId| members.get(&id).expect("every direct VSPC member was loaded");
    let conflict = |child: CompactId, expected_parent: CompactId| {
        let stored_parent = stored(child).selected_parent_id;
        (stored_parent != expected_parent).then_some(VspcPathConflictIds { child, expected_parent, stored_parent })
    };

    if change.removed.is_empty() {
        return conflict(change.added[0], change.source.id());
    }

    for pair in change.removed.windows(2) {
        if let Some(conflict) = conflict(pair[0], pair[1]) {
            return Some(conflict);
        }
    }

    let child = change.added[0];
    let expected_parent = stored(*change.removed.last().expect("the removed path is nonempty")).selected_parent_id;
    if let Some(conflict) = conflict(child, expected_parent) {
        return Some(conflict);
    }

    for pair in change.added.windows(2) {
        if let Some(conflict) = conflict(pair[1], pair[0]) {
            return Some(conflict);
        }
    }
    None
}

async fn resolve_path_conflict(
    transaction: &mut Transaction<'_, Postgres>,
    conflict: VspcPathConflictIds,
) -> Result<VspcPathConflict, TransactionAttemptError<SemanticError>> {
    let mut seen = HashSet::new();
    let ids: Vec<_> =
        [conflict.child, conflict.expected_parent, conflict.stored_parent].into_iter().filter(|id| seen.insert(*id)).collect();
    let encoded: Vec<_> = ids.iter().map(|id| id.get()).collect();
    let rows = sqlx::query("SELECT id, hash FROM block_identifiers WHERE id = ANY($1::BIGINT[])")
        .bind(encoded)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| TransactionAttemptError::database("VSPC path-conflict evidence load", error))?;
    if rows.len() != ids.len() {
        return Err(TransactionAttemptError::Storage(StorageError::invalid_metadata("VSPC path-conflict evidence is incomplete")));
    }

    let mut hashes = HashMap::with_capacity(rows.len());
    for row in rows {
        let raw_id: i64 = row.try_get("id").map_err(|error| TransactionAttemptError::database("VSPC conflict ID decode", error))?;
        let id = decode_id(raw_id, "VSPC path-conflict evidence")?;
        let encoded_hash: Vec<u8> =
            row.try_get("hash").map_err(|error| TransactionAttemptError::database("VSPC conflict hash decode", error))?;
        let hash = decode_hash(&encoded_hash, "VSPC path-conflict evidence")?;
        hashes.insert(id, hash);
    }

    let resolve = |id: CompactId| {
        hashes.get(&id).copied().ok_or_else(|| {
            TransactionAttemptError::Storage(StorageError::invalid_metadata("VSPC path-conflict evidence is incomplete"))
        })
    };
    Ok(VspcPathConflict {
        child: resolve(conflict.child)?,
        expected_parent: resolve(conflict.expected_parent)?,
        stored_parent: resolve(conflict.stored_parent)?,
    })
}

async fn load_merge_sets(
    transaction: &mut Transaction<'_, Postgres>,
    caches: &ProcessingCacheDomains,
    added: &[CompactId],
) -> Result<(HashMap<CompactId, CachedMergeSets>, Vec<(CompactId, CachedMergeSets)>), TransactionAttemptError<SemanticError>> {
    let mut merge_sets = HashMap::new();
    let mut misses = Vec::new();
    let mut seen = HashSet::new();
    for id in added.iter().copied().filter(|id| seen.insert(*id)) {
        if let Some(cached) = caches.merge_sets(id) {
            merge_sets.insert(id, cached);
        } else {
            misses.push(id);
        }
    }
    if misses.is_empty() {
        return Ok((merge_sets, Vec::new()));
    }

    let encoded: Vec<i64> = misses.iter().map(|id| id.get()).collect();
    let rows = sqlx::query("SELECT id, blue_merge_set, red_merge_set FROM blocks WHERE id = ANY($1::BIGINT[])")
        .bind(encoded)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| TransactionAttemptError::database("VSPC merge-set load", error))?;
    let mut cache_updates = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_id: i64 =
            row.try_get("id").map_err(|error| TransactionAttemptError::database("VSPC merge-set owner decode", error))?;
        let id = decode_id(raw_id, "VSPC merge-set owner")?;
        let blue: Vec<i64> =
            row.try_get("blue_merge_set").map_err(|error| TransactionAttemptError::database("blue merge-set decode", error))?;
        let red: Vec<i64> =
            row.try_get("red_merge_set").map_err(|error| TransactionAttemptError::database("red merge-set decode", error))?;
        let sets = CachedMergeSets { blue: decode_ids(&blue, "blue merge set")?, red: decode_ids(&red, "red merge set")? };
        merge_sets.insert(id, sets.clone());
        cache_updates.push((id, sets));
    }
    if merge_sets.len() != seen.len() {
        return Err(TransactionAttemptError::Storage(StorageError::invalid_metadata(
            "materialized added block has no persisted merge sets",
        )));
    }
    Ok((merge_sets, cache_updates))
}

async fn apply_membership(
    transaction: &mut Transaction<'_, Postgres>,
    removed: &[CompactId],
    added: &[CompactId],
) -> Result<(), TransactionAttemptError<SemanticError>> {
    if !removed.is_empty() {
        let ids: Vec<i64> = removed.iter().map(|id| id.get()).collect();
        sqlx::query("UPDATE blocks SET is_in_vspc = FALSE, color = $2 WHERE id = ANY($1::BIGINT[])")
            .bind(ids)
            .bind(BlockColor::Gray as i16)
            .execute(&mut **transaction)
            .await
            .map_err(|error| TransactionAttemptError::database("VSPC removal", error))?;
    }
    let ids: Vec<i64> = added.iter().map(|id| id.get()).collect();
    sqlx::query("UPDATE blocks SET is_in_vspc = TRUE WHERE id = ANY($1::BIGINT[])")
        .bind(ids)
        .execute(&mut **transaction)
        .await
        .map_err(|error| TransactionAttemptError::database("VSPC addition", error))?;
    Ok(())
}

async fn apply_merge_set_colors(
    transaction: &mut Transaction<'_, Postgres>,
    added: &[CompactId],
    merge_sets: &HashMap<CompactId, CachedMergeSets>,
) -> Result<(), TransactionAttemptError<SemanticError>> {
    let mut final_colors = HashMap::new();
    for id in added {
        let sets = merge_sets.get(id).expect("every added block has immutable merge sets");
        for member in sets.blue.iter().copied() {
            final_colors.insert(member, BlockColor::Blue);
        }
        for member in sets.red.iter().copied() {
            final_colors.insert(member, BlockColor::Red);
        }
    }
    if final_colors.is_empty() {
        return Ok(());
    }
    let mut colors: Vec<_> = final_colors.into_iter().collect();
    colors.sort_unstable_by_key(|(id, _)| *id);
    let ids: Vec<i64> = colors.iter().map(|(id, _)| id.get()).collect();
    let values: Vec<i16> = colors.iter().map(|(_, color)| *color as i16).collect();
    sqlx::query(
        "UPDATE blocks AS block
         SET color = input.color
         FROM UNNEST($1::BIGINT[], $2::SMALLINT[]) AS input(id, color)
         WHERE block.id = input.id",
    )
    .bind(ids)
    .bind(values)
    .execute(&mut **transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("VSPC merge-set coloring", error))?;
    Ok(())
}

async fn update_level_scores(
    transaction: &mut Transaction<'_, Postgres>,
    levels: &BTreeSet<u32>,
) -> Result<Arc<[LevelCommitted]>, TransactionAttemptError<SemanticError>> {
    let encoded: Vec<i64> = levels.iter().copied().map(i64::from).collect();
    let rows = sqlx::query(
        "WITH updated AS (
             UPDATE levels AS level
             SET daa_score = COALESCE(
                 (SELECT block.daa_score
                  FROM blocks block
                  WHERE block.level = level.level AND block.is_in_vspc),
                 $2
             )
             WHERE level.level = ANY($1::BIGINT[])
             RETURNING level.level, level.size, level.daa_score
         )
         SELECT level, size, daa_score FROM updated ORDER BY level",
    )
    .bind(encoded)
    .bind(NO_VSPC_DAA_SCORE)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("VSPC level-score update", error))?;
    if rows.len() != levels.len() {
        return Err(TransactionAttemptError::Storage(StorageError::invalid_metadata("affected VSPC block has no level-state row")));
    }
    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_level: i64 = row.try_get("level").map_err(|error| TransactionAttemptError::database("VSPC level decode", error))?;
        let raw_size: i32 = row.try_get("size").map_err(|error| TransactionAttemptError::database("VSPC level size decode", error))?;
        let raw_daa: i64 =
            row.try_get("daa_score").map_err(|error| TransactionAttemptError::database("VSPC level DAA decode", error))?;
        let level = u32::try_from(raw_level)
            .ok()
            .filter(|level| *level > 0)
            .ok_or_else(|| TransactionAttemptError::Storage(StorageError::invalid_metadata("stored VSPC level is invalid")))?;
        let size = u16::try_from(raw_size)
            .ok()
            .filter(|size| *size > 0)
            .ok_or_else(|| TransactionAttemptError::Storage(StorageError::invalid_metadata("stored VSPC level size is invalid")))?;
        let daa_score = if raw_daa == NO_VSPC_DAA_SCORE {
            None
        } else {
            Some(u64::try_from(raw_daa).map_err(|_| {
                TransactionAttemptError::Storage(StorageError::invalid_metadata("stored VSPC level DAA score is negative"))
            })?)
        };
        snapshots.push(LevelCommitted { level, size, daa_score });
    }
    Ok(Arc::from(snapshots))
}

fn decode_id(raw: i64, context: &'static str) -> Result<CompactId, TransactionAttemptError<SemanticError>> {
    CompactId::new(raw).ok_or_else(|| {
        TransactionAttemptError::Storage(StorageError::invalid_metadata(format!("{context} compact ID is not positive: {raw}")))
    })
}

fn decode_ids(raw: &[i64], context: &'static str) -> Result<Arc<[CompactId]>, TransactionAttemptError<SemanticError>> {
    raw.iter().map(|id| decode_id(*id, context)).collect::<Result<Vec<_>, _>>().map(Arc::from)
}

fn decode_hash(raw: &[u8], context: &'static str) -> Result<BlockHash, TransactionAttemptError<SemanticError>> {
    BlockHash::try_from(raw)
        .map_err(|error| TransactionAttemptError::Storage(StorageError::invalid_metadata(format!("invalid {context} hash: {error}"))))
}

async fn commit(transaction: Transaction<'_, Postgres>, behavior: CommitBehavior) -> Result<(), sqlx::Error> {
    match behavior {
        CommitBehavior::Normal => transaction.commit().await,
        #[cfg(test)]
        CommitBehavior::CommitThenLoseAcknowledgement => {
            transaction.commit().await?;
            Err(sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "injected VSPC COMMIT acknowledgement loss")))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_model::{
        block::{BlockColor, BlockCoordinate, BlockHash, CompactId, ConsensusOrder, VspcPoint},
        graph_update::LevelCommitted,
        lifecycle::PersistenceFault,
        vspc::ReadyVspcChange,
    };
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    };

    use super::{CommitBehavior, VspcPathConflictIds, resolve_path_conflict, seed_members_from_cache};
    use crate::{
        cache::{CachedMaterializedBlock, ProcessingCacheDomains},
        database::{LockedDatabase, PreparedDatabase, open_processing_generation},
        error::{ProcessingDbError, StorageError},
        generation::ValidatedDbClient,
        operation::{ApplyVspcChangeError, VspcPathConflict},
        runtime::{RetirementReceiver, RetirementTarget, retirement_channel},
        state::DatabaseState,
    };

    const POSTGRES_PORT: u16 = 5432;

    struct GraphFixture {
        common: (BlockHash, CompactId),
        old_parent: (BlockHash, CompactId),
        old_sink: (BlockHash, CompactId),
        new_parent: (BlockHash, CompactId),
        new_sink: (BlockHash, CompactId),
        blue_member: (BlockHash, CompactId),
        red_member: (BlockHash, CompactId),
        boundary: (BlockHash, CompactId),
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    fn id(value: i64) -> CompactId {
        CompactId::new(value).expect("positive compact ID")
    }

    #[test]
    fn member_cache_seeding_requires_only_materialized_block_metadata() {
        let caches = ProcessingCacheDomains::new();
        let complete = id(1);
        let missing = id(2);
        let block =
            CachedMaterializedBlock { coordinate: BlockCoordinate::new(1, 0).expect("coordinate"), selected_parent_id: id(11) };

        caches.publish_materialized_block(complete, block);
        let (members, misses) = seed_members_from_cache(&caches, &[complete, missing]);

        assert_eq!(members.len(), 1);
        assert_eq!(members[&complete], block);
        assert_eq!(misses, [missing]);
    }

    fn point(block: (BlockHash, CompactId)) -> VspcPoint {
        VspcPoint::new(ConsensusOrder::new(Default::default(), block.0), block.1)
    }

    fn change(
        source: (BlockHash, CompactId),
        destination: (BlockHash, CompactId),
        removed: Vec<CompactId>,
        added: Vec<CompactId>,
    ) -> ReadyVspcChange {
        ReadyVspcChange { source: point(source), destination: point(destination), removed: removed.into(), added: added.into() }
    }

    async fn fixture() -> (ContainerAsync<Postgres>, String) {
        let container = Postgres::default().with_tag("17-alpine").start().await.expect("PostgreSQL fixture must start");
        let host = container.get_host().await.expect("fixture host must resolve");
        let port = container.get_host_port_ipv4(POSTGRES_PORT).await.expect("fixture PostgreSQL port must resolve");
        let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
        (container, database_url)
    }

    async fn processing_client(database_url: &str) -> (PreparedDatabase, Arc<ValidatedDbClient>, RetirementReceiver) {
        let (mut database, state) =
            LockedDatabase::connect(database_url).await.expect("database lock").prepare().await.expect("schema preparation");
        assert_eq!(state, DatabaseState::Uninitialized);
        let state = database
            .initialize_if_uninitialized(NetworkId::new(NetworkType::Mainnet), hash(0), None)
            .await
            .expect("database initialization");
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

    struct BlockRow<'a> {
        hash: BlockHash,
        selected_parent: CompactId,
        level: i64,
        slot: i32,
        daa_score: i64,
        color: BlockColor,
        in_vspc: bool,
        blue: &'a [CompactId],
        red: &'a [CompactId],
    }

    async fn insert_block(database: &mut PreparedDatabase, block: BlockRow<'_>) -> CompactId {
        let id = insert_identity(database, block.hash).await;
        let blue: Vec<i64> = block.blue.iter().map(|id| id.get()).collect();
        let red: Vec<i64> = block.red.iter().map(|id| id.get()).collect();
        sqlx::query(
            "INSERT INTO blocks (
                 id, timestamp, daa_score, level, slot, selected_parent_id,
                 color, is_in_vspc, blue_merge_set, red_merge_set
             ) VALUES ($1, 0, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(id.get())
        .bind(block.daa_score)
        .bind(block.level)
        .bind(block.slot)
        .bind(block.selected_parent.get())
        .bind(block.color as i16)
        .bind(block.in_vspc)
        .bind(blue)
        .bind(red)
        .execute(database.connection_mut())
        .await
        .expect("block insertion");
        id
    }

    async fn install_graph(database: &mut PreparedDatabase) -> GraphFixture {
        let origin = insert_identity(database, hash(1)).await;
        let common = (
            hash(2),
            insert_block(
                database,
                BlockRow {
                    hash: hash(2),
                    selected_parent: origin,
                    level: 1,
                    slot: 0,
                    daa_score: 10,
                    color: BlockColor::Gray,
                    in_vspc: true,
                    blue: &[],
                    red: &[],
                },
            )
            .await,
        );
        let old_parent = (
            hash(3),
            insert_block(
                database,
                BlockRow {
                    hash: hash(3),
                    selected_parent: common.1,
                    level: 2,
                    slot: 0,
                    daa_score: 20,
                    color: BlockColor::Blue,
                    in_vspc: true,
                    blue: &[],
                    red: &[],
                },
            )
            .await,
        );
        let old_sink = (
            hash(4),
            insert_block(
                database,
                BlockRow {
                    hash: hash(4),
                    selected_parent: old_parent.1,
                    level: 3,
                    slot: 0,
                    daa_score: 30,
                    color: BlockColor::Red,
                    in_vspc: true,
                    blue: &[],
                    red: &[],
                },
            )
            .await,
        );
        let blue_member = (
            hash(5),
            insert_block(
                database,
                BlockRow {
                    hash: hash(5),
                    selected_parent: common.1,
                    level: 2,
                    slot: 1,
                    daa_score: 22,
                    color: BlockColor::Gray,
                    in_vspc: false,
                    blue: &[],
                    red: &[],
                },
            )
            .await,
        );
        let red_member = (
            hash(6),
            insert_block(
                database,
                BlockRow {
                    hash: hash(6),
                    selected_parent: common.1,
                    level: 2,
                    slot: 2,
                    daa_score: 23,
                    color: BlockColor::Gray,
                    in_vspc: false,
                    blue: &[],
                    red: &[],
                },
            )
            .await,
        );
        let boundary = (hash(7), insert_identity(database, hash(7)).await);
        let new_parent_blue = [blue_member.1, boundary.1];
        let new_parent_red = [blue_member.1];
        let new_parent = (
            hash(8),
            insert_block(
                database,
                BlockRow {
                    hash: hash(8),
                    selected_parent: common.1,
                    level: 2,
                    slot: 3,
                    daa_score: 21,
                    color: BlockColor::Gray,
                    in_vspc: false,
                    blue: &new_parent_blue,
                    red: &new_parent_red,
                },
            )
            .await,
        );
        let new_sink_blue = [blue_member.1];
        let new_sink_red = [red_member.1];
        let new_sink = (
            hash(9),
            insert_block(
                database,
                BlockRow {
                    hash: hash(9),
                    selected_parent: new_parent.1,
                    level: 3,
                    slot: 1,
                    daa_score: 31,
                    color: BlockColor::Gray,
                    in_vspc: false,
                    blue: &new_sink_blue,
                    red: &new_sink_red,
                },
            )
            .await,
        );
        for (level, size, daa_score) in [(1_i64, 1_i64, 10_i64), (2, 4, 20), (3, 2, 30)] {
            sqlx::query("INSERT INTO levels (level, size, daa_score) VALUES ($1, $2, $3)")
                .bind(level)
                .bind(size)
                .bind(daa_score)
                .execute(database.connection_mut())
                .await
                .expect("level insertion");
        }
        GraphFixture { common, old_parent, old_sink, new_parent, new_sink, blue_member, red_member, boundary }
    }

    async fn block_state(database: &mut PreparedDatabase, id: CompactId) -> (i16, bool) {
        sqlx::query_as("SELECT color, is_in_vspc FROM blocks WHERE id = $1")
            .bind(id.get())
            .fetch_one(database.connection_mut())
            .await
            .expect("block state")
    }

    #[tokio::test]
    async fn applies_ordered_reorg_coloring_and_final_level_snapshots_atomically() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, _retirements) = processing_client(&database_url).await;
        let graph = install_graph(&mut database).await;
        let transition = change(
            graph.old_sink,
            graph.new_sink,
            vec![graph.old_sink.1, graph.old_parent.1],
            vec![graph.new_parent.1, graph.new_sink.1],
        );

        let outcome = client.apply_vspc_change(transition).await.expect("VSPC transition");

        assert_eq!(outcome.destination, point(graph.new_sink));
        assert_eq!(
            outcome.level_snapshots.as_ref(),
            [LevelCommitted { level: 2, size: 4, daa_score: Some(21) }, LevelCommitted { level: 3, size: 2, daa_score: Some(31) },]
        );
        assert_eq!(block_state(&mut database, graph.old_parent.1).await, (BlockColor::Gray as i16, false));
        assert_eq!(block_state(&mut database, graph.old_sink.1).await, (BlockColor::Gray as i16, false));
        assert!(block_state(&mut database, graph.new_parent.1).await.1);
        assert!(block_state(&mut database, graph.new_sink.1).await.1);
        assert_eq!(block_state(&mut database, graph.blue_member.1).await.0, BlockColor::Blue as i16);
        assert_eq!(block_state(&mut database, graph.red_member.1).await.0, BlockColor::Red as i16);
        assert!(client.has_cached_merge_sets(graph.new_parent.1));
        assert!(client.has_cached_merge_sets(graph.new_sink.1));
        assert_eq!(
            client.cached_materialized_block(graph.new_sink.1),
            Some(CachedMaterializedBlock {
                coordinate: BlockCoordinate::new(3, 1).expect("coordinate"),
                selected_parent_id: graph.new_parent.1,
            })
        );
        assert_eq!(client.cached_vspc_sink(), Some(crate::cache::CachedVspcSink { id: graph.new_sink.1, hash: graph.new_sink.0 }));
        let boundary_block: Option<i64> = sqlx::query_scalar("SELECT id FROM blocks WHERE id = $1")
            .bind(graph.boundary.1.get())
            .fetch_optional(database.connection_mut())
            .await
            .expect("boundary lookup");
        assert_eq!(boundary_block, None);
    }

    #[tokio::test]
    async fn rejects_source_materiality_and_each_path_discontinuity_before_mutation() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, _retirements) = processing_client(&database_url).await;
        let graph = install_graph(&mut database).await;

        let wrong_source =
            client.apply_vspc_change(change(graph.old_parent, graph.new_parent, Vec::new(), vec![graph.new_parent.1])).await;
        assert_eq!(wrong_source, Err(ApplyVspcChangeError::VspcSourceDiscontinuity));

        let boundary = client.apply_vspc_change(change(graph.old_sink, graph.boundary, Vec::new(), vec![graph.boundary.1])).await;
        assert_eq!(boundary, Err(ApplyVspcChangeError::VspcMemberNotMaterialized { id: graph.boundary.1 }));

        let added_only =
            client.apply_vspc_change(change(graph.old_sink, graph.new_parent, Vec::new(), vec![graph.new_parent.1])).await;
        assert_eq!(
            added_only,
            Err(ApplyVspcChangeError::VspcPathDiscontinuity(VspcPathConflict {
                child: graph.new_parent.0,
                expected_parent: graph.old_sink.0,
                stored_parent: graph.common.0,
            }))
        );

        let removed_path = client
            .apply_vspc_change(change(
                graph.old_sink,
                graph.new_parent,
                vec![graph.old_sink.1, graph.common.1],
                vec![graph.new_parent.1],
            ))
            .await;
        assert_eq!(
            removed_path,
            Err(ApplyVspcChangeError::VspcPathDiscontinuity(VspcPathConflict {
                child: graph.old_sink.0,
                expected_parent: graph.common.0,
                stored_parent: graph.old_parent.0,
            }))
        );

        let pivot = client
            .apply_vspc_change(change(
                graph.old_sink,
                graph.new_sink,
                vec![graph.old_sink.1, graph.old_parent.1],
                vec![graph.new_sink.1],
            ))
            .await;
        assert_eq!(
            pivot,
            Err(ApplyVspcChangeError::VspcPathDiscontinuity(VspcPathConflict {
                child: graph.new_sink.0,
                expected_parent: graph.common.0,
                stored_parent: graph.new_parent.0,
            }))
        );

        let added_path = client
            .apply_vspc_change(change(
                graph.old_sink,
                graph.old_sink,
                vec![graph.old_sink.1, graph.old_parent.1],
                vec![graph.new_parent.1, graph.old_sink.1],
            ))
            .await;
        assert_eq!(
            added_path,
            Err(ApplyVspcChangeError::VspcPathDiscontinuity(VspcPathConflict {
                child: graph.old_sink.0,
                expected_parent: graph.new_parent.0,
                stored_parent: graph.old_parent.0,
            }))
        );

        let duplicate = client
            .apply_vspc_change(change(
                graph.old_sink,
                graph.old_sink,
                vec![graph.old_sink.1, graph.old_sink.1],
                vec![graph.old_sink.1],
            ))
            .await;
        assert!(matches!(duplicate, Err(ApplyVspcChangeError::VspcPathDiscontinuity(_))));
        assert_eq!(block_state(&mut database, graph.old_sink.1).await, (BlockColor::Red as i16, true));
        assert!(!block_state(&mut database, graph.new_parent.1).await.1);
        assert!(!block_state(&mut database, graph.new_sink.1).await.1);
        assert_eq!(client.cached_vspc_sink(), None);
        assert_eq!(client.cached_materialized_block(graph.new_parent.1), None);
    }

    #[tokio::test]
    async fn incomplete_path_conflict_evidence_is_invalid_metadata() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, _retirements) = processing_client(&database_url).await;
        let graph = install_graph(&mut database).await;
        let mut transaction = client.pool().begin().await.expect("evidence transaction");

        let result = resolve_path_conflict(
            &mut transaction,
            VspcPathConflictIds { child: graph.old_sink.1, expected_parent: id(i64::MAX), stored_parent: graph.old_parent.1 },
        )
        .await;

        assert!(matches!(result, Err(crate::transaction::TransactionAttemptError::Storage(StorageError::InvalidMetadata { .. }))));
        transaction.rollback().await.expect("evidence rollback");
    }

    #[tokio::test]
    async fn removed_added_intersection_restores_membership_and_publishes_final_level() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, _retirements) = processing_client(&database_url).await;
        let graph = install_graph(&mut database).await;

        let outcome = client
            .apply_vspc_change(change(graph.old_sink, graph.old_sink, vec![graph.old_sink.1], vec![graph.old_sink.1]))
            .await
            .expect("intersection transition");

        assert_eq!(outcome.destination, point(graph.old_sink));
        assert_eq!(outcome.level_snapshots.as_ref(), [LevelCommitted { level: 3, size: 2, daa_score: Some(30) }]);
        assert_eq!(block_state(&mut database, graph.old_sink.1).await, (BlockColor::Gray as i16, true));
    }

    async fn complete_processing_retirement(retirements: &mut RetirementReceiver, expected: &Arc<ValidatedDbClient>) {
        let request = retirements.recv().await.expect("processing retirement request");
        let retired = match request.target() {
            RetirementTarget::Processing(generation) => generation.upgrade().expect("processing generation remains alive"),
            RetirementTarget::Api(_) => panic!("VSPC mutation requested API retirement"),
        };
        assert!(Arc::ptr_eq(&retired, expected));
        assert!(retired.retire());
        request.complete(Ok(()));
    }

    #[tokio::test]
    async fn ambiguous_commit_retires_generation_without_publishing_merge_set_cache() {
        let (_container, database_url) = fixture().await;
        let (mut database, client, mut retirements) = processing_client(&database_url).await;
        let graph = install_graph(&mut database).await;
        client.inject_vspc_commit_behavior(CommitBehavior::CommitThenLoseAcknowledgement);
        let operation_client = Arc::clone(&client);
        let transition = change(
            graph.old_sink,
            graph.new_sink,
            vec![graph.old_sink.1, graph.old_parent.1],
            vec![graph.new_parent.1, graph.new_sink.1],
        );
        let operation = tokio::spawn(async move { operation_client.apply_vspc_change(transition).await });

        complete_processing_retirement(&mut retirements, &client).await;
        assert_eq!(
            operation.await.expect("VSPC task"),
            Err(ApplyVspcChangeError::Database(ProcessingDbError::Persistence(PersistenceFault::AmbiguousCommit)))
        );
        assert!(!client.is_valid());
        assert!(!client.has_cached_merge_sets(graph.new_parent.1));
        assert!(!client.has_cached_merge_sets(graph.new_sink.1));
        assert_eq!(client.cached_materialized_block(graph.new_parent.1), None);
        assert_eq!(client.cached_vspc_sink(), None);
        assert!(!block_state(&mut database, graph.old_sink.1).await.1);
        assert!(block_state(&mut database, graph.new_sink.1).await.1);
    }
}
