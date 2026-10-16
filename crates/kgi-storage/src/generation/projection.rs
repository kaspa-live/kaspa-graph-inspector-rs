use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use kgi_api_model::{
    graph::{EdgeId, GraphBlock, GraphEdge, Level, MAX_CACHE_DEPTH},
    projection::{
        GraphViewSeed, GraphViewSeedOutcome, GraphViewSeedRequest, GraphWindowAnchor, GraphWindowAnchorUnavailable,
        GraphWindowResolution, MAX_WINDOW_DEPTH,
    },
};
use kgi_model::block::{BlockColor, BlockCoordinate, BlockHash, CompactId, MAX_DAA_SCORE};
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::{
    database::{NO_VSPC_DAA_SCORE, timestamp_from_sql},
    error::{ApiReadError, sqlx_connection_lost},
};

pub(super) struct GraphProjection<'connection> {
    transaction: Transaction<'connection, Postgres>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProjectionScope {
    pub resolution: GraphWindowResolution,
    pub max_depth: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProjectionScopeOutcome {
    Resolved(ProjectionScope),
    AnchorUnavailable(GraphWindowAnchorUnavailable),
}

struct PreparedGraphBlock {
    id: CompactId,
    coordinate: BlockCoordinate,
    timestamp: u64,
    daa_score: u64,
    selected_parent_id: CompactId,
    blue_merge_set: Vec<CompactId>,
    red_merge_set: Vec<CompactId>,
    color: BlockColor,
    is_in_vspc: bool,
    direct_parents: Vec<CompactId>,
}

struct PreparedGraphEdge {
    child_id: CompactId,
    parent_id: CompactId,
    child_coordinate: BlockCoordinate,
    parent_coordinate: BlockCoordinate,
}

pub(super) async fn load(
    pool: &PgPool,
    genesis_hash: BlockHash,
    request: GraphViewSeedRequest,
) -> Result<GraphViewSeedOutcome, ApiReadError> {
    let mut projection = GraphProjection::begin(pool).await?;
    let scope = match projection.resolve_scope(request).await? {
        ProjectionScopeOutcome::Resolved(scope) => scope,
        ProjectionScopeOutcome::AnchorUnavailable(unavailable) => {
            projection.commit().await?;
            return Ok(GraphViewSeedOutcome::AnchorUnavailable(unavailable));
        }
    };
    let seed = projection.load_seed(scope, genesis_hash).await?;
    projection.commit().await?;
    Ok(GraphViewSeedOutcome::Loaded(seed))
}

impl<'connection> GraphProjection<'connection> {
    pub(super) async fn begin(pool: &'connection PgPool) -> Result<Self, ApiReadError> {
        let transaction = pool.begin().await.map_err(query_error)?;
        let mut projection = Self { transaction };
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *projection.transaction)
            .await
            .map_err(query_error)?;
        Ok(projection)
    }

    pub(super) async fn resolve_scope(&mut self, request: GraphViewSeedRequest) -> Result<ProjectionScopeOutcome, ApiReadError> {
        match request {
            GraphViewSeedRequest::HeadPublication => {
                let high_level = self.database_head().await?.ok_or(ApiReadError::InconsistentProjection)?;
                Ok(ProjectionScopeOutcome::Resolved(head_scope(high_level)))
            }
            GraphViewSeedRequest::AnchoredWindow { anchor, max_depth } => {
                assert!(
                    (1..=MAX_WINDOW_DEPTH).contains(&max_depth),
                    "anchored graph projection depth must be validated and capped by the API owner"
                );
                let Some(resolved_level) = self.resolve_anchor(anchor).await? else {
                    return Ok(ProjectionScopeOutcome::AnchorUnavailable(anchor_unavailable(anchor)));
                };
                let database_head = self.database_head().await?.ok_or(ApiReadError::InconsistentProjection)?;
                if resolved_level > database_head {
                    return Err(ApiReadError::InconsistentProjection);
                }
                Ok(ProjectionScopeOutcome::Resolved(ProjectionScope {
                    resolution: anchored_resolution(resolved_level, database_head, max_depth),
                    max_depth,
                }))
            }
        }
    }

    pub(super) async fn commit(self) -> Result<(), ApiReadError> {
        self.transaction.commit().await.map_err(query_error)
    }

    async fn load_seed(&mut self, scope: ProjectionScope, genesis_hash: BlockHash) -> Result<GraphViewSeed, ApiReadError> {
        let mut prepared = self.load_nominal_blocks(scope.resolution).await?;
        self.load_direct_parent_ids(scope.resolution, &mut prepared).await?;
        let prepared_edges = self.load_edges(scope.resolution).await?;
        let references = self.load_reference_hashes(&prepared, &prepared_edges).await?;
        let blocks = finalize_blocks(prepared, &references, genesis_hash)?;
        let edges = finalize_edges(prepared_edges, &references);
        let levels = self.load_levels(scope.resolution, &edges).await?;
        let (snapshot_max_materialized_id, snapshot_vspc_sink) = self.load_snapshot_metadata().await?;
        Ok(GraphViewSeed {
            resolution: scope.resolution,
            max_depth: scope.max_depth,
            levels,
            blocks,
            edges,
            snapshot_max_materialized_id,
            snapshot_vspc_sink,
        })
    }

    async fn load_nominal_blocks(
        &mut self,
        resolution: GraphWindowResolution,
    ) -> Result<HashMap<CompactId, PreparedGraphBlock>, ApiReadError> {
        let low = level_to_sql(resolution.effective_start_level)?;
        let high = level_to_sql(resolution.effective_end_level)?;
        let rows = sqlx::query(
            "SELECT block.id, block.timestamp, block.daa_score,
                    block.level, block.slot, block.selected_parent_id, block.color,
                    block.is_in_vspc, block.blue_merge_set, block.red_merge_set
             FROM blocks block
             WHERE block.level BETWEEN $1 AND $2
             ORDER BY block.level, block.slot",
        )
        .bind(low)
        .bind(high)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(query_error)?;

        let mut blocks = HashMap::with_capacity(rows.len());
        for row in rows {
            let id = decode_id(row.try_get("id").map_err(inconsistent_decode)?)?;
            let coordinate = decode_coordinate(
                row.try_get("level").map_err(inconsistent_decode)?,
                row.try_get("slot").map_err(inconsistent_decode)?,
            )?;
            let block = PreparedGraphBlock {
                id,
                coordinate,
                timestamp: timestamp_from_sql(row.try_get("timestamp").map_err(inconsistent_decode)?),
                daa_score: decode_daa_score(row.try_get("daa_score").map_err(inconsistent_decode)?)?,
                selected_parent_id: decode_id(row.try_get("selected_parent_id").map_err(inconsistent_decode)?)?,
                blue_merge_set: decode_ids(row.try_get("blue_merge_set").map_err(inconsistent_decode)?)?,
                red_merge_set: decode_ids(row.try_get("red_merge_set").map_err(inconsistent_decode)?)?,
                color: decode_color(row.try_get("color").map_err(inconsistent_decode)?)?,
                is_in_vspc: row.try_get("is_in_vspc").map_err(inconsistent_decode)?,
                direct_parents: Vec::new(),
            };
            let _ = blocks.insert(id, block);
        }
        Ok(blocks)
    }

    async fn load_reference_hashes(
        &mut self,
        blocks: &HashMap<CompactId, PreparedGraphBlock>,
        edges: &[PreparedGraphEdge],
    ) -> Result<HashMap<CompactId, BlockHash>, ApiReadError> {
        let mut required = HashSet::new();
        for block in blocks.values() {
            required.insert(block.id);
            required.extend(block.blue_merge_set.iter().copied());
            required.extend(block.red_merge_set.iter().copied());
            required.extend(block.direct_parents.iter().copied());
        }
        for edge in edges {
            required.insert(edge.child_id);
            required.insert(edge.parent_id);
        }
        let ids: Vec<_> = required.iter().map(|id| id.get()).collect();
        let rows = sqlx::query("SELECT id, hash FROM block_identifiers WHERE id = ANY($1::BIGINT[])")
            .bind(&ids)
            .fetch_all(&mut *self.transaction)
            .await
            .map_err(query_error)?;
        let mut references = HashMap::with_capacity(rows.len());
        for row in rows {
            let id = decode_id(row.try_get("id").map_err(inconsistent_decode)?)?;
            let hash = decode_hash(row.try_get("hash").map_err(inconsistent_decode)?)?;
            if references.insert(id, hash).is_some() {
                return Err(ApiReadError::InconsistentProjection);
            }
        }
        if references.len() != required.len() || !required.iter().all(|id| references.contains_key(id)) {
            return Err(ApiReadError::InconsistentProjection);
        }
        Ok(references)
    }

    async fn load_direct_parent_ids(
        &mut self,
        resolution: GraphWindowResolution,
        blocks: &mut HashMap<CompactId, PreparedGraphBlock>,
    ) -> Result<(), ApiReadError> {
        let rows = sqlx::query(
            "SELECT relation.child_id, relation.parent_id
             FROM parents relation
             WHERE relation.child_level BETWEEN $1 AND $2
             ORDER BY relation.child_id, relation.parent_id",
        )
        .bind(level_to_sql(resolution.effective_start_level)?)
        .bind(level_to_sql(resolution.effective_end_level)?)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(query_error)?;

        for row in rows {
            let child_id = decode_id(row.try_get("child_id").map_err(inconsistent_decode)?)?;
            let parent_id = decode_id(row.try_get("parent_id").map_err(inconsistent_decode)?)?;
            let block = blocks.get_mut(&child_id).ok_or(ApiReadError::InconsistentProjection)?;
            if parent_id == child_id {
                return Err(ApiReadError::InconsistentProjection);
            }
            block.direct_parents.push(parent_id);
        }
        Ok(())
    }

    async fn load_edges(&mut self, resolution: GraphWindowResolution) -> Result<Vec<PreparedGraphEdge>, ApiReadError> {
        let rows = sqlx::query(
            "SELECT child_id, parent_id, child_level, child_slot, parent_level, parent_slot
             FROM parents
             WHERE parent_level > 0
               AND child_level >= $1
               AND parent_level <= $2",
        )
        .bind(level_to_sql(resolution.effective_start_level)?)
        .bind(level_to_sql(resolution.effective_end_level)?)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(query_error)?;

        let mut edges = Vec::with_capacity(rows.len());
        for row in rows {
            edges.push(PreparedGraphEdge {
                child_id: decode_id(row.try_get("child_id").map_err(inconsistent_decode)?)?,
                parent_id: decode_id(row.try_get("parent_id").map_err(inconsistent_decode)?)?,
                child_coordinate: decode_coordinate(
                    row.try_get("child_level").map_err(inconsistent_decode)?,
                    row.try_get("child_slot").map_err(inconsistent_decode)?,
                )?,
                parent_coordinate: decode_coordinate(
                    row.try_get("parent_level").map_err(inconsistent_decode)?,
                    row.try_get("parent_slot").map_err(inconsistent_decode)?,
                )?,
            });
        }
        Ok(edges)
    }

    async fn load_levels(
        &mut self,
        resolution: GraphWindowResolution,
        edges: &HashMap<EdgeId, GraphEdge>,
    ) -> Result<BTreeMap<u64, Level>, ApiReadError> {
        let mut required: HashSet<u64> = (resolution.effective_start_level..=resolution.effective_end_level).collect();
        for edge in edges.values() {
            required.insert(edge.child_coordinate.level());
            required.insert(edge.parent_coordinate.level());
        }
        let raw_levels: Vec<_> = required.iter().map(|level| level_to_sql(*level)).collect::<Result<_, _>>()?;
        let rows = sqlx::query("SELECT level, size, daa_score FROM levels WHERE level = ANY($1::BIGINT[]) ORDER BY level")
            .bind(&raw_levels)
            .fetch_all(&mut *self.transaction)
            .await
            .map_err(query_error)?;
        let mut levels = BTreeMap::new();
        for row in rows {
            let level = decode_positive_level(row.try_get("level").map_err(inconsistent_decode)?)?;
            let size = u64::try_from(row.try_get::<i64, _>("size").map_err(inconsistent_decode)?)
                .ok()
                .filter(|size| *size > 0)
                .ok_or(ApiReadError::InconsistentProjection)?;
            let raw_daa: i64 = row.try_get("daa_score").map_err(inconsistent_decode)?;
            let daa_score = if raw_daa == NO_VSPC_DAA_SCORE { None } else { Some(decode_daa_score(raw_daa)?) };
            let _ = levels.insert(level, Level { size, daa_score });
        }
        if levels.len() != required.len() {
            return Err(ApiReadError::InconsistentProjection);
        }
        Ok(levels)
    }

    async fn load_snapshot_metadata(&mut self) -> Result<(CompactId, BlockHash), ApiReadError> {
        let max_id = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(id) FROM blocks")
            .fetch_one(&mut *self.transaction)
            .await
            .map_err(query_error)?
            .ok_or(ApiReadError::InconsistentProjection)
            .and_then(decode_id)?;
        let sink_hash = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT identity.hash
             FROM blocks block
             JOIN block_identifiers identity ON identity.id = block.id
             WHERE block.is_in_vspc
             ORDER BY block.id DESC
             LIMIT 1",
        )
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(query_error)?
        .ok_or(ApiReadError::InconsistentProjection)
        .and_then(decode_hash)?;
        Ok((max_id, sink_hash))
    }

    async fn database_head(&mut self) -> Result<Option<u64>, ApiReadError> {
        let level = sqlx::query_scalar::<_, i64>("SELECT level FROM blocks ORDER BY level DESC, slot DESC LIMIT 1")
            .fetch_optional(&mut *self.transaction)
            .await
            .map_err(query_error)?;
        level.map(decode_positive_level).transpose()
    }

    async fn resolve_anchor(&mut self, anchor: GraphWindowAnchor) -> Result<Option<u64>, ApiReadError> {
        let level = match anchor {
            GraphWindowAnchor::Level(requested_level) => {
                assert!(requested_level > 0, "level anchors must be validated by the API owner");
                let Ok(requested_level) = i64::try_from(requested_level) else {
                    return Ok(None);
                };
                sqlx::query_scalar::<_, i64>("SELECT level FROM levels WHERE level = $1")
                    .bind(requested_level)
                    .fetch_optional(&mut *self.transaction)
                    .await
                    .map_err(query_error)?
            }
            GraphWindowAnchor::BlockHash(requested_hash) => sqlx::query_scalar::<_, i64>(
                "SELECT block.level
                     FROM block_identifiers identity
                     JOIN blocks block ON block.id = identity.id
                     WHERE identity.hash = $1",
            )
            .bind(requested_hash.as_bytes().as_slice())
            .fetch_optional(&mut *self.transaction)
            .await
            .map_err(query_error)?,
            GraphWindowAnchor::DaaScore(requested_score) => {
                assert!(requested_score <= MAX_DAA_SCORE, "DAA-score anchors must be validated by the API owner");
                let requested_score = i64::try_from(requested_score).expect("validated DAA score fits BIGINT");
                sqlx::query_scalar::<_, i64>(
                    "SELECT level
                     FROM levels
                     WHERE daa_score <= $1
                     ORDER BY daa_score DESC, level DESC
                     LIMIT 1",
                )
                .bind(requested_score)
                .fetch_optional(&mut *self.transaction)
                .await
                .map_err(query_error)?
            }
        };
        level.map(decode_positive_level).transpose()
    }
}

fn finalize_blocks(
    prepared: HashMap<CompactId, PreparedGraphBlock>,
    references: &HashMap<CompactId, BlockHash>,
    genesis_hash: BlockHash,
) -> Result<HashMap<BlockHash, GraphBlock>, ApiReadError> {
    let mut blocks = HashMap::with_capacity(prepared.len());
    for (_, prepared) in prepared {
        let hash = references[&prepared.id];
        let selected_parent_index = if hash == genesis_hash {
            None
        } else {
            let index = prepared
                .direct_parents
                .iter()
                .position(|id| *id == prepared.selected_parent_id)
                .ok_or(ApiReadError::InconsistentProjection)?;
            Some(u32::try_from(index).map_err(|_| ApiReadError::InconsistentProjection)?)
        };

        let blue_merge_set = resolve_references(&prepared.blue_merge_set, references);
        let red_merge_set = resolve_references(&prepared.red_merge_set, references);
        let direct_parents = resolve_references(&prepared.direct_parents, references);
        let block = GraphBlock {
            hash,
            coordinate: prepared.coordinate,
            timestamp: prepared.timestamp,
            daa_score: prepared.daa_score,
            selected_parent_index,
            direct_parents,
            blue_merge_set,
            red_merge_set,
            color: prepared.color,
            is_in_vspc: prepared.is_in_vspc,
        };
        let _ = blocks.insert(block.hash, block);
    }
    Ok(blocks)
}

fn finalize_edges(prepared: Vec<PreparedGraphEdge>, references: &HashMap<CompactId, BlockHash>) -> HashMap<EdgeId, GraphEdge> {
    prepared
        .into_iter()
        .map(|edge| {
            let id = EdgeId { parent: references[&edge.parent_id], child: references[&edge.child_id] };
            (id, GraphEdge { id, parent_coordinate: edge.parent_coordinate, child_coordinate: edge.child_coordinate })
        })
        .collect()
}

fn resolve_references(ids: &[CompactId], references: &HashMap<CompactId, BlockHash>) -> Arc<[BlockHash]> {
    ids.iter().map(|id| references[id]).collect::<Vec<_>>().into()
}

fn decode_hash(bytes: Vec<u8>) -> Result<BlockHash, ApiReadError> {
    BlockHash::try_from(bytes.as_slice()).map_err(|_| ApiReadError::InconsistentProjection)
}

fn decode_id(id: i64) -> Result<CompactId, ApiReadError> {
    CompactId::new(id).ok_or(ApiReadError::InconsistentProjection)
}

fn decode_ids(ids: Vec<i64>) -> Result<Vec<CompactId>, ApiReadError> {
    ids.into_iter().map(decode_id).collect()
}

fn decode_coordinate(level: i64, slot: i64) -> Result<BlockCoordinate, ApiReadError> {
    let level = decode_positive_level(level)?;
    let slot = u64::try_from(slot).map_err(|_| ApiReadError::InconsistentProjection)?;
    BlockCoordinate::new(level, slot).ok_or(ApiReadError::InconsistentProjection)
}

fn decode_daa_score(score: i64) -> Result<u64, ApiReadError> {
    u64::try_from(score).ok().filter(|score| *score <= MAX_DAA_SCORE).ok_or(ApiReadError::InconsistentProjection)
}

fn decode_color(color: i16) -> Result<BlockColor, ApiReadError> {
    match color {
        0 => Ok(BlockColor::Gray),
        1 => Ok(BlockColor::Blue),
        2 => Ok(BlockColor::Red),
        _ => Err(ApiReadError::InconsistentProjection),
    }
}

fn level_to_sql(level: u64) -> Result<i64, ApiReadError> {
    i64::try_from(level).map_err(|_| ApiReadError::InconsistentProjection)
}

fn inconsistent_decode(_: sqlx::Error) -> ApiReadError {
    ApiReadError::InconsistentProjection
}

fn head_scope(high_level: u64) -> ProjectionScope {
    let low_level = high_level.saturating_sub(MAX_CACHE_DEPTH - 1).max(1);
    ProjectionScope {
        resolution: GraphWindowResolution {
            resolved_level: high_level,
            effective_start_level: low_level,
            effective_end_level: high_level,
        },
        max_depth: MAX_CACHE_DEPTH,
    }
}

fn anchored_resolution(resolved_level: u64, database_head: u64, max_depth: u64) -> GraphWindowResolution {
    let levels_above = (max_depth - 1) / 2;
    let levels_below = (max_depth - 1) - levels_above;
    let mut low_level = resolved_level.saturating_sub(levels_below).max(1);
    let mut high_level = resolved_level.saturating_add(levels_above).min(database_head);

    let initial_depth = high_level - low_level + 1;
    let unused_depth = max_depth - initial_depth;
    high_level = high_level.saturating_add(unused_depth).min(database_head);

    let shifted_depth = high_level - low_level + 1;
    let remaining_depth = max_depth - shifted_depth;
    low_level = low_level.saturating_sub(remaining_depth).max(1);

    GraphWindowResolution { resolved_level, effective_start_level: low_level, effective_end_level: high_level }
}

const fn anchor_unavailable(anchor: GraphWindowAnchor) -> GraphWindowAnchorUnavailable {
    match anchor {
        GraphWindowAnchor::Level(requested_level) => GraphWindowAnchorUnavailable::LevelNotRetained { requested_level },
        GraphWindowAnchor::BlockHash(requested_hash) => GraphWindowAnchorUnavailable::BlockNotMaterialized { requested_hash },
        GraphWindowAnchor::DaaScore(requested_score) => GraphWindowAnchorUnavailable::NoRetainedDaaMatch { requested_score },
    }
}

fn decode_positive_level(level: i64) -> Result<u64, ApiReadError> {
    u64::try_from(level).ok().filter(|level| *level > 0).ok_or(ApiReadError::InconsistentProjection)
}

fn query_error(error: sqlx::Error) -> ApiReadError {
    if sqlx_connection_lost(&error) { ApiReadError::GenerationLost } else { ApiReadError::QueryFailed }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{GraphProjection, ProjectionScope, ProjectionScopeOutcome, anchored_resolution, head_scope};
    use kaspa_consensus_core::{
        blockhash::ORIGIN,
        network::{NetworkId, NetworkType},
    };
    use kgi_api_model::{
        graph::{EdgeId, MAX_CACHE_DEPTH},
        projection::{
            GraphViewSeedOutcome, GraphViewSeedRequest, GraphWindowAnchor, GraphWindowAnchorUnavailable, GraphWindowResolution,
            MAX_WINDOW_DEPTH,
        },
    };
    use kgi_model::block::{BlockColor, BlockHash, CompactId};
    use sqlx::{PgPool, postgres::PgPoolOptions};
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    };

    const POSTGRES_PORT: u16 = 5432;

    async fn fixture() -> (ContainerAsync<Postgres>, PgPool) {
        let container = Postgres::default().with_tag("17-alpine").start().await.expect("PostgreSQL fixture must start");
        let host = container.get_host().await.expect("fixture host must resolve");
        let port = container.get_host_port_ipv4(POSTGRES_PORT).await.expect("fixture PostgreSQL port must resolve");
        let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
        let pool = PgPoolOptions::new().max_connections(4).connect(&database_url).await.expect("fixture pool must connect");
        crate::migration::MIGRATOR.run(&pool).await.expect("fixture schema migration");
        (container, pool)
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    async fn insert_identity(pool: &PgPool, hash: BlockHash) -> i64 {
        sqlx::query_scalar("INSERT INTO block_identifiers (hash) VALUES ($1) RETURNING id")
            .bind(hash.as_bytes().as_slice())
            .fetch_one(pool)
            .await
            .expect("identity insertion")
    }

    async fn insert_block(pool: &PgPool, hash: BlockHash, selected_parent_id: i64, level: i64, daa_score: Option<i64>) -> i64 {
        let id = insert_identity(pool, hash).await;
        sqlx::query("INSERT INTO levels (level, size, daa_score) VALUES ($1, 1, $2)")
            .bind(level)
            .bind(daa_score.unwrap_or(crate::database::NO_VSPC_DAA_SCORE))
            .execute(pool)
            .await
            .expect("level insertion");
        sqlx::query(
            "INSERT INTO blocks (
                 id, timestamp, daa_score, level, slot, selected_parent_id,
                 color, is_in_vspc, blue_merge_set, red_merge_set
             ) VALUES ($1, 0, $2, $3, 0, $4, 0, $5, '{}', '{}')",
        )
        .bind(id)
        .bind(level * 10)
        .bind(level)
        .bind(selected_parent_id)
        .bind(daa_score.is_some())
        .execute(pool)
        .await
        .expect("block insertion");
        id
    }

    async fn insert_network_binding(pool: &PgPool, genesis_hash: BlockHash) {
        sqlx::query("INSERT INTO network_metadata (network_id, genesis_hash) VALUES ('mainnet', $1)")
            .bind(genesis_hash.as_bytes().as_slice())
            .execute(pool)
            .await
            .expect("network binding insertion");
    }

    async fn insert_parent(pool: &PgPool, child_id: i64, child_level: i64, parent_id: i64, parent_level: i64) {
        sqlx::query(
            "INSERT INTO parents (child_id, parent_id, child_level, child_slot, parent_level, parent_slot)
             VALUES ($1, $2, $3, 0, $4, 0)",
        )
        .bind(child_id)
        .bind(parent_id)
        .bind(child_level)
        .bind(parent_level)
        .execute(pool)
        .await
        .expect("parent insertion");
    }

    async fn seed_scope_fixture(pool: &PgPool) -> (HashMap<u64, BlockHash>, i64) {
        let origin_id = insert_identity(pool, hash(0)).await;
        let mut hashes = HashMap::new();
        let mut selected_parent_id = origin_id;
        insert_network_binding(pool, hash(1)).await;
        for (level, daa_score) in [(1, Some(10)), (2, None), (3, Some(30)), (4, Some(30)), (5, Some(50))] {
            let block_hash = hash(u8::try_from(level).expect("test level fits u8"));
            selected_parent_id = insert_block(pool, block_hash, selected_parent_id, level, daa_score).await;
            hashes.insert(u64::try_from(level).expect("positive test level"), block_hash);
        }
        insert_identity(pool, hash(99)).await;
        (hashes, selected_parent_id)
    }

    async fn seed_complete_projection_fixture(pool: &PgPool) -> [(BlockHash, i64); 3] {
        let origin_id = insert_identity(pool, ORIGIN).await;
        insert_network_binding(pool, hash(1)).await;
        let first_id = insert_block(pool, hash(1), origin_id, 1, Some(10)).await;
        let second_id = insert_block(pool, hash(2), first_id, 2, Some(20)).await;
        let third_id = insert_block(pool, hash(3), second_id, 3, Some(30)).await;
        insert_parent(pool, second_id, 2, first_id, 1).await;
        insert_parent(pool, third_id, 3, second_id, 2).await;
        sqlx::query("UPDATE blocks SET color = 1, is_in_vspc = TRUE, blue_merge_set = ARRAY[$2] WHERE id = $1")
            .bind(second_id)
            .bind(first_id)
            .execute(pool)
            .await
            .expect("projection state update");
        sqlx::query("UPDATE blocks SET color = 2, is_in_vspc = TRUE, red_merge_set = ARRAY[$2] WHERE id = $1")
            .bind(third_id)
            .bind(first_id)
            .execute(pool)
            .await
            .expect("projection state update");
        [(hash(1), first_id), (hash(2), second_id), (hash(3), third_id)]
    }

    fn scope(resolved_level: u64, database_head: u64, max_depth: u64) -> ProjectionScope {
        ProjectionScope { resolution: anchored_resolution(resolved_level, database_head, max_depth), max_depth }
    }

    #[test]
    fn head_depth_retains_the_latest_cache_extent() {
        let high_level = 1_200;
        let low_level = high_level - MAX_CACHE_DEPTH + 1;
        assert_eq!(
            head_scope(high_level),
            ProjectionScope {
                resolution: GraphWindowResolution {
                    resolved_level: high_level,
                    effective_start_level: low_level,
                    effective_end_level: high_level,
                },
                max_depth: MAX_CACHE_DEPTH,
            }
        );
        assert_eq!(head_scope(400).resolution.effective_start_level, 1);
    }

    #[test]
    fn anchored_extents_allocate_even_depth_to_the_historical_side() {
        assert_eq!(
            scope(10, 20, 5),
            ProjectionScope {
                resolution: GraphWindowResolution { resolved_level: 10, effective_start_level: 8, effective_end_level: 12 },
                max_depth: 5,
            }
        );
        assert_eq!(
            scope(10, 20, 4),
            ProjectionScope {
                resolution: GraphWindowResolution { resolved_level: 10, effective_start_level: 8, effective_end_level: 11 },
                max_depth: 4,
            }
        );
    }

    #[test]
    fn anchored_extents_shift_at_both_database_boundaries() {
        assert_eq!(
            scope(1, 10, 5).resolution,
            GraphWindowResolution { resolved_level: 1, effective_start_level: 1, effective_end_level: 5 }
        );
        assert_eq!(
            scope(2, 10, 5).resolution,
            GraphWindowResolution { resolved_level: 2, effective_start_level: 1, effective_end_level: 5 }
        );
        assert_eq!(
            scope(9, 10, 5).resolution,
            GraphWindowResolution { resolved_level: 9, effective_start_level: 6, effective_end_level: 10 }
        );
        assert_eq!(
            scope(10, 10, 5).resolution,
            GraphWindowResolution { resolved_level: 10, effective_start_level: 6, effective_end_level: 10 }
        );
        assert_eq!(
            scope(2, 3, MAX_WINDOW_DEPTH).resolution,
            GraphWindowResolution { resolved_level: 2, effective_start_level: 1, effective_end_level: 3 }
        );
    }

    #[tokio::test]
    async fn stable_read_only_snapshot_resolves_every_indexed_anchor_and_extent() {
        let (_container, pool) = fixture().await;
        let (hashes, previous_id) = seed_scope_fixture(&pool).await;
        let mut projection = GraphProjection::begin(&pool).await.expect("projection snapshot");

        let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
            .fetch_one(&mut *projection.transaction)
            .await
            .expect("transaction isolation");
        let read_only: String = sqlx::query_scalar("SHOW transaction_read_only")
            .fetch_one(&mut *projection.transaction)
            .await
            .expect("transaction read-only state");
        assert_eq!(isolation, "repeatable read");
        assert_eq!(read_only, "on");

        assert_eq!(
            projection.resolve_scope(GraphViewSeedRequest::HeadPublication).await,
            Ok(ProjectionScopeOutcome::Resolved(head_scope(5)))
        );
        assert_eq!(
            projection.resolve_scope(GraphViewSeedRequest::AnchoredWindow { anchor: GraphWindowAnchor::Level(3), max_depth: 4 }).await,
            Ok(ProjectionScopeOutcome::Resolved(scope(3, 5, 4)))
        );
        assert_eq!(
            projection
                .resolve_scope(GraphViewSeedRequest::AnchoredWindow { anchor: GraphWindowAnchor::BlockHash(hashes[&4]), max_depth: 3 })
                .await,
            Ok(ProjectionScopeOutcome::Resolved(scope(4, 5, 3)))
        );
        assert_eq!(
            projection
                .resolve_scope(GraphViewSeedRequest::AnchoredWindow { anchor: GraphWindowAnchor::DaaScore(30), max_depth: 3 })
                .await,
            Ok(ProjectionScopeOutcome::Resolved(scope(4, 5, 3)))
        );
        assert_eq!(
            projection
                .resolve_scope(GraphViewSeedRequest::AnchoredWindow { anchor: GraphWindowAnchor::DaaScore(100), max_depth: 3 })
                .await,
            Ok(ProjectionScopeOutcome::Resolved(scope(5, 5, 3)))
        );

        for (anchor, unavailable) in [
            (GraphWindowAnchor::Level(6), GraphWindowAnchorUnavailable::LevelNotRetained { requested_level: 6 }),
            (GraphWindowAnchor::BlockHash(hash(99)), GraphWindowAnchorUnavailable::BlockNotMaterialized { requested_hash: hash(99) }),
            (
                GraphWindowAnchor::BlockHash(hash(100)),
                GraphWindowAnchorUnavailable::BlockNotMaterialized { requested_hash: hash(100) },
            ),
            (GraphWindowAnchor::DaaScore(9), GraphWindowAnchorUnavailable::NoRetainedDaaMatch { requested_score: 9 }),
        ] {
            assert_eq!(
                projection.resolve_scope(GraphViewSeedRequest::AnchoredWindow { anchor, max_depth: 3 }).await,
                Ok(ProjectionScopeOutcome::AnchorUnavailable(unavailable))
            );
        }

        let level_six_hash = hash(6);
        insert_block(&pool, level_six_hash, previous_id, 6, Some(60)).await;
        assert_eq!(
            projection.resolve_scope(GraphViewSeedRequest::HeadPublication).await,
            Ok(ProjectionScopeOutcome::Resolved(head_scope(5))),
            "the established snapshot must not observe a later commit"
        );
        projection.commit().await.expect("projection snapshot commit");

        let mut successor = GraphProjection::begin(&pool).await.expect("successor projection snapshot");
        assert_eq!(
            successor.resolve_scope(GraphViewSeedRequest::HeadPublication).await,
            Ok(ProjectionScopeOutcome::Resolved(head_scope(6)))
        );
        assert_eq!(
            successor
                .resolve_scope(GraphViewSeedRequest::AnchoredWindow {
                    anchor: GraphWindowAnchor::BlockHash(level_six_hash),
                    max_depth: 3,
                })
                .await,
            Ok(ProjectionScopeOutcome::Resolved(scope(6, 6, 3)))
        );
        successor.commit().await.expect("successor projection snapshot commit");
    }

    #[tokio::test]
    async fn api_client_loads_complete_projection_and_crossing_edge_context() {
        let (_container, pool) = fixture().await;
        let blocks = seed_complete_projection_fixture(&pool).await;
        let (retirement_tx, mut retirements) = crate::runtime::retirement_channel();
        let client = super::super::ValidatedApiDbClient::new(
            pool,
            super::super::DatabaseBinding::new(NetworkId::new(NetworkType::Mainnet), blocks[0].0),
            retirement_tx,
            crate::replacement::ApiReplacementGate::new(),
        );

        let outcome = client
            .load_graph_view_seed(GraphViewSeedRequest::AnchoredWindow { anchor: GraphWindowAnchor::Level(2), max_depth: 2 })
            .await
            .expect("complete projection");
        let GraphViewSeedOutcome::Loaded(seed) = outcome else {
            panic!("level anchor must resolve");
        };
        assert_eq!(seed.resolution, GraphWindowResolution { resolved_level: 2, effective_start_level: 1, effective_end_level: 2 });
        assert_eq!(seed.blocks.len(), 2);
        let genesis = &seed.blocks[&blocks[0].0];
        assert_eq!(genesis.selected_parent_index, None);
        assert!(genesis.direct_parents.is_empty());
        let second = &seed.blocks[&blocks[1].0];
        assert_eq!(second.selected_parent_index, Some(0));
        assert_eq!(&*second.direct_parents, &[blocks[0].0]);
        assert_eq!(&*second.blue_merge_set, &[blocks[0].0]);
        assert_eq!(second.color, BlockColor::Blue);
        assert!(second.is_in_vspc);

        assert_eq!(seed.edges.len(), 2);
        assert!(seed.edges.contains_key(&EdgeId { parent: blocks[0].0, child: blocks[1].0 }));
        assert!(seed.edges.contains_key(&EdgeId { parent: blocks[1].0, child: blocks[2].0 }));
        assert_eq!(seed.levels.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(seed.snapshot_max_materialized_id, CompactId::new(blocks[2].1).expect("fixture ID is positive"));
        assert_eq!(seed.snapshot_vspc_sink, blocks[2].0);
        assert!(retirements.try_recv().is_err());
    }
}
