use kgi_api_model::{
    graph::MAX_CACHE_DEPTH,
    projection::{GraphViewSeedRequest, GraphWindowAnchor, GraphWindowAnchorUnavailable, GraphWindowResolution, MAX_WINDOW_DEPTH},
};
use kgi_model::block::MAX_DAA_SCORE;
use sqlx::{PgPool, Postgres, Transaction};

use crate::error::{ApiReadError, sqlx_connection_lost};

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
    use kgi_api_model::{
        graph::MAX_CACHE_DEPTH,
        projection::{GraphViewSeedRequest, GraphWindowAnchor, GraphWindowAnchorUnavailable, GraphWindowResolution, MAX_WINDOW_DEPTH},
    };
    use kgi_model::block::BlockHash;
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

    async fn seed_scope_fixture(pool: &PgPool) -> (HashMap<u64, BlockHash>, i64) {
        let origin_id = insert_identity(pool, hash(0)).await;
        let mut hashes = HashMap::new();
        let mut selected_parent_id = origin_id;
        for (level, daa_score) in [(1, Some(10)), (2, None), (3, Some(30)), (4, Some(30)), (5, Some(50))] {
            let block_hash = hash(u8::try_from(level).expect("test level fits u8"));
            selected_parent_id = insert_block(pool, block_hash, selected_parent_id, level, daa_score).await;
            hashes.insert(u64::try_from(level).expect("positive test level"), block_hash);
        }
        insert_identity(pool, hash(99)).await;
        (hashes, selected_parent_id)
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
}
