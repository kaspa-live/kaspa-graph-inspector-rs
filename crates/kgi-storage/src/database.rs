use kaspa_consensus_core::network::NetworkId;
use kgi_model::{
    block::{BlockHash, MAX_BLUE_SCORE, MAX_DAA_SCORE, Timestamp},
    lifecycle::ScoreRangeFault,
};
use sqlx::{Connection, PgConnection};

use crate::{
    error::{StorageError, StorageRejection},
    schema::{self, DatabaseState},
};

const ADVISORY_LOCK_KEY: i64 = 0x4b47_4932_0000_0001;

pub(crate) const fn timestamp_to_sql(timestamp: Timestamp) -> i64 {
    i64::from_ne_bytes(timestamp.to_ne_bytes())
}

pub(crate) const fn timestamp_from_sql(timestamp: i64) -> Timestamp {
    u64::from_ne_bytes(timestamp.to_ne_bytes())
}

pub(crate) fn daa_score_to_sql(score: u64) -> Result<i64, StorageError> {
    if score > MAX_DAA_SCORE {
        return Err(StorageError::ScoreOutOfRange(ScoreRangeFault::DaaScore));
    }
    i64::try_from(score).map_err(|_| StorageError::ScoreOutOfRange(ScoreRangeFault::DaaScore))
}

pub(crate) fn blue_score_to_sql(score: u64) -> Result<i64, StorageError> {
    if score > MAX_BLUE_SCORE {
        return Err(StorageError::ScoreOutOfRange(ScoreRangeFault::BlueScore));
    }
    i64::try_from(score).map_err(|_| StorageError::ScoreOutOfRange(ScoreRangeFault::BlueScore))
}

pub(crate) struct LockedDatabase {
    connection: PgConnection,
}

impl LockedDatabase {
    pub(crate) async fn connect(database_url: &str) -> Result<Self, StorageError> {
        let mut connection = PgConnection::connect(database_url).await.map_err(|error| StorageError::database("connection", error))?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(ADVISORY_LOCK_KEY)
            .fetch_one(&mut connection)
            .await
            .map_err(|error| StorageError::database("advisory-lock acquisition", error))?;
        if !acquired {
            return Err(StorageRejection::DatabaseAlreadyInUse.into());
        }
        Ok(Self { connection })
    }

    pub(crate) async fn prepare(&mut self) -> Result<DatabaseState, StorageError> {
        schema::prepare(&mut self.connection).await
    }

    pub(crate) async fn initialize_if_uninitialized(
        &mut self,
        network_id: NetworkId,
        genesis_hash: BlockHash,
        reinitialization_token: Option<&str>,
    ) -> Result<DatabaseState, StorageError> {
        let state = schema::classify(&mut self.connection).await?;
        if let Some(observed) = state.binding() {
            ensure_binding(observed, network_id, genesis_hash)?;
            return Ok(state);
        }

        let mut transaction =
            self.connection.begin().await.map_err(|error| StorageError::database("initialization transaction start", error))?;
        sqlx::query(
            "INSERT INTO node_metadata (singleton, network_id, genesis_hash, db_pp_blue_score)
             VALUES (TRUE, $1, $2, 0)",
        )
        .bind(network_id.to_string())
        .bind(genesis_hash.as_bytes().as_slice())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::database("node-metadata initialization", error))?;
        if let Some(token) = reinitialization_token {
            sqlx::query("UPDATE administrative_metadata SET last_reinitialization_token = $1 WHERE singleton")
                .bind(token)
                .execute(&mut *transaction)
                .await
                .map_err(|error| StorageError::database("administrative-metadata initialization", error))?;
        }
        transaction.commit().await.map_err(|error| StorageError::database("initialization transaction commit", error))?;

        let initialized = schema::classify(&mut self.connection).await?;
        if !matches!(initialized, DatabaseState::Empty(_)) {
            return Err(StorageError::invalid_metadata("initialization did not produce an Empty database"));
        }
        Ok(initialized)
    }

    #[cfg(test)]
    pub(crate) fn connection_mut(&mut self) -> &mut PgConnection {
        &mut self.connection
    }
}

fn ensure_binding(
    observed: crate::schema::DatabaseBinding,
    expected_network_id: NetworkId,
    expected_genesis_hash: BlockHash,
) -> Result<(), StorageError> {
    if observed.network_id == expected_network_id && observed.genesis_hash == expected_genesis_hash {
        return Ok(());
    }
    Err(StorageRejection::NetworkMismatch {
        expected_network_id,
        expected_genesis_hash,
        observed_network_id: observed.network_id,
        observed_genesis_hash: observed.genesis_hash,
    }
    .into())
}

#[cfg(test)]
mod tests {
    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_model::block::BlockHash;
    use sqlx::{Connection, PgConnection};
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    };

    use super::{LockedDatabase, blue_score_to_sql, daa_score_to_sql, timestamp_from_sql, timestamp_to_sql};
    use crate::{
        error::{StorageError, StorageRejection},
        migration,
        schema::{self, DatabaseState},
    };

    const POSTGRES_PORT: u16 = 5432;

    async fn fixture() -> (ContainerAsync<Postgres>, String) {
        let container = Postgres::default().with_tag("17-alpine").start().await.expect("PostgreSQL fixture must start");
        let host = container.get_host().await.expect("fixture host must resolve");
        let port = container.get_host_port_ipv4(POSTGRES_PORT).await.expect("fixture PostgreSQL port must resolve");
        let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
        (container, database_url)
    }

    fn mainnet() -> NetworkId {
        NetworkId::new(NetworkType::Mainnet)
    }

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_bytes([byte; 32])
    }

    #[tokio::test]
    async fn initialization_is_atomic_idempotent_and_immutably_bound() {
        let (_container, database_url) = fixture().await;
        let mut database = LockedDatabase::connect(&database_url).await.expect("database lock");
        assert_eq!(database.prepare().await.expect("schema preparation"), DatabaseState::Uninitialized);

        let initialized =
            database.initialize_if_uninitialized(mainnet(), hash(1), Some("deployment-a")).await.expect("first initialization");
        let DatabaseState::Empty(metadata) = initialized else {
            panic!("first initialization must produce Empty");
        };
        assert_eq!(metadata.network_id(), mainnet());
        assert_eq!(metadata.genesis_hash(), hash(1));
        assert_eq!(metadata.db_pp_blue_score(), 0);

        let repeated = database
            .initialize_if_uninitialized(mainnet(), hash(1), Some("ignored-after-initialization"))
            .await
            .expect("idempotent initialization");
        assert!(matches!(repeated, DatabaseState::Empty(_)));
        let token: Option<String> = sqlx::query_scalar("SELECT last_reinitialization_token FROM administrative_metadata")
            .fetch_one(database.connection_mut())
            .await
            .expect("administrative token");
        assert_eq!(token.as_deref(), Some("deployment-a"));

        let mismatch = database.initialize_if_uninitialized(mainnet(), hash(2), None).await.expect_err("Genesis rebinding must fail");
        assert!(matches!(
            mismatch,
            StorageError::Rejected(StorageRejection::NetworkMismatch {
                expected_network_id,
                expected_genesis_hash,
                observed_network_id,
                observed_genesis_hash,
            }) if expected_network_id == mainnet()
                && expected_genesis_hash == hash(2)
                && observed_network_id == mainnet()
                && observed_genesis_hash == hash(1)
        ));
    }

    #[tokio::test]
    async fn advisory_lock_excludes_a_second_owner() {
        let (_container, database_url) = fixture().await;
        let _owner = LockedDatabase::connect(&database_url).await.expect("first owner");
        let contender = match LockedDatabase::connect(&database_url).await {
            Ok(_) => panic!("second owner must be rejected"),
            Err(error) => error,
        };
        assert_eq!(contender, StorageError::Rejected(StorageRejection::DatabaseAlreadyInUse));
    }

    #[tokio::test]
    async fn preparatory_schema_and_rolled_back_binding_remain_uninitialized() {
        let (_container, database_url) = fixture().await;
        let mut database = LockedDatabase::connect(&database_url).await.expect("database lock");
        assert_eq!(database.prepare().await.expect("schema preparation"), DatabaseState::Uninitialized);

        let mut transaction = database.connection_mut().begin().await.expect("transaction");
        sqlx::query(
            "INSERT INTO node_metadata (singleton, network_id, genesis_hash, db_pp_blue_score)
             VALUES (TRUE, $1, $2, 0)",
        )
        .bind(mainnet().to_string())
        .bind(hash(3).as_bytes().as_slice())
        .execute(&mut *transaction)
        .await
        .expect("provisional binding");
        transaction.rollback().await.expect("rollback");

        assert_eq!(schema::classify(database.connection_mut()).await.expect("classification"), DatabaseState::Uninitialized);
    }

    #[tokio::test]
    async fn supported_older_schema_migrates_before_classification() {
        let (_container, database_url) = fixture().await;
        let mut connection = PgConnection::connect(&database_url).await.expect("fixture connection");
        migration::MIGRATOR.run_to(1, &mut connection).await.expect("first migration only");
        connection.close().await.expect("close migration connection");

        let mut database = LockedDatabase::connect(&database_url).await.expect("database lock");
        assert_eq!(database.prepare().await.expect("remaining migrations"), DatabaseState::Uninitialized);
        let processing_table_exists: bool = sqlx::query_scalar("SELECT to_regclass('public.blocks') IS NOT NULL")
            .fetch_one(database.connection_mut())
            .await
            .expect("processing table lookup");
        assert!(processing_table_exists);
        let indexes = sqlx::query_scalar::<_, String>(
            "SELECT indexname
             FROM pg_indexes
             WHERE schemaname = current_schema()
               AND indexname IN ('parents_parent_coordinate_idx', 'levels_daa_score_level_idx')
             ORDER BY indexname",
        )
        .fetch_all(database.connection_mut())
        .await
        .expect("query-path index lookup");
        assert_eq!(indexes, ["levels_daa_score_level_idx", "parents_parent_coordinate_idx"]);

        let (_dirty_container, dirty_url) = fixture().await;
        let mut connection = PgConnection::connect(&dirty_url).await.expect("fixture connection");
        migration::MIGRATOR.run_to(1, &mut connection).await.expect("first migration only");
        sqlx::query("UPDATE _sqlx_migrations SET success = FALSE WHERE version = 1")
            .execute(&mut connection)
            .await
            .expect("dirty migration marker");
        connection.close().await.expect("close migration connection");
        let mut dirty = LockedDatabase::connect(&dirty_url).await.expect("database lock");
        assert!(matches!(dirty.prepare().await, Err(StorageError::Migration { .. })));
    }

    #[tokio::test]
    async fn unknown_partial_and_newer_schemas_are_rejected() {
        let (_unknown_container, unknown_url) = fixture().await;
        let mut connection = PgConnection::connect(&unknown_url).await.expect("fixture connection");
        sqlx::query("CREATE TABLE unrelated_user_data (id BIGINT PRIMARY KEY)").execute(&mut connection).await.expect("unknown table");
        connection.close().await.expect("close fixture connection");
        let mut unknown = LockedDatabase::connect(&unknown_url).await.expect("database lock");
        assert!(matches!(unknown.prepare().await, Err(StorageError::Rejected(StorageRejection::UnsupportedSchema { .. }))));

        let (_partial_container, partial_url) = fixture().await;
        let mut connection = PgConnection::connect(&partial_url).await.expect("fixture connection");
        sqlx::query("CREATE TABLE node_metadata (singleton BOOLEAN PRIMARY KEY)")
            .execute(&mut connection)
            .await
            .expect("partial table");
        connection.close().await.expect("close fixture connection");
        let mut partial = LockedDatabase::connect(&partial_url).await.expect("database lock");
        assert!(matches!(partial.prepare().await, Err(StorageError::Rejected(StorageRejection::UnsupportedSchema { .. }))));

        let (_tampered_container, tampered_url) = fixture().await;
        let mut tampered = LockedDatabase::connect(&tampered_url).await.expect("database lock");
        tampered.prepare().await.expect("current schema");
        sqlx::query("ALTER TABLE administrative_metadata DROP COLUMN last_reinitialization_token")
            .execute(tampered.connection_mut())
            .await
            .expect("tamper with current schema");
        assert!(matches!(tampered.prepare().await, Err(StorageError::Rejected(StorageRejection::UnsupportedSchema { .. }))));

        let (_newer_container, newer_url) = fixture().await;
        let mut newer = LockedDatabase::connect(&newer_url).await.expect("database lock");
        newer.prepare().await.expect("current schema");
        let future_version = migration::current_version() + 1;
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
             VALUES ($1, 'future', TRUE, '\\x00', 0)",
        )
        .bind(future_version)
        .execute(newer.connection_mut())
        .await
        .expect("future migration marker");
        assert_eq!(
            newer.prepare().await.expect_err("newer schema must be rejected"),
            StorageError::Rejected(StorageRejection::SchemaTooNew {
                observed: future_version,
                supported: migration::current_version(),
            })
        );
    }

    #[tokio::test]
    async fn processing_contents_distinguish_initialized_and_inconsistent() {
        let (_container, database_url) = fixture().await;
        let mut database = LockedDatabase::connect(&database_url).await.expect("database lock");
        database.prepare().await.expect("schema preparation");
        database.initialize_if_uninitialized(mainnet(), hash(9), None).await.expect("initialization");

        sqlx::query("INSERT INTO block_identifiers (id, hash) VALUES (1, $1), (2, $2)")
            .bind(hash(8).as_bytes().as_slice())
            .bind(hash(9).as_bytes().as_slice())
            .execute(database.connection_mut())
            .await
            .expect("ORIGIN and Genesis identities");
        sqlx::query("INSERT INTO levels (level, size, daa_score) VALUES (1, 1, 0)")
            .execute(database.connection_mut())
            .await
            .expect("Genesis level");
        sqlx::query(
            "INSERT INTO blocks (
                id, timestamp, daa_score, level, slot, selected_parent_id,
                color, is_in_vspc, blue_merge_set, red_merge_set
             ) VALUES (2, 0, 0, 1, 0, 1, 1, TRUE, ARRAY[]::BIGINT[], ARRAY[]::BIGINT[])",
        )
        .execute(database.connection_mut())
        .await
        .expect("Genesis block");
        assert!(matches!(
            schema::classify(database.connection_mut()).await.expect("Genesis classification"),
            DatabaseState::Initialized(_)
        ));

        sqlx::query("INSERT INTO block_identifiers (id, hash) VALUES (3, $1)")
            .bind(hash(7).as_bytes().as_slice())
            .execute(database.connection_mut())
            .await
            .expect("extra boundary identity");
        assert!(matches!(
            schema::classify(database.connection_mut()).await.expect("inconsistent classification"),
            DatabaseState::Inconsistent { .. }
        ));

        sqlx::query("ALTER TABLE node_metadata DROP CONSTRAINT node_metadata_db_pp_blue_score_check")
            .execute(database.connection_mut())
            .await
            .expect("permit corrupted score fixture");
        sqlx::query("UPDATE node_metadata SET db_pp_blue_score = -1")
            .execute(database.connection_mut())
            .await
            .expect("corrupt pruning-point score");
        let negative = schema::classify(database.connection_mut()).await.expect("negative score classification");
        assert!(matches!(
            negative,
            DatabaseState::Inconsistent {
                binding,
                metadata: None,
            } if binding.network_id == mainnet() && binding.genesis_hash == hash(9)
        ));
    }

    #[tokio::test]
    async fn score_constraints_and_timestamp_bit_patterns_are_exact() {
        let (_container, database_url) = fixture().await;
        let mut database = LockedDatabase::connect(&database_url).await.expect("database lock");
        database.prepare().await.expect("schema preparation");
        database.initialize_if_uninitialized(mainnet(), hash(5), None).await.expect("initialization");
        sqlx::query("INSERT INTO block_identifiers (id, hash) VALUES (1, $1), (2, $2)")
            .bind(hash(4).as_bytes().as_slice())
            .bind(hash(5).as_bytes().as_slice())
            .execute(database.connection_mut())
            .await
            .expect("identities");
        sqlx::query("INSERT INTO levels (level, size) VALUES (1, 1)").execute(database.connection_mut()).await.expect("level");
        sqlx::query(
            "INSERT INTO blocks (
                id, timestamp, daa_score, level, slot, selected_parent_id,
                color, is_in_vspc, blue_merge_set, red_merge_set
             ) VALUES (2, 0, 0, 1, 0, 1, 1, TRUE, ARRAY[]::BIGINT[], ARRAY[]::BIGINT[])",
        )
        .execute(database.connection_mut())
        .await
        .expect("block");

        for timestamp in [0, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            sqlx::query("UPDATE blocks SET timestamp = $1 WHERE id = 2")
                .bind(timestamp_to_sql(timestamp))
                .execute(database.connection_mut())
                .await
                .expect("timestamp write");
            let stored: i64 = sqlx::query_scalar("SELECT timestamp FROM blocks WHERE id = 2")
                .fetch_one(database.connection_mut())
                .await
                .expect("timestamp read");
            assert_eq!(timestamp_from_sql(stored), timestamp);
        }

        assert_eq!(daa_score_to_sql(0), Ok(0));
        assert_eq!(daa_score_to_sql(kgi_model::block::MAX_DAA_SCORE), Ok(i64::MAX - 1));
        assert_eq!(
            daa_score_to_sql(kgi_model::block::MAX_DAA_SCORE + 1),
            Err(StorageError::ScoreOutOfRange(kgi_model::lifecycle::ScoreRangeFault::DaaScore))
        );
        assert_eq!(blue_score_to_sql(kgi_model::block::MAX_BLUE_SCORE), Ok(i64::MAX));
        assert_eq!(
            blue_score_to_sql(kgi_model::block::MAX_BLUE_SCORE + 1),
            Err(StorageError::ScoreOutOfRange(kgi_model::lifecycle::ScoreRangeFault::BlueScore))
        );

        let invalid_daa =
            sqlx::query("UPDATE blocks SET daa_score = $1 WHERE id = 2").bind(i64::MAX).execute(database.connection_mut()).await;
        assert!(invalid_daa.is_err());
    }
}
