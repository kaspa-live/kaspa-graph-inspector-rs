use std::sync::Arc;

use async_trait::async_trait;
use kaspa_consensus_core::network::NetworkId;
use kgi_model::{
    block::{BlockHash, MAX_BLUE_SCORE, MAX_DAA_SCORE, Timestamp},
    lifecycle::ScoreRangeFault,
};
use sqlx::{Connection, PgConnection, Postgres, Transaction, postgres::PgPoolOptions};

use crate::{
    error::{StorageError, StorageRejection},
    generation::{DatabaseBinding, ValidatedApiDbClient, ValidatedDbClient},
    runtime::RetirementSender,
    schema,
    state::{DatabaseState, ProcessingStateInspection},
};

const ADVISORY_LOCK_KEY: i64 = 0x4b47_4932_0000_0001;
const PROCESSING_POOL_SIZE: u32 = 4;
const API_POOL_SIZE: u32 = 8;

pub(crate) struct ValidatedGenerations {
    processing: Arc<ValidatedDbClient>,
    api: Option<Arc<ValidatedApiDbClient>>,
}

impl ValidatedGenerations {
    pub(crate) fn into_parts(self) -> (Arc<ValidatedDbClient>, Option<Arc<ValidatedApiDbClient>>) {
        (self.processing, self.api)
    }
}

#[allow(dead_code, reason = "used by persistence transactions in the next increment")]
pub(crate) const fn timestamp_to_sql(timestamp: Timestamp) -> i64 {
    i64::from_ne_bytes(timestamp.to_ne_bytes())
}

#[allow(dead_code, reason = "used by persistence transactions in the next increment")]
pub(crate) const fn timestamp_from_sql(timestamp: i64) -> Timestamp {
    u64::from_ne_bytes(timestamp.to_ne_bytes())
}

#[allow(dead_code, reason = "used by persistence transactions in the next increment")]
pub(crate) fn daa_score_to_sql(score: u64) -> Result<i64, StorageError> {
    if score > MAX_DAA_SCORE {
        return Err(StorageError::ScoreOutOfRange(ScoreRangeFault::DaaScore));
    }
    i64::try_from(score).map_err(|_| StorageError::ScoreOutOfRange(ScoreRangeFault::DaaScore))
}

#[allow(dead_code, reason = "used by persistence transactions in the next increment")]
pub(crate) fn blue_score_to_sql(score: u64) -> Result<i64, StorageError> {
    if score > MAX_BLUE_SCORE {
        return Err(StorageError::ScoreOutOfRange(ScoreRangeFault::BlueScore));
    }
    i64::try_from(score).map_err(|_| StorageError::ScoreOutOfRange(ScoreRangeFault::BlueScore))
}

pub(crate) struct LockedDatabase {
    connection: PgConnection,
}

pub(crate) struct PreparedDatabase {
    connection: PgConnection,
}

#[derive(Clone, Copy)]
enum InitializationCommitFault {
    None,
    #[cfg(test)]
    LoseAcknowledgementAfterCommit,
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

    pub(crate) async fn prepare(mut self) -> Result<(PreparedDatabase, DatabaseState), StorageError> {
        schema::prepare(&mut self.connection).await?;
        let state = ProcessingStateInspection::classify(&mut self.connection).await?;
        Ok((PreparedDatabase { connection: self.connection }, state))
    }
}

#[async_trait]
pub(crate) trait DatabaseConnector: Send + Sync {
    async fn connect(&self, database_url: &str) -> Result<LockedDatabase, StorageError>;
}

pub(crate) struct SqlxDatabaseConnector;

#[async_trait]
impl DatabaseConnector for SqlxDatabaseConnector {
    async fn connect(&self, database_url: &str) -> Result<LockedDatabase, StorageError> {
        LockedDatabase::connect(database_url).await
    }
}

impl PreparedDatabase {
    pub(crate) async fn initialize_if_uninitialized(
        &mut self,
        network_id: NetworkId,
        genesis_hash: BlockHash,
        reinitialization_token: Option<String>,
    ) -> Result<DatabaseState, StorageError> {
        self.initialize_if_uninitialized_with_commit_fault(
            network_id,
            genesis_hash,
            reinitialization_token,
            InitializationCommitFault::None,
        )
        .await
    }

    async fn initialize_if_uninitialized_with_commit_fault(
        &mut self,
        network_id: NetworkId,
        genesis_hash: BlockHash,
        reinitialization_token: Option<String>,
        commit_fault: InitializationCommitFault,
    ) -> Result<DatabaseState, StorageError> {
        let state = self.classify().await?;
        if let Some(observed) = state.binding() {
            ensure_binding(observed, network_id, genesis_hash)?;
            return Ok(state);
        }

        let mut transaction =
            self.connection.begin().await.map_err(|error| StorageError::database("initialization transaction start", error))?;
        sqlx::query(
            "INSERT INTO network_metadata (singleton, network_id, genesis_hash)
                 VALUES (TRUE, $1, $2)",
        )
        .bind(network_id.to_string())
        .bind(genesis_hash.as_bytes().as_slice())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::database("network-metadata initialization", error))?;
        if let Some(token) = reinitialization_token {
            sqlx::query("UPDATE administrative_metadata SET last_reinitialization_token = $1 WHERE singleton")
                .bind(token)
                .execute(&mut *transaction)
                .await
                .map_err(|error| StorageError::database("administrative-metadata initialization", error))?;
        }
        commit_initialization(transaction, commit_fault)
            .await
            .map_err(|error| StorageError::mutation_commit("initialization transaction commit", error))?;

        let initialized = self.classify().await?;
        if !matches!(initialized, DatabaseState::Empty(_)) {
            return Err(StorageError::invalid_metadata("initialization did not produce an Empty database"));
        }
        Ok(initialized)
    }

    pub(crate) async fn classify(&mut self) -> Result<DatabaseState, StorageError> {
        ProcessingStateInspection::classify(&mut self.connection).await
    }

    pub(crate) async fn ping(&mut self) -> Result<(), StorageError> {
        self.connection.ping().await.map_err(|error| StorageError::database("advisory-lock health check", error))
    }

    #[cfg(test)]
    pub(crate) fn connection_mut(&mut self) -> &mut PgConnection {
        &mut self.connection
    }
}

async fn commit_initialization(
    transaction: Transaction<'_, Postgres>,
    commit_fault: InitializationCommitFault,
) -> Result<(), sqlx::Error> {
    transaction.commit().await?;
    #[cfg(test)]
    if matches!(commit_fault, InitializationCommitFault::LoseAcknowledgementAfterCommit) {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "injected initialization COMMIT acknowledgement loss",
        )));
    }
    let _ = commit_fault;
    Ok(())
}

pub(crate) async fn open_validated_generations(
    database_url: String,
    state: DatabaseState,
    retirement_tx: RetirementSender,
) -> Result<Option<ValidatedGenerations>, StorageError> {
    let (binding, coherent) = match state {
        DatabaseState::Uninitialized => return Ok(None),
        DatabaseState::Empty(binding) | DatabaseState::Initialized(binding) => (binding, true),
        DatabaseState::Inconsistent { binding } => (binding, false),
    };

    let processing = open_processing_generation(database_url.clone(), binding, retirement_tx.clone()).await?;

    let api = if coherent {
        match open_api_generation(database_url, retirement_tx).await {
            Ok(api) => Some(api),
            Err(error) => {
                processing.retire();
                processing.close().await;
                return Err(error);
            }
        }
    } else {
        None
    };

    Ok(Some(ValidatedGenerations { processing, api }))
}

pub(crate) async fn open_processing_generation(
    database_url: String,
    binding: DatabaseBinding,
    retirement_tx: RetirementSender,
) -> Result<Arc<ValidatedDbClient>, StorageError> {
    let pool = PgPoolOptions::new()
        .max_connections(PROCESSING_POOL_SIZE)
        .connect(&database_url)
        .await
        .map_err(|error| StorageError::database("processing-pool connection", error))?;
    if let Err(error) = validate_processing_pool(&pool).await {
        pool.close().await;
        return Err(error);
    }
    Ok(ValidatedDbClient::new(pool, binding, retirement_tx))
}

pub(crate) async fn open_api_generation(
    database_url: String,
    retirement_tx: RetirementSender,
) -> Result<Arc<ValidatedApiDbClient>, StorageError> {
    let pool = PgPoolOptions::new()
        .max_connections(API_POOL_SIZE)
        .after_connect(|connection, _metadata| {
            Box::pin(async move {
                sqlx::query("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY").execute(connection).await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await
        .map_err(|error| StorageError::database("API-pool connection", error))?;
    if let Err(error) = validate_api_pool(&pool).await {
        pool.close().await;
        return Err(error);
    }
    Ok(ValidatedApiDbClient::new(pool, retirement_tx))
}

async fn validate_processing_pool(pool: &sqlx::PgPool) -> Result<(), StorageError> {
    validate_pool_access(pool, "processing-pool validation", "off").await
}

async fn validate_api_pool(pool: &sqlx::PgPool) -> Result<(), StorageError> {
    validate_pool_access(pool, "API-pool validation", "on").await
}

async fn validate_pool_access(pool: &sqlx::PgPool, operation: &'static str, expected: &str) -> Result<(), StorageError> {
    let read_only: String = sqlx::query_scalar("SHOW default_transaction_read_only")
        .fetch_one(pool)
        .await
        .map_err(|error| StorageError::database(operation, error))?;
    if read_only == expected {
        Ok(())
    } else {
        Err(StorageError::Database {
            operation,
            diagnostic: Arc::from(format!("expected default_transaction_read_only={expected}, observed {read_only}")),
        })
    }
}

fn ensure_binding(
    observed: DatabaseBinding,
    expected_network_id: NetworkId,
    expected_genesis_hash: BlockHash,
) -> Result<(), StorageError> {
    if observed.network_id() == expected_network_id && observed.genesis_hash() == expected_genesis_hash {
        return Ok(());
    }
    Err(StorageRejection::NetworkMismatch {
        expected_network_id,
        expected_genesis_hash,
        observed_network_id: observed.network_id(),
        observed_genesis_hash: observed.genesis_hash(),
    }
    .into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_model::{block::BlockHash, lifecycle::PersistenceFault};
    use sqlx::{Connection, PgConnection};
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    };

    use super::{
        API_POOL_SIZE, InitializationCommitFault, LockedDatabase, PROCESSING_POOL_SIZE, PreparedDatabase, blue_score_to_sql,
        daa_score_to_sql, open_validated_generations, timestamp_from_sql, timestamp_to_sql,
    };
    use crate::{
        error::{StorageError, StorageRejection},
        generation::StoredSessionState,
        migration,
        runtime::retirement_channel,
        state::DatabaseState,
    };

    const POSTGRES_PORT: u16 = 5432;

    async fn fixture() -> (ContainerAsync<Postgres>, String) {
        let container = Postgres::default().with_tag("17-alpine").start().await.expect("PostgreSQL fixture must start");
        let host = container.get_host().await.expect("fixture host must resolve");
        let port = container.get_host_port_ipv4(POSTGRES_PORT).await.expect("fixture PostgreSQL port must resolve");
        let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
        (container, database_url)
    }

    async fn prepared_database(database_url: &str) -> (PreparedDatabase, DatabaseState) {
        LockedDatabase::connect(database_url).await.expect("database lock").prepare().await.expect("schema preparation")
    }

    async fn assert_schema_tamper_rejected(database_url: &str, statement: &'static str) {
        let (mut database, _) = prepared_database(database_url).await;
        sqlx::query(statement).execute(database.connection_mut()).await.expect("schema tamper");
        drop(database);

        let locked = LockedDatabase::connect(database_url).await.expect("database lock after tamper");
        assert!(matches!(locked.prepare().await, Err(StorageError::Rejected(StorageRejection::UnsupportedSchema { .. }))));
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
        let (mut database, state) = prepared_database(&database_url).await;
        assert_eq!(state, DatabaseState::Uninitialized);

        let initialized = database
            .initialize_if_uninitialized(mainnet(), hash(1), Some("deployment-a".to_owned()))
            .await
            .expect("first initialization");
        let DatabaseState::Empty(binding) = initialized else {
            panic!("first initialization must produce Empty");
        };
        assert_eq!(binding.network_id(), mainnet());
        assert_eq!(binding.genesis_hash(), hash(1));
        let processing_metadata_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM processing_metadata")
            .fetch_one(database.connection_mut())
            .await
            .expect("processing-metadata count");
        assert_eq!(processing_metadata_rows, 0);

        let repeated = database
            .initialize_if_uninitialized(mainnet(), hash(1), Some("ignored-after-initialization".to_owned()))
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
    async fn initialization_commit_acknowledgement_loss_is_ambiguous_and_reconciled_from_database_truth() {
        let (_container, database_url) = fixture().await;
        let (mut database, state) = prepared_database(&database_url).await;
        assert_eq!(state, DatabaseState::Uninitialized);

        let error = database
            .initialize_if_uninitialized_with_commit_fault(
                mainnet(),
                hash(3),
                Some("ambiguous-deployment".to_owned()),
                InitializationCommitFault::LoseAcknowledgementAfterCommit,
            )
            .await
            .expect_err("lost COMMIT acknowledgement must not report a definite outcome");
        assert_eq!(error, StorageError::Persistence(PersistenceFault::AmbiguousCommit));

        drop(database);
        let (mut reconnected, observed) = prepared_database(&database_url).await;
        let DatabaseState::Empty(binding) = observed else {
            panic!("reconnection must classify the committed database truth as Empty");
        };
        assert_eq!(binding.network_id(), mainnet());
        assert_eq!(binding.genesis_hash(), hash(3));
        let token: Option<String> = sqlx::query_scalar("SELECT last_reinitialization_token FROM administrative_metadata")
            .fetch_one(reconnected.connection_mut())
            .await
            .expect("administrative token after reconnect");
        assert_eq!(token.as_deref(), Some("ambiguous-deployment"));
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
    async fn coherent_state_opens_independent_capped_pool_generations() {
        let (_container, database_url) = fixture().await;
        let (mut database, uninitialized) = prepared_database(&database_url).await;
        let (retirement_tx, _retirement_rx) = retirement_channel();
        assert!(
            open_validated_generations(database_url.clone(), uninitialized, retirement_tx.clone())
                .await
                .expect("uninitialized capability classification")
                .is_none()
        );
        let state = database.initialize_if_uninitialized(mainnet(), hash(6), None).await.expect("initialization");

        let generations = open_validated_generations(database_url.clone(), state, retirement_tx)
            .await
            .expect("validated generation creation")
            .expect("initialized database must publish generations");
        let (processing, api) = generations.into_parts();
        let api = api.expect("coherent database must publish an API generation");

        assert_eq!(processing.binding().network_id(), mainnet());
        assert_eq!(processing.binding().genesis_hash(), hash(6));
        assert_eq!(processing.load_session_state().await.expect("empty session state"), StoredSessionState::Empty);
        assert_eq!(processing.pool().options().get_max_connections(), PROCESSING_POOL_SIZE);
        assert_eq!(api.pool().options().get_max_connections(), API_POOL_SIZE);
        assert!(api.is_valid());

        let mut processing_connection = processing.pool().acquire().await.expect("processing connection");
        let mut api_connection = api.pool().acquire().await.expect("API connection");
        let processing_pid: i32 =
            sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *processing_connection).await.expect("processing PID");
        let api_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *api_connection).await.expect("API PID");
        assert_ne!(processing_pid, api_pid);

        let processing_read_only: String = sqlx::query_scalar("SHOW default_transaction_read_only")
            .fetch_one(&mut *processing_connection)
            .await
            .expect("processing access mode");
        let api_read_only: String =
            sqlx::query_scalar("SHOW default_transaction_read_only").fetch_one(&mut *api_connection).await.expect("API access mode");
        assert_eq!(processing_read_only, "off");
        assert_eq!(api_read_only, "on");
        assert!(
            sqlx::query("UPDATE administrative_metadata SET last_reinitialization_token = 'forbidden'")
                .execute(&mut *api_connection)
                .await
                .is_err()
        );

        let api_clone = Arc::clone(&api);
        assert!(api.retire());
        assert!(!api.is_valid());
        assert!(!api_clone.is_valid());
        assert!(!api.retire());
        assert!(processing.is_valid());
        assert!(processing.retire());
        assert!(!processing.is_valid());
    }

    #[tokio::test]
    async fn inconsistent_state_opens_only_a_rebuild_usable_processing_generation() {
        let (_container, database_url) = fixture().await;
        let (mut database, _) = prepared_database(&database_url).await;
        database.initialize_if_uninitialized(mainnet(), hash(7), None).await.expect("initialization");
        sqlx::query("ALTER TABLE processing_metadata DROP CONSTRAINT processing_metadata_db_pp_blue_score_check")
            .execute(database.connection_mut())
            .await
            .expect("permit corrupted score fixture");
        sqlx::query("INSERT INTO processing_metadata (singleton, db_pp_blue_score) VALUES (TRUE, -1)")
            .execute(database.connection_mut())
            .await
            .expect("corrupt pruning-point score");
        let state = database.classify().await.expect("inconsistent classification");
        let (retirement_tx, _retirement_rx) = retirement_channel();

        let generations = open_validated_generations(database_url.clone(), state, retirement_tx)
            .await
            .expect("validated generation creation")
            .expect("bound inconsistent database must publish processing generation");
        let (processing, api) = generations.into_parts();

        assert_eq!(processing.binding().network_id(), mainnet());
        assert_eq!(processing.binding().genesis_hash(), hash(7));
        assert!(api.is_none());
    }

    #[tokio::test]
    async fn preparatory_schema_and_rolled_back_binding_remain_uninitialized() {
        let (_container, database_url) = fixture().await;
        let (mut database, state) = prepared_database(&database_url).await;
        assert_eq!(state, DatabaseState::Uninitialized);

        let mut transaction = database.connection_mut().begin().await.expect("transaction");
        sqlx::query(
            "INSERT INTO network_metadata (singleton, network_id, genesis_hash)
             VALUES (TRUE, $1, $2)",
        )
        .bind(mainnet().to_string())
        .bind(hash(3).as_bytes().as_slice())
        .execute(&mut *transaction)
        .await
        .expect("provisional binding");
        transaction.rollback().await.expect("rollback");

        assert_eq!(database.classify().await.expect("classification"), DatabaseState::Uninitialized);
    }

    #[tokio::test]
    async fn supported_older_schema_migrates_before_classification() {
        let (_container, database_url) = fixture().await;
        let mut connection = PgConnection::connect(&database_url).await.expect("fixture connection");
        migration::MIGRATOR.run_to(1, &mut connection).await.expect("first migration only");
        sqlx::query(
            "INSERT INTO network_metadata (singleton, network_id, genesis_hash)
             VALUES (TRUE, $1, $2)",
        )
        .bind(mainnet().to_string())
        .bind(hash(10).as_bytes().as_slice())
        .execute(&mut connection)
        .await
        .expect("version-one empty binding");
        connection.close().await.expect("close migration connection");

        let (mut database, state) = prepared_database(&database_url).await;
        let DatabaseState::Empty(binding) = state else {
            panic!("version-one empty binding must migrate to Empty");
        };
        assert_eq!(binding.network_id(), mainnet());
        assert_eq!(binding.genesis_hash(), hash(10));
        let processing_metadata_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM processing_metadata")
            .fetch_one(database.connection_mut())
            .await
            .expect("migrated processing-metadata count");
        assert_eq!(processing_metadata_rows, 0);
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
        let dirty = LockedDatabase::connect(&dirty_url).await.expect("database lock");
        assert!(matches!(dirty.prepare().await, Err(StorageError::Migration { .. })));
    }

    #[tokio::test]
    async fn unknown_partial_and_newer_schemas_are_rejected() {
        let (_unknown_container, unknown_url) = fixture().await;
        let mut connection = PgConnection::connect(&unknown_url).await.expect("fixture connection");
        sqlx::query("CREATE TABLE unrelated_user_data (id BIGINT PRIMARY KEY)").execute(&mut connection).await.expect("unknown table");
        connection.close().await.expect("close fixture connection");
        let unknown = LockedDatabase::connect(&unknown_url).await.expect("database lock");
        assert!(matches!(unknown.prepare().await, Err(StorageError::Rejected(StorageRejection::UnsupportedSchema { .. }))));

        let (_partial_container, partial_url) = fixture().await;
        let mut connection = PgConnection::connect(&partial_url).await.expect("fixture connection");
        sqlx::query("CREATE TABLE network_metadata (singleton BOOLEAN PRIMARY KEY)")
            .execute(&mut connection)
            .await
            .expect("partial table");
        connection.close().await.expect("close fixture connection");
        let partial = LockedDatabase::connect(&partial_url).await.expect("database lock");
        assert!(matches!(partial.prepare().await, Err(StorageError::Rejected(StorageRejection::UnsupportedSchema { .. }))));

        let (_tampered_container, tampered_url) = fixture().await;
        let (mut tampered, _) = prepared_database(&tampered_url).await;
        sqlx::query("ALTER TABLE administrative_metadata DROP COLUMN last_reinitialization_token")
            .execute(tampered.connection_mut())
            .await
            .expect("tamper with current schema");
        drop(tampered);
        let tampered = LockedDatabase::connect(&tampered_url).await.expect("database lock");
        assert!(matches!(tampered.prepare().await, Err(StorageError::Rejected(StorageRejection::UnsupportedSchema { .. }))));

        let (_newer_container, newer_url) = fixture().await;
        let (mut newer, _) = prepared_database(&newer_url).await;
        let future_version = migration::current_version() + 1;
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
             VALUES ($1, 'future', TRUE, '\\x00', 0)",
        )
        .bind(future_version)
        .execute(newer.connection_mut())
        .await
        .expect("future migration marker");
        drop(newer);
        let newer = LockedDatabase::connect(&newer_url).await.expect("database lock");
        let error = match newer.prepare().await {
            Ok(_) => panic!("newer schema must be rejected"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            StorageError::Rejected(StorageRejection::SchemaTooNew {
                observed: future_version,
                supported: migration::current_version(),
            })
        );
    }

    #[tokio::test]
    async fn missing_required_indexes_and_constraints_are_rejected() {
        {
            let (_container, database_url) = fixture().await;
            assert_schema_tamper_rejected(&database_url, "DROP INDEX blocks_vspc_sink_idx").await;
        }
        {
            let (_container, database_url) = fixture().await;
            assert_schema_tamper_rejected(&database_url, "ALTER TABLE blocks DROP CONSTRAINT blocks_level_slot_key").await;
        }
        {
            let (_container, database_url) = fixture().await;
            assert_schema_tamper_rejected(&database_url, "ALTER TABLE blocks DROP CONSTRAINT blocks_daa_score_check").await;
        }
        {
            let (_container, database_url) = fixture().await;
            assert_schema_tamper_rejected(&database_url, "ALTER TABLE blocks DROP CONSTRAINT blocks_selected_parent_id_fkey").await;
        }
    }

    #[tokio::test]
    async fn bounded_session_state_distinguishes_initialized_and_inconsistent() {
        let (_container, database_url) = fixture().await;
        let (mut database, _) = prepared_database(&database_url).await;
        database.initialize_if_uninitialized(mainnet(), hash(9), None).await.expect("initialization");

        sqlx::query("INSERT INTO processing_metadata (singleton, db_pp_blue_score) VALUES (TRUE, 0)")
            .execute(database.connection_mut())
            .await
            .expect("Genesis processing metadata");

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
        let initialized = database.classify().await.expect("Genesis classification");
        assert!(matches!(initialized, DatabaseState::Initialized(_)));

        let (retirement_tx, _retirement_rx) = retirement_channel();
        let generations = open_validated_generations(database_url.clone(), initialized, retirement_tx)
            .await
            .expect("validated generation creation")
            .expect("initialized database must publish generations");
        let (processing, _api) = generations.into_parts();
        let StoredSessionState::Initialized(snapshot) = processing.load_session_state().await.expect("initialized session state")
        else {
            panic!("Genesis contents must produce an initialized session snapshot");
        };
        assert_eq!(snapshot.db_pp_hash, hash(9));
        assert_eq!(snapshot.db_pp_blue_score, 0);
        assert_eq!(snapshot.committed_vspc_sink.hash, hash(9));
        assert_eq!(snapshot.committed_vspc_sink.id.get(), 2);
        assert_eq!(snapshot.committed_vspc_sink.selected_parent, hash(8));
        assert_eq!(snapshot.committed_vspc_sink.daa_score, 0);

        sqlx::query("INSERT INTO block_identifiers (id, hash) VALUES (3, $1)")
            .bind(hash(7).as_bytes().as_slice())
            .execute(database.connection_mut())
            .await
            .expect("extra boundary identity");
        // Session loading deliberately does not rescan every retained identity.
        assert!(matches!(database.classify().await.expect("bounded classification"), DatabaseState::Initialized(_)));
        assert!(matches!(
            processing.load_session_state().await.expect("fresh bounded session state"),
            StoredSessionState::Initialized(_)
        ));

        sqlx::query("ALTER TABLE processing_metadata DROP CONSTRAINT processing_metadata_db_pp_blue_score_check")
            .execute(database.connection_mut())
            .await
            .expect("permit corrupted score fixture");
        sqlx::query("UPDATE processing_metadata SET db_pp_blue_score = -1")
            .execute(database.connection_mut())
            .await
            .expect("corrupt pruning-point score");
        let negative = database.classify().await.expect("negative score classification");
        assert!(matches!(
            negative,
            DatabaseState::Inconsistent { binding }
                if binding.network_id() == mainnet() && binding.genesis_hash() == hash(9)
        ));
        assert_eq!(processing.load_session_state().await.expect("negative metadata session state"), StoredSessionState::Inconsistent);
    }

    #[tokio::test]
    async fn score_constraints_and_timestamp_bit_patterns_are_exact() {
        let (_container, database_url) = fixture().await;
        let (mut database, _) = prepared_database(&database_url).await;
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
