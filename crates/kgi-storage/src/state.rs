use std::{str::FromStr, sync::Arc};

use kaspa_consensus_core::network::NetworkId;
use kgi_model::block::{BlockHash, CompactId, MAX_DAA_SCORE};
use sqlx::{Connection, PgConnection, PgPool, Postgres, Row, Transaction};

use crate::{
    error::{StorageError, StorageRejection},
    generation::{DatabaseBinding, StoredSessionSnapshot, StoredSessionState, StoredVspcSink},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DatabaseState {
    Uninitialized,
    Empty(DatabaseBinding),
    Initialized(DatabaseBinding),
    Inconsistent { binding: DatabaseBinding },
}

impl DatabaseState {
    pub(crate) fn binding(&self) -> Option<DatabaseBinding> {
        match self {
            Self::Uninitialized => None,
            Self::Empty(binding) | Self::Initialized(binding) | Self::Inconsistent { binding } => Some(*binding),
        }
    }
}

#[derive(Clone, Copy)]
struct ProcessingMetadata {
    db_pp_blue_score: u64,
}

enum StoredProcessingMetadata {
    Absent,
    Valid(ProcessingMetadata),
    Invalid,
}

#[derive(Clone, Copy)]
struct ProcessingPresence {
    identifiers: bool,
    blocks: bool,
    levels: bool,
    parents: bool,
}

impl ProcessingPresence {
    fn is_empty(self) -> bool {
        !self.identifiers && !self.blocks && !self.levels && !self.parents
    }
}

#[derive(Clone, Copy)]
struct StoredPruningPoint {
    hash: BlockHash,
    is_in_vspc: bool,
}

pub(crate) struct ProcessingStateInspection<'connection> {
    transaction: Transaction<'connection, Postgres>,
}

impl ProcessingStateInspection<'_> {
    pub(crate) async fn classify(connection: &mut PgConnection) -> Result<DatabaseState, StorageError> {
        let transaction =
            connection.begin().await.map_err(|error| StorageError::database("classification transaction start", error))?;
        ProcessingStateInspection::begin(transaction).await?.finish_database_classification().await
    }

    pub(crate) async fn load(pool: &PgPool, binding: DatabaseBinding) -> Result<StoredSessionState, StorageError> {
        let transaction = pool.begin().await.map_err(|error| StorageError::database("session-state transaction start", error))?;
        ProcessingStateInspection::begin(transaction).await?.finish_session_state(binding).await
    }
}

impl<'connection> ProcessingStateInspection<'connection> {
    async fn begin(transaction: Transaction<'connection, Postgres>) -> Result<Self, StorageError> {
        let mut inspection = Self { transaction };
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *inspection.transaction)
            .await
            .map_err(|error| StorageError::database("read snapshot configuration", error))?;
        Ok(inspection)
    }

    async fn finish_database_classification(mut self) -> Result<DatabaseState, StorageError> {
        self.validate_administrative_metadata().await?;
        let binding = self.read_network_binding().await?;

        let Some(binding) = binding else {
            let metadata = self.read_processing_metadata().await?;
            let presence = self.processing_presence().await?;
            if presence.is_empty() && matches!(metadata, StoredProcessingMetadata::Absent) {
                self.commit("classification transaction commit").await?;
                return Ok(DatabaseState::Uninitialized);
            }
            return Err(StorageRejection::UnsupportedSchema {
                diagnostic: Arc::from("processing state exists without a complete network binding"),
            }
            .into());
        };

        let session_state = self.inspect_processing_state(binding).await?;
        self.commit("classification transaction commit").await?;
        Ok(match session_state {
            StoredSessionState::Empty => DatabaseState::Empty(binding),
            StoredSessionState::Initialized(_) => DatabaseState::Initialized(binding),
            StoredSessionState::Inconsistent => DatabaseState::Inconsistent { binding },
        })
    }

    async fn finish_session_state(mut self, binding: DatabaseBinding) -> Result<StoredSessionState, StorageError> {
        let state = self.inspect_processing_state(binding).await?;
        self.commit("session-state transaction commit").await?;
        Ok(state)
    }

    async fn commit(self, operation: &'static str) -> Result<(), StorageError> {
        self.transaction.commit().await.map_err(|error| StorageError::database(operation, error))
    }

    async fn validate_administrative_metadata(&mut self) -> Result<(), StorageError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM administrative_metadata")
            .fetch_one(&mut *self.transaction)
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

    async fn read_network_binding(&mut self) -> Result<Option<DatabaseBinding>, StorageError> {
        let rows = sqlx::query("SELECT network_id, genesis_hash FROM network_metadata")
            .fetch_all(&mut *self.transaction)
            .await
            .map_err(|error| StorageError::database("network-metadata read", error))?;
        match rows.as_slice() {
            [] => Ok(None),
            [row] => {
                let network_text: String =
                    row.try_get("network_id").map_err(|error| StorageError::invalid_metadata(error.to_string()))?;
                let network_id = NetworkId::from_str(&network_text)
                    .map_err(|error| StorageError::invalid_metadata(format!("invalid network ID {network_text:?}: {error}")))?;
                let genesis_bytes: Vec<u8> =
                    row.try_get("genesis_hash").map_err(|error| StorageError::invalid_metadata(error.to_string()))?;
                let genesis_hash = BlockHash::try_from(genesis_bytes.as_slice())
                    .map_err(|error| StorageError::invalid_metadata(format!("invalid Genesis hash: {error}")))?;
                Ok(Some(DatabaseBinding::new(network_id, genesis_hash)))
            }
            _ => Err(StorageError::invalid_metadata("multiple network-metadata rows")),
        }
    }

    async fn read_processing_metadata(&mut self) -> Result<StoredProcessingMetadata, StorageError> {
        let scores = sqlx::query_scalar::<_, i64>("SELECT db_pp_blue_score FROM processing_metadata")
            .fetch_all(&mut *self.transaction)
            .await
            .map_err(|error| StorageError::database("processing-metadata read", error))?;
        match scores.as_slice() {
            [] => Ok(StoredProcessingMetadata::Absent),
            [score] => Ok(match u64::try_from(*score) {
                Ok(db_pp_blue_score) => StoredProcessingMetadata::Valid(ProcessingMetadata { db_pp_blue_score }),
                Err(_) => StoredProcessingMetadata::Invalid,
            }),
            _ => Err(StorageError::invalid_metadata("multiple processing-metadata rows")),
        }
    }

    async fn processing_presence(&mut self) -> Result<ProcessingPresence, StorageError> {
        let row = sqlx::query(
            "SELECT
                EXISTS (SELECT 1 FROM block_identifiers) AS identifiers,
                EXISTS (SELECT 1 FROM blocks) AS blocks,
                EXISTS (SELECT 1 FROM levels) AS levels,
                EXISTS (SELECT 1 FROM parents) AS parents",
        )
        .fetch_one(&mut *self.transaction)
        .await
        .map_err(|error| StorageError::database("processing-state inspection", error))?;
        Ok(ProcessingPresence {
            identifiers: row.try_get("identifiers").map_err(|error| StorageError::database("identifier presence decode", error))?,
            blocks: row.try_get("blocks").map_err(|error| StorageError::database("block presence decode", error))?,
            levels: row.try_get("levels").map_err(|error| StorageError::database("level presence decode", error))?,
            parents: row.try_get("parents").map_err(|error| StorageError::database("parent presence decode", error))?,
        })
    }

    async fn inspect_processing_state(&mut self, binding: DatabaseBinding) -> Result<StoredSessionState, StorageError> {
        let metadata = self.read_processing_metadata().await?;
        let presence = self.processing_presence().await?;
        if presence.is_empty() {
            return Ok(if matches!(metadata, StoredProcessingMetadata::Absent) {
                StoredSessionState::Empty
            } else {
                StoredSessionState::Inconsistent
            });
        }

        let StoredProcessingMetadata::Valid(metadata) = metadata else {
            return Ok(StoredSessionState::Inconsistent);
        };
        let Some(pruning_point) = self.read_pruning_point().await? else {
            return Ok(StoredSessionState::Inconsistent);
        };
        if !pruning_point.is_in_vspc || (pruning_point.hash == binding.genesis_hash() && metadata.db_pp_blue_score != 0) {
            return Ok(StoredSessionState::Inconsistent);
        }
        let Some(committed_vspc_sink) = self.read_committed_vspc_sink().await? else {
            return Ok(StoredSessionState::Inconsistent);
        };

        Ok(StoredSessionState::Initialized(StoredSessionSnapshot {
            db_pp_hash: pruning_point.hash,
            db_pp_blue_score: metadata.db_pp_blue_score,
            committed_vspc_sink,
        }))
    }

    async fn read_pruning_point(&mut self) -> Result<Option<StoredPruningPoint>, StorageError> {
        let row = sqlx::query(
            "SELECT identity.hash, block.is_in_vspc
             FROM blocks block
             JOIN block_identifiers identity ON identity.id = block.id
             JOIN levels level_state ON level_state.level = block.level
             WHERE block.level = 1 AND block.slot = 0",
        )
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(|error| StorageError::database("pruning-point inspection", error))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let hash: Vec<u8> = row.try_get("hash").map_err(|error| StorageError::database("pruning-point hash decode", error))?;
        let is_in_vspc: bool =
            row.try_get("is_in_vspc").map_err(|error| StorageError::database("pruning-point VSPC decode", error))?;
        Ok(BlockHash::try_from(hash.as_slice()).ok().map(|hash| StoredPruningPoint { hash, is_in_vspc }))
    }

    async fn read_committed_vspc_sink(&mut self) -> Result<Option<StoredVspcSink>, StorageError> {
        let row = sqlx::query(
            "SELECT sink.id, sink_identity.hash, selected_parent.hash AS selected_parent_hash, sink.daa_score
             FROM blocks sink
             JOIN block_identifiers sink_identity ON sink_identity.id = sink.id
             JOIN block_identifiers selected_parent ON selected_parent.id = sink.selected_parent_id
             WHERE sink.is_in_vspc
             ORDER BY sink.id DESC
             LIMIT 1",
        )
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(|error| StorageError::database("committed VSPC sink inspection", error))?;
        let Some(row) = row else {
            return Ok(None);
        };

        let id: i64 = row.try_get("id").map_err(|error| StorageError::database("committed sink ID decode", error))?;
        let hash: Vec<u8> = row.try_get("hash").map_err(|error| StorageError::database("committed sink hash decode", error))?;
        let selected_parent: Vec<u8> = row
            .try_get("selected_parent_hash")
            .map_err(|error| StorageError::database("committed sink selected-parent decode", error))?;
        let daa_score: i64 =
            row.try_get("daa_score").map_err(|error| StorageError::database("committed sink DAA-score decode", error))?;

        let sink = CompactId::new(id)
            .zip(BlockHash::try_from(hash.as_slice()).ok())
            .zip(BlockHash::try_from(selected_parent.as_slice()).ok())
            .zip(u64::try_from(daa_score).ok().filter(|score| *score <= MAX_DAA_SCORE))
            .map(|(((id, hash), selected_parent), daa_score)| StoredVspcSink { hash, id, selected_parent, daa_score });
        Ok(sink)
    }
}
