use std::collections::{HashMap, HashSet};

use kgi_model::block::{
    BlockColor, BlockCoordinate, BlockHash, CompactId, ConsensusOrder, MaterializedSyncAnchor, ValidatedNodeBlock, VspcPoint,
};
use sqlx::{Postgres, Transaction};

use super::{
    ValidatedDbClient,
    block_rows::{self, BlockRow, ParentRow},
    interning,
};
use crate::{
    cache::{CachedIdentity, CachedMaterializedBlock, CachedMergeSets, CachedVspcSink, ProcessingCacheDomains},
    database::{blue_score_to_sql, daa_score_to_sql},
    error::StorageError,
    transaction::{self, TransactionAttemptError},
};

struct PreparedRebuild {
    block: ValidatedNodeBlock,
    universal: Vec<BlockHash>,
    daa_score: i64,
    blue_score: i64,
}

struct RebuildCommit {
    anchor: MaterializedSyncAnchor,
    caches: ProcessingCacheDomains,
}

#[derive(Clone, Copy)]
pub(crate) enum CommitBehavior {
    Normal,
    #[cfg(test)]
    CommitThenLoseAcknowledgement,
    #[cfg(test)]
    RollbackThenFail,
    #[cfg(test)]
    RollbackThenLoseConnection,
}

impl PreparedRebuild {
    fn new(block: ValidatedNodeBlock, genesis_hash: BlockHash) -> Result<Self, StorageError> {
        let daa_score = daa_score_to_sql(block.daa_score)?;
        let blue_score = blue_score_to_sql(block.blue_score)?;
        let mut seen = HashSet::new();
        let mut universal = Vec::new();
        for hash in block.red_merge_set.iter().chain(&block.blue_merge_set).chain(&block.direct_parents).copied() {
            if seen.insert(hash) {
                universal.push(hash);
            }
        }
        if block.hash == genesis_hash {
            // Exact Genesis has empty relationship vectors, so its synthetic ORIGIN selected parent must be interned explicitly.
            universal.push(block.selected_parent);
        }
        universal.push(block.hash);
        Ok(Self { block, universal, daa_score, blue_score })
    }
}

pub(super) fn validate_input(block: &ValidatedNodeBlock) -> Result<(), StorageError> {
    daa_score_to_sql(block.daa_score)?;
    blue_score_to_sql(block.blue_score)?;
    Ok(())
}

impl ValidatedDbClient {
    pub(crate) async fn perform_rebuild(&self, pruning_point: ValidatedNodeBlock) -> Result<MaterializedSyncAnchor, StorageError> {
        let prepared = PreparedRebuild::new(pruning_point, self.binding.genesis_hash())?;
        let commit_behavior = self.next_rebuild_commit_behavior();
        let _mutation =
            self.mutation_guard.try_write().expect("Rebuild requires completed processor deactivation and exclusive mutation access");
        let committed = transaction::run(&self.transaction_timing, || attempt(self.runtime.pool(), &prepared, commit_behavior))
            .await?
            .expect("the Rebuild transaction has no semantic rollback outcome");
        self.caches.replace(std::sync::Arc::new(committed.caches));
        Ok(committed.anchor)
    }

    fn next_rebuild_commit_behavior(&self) -> CommitBehavior {
        #[cfg(test)]
        {
            return self
                .rebuild_commit_behavior
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .unwrap_or(CommitBehavior::Normal);
        }
        #[cfg(not(test))]
        CommitBehavior::Normal
    }

    #[cfg(test)]
    pub(crate) fn inject_rebuild_commit_behavior(&self, behavior: CommitBehavior) {
        *self.rebuild_commit_behavior.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(behavior);
    }
}

async fn attempt(
    pool: &sqlx::PgPool,
    prepared: &PreparedRebuild,
    commit_behavior: CommitBehavior,
) -> Result<RebuildCommit, TransactionAttemptError<std::convert::Infallible>> {
    let mut transaction = pool.begin().await.map_err(|error| TransactionAttemptError::database("Rebuild transaction start", error))?;

    sqlx::query(
        "TRUNCATE parents, blocks, levels, block_identifiers, processing_metadata
         RESTART IDENTITY RESTRICT",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("Rebuild processing-data clear", error))?;

    let identities = interning::intern_identities(&mut transaction, &prepared.universal).await?;
    let own_id = identity(&identities, prepared.block.hash)?;
    let selected_parent_id = identity(&identities, prepared.block.selected_parent)?;
    let blue_ids = ids(&identities, &prepared.block.blue_merge_set)?;
    let red_ids = ids(&identities, &prepared.block.red_merge_set)?;

    let parent_ids = ids(&identities, &prepared.block.direct_parents)?;
    let coordinate = BlockCoordinate::new(1, 0).expect("Rebuild pruning point uses level one");
    let parent_rows: Vec<_> = parent_ids.iter().map(|id| ParentRow { id: *id, coordinate: None }).collect();
    block_rows::insert_block_and_parents(
        &mut transaction,
        BlockRow {
            id: own_id,
            timestamp: prepared.block.timestamp,
            daa_score: prepared.daa_score,
            coordinate,
            selected_parent_id,
            color: BlockColor::Gray,
            is_in_vspc: true,
            blue_merge_set: &blue_ids,
            red_merge_set: &red_ids,
        },
        &parent_rows,
    )
    .await?;

    sqlx::query("INSERT INTO levels (level, size, daa_score) VALUES (1, 1, $1)")
        .bind(prepared.daa_score)
        .execute(&mut *transaction)
        .await
        .map_err(|error| TransactionAttemptError::database("Rebuild level initialization", error))?;
    sqlx::query("INSERT INTO processing_metadata (singleton, db_pp_blue_score) VALUES (TRUE, $1)")
        .bind(prepared.blue_score)
        .execute(&mut *transaction)
        .await
        .map_err(|error| TransactionAttemptError::database("Rebuild processing-metadata initialization", error))?;

    #[cfg(test)]
    if matches!(commit_behavior, CommitBehavior::RollbackThenLoseConnection) {
        transaction
            .rollback()
            .await
            .map_err(|error| TransactionAttemptError::database("Rebuild rollback before injected connection loss", error))?;
        return Err(TransactionAttemptError::database(
            "Rebuild transaction before commit",
            sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "injected Rebuild pre-COMMIT connection loss")),
        ));
    }

    commit(transaction, commit_behavior)
        .await
        .map_err(|error| TransactionAttemptError::commit("Rebuild transaction commit", error))?;

    let caches = ProcessingCacheDomains::new();
    for (hash, id) in &identities {
        caches.publish_identity(*hash, CachedIdentity { id: *id, materialized: *hash == prepared.block.hash });
    }
    caches.publish_materialized_block(own_id, CachedMaterializedBlock { coordinate, selected_parent_id });
    caches.publish_merge_sets(own_id, CachedMergeSets { blue: blue_ids.into(), red: red_ids.into() });
    caches.publish_vspc_sink(CachedVspcSink { id: own_id, hash: prepared.block.hash });

    Ok(RebuildCommit {
        anchor: MaterializedSyncAnchor {
            point: VspcPoint::new(ConsensusOrder::new(prepared.block.blue_work, prepared.block.hash), own_id),
            selected_parent: prepared.block.selected_parent,
            blue_score: prepared.block.blue_score,
        },
        caches,
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
                "injected Rebuild COMMIT acknowledgement loss",
            )))
        }
        #[cfg(test)]
        CommitBehavior::RollbackThenFail => {
            transaction.rollback().await?;
            Err(sqlx::Error::RowNotFound)
        }
        #[cfg(test)]
        CommitBehavior::RollbackThenLoseConnection => unreachable!("handled before the COMMIT phase"),
    }
}

fn identity(
    identities: &HashMap<BlockHash, CompactId>,
    hash: BlockHash,
) -> Result<CompactId, TransactionAttemptError<std::convert::Infallible>> {
    identities
        .get(&hash)
        .copied()
        .ok_or_else(|| TransactionAttemptError::Storage(StorageError::internal_failure("Rebuild reference identity was not interned")))
}

fn ids(
    identities: &HashMap<BlockHash, CompactId>,
    hashes: &[BlockHash],
) -> Result<Vec<CompactId>, TransactionAttemptError<std::convert::Infallible>> {
    hashes.iter().map(|hash| identity(identities, *hash)).collect()
}
