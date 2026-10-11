use kgi_model::block::{BlockColor, BlockCoordinate, CompactId, Timestamp};
use sqlx::{Postgres, Transaction};

use crate::{database::timestamp_to_sql, error::StorageError, transaction::TransactionAttemptError};

pub(super) struct BlockRow<'a> {
    pub id: CompactId,
    pub timestamp: Timestamp,
    pub daa_score: i64,
    pub coordinate: BlockCoordinate,
    pub selected_parent_id: CompactId,
    pub color: BlockColor,
    pub is_in_vspc: bool,
    pub blue_merge_set: &'a [CompactId],
    pub red_merge_set: &'a [CompactId],
}

#[derive(Clone, Copy)]
pub(super) struct ParentRow {
    pub id: CompactId,
    pub coordinate: Option<BlockCoordinate>,
}

pub(super) async fn insert_block_and_parents<S>(
    transaction: &mut Transaction<'_, Postgres>,
    block: BlockRow<'_>,
    parents: &[ParentRow],
) -> Result<(), TransactionAttemptError<S>> {
    let (block_level, block_slot) = encode_coordinate(block.coordinate)?;
    let blue_merge_set: Vec<_> = block.blue_merge_set.iter().map(|id| id.get()).collect();
    let red_merge_set: Vec<_> = block.red_merge_set.iter().map(|id| id.get()).collect();
    sqlx::query(
        "INSERT INTO blocks (
             id, timestamp, daa_score, level, slot, selected_parent_id,
             color, is_in_vspc, blue_merge_set, red_merge_set
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(block.id.get())
    .bind(timestamp_to_sql(block.timestamp))
    .bind(block.daa_score)
    .bind(block_level)
    .bind(block_slot)
    .bind(block.selected_parent_id.get())
    .bind(block.color as i16)
    .bind(block.is_in_vspc)
    .bind(blue_merge_set)
    .bind(red_merge_set)
    .execute(&mut **transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("materialized block-row insertion", error))?;

    let mut parent_ids = Vec::with_capacity(parents.len());
    let mut parent_levels = Vec::with_capacity(parents.len());
    let mut parent_slots = Vec::with_capacity(parents.len());
    for parent in parents {
        parent_ids.push(parent.id.get());
        let (level, slot) = match parent.coordinate {
            Some(coordinate) => encode_coordinate(coordinate)?,
            None => (0, 0),
        };
        parent_levels.push(level);
        parent_slots.push(slot);
    }
    sqlx::query(
        "INSERT INTO parents (
             child_id, parent_id, child_level, child_slot, parent_level, parent_slot
         )
         SELECT $1, relation.parent_id, $2, $3, relation.parent_level, relation.parent_slot
         FROM UNNEST($4::BIGINT[], $5::BIGINT[], $6::BIGINT[])
             AS relation(parent_id, parent_level, parent_slot)",
    )
    .bind(block.id.get())
    .bind(block_level)
    .bind(block_slot)
    .bind(parent_ids)
    .bind(parent_levels)
    .bind(parent_slots)
    .execute(&mut **transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("direct-parent row insertion", error))?;
    Ok(())
}

fn encode_coordinate<S>(coordinate: BlockCoordinate) -> Result<(i64, i64), TransactionAttemptError<S>> {
    let level = i64::try_from(coordinate.level())
        .map_err(|_| TransactionAttemptError::Storage(StorageError::invalid_metadata("block level exceeds BIGINT")))?;
    let slot = i64::try_from(coordinate.slot())
        .map_err(|_| TransactionAttemptError::Storage(StorageError::invalid_metadata("block slot exceeds BIGINT")))?;
    Ok((level, slot))
}
