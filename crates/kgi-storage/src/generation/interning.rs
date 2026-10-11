use std::collections::HashMap;

use kgi_model::block::{BlockHash, CompactId};
use sqlx::{Postgres, Row, Transaction};

use crate::{error::StorageError, transaction::TransactionAttemptError};

pub(super) async fn intern_identities<S>(
    transaction: &mut Transaction<'_, Postgres>,
    hashes: &[BlockHash],
) -> Result<HashMap<BlockHash, CompactId>, TransactionAttemptError<S>> {
    if hashes.is_empty() {
        return Ok(HashMap::new());
    }

    let encoded: Vec<Vec<u8>> = hashes.iter().map(|hash| hash.as_bytes().to_vec()).collect();
    let rows = sqlx::query(
        "WITH input(hash, position) AS MATERIALIZED (
             SELECT hash, position
             FROM UNNEST($1::BYTEA[]) WITH ORDINALITY AS input(hash, position)
         ),
         inserted AS (
             INSERT INTO block_identifiers (hash)
             SELECT hash FROM input ORDER BY position
             RETURNING id, hash
         )
         SELECT inserted.id, inserted.hash
         FROM inserted
         JOIN input USING (hash)
         ORDER BY input.position",
    )
    .bind(encoded)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| TransactionAttemptError::database("block-identity interning", error))?;
    if rows.len() != hashes.len() {
        return Err(TransactionAttemptError::Storage(StorageError::invalid_metadata(
            "batched block-identity interning returned an incomplete result",
        )));
    }

    let mut identities = HashMap::with_capacity(rows.len());
    for row in rows {
        let encoded_hash: Vec<u8> =
            row.try_get("hash").map_err(|error| TransactionAttemptError::database("interned block hash decode", error))?;
        let hash = BlockHash::try_from(encoded_hash.as_slice()).map_err(|error| {
            TransactionAttemptError::Storage(StorageError::invalid_metadata(format!("invalid interned block hash: {error}")))
        })?;
        let raw_id: i64 = row.try_get("id").map_err(|error| TransactionAttemptError::database("interned block ID decode", error))?;
        let id = CompactId::new(raw_id).ok_or_else(|| {
            TransactionAttemptError::Storage(StorageError::invalid_metadata(format!("generated compact ID is not positive: {raw_id}")))
        })?;
        identities.insert(hash, id);
    }
    Ok(identities)
}
