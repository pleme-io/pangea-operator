use pangea_leaf::migrate::{
    self, Direction, LeafState, MigrateError, Receipt, ARCHIVE_ROW, ARTIFACT_KIND, LIVE_ROW,
    REVERTED_ROW,
};
use pangea_leaf::plan::StateMove;
use sqlx::{PgPool, Postgres, Transaction};

use super::schema::{tofu_state_schema_ident, tofu_state_table_ident};
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Migrated(Receipt),
    AlreadyMigrated(Receipt),
    Reversed(Receipt),
    Refused(String),
}

fn refused(e: MigrateError) -> Outcome {
    Outcome::Refused(e.to_string())
}

async fn ensure_table(
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
    template: &str,
) -> Result<String> {
    let folded = tofu_state_schema_ident(schema, template)?;
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {folded}"))
        .execute(&mut **tx)
        .await
        .map_err(Error::Database)?;
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {folded}.states (id BIGSERIAL PRIMARY KEY, name TEXT UNIQUE, data TEXT)"
    ))
    .execute(&mut **tx)
    .await
    .map_err(Error::Database)?;
    tofu_state_table_ident(schema, template)
}

async fn read_row(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    row: &str,
) -> Result<Option<Vec<u8>>> {
    let got: Option<(Option<String>,)> =
        sqlx::query_as(&format!("SELECT data FROM {table} WHERE name = $1"))
            .bind(row)
            .fetch_optional(&mut **tx)
            .await
            .map_err(Error::Database)?;
    Ok(got.and_then(|(d,)| d).map(String::into_bytes))
}

async fn insert_row(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    row: &str,
    bytes: &[u8],
) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| Error::Config(format!("state bytes are not UTF-8: {e}")))?;
    sqlx::query(&format!("INSERT INTO {table} (name, data) VALUES ($1, $2)"))
        .bind(row)
        .bind(text)
        .execute(&mut **tx)
        .await
        .map_err(Error::Database)?;
    Ok(())
}

async fn upsert_row(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    row: &str,
    bytes: &[u8],
) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| Error::Config(format!("state bytes are not UTF-8: {e}")))?;
    sqlx::query(&format!(
        "INSERT INTO {table} (name, data) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data"
    ))
    .bind(row)
    .bind(text)
    .execute(&mut **tx)
    .await
    .map_err(Error::Database)?;
    Ok(())
}

async fn ensure_artifacts(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS pangea_meta")
        .execute(&mut **tx)
        .await
        .map_err(Error::Database)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pangea_meta.artifacts ( \
            schema_name VARCHAR(63) NOT NULL, template_name VARCHAR(253) NOT NULL, \
            kind VARCHAR(32) NOT NULL, content_hash CHAR(64) NOT NULL, data BYTEA NOT NULL, \
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), source_revision TEXT, \
            PRIMARY KEY (schema_name, template_name, kind))",
    )
    .execute(&mut **tx)
    .await
    .map_err(Error::Database)?;
    Ok(())
}

async fn read_receipt(
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
    fat: &str,
) -> Result<Option<Receipt>> {
    let row: Option<(Vec<u8>,)> = sqlx::query_as(
        "SELECT data FROM pangea_meta.artifacts WHERE schema_name = $1 AND template_name = $2 AND kind = $3",
    )
    .bind(schema)
    .bind(fat)
    .bind(ARTIFACT_KIND)
    .fetch_optional(&mut **tx)
    .await
    .map_err(Error::Database)?;
    row.map(|(d,)| serde_json::from_slice(&d).map_err(Error::Serialization))
        .transpose()
}

async fn write_receipt(
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
    fat: &str,
    receipt: &Receipt,
) -> Result<()> {
    let bytes = serde_json::to_vec(receipt).map_err(Error::Serialization)?;
    let hash = blake3::hash(&bytes).to_hex().to_string();
    sqlx::query(
        "INSERT INTO pangea_meta.artifacts (schema_name, template_name, kind, content_hash, data, updated_at) \
         VALUES ($1, $2, $3, $4, $5, NOW()) \
         ON CONFLICT (schema_name, template_name, kind) DO UPDATE SET \
         content_hash = EXCLUDED.content_hash, data = EXCLUDED.data, updated_at = NOW()",
    )
    .bind(schema)
    .bind(fat)
    .bind(ARTIFACT_KIND)
    .bind(hash)
    .bind(bytes)
    .execute(&mut **tx)
    .await
    .map_err(Error::Database)?;
    Ok(())
}

pub async fn forward(
    pool: &PgPool,
    schema: &str,
    fat: &str,
    moves: &[StateMove],
) -> Result<Outcome> {
    forward_with(pool, schema, fat, |_| Ok(moves.to_vec())).await
}

pub async fn forward_by_key(
    pool: &PgPool,
    schema: &str,
    fat: &str,
    pangea_namespace: &str,
    leaves: &[(String, String)],
) -> Result<Outcome> {
    forward_with(pool, schema, fat, |bytes| {
        migrate::moves_by_key(fat, Some(pangea_namespace), bytes, leaves)
    })
    .await
}

async fn forward_with(
    pool: &PgPool,
    schema: &str,
    fat: &str,
    make_moves: impl FnOnce(&[u8]) -> std::result::Result<Vec<StateMove>, MigrateError>,
) -> Result<Outcome> {
    let mut tx = pool.begin().await.map_err(Error::Database)?;
    ensure_artifacts(&mut tx).await?;
    if let Some(r) = read_receipt(&mut tx, schema, fat).await? {
        if r.direction == Direction::Forward {
            return Ok(Outcome::AlreadyMigrated(r));
        }
    }
    let fat_table = tofu_state_table_ident(schema, fat)?;
    let Some(fat_bytes) = read_row(&mut tx, &fat_table, LIVE_ROW).await? else {
        return Ok(Outcome::Refused(format!(
            "`{fat}` has no `{LIVE_ROW}` state row"
        )));
    };
    let moves = match make_moves(&fat_bytes) {
        Ok(m) => m,
        Err(e) => return Ok(refused(e)),
    };
    let split = match migrate::split(fat, &fat_bytes, &moves) {
        Ok(s) => s,
        Err(e) => return Ok(refused(e)),
    };
    upsert_row(&mut tx, &fat_table, ARCHIVE_ROW, &fat_bytes).await?;
    for leaf in &split.leaves {
        let table = ensure_table(&mut tx, schema, &leaf.template).await?;
        if read_row(&mut tx, &table, LIVE_ROW).await?.is_some() {
            return Ok(Outcome::Refused(format!(
                "leaf `{}` already has a `{LIVE_ROW}` state row",
                leaf.template
            )));
        }
        insert_row(&mut tx, &table, LIVE_ROW, &leaf.bytes).await?;
    }
    let mut read_back = Vec::with_capacity(split.leaves.len());
    for leaf in &split.leaves {
        let table = tofu_state_table_ident(schema, &leaf.template)?;
        let bytes = read_row(&mut tx, &table, LIVE_ROW)
            .await?
            .unwrap_or_default();
        read_back.push(LeafState {
            template: leaf.template.clone(),
            bytes,
        });
    }
    if let Err(e) = migrate::verify_written(&split.receipt, &read_back) {
        return Ok(refused(e));
    }
    if read_row(&mut tx, &fat_table, LIVE_ROW).await?.as_deref() != Some(fat_bytes.as_slice()) {
        return Ok(Outcome::Refused(format!(
            "`{fat}` state moved during the migration"
        )));
    }
    write_receipt(&mut tx, schema, fat, &split.receipt).await?;
    tx.commit().await.map_err(Error::Database)?;
    Ok(Outcome::Migrated(split.receipt))
}

pub async fn reverse(pool: &PgPool, schema: &str, fat: &str) -> Result<Outcome> {
    let mut tx = pool.begin().await.map_err(Error::Database)?;
    ensure_artifacts(&mut tx).await?;
    let receipt = match read_receipt(&mut tx, schema, fat).await? {
        Some(r) if r.direction == Direction::Forward => r,
        Some(_) => return Ok(Outcome::Refused(format!("`{fat}` is already reversed"))),
        None => {
            return Ok(Outcome::Refused(format!(
                "`{fat}` has no {ARTIFACT_KIND} artifact"
            )))
        }
    };
    let fat_table = tofu_state_table_ident(schema, fat)?;
    let Some(archive) = read_row(&mut tx, &fat_table, ARCHIVE_ROW).await? else {
        return Ok(Outcome::Refused(format!(
            "`{fat}` has no `{ARCHIVE_ROW}` archive row"
        )));
    };
    let mut leaves = Vec::with_capacity(receipt.leaves.len());
    for l in &receipt.leaves {
        let table = tofu_state_table_ident(schema, &l.template)?;
        let Some(bytes) = read_row(&mut tx, &table, LIVE_ROW).await? else {
            return Ok(refused(MigrateError::MissingLeaf(l.template.clone())));
        };
        leaves.push(LeafState {
            template: l.template.clone(),
            bytes,
        });
    }
    let merged = match migrate::merge(&archive, &receipt, &leaves) {
        Ok(m) => m,
        Err(e) => return Ok(refused(e)),
    };
    upsert_row(&mut tx, &fat_table, LIVE_ROW, &merged).await?;
    for leaf in &leaves {
        let table = tofu_state_table_ident(schema, &leaf.template)?;
        upsert_row(&mut tx, &table, REVERTED_ROW, &leaf.bytes).await?;
        sqlx::query(&format!("DELETE FROM {table} WHERE name = $1"))
            .bind(LIVE_ROW)
            .execute(&mut *tx)
            .await
            .map_err(Error::Database)?;
    }
    let reversed = Receipt {
        direction: Direction::Reversed,
        ..receipt
    };
    write_receipt(&mut tx, schema, fat, &reversed).await?;
    tx.commit().await.map_err(Error::Database)?;
    Ok(Outcome::Reversed(reversed))
}
