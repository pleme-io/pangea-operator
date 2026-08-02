//! PostgreSQL schema management for namespace isolation.
//!
//! Each PangeaNamespace gets its own PostgreSQL schema, providing
//! complete isolation of state data between namespaces.

use crate::error::{Error, Result};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Manages PostgreSQL schemas for Pangea namespaces.
#[derive(Clone)]
pub struct SchemaManager {
    pool: Arc<PgPool>,
}

impl SchemaManager {
    /// Create a new schema manager.
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Ensure a schema exists for the given namespace.
    pub async fn ensure_schema(&self, schema_name: &str) -> Result<()> {
        debug!(schema_name, "Ensuring schema exists");

        let query = create_schema_ddl(schema_name)?;
        sqlx::query(&query)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        info!(schema_name, "Schema ready");
        Ok(())
    }

    /// Create the state table for a template within a schema.
    ///
    /// This follows OpenTofu's pg backend table structure.
    pub async fn ensure_state_table(&self, schema_name: &str, template_name: &str) -> Result<()> {
        // Each part is validated + quoted SEPARATELY: the dot between schema
        // and table is SQL syntax, not part of either name, so quoting the
        // joined string would ask for one table literally named
        // `schema.table_states`.
        let quoted_schema = checked_quoted_ident(schema_name)?;
        let quoted_table = checked_quoted_ident(&format!("{template_name}_states"))?;
        let table_name = format!("{quoted_schema}.{quoted_table}");

        debug!(table_name, "Ensuring state table exists");

        // Create table matching OpenTofu pg backend format
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL DEFAULT 'default',
                data BYTEA,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
            table_name
        );

        sqlx::query(&create_table)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        // Create unique index on name
        let create_index = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (name)",
            checked_quoted_ident(&format!("{template_name}_name_idx"))?,
            table_name
        );

        sqlx::query(&create_index)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        info!(table_name, "State table ready");
        Ok(())
    }

    /// Check if a schema exists.
    pub async fn schema_exists(&self, schema_name: &str) -> Result<bool> {
        let row: (bool,) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
        )
        .bind(schema_name)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| Error::Database(e))?;

        Ok(row.0)
    }

    /// List all schemas managed by Pangea (matching prefix).
    pub async fn list_schemas(&self, prefix: &str) -> Result<Vec<String>> {
        let pattern = format!("{}%", prefix);

        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE $1",
        )
        .bind(&pattern)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| Error::Database(e))?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    /// Drop a schema and all its contents.
    ///
    /// **Warning**: This is destructive and cannot be undone.
    pub async fn drop_schema(&self, schema_name: &str) -> Result<()> {
        let quoted = checked_quoted_ident(schema_name)?;

        warn!(schema_name, "Dropping schema (destructive operation)");

        let query = format!("DROP SCHEMA IF EXISTS {quoted} CASCADE");
        sqlx::query(&query)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| Error::Database(e))?;

        info!(schema_name, "Schema dropped");
        Ok(())
    }

    /// Get table count in a schema.
    pub async fn table_count(&self, schema_name: &str) -> Result<i64> {
        let row: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_type = 'BASE TABLE'
            "#,
        )
        .bind(schema_name)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| Error::Database(e))?;

        Ok(row.0)
    }
}

/// Strict bare-identifier check: `[a-z_][a-z0-9_]*`, ≤63 bytes.
///
/// `pub(crate)` so other backend modules (e.g. `artifact_store`) reuse the
/// one canonical guard rather than re-implementing it — the SQL-injection
/// defense lives in exactly one place.
///
/// DELIBERATELY UNCHANGED. Three other call sites
/// (`artifact_store::live_state_schema`, `live_state_table`,
/// `state::qualified_state_table`) use this as their injection guard, by
/// validating a SANITIZED projection of the name and then quoting the
/// original. Widening this function to accept hyphens — which I tried
/// first — silently removes the guard from all three, and their own tests
/// caught it. Keep it strict; use [`checked_quoted_ident`] to accept a
/// real-world name.
pub(crate) fn is_valid_identifier(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }

    let first = name.chars().next().unwrap();
    if !first.is_ascii_lowercase() && first != '_' {
        return false;
    }

    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// The `CREATE SCHEMA` statement for a namespace, as a pure function.
///
/// Extracted from `ensure_schema` so the WIRING is testable, not just the
/// helpers. Measured while fixing this: reverting `ensure_schema` to the old
/// unquoted interpolation broke nothing in the suite, because every test
/// exercised the pure predicates and none touched the call site (it needs a
/// live `PgPool`). The fix was correct and completely uncovered — which is
/// the same vacuity class as a gate that scans zero files.
pub(crate) fn create_schema_ddl(schema_name: &str) -> Result<String> {
    Ok(format!(
        "CREATE SCHEMA IF NOT EXISTS {}",
        checked_quoted_ident(schema_name)?
    ))
}

/// Validate a real-world (k8s-shaped) identifier and render it QUOTED.
///
/// ── THE BUG THIS FIXES (2026-08-01) ─────────────────────────────────────
///
/// `ensure_schema` interpolated the name UNQUOTED, so it needed
/// [`is_valid_identifier`]'s bare-identifier rule — which rejects every
/// namespace containing a hyphen. pangea namespaces are named after repos
/// and clusters, so hyphens are the norm.
///
/// Measured: `pangea_camelot` exists (no hyphen, passed) while
/// `pangea_pleme-io-opensource` DOES NOT — `ensure_schema` refused it on
/// every reconcile with `Configuration error: Invalid schema name`, so the
/// fleet's largest workspace (2823 resources) never converged. The tell
/// that this was a bug rather than policy: sibling `_states` schemas WITH
/// the same hyphens already existed in the same database, written by
/// magma's backend, which quotes.
///
/// ── THE GUARD, WHICH IS NOT NEW ─────────────────────────────────────────
///
/// This is `artifact_store::live_state_schema`'s existing pattern, lifted
/// so both live in one place rather than being written twice: validate a
/// SANITIZED projection (`-`/`.` → `_`) through the strict checker, then
/// quote the ORIGINAL. A name that still fails after sanitizing carries a
/// character no k8s name can hold — a quote, semicolon, whitespace,
/// control byte — and is refused. Everything that survives is inert once
/// quoted.
pub(crate) fn checked_quoted_ident(name: &str) -> Result<String> {
    let sanitized = name.replace(['-', '.'], "_");
    if !is_valid_identifier(&sanitized) {
        return Err(Error::Config(format!("Invalid schema name: {name}")));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifier() {
        assert!(is_valid_identifier("pangea_production"));
        assert!(is_valid_identifier("my_schema"));
        assert!(is_valid_identifier("_private"));

        assert!(!is_valid_identifier("")); // Empty
        assert!(!is_valid_identifier("1starts_with_number"));
        assert!(!is_valid_identifier("has-dashes"));
        assert!(!is_valid_identifier("HasCaps"));
        assert!(!is_valid_identifier("has spaces"));
        assert!(!is_valid_identifier("has;semicolon"));
    }

    /// THE regression, pinned. `pangea_pleme-io-opensource` was refused on
    /// every reconcile, so the fleet's largest workspace could not create its
    /// schema and never converged — while sibling `_states` schemas with the
    /// same hyphens already existed, written by magma's backend, which quotes.
    ///
    /// The fix is at the QUOTING layer, not the validator: `is_valid_identifier`
    /// stays strict (three other call sites depend on that), and hyphenated
    /// names go through `checked_quoted_ident`.
    #[test]
    fn hyphenated_namespace_is_accepted_by_the_quoting_path() {
        assert!(checked_quoted_ident("pangea_pleme-io-opensource").is_ok());
        assert!(checked_quoted_ident("has-dashes").is_ok());
        // …and still refused as a BARE identifier, unchanged.
        assert!(!is_valid_identifier("pangea_pleme-io-opensource"));
        assert!(!is_valid_identifier("has-dashes"));
    }

    #[test]
    /// Covers the CALL SITE, not just the helper. The bug lived in the DDL
    /// string; asserting the helper alone left the wiring uncovered, which is
    /// how reverting the fix passed the whole suite.
    #[test]
    fn create_schema_ddl_quotes_a_hyphenated_namespace() {
        assert_eq!(
            create_schema_ddl("pangea_pleme-io-opensource").unwrap(),
            "CREATE SCHEMA IF NOT EXISTS \"pangea_pleme-io-opensource\""
        );
        assert_eq!(
            create_schema_ddl("pangea_camelot").unwrap(),
            "CREATE SCHEMA IF NOT EXISTS \"pangea_camelot\""
        );
        // The payload guard still applies at the call site.
        assert!(create_schema_ddl("schema; DROP TABLE").is_err());
    }

    #[test]
    fn checked_quoted_ident_accepts_hyphens_and_quotes_them() {
        // The regression: this exact name was refused on every reconcile.
        assert_eq!(
            checked_quoted_ident("pangea_pleme-io-opensource").unwrap(),
            "\"pangea_pleme-io-opensource\""
        );
        // The name that always worked keeps working, byte-identical apart
        // from the quotes.
        assert_eq!(
            checked_quoted_ident("pangea_camelot").unwrap(),
            "\"pangea_camelot\""
        );
        // Dots are k8s-shaped too, and sanitize the same way.
        assert_eq!(
            checked_quoted_ident("pangea_a.b-c").unwrap(),
            "\"pangea_a.b-c\""
        );
    }

    #[test]
    fn test_valid_identifier_max_length() {
        let name = "a".repeat(63);
        assert!(is_valid_identifier(&name));

        let too_long = "a".repeat(64);
        assert!(!is_valid_identifier(&too_long));
    }

    #[test]
    fn test_valid_identifier_single_char() {
        assert!(is_valid_identifier("a"));
        assert!(is_valid_identifier("_"));
    }

    #[test]
    fn test_valid_identifier_with_digits() {
        assert!(is_valid_identifier("schema_v2"));
        assert!(is_valid_identifier("_123"));
        assert!(is_valid_identifier("a1b2c3"));
    }

    #[test]
    fn test_valid_identifier_sql_injection_attempts() {
        // Unchanged contract: the strict checker still refuses all of these,
        // which is what keeps the THREE other call sites safe
        // (artifact_store::live_state_{schema,table}, state::qualified_state_table).
        assert!(!is_valid_identifier("schema; DROP TABLE"));
        assert!(!is_valid_identifier("schema' OR '1'='1"));
        assert!(!is_valid_identifier("schema--comment"));
        assert!(!is_valid_identifier("schema/*comment*/"));
        assert!(!is_valid_identifier("schema.other"));
    }

    /// The payloads `checked_quoted_ident` must REFUSE outright: after
    /// sanitizing `-`/`.`, they still carry a character no k8s name can hold.
    #[test]
    fn checked_quoted_ident_refuses_payloads_sanitizing_cannot_explain() {
        for evil in [
            "schema; DROP TABLE",
            "schema' OR '1'='1",
            "schema/*comment*/",
            "has\"quote",
            "has spaces",
        ] {
            assert!(
                checked_quoted_ident(evil).is_err(),
                "{evil:?} must be refused, not quoted"
            );
        }
    }

    /// The payloads sanitizing DOES explain — they survive as legal names,
    /// and quoting is what makes them inert. `schema.other` in particular
    /// must render as ONE identifier, never a qualified schema.table
    /// reference, and `--` is not a comment inside quotes.
    #[test]
    fn checked_quoted_ident_seals_names_that_survive_sanitizing() {
        for benign in ["schema--comment", "schema.other", "a-b.c"] {
            let rendered = checked_quoted_ident(benign).unwrap();
            assert_eq!(rendered, format!("\"{benign}\""));
            assert_eq!(
                rendered.matches('"').count(),
                2,
                "{benign:?} rendered as {rendered:?} — the identifier is not sealed"
            );
        }
    }

    #[test]
    fn test_valid_identifier_unicode() {
        assert!(!is_valid_identifier("schéma"));
        assert!(!is_valid_identifier("表"));
    }

    #[test]
    fn test_valid_identifier_uppercase_rejected() {
        assert!(!is_valid_identifier("Schema"));
        assert!(!is_valid_identifier("SCHEMA"));
        assert!(!is_valid_identifier("schemA"));
    }
}
