//! `ArtifactStore` — Postgres-backed store for the operator's durable
//! reconcile artifacts on the **magma** execution path.
//!
//! # Why this exists — the magma-native destination
//!
//! Per the org ★★ MAGMA-NATIVE EXECUTION directive (and
//! `theory/MAGMA-OPERATOR-BACKEND.md` §II-bis): the operator pod is
//! pure compute. Transient values live on the heap; **durable values
//! live in Postgres; NOTHING durable lives on local disk** on the
//! magma path. The one sanctioned filesystem reach is loading provider
//! plugins (the gRPC plugin binaries the OS must `exec` from a path) —
//! everything else (compile-rendered config, the typed plan, the
//! compliance bundle, state) is RAM + Postgres.
//!
//! Before this store, the magma executor wrote three durable artifacts
//! to the pod-local workspace dir:
//!   * `main.tf.json`     — the compile→plan rendered-config handoff
//!   * `magma-plan.json`  — the typed plan checkpoint (plan→apply)
//!   * `magma-bundle.json`— the typed compliance bundle
//! Those files are ephemeral: a pod roll wipes the emptyDir workspace,
//! which os-error-2-looped every Planning/Applying/Ready drift check
//! (operator incident 2026-06-03). Routing the three artifacts through
//! Postgres makes that whole failure class **unrepresentable** — a
//! restart loses nothing and recomputes from the durable source.
//!
//! # The atomic apply op
//!
//! [`ArtifactStore::put_apply_result`] writes the post-apply **state**
//! row and the post-apply **bundle** artifact inside ONE Postgres
//! transaction, so they land together-or-not-at-all (the operator's
//! explicit atomicity requirement — a half-written apply where state
//! advanced but the receipt is missing, or vice-versa, cannot occur).
//!
//! # Integrity
//!
//! Every stored blob carries a BLAKE3 content hash (`content_hash`).
//! `get` recomputes the hash on read and refuses bytes whose hash does
//! not match — a corrupt or torn row surfaces as a typed
//! [`Error::StateBackend`] instead of feeding garbage into the plan /
//! apply / bundle pipeline.

use std::sync::Arc;

use sqlx::{PgPool, Postgres, Transaction};
use tracing::{debug, info};

use crate::backend::schema::is_valid_identifier;
use crate::error::{Error, Result};

/// Artifact kind discriminator stored in the `kind` column. Three
/// durable artifacts the magma reconcile path produces.
pub mod kind {
    /// The compile→plan rendered terraform JSON (was `main.tf.json`).
    pub const RENDERED_CONFIG: &str = "rendered_config";
    /// The typed magma plan checkpoint (was `magma-plan.json`).
    pub const PLAN: &str = "plan";
    /// The typed magma compliance bundle (was `magma-bundle.json`).
    pub const BUNDLE: &str = "bundle";
}

/// Postgres-backed store for the operator's durable reconcile
/// artifacts. Keyed by `(schema_name, template_name, kind)`; one row
/// per kind per template (latest-wins upsert). Cheap to clone
/// (`Arc<PgPool>`).
#[derive(Clone)]
pub struct ArtifactStore {
    pool: Arc<PgPool>,
}

impl ArtifactStore {
    /// Build an artifact store over a shared pool.
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Ensure the `pangea_meta.artifacts` table exists. Idempotent —
    /// `CREATE SCHEMA / TABLE IF NOT EXISTS`. Call once at startup
    /// alongside the other table-ensures (lock table, state tables).
    pub async fn ensure_table(&self) -> Result<()> {
        sqlx::query("CREATE SCHEMA IF NOT EXISTS pangea_meta")
            .execute(self.pool.as_ref())
            .await
            .map_err(Error::Database)?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pangea_meta.artifacts (
                schema_name   VARCHAR(63)  NOT NULL,
                template_name VARCHAR(253) NOT NULL,
                kind          VARCHAR(32)  NOT NULL,
                content_hash  CHAR(64)     NOT NULL,
                data          BYTEA        NOT NULL,
                updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                PRIMARY KEY (schema_name, template_name, kind)
            )
            "#,
        )
        .execute(self.pool.as_ref())
        .await
        .map_err(Error::Database)?;

        info!("pangea_meta.artifacts table ready");
        Ok(())
    }

    /// Atomic single-statement upsert of one artifact blob. Returns the
    /// BLAKE3 content hash of the stored bytes. The blob's identity is
    /// `(schema, template, kind)`; re-storing the same kind replaces
    /// the prior blob and stamps `updated_at`.
    async fn put(
        &self,
        schema: &str,
        template: &str,
        kind: &str,
        bytes: &[u8],
    ) -> Result<String> {
        let content_hash = blake3::hash(bytes).to_hex().to_string();

        sqlx::query(
            r#"
            INSERT INTO pangea_meta.artifacts
                (schema_name, template_name, kind, content_hash, data, updated_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (schema_name, template_name, kind) DO UPDATE SET
                content_hash = EXCLUDED.content_hash,
                data         = EXCLUDED.data,
                updated_at   = NOW()
            "#,
        )
        .bind(schema)
        .bind(template)
        .bind(kind)
        .bind(&content_hash)
        .bind(bytes)
        .execute(self.pool.as_ref())
        .await
        .map_err(Error::Database)?;

        debug!(schema, template, kind, content_hash, bytes = bytes.len(), "artifact stored");
        Ok(content_hash)
    }

    /// Fetch one artifact blob and verify its BLAKE3 integrity. Returns
    /// `Ok(None)` when no row exists for the key; `Err(StateBackend)`
    /// when the stored bytes do not hash to the recorded `content_hash`
    /// (a corrupt / torn row never feeds downstream).
    async fn get(&self, schema: &str, template: &str, kind: &str) -> Result<Option<Vec<u8>>> {
        let row: Option<(String, Vec<u8>)> = sqlx::query_as(
            r#"
            SELECT content_hash, data
            FROM pangea_meta.artifacts
            WHERE schema_name = $1 AND template_name = $2 AND kind = $3
            "#,
        )
        .bind(schema)
        .bind(template)
        .bind(kind)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(Error::Database)?;

        match row {
            None => Ok(None),
            Some((stored_hash, data)) => {
                let actual = blake3::hash(&data).to_hex().to_string();
                if actual != stored_hash {
                    return Err(Error::StateBackend(format!(
                        "artifact integrity check failed for {schema}/{template}/{kind}: \
                         stored hash {stored_hash}, computed {actual}"
                    )));
                }
                Ok(Some(data))
            }
        }
    }

    // ── Rendered-config helpers (feature-agnostic JSON) ──────────────

    /// Persist the compile→plan rendered terraform JSON. Stores the
    /// canonical `serde_json::to_vec` bytes (the plan phase reads them
    /// back through [`get_rendered_config`] and feeds
    /// `MagmaExecutor::load_config_from_value`). Returns the content
    /// hash.
    pub async fn put_rendered_config(
        &self,
        schema: &str,
        template: &str,
        value: &serde_json::Value,
    ) -> Result<String> {
        let bytes = serde_json::to_vec(value).map_err(Error::Serialization)?;
        self.put(schema, template, kind::RENDERED_CONFIG, &bytes).await
    }

    /// Read the compile→plan rendered terraform JSON. `Ok(None)` when
    /// the compile phase has not yet persisted it.
    pub async fn get_rendered_config(
        &self,
        schema: &str,
        template: &str,
    ) -> Result<Option<serde_json::Value>> {
        match self.get(schema, template, kind::RENDERED_CONFIG).await? {
            None => Ok(None),
            Some(bytes) => {
                let value = serde_json::from_slice(&bytes).map_err(Error::Serialization)?;
                Ok(Some(value))
            }
        }
    }

    /// Fetch the raw bundle blob bytes (the `serde_json::to_vec(&Bundle)`
    /// form) with integrity verification, without linking the magma
    /// `Bundle` type. Feature-agnostic so the (non-feature-gated) cycle-
    /// receipt reader can derive its `CycleArtifact` / `ApplyOutcome`
    /// from Postgres instead of a pod-local `magma-bundle.json`.
    /// `Ok(None)` when no bundle has been persisted yet.
    pub async fn get_bundle_bytes(
        &self,
        schema: &str,
        template: &str,
    ) -> Result<Option<Vec<u8>>> {
        self.get(schema, template, kind::BUNDLE).await
    }

    // ── The atomic apply op ──────────────────────────────────────────

    /// **THE ATOMIC APPLY OP.** Persist the post-apply state row and the
    /// post-apply bundle artifact inside ONE Postgres transaction —
    /// state + receipt land together-or-not-at-all.
    ///
    /// The state row is written into the OpenTofu `pg`-backend state
    /// table (`state_table` is the already-quoted
    /// `"{schema}_{template}_states".states` identifier the live system
    /// reads — see `TofuPgStateBackend::states_table`) using the SAME
    /// upsert SQL `TofuPgStateBackend::save_state` uses, so the row is
    /// byte-identical to a non-atomic write. `state_bytes` are the
    /// encoded state in the executor's `BackendShape` (UTF-8 tofu JSON
    /// for `Tofu` shape — the production default — or magma JSON for
    /// `Magma`); they are stored in the TEXT `data` column, so they
    /// MUST be valid UTF-8.
    ///
    /// On any error inside the transaction the `tx` drops without
    /// commit → Postgres rolls back both writes. This makes a
    /// half-applied reconcile (state advanced but no receipt, or
    /// receipt without state) unrepresentable.
    ///
    /// `state_table` is gated through `is_valid_identifier` on its
    /// component schema/template names by the caller (the value passed
    /// here is the already-assembled quoted identifier); `bundle_bytes`
    /// is the exact `serde_json::to_vec(&bundle)` blob.
    pub async fn put_apply_result(
        &self,
        artifact_schema: &str,
        artifact_template: &str,
        state_table: &str,
        state_name: &str,
        state_bytes: &[u8],
        bundle_bytes: &[u8],
    ) -> Result<()> {
        // State bytes land in OpenTofu's TEXT `data` column.
        let state_text = std::str::from_utf8(state_bytes).map_err(|e| {
            Error::StateBackend(format!("apply state bytes are not valid UTF-8: {e}"))
        })?;
        let bundle_hash = blake3::hash(bundle_bytes).to_hex().to_string();

        let mut tx: Transaction<'_, Postgres> =
            self.pool.begin().await.map_err(Error::Database)?;

        // (a) upsert the state row into the live OpenTofu states table.
        //     Identical SQL to TofuPgStateBackend::save_state so the
        //     atomic path and the non-atomic path produce the same row.
        let state_sql = format!(
            "INSERT INTO {state_table} (name, data) VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data"
        );
        sqlx::query(&state_sql)
            .bind(state_name)
            .bind(state_text)
            .execute(&mut *tx)
            .await
            .map_err(Error::Database)?;

        // (b) upsert the bundle artifact in the SAME transaction.
        sqlx::query(
            r#"
            INSERT INTO pangea_meta.artifacts
                (schema_name, template_name, kind, content_hash, data, updated_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (schema_name, template_name, kind) DO UPDATE SET
                content_hash = EXCLUDED.content_hash,
                data         = EXCLUDED.data,
                updated_at   = NOW()
            "#,
        )
        .bind(artifact_schema)
        .bind(artifact_template)
        .bind(kind::BUNDLE)
        .bind(&bundle_hash)
        .bind(bundle_bytes)
        .execute(&mut *tx)
        .await
        .map_err(Error::Database)?;

        tx.commit().await.map_err(Error::Database)?;

        info!(
            artifact_schema,
            artifact_template,
            state_table,
            state_name,
            bundle_hash,
            "apply result committed atomically (state + bundle)"
        );
        Ok(())
    }

    /// Assemble + validate the live OpenTofu state-table identifier for
    /// a `(schema, template)` pair — the quoted
    /// `"{schema}_{template}_states".states` form `TofuPgStateBackend`
    /// reads. Gates the component identifiers through
    /// [`is_valid_identifier`] (rejecting the leading/embedded chars an
    /// injection would need) and defensively doubles any embedded quote.
    ///
    /// k8s object names use `[a-z0-9.-]`; hyphens and dots are NOT valid
    /// SQL identifier characters, which is exactly why the assembled
    /// schema name is double-quoted. `is_valid_identifier` would reject
    /// those names, so we validate the SANITIZED token (hyphens/dots →
    /// underscores) purely as an injection guard, then quote the
    /// original.
    pub fn live_state_table(schema_name: &str, template_name: &str) -> Result<String> {
        // Injection guard on a sanitized projection: a name that still
        // fails after `-`/`.` → `_` carries a character no k8s name can
        // (quote, semicolon, whitespace, control) — refuse it.
        let sanitize = |s: &str| s.replace(['-', '.'], "_");
        if !is_valid_identifier(&sanitize(schema_name))
            || !is_valid_identifier(&sanitize(template_name))
        {
            return Err(Error::Config(format!(
                "invalid schema/template identifier for state table: {schema_name}/{template_name}"
            )));
        }
        let schema = format!("{schema_name}_{template_name}_states");
        let quoted = schema.replace('"', "\"\"");
        Ok(format!("\"{quoted}\".states"))
    }
}

// ── Typed magma helpers (feature-gated) ──────────────────────────────
//
// These reference the magma crates (magma_types::Plan,
// magma_bundle::Bundle) which are only linked under `executor_magma`,
// so the typed surface is gated. The raw-bytes base above stays
// feature-agnostic.

#[cfg(feature = "executor_magma")]
impl ArtifactStore {
    /// Persist the typed magma plan checkpoint (was `magma-plan.json`).
    pub async fn put_plan(
        &self,
        schema: &str,
        template: &str,
        plan: &magma_types::Plan,
    ) -> Result<String> {
        let bytes = serde_json::to_vec(plan).map_err(Error::Serialization)?;
        self.put(schema, template, kind::PLAN, &bytes).await
    }

    /// Read the typed magma plan checkpoint. `Ok(None)` when the plan
    /// phase has not yet persisted it.
    pub async fn get_plan(
        &self,
        schema: &str,
        template: &str,
    ) -> Result<Option<magma_types::Plan>> {
        match self.get(schema, template, kind::PLAN).await? {
            None => Ok(None),
            Some(bytes) => {
                let plan = serde_json::from_slice(&bytes).map_err(Error::Serialization)?;
                Ok(Some(plan))
            }
        }
    }

    /// Persist the typed magma compliance bundle (was
    /// `magma-bundle.json`).
    pub async fn put_bundle(
        &self,
        schema: &str,
        template: &str,
        bundle: &magma_bundle::Bundle,
    ) -> Result<String> {
        let bytes = serde_json::to_vec(bundle).map_err(Error::Serialization)?;
        self.put(schema, template, kind::BUNDLE, &bytes).await
    }

    /// Read the typed magma compliance bundle. `Ok(None)` when no
    /// bundle has been persisted (no plan/apply has run yet).
    pub async fn get_bundle(
        &self,
        schema: &str,
        template: &str,
    ) -> Result<Option<magma_bundle::Bundle>> {
        match self.get(schema, template, kind::BUNDLE).await? {
            None => Ok(None),
            Some(bytes) => {
                let bundle = serde_json::from_slice(&bytes).map_err(Error::Serialization)?;
                Ok(Some(bundle))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── live_state_table — identifier assembly + injection guard ─────

    #[test]
    fn live_state_table_matches_opentofu_pg_layout() {
        // Same layout TofuPgStateBackend::states_table produces — the
        // atomic path must address the exact same live row.
        assert_eq!(
            ArtifactStore::live_state_table("pangea_cloudflare-pleme", "cloudflare-pleme").unwrap(),
            "\"pangea_cloudflare-pleme_cloudflare-pleme_states\".states"
        );
        assert_eq!(
            ArtifactStore::live_state_table("pangea_rio-infra", "rio-zot-cloudflare-tunnel")
                .unwrap(),
            "\"pangea_rio-infra_rio-zot-cloudflare-tunnel_states\".states"
        );
    }

    #[test]
    fn live_state_table_allows_dotted_k8s_names() {
        // k8s object names can contain dots; they sanitize to `_` for
        // the injection check and are quoted in the identifier.
        let t = ArtifactStore::live_state_table("pangea_a.b", "c.d").unwrap();
        assert_eq!(t, "\"pangea_a.b_c.d_states\".states");
    }

    #[test]
    fn live_state_table_rejects_injection_attempts() {
        // A name carrying a character no k8s name can (quote / semicolon
        // / whitespace) is refused — it would break out of the quoted
        // identifier.
        assert!(ArtifactStore::live_state_table("a\"b", "t").is_err());
        assert!(ArtifactStore::live_state_table("a; DROP TABLE x", "t").is_err());
        assert!(ArtifactStore::live_state_table("a b", "t").is_err());
        assert!(ArtifactStore::live_state_table("a", "t' OR '1'='1").is_err());
    }

    // ── content-hash integrity (pure helper) ─────────────────────────

    #[test]
    fn content_hash_is_blake3_hex_64() {
        let h = blake3::hash(b"some artifact bytes").to_hex().to_string();
        assert_eq!(h.len(), 64, "BLAKE3 hex is 64 chars");
        // Deterministic — the same bytes always hash the same way, so a
        // corrupt row is detectable by recompute-and-compare.
        let h2 = blake3::hash(b"some artifact bytes").to_hex().to_string();
        assert_eq!(h, h2);
        let h3 = blake3::hash(b"some artifact bytez").to_hex().to_string();
        assert_ne!(h, h3, "a single-byte corruption changes the hash");
    }

    #[test]
    fn integrity_mismatch_is_detectable() {
        // The exact check `get` performs: recompute on read, compare to
        // the stored hash. A torn/corrupt blob fails the comparison.
        let original = b"plan bytes";
        let stored_hash = blake3::hash(original).to_hex().to_string();
        let corrupted = b"plan bytez";
        let recomputed = blake3::hash(corrupted).to_hex().to_string();
        assert_ne!(recomputed, stored_hash, "corruption must be caught");
    }

    #[test]
    fn kind_discriminators_are_distinct() {
        // The three durable artifacts occupy distinct rows per template
        // (PK includes `kind`); the discriminators must not collide.
        let kinds = [kind::RENDERED_CONFIG, kind::PLAN, kind::BUNDLE];
        for (i, a) in kinds.iter().enumerate() {
            for b in &kinds[i + 1..] {
                assert_ne!(a, b, "artifact kinds must be distinct");
            }
            assert!(a.len() <= 32, "kind must fit VARCHAR(32): {a}");
        }
    }

    // NOTE: round-trip / atomicity tests against a real Postgres
    // (put_rendered_config/get_rendered_config, put_plan/get_plan,
    // put_bundle/get_bundle, and the put_apply_result transaction
    // rolling back both writes on error) are covered by the operator's
    // PG integration test harness — they require a live `PgPool`, which
    // these unit tests intentionally do not stand up (mirrors the
    // sqlx-without-PG testability boundary documented in
    // backend/state_backend.rs). The pure helpers above pin the
    // identifier-assembly + content-hash-integrity logic that the
    // transaction relies on.
}
