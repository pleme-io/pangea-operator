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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sqlx::{PgPool, Postgres, Transaction};
use tracing::{debug, info};
// `warn` is imported under the SAME cfg as its only call site (the
// `executor_magma` block below), not unconditionally. The crate is
// `#![deny(unused_imports)]`, and crd-drift.yml builds with
// `--no-default-features` — which drops `executor_magma`, leaving `warn`
// unused and turning a lint into a hard build failure. That kept crd-drift
// red on every commit from at least 2026-07-30 to 2026-08-01 while the
// default-features builds stayed green, so nothing on the main path
// disagreed.
//
// Tying the import to the cfg its use lives under makes the two unable to
// drift: adding a `warn!` outside the block fails to compile rather than
// silently re-breaking the no-default-features build.
#[cfg(feature = "executor_magma")]
use tracing::warn;

use crate::backend::schema::{tofu_state_schema_ident, tofu_state_table_ident};
use crate::error::{Error, Result};

/// Artifact kind discriminator stored in the `kind` column. Four
/// durable artifacts the magma reconcile path produces.
pub mod kind {
    /// The compile→plan rendered terraform JSON (was `main.tf.json`).
    pub const RENDERED_CONFIG: &str = "rendered_config";
    /// The typed magma plan checkpoint (was `magma-plan.json`).
    pub const PLAN: &str = "plan";
    /// The typed magma compliance bundle (was `magma-bundle.json`).
    pub const BUNDLE: &str = "bundle";
    /// The resumable apply position (`magma_apply::cursor::ApplyCursor`),
    /// per MAGMA-OPERATOR-BACKEND.md §II-ter / M0.16.
    ///
    /// This row is what makes apply progress **monotonic across reconciles**:
    /// a cycle that runs out of quantum (or a pod that dies mid-apply) leaves
    /// the frontier here, and the next cycle resumes from it instead of
    /// re-running the plan from the beginning and re-spending the provider's
    /// rate-limit budget on work already done.
    ///
    /// **No delete verb is needed, by construction.** An `ApplyCursor` is
    /// bound to its plan's BLAKE3 `PlanId`, and `ApplyCursor::resume` returns
    /// `None` on mismatch — so a cursor left behind by a finished plan is
    /// self-invalidating against the *next* plan, and against a re-apply of
    /// the *same* plan it correctly reports everything already done. A stale
    /// row is inert rather than dangerous, so we do not add a delete path
    /// whose failure modes would be worse than the state it removes.
    pub const APPLY_CURSOR: &str = "apply_cursor";
}

/// Postgres-backed store for the operator's durable reconcile
/// artifacts. Keyed by `(schema_name, template_name, kind)`; one row
/// per kind per template (latest-wins upsert). Cheap to clone
/// (`Arc<PgPool>`).
#[derive(Clone)]
pub struct ArtifactStore {
    pool: Arc<PgPool>,
    /// Self-healing flag: set once `ensure_table` succeeds. Every public
    /// op calls [`ArtifactStore::ensure_ready`] first — until the flag is
    /// true, each op re-runs the cheap `CREATE … IF NOT EXISTS` ensure.
    /// This is the `MissingArtifactTable → ensure` Absolute remediation:
    /// a startup DB blip that fails the one-shot ensure in `main.rs` no
    /// longer leaves the magma path armed-but-tableless until a pod
    /// restart — the next op converges. Steady-state cost is ~zero (one
    /// relaxed atomic load once the flag is true). `Arc`'d so it is
    /// shared across the cheap `Clone`s of the store.
    ensured: Arc<AtomicBool>,
}

impl ArtifactStore {
    /// Build an artifact store over a shared pool.
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self {
            pool,
            ensured: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Self-heal the durable-artifact table on the first op that needs
    /// it (and on every op until it succeeds). Cheap no-op once the
    /// `ensured` flag is set — a single relaxed atomic load. Called at
    /// the top of every public op so a startup-time ensure failure (DB
    /// not yet reachable) converges on the next tick rather than waiting
    /// for a pod restart. Per the ★★ MAGMA-NATIVE EXECUTION directive:
    /// converge, don't gate the magma path off (no disk regression).
    async fn ensure_ready(&self) -> Result<()> {
        if self.ensured.load(Ordering::Relaxed) {
            return Ok(());
        }
        // `ensure_table` sets the flag on success; a failure here leaves
        // it false so the next op retries (converge, don't gate off).
        self.ensure_table().await
    }

    /// Ensure the `pangea_meta.artifacts` table exists. Idempotent —
    /// `CREATE SCHEMA / TABLE IF NOT EXISTS`. Call once at startup
    /// alongside the other table-ensures (lock table, state tables);
    /// also self-healed lazily by [`ArtifactStore::ensure_ready`] on
    /// every op until it first succeeds.
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

        // `source_revision` — the source revision (git HEAD SHA or the
        // `cm:` content hash of an inline/configMap source) the stored
        // artifact was PRODUCED AT. Nullable + additive: a legacy row
        // written before this column existed carries NULL, which the
        // reuse gate treats as "revision unknown → stale", forcing
        // exactly one recompile (the same converge-by-one-recompile
        // discipline as the legacy-CR `compiled_revision == None` case).
        // This is what lets `compiled_config_available` reject a cached
        // `rendered_config` produced from an OLDER revision — the fix for
        // the git-sourced stale-render class where a source change was
        // stamped onto `status.compiledRevision` but the operator kept
        // serving the render from the prior revision. Idempotent
        // `ADD COLUMN IF NOT EXISTS` at the same table-ensure site.
        sqlx::query(
            "ALTER TABLE pangea_meta.artifacts ADD COLUMN IF NOT EXISTS source_revision TEXT",
        )
        .execute(self.pool.as_ref())
        .await
        .map_err(Error::Database)?;

        // Prime the self-healing flag so the startup ensure (main.rs)
        // primes it too — subsequent ops skip the redundant re-ensure.
        self.ensured.store(true, Ordering::Relaxed);
        info!("pangea_meta.artifacts table ready");
        Ok(())
    }

    /// Atomic single-statement upsert of one artifact blob. Returns the
    /// BLAKE3 content hash of the stored bytes. The blob's identity is
    /// `(schema, template, kind)`; re-storing the same kind replaces
    /// the prior blob and stamps `updated_at`.
    ///
    /// `source_revision` records the source revision (git HEAD SHA or the
    /// `cm:` content hash of an inline/configMap source) this artifact was
    /// produced at, so the reuse gate can compare like-for-like against the
    /// revision the operator now believes it should be at
    /// (`status.compiledRevision`). `None` leaves the column NULL — treated
    /// as "revision unknown → stale" by the reuse gate.
    async fn put(
        &self,
        schema: &str,
        template: &str,
        kind: &str,
        bytes: &[u8],
        source_revision: Option<&str>,
    ) -> Result<String> {
        self.ensure_ready().await?;
        let content_hash = blake3::hash(bytes).to_hex().to_string();

        sqlx::query(
            r#"
            INSERT INTO pangea_meta.artifacts
                (schema_name, template_name, kind, content_hash, data, source_revision, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (schema_name, template_name, kind) DO UPDATE SET
                content_hash    = EXCLUDED.content_hash,
                data            = EXCLUDED.data,
                source_revision = EXCLUDED.source_revision,
                updated_at      = NOW()
            "#,
        )
        .bind(schema)
        .bind(template)
        .bind(kind)
        .bind(&content_hash)
        .bind(bytes)
        .bind(source_revision)
        .execute(self.pool.as_ref())
        .await
        .map_err(Error::Database)?;

        debug!(
            schema,
            template,
            kind,
            content_hash,
            source_revision = source_revision.unwrap_or("(none)"),
            bytes = bytes.len(),
            "artifact stored"
        );
        Ok(content_hash)
    }

    /// Fetch one artifact blob and verify its BLAKE3 integrity. Returns
    /// `Ok(None)` when no row exists for the key; `Err(StateBackend)`
    /// when the stored bytes do not hash to the recorded `content_hash`
    /// (a corrupt / torn row never feeds downstream).
    async fn get(&self, schema: &str, template: &str, kind: &str) -> Result<Option<Vec<u8>>> {
        self.ensure_ready().await?;
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
    ///
    /// `source_revision` is the revision this render was produced FROM —
    /// the git HEAD SHA on `status.compiledRevision` (git sources) or the
    /// `cm:` content hash (inline/configMap sources). It is recorded so a
    /// later phase guard can decide whether the cached render still
    /// matches the revision the operator now believes it should be at,
    /// and recompile on mismatch. `None` leaves the column NULL (treated
    /// as stale by the reuse gate → one recompile).
    pub async fn put_rendered_config(
        &self,
        schema: &str,
        template: &str,
        value: &serde_json::Value,
        source_revision: Option<&str>,
    ) -> Result<String> {
        let bytes = serde_json::to_vec(value).map_err(Error::Serialization)?;
        self.put(
            schema,
            template,
            kind::RENDERED_CONFIG,
            &bytes,
            source_revision,
        )
        .await
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

    /// The `source_revision` recorded alongside the stored rendered
    /// config (the git HEAD SHA / `cm:` content hash it was produced
    /// from), or `None` when there is no rendered-config row yet OR a
    /// legacy row (written before the `source_revision` column existed)
    /// carries NULL. The reuse gate treats both `None` cases as "not a
    /// match for the current revision" → recompile once. This is the
    /// revision half of the reuse check; the bytes come from
    /// [`get_rendered_config`].
    pub async fn get_rendered_config_revision(
        &self,
        schema: &str,
        template: &str,
    ) -> Result<Option<String>> {
        self.ensure_ready().await?;
        let row: Option<(Option<String>,)> = sqlx::query_as(
            r#"
            SELECT source_revision
            FROM pangea_meta.artifacts
            WHERE schema_name = $1 AND template_name = $2 AND kind = $3
            "#,
        )
        .bind(schema)
        .bind(template)
        .bind(kind::RENDERED_CONFIG)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(Error::Database)?;

        Ok(row.and_then(|(rev,)| rev))
    }

    /// Fetch the raw bundle blob bytes (the `serde_json::to_vec(&Bundle)`
    /// form) with integrity verification, without linking the magma
    /// `Bundle` type. Feature-agnostic so the (non-feature-gated) cycle-
    /// receipt reader can derive its `CycleArtifact` / `ApplyOutcome`
    /// from Postgres instead of a pod-local `magma-bundle.json`.
    /// `Ok(None)` when no bundle has been persisted yet.
    pub async fn get_bundle_bytes(&self, schema: &str, template: &str) -> Result<Option<Vec<u8>>> {
        self.get(schema, template, kind::BUNDLE).await
    }

    /// How many of the current plan's changes the resumable apply
    /// engine has completed and checkpointed — `ApplyCursor::len()`.
    ///
    /// This is the operator's **progress term**: monotonic within a plan
    /// (the cursor's mutators are additive-only) and durable across pod
    /// restarts, which is exactly what a "is this template alive or
    /// wedged?" judgement needs and what a wall clock cannot supply.
    /// Consumed by the post-reconcile progress sample that maintains
    /// `status.applyCursorAdvancedAt`.
    ///
    /// Shares `get_apply_cursor`'s failure posture: an absent or
    /// undecodable row is `Ok(None)` — "no reading", never an error. A
    /// progress *sensor* that failed loudly would take a reconcile down
    /// over a cache row; the honest answer is that we have no reading,
    /// and the caller falls back to the wall clock.
    ///
    /// Feature-agnostic like `get_bundle_bytes`, for the same reason:
    /// its two callers (the cycle-receipt writer and the reactive-policy
    /// stage) are not feature-gated. Without `executor_magma` there is
    /// no cursor to read, and `None` states that honestly rather than
    /// fabricating a zero.
    pub async fn apply_cursor_len(&self, schema: &str, template: &str) -> Result<Option<u64>> {
        #[cfg(feature = "executor_magma")]
        {
            Ok(self
                .get_apply_cursor(schema, template)
                .await?
                .map(|c| c.len() as u64))
        }
        #[cfg(not(feature = "executor_magma"))]
        {
            let _ = (schema, template);
            Ok(None)
        }
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
        self.ensure_ready().await?;
        // State bytes land in OpenTofu's TEXT `data` column.
        let state_text = std::str::from_utf8(state_bytes).map_err(|e| {
            Error::StateBackend(format!("apply state bytes are not valid UTF-8: {e}"))
        })?;
        let bundle_hash = blake3::hash(bundle_bytes).to_hex().to_string();

        // Make the magma apply self-sufficient with zero tofu: under
        // `PANGEA_FORBID_TOFU=true` a brand-new template's `.states`
        // table is NEVER created (tofu's pg backend, which would
        // `CREATE SCHEMA … states`, never runs), so the INSERT below
        // would fail with relation-does-not-exist. Idempotently create
        // the schema + `.states` table in the EXACT OpenTofu pg-backend
        // layout before the upsert, so a fresh template's first magma
        // apply succeeds and a later tofu read still finds its rows.
        self.ensure_tofu_states_table(artifact_schema, artifact_template)
            .await?;

        let mut tx: Transaction<'_, Postgres> = self.pool.begin().await.map_err(Error::Database)?;

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

    /// The live OpenTofu state-table identifier for a
    /// `(schema, template)` pair — the quoted
    /// `"{schema}_{template}_states".states` form `TofuPgStateBackend`
    /// reads.
    ///
    /// Assembly + the injection guard are
    /// [`tofu_state_table_ident`](crate::backend::schema::tofu_state_table_ident),
    /// which `TofuPgStateBackend` also calls. The two used to derive this
    /// identifier independently and only THIS side ran the guard; see
    /// that function for the unification and the k8s-name/quoting
    /// rationale.
    pub fn live_state_table(schema_name: &str, template_name: &str) -> Result<String> {
        tofu_state_table_ident(schema_name, template_name)
    }

    /// Just the quoted OpenTofu pg-backend SCHEMA identifier for a
    /// `(schema, template)` pair — the `"{schema}_{template}_states"`
    /// form (no `.states` table suffix). Used by
    /// [`ensure_tofu_states_table`](Self::ensure_tofu_states_table) to
    /// `CREATE SCHEMA` and to qualify the `states` table.
    ///
    /// Assembly + guard are
    /// [`tofu_state_schema_ident`](crate::backend::schema::tofu_state_schema_ident).
    pub fn live_state_schema(schema_name: &str, template_name: &str) -> Result<String> {
        // The assembly + guard live in `backend::schema`, which is also
        // what `TofuPgStateBackend` calls — the two used to derive this
        // identifier independently. See `tofu_state_schema_ident`.
        tofu_state_schema_ident(schema_name, template_name)
    }

    /// Idempotently create the OpenTofu `pg`-backend schema + `states`
    /// table for a `(schema, template)` pair, in the EXACT layout
    /// OpenTofu's pg backend uses, so a brand-new template's first
    /// **magma** apply is self-sufficient with zero tofu.
    ///
    /// Under `PANGEA_FORBID_TOFU=true`, tofu's pg backend — which would
    /// otherwise `CREATE SCHEMA … states` on `tofu init` — never runs,
    /// so a never-tofu-initialized template has no `.states` table and
    /// [`put_apply_result`]'s state upsert fails with
    /// relation-does-not-exist. This creates it first.
    ///
    /// The DDL mirrors OpenTofu/Terraform's `pg` backend
    /// (`backend/remote-state/pg`):
    /// ```sql
    /// CREATE SCHEMA IF NOT EXISTS "<schema>";
    /// CREATE TABLE IF NOT EXISTS "<schema>".states (
    ///   id   BIGSERIAL PRIMARY KEY,
    ///   name TEXT      UNIQUE,
    ///   data TEXT
    /// );
    /// ```
    /// `name` is `UNIQUE` so the `ON CONFLICT (name)` upsert in
    /// [`put_apply_result`] / `TofuPgStateBackend::save_state` has the
    /// arbiter constraint it requires. `id` is `BIGSERIAL` (int8 / bigint),
    /// NOT `SERIAL` (int4): `magma_backend::Backend::read_state` selects the
    /// `id` column first and decodes it as `i64`, so a `SERIAL` (int4) column
    /// makes read_state fail with "Rust type `i64` (as SQL type `INT8`) is not
    /// compatible with SQL type `INT4`" the moment the table exists and is
    /// read. `bigint` is also what the `RETURNING id` / `id bigint` readers in
    /// `tofu_pg_state_backend.rs` expect, so a later tofu read still works.
    /// (Fixed 2026-06-20: the original `SERIAL` blocked every magma read once
    /// `ensure_state_table` started provisioning the table on the read path.)
    ///
    /// The trailing idempotent `ALTER COLUMN id TYPE BIGINT` widens any table
    /// that a prior build already created as `SERIAL`/int4 — `CREATE TABLE IF
    /// NOT EXISTS` alone would leave the old int4 column in place. int4→int8 is
    /// a lossless widening and the attached sequence stays valid, so the ALTER
    /// is a safe no-op once the column is already bigint.
    ///
    /// Identifiers are gated through
    /// [`live_state_schema`](Self::live_state_schema) →
    /// `schema::is_valid_identifier` (the injection guard), so no caller
    /// input can break out of the quoted schema identifier.
    pub async fn ensure_tofu_states_table(
        &self,
        schema_name: &str,
        template_name: &str,
    ) -> Result<()> {
        let schema = Self::live_state_schema(schema_name, template_name)?;

        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
        sqlx::query(&create_schema)
            .execute(self.pool.as_ref())
            .await
            .map_err(Error::Database)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {schema}.states ( \
                id   BIGSERIAL PRIMARY KEY, \
                name TEXT      UNIQUE, \
                data TEXT \
             )"
        );
        sqlx::query(&create_table)
            .execute(self.pool.as_ref())
            .await
            .map_err(Error::Database)?;

        // Idempotently widen a pre-existing int4 `id` (created by a prior
        // build's `SERIAL` DDL) to int8 so `magma_backend::read_state`'s i64
        // decode matches. No-op once already bigint; lossless widening.
        let widen_id = format!("ALTER TABLE {schema}.states ALTER COLUMN id TYPE BIGINT");
        sqlx::query(&widen_id)
            .execute(self.pool.as_ref())
            .await
            .map_err(Error::Database)?;

        debug!(schema = %schema, "OpenTofu pg-backend states table ensured (magma self-sufficient)");
        Ok(())
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
    ///
    /// `source_revision` records the source revision the plan was
    /// COMPUTED FROM — the same value the `rendered_config` it was
    /// derived from carries (the plan phase reads the render via
    /// `get_rendered_config`, then plans against it, so a fresh plan is
    /// always same-revision as the render currently in the store). It is
    /// recorded so the apply-phase reuse gate can reject a plan produced
    /// from an OLDER revision than the render now in the store — the
    /// sibling of the `rendered_config` stale-render fix. A cached plan
    /// whose `source_revision` no longer matches the render's is stale:
    /// apply would re-execute a noOp plan and a source change (e.g.
    /// org.yaml) would silently never converge. `None` leaves the column
    /// NULL — treated as "revision unknown → stale" by the reuse gate,
    /// forcing exactly one plan recompute.
    pub async fn put_plan(
        &self,
        schema: &str,
        template: &str,
        plan: &magma_types::Plan,
        source_revision: Option<&str>,
    ) -> Result<String> {
        let bytes = serde_json::to_vec(plan).map_err(Error::Serialization)?;
        self.put(schema, template, kind::PLAN, &bytes, source_revision)
            .await
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

    /// Persist the resumable apply position. Idempotent upsert — the engine
    /// may checkpoint the same cursor twice if a cycle ends immediately after
    /// a node, which `put`'s `ON CONFLICT DO UPDATE` already absorbs.
    ///
    /// `ApplyCursor` carries `#[serde(try_from/into = "ApplyCursorWire")]`, so
    /// the wire form is validated on the way back in: a duplicate entry is a
    /// `CursorError` at parse time rather than a cursor that silently
    /// double-counts. That is the *parse-time-rejected* half of the tier
    /// stated in MAGMA-OPERATOR-BACKEND.md §II-ter — worth naming, because the
    /// in-crate half (additive-only mutators) does not survive a DB roundtrip
    /// on its own.
    pub async fn put_apply_cursor(
        &self,
        schema: &str,
        template: &str,
        cursor: &magma_apply::cursor::ApplyCursor,
    ) -> Result<String> {
        let bytes = serde_json::to_vec(cursor).map_err(Error::Serialization)?;
        self.put(schema, template, kind::APPLY_CURSOR, &bytes, None)
            .await
    }

    /// Read the resumable apply position. `Ok(None)` when no apply has
    /// yielded yet.
    ///
    /// A decode failure is deliberately **not** fatal here: the caller treats
    /// `Ok(None)` and a torn cursor the same way — start the plan from the
    /// beginning. That is safe precisely because re-application is
    /// idempotent and the cursor's skip predicate is safety-monotone (it can
    /// only cause more re-application, never less), so the worst outcome of
    /// losing a cursor is repeated work, never skipped work. Refusing to
    /// apply at all because a *cache* row was corrupt would be the strictly
    /// worse failure.
    pub async fn get_apply_cursor(
        &self,
        schema: &str,
        template: &str,
    ) -> Result<Option<magma_apply::cursor::ApplyCursor>> {
        match self.get(schema, template, kind::APPLY_CURSOR).await? {
            None => Ok(None),
            Some(bytes) => match serde_json::from_slice(&bytes) {
                Ok(cursor) => Ok(Some(cursor)),
                Err(e) => {
                    warn!(
                        schema, template,
                        error = %e,
                        "apply cursor undecodable; starting this plan from the beginning \
                         (safe: re-application is idempotent and the skip predicate is \
                         safety-monotone)"
                    );
                    Ok(None)
                }
            },
        }
    }

    /// The `source_revision` recorded alongside the stored plan (the
    /// revision it was computed from), or `None` when there is no plan
    /// row yet OR a legacy row (written before plan revisions were
    /// recorded) carries NULL. The apply-phase reuse gate treats both
    /// `None` cases as "not a match for the current revision" →
    /// recompute once. This is the revision half of the plan-reuse
    /// check; the bytes come from [`get_plan`]. Mirrors
    /// [`get_rendered_config_revision`].
    pub async fn get_plan_revision(&self, schema: &str, template: &str) -> Result<Option<String>> {
        self.ensure_ready().await?;
        let row: Option<(Option<String>,)> = sqlx::query_as(
            r#"
            SELECT source_revision
            FROM pangea_meta.artifacts
            WHERE schema_name = $1 AND template_name = $2 AND kind = $3
            "#,
        )
        .bind(schema)
        .bind(template)
        .bind(kind::PLAN)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(Error::Database)?;

        Ok(row.and_then(|(rev,)| rev))
    }

    /// Persist the typed magma compliance bundle (was
    /// `magma-bundle.json`).
    ///
    /// `source_revision` records the revision the bundle's plan was
    /// computed from, kept uniform with [`put_plan`] so the plan and its
    /// companion bundle carry the same revision stamp (the "Keys/revision
    /// MUST match" invariant extended to plan + bundle). The bundle is
    /// not itself a reuse-gated artifact, but stamping it keeps the two
    /// compile→plan handoff artifacts revision-coherent. `None` leaves
    /// the column NULL.
    pub async fn put_bundle(
        &self,
        schema: &str,
        template: &str,
        bundle: &magma_bundle::Bundle,
        source_revision: Option<&str>,
    ) -> Result<String> {
        let bytes = serde_json::to_vec(bundle).map_err(Error::Serialization)?;
        self.put(schema, template, kind::BUNDLE, &bytes, source_revision)
            .await
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

    // ── live_state_schema / ensure_tofu_states_table identifier assembly ──

    #[test]
    fn live_state_schema_is_table_minus_dot_states() {
        // The schema identifier is exactly the table identifier without
        // the `.states` suffix — `ensure_tofu_states_table` qualifies
        // `<schema>.states`, so they MUST agree byte-for-byte.
        let schema =
            ArtifactStore::live_state_schema("pangea_cloudflare-pleme", "cloudflare-pleme")
                .unwrap();
        assert_eq!(
            schema,
            "\"pangea_cloudflare-pleme_cloudflare-pleme_states\""
        );
        let table =
            ArtifactStore::live_state_table("pangea_cloudflare-pleme", "cloudflare-pleme").unwrap();
        assert_eq!(table, format!("{schema}.states"));
    }

    #[test]
    fn live_state_schema_allows_dotted_k8s_names() {
        let s = ArtifactStore::live_state_schema("pangea_a.b", "c.d").unwrap();
        assert_eq!(s, "\"pangea_a.b_c.d_states\"");
    }

    #[test]
    fn live_state_schema_rejects_injection_attempts() {
        // ensure_tofu_states_table interpolates this into CREATE SCHEMA /
        // CREATE TABLE; an identifier carrying a quote/semicolon/space
        // that survives the `-`/`.` → `_` sanitization is refused.
        assert!(ArtifactStore::live_state_schema("a\"b", "t").is_err());
        assert!(ArtifactStore::live_state_schema("a; DROP TABLE x", "t").is_err());
        assert!(ArtifactStore::live_state_schema("a b", "t").is_err());
        assert!(ArtifactStore::live_state_schema("a", "t' OR '1'='1").is_err());
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
