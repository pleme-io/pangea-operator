//! Typed `StateBackend` trait — abstraction over OpenTofu state
//! persistence (PostgreSQL today, possibly other backends later).
//!
//! Why this exists:
//!
//!   1. **Testability without sqlx integration.** sqlx::query! macros
//!      compile-check against a real PG, which means controller-level
//!      tests need a real DB just to run. This trait lets future tests
//!      inject `InMemoryStateBackend` and exercise the controller's
//!      state-handling paths without testcontainers.
//!
//!   2. **Future swap.** Today PG is the only backend. A future
//!      variant could write state to S3, etcd, or the K8s ConfigMap
//!      tier. The trait is the seam.
//!
//!   3. **Operator restart resilience tests.** Property tests around
//!      "what happens when state save races a reconcile cancellation"
//!      need to inject failures at write time — only feasible with a
//!      mock backend.
//!
//! The real implementation `PostgresStateBackend` lives in
//! `backend/state.rs` (delegates to the existing `StateStore` struct
//! 1:1). The unit-test mock `InMemoryStateBackend` lives here.
//!
//! Symmetric to `executor::iac_executor::IacExecutor` (the trait
//! pattern for tofu CLI calls). Together, these two seams make the
//! controller fully unit-testable.

use crate::backend::state::{StateEntry, StateStore, TerraformState};
use crate::error::Result;

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Async trait covering every state-persistence operation the
/// operator's controllers invoke. Object-safe by design.
///
/// Idempotency contract: implementations MUST treat `save_state`
/// with the same key as upsert. The PG impl uses `INSERT ... ON
/// CONFLICT DO UPDATE`; the in-memory impl replaces the entry.
#[async_trait]
pub trait StateBackend: Send + Sync + 'static {
    /// Get the latest state entry for a template/state-name pair.
    /// Returns `None` if no state has been saved yet.
    async fn get_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<StateEntry>>;

    /// Get + parse the latest state as a `TerraformState`. Returns
    /// `None` if no state exists OR if the stored bytes don't
    /// deserialize.
    async fn get_parsed_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<TerraformState>>;

    /// Upsert state bytes. Returns the row ID (PG) or a synthetic ID
    /// (in-memory). Caller treats the ID as opaque.
    async fn save_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
        data: &[u8],
    ) -> Result<i64>;

    /// Delete a state entry. Returns true iff a row was actually
    /// removed (false if it didn't exist).
    async fn delete_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<bool>;

    /// List all state entries for a template. Newest first.
    async fn list_states(&self, schema_name: &str, template_name: &str) -> Result<Vec<StateEntry>>;
}

/// `StateBackend` implementation backed by `StateStore` (PG via sqlx).
/// Thin delegating wrapper; each method calls the inherent method
/// 1:1 with no behavior change.
pub struct PostgresStateBackend {
    store: Arc<StateStore>,
}

impl PostgresStateBackend {
    pub fn new(store: Arc<StateStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl StateBackend for PostgresStateBackend {
    async fn get_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<StateEntry>> {
        self.store
            .get_state(schema_name, template_name, state_name)
            .await
    }

    async fn get_parsed_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<TerraformState>> {
        self.store
            .get_parsed_state(schema_name, template_name, state_name)
            .await
    }

    async fn save_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
        data: &[u8],
    ) -> Result<i64> {
        self.store
            .save_state(schema_name, template_name, state_name, data)
            .await
    }

    async fn delete_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<bool> {
        self.store
            .delete_state(schema_name, template_name, state_name)
            .await
    }

    async fn list_states(&self, schema_name: &str, template_name: &str) -> Result<Vec<StateEntry>> {
        self.store.list_states(schema_name, template_name).await
    }
}

/// In-memory `StateBackend` for unit tests. Stores entries in a
/// nested `BTreeMap` keyed by `(schema_name, template_name,
/// state_name)`. Thread-safe via `Mutex` (tests are async but rarely
/// concurrent within a single test case).
///
/// Lifecycle:
///   * Construct empty: `InMemoryStateBackend::new()`
///   * Inject seed data via `save_state` calls
///   * Inspect via `recorded_keys()` for assertions
pub struct InMemoryStateBackend {
    inner: Mutex<InnerState>,
}

struct InnerState {
    /// (schema, template, state_name) -> StateEntry
    entries: BTreeMap<(String, String, String), StateEntry>,
    /// Monotonic ID generator for synthesized row IDs.
    next_id: i64,
}

impl InMemoryStateBackend {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(InnerState {
                entries: BTreeMap::new(),
                next_id: 1,
            }),
        }
    }

    /// Snapshot of every populated key. Use in test assertions:
    /// `assert_eq!(backend.recorded_keys().len(), 3);`
    pub fn recorded_keys(&self) -> Vec<(String, String, String)> {
        self.inner.lock().unwrap().entries.keys().cloned().collect()
    }
}

impl Default for InMemoryStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateBackend for InMemoryStateBackend {
    async fn get_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<StateEntry>> {
        let key = (
            schema_name.to_string(),
            template_name.to_string(),
            state_name.to_string(),
        );
        Ok(self.inner.lock().unwrap().entries.get(&key).cloned())
    }

    async fn get_parsed_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<Option<TerraformState>> {
        let entry = self
            .get_state(schema_name, template_name, state_name)
            .await?;
        Ok(entry
            .and_then(|e| e.data)
            .and_then(|bytes| serde_json::from_slice(&bytes).ok()))
    }

    async fn save_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
        data: &[u8],
    ) -> Result<i64> {
        let key = (
            schema_name.to_string(),
            template_name.to_string(),
            state_name.to_string(),
        );
        let mut inner = self.inner.lock().unwrap();
        let id = inner.next_id;
        inner.next_id += 1;
        inner.entries.insert(
            key,
            StateEntry {
                id,
                name: state_name.to_string(),
                data: Some(data.to_vec()),
                created_at: chrono::Utc::now(),
            },
        );
        Ok(id)
    }

    async fn delete_state(
        &self,
        schema_name: &str,
        template_name: &str,
        state_name: &str,
    ) -> Result<bool> {
        let key = (
            schema_name.to_string(),
            template_name.to_string(),
            state_name.to_string(),
        );
        Ok(self.inner.lock().unwrap().entries.remove(&key).is_some())
    }

    async fn list_states(&self, schema_name: &str, template_name: &str) -> Result<Vec<StateEntry>> {
        let inner = self.inner.lock().unwrap();
        let mut entries: Vec<StateEntry> = inner
            .entries
            .iter()
            .filter(|((s, t, _), _)| s == schema_name && t == template_name)
            .map(|(_, v)| v.clone())
            .collect();
        // Newest first to mirror the PG impl's ORDER BY created_at DESC.
        entries.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_backend_is_object_safe() {
        // Compile-time check — Arc<dyn StateBackend> must build.
        fn _accepts_dyn(_b: Arc<dyn StateBackend>) {}
    }

    #[tokio::test]
    async fn in_memory_get_returns_none_when_empty() {
        let b = InMemoryStateBackend::new();
        let result = b.get_state("schema", "tmpl", "state").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn in_memory_save_then_get_round_trips() {
        let b = InMemoryStateBackend::new();
        let id = b
            .save_state("schema", "tmpl", "state", b"hello world")
            .await
            .unwrap();
        assert_eq!(id, 1);

        let got = b.get_state("schema", "tmpl", "state").await.unwrap();
        assert!(got.is_some());
        let entry = got.unwrap();
        assert_eq!(entry.id, 1);
        assert_eq!(entry.name, "state");
        assert_eq!(entry.data.as_deref(), Some(&b"hello world"[..]));
    }

    #[tokio::test]
    async fn in_memory_save_is_upsert() {
        let b = InMemoryStateBackend::new();
        let id1 = b
            .save_state("schema", "tmpl", "state", b"v1")
            .await
            .unwrap();
        let id2 = b
            .save_state("schema", "tmpl", "state", b"v2")
            .await
            .unwrap();
        assert_ne!(id1, id2, "upsert should issue new IDs");

        let got = b.get_state("schema", "tmpl", "state").await.unwrap();
        assert_eq!(got.unwrap().data.as_deref(), Some(&b"v2"[..]));
    }

    #[tokio::test]
    async fn in_memory_delete_removes_entry() {
        let b = InMemoryStateBackend::new();
        b.save_state("schema", "tmpl", "state", b"data")
            .await
            .unwrap();
        let removed = b.delete_state("schema", "tmpl", "state").await.unwrap();
        assert!(removed);
        let got = b.get_state("schema", "tmpl", "state").await.unwrap();
        assert!(got.is_none());

        // Idempotent: second delete returns false.
        let removed = b.delete_state("schema", "tmpl", "state").await.unwrap();
        assert!(!removed);
    }

    #[tokio::test]
    async fn in_memory_list_returns_only_matching_template() {
        let b = InMemoryStateBackend::new();
        b.save_state("s", "t1", "s1", b"a").await.unwrap();
        b.save_state("s", "t1", "s2", b"b").await.unwrap();
        b.save_state("s", "t2", "s1", b"c").await.unwrap();

        let listed = b.list_states("s", "t1").await.unwrap();
        assert_eq!(listed.len(), 2);
        // Newest first — s2 was saved after s1.
        assert_eq!(listed[0].name, "s2");
        assert_eq!(listed[1].name, "s1");
    }

    #[tokio::test]
    async fn in_memory_get_parsed_state_handles_garbage_bytes() {
        let b = InMemoryStateBackend::new();
        b.save_state("s", "t", "s", b"not-json").await.unwrap();
        let parsed = b.get_parsed_state("s", "t", "s").await.unwrap();
        assert!(
            parsed.is_none(),
            "garbage bytes should yield None, not an error"
        );
    }

    #[tokio::test]
    async fn dyn_dispatch_works() {
        let b: Arc<dyn StateBackend> = Arc::new(InMemoryStateBackend::new());
        b.save_state("s", "t", "s", b"x").await.unwrap();
        let listed = b.list_states("s", "t").await.unwrap();
        assert_eq!(listed.len(), 1);
    }

    #[test]
    fn recorded_keys_inspects_state_for_assertions() {
        let b = InMemoryStateBackend::new();
        // Sync test of the inspector — async ops happen via blocking
        // tokio runtime when this is exercised in an integration test.
        // Here we just verify the empty case shape.
        assert_eq!(b.recorded_keys().len(), 0);
    }
}
