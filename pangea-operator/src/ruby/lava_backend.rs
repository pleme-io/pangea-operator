//! Lava backend — dashboard rendering with no Ruby in the process.
//!
//! ## Why this exists
//!
//! `pangea-ruby-eval` embeds a full CRuby via magnus/rb-sys, in-process,
//! sharing the operator's address space and OS credentials. It has no
//! `$SAFE`, no taint, no timeout, no thread supervision and no restricted
//! binding, and it evaluates `spec.source.inline.ruby` verbatim — so
//! `Kernel#system`, `File.read`, `Net::HTTP` and backticks are all
//! reachable from a CR, and `while true; end` wedges the thread
//! permanently. All the load-path engineering in that crate is about
//! `require` *correctness*, not `require` *containment*.
//!
//! That matters in two places at once: the FedRAMP boundary cannot
//! receive Ruby at all, and the same operator runs on the fleet control
//! plane today.
//!
//! ## What this backend implements
//!
//! Only `render_dashboard`. The four compile/list/smoke methods have no
//! bodies in the trait, so they must be present — they return a typed
//! not-implemented rather than a wrong answer, the same shape
//! `EmbeddedCompilerBackend::compile_any` already uses. A lava backend
//! that silently returned an empty architecture listing would read as
//! "this gem has no architectures", which is worse than an error.

use std::path::PathBuf;

use async_trait::async_trait;

use super::backend::{
    ArchListing, BackendError, CompileAnyRequest, CompileAnyResult, CompileRequest, CompileResult,
    CompilerBackend, FixtureOutcome, SmokeRequest, SourceKind,
};

/// Where the image keeps its `(deflava-dashboard …)` catalogue.
///
/// Read once at construction. A catalogue that is absent is not an error
/// here — an operator serving only inline tlisp never touches it — but a
/// *named* architecture that cannot be found is, and says so with the
/// path it looked in.
const DEFAULT_CATALOG: &str = "/usr/share/lava/dashboards";

#[derive(Clone, Debug)]
pub struct LavaCompilerBackend {
    catalog: PathBuf,
}

impl Default for LavaCompilerBackend {
    fn default() -> Self {
        Self::from_env()
    }
}

impl LavaCompilerBackend {
    #[must_use]
    pub fn new(catalog: impl Into<PathBuf>) -> Self {
        Self {
            catalog: catalog.into(),
        }
    }

    #[must_use]
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("LAVA_DASHBOARD_CATALOG")
                .unwrap_or_else(|_| DEFAULT_CATALOG.to_string()),
        )
    }

    /// Resolve a catalogue entry to its `.tlisp` source.
    ///
    /// The name is checked against a strict character set before it
    /// touches the filesystem: a catalogue lookup that accepted `..`
    /// would turn a name into a path traversal, which is precisely the
    /// class this whole backend exists to close.
    fn load_architecture(&self, name: &str) -> Result<String, BackendError> {
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(BackendError::Evaluator(format!(
                "dashboard architecture name {name:?} is not a bare identifier \
                 (letters, digits, '-' and '_' only)"
            )));
        }
        let path = self.catalog.join(format!("{name}.tlisp"));
        std::fs::read_to_string(&path).map_err(|e| {
            BackendError::Evaluator(format!(
                "dashboard architecture {name:?} not found in the image catalogue at {}: {e}",
                self.catalog.display()
            ))
        })
    }

    /// Render `(deflava-dashboard …)` source to Grafana dashboard JSON.
    pub fn render_tlisp(&self, src: &str) -> Result<String, BackendError> {
        let theme = lava_core::Theme::default();
        let value = lava_eval::render_dashboard_grafana_json(src, &theme)
            .map_err(|e| BackendError::Evaluator(e.to_string()))?;
        serde_json::to_string(&value)
            .map_err(|e| BackendError::Evaluator(format!("serialize dashboard: {e}")))
    }

    /// Render a named catalogue architecture with its parameters.
    ///
    /// Parameters are appended to the form as a `:params` clause rather
    /// than string-substituted into the source: the whole point of the
    /// typed variant is that nothing in the CR is ever evaluated as
    /// code, and splicing text back in would give that away.
    pub fn render_architecture(
        &self,
        name: &str,
        params: Option<&serde_json::Value>,
    ) -> Result<String, BackendError> {
        let src = self.load_architecture(name)?;
        match params {
            None => self.render_tlisp(&src),
            Some(p) => {
                let bound = bind_params(&src, p)?;
                self.render_tlisp(&bound)
            }
        }
    }
}

/// Substitute `{key}` placeholders from a flat parameter object.
///
/// Deliberately flat and deliberately textual only at the *value* level:
/// each value is rendered as a scalar and injected where the architecture
/// author wrote `{key}`. A value that is itself structured is refused
/// rather than serialised into the source, because a JSON object pasted
/// into a `.tlisp` string is not the same document and would parse as
/// something the author never wrote.
fn bind_params(src: &str, params: &serde_json::Value) -> Result<String, BackendError> {
    let serde_json::Value::Object(map) = params else {
        return Err(BackendError::Evaluator(
            "dashboard params must be an object".to_string(),
        ));
    };
    // A Grafana legend is `{{label}}`, and a param sharing a label's name
    // must not be substituted inside one. Naive replacement turns
    // `{{namespace}}` into `{default}` — the inner `{namespace}` matches —
    // and the result is not a broken legend but an INTERPOLATION ERROR
    // from the evaluator, `unknown var "default"`, pointing nowhere near
    // the legend that caused it.
    //
    // Found 2026-08-11 the moment a board legended by `{{namespace}}` and
    // `{{job}}` was written; every earlier board legended by `{{pod}}`,
    // and `pod` happened not to be a param. The bug was one panel away
    // from the start and nothing would have caught it until a board used
    // the wrong two words.
    //
    // Masking `{{`/`}}` around the substitution is the whole fix: a
    // doubled brace is never a placeholder, so it is never a candidate.
    // The sentinel uses NUL, which cannot appear in a `.tlisp` source —
    // the lexer would have rejected the file long before this point.
    const LB: &str = "\u{0}lava-lb\u{0}";
    const RB: &str = "\u{0}lava-rb\u{0}";
    let mut out = src.replace("{{", LB).replace("}}", RB);
    for (k, v) in map {
        let scalar = match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            other => {
                return Err(BackendError::Evaluator(format!(
                    "dashboard param {k:?} is {}, but only strings, numbers and booleans \
                     can be bound — structure belongs in the architecture, not the CR",
                    match other {
                        serde_json::Value::Array(_) => "an array",
                        serde_json::Value::Object(_) => "an object",
                        _ => "null",
                    }
                )))
            }
        };
        out = out.replace(&format!("{{{k}}}"), &scalar);
    }
    Ok(out.replace(LB, "{{").replace(RB, "}}"))
}

#[async_trait]
impl CompilerBackend for LavaCompilerBackend {
    async fn list_architectures(&self, _gem: &str) -> Result<ArchListing, BackendError> {
        Err(BackendError::Evaluator(
            "the lava backend renders dashboards only; architecture listing needs the \
             embedded Ruby backend"
                .to_string(),
        ))
    }

    async fn smoke_test(&self, _req: SmokeRequest) -> Result<FixtureOutcome, BackendError> {
        Err(BackendError::Evaluator(
            "the lava backend renders dashboards only; smoke tests need the embedded \
             Ruby backend"
                .to_string(),
        ))
    }

    async fn compile(&self, _req: CompileRequest) -> Result<CompileResult, BackendError> {
        Err(BackendError::Evaluator(
            "the lava backend renders dashboards only; Pangea DSL compilation needs the \
             embedded Ruby backend"
                .to_string(),
        ))
    }

    async fn compile_any(&self, _req: CompileAnyRequest) -> Result<CompileAnyResult, BackendError> {
        Err(BackendError::Evaluator(
            "the lava backend renders dashboards only; compile-any needs the embedded \
             Ruby backend"
                .to_string(),
        ))
    }

    async fn render_dashboard_architecture(
        &self,
        name: String,
        params: Option<serde_json::Value>,
    ) -> Result<String, BackendError> {
        self.render_architecture(&name, params.as_ref())
    }

    async fn render_dashboard(
        &self,
        source: String,
        _extend_modules: Vec<String>,
        kind: SourceKind,
    ) -> Result<String, BackendError> {
        if kind != SourceKind::Lisp {
            return Err(BackendError::Evaluator(format!(
                "the lava backend cannot evaluate {kind:?} source"
            )));
        }
        // `extend_modules` is deliberately ignored. Its CRD default is
        // ["Pangea::Grafana"] — a Ruby module name — so every lava-backed
        // CR that does not override it would otherwise inherit a
        // meaningless value. The lava vocabulary is compiled in, not
        // required at eval time, so there is nothing for it to select.
        self.render_tlisp(&source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SRC: &str = r#"
(deflava-dashboard probe
  :uid "probe"
  :title "Probe"
  :datasources ((:uid "mimir" :type "prometheus" :lang "promql"))
  :rows ((:title "r"
          :panels ((:id "up" :kind "stat" :title "Up"
                    :queries ((:expr "min(up{job=\"api\"})" :datasource "mimir")))))))
"#;

    #[test]
    fn it_renders_tlisp_to_grafana_json() {
        let b = LavaCompilerBackend::new("/nonexistent");
        let out = b.render_tlisp(SRC).unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["uid"], "probe");
        assert_eq!(v["schemaVersion"], 39);
        // The PromQL label matcher survived — braces are not interpolation.
        assert!(out.contains(r#"min(up{job=\"api\"})"#), "{out}");
    }

    #[test]
    fn a_traversing_architecture_name_is_refused_before_it_reaches_the_filesystem() {
        let b = LavaCompilerBackend::new("/usr/share/lava/dashboards");
        for bad in ["../../etc/passwd", "a/b", "", "name with space", "n;rm"] {
            let e = b.render_architecture(bad, None).unwrap_err();
            assert!(
                e.to_string().contains("bare identifier"),
                "{bad:?} produced {e}"
            );
        }
    }

    #[test]
    fn a_missing_catalogue_entry_names_where_it_looked() {
        let b = LavaCompilerBackend::new("/nonexistent-catalog");
        let e = b.render_architecture("workload-overview", None).unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("workload-overview"), "{msg}");
        assert!(msg.contains("/nonexistent-catalog"), "{msg}");
    }

    #[test]
    fn structured_params_are_refused_rather_than_pasted_into_the_source() {
        let params = serde_json::json!({ "jobs": ["a", "b"] });
        let e = bind_params("(deflava-dashboard d)", &params).unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("an array"), "{msg}");
        assert!(msg.contains("belongs in the architecture"), "{msg}");
    }

    #[test]
    fn scalar_params_bind_by_placeholder() {
        let out = bind_params(
            r#"(deflava-dashboard d :uid "{env}-board" :title "{env}" :replicas {n})"#,
            &serde_json::json!({ "env": "camelot", "n": 3 }),
        )
        .unwrap();
        assert!(out.contains(r#":uid "camelot-board""#), "{out}");
        assert!(out.contains(":replicas 3"), "{out}");
    }

    /// A Grafana legend is `{{label}}` and must survive binding intact,
    /// even when a param shares the label's name.
    ///
    /// Found live 2026-08-11. Naive replacement rewrites the INNER
    /// `{namespace}` of `{{namespace}}`, yielding `{default}` — and the
    /// symptom is not a wrong legend but the evaluator refusing the whole
    /// document with `unknown var "default"`, an error naming a value the
    /// author never wrote and pointing nowhere near the legend.
    ///
    /// It was one panel away from the start: every board until then
    /// legended by `{{pod}}`, and `pod` happened not to be a param. The
    /// first board legended by `{{namespace}}` broke instantly.
    #[test]
    fn a_grafana_legend_is_not_a_placeholder() {
        let out = bind_params(
            r#"(:legend "{{namespace}}/{{job}}" :expr "up{namespace=\"{namespace}\"}" :title "{namespace}")"#,
            &serde_json::json!({ "namespace": "camelot", "job": "auth" }),
        )
        .unwrap();

        // The legend survives untouched — both labels.
        assert!(out.contains(r#":legend "{{namespace}}/{{job}}""#), "{out}");
        // …while genuine single-brace placeholders still bind, including
        // one sitting inside a PromQL selector in the same string.
        assert!(out.contains(r#"up{namespace=\"camelot\"}"#), "{out}");
        assert!(out.contains(r#":title "camelot""#), "{out}");
    }

    #[tokio::test]
    async fn the_compile_methods_report_a_typed_gap_rather_than_an_empty_answer() {
        let b = LavaCompilerBackend::new("/nonexistent");
        let e = b.list_architectures("pangea-aws").await.unwrap_err();
        // An empty listing would read as "this gem has no architectures".
        assert!(e.to_string().contains("dashboards only"), "{e}");
    }
}

/// Routes a dashboard render to the evaluator its source actually needs,
/// and delegates everything else to the Ruby backend.
///
/// This is what makes the migration incremental. The 17 boilerplate CRs
/// and the one bespoke board do not have to move on the same day, and a
/// cluster can serve both shapes while they do — which matters because
/// `PANGEA_COMPILER_BACKEND` is a single process-wide string, so without
/// a dispatcher the choice of evaluator would be a flag day.
pub struct DispatchingCompilerBackend {
    ruby: std::sync::Arc<dyn CompilerBackend>,
    lava: LavaCompilerBackend,
}

impl DispatchingCompilerBackend {
    #[must_use]
    pub fn new(ruby: std::sync::Arc<dyn CompilerBackend>, lava: LavaCompilerBackend) -> Self {
        Self { ruby, lava }
    }
}

#[async_trait]
impl CompilerBackend for DispatchingCompilerBackend {
    async fn prepare_gem(&self, source: &super::backend::GemSource) -> Result<(), BackendError> {
        self.ruby.prepare_gem(source).await
    }
    async fn list_architectures(&self, gem: &str) -> Result<ArchListing, BackendError> {
        self.ruby.list_architectures(gem).await
    }
    async fn smoke_test(&self, req: SmokeRequest) -> Result<FixtureOutcome, BackendError> {
        self.ruby.smoke_test(req).await
    }
    async fn compile(&self, req: CompileRequest) -> Result<CompileResult, BackendError> {
        self.ruby.compile(req).await
    }
    async fn compile_any(&self, req: CompileAnyRequest) -> Result<CompileAnyResult, BackendError> {
        self.ruby.compile_any(req).await
    }
    async fn render_dashboard_architecture(
        &self,
        name: String,
        params: Option<serde_json::Value>,
    ) -> Result<String, BackendError> {
        self.lava.render_dashboard_architecture(name, params).await
    }
    async fn render_dashboard(
        &self,
        source: String,
        extend_modules: Vec<String>,
        kind: SourceKind,
    ) -> Result<String, BackendError> {
        match kind {
            SourceKind::Lisp => self.lava.render_dashboard(source, extend_modules, kind).await,
            _ => self.ruby.render_dashboard(source, extend_modules, kind).await,
        }
    }
}
