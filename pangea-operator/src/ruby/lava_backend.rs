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

/// Where the image keeps its `(deflava-architecture …)` catalogue.
///
/// Separate from the dashboard catalogue on purpose. They are different
/// vocabularies rendering to different targets — Grafana JSON versus
/// terraform.json — and a name colliding across them should resolve to
/// the one the caller asked for, not to whichever directory was searched
/// first.
const DEFAULT_ARCH_CATALOG: &str = "/usr/share/lava/architectures";

#[derive(Clone, Debug)]
pub struct LavaCompilerBackend {
    catalog: PathBuf,
    architectures: PathBuf,
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
            architectures: PathBuf::from(DEFAULT_ARCH_CATALOG),
        }
    }

    /// Point the architecture catalogue somewhere else. Used by tests and
    /// by an image that lays its catalogue out differently.
    #[must_use]
    pub fn with_architectures(mut self, dir: impl Into<PathBuf>) -> Self {
        self.architectures = dir.into();
        self
    }

    #[must_use]
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("LAVA_DASHBOARD_CATALOG")
                .unwrap_or_else(|_| DEFAULT_CATALOG.to_string()),
        )
        .with_architectures(
            std::env::var("LAVA_ARCHITECTURE_CATALOG")
                .unwrap_or_else(|_| DEFAULT_ARCH_CATALOG.to_string()),
        )
    }

    /// Resolve an architecture name to its `.tlisp` source.
    ///
    /// Same bare-identifier check as the dashboard loader, for the same
    /// reason: a catalogue lookup that accepted `..` turns a CR field into
    /// a path traversal.
    fn load_arch_source(&self, name: &str) -> Result<String, BackendError> {
        check_bare_identifier(name, "architecture")?;
        let path = self.architectures.join(format!("{name}.tlisp"));
        std::fs::read_to_string(&path).map_err(|e| {
            BackendError::Evaluator(format!(
                "architecture {name:?} not found in the image catalogue at {}: {e}",
                self.architectures.display()
            ))
        })
    }

    /// Evaluate a `(deflava-architecture …)` source with typed bindings and
    /// render it to terraform JSON.
    ///
    /// ── ★ BINDINGS ARE TYPED, NOT SPLICED ────────────────────────────
    /// The dashboard path substitutes `{key}` textually because Grafana
    /// documents are templated that way. Architectures are not: lava takes
    /// an `InputBindings` and the evaluator resolves names itself, so a
    /// caller's value can never become syntax. That difference is the whole
    /// reason this path can accept variables from a CR at all.
    pub fn render_architecture_terraform(
        &self,
        src: &str,
        variables: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, BackendError> {
        let bindings = bindings_from_variables(variables)?;
        let arch = lava_eval::eval_architecture(src, &bindings)
            .map_err(|e| BackendError::Evaluator(format!("lava evaluation failed: {e}")))?;
        arch.render_terraform_json()
            .map_err(|e| BackendError::Evaluator(format!("lava render failed: {e}")))
    }

    /// Resolve a catalogue entry to its `.tlisp` source.
    ///
    /// The name is checked against a strict character set before it
    /// touches the filesystem: a catalogue lookup that accepted `..`
    /// would turn a name into a path traversal, which is precisely the
    /// class this whole backend exists to close.
    fn load_architecture(&self, name: &str) -> Result<String, BackendError> {
        check_bare_identifier(name, "dashboard architecture")?;
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

/// Reject anything that is not a bare identifier before it reaches the
/// filesystem.
///
/// Shared by both catalogues. A lookup that accepted `..` would turn a CR
/// field into a path traversal, which is the class this whole backend
/// exists to close — so it is checked once, in one place, rather than
/// re-implemented per catalogue where one copy could drift lenient.
fn check_bare_identifier(name: &str, what: &str) -> Result<(), BackendError> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(BackendError::Evaluator(format!(
            "{what} name {name:?} is not a bare identifier \
             (letters, digits, '-' and '_' only)"
        )));
    }
    Ok(())
}

/// Convert a compile request's `variables` into lava `InputBindings`.
///
/// ── ★ WHAT IS DELIBERATELY REFUSED ───────────────────────────────────
/// lava's binding surface is scalars and lists-of-scalars, and this maps
/// onto it exactly rather than flattening. A nested object, or a list
/// containing one, is REFUSED with the offending key named — because the
/// alternative is serialising it to a string, which produces a binding
/// that evaluates without error and means something different from what
/// the caller wrote. Structure belongs in the architecture, where it is
/// typed, not smuggled through a variable.
///
/// Numbers and booleans become their text. That is not a loss: lava
/// re-types a scalar by shape at evaluation, so `8080` arrives as a JSON
/// number in the rendered document, and a value that must stay text
/// (a zero-padded id) survives because that re-typing is round-trip
/// checked upstream.
fn bindings_from_variables(
    variables: &std::collections::HashMap<String, serde_json::Value>,
) -> Result<lava_eval::InputBindings, BackendError> {
    use serde_json::Value;
    let mut b = lava_eval::InputBindings::new();
    // Sorted so a rendering is reproducible regardless of HashMap order —
    // the same reason the goldens this is diffed against are key-sorted.
    let mut keys: Vec<&String> = variables.keys().collect();
    keys.sort();
    for k in keys {
        match &variables[k] {
            Value::String(v) => b.set_str(k.clone(), v.clone()),
            Value::Number(n) => b.set_str(k.clone(), n.to_string()),
            Value::Bool(v) => b.set_str(k.clone(), v.to_string()),
            Value::Array(items) => {
                // ── ★ AN ARRAY OF OBJECTS IS A RECORD LIST ────────────────
                // This is what lets a catalogue reach lava at all. The org
                // workspace is 997 rows each with a name, description,
                // visibility and flags; before records existed the only
                // encodings were parallel lists (which for-each cannot
                // express) or a delimited blob (which makes the architecture
                // parse its own input).
                //
                // Decided by the FIRST element's shape and then required of
                // every element, rather than per-element: a list that is
                // half scalars and half objects is a malformed input, and
                // silently binding the scalar half would render a document
                // missing rows nobody could account for.
                let first_is_object = items.first().is_some_and(Value::is_object);
                if first_is_object {
                    let mut rows = Vec::with_capacity(items.len());
                    for (i, it) in items.iter().enumerate() {
                        let Value::Object(map) = it else {
                            return Err(BackendError::Evaluator(format!(
                                "variable {k:?}[{i}] is not an object, but {k:?}[0] is — a \
                                 record list must be uniform, and binding only the object \
                                 rows would render a document silently missing the rest"
                            )));
                        };
                        let mut row = std::collections::BTreeMap::new();
                        for (field, v) in map {
                            let scalar = match v {
                                Value::String(x) => x.clone(),
                                Value::Number(x) => x.to_string(),
                                Value::Bool(x) => x.to_string(),
                                Value::Null => String::new(),
                                _ => {
                                    return Err(BackendError::Evaluator(format!(
                                        "variable {k:?}[{i}].{field} is structured; a record \
                                         field is a scalar, and nesting belongs in the \
                                         architecture where it is typed"
                                    )))
                                }
                            };
                            row.insert(field.clone(), scalar);
                        }
                        rows.push(row);
                    }
                    b.set_records(k.clone(), rows);
                } else {
                    let mut out = Vec::with_capacity(items.len());
                    for (i, it) in items.iter().enumerate() {
                        match it {
                            Value::String(v) => out.push(v.clone()),
                            Value::Number(n) => out.push(n.to_string()),
                            Value::Bool(v) => out.push(v.to_string()),
                            _ => {
                                return Err(BackendError::Evaluator(format!(
                                    "variable {k:?}[{i}] is structured; lava binds lists of \
                                     scalars, and serialising structure into a binding would \
                                     evaluate cleanly while meaning something else"
                                )))
                            }
                        }
                    }
                    b.set_list(k.clone(), out);
                }
            }
            Value::Null => {
                return Err(BackendError::Evaluator(format!(
                    "variable {k:?} is null; lava has no null binding, and treating it as \
                     an empty string would silently substitute the wrong value"
                )))
            }
            Value::Object(_) => {
                return Err(BackendError::Evaluator(format!(
                    "variable {k:?} is an object; structure belongs in the architecture, \
                     not in a variable"
                )))
            }
        }
    }
    Ok(b)
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
    /// Enumerate the image's architecture catalogue.
    ///
    /// The `gem` argument is ignored, and that is the honest answer rather
    /// than a shrug: a gem is a Ruby packaging unit. lava architectures are
    /// baked into the image as `.tlisp`, so there is exactly one catalogue
    /// and no per-gem partition to filter by. Erroring on a gem name would
    /// break callers that pass one for the Ruby backend's benefit; silently
    /// returning a filtered-empty list would read as "this gem has no
    /// architectures", which is worse than either.
    async fn list_architectures(&self, gem: &str) -> Result<ArchListing, BackendError> {
        let dir = &self.architectures;
        let entries = std::fs::read_dir(dir).map_err(|e| {
            BackendError::Evaluator(format!(
                "architecture catalogue {} is unreadable: {e}",
                dir.display()
            ))
        })?;
        let mut names: Vec<String> = Vec::new();
        for e in entries.flatten() {
            let path = e.path();
            if path.extension().and_then(|x| x.to_str()) != Some("tlisp") {
                continue;
            }
            if let Some(stem) = path.file_stem().and_then(|x| x.to_str()) {
                // A file whose name is not a bare identifier could never be
                // LOADED by name, so listing it would advertise something
                // unreachable.
                if check_bare_identifier(stem, "architecture").is_ok() {
                    names.push(stem.to_string());
                }
            }
        }
        names.sort();
        Ok(ArchListing {
            // Echoed back verbatim. The catalogue is not partitioned by gem
            // (see the doc comment), so this is the caller's own label
            // returned unchanged rather than a claim that these
            // architectures came from that gem.
            gem: gem.to_string(),
            classes: names,
            // No version: a gem has one, an image catalogue does not, and
            // inventing a value here would be a claim nothing backs.
            version: None,
        })
    }

    async fn smoke_test(&self, _req: SmokeRequest) -> Result<FixtureOutcome, BackendError> {
        Err(BackendError::Evaluator(
            "the lava backend renders dashboards only; smoke tests need the embedded \
             Ruby backend"
                .to_string(),
        ))
    }

    /// Compile an architecture to terraform JSON, with no Ruby in the
    /// process.
    ///
    /// Source resolution, in order, because a caller that supplied inline
    /// source meant it:
    ///   1. `source`      — inline `.tlisp`, used verbatim
    ///   2. `template_name` — a catalogue entry
    ///   3. `template_path` — its file stem, so a caller that only ever had
    ///                        a path still resolves
    ///
    /// `rubylib_paths` is ignored and cannot matter: it prepends to a Ruby
    /// `$LOAD_PATH`, and there is no interpreter here. Erroring on it would
    /// break every caller that populates it unconditionally for the
    /// embedded backend's benefit.
    async fn compile(&self, req: CompileRequest) -> Result<CompileResult, BackendError> {
        let src = if let Some(inline) = req.source.as_deref() {
            inline.to_string()
        } else {
            let name = req
                .template_name
                .clone()
                .or_else(|| {
                    req.template_path.as_deref().and_then(|p| {
                        std::path::Path::new(p)
                            .file_stem()
                            .and_then(|x| x.to_str())
                            .map(str::to_string)
                    })
                })
                .ok_or_else(|| {
                    BackendError::Evaluator(
                        "compile request carries no source, template_name or template_path — \
                         nothing identifies what to render"
                            .to_string(),
                    )
                })?;
            self.load_arch_source(&name)?
        };

        let value = self.render_architecture_terraform(&src, &req.variables)?;
        // Pretty for the on-disk form tofu and humans read; the typed value
        // travels beside it so magma consumers skip a parse round-trip.
        let terraform_json = serde_json::to_string_pretty(&value)
            .map_err(|e| BackendError::Evaluator(format!("serialize terraform json: {e}")))?;
        Ok(CompileResult {
            terraform_json,
            synthesis_value: Some(value),
        })
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
    async fn an_unreadable_catalogue_is_an_error_not_an_empty_listing() {
        // ── ★ THIS TEST USED TO ASSERT THE OPPOSITE ────────────────────
        // It pinned "dashboards only" — the typed gap that made a
        // Ruby-free operator undeployable, and which compile/
        // list_architectures now close. What it was PROTECTING is still
        // exactly right and is kept: an empty listing reads as "this gem
        // has no architectures", which is worse than an error, so a
        // catalogue that cannot be read must say so.
        let b = LavaCompilerBackend::new("/nonexistent").with_architectures("/nonexistent-arch");
        let e = b.list_architectures("pangea-aws").await.unwrap_err();
        assert!(
            e.to_string().contains("unreadable"),
            "an absent catalogue must be reported, never rendered as an empty list: {e}"
        );
    }

    #[tokio::test]
    async fn a_real_catalogue_lists_only_loadable_architectures() {
        let dir = std::env::temp_dir().join(format!("lava-arch-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("aws-sg.tlisp"), "").unwrap();
        std::fs::write(dir.join("dns_zone.tlisp"), "").unwrap();
        // Neither of these can ever be LOADED by name, so listing them
        // would advertise something unreachable.
        std::fs::write(dir.join("not a bare name.tlisp"), "").unwrap();
        std::fs::write(dir.join("README.md"), "").unwrap();

        let b = LavaCompilerBackend::new("/nonexistent").with_architectures(&dir);
        let got = b.list_architectures("some-gem").await.unwrap();
        assert_eq!(got.classes, vec!["aws-sg".to_string(), "dns_zone".to_string()]);
        // The gem label is echoed, never invented — the catalogue is not
        // partitioned by gem and pretending otherwise would be a claim
        // nothing backs.
        assert_eq!(got.gem, "some-gem");
        assert_eq!(got.version, None);
        std::fs::remove_dir_all(&dir).ok();
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

#[cfg(test)]
mod compile_tests {
    use super::*;
    use std::collections::HashMap;

    /// The smallest architecture that renders a real resource, written
    /// inline so the test does not depend on an image catalogue.
    const ARCH: &str = r#"
(deflava-interface probe
  :doc "test"
  :inputs ((:name :type :string :required #t)
    (:sg :type :string :required #t)
    (:cidrs :type (:list-of :string) :required #t))
  :outputs ((:sg-id :type :string)))

(deflava-architecture probe
  :inputs ((:name "p") (:sg "sg-0") (:cidrs ("10.0.0.0/8")))
  :resources ((aws-security-group-rule
     "{name}-in"
     :type "ingress"
     :from-port 22
     :to-port 22
     :protocol "tcp"
     :cidr-blocks "{cidrs}"
     :security-group-id "{sg}"))
  :outputs (:sg_id "{sg}")
  :result (probe :sg-id "{sg}"))
"#;

    fn vars(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect()
    }

    #[tokio::test]
    async fn compile_renders_terraform_json_with_no_ruby() {
        // ★ THE POINT OF THE WHOLE EXERCISE. This is the method that
        // returned "the lava backend renders dashboards only" — the single
        // Err that made a Ruby-free operator undeployable.
        let b = LavaCompilerBackend::new("/nonexistent-dashboards");
        let req = CompileRequest {
            source: Some(ARCH.to_string()),
            template_path: None,
            rubylib_paths: vec![],
            variables: vars(&[
                ("name", serde_json::json!("probe")),
                ("sg", serde_json::json!("sg-abc")),
                ("cidrs", serde_json::json!(["203.0.113.0/32"])),
            ]),
            template_name: None,
        };
        let got = b.compile(req).await.expect("lava must compile an architecture");
        let v = got.synthesis_value.expect("typed value travels beside the string");
        let rule = &v["resource"]["aws_security_group_rule"]["probe-in"];
        assert_eq!(rule["security_group_id"], "sg-abc");
        assert_eq!(rule["cidr_blocks"][0], "203.0.113.0/32");
        // A bare integer in the source stays a JSON NUMBER. terraform's
        // schema wants a number here, and a stringified "22" is the shape
        // that diverges from the Ruby oracle.
        assert_eq!(rule["from_port"], 22);
        assert_eq!(v["output"]["sg_id"]["value"], "sg-abc");
        // The string form is the same document, not a second rendering.
        let reparsed: serde_json::Value =
            serde_json::from_str(&got.terraform_json).expect("terraform_json parses");
        assert_eq!(reparsed, v, "the two surfaces must never disagree");
    }

    #[tokio::test]
    async fn a_list_variable_binds_as_a_list_not_a_joined_string() {
        // Comma-joining a list is the tempting shortcut and it renders a
        // DIFFERENT document — one cidr_blocks entry containing a comma,
        // which terraform accepts and applies wrongly.
        let b = LavaCompilerBackend::new("/nonexistent");
        let req = CompileRequest {
            source: Some(ARCH.to_string()),
            template_path: None,
            rubylib_paths: vec![],
            variables: vars(&[
                ("name", serde_json::json!("p")),
                ("sg", serde_json::json!("sg-1")),
                ("cidrs", serde_json::json!(["10.0.0.0/8", "192.168.0.0/16"])),
            ]),
            template_name: None,
        };
        let v = b.compile(req).await.unwrap().synthesis_value.unwrap();
        let cidrs = &v["resource"]["aws_security_group_rule"]["p-in"]["cidr_blocks"];
        assert!(cidrs.is_array(), "must be an array, got {cidrs}");
        assert_eq!(cidrs.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn structured_variables_are_refused_rather_than_stringified() {
        // Serialising an object into a binding evaluates cleanly and means
        // something else — the failure would surface as a wrong document,
        // not an error.
        let b = LavaCompilerBackend::new("/nonexistent");
        // NOTE `[{"a": 1}]` is deliberately absent from this list now: an
        // array of objects is a RECORD LIST, which is how a catalogue reaches
        // lava at all. What remains refused is structure that has no binding
        // shape — a bare object, a null, and (covered in
        // record_variable_tests) a nested field inside a record row.
        for bad in [
            serde_json::json!({"nested": 1}),
            serde_json::json!(null),
        ] {
            let req = CompileRequest {
                source: Some(ARCH.to_string()),
                template_path: None,
                rubylib_paths: vec![],
                variables: vars(&[("name", serde_json::json!("p")), ("bad", bad.clone())]),
                template_name: None,
            };
            let err = b.compile(req).await.expect_err("structure must be refused");
            let msg = format!("{err}");
            assert!(msg.contains("bad"), "the error must NAME the key: {msg}");
        }
    }

    #[tokio::test]
    async fn a_request_identifying_nothing_is_an_error_not_an_empty_render() {
        let b = LavaCompilerBackend::new("/nonexistent");
        let req = CompileRequest {
            source: None,
            template_path: None,
            rubylib_paths: vec![],
            variables: HashMap::new(),
            template_name: None,
        };
        let err = b.compile(req).await.expect_err("nothing to render is an error");
        assert!(format!("{err}").contains("nothing identifies"));
    }

    #[tokio::test]
    async fn a_traversing_template_name_never_reaches_the_filesystem() {
        let b = LavaCompilerBackend::new("/nonexistent").with_architectures("/nonexistent-arch");
        let req = CompileRequest {
            source: None,
            template_path: None,
            rubylib_paths: vec![],
            variables: HashMap::new(),
            template_name: Some("../../etc/passwd".to_string()),
        };
        let err = b.compile(req).await.expect_err("traversal must be refused");
        assert!(
            format!("{err}").contains("bare identifier"),
            "must be refused as a name, not attempted as a path: {err}"
        );
    }

    #[tokio::test]
    async fn rubylib_paths_are_ignored_rather_than_rejected() {
        // Callers populate this unconditionally for the embedded backend.
        // Erroring would break them for a field that cannot matter when
        // there is no interpreter.
        let b = LavaCompilerBackend::new("/nonexistent");
        let req = CompileRequest {
            source: Some(ARCH.to_string()),
            template_path: None,
            rubylib_paths: vec!["/opt/gems/lib".to_string()],
            variables: vars(&[("name", serde_json::json!("p")), ("sg", serde_json::json!("s"))]),
            template_name: None,
        };
        assert!(b.compile(req).await.is_ok());
    }
}

#[cfg(test)]
mod record_variable_tests {
    use super::*;
    use std::collections::HashMap;

    const ARCH: &str = r#"
(deflava-architecture rows
  :inputs ((:items ()))
  :resources ((for-each ((i it) (enumerate items))
    (github-repository "{it_name}" :name "{it_name}" :archived "{it_archived}"))))
"#;

    fn vars(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect()
    }

    async fn compile(v: HashMap<String, serde_json::Value>) -> Result<CompileResult, BackendError> {
        LavaCompilerBackend::new("/nonexistent")
            .compile(CompileRequest {
                source: Some(ARCH.to_string()),
                template_path: None,
                rubylib_paths: vec![],
                variables: v,
                template_name: None,
            })
            .await
    }

    #[tokio::test]
    async fn an_array_of_objects_binds_as_a_record_list() {
        // ★ THE WIRE THE CATALOGUE TRAVELS. Without this, `variables` refused
        // structure outright and a 997-row catalogue had no way to reach lava
        // at all — no CRD change was needed, only this shape being accepted.
        let got = compile(vars(&[(
            "items",
            serde_json::json!([
                {"name": "alpha", "archived": false},
                {"name": "beta",  "archived": true},
            ]),
        )]))
        .await
        .expect("a record list must bind");
        let v = got.synthesis_value.unwrap();
        let repos = &v["resource"]["github_repository"];
        assert_eq!(repos.as_object().unwrap().len(), 2);
        // Booleans survive as booleans through the whole wire: JSON bool ->
        // record field -> shape-typed on whole-value reference.
        assert_eq!(repos["alpha"]["archived"], false);
        assert_eq!(repos["beta"]["archived"], true);
    }

    #[tokio::test]
    async fn a_scalar_array_still_binds_as_a_list() {
        // The record branch is chosen by the FIRST element's shape; a scalar
        // array must be untouched by it.
        const L: &str = r#"
(deflava-architecture l
  :inputs ((:azs ()))
  :resources ((for-each ((i az) (enumerate azs))
    (aws-subnet "s-{i}" :availability-zone "{az}"))))
"#;
        let got = LavaCompilerBackend::new("/nonexistent")
            .compile(CompileRequest {
                source: Some(L.to_string()),
                template_path: None,
                rubylib_paths: vec![],
                variables: vars(&[("azs", serde_json::json!(["us-east-2a", "us-east-2b"]))]),
                template_name: None,
            })
            .await
            .expect("a scalar list must still bind");
        let v = got.synthesis_value.unwrap();
        assert_eq!(v["resource"]["aws_subnet"]["s-1"]["availability_zone"], "us-east-2b");
    }

    #[tokio::test]
    async fn a_mixed_array_is_refused_rather_than_half_bound() {
        // ★ A list half scalars and half objects is malformed input. Binding
        // only the object rows renders a document silently missing the rest —
        // resources nobody can account for being absent.
        let err = compile(vars(&[(
            "items",
            serde_json::json!([{"name": "alpha"}, "beta"]),
        )]))
        .await
        .expect_err("a mixed list must be refused");
        let m = format!("{err}");
        assert!(m.contains("uniform"), "the error must say why: {m}");
        assert!(m.contains("items"), "and name the variable: {m}");
    }

    #[tokio::test]
    async fn a_nested_field_inside_a_record_is_refused() {
        let err = compile(vars(&[(
            "items",
            serde_json::json!([{"name": "alpha", "deep": {"a": 1}}]),
        )]))
        .await
        .expect_err("nested structure must be refused");
        assert!(format!("{err}").contains("deep"), "must name the field");
    }
}
