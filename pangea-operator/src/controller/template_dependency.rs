//! Cross-template dependency + outputs — Terragrunt `dependency.<x>.outputs`
//! parity (P2). Promotes the (previously dead) `InfrastructureTemplate.spec.
//! variableRefs` to a LIVE mechanism: a template's variable is sourced from
//! ANOTHER template's `status.outputs`, exactly as Terragrunt's
//! `dependency.vpc.outputs.vpc_id` flows one unit's output into another's input.
//!
//! This is the standalone-template analogue of the `InfrastructureFlow` step
//! resolver (`executor::variable_resolver`): same data shape (named upstreams →
//! their outputs), promoted from inside-a-Flow to between top-level templates.
//! It adds `mockOutputs` (Terragrunt parity) so a downstream can PLAN before its
//! upstream has applied — the most-used companion of `dependency` in the real
//! akeylesslabs workloads.
//!
//! Pure (`serde_json` + `std`), fully unit-testable; the kube fetch of each
//! upstream's `status.outputs` is the thin wiring in the controller.

use std::collections::BTreeMap;

use serde_json::Value;

pub type VariableMap = BTreeMap<String, Value>;
/// Upstream template name → its `status.outputs` (output-key → value).
pub type UpstreamOutputs = BTreeMap<String, VariableMap>;

/// One resolved cross-template reference (the plain shape the pure resolver
/// works on — the CRD `VariableRef` converts to this at the wiring boundary, so
/// the resolution logic carries no kube dependency).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepRef {
    /// Upstream template this ref reads from.
    pub template: String,
    /// Which key of the upstream's `status.outputs` to read.
    pub output_key: String,
    /// Plan-time fallback used when the upstream's real output isn't available
    /// yet (Terragrunt `mock_outputs`). `None` ⇒ the upstream must be Ready
    /// before this template can proceed.
    pub mock: Option<Value>,
}

/// The outcome of resolving a template's cross-template dependencies.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DependencyResolution {
    /// Variables resolved from a REAL upstream output (safe to apply).
    pub resolved: VariableMap,
    /// Variables filled from a `mock` because the upstream wasn't ready
    /// (valid for PLAN, must NOT be applied — the caller gates apply on
    /// `unresolved_templates.is_empty()`).
    pub mocked: VariableMap,
    /// Distinct upstream templates whose output is missing AND have no mock —
    /// the caller requeues until these become Ready (the run-all gate).
    pub unresolved_templates: Vec<String>,
}

impl DependencyResolution {
    /// All dependency-sourced variables (real ⊕ mocked) to inject. Real wins if
    /// a key somehow appears in both (it won't — keys are the ref names).
    #[must_use]
    pub fn all_variables(&self) -> VariableMap {
        let mut m = self.mocked.clone();
        m.extend(self.resolved.clone());
        m
    }

    /// True iff every dependency resolved against a REAL upstream output — i.e.
    /// safe to APPLY (no mocks in play, no missing upstreams).
    #[must_use]
    pub fn fully_satisfied(&self) -> bool {
        self.unresolved_templates.is_empty() && self.mocked.is_empty()
    }
}

/// Resolve each `(var_name → DepRef)` against the upstream templates' outputs.
/// A ref resolves to the upstream's real output if present; else to its `mock`
/// (recorded in `mocked`); else the upstream is recorded in
/// `unresolved_templates` (deduped, sorted — deterministic). Type-preserving:
/// a JSON object/array output is injected as-is, not stringified.
#[must_use]
pub fn resolve_dependency_vars(
    refs: &BTreeMap<String, DepRef>,
    upstream: &UpstreamOutputs,
) -> DependencyResolution {
    let mut out = DependencyResolution::default();
    let mut unresolved = std::collections::BTreeSet::new();
    for (var_name, dep) in refs {
        match upstream
            .get(&dep.template)
            .and_then(|o| o.get(&dep.output_key))
        {
            Some(val) => {
                out.resolved.insert(var_name.clone(), val.clone());
            }
            None => match &dep.mock {
                Some(mock) => {
                    out.mocked.insert(var_name.clone(), mock.clone());
                }
                None => {
                    unresolved.insert(dep.template.clone());
                }
            },
        }
    }
    out.unresolved_templates = unresolved.into_iter().collect();
    out
}

/// The distinct set of upstream templates a ref-map depends on — the edges the
/// cross-template run-all DAG (P3) is built from.
#[must_use]
pub fn dependency_edges(refs: &BTreeMap<String, DepRef>) -> Vec<String> {
    let set: std::collections::BTreeSet<&str> =
        refs.values().map(|d| d.template.as_str()).collect();
    set.into_iter().map(String::from).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn refs(pairs: &[(&str, &str, &str, Option<Value>)]) -> BTreeMap<String, DepRef> {
        pairs
            .iter()
            .map(|(var, tpl, key, mock)| {
                (
                    (*var).to_string(),
                    DepRef {
                        template: (*tpl).to_string(),
                        output_key: (*key).to_string(),
                        mock: mock.clone(),
                    },
                )
            })
            .collect()
    }

    fn upstream(pairs: &[(&str, &[(&str, Value)])]) -> UpstreamOutputs {
        pairs
            .iter()
            .map(|(tpl, outs)| {
                (
                    (*tpl).to_string(),
                    outs.iter()
                        .map(|(k, v)| ((*k).to_string(), v.clone()))
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn resolves_real_upstream_outputs_type_preserving() {
        let r = refs(&[
            ("vpc_id", "vpc", "vpc_id", None),
            ("subnets", "vpc", "private_subnets", None),
        ]);
        let up = upstream(&[(
            "vpc",
            &[
                ("vpc_id", json!("vpc-123")),
                ("private_subnets", json!(["a", "b"])),
            ],
        )]);
        let res = resolve_dependency_vars(&r, &up);
        assert_eq!(res.resolved["vpc_id"], json!("vpc-123"));
        assert_eq!(
            res.resolved["subnets"],
            json!(["a", "b"]),
            "array output preserved, not stringified"
        );
        assert!(res.fully_satisfied(), "all real → apply-safe");
        assert!(res.unresolved_templates.is_empty());
    }

    #[test]
    fn missing_upstream_with_mock_is_plan_able_not_apply_able() {
        let r = refs(&[("vpc_id", "vpc", "vpc_id", Some(json!("vpc-mock")))]);
        let up = upstream(&[]); // vpc not applied yet
        let res = resolve_dependency_vars(&r, &up);
        assert_eq!(res.mocked["vpc_id"], json!("vpc-mock"));
        assert!(res.resolved.is_empty());
        assert!(
            res.unresolved_templates.is_empty(),
            "mock satisfies the plan gate"
        );
        assert!(
            !res.fully_satisfied(),
            "mocked ⇒ NOT apply-safe (must wait for the real output)"
        );
        assert_eq!(res.all_variables()["vpc_id"], json!("vpc-mock"));
    }

    #[test]
    fn missing_upstream_without_mock_is_an_unresolved_dependency() {
        let r = refs(&[
            ("a", "alpha", "k", None),
            ("b", "beta", "k", None),
            ("c", "alpha", "k2", None), // same upstream, deduped
        ]);
        let up = upstream(&[("beta", &[("k", json!(1))])]); // only beta ready
        let res = resolve_dependency_vars(&r, &up);
        assert_eq!(res.resolved["b"], json!(1));
        assert_eq!(
            res.unresolved_templates,
            vec!["alpha".to_string()],
            "alpha missing+no-mock, deduped"
        );
        assert!(!res.fully_satisfied());
    }

    #[test]
    fn edges_are_the_distinct_upstreams_for_the_dag() {
        let r = refs(&[
            ("a", "vpc", "id", None),
            ("b", "vpc", "cidr", None),
            ("c", "iam", "role", None),
        ]);
        assert_eq!(
            dependency_edges(&r),
            vec!["iam".to_string(), "vpc".to_string()],
            "deduped + sorted"
        );
    }

    #[test]
    fn no_refs_is_trivially_satisfied() {
        let res = resolve_dependency_vars(&BTreeMap::new(), &UpstreamOutputs::new());
        assert!(res.fully_satisfied());
        assert!(res.all_variables().is_empty());
    }
}
