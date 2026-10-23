use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::address::{strip_index, LeafKey};
use crate::classify::{classify, Classification};
use crate::dag::{DagError, TemplateDag};
use crate::names::leaf_names;
use crate::partition::{Bijection, Leaf, Partition, PartitionError};
use crate::sources::{PlanAction, PlanView};

pub const LEAF_KEY_VARIABLE: &str = "leaf_key";
pub const LEAF_OF_LABEL: &str = "pangea.pleme.io/leaf-of";
pub const WORKSPACE_LABEL: &str = "pangea.pleme.io/workspace";
pub const API_VERSION: &str = "pangea.pleme.io/v1alpha1";

#[derive(Debug, Clone, thiserror::Error)]
pub enum PlanError {
    #[error(transparent)]
    Partition(#[from] PartitionError),
    #[error("template has no metadata.name")]
    NoName,
    #[error("template has no spec object")]
    NoSpec,
}

#[derive(Debug, Clone)]
pub struct PlannerInput<'a> {
    pub template: &'a Value,
    pub addresses: &'a [String],
    pub plan: Option<&'a PlanView>,
    pub cycle_p95_ms: Option<u64>,
    pub budget_ms: Option<u64>,
    pub group_references: bool,
    pub max_concurrent_leaves: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "detail", rename_all = "camelCase")]
pub enum CheckStatus {
    Pass,
    Fail(String),
    Unproven(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Check {
    pub name: String,
    #[serde(flatten)]
    pub status: CheckStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Verdict {
    NotFat,
    NothingToSplit,
    Refused,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Projected {
    pub add: usize,
    pub change: usize,
    pub destroy: usize,
    pub no_op: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeafPlan {
    pub template: String,
    pub key: LeafKey,
    pub merged_from: Vec<LeafKey>,
    pub addresses: Vec<String>,
    pub upstreams: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateMove {
    pub from_template: String,
    pub to_template: String,
    pub pangea_namespace: Option<String>,
    pub addresses: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationPlan {
    pub fat_template: String,
    pub classification: Classification,
    pub verdict: Verdict,
    pub checks: Vec<Check>,
    pub bijection: Option<Bijection>,
    pub projected: Option<Projected>,
    pub leaf_count: usize,
    pub partition_sizes: BTreeMap<usize, usize>,
    pub leaves: Vec<LeafPlan>,
    pub state_moves: Vec<StateMove>,
    pub catalog: Option<Value>,
    pub leaf_templates: Vec<Value>,
    pub fat_patch: Option<Value>,
}

fn string_at<'a>(v: &'a Value, ptr: &str) -> Option<&'a str> {
    v.pointer(ptr).and_then(Value::as_str)
}

fn leaf_manifest(
    fat: &Value,
    fat_name: &str,
    leaf: &LeafPlan,
    catalog: &str,
    variable_refs: &BTreeMap<String, Value>,
) -> Value {
    let mut spec: Map<String, Value> = fat
        .get("spec")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    spec.insert(
        "variables".into(),
        json!({ LEAF_KEY_VARIABLE: leaf.key.raw() }),
    );
    spec.insert("destroyProtection".into(), json!(true));
    spec.insert("suspend".into(), json!(false));
    spec.insert("importPolicy".into(), json!({ "autoOnConflict": true }));
    spec.remove("destroyAuthorization");
    spec.remove("approvedPlanHash");
    if variable_refs.is_empty() {
        spec.remove("variableRefs");
    } else {
        spec.insert("variableRefs".into(), json!(variable_refs));
    }
    let mut meta = Map::new();
    meta.insert("name".into(), json!(leaf.template));
    if let Some(ns) = string_at(fat, "/metadata/namespace") {
        meta.insert("namespace".into(), json!(ns));
    }
    meta.insert(
        "labels".into(),
        json!({ WORKSPACE_LABEL: catalog, LEAF_OF_LABEL: fat_name }),
    );
    json!({
        "apiVersion": API_VERSION,
        "kind": "InfrastructureTemplate",
        "metadata": Value::Object(meta),
        "spec": Value::Object(spec),
    })
}

pub fn plan_migration(input: &PlannerInput<'_>) -> Result<MigrationPlan, PlanError> {
    let fat_name = string_at(input.template, "/metadata/name")
        .ok_or(PlanError::NoName)?
        .to_string();
    let fat_spec = input
        .template
        .get("spec")
        .and_then(Value::as_object)
        .ok_or(PlanError::NoSpec)?;
    let pangea_namespace = fat_spec
        .get("pangeaNamespace")
        .and_then(Value::as_str)
        .map(str::to_string);

    let classification = classify(
        input.addresses.iter().map(String::as_str),
        input.cycle_p95_ms,
        input.budget_ms,
    )
    .map_err(PartitionError::from)?;

    let empty = |verdict: Verdict, checks: Vec<Check>| MigrationPlan {
        fat_template: fat_name.clone(),
        classification: classification.clone(),
        verdict,
        checks,
        bijection: None,
        projected: None,
        leaf_count: 0,
        partition_sizes: BTreeMap::new(),
        leaves: Vec::new(),
        state_moves: Vec::new(),
        catalog: None,
        leaf_templates: Vec::new(),
        fat_patch: None,
    };

    if !classification.fat {
        return Ok(empty(Verdict::NotFat, Vec::new()));
    }

    let mut partition = Partition::build(input.addresses)?;

    if input.group_references {
        if let Some(view) = input.plan {
            partition.merge(&reference_leaf_edges(&partition, view));
        }
    }

    if partition.len() <= 1 {
        let mut p = empty(Verdict::NothingToSplit, Vec::new());
        p.leaf_count = partition.len();
        return Ok(p);
    }

    let keys: Vec<LeafKey> = partition.leaves().map(|l| l.key.clone()).collect();
    let names = leaf_names(&fat_name, &keys);
    let mut partition_sizes: BTreeMap<usize, usize> = BTreeMap::new();
    for l in partition.leaves() {
        *partition_sizes.entry(l.addresses.len()).or_default() += 1;
    }

    let edges = singleton_edges(&partition, input.plan);
    let mut upstreams: BTreeMap<LeafKey, BTreeSet<LeafKey>> = BTreeMap::new();
    for (from, to) in &edges {
        upstreams.entry(from.clone()).or_default().insert(to.clone());
    }

    let mut dag = TemplateDag::new();
    for l in partition.leaves() {
        let ups: Vec<String> = upstreams
            .get(&l.key)
            .map(|s| s.iter().map(|k| names[k].clone()).collect())
            .unwrap_or_default();
        dag.add(&names[&l.key], &ups);
    }

    let mut checks = Vec::new();

    let bijection = partition.bijection(input.addresses);
    checks.push(Check {
        name: "bijection".into(),
        status: if bijection.ok {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail(format!(
                "before {} ({}) != after {} ({}), disjoint={}",
                bijection.hash_before,
                bijection.count_before,
                bijection.hash_after,
                bijection.count_after,
                bijection.disjoint
            ))
        },
    });

    checks.push(Check {
        name: "acyclic".into(),
        status: match dag.apply_order() {
            Ok(_) => CheckStatus::Pass,
            Err(DagError::Cycle(c)) => CheckStatus::Fail(format!("cycle among {}", c.join(", "))),
        },
    });

    let mut seen_names = BTreeSet::new();
    let unique = names.values().all(|n| seen_names.insert(n.as_str()));
    checks.push(Check {
        name: "unique-names".into(),
        status: if unique {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail("two leaves share a template name".into())
        },
    });

    let projected = input.plan.map(|view| project(view));
    checks.push(Check {
        name: "no-change".into(),
        status: match (&projected, input.plan) {
            (Some(p), Some(view)) => {
                let unplaced: Vec<&String> = view
                    .changes
                    .iter()
                    .filter(|(a, act)| !act.adds() && partition.leaf_of(a).is_none())
                    .map(|(a, _)| a)
                    .filter(|a| !crate::address::is_data(a))
                    .collect();
                if p.add + p.change + p.destroy > 0 {
                    CheckStatus::Fail(format!(
                        "plan shows {} add, {} change, {} destroy",
                        p.add, p.change, p.destroy
                    ))
                } else if !unplaced.is_empty() {
                    CheckStatus::Fail(format!(
                        "{} planned addresses are in no leaf, first `{}`",
                        unplaced.len(),
                        unplaced[0]
                    ))
                } else {
                    CheckStatus::Pass
                }
            }
            _ => CheckStatus::Unproven("no plan supplied; step 5 runs live".into()),
        },
    });

    let verdict = if checks.iter().any(|c| matches!(c.status, CheckStatus::Fail(_))) {
        Verdict::Refused
    } else {
        Verdict::Ready
    };

    let leaf_plans: Vec<LeafPlan> = partition
        .leaves()
        .map(|l: &Leaf| LeafPlan {
            template: names[&l.key].clone(),
            key: l.key.clone(),
            merged_from: l.merged_from.clone(),
            addresses: l.addresses.clone(),
            upstreams: upstreams
                .get(&l.key)
                .map(|s| s.iter().map(|k| names[k].clone()).collect())
                .unwrap_or_default(),
        })
        .collect();

    let mut plan = MigrationPlan {
        fat_template: fat_name.clone(),
        classification,
        verdict: verdict.clone(),
        checks,
        bijection: Some(bijection),
        projected,
        leaf_count: leaf_plans.len(),
        partition_sizes,
        leaves: leaf_plans,
        state_moves: Vec::new(),
        catalog: None,
        leaf_templates: Vec::new(),
        fat_patch: None,
    };

    if verdict == Verdict::Ready {
        plan.state_moves = plan
            .leaves
            .iter()
            .map(|l| StateMove {
                from_template: fat_name.clone(),
                to_template: l.template.clone(),
                pangea_namespace: pangea_namespace.clone(),
                addresses: l.addresses.clone(),
            })
            .collect();
        plan.catalog = Some(catalog_manifest(
            &fat_name,
            fat_spec.get("variables"),
            input.max_concurrent_leaves,
        ));
        plan.leaf_templates = plan
            .leaves
            .iter()
            .map(|l| {
                let refs = variable_refs_for(l, &edges, &names);
                leaf_manifest(input.template, &fat_name, l, &fat_name, &refs)
            })
            .collect();
        plan.fat_patch = Some(json!({ "spec": { "suspend": true } }));
    }
    Ok(plan)
}

fn catalog_manifest(name: &str, variables: Option<&Value>, max: Option<u32>) -> Value {
    let mut spec = Map::new();
    spec.insert("source".into(), json!({}));
    if let Some(v) = variables {
        spec.insert("variables".into(), v.clone());
    }
    if let Some(m) = max {
        spec.insert("maxConcurrentLeaves".into(), json!(m));
    }
    json!({
        "apiVersion": API_VERSION,
        "kind": "WorkspaceCatalog",
        "metadata": { "name": name },
        "spec": Value::Object(spec),
    })
}

fn stripped_index(partition: &Partition) -> BTreeMap<String, BTreeSet<LeafKey>> {
    let mut m: BTreeMap<String, BTreeSet<LeafKey>> = BTreeMap::new();
    for l in partition.leaves() {
        for a in &l.addresses {
            if let Ok(s) = strip_index(a) {
                m.entry(s).or_default().insert(l.key.clone());
            }
        }
    }
    m
}

fn reference_leaf_edges(partition: &Partition, view: &PlanView) -> Vec<(LeafKey, LeafKey)> {
    let idx = stripped_index(partition);
    let mut out = Vec::new();
    for (from, to) in &view.references {
        let (Some(fs), Some(ts)) = (idx.get(from), idx.get(to)) else {
            continue;
        };
        for f in fs {
            for t in ts {
                if f != t && (fs.len() == 1 || ts.len() == 1) {
                    out.push((f.clone(), t.clone()));
                }
            }
        }
    }
    out
}

fn singleton_edges(partition: &Partition, view: Option<&PlanView>) -> Vec<(LeafKey, LeafKey)> {
    let Some(view) = view else {
        return Vec::new();
    };
    let idx = stripped_index(partition);
    let mut out = BTreeSet::new();
    for (from, to) in &view.references {
        let (Some(fs), Some(ts)) = (idx.get(from), idx.get(to)) else {
            continue;
        };
        if ts.len() != 1 {
            continue;
        }
        let t = ts.iter().next().expect("one element");
        for f in fs {
            if f != t {
                out.insert((f.clone(), t.clone()));
            }
        }
    }
    out.into_iter().collect()
}

fn variable_refs_for(
    leaf: &LeafPlan,
    edges: &[(LeafKey, LeafKey)],
    names: &BTreeMap<LeafKey, String>,
) -> BTreeMap<String, Value> {
    let mut refs = BTreeMap::new();
    for (from, to) in edges {
        if names[from] != leaf.template {
            continue;
        }
        let up = &names[to];
        let var = format!("upstream_{}", up.replace('-', "_"));
        refs.insert(
            var,
            json!({ "templateRef": { "name": up }, "outputKey": "ready" }),
        );
    }
    refs
}

fn project(view: &PlanView) -> Projected {
    let mut p = Projected {
        add: 0,
        change: 0,
        destroy: 0,
        no_op: 0,
    };
    for (addr, action) in &view.changes {
        if crate::address::is_data(addr) || matches!(action, PlanAction::Read) {
            continue;
        }
        if action.adds() {
            p.add += 1;
        }
        if action.changes() {
            p.change += 1;
        }
        if action.destroys() {
            p.destroy += 1;
        }
        if matches!(action, PlanAction::NoOp) {
            p.no_op += 1;
        }
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::plan_view;

    fn tmpl() -> Value {
        json!({
            "metadata": {"name": "fat", "namespace": "default"},
            "spec": {
                "pangeaNamespace": "ns",
                "templateName": "t",
                "variables": {"owner": "o", "repos": [1, 2]},
                "destroyProtection": true,
                "source": {}
            }
        })
    }

    fn addrs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    fn input<'a>(t: &'a Value, a: &'a [String], plan: Option<&'a PlanView>) -> PlannerInput<'a> {
        PlannerInput {
            template: t,
            addresses: a,
            plan,
            cycle_p95_ms: None,
            budget_ms: None,
            group_references: false,
            max_concurrent_leaves: None,
        }
    }

    #[test]
    fn a_single_leaf_template_is_not_fat() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "c.d[\"x\"]"]);
        let p = plan_migration(&input(&t, &a, None)).unwrap();
        assert_eq!(p.verdict, Verdict::NotFat);
        assert!(p.leaf_templates.is_empty());
    }

    #[test]
    fn a_fat_template_plans_one_leaf_per_key() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "c.d[\"x\"]", "a.b[\"y\"]"]);
        let p = plan_migration(&input(&t, &a, None)).unwrap();
        assert_eq!(p.verdict, Verdict::Ready);
        assert_eq!(p.leaf_count, 2);
        assert_eq!(p.partition_sizes.get(&2), Some(&1));
        assert_eq!(p.partition_sizes.get(&1), Some(&1));
        assert!(p.bijection.as_ref().unwrap().ok);
        assert_eq!(p.state_moves.len(), 2);
        assert_eq!(p.leaf_templates.len(), 2);
    }

    #[test]
    fn leaf_manifests_carry_the_key_protection_and_import_policy() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "a.b[\"y\"]"]);
        let p = plan_migration(&input(&t, &a, None)).unwrap();
        let m = &p.leaf_templates[0];
        assert_eq!(m["kind"], "InfrastructureTemplate");
        assert_eq!(m["spec"]["destroyProtection"], true);
        assert_eq!(m["spec"]["importPolicy"]["autoOnConflict"], true);
        assert_eq!(m["spec"]["variables"]["leaf_key"], "x");
        assert_eq!(m["metadata"]["labels"][WORKSPACE_LABEL], "fat");
        assert_eq!(m["spec"]["templateName"], "t");
    }

    #[test]
    fn shared_variables_are_lifted_to_the_catalog() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "a.b[\"y\"]"]);
        let mut i = input(&t, &a, None);
        i.max_concurrent_leaves = Some(6);
        let p = plan_migration(&i).unwrap();
        let c = p.catalog.unwrap();
        assert_eq!(c["kind"], "WorkspaceCatalog");
        assert_eq!(c["spec"]["variables"]["owner"], "o");
        assert_eq!(c["spec"]["maxConcurrentLeaves"], 6);
    }

    #[test]
    fn the_fat_template_is_suspended_never_deleted() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "a.b[\"y\"]"]);
        let p = plan_migration(&input(&t, &a, None)).unwrap();
        assert_eq!(p.fat_patch.unwrap()["spec"]["suspend"], true);
    }

    #[test]
    fn an_offline_plan_without_a_plan_file_is_unproven_not_passed() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "a.b[\"y\"]"]);
        let p = plan_migration(&input(&t, &a, None)).unwrap();
        let c = p.checks.iter().find(|c| c.name == "no-change").unwrap();
        assert!(matches!(c.status, CheckStatus::Unproven(_)));
    }

    #[test]
    fn a_no_op_plan_proves_zero_change() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "a.b[\"y\"]"]);
        let view = plan_view(&json!({"resource_changes":[
            {"address":"a.b[\"x\"]","change":{"actions":["no-op"]}},
            {"address":"a.b[\"y\"]","change":{"actions":["no-op"]}}
        ]}))
        .unwrap();
        let p = plan_migration(&input(&t, &a, Some(&view))).unwrap();
        assert_eq!(p.verdict, Verdict::Ready);
        assert_eq!(
            p.projected,
            Some(Projected { add: 0, change: 0, destroy: 0, no_op: 2 })
        );
        let c = p.checks.iter().find(|c| c.name == "no-change").unwrap();
        assert_eq!(c.status, CheckStatus::Pass);
    }

    #[test]
    fn a_plan_with_a_change_refuses_the_migration_and_emits_nothing() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "a.b[\"y\"]"]);
        let view = plan_view(&json!({"resource_changes":[
            {"address":"a.b[\"x\"]","change":{"actions":["update"]}},
            {"address":"a.b[\"y\"]","change":{"actions":["no-op"]}}
        ]}))
        .unwrap();
        let p = plan_migration(&input(&t, &a, Some(&view))).unwrap();
        assert_eq!(p.verdict, Verdict::Refused);
        assert!(p.leaf_templates.is_empty());
        assert!(p.catalog.is_none());
        assert!(p.state_moves.is_empty());
        assert!(p.fat_patch.is_none());
    }

    #[test]
    fn a_planned_address_outside_the_state_refuses() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "a.b[\"y\"]"]);
        let view = plan_view(&json!({"resource_changes":[
            {"address":"a.b[\"x\"]","change":{"actions":["no-op"]}},
            {"address":"a.b[\"y\"]","change":{"actions":["no-op"]}},
            {"address":"z.q","change":{"actions":["no-op"]}}
        ]}))
        .unwrap();
        let p = plan_migration(&input(&t, &a, Some(&view))).unwrap();
        assert_eq!(p.verdict, Verdict::Refused);
    }

    #[test]
    fn cross_leaf_references_to_a_singleton_become_variable_refs() {
        let t = tmpl();
        let a = addrs(&["gh.perm[\"x\"]", "gh.perm[\"y\"]", "module.net.vpc.main"]);
        let mut view = PlanView::default();
        view.references.insert(("gh.perm".into(), "module.net.vpc.main".into()));
        let p = plan_migration(&input(&t, &a, Some(&view))).unwrap();
        let x = p.leaves.iter().find(|l| l.key == LeafKey::Keyed("x".into())).unwrap();
        assert_eq!(x.upstreams.len(), 1);
        let m = p
            .leaf_templates
            .iter()
            .find(|m| m["metadata"]["name"] == json!(x.template))
            .unwrap();
        assert!(m["spec"]["variableRefs"].is_object());
    }

    #[test]
    fn a_reference_cycle_among_singletons_refuses() {
        let t = tmpl();
        let a = addrs(&["a.one", "b.two"]);
        let mut view = PlanView::default();
        view.references.insert(("a.one".into(), "b.two".into()));
        view.references.insert(("b.two".into(), "a.one".into()));
        let p = plan_migration(&input(&t, &a, Some(&view))).unwrap();
        assert_eq!(p.verdict, Verdict::Refused);
        let c = p.checks.iter().find(|c| c.name == "acyclic").unwrap();
        assert!(matches!(c.status, CheckStatus::Fail(_)));
    }

    #[test]
    fn grouping_references_merges_resources_that_name_each_other() {
        let t = tmpl();
        let a = addrs(&["gh.repo_a", "gh.perm_a", "gh.repo_b", "gh.perm_b"]);
        let mut view = PlanView::default();
        view.references.insert(("gh.perm_a".into(), "gh.repo_a".into()));
        view.references.insert(("gh.perm_b".into(), "gh.repo_b".into()));
        let mut i = input(&t, &a, Some(&view));
        i.group_references = true;
        let p = plan_migration(&i).unwrap();
        assert_eq!(p.leaf_count, 2);
        assert!(p.leaves.iter().all(|l| l.addresses.len() == 2));
        assert!(p.bijection.unwrap().ok);
    }

    #[test]
    fn the_plan_is_deterministic() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]", "a.b[\"y\"]", "c.d"]);
        let one = plan_migration(&input(&t, &a, None)).unwrap();
        let two = plan_migration(&input(&t, &a, None)).unwrap();
        assert_eq!(one, two);
    }

    #[test]
    fn a_slow_single_leaf_has_nothing_to_split() {
        let t = tmpl();
        let a = addrs(&["a.b[\"x\"]"]);
        let mut i = input(&t, &a, None);
        i.cycle_p95_ms = Some(10);
        i.budget_ms = Some(1);
        let p = plan_migration(&i).unwrap();
        assert_eq!(p.verdict, Verdict::NothingToSplit);
    }

    #[test]
    fn a_malformed_address_stops_the_plan() {
        let t = tmpl();
        let a = addrs(&["a..b"]);
        assert!(plan_migration(&input(&t, &a, None)).is_err());
    }
}
