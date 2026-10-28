use std::collections::{BTreeMap, BTreeSet};

use pangea_leaf::address::{leaf_key, LeafKey};
use pangea_leaf::admission::{decide_with, skip_declaration, Admission, AdmissionMode, Fix};
use pangea_leaf::admit::{admit, Candidate, Caps, InFlight, PriorityMode, WorkClass};
use pangea_leaf::classify::classify;
use pangea_leaf::dag::{DagError, TemplateDag};
use pangea_leaf::drift::{drift_due, DriftLeaf};
use pangea_leaf::names::leaf_names;
use pangea_leaf::partition::Partition;
use pangea_leaf::ratchet::{next_status, CycleWindow, LeafStatus};
use pangea_leaf::rollup::{judge, LeafState, LeafView};
use pangea_leaf::schedule::{blocked_by, inputs_digest, stale_leaves, Upstream};
use serde_json::{json, Value};

fn s(v: &Value) -> String {
    v.as_str().expect("text").to_string()
}

fn strs(v: &Value) -> Vec<String> {
    v.as_array().expect("list").iter().map(s).collect()
}

fn opt_u64(v: &Value) -> Option<u64> {
    v.as_u64()
}

fn key_of(v: &Value) -> LeafKey {
    let raw = s(&v["value"]);
    match v["kind"].as_str().expect("kind") {
        "keyed" => LeafKey::Keyed(raw),
        "module" => LeafKey::Module(raw),
        _ => LeafKey::Address(raw),
    }
}

fn key_json(k: &LeafKey) -> Value {
    json!({"kind": k.tag(), "value": k.raw()})
}

fn admission_json(a: &Admission) -> Value {
    match a {
        Admission::Admit => json!({"verdict": "admit"}),
        Admission::AdmitOff => json!({"verdict": "admitOff"}),
        Admission::AdmitGrandfathered => json!({"verdict": "admitGrandfathered"}),
        Admission::AdmitSkipped(r) => json!({"verdict": "admitSkipped", "reason": r.as_str()}),
        Admission::Refuse(m) => json!({"verdict": "refuse", "message": m}),
    }
}

fn class_of(v: &Value) -> WorkClass {
    match v.as_str().expect("class") {
        "apply" => WorkClass::Apply,
        "change" => WorkClass::Change,
        "unblocked" => WorkClass::Unblocked,
        "retry" => WorkClass::Retry,
        _ => WorkClass::Drift,
    }
}

fn counts(v: &Value) -> BTreeMap<String, usize> {
    v.as_object()
        .map(|m| {
            m.iter()
                .map(|(k, n)| (k.clone(), n.as_u64().expect("count") as usize))
                .collect()
        })
        .unwrap_or_default()
}

fn state_of(v: &Value) -> LeafState {
    let text = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
    match v["kind"].as_str().expect("state kind") {
        "ready" => LeafState::Ready,
        "planning" => LeafState::Planning,
        "applying" => LeafState::Applying,
        "failed" => LeafState::Failed {
            reason: text("reason").unwrap_or_default(),
            since: text("since"),
        },
        "stalled" => LeafState::Stalled {
            state: text("state").unwrap_or_default(),
            since: text("since"),
        },
        _ => LeafState::RateLimited {
            until: text("until").unwrap_or_default(),
        },
    }
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: happa_oracle INPUTS.json");
    let input: Value = serde_json::from_str(&std::fs::read_to_string(path).expect("read inputs"))
        .expect("inputs are JSON");

    let addresses = strs(&input["addresses"]);
    let keys: Vec<Value> = addresses
        .iter()
        .map(|a| key_json(&leaf_key(a).expect("fixture addresses are well formed")))
        .collect();
    let bad: Vec<Value> = strs(&input["bad_addresses"])
        .iter()
        .map(|a| json!(leaf_key(a).expect_err("a bad address").to_string()))
        .collect();

    let mut partition = Partition::build(&addresses).expect("partition");
    let refs: Vec<(LeafKey, LeafKey)> = input["refs"]
        .as_array()
        .expect("refs")
        .iter()
        .map(|p| (key_of(&p[0]), key_of(&p[1])))
        .collect();
    partition.merge(&refs);
    let leaves: Vec<Value> = partition
        .leaves()
        .map(|l| {
            json!({
                "key": key_json(&l.key),
                "merged_from": l.merged_from.iter().map(key_json).collect::<Vec<_>>(),
                "addresses": l.addresses,
            })
        })
        .collect();
    let b = partition.bijection(&addresses);
    let leaf_keys: Vec<LeafKey> = partition.leaves().map(|l| l.key.clone()).collect();
    let names = leaf_names(&s(&input["base"]), &leaf_keys);
    let names_json: Vec<Value> = leaf_keys
        .iter()
        .map(|k| json!([key_json(k), names[k]]))
        .collect();

    let classified: Vec<Value> = input["classify"]
        .as_array()
        .expect("classify")
        .iter()
        .map(|c| {
            let addrs = strs(&c["addresses"]);
            let r = classify(
                addrs.iter().map(String::as_str),
                opt_u64(&c["p95"]),
                opt_u64(&c["budget"]),
            )
            .expect("classify");
            serde_json::to_value(r).expect("classification")
        })
        .collect();

    let named: Vec<Value> = input["names"]
        .as_array()
        .expect("names")
        .iter()
        .map(|n| {
            let ks: Vec<LeafKey> = n["keys"].as_array().expect("keys").iter().map(key_of).collect();
            let m = leaf_names(&s(&n["base"]), &ks);
            json!(ks.iter().map(|k| m[k].clone()).collect::<Vec<_>>())
        })
        .collect();

    let digests: Vec<Value> = input["digests"]
        .as_array()
        .expect("digests")
        .iter()
        .map(|d| {
            let part = |i: usize| d.get(i).filter(|v| !v.is_null());
            json!(inputs_digest(part(0), part(1), part(2)))
        })
        .collect();

    let changed: Vec<Value> = input["changed"]
        .as_array()
        .expect("changed")
        .iter()
        .map(|rows| {
            let rows: Vec<(String, Option<String>, String)> = rows
                .as_array()
                .expect("rows")
                .iter()
                .map(|r| (s(&r[0]), r[1].as_str().map(str::to_string), s(&r[2])))
                .collect();
            json!(stale_leaves(
                rows.iter().map(|(n, rec, cur)| (n.as_str(), rec.as_deref(), cur.as_str()))
            ))
        })
        .collect();

    let blocked: Vec<Value> = input["blocked"]
        .as_array()
        .expect("blocked")
        .iter()
        .map(|c| {
            let ups: Vec<Upstream> = c["upstreams"]
                .as_array()
                .expect("upstreams")
                .iter()
                .map(|u| Upstream {
                    template: s(&u["template"]),
                    has_mock: u["has_mock"].as_bool().unwrap_or(false),
                })
                .collect();
            let ready: BTreeSet<String> = strs(&c["ready"]).into_iter().collect();
            json!(blocked_by(
                &s(&c["leaf"]),
                &ups,
                &ready,
                c["applying"].as_bool().unwrap_or(false)
            ))
        })
        .collect();

    let admissions: Vec<Value> = input["admission"]
        .as_array()
        .expect("admission")
        .iter()
        .map(|c| {
            let mode = AdmissionMode::parse(&s(&c["mode"])).expect("mode");
            let cls = pangea_leaf::classify::classify_counts(
                c["leaf_keys"].as_u64().expect("leaf_keys") as usize,
                None,
                None,
            );
            let skip = skip_declaration(c["skip"].as_str());
            let fix = Fix {
                workspace: c["workspace"].as_str().map(str::to_string),
                draft: c["draft"].as_str().map(str::to_string),
            };
            admission_json(&decide_with(
                mode,
                c["never_reconciled"].as_bool().expect("never_reconciled"),
                &cls,
                &skip,
                &fix,
            ))
        })
        .collect();

    let ratchets: Vec<Value> = input["ratchet"]
        .as_array()
        .expect("ratchet")
        .iter()
        .map(|c| {
            let prev: Option<LeafStatus> = if c["prev"].is_null() {
                None
            } else {
                Some(serde_json::from_value(c["prev"].clone()).expect("prev status"))
            };
            let st = next_status(
                prev.as_ref(),
                c["leaf_keys"].as_u64().expect("leaf_keys") as u32,
                c["fat"].as_bool().unwrap_or(false),
                opt_u64(&c["p95"]),
                c["tolerance"].as_u64().expect("tolerance") as u32,
                None,
                None,
            );
            serde_json::to_value(st).expect("status")
        })
        .collect();

    let p95s: Vec<Value> = input["p95"]
        .as_array()
        .expect("p95")
        .iter()
        .map(|w| {
            let mut win = CycleWindow::default();
            for ms in w.as_array().expect("samples") {
                win.push(ms.as_u64().expect("ms"));
            }
            json!(win.p95_ms())
        })
        .collect();

    let dags: Vec<Value> = input["dags"]
        .as_array()
        .expect("dags")
        .iter()
        .map(|d| {
            let mut dag = TemplateDag::new();
            for row in d.as_array().expect("rows") {
                dag.add(&s(&row[0]), &strs(&row[1]));
            }
            match dag.apply_order() {
                Ok(o) => json!({"order": o}),
                Err(DagError::Cycle(c)) => json!({"stuck": c}),
            }
        })
        .collect();

    let admitted: Vec<Value> = input["admit"]
        .as_array()
        .expect("admit")
        .iter()
        .map(|c| {
            let cands: Vec<Candidate> = c["candidates"]
                .as_array()
                .expect("candidates")
                .iter()
                .map(|x| Candidate {
                    name: s(&x["name"]),
                    scope: s(&x["scope"]),
                    credential: x["credential"].as_str().map(str::to_string),
                    class: class_of(&x["class"]),
                    depth: x["depth"].as_u64().unwrap_or(0) as u32,
                    since: x["since"].as_u64().unwrap_or(0),
                })
                .collect();
            let k = &c["caps"];
            let caps = Caps {
                global: k["global"].as_u64().expect("global") as usize,
                per_scope: k["per_scope"].as_u64().expect("per_scope") as usize,
                scope_caps: counts(&k["scope_caps"]),
                per_credential: k["per_credential"].as_u64().unwrap_or(0) as usize,
                drift_share_permille: k["drift_share_permille"].as_u64().unwrap_or(500) as u16,
                priority: PriorityMode::parse(k["priority"].as_str().unwrap_or("on"))
                    .expect("priority"),
            };
            let f = &c["in_flight"];
            let in_flight = InFlight {
                total: f["total"].as_u64().unwrap_or(0) as usize,
                by_scope: counts(&f["by_scope"]),
                drift_by_scope: counts(&f["drift_by_scope"]),
                by_credential: counts(&f["by_credential"]),
            };
            json!(admit(&cands, &caps, &in_flight))
        })
        .collect();

    let judged: Vec<Value> = input["judge"]
        .as_array()
        .expect("judge")
        .iter()
        .map(|c| {
            let views: Vec<LeafView> = c["leaves"]
                .as_array()
                .expect("leaves")
                .iter()
                .map(|l| LeafView {
                    name: s(&l["name"]),
                    state: state_of(&l["state"]),
                    upstreams: strs(&l["upstreams"]),
                })
                .collect();
            serde_json::to_value(judge(&views, c["cap"].as_u64().unwrap_or(50) as usize))
                .expect("rollup")
        })
        .collect();

    let drifted: Vec<Value> = input["drift"]
        .as_array()
        .expect("drift")
        .iter()
        .map(|c| {
            let leaves: Vec<DriftLeaf> = c["leaves"]
                .as_array()
                .expect("leaves")
                .iter()
                .map(|l| DriftLeaf {
                    name: s(&l["name"]),
                    last_check: l["last_check"].as_u64(),
                })
                .collect();
            json!(drift_due(
                &leaves,
                c["now"].as_u64().expect("now"),
                c["interval"].as_u64().expect("interval")
            ))
        })
        .collect();

    let out = json!({
        "keys": keys,
        "bad_addresses": bad,
        "leaves": leaves,
        "bijection": b,
        "names_of_leaves": names_json,
        "classify": classified,
        "names": named,
        "digests": digests,
        "changed": changed,
        "blocked": blocked,
        "admission": admissions,
        "ratchet": ratchets,
        "p95": p95s,
        "dags": dags,
        "admit": admitted,
        "judge": judged,
        "drift": drifted,
    });
    println!("{}", serde_json::to_string_pretty(&out).expect("json"));
}
