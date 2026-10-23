use assert_cmd::Command;
use serde_json::{json, Value};
use std::path::PathBuf;

fn scratch(name: &str, body: &Value) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pangea-leaf-cli-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, serde_json::to_string(body).unwrap()).unwrap();
    path
}

fn template() -> Value {
    json!({
        "metadata": {"name": "fat", "namespace": "default"},
        "spec": {"pangeaNamespace": "ns", "templateName": "t", "variables": {"owner": "o"}, "source": {}}
    })
}

fn state() -> Value {
    json!({"version": 4, "resources": [
        {"mode": "managed", "type": "gh_repo", "name": "a", "instances": [{}]},
        {"mode": "managed", "type": "gh_repo", "name": "b", "instances": [{}]},
        {"mode": "managed", "type": "gh_perm", "name": "a", "instances": [{}]}
    ]})
}

fn run(args: &[&str]) -> std::process::Output {
    Command::cargo_bin("pangea").unwrap().args(args).output().unwrap()
}

#[test]
fn a_fat_template_plans_ready_and_emits_manifests() {
    let t = scratch("t.json", &template());
    let s = scratch("s.json", &state());
    let out = std::env::temp_dir().join(format!("pangea-leaf-emit-{}", std::process::id()));
    let o = run(&[
        "leaf", "plan",
        "--template", t.to_str().unwrap(),
        "--state", s.to_str().unwrap(),
        "--emit-dir", out.to_str().unwrap(),
        "--summary",
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["verdict"], "ready");
    assert_eq!(v["leafCount"], 3);
    assert_eq!(v["bijection"]["ok"], true);
    assert!(out.join("catalog.yaml").exists());
    assert!(out.join("fat-patch.yaml").exists());
    assert_eq!(std::fs::read_dir(out.join("leaves")).unwrap().count(), 3);
}

#[test]
fn grouping_references_merges_a_repo_with_its_permission() {
    let t = scratch("tg.json", &template());
    let s = scratch("sg.json", &state());
    let p = scratch(
        "pg.json",
        &json!({"resource_changes": [
            {"address": {"module": [], "kind": "managed", "type_id": "gh_repo", "name": "a", "key": null},
             "action": "no_op", "after": {}},
            {"address": {"module": [], "kind": "managed", "type_id": "gh_repo", "name": "b", "key": null},
             "action": "no_op", "after": {}},
            {"address": {"module": [], "kind": "managed", "type_id": "gh_perm", "name": "a", "key": null},
             "action": "no_op", "after": {"repository": "${gh_repo.a.name}"}}
        ]}),
    );
    let o = run(&[
        "leaf", "plan",
        "--template", t.to_str().unwrap(),
        "--state", s.to_str().unwrap(),
        "--plan", p.to_str().unwrap(),
        "--group-references",
        "--summary",
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["leafCount"], 2);
    assert_eq!(v["projected"]["add"], 0);
}

#[test]
fn a_plan_with_a_change_exits_nonzero_and_names_the_failed_check() {
    let t = scratch("tr.json", &template());
    let s = scratch("sr.json", &state());
    let p = scratch(
        "pr.json",
        &json!({"resource_changes": [
            {"address": "gh_repo.a", "change": {"actions": ["update"]}}
        ]}),
    );
    let o = run(&[
        "leaf", "plan",
        "--template", t.to_str().unwrap(),
        "--state", s.to_str().unwrap(),
        "--plan", p.to_str().unwrap(),
        "--summary",
    ]);
    assert_eq!(o.status.code(), Some(2));
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["verdict"], "refused");
}

#[test]
fn a_malformed_state_address_fails_closed() {
    let t = scratch("tm.json", &template());
    let s = scratch(
        "sm.json",
        &json!({"resources": [{"mode": "managed", "type": "a", "name": "b", "module": "module..x", "instances": [{}]}]}),
    );
    let o = run(&[
        "leaf", "plan",
        "--template", t.to_str().unwrap(),
        "--state", s.to_str().unwrap(),
        "--summary",
    ]);
    assert!(!o.status.success());
}

#[test]
fn a_single_leaf_state_is_not_fat() {
    let t = scratch("tn.json", &template());
    let a = std::env::temp_dir().join(format!("pangea-leaf-addr-{}.txt", std::process::id()));
    std::fs::write(&a, "x.y[\"k\"]\nz.w[\"k\"]\n").unwrap();
    let o = run(&[
        "leaf", "plan",
        "--template", t.to_str().unwrap(),
        "--addresses", a.to_str().unwrap(),
        "--summary",
    ]);
    assert!(o.status.success());
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["verdict"], "notFat");
}
