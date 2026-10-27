use assert_cmd::Command;
use serde_json::{json, Value};
use std::path::PathBuf;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("pangea-leaf-migrate-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn state() -> Vec<u8> {
    serde_json::to_vec_pretty(
        &json!({"version": 4, "serial": 9, "lineage": "l", "resources": [
            {"mode": "managed", "type": "gh_repo", "name": "r", "instances": [
                {"index_key": "a", "attributes": {"id": "a"}},
                {"index_key": "b", "attributes": {"id": "b"}}]}
        ]}),
    )
    .unwrap()
}

fn moves(second: &[&str]) -> Value {
    json!([
        {"fromTemplate": "fat", "toTemplate": "fat-a", "pangeaNamespace": "ns", "addresses": ["gh_repo.r[\"a\"]"]},
        {"fromTemplate": "fat", "toTemplate": "fat-b", "pangeaNamespace": "ns", "addresses": second}
    ])
}

fn run(args: &[&str]) -> std::process::Output {
    Command::cargo_bin("pangea")
        .unwrap()
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn a_forged_move_set_exits_2_and_writes_no_leaf() {
    let d = dir("forged");
    std::fs::write(d.join("s.json"), state()).unwrap();
    std::fs::write(d.join("m.json"), moves(&[]).to_string()).unwrap();
    let out_dir = d.join("out");
    let o = run(&[
        "leaf",
        "migrate",
        "--fat-template",
        "fat",
        "--state",
        d.join("s.json").to_str().unwrap(),
        "--moves",
        d.join("m.json").to_str().unwrap(),
        "--emit-dir",
        out_dir.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(2));
    assert!(!out_dir.join("fat-a.tfstate.json").exists());
    assert!(!out_dir.join("receipt.json").exists());
}

#[test]
fn forward_then_reverse_round_trips_the_fat_state_byte_equal() {
    let d = dir("round");
    std::fs::write(d.join("s.json"), state()).unwrap();
    std::fs::write(d.join("m.json"), moves(&["gh_repo.r[\"b\"]"]).to_string()).unwrap();
    let out_dir = d.join("out");
    let o = run(&[
        "leaf",
        "migrate",
        "--fat-template",
        "fat",
        "--state",
        d.join("s.json").to_str().unwrap(),
        "--moves",
        d.join("m.json").to_str().unwrap(),
        "--emit-dir",
        out_dir.to_str().unwrap(),
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(out_dir.join("fat-a.tfstate.json").exists());
    assert!(out_dir.join("fat-b.tfstate.json").exists());
    let o = run(&[
        "leaf",
        "migrate",
        "--fat-template",
        "fat",
        "--reverse",
        "--state",
        d.join("s.json").to_str().unwrap(),
        "--receipt",
        out_dir.join("receipt.json").to_str().unwrap(),
        "--emit-dir",
        out_dir.to_str().unwrap(),
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(
        std::fs::read(out_dir.join("fat.merged.tfstate.json")).unwrap(),
        state()
    );
}
