use pangea_leaf::migrate::{
    managed_addresses, merge, split, verify_written, LeafState, MigrateError,
};
use pangea_leaf::partition::multiset_hash;
use pangea_leaf::plan::StateMove;
use serde_json::{json, Value};

const FAT: &str = "org";

fn fat_state() -> Vec<u8> {
    let doc = json!({
        "version": 4,
        "terraform_version": "1.7.0",
        "serial": 181,
        "lineage": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "outputs": {"count": {"value": 2, "type": "number"}},
        "resources": [
            {"mode": "managed", "type": "github_repository", "name": "repos",
             "provider": "provider[\"registry.terraform.io/integrations/github\"]",
             "instances": [
                {"index_key": "alpha", "schema_version": 1, "attributes": {"id": "alpha", "visibility": "public"}},
                {"index_key": "beta", "schema_version": 1, "attributes": {"id": "beta", "visibility": "private"}}
             ]},
            {"mode": "managed", "type": "github_branch_protection", "name": "main",
             "provider": "provider[\"registry.terraform.io/integrations/github\"]",
             "instances": [
                {"index_key": "alpha", "schema_version": 2, "attributes": {"id": "p-alpha"}},
                {"index_key": "beta", "schema_version": 2, "attributes": {"id": "p-beta"}}
             ]},
            {"mode": "data", "type": "github_user", "name": "me",
             "provider": "provider[\"registry.terraform.io/integrations/github\"]",
             "instances": [{"schema_version": 0, "attributes": {"id": "1"}}]}
        ]
    });
    serde_json::to_vec_pretty(&doc).unwrap()
}

fn mv(to: &str, addrs: &[&str]) -> StateMove {
    StateMove {
        from_template: FAT.into(),
        to_template: to.into(),
        pangea_namespace: Some("ns".into()),
        addresses: addrs.iter().map(|s| (*s).to_string()).collect(),
    }
}

fn good_moves() -> Vec<StateMove> {
    vec![
        mv(
            "org-alpha",
            &[
                "github_repository.repos[\"alpha\"]",
                "github_branch_protection.main[\"alpha\"]",
            ],
        ),
        mv(
            "org-beta",
            &[
                "github_repository.repos[\"beta\"]",
                "github_branch_protection.main[\"beta\"]",
            ],
        ),
    ]
}

#[test]
fn a_forged_bijection_is_refused_before_any_leaf_is_produced() {
    let fat = fat_state();
    let mut dropped = good_moves();
    dropped[1].addresses.pop();
    assert!(matches!(
        split(FAT, &fat, &dropped),
        Err(MigrateError::Bijection(_))
    ));

    let mut doubled = good_moves();
    doubled[1]
        .addresses
        .push("github_repository.repos[\"alpha\"]".into());
    assert!(matches!(
        split(FAT, &fat, &doubled),
        Err(MigrateError::Bijection(_))
    ));

    let mut invented = good_moves();
    invented[1].addresses[1] = "github_branch_protection.main[\"gamma\"]".into();
    assert!(matches!(
        split(FAT, &fat, &invented),
        Err(MigrateError::Bijection(_))
    ));
}

#[test]
fn a_move_from_another_template_or_twice_to_one_leaf_is_refused() {
    let fat = fat_state();
    let mut other = good_moves();
    other[0].from_template = "elsewhere".into();
    assert!(matches!(
        split(FAT, &fat, &other),
        Err(MigrateError::WrongSource { .. })
    ));
    let mut twice = good_moves();
    twice[1].to_template = "org-alpha".into();
    assert!(matches!(
        split(FAT, &fat, &twice),
        Err(MigrateError::DuplicateLeaf(_))
    ));
    assert_eq!(split(FAT, &fat, &[]), Err(MigrateError::NoMoves));
}

#[test]
fn a_good_split_writes_n_leaves_that_read_back_to_the_same_multiset() {
    let fat = fat_state();
    let s = split(FAT, &fat, &good_moves()).unwrap();
    assert_eq!(s.leaves.len(), 2);
    let before = managed_addresses(&fat).unwrap();
    let mut after = Vec::new();
    for l in &s.leaves {
        let a = managed_addresses(&l.bytes).unwrap();
        assert_eq!(a.len(), 2);
        after.extend(a);
        let doc: Value = serde_json::from_slice(&l.bytes).unwrap();
        assert_eq!(doc["serial"], json!(1));
        assert_ne!(
            doc["lineage"],
            json!("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        );
    }
    assert_eq!(multiset_hash(&before), multiset_hash(&after));
    assert_eq!(s.receipt.hash_before, s.receipt.hash_after);
    assert!(s.receipt.disjoint);
    verify_written(&s.receipt, &s.leaves).unwrap();
}

#[test]
fn attributes_move_unchanged() {
    let fat = fat_state();
    let s = split(FAT, &fat, &good_moves()).unwrap();
    let beta: Value = serde_json::from_slice(&s.leaves[1].bytes).unwrap();
    let repo = &beta["resources"][0];
    assert_eq!(repo["type"], json!("github_repository"));
    assert_eq!(
        repo["instances"][0],
        json!({"index_key": "beta", "schema_version": 1, "attributes": {"id": "beta", "visibility": "private"}})
    );
    assert_eq!(
        repo["provider"],
        json!("provider[\"registry.terraform.io/integrations/github\"]")
    );
}

#[test]
fn the_fat_bytes_are_not_mutated() {
    let fat = fat_state();
    let copy = fat.clone();
    let _ = split(FAT, &fat, &good_moves()).unwrap();
    assert_eq!(fat, copy);
}

#[test]
fn a_tampered_read_back_fails_verification() {
    let fat = fat_state();
    let mut s = split(FAT, &fat, &good_moves()).unwrap();
    s.leaves.pop();
    assert!(verify_written(&s.receipt, &s.leaves).is_err());
}

#[test]
fn reverse_merge_restores_the_fat_row_byte_equal() {
    let fat = fat_state();
    let s = split(FAT, &fat, &good_moves()).unwrap();
    let merged = merge(&fat, &s.receipt, &s.leaves).unwrap();
    assert_eq!(merged, fat);
}

#[test]
fn reverse_merge_after_a_leaf_changed_keeps_every_address_and_moves_the_serial() {
    let fat = fat_state();
    let s = split(FAT, &fat, &good_moves()).unwrap();
    let mut leaves = s.leaves.clone();
    let mut beta: Value = serde_json::from_slice(&leaves[1].bytes).unwrap();
    beta["serial"] = json!(4);
    beta["resources"][0]["instances"][0]["attributes"]["visibility"] = json!("public");
    leaves[1] = LeafState {
        template: "org-beta".into(),
        bytes: serde_json::to_vec(&beta).unwrap(),
    };
    let merged = merge(&fat, &s.receipt, &leaves).unwrap();
    let doc: Value = serde_json::from_slice(&merged).unwrap();
    assert_eq!(doc["serial"], json!(182));
    assert_eq!(
        doc["lineage"],
        json!("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    );
    assert_eq!(
        multiset_hash(&managed_addresses(&merged).unwrap()),
        multiset_hash(&managed_addresses(&fat).unwrap())
    );
    assert_eq!(
        doc["resources"][0]["instances"][1]["attributes"]["visibility"],
        json!("public")
    );
    assert_eq!(doc["resources"][2]["mode"], json!("data"));
}

#[test]
fn reverse_merge_refuses_a_missing_leaf() {
    let fat = fat_state();
    let s = split(FAT, &fat, &good_moves()).unwrap();
    assert_eq!(
        merge(&fat, &s.receipt, &s.leaves[..1]),
        Err(MigrateError::MissingLeaf("org-beta".into()))
    );
}

fn leaf_keys(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(l, k)| ((*l).to_string(), (*k).to_string()))
        .collect()
}

#[test]
fn declared_leaf_keys_place_every_address_and_the_moves_split_cleanly() {
    let fat = fat_state();
    let moves = pangea_leaf::migrate::moves_by_key(
        FAT,
        Some("ns"),
        &fat,
        &leaf_keys(&[
            ("org-alpha", "alpha"),
            ("org-beta", "beta"),
            ("org-new", "gamma"),
        ]),
    )
    .unwrap();
    assert_eq!(moves.len(), 2);
    let s = split(FAT, &fat, &moves).unwrap();
    assert_eq!(s.receipt.hash_before, s.receipt.hash_after);
}

#[test]
fn an_address_whose_key_no_leaf_declares_refuses_the_move() {
    let fat = fat_state();
    assert!(matches!(
        pangea_leaf::migrate::moves_by_key(FAT, None, &fat, &leaf_keys(&[("org-alpha", "alpha")])),
        Err(MigrateError::Bijection(_))
    ));
    assert!(matches!(
        pangea_leaf::migrate::moves_by_key(
            FAT,
            None,
            &fat,
            &leaf_keys(&[("org-alpha", "alpha"), ("org-twin", "alpha")])
        ),
        Err(MigrateError::DuplicateLeaf(_))
    ));
}
