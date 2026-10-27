use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::partition::multiset_hash;
use crate::plan::StateMove;
use crate::sources::{
    addresses_from_state, instance_address, is_managed, resource_base, SourceError,
};

pub const ARCHIVE_ROW: &str = "pre-leaf";
pub const LIVE_ROW: &str = "default";
pub const REVERTED_ROW: &str = "reverted";
pub const ARTIFACT_KIND: &str = "leaf-migration";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MigrateError {
    #[error("the fat state does not parse: {0}")]
    Parse(String),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("no state moves were given")]
    NoMoves,
    #[error("two moves target the same leaf `{0}`")]
    DuplicateLeaf(String),
    #[error("move to `{to}` names `{from}` as its source, not `{fat}`")]
    WrongSource {
        fat: String,
        from: String,
        to: String,
    },
    #[error("bijection refused: {0}")]
    Bijection(String),
    #[error("leaf `{0}` has no state in the merge input")]
    MissingLeaf(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Direction {
    Forward,
    Reversed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeafReceipt {
    pub template: String,
    pub addresses: usize,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    pub fat_template: String,
    pub direction: Direction,
    pub fat_lineage: String,
    pub fat_serial: u64,
    pub archive_hash: String,
    pub count_before: usize,
    pub count_after: usize,
    pub hash_before: String,
    pub hash_after: String,
    pub disjoint: bool,
    pub leaves: Vec<LeafReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafState {
    pub template: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    pub leaves: Vec<LeafState>,
    pub receipt: Receipt,
}

fn parse(bytes: &[u8]) -> Result<Value, MigrateError> {
    serde_json::from_slice(bytes).map_err(|e| MigrateError::Parse(e.to_string()))
}

fn content_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

#[must_use]
pub fn lineage_for(fat_lineage: &str, leaf: &str) -> String {
    let mut h = blake3::Hasher::new();
    h.update(fat_lineage.as_bytes());
    h.update(&[0]);
    h.update(leaf.as_bytes());
    let x = h.finalize().to_hex().to_string();
    format!(
        "{}-{}-{}-{}-{}",
        &x[0..8],
        &x[8..12],
        &x[12..16],
        &x[16..20],
        &x[20..32]
    )
}

fn str_field(v: &Value, k: &str) -> String {
    v.get(k)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn serial_of(v: &Value) -> u64 {
    v.get("serial").and_then(Value::as_u64).unwrap_or(0)
}

fn resources_of(v: &Value) -> Result<&Vec<Value>, MigrateError> {
    v.get("resources")
        .and_then(Value::as_array)
        .ok_or_else(|| MigrateError::Source(SourceError::State("no `resources` array".into())))
}

pub fn managed_addresses(bytes: &[u8]) -> Result<Vec<String>, MigrateError> {
    Ok(addresses_from_state(&parse(bytes)?)?)
}

fn check_moves(fat_template: &str, moves: &[StateMove]) -> Result<(), MigrateError> {
    if moves.is_empty() {
        return Err(MigrateError::NoMoves);
    }
    let mut seen = BTreeSet::new();
    for m in moves {
        if m.from_template != fat_template {
            return Err(MigrateError::WrongSource {
                fat: fat_template.to_string(),
                from: m.from_template.clone(),
                to: m.to_template.clone(),
            });
        }
        if !seen.insert(m.to_template.as_str()) {
            return Err(MigrateError::DuplicateLeaf(m.to_template.clone()));
        }
    }
    Ok(())
}

fn bijection(before: &[String], after: &[String]) -> (String, String, bool, bool) {
    let unique: BTreeSet<&String> = after.iter().collect();
    let disjoint = unique.len() == after.len();
    let hb = multiset_hash(before);
    let ha = multiset_hash(after);
    let ok = disjoint && hb == ha;
    (hb, ha, disjoint, ok)
}

fn leaf_document(fat: &Value, lineage: String, resources: Vec<Value>) -> Value {
    let mut doc = Map::new();
    doc.insert(
        "version".into(),
        fat.get("version").cloned().unwrap_or(json!(4)),
    );
    doc.insert(
        "terraform_version".into(),
        fat.get("terraform_version").cloned().unwrap_or(json!("")),
    );
    doc.insert("serial".into(), json!(1));
    doc.insert("lineage".into(), json!(lineage));
    doc.insert("outputs".into(), json!({}));
    doc.insert("resources".into(), Value::Array(resources));
    Value::Object(doc)
}

pub fn split(fat_template: &str, fat: &[u8], moves: &[StateMove]) -> Result<Split, MigrateError> {
    check_moves(fat_template, moves)?;
    let doc = parse(fat)?;
    let before = addresses_from_state(&doc)?;
    let claimed: Vec<String> = moves.iter().flat_map(|m| m.addresses.clone()).collect();
    let (hb, ha, disjoint, ok) = bijection(&before, &claimed);
    if !ok {
        return Err(MigrateError::Bijection(format!(
            "state {hb} ({}) vs moves {ha} ({}), disjoint={disjoint}",
            before.len(),
            claimed.len()
        )));
    }
    let owner: BTreeMap<&str, usize> = moves
        .iter()
        .enumerate()
        .flat_map(|(i, m)| m.addresses.iter().map(move |a| (a.as_str(), i)))
        .collect();
    let mut per_leaf: Vec<Vec<Value>> = vec![Vec::new(); moves.len()];
    for r in resources_of(&doc)? {
        if !is_managed(r) {
            continue;
        }
        let base = resource_base(r)?;
        let instances = r
            .get("instances")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut buckets: BTreeMap<usize, Vec<Value>> = BTreeMap::new();
        for inst in instances {
            let addr = instance_address(&base, &inst);
            let i = owner
                .get(addr.as_str())
                .copied()
                .ok_or_else(|| MigrateError::Bijection(format!("`{addr}` is in no move")))?;
            buckets.entry(i).or_default().push(inst);
        }
        for (i, insts) in buckets {
            let mut block = r.clone();
            if let Some(o) = block.as_object_mut() {
                o.insert("instances".into(), Value::Array(insts));
            }
            per_leaf[i].push(block);
        }
    }
    let fat_lineage = str_field(&doc, "lineage");
    let mut leaves = Vec::with_capacity(moves.len());
    let mut receipts = Vec::with_capacity(moves.len());
    let mut after = Vec::with_capacity(before.len());
    for (m, resources) in moves.iter().zip(per_leaf) {
        let leaf_doc = leaf_document(&doc, lineage_for(&fat_lineage, &m.to_template), resources);
        let bytes =
            serde_json::to_vec_pretty(&leaf_doc).map_err(|e| MigrateError::Parse(e.to_string()))?;
        let got = addresses_from_state(&leaf_doc)?;
        receipts.push(LeafReceipt {
            template: m.to_template.clone(),
            addresses: got.len(),
            content_hash: content_hash(&bytes),
        });
        after.extend(got);
        leaves.push(LeafState {
            template: m.to_template.clone(),
            bytes,
        });
    }
    let (hb, ha, disjoint, ok) = bijection(&before, &after);
    if !ok {
        return Err(MigrateError::Bijection(format!(
            "written leaves {ha} ({}) != state {hb} ({}), disjoint={disjoint}",
            after.len(),
            before.len()
        )));
    }
    Ok(Split {
        leaves,
        receipt: Receipt {
            fat_template: fat_template.to_string(),
            direction: Direction::Forward,
            fat_lineage,
            fat_serial: serial_of(&doc),
            archive_hash: content_hash(fat),
            count_before: before.len(),
            count_after: after.len(),
            hash_before: hb,
            hash_after: ha,
            disjoint,
            leaves: receipts,
        },
    })
}

pub fn verify_written(receipt: &Receipt, leaves: &[LeafState]) -> Result<(), MigrateError> {
    let mut after = Vec::new();
    for l in leaves {
        after.extend(managed_addresses(&l.bytes)?);
    }
    let (_, ha, disjoint, _) = bijection(&after, &after);
    if !disjoint || ha != receipt.hash_before || after.len() != receipt.count_before {
        return Err(MigrateError::Bijection(format!(
            "read back {ha} ({}) != recorded {} ({}), disjoint={disjoint}",
            after.len(),
            receipt.hash_before,
            receipt.count_before
        )));
    }
    Ok(())
}

fn block_id(r: &Value) -> (String, String, String, String) {
    (
        str_field(r, "module"),
        str_field(r, "mode"),
        str_field(r, "type"),
        str_field(r, "name"),
    )
}

pub fn merge(
    archive: &[u8],
    receipt: &Receipt,
    leaves: &[LeafState],
) -> Result<Vec<u8>, MigrateError> {
    let by_name: BTreeMap<&str, &LeafState> =
        leaves.iter().map(|l| (l.template.as_str(), l)).collect();
    for r in &receipt.leaves {
        if !by_name.contains_key(r.template.as_str()) {
            return Err(MigrateError::MissingLeaf(r.template.clone()));
        }
    }
    let unchanged = receipt
        .leaves
        .iter()
        .all(|r| content_hash(&by_name[r.template.as_str()].bytes) == r.content_hash);
    if unchanged && content_hash(archive) == receipt.archive_hash {
        return Ok(archive.to_vec());
    }
    let mut doc = parse(archive)?;
    let mut leaf_blocks: BTreeMap<(String, String, String, String), Vec<Value>> = BTreeMap::new();
    let mut order: Vec<(String, String, String, String)> = Vec::new();
    let mut serial = serial_of(&doc);
    let mut expected = Vec::new();
    for r in &receipt.leaves {
        let ld = parse(&by_name[r.template.as_str()].bytes)?;
        serial = serial.max(serial_of(&ld));
        expected.extend(addresses_from_state(&ld)?);
        for block in resources_of(&ld)? {
            let id = block_id(block);
            let insts = block
                .get("instances")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if !leaf_blocks.contains_key(&id) {
                order.push(id.clone());
            }
            leaf_blocks.entry(id).or_default().extend(insts);
        }
    }
    let mut out = Vec::new();
    for r in resources_of(&doc)?.clone() {
        if !is_managed(&r) {
            out.push(r);
            continue;
        }
        let id = block_id(&r);
        if let Some(insts) = leaf_blocks.remove(&id) {
            let mut block = r.clone();
            if let Some(o) = block.as_object_mut() {
                o.insert("instances".into(), Value::Array(insts));
            }
            out.push(block);
        }
    }
    for id in order {
        if let Some(insts) = leaf_blocks.remove(&id) {
            out.push(json!({
                "module": if id.0.is_empty() { Value::Null } else { json!(id.0) },
                "mode": id.1, "type": id.2, "name": id.3, "instances": insts,
            }));
        }
    }
    for b in &mut out {
        if let Some(o) = b.as_object_mut() {
            if o.get("module").is_some_and(Value::is_null) {
                o.remove("module");
            }
        }
    }
    if let Some(o) = doc.as_object_mut() {
        o.insert("resources".into(), Value::Array(out));
        o.insert("serial".into(), json!(serial + 1));
    }
    let merged = addresses_from_state(&doc)?;
    let (hb, ha, disjoint, ok) = bijection(&expected, &merged);
    if !ok {
        return Err(MigrateError::Bijection(format!(
            "merged {ha} ({}) != leaves {hb} ({}), disjoint={disjoint}",
            merged.len(),
            expected.len()
        )));
    }
    serde_json::to_vec_pretty(&doc).map_err(|e| MigrateError::Parse(e.to_string()))
}
