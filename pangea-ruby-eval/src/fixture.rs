//! Fixture parsing — file read + SHA-256 hash + YAML → JSON with
//! stringified keys.
//!
//! M8.3 of the hollow-out plan: Pangea Ruby stops calling `File.read`,
//! `Digest::SHA256`, and `YAML.safe_load`. Rust does it all on the
//! way in; the Ruby evaluator only ever sees a pre-built Hash via
//! magnus value injection.
//!
//! Mirrors the existing `pangea-compiler/app.rb` lines 332-355 +
//! the `_stringify_keys` helper at lines 366-374.

use crate::EvalError;
use serde_json::{Map, Value as Json};
use sha2::{Digest, Sha256};
use std::path::Path;

/// What we hand back to callers: the parsed inputs (key-stringified
/// and JSON-shaped) + a 12-char SHA-256 prefix used by reconcilers
/// to detect fixture changes.
#[derive(Debug, Clone)]
pub struct ParsedFixture {
    pub inputs: Json,
    pub input_hash: String,
}

/// Read + parse a fixture YAML file from disk. Mirrors
/// `pangea-compiler/app.rb`'s smoke-test fixture flow but in Rust:
/// no `require 'yaml'`, no `require 'digest'`, no `File.read` on the
/// Ruby side.
///
/// Recursive key stringification matches `_stringify_keys` — symbol
/// keys (`:foo`) and string keys (`"foo"`) collapse to the same
/// JSON-string key.
pub fn parse_yaml_fixture(path: &Path) -> Result<ParsedFixture, EvalError> {
    let raw = std::fs::read(path)
        .map_err(|e| EvalError::Other(format!("read fixture {}: {e}", path.display())))?;

    let input_hash = short_sha256_hex(&raw);

    // Parse as serde_yaml::Value first (handles tagged nodes, anchors,
    // symbol-shaped keys), then map to serde_json::Value with all
    // keys stringified.
    let yaml_val: serde_yaml::Value = serde_yaml::from_slice(&raw)
        .map_err(|e| EvalError::Other(format!("yaml parse {}: {e}", path.display())))?;

    let inputs = yaml_to_json_with_string_keys(yaml_val)?;

    Ok(ParsedFixture { inputs, input_hash })
}

/// 12-char prefix of the SHA-256 hex digest. Matches the existing
/// `pangea-compiler/app.rb` `Digest::SHA256.hexdigest(raw)[0, 12]`.
pub fn short_sha256_hex(raw: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw);
    let digest = hasher.finalize();
    // hex-encode the first 6 bytes -> 12 chars.
    let mut out = String::with_capacity(12);
    for b in digest.iter().take(6) {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

/// Convert a `serde_yaml::Value` to a `serde_json::Value` with all
/// mapping keys stringified.
///
/// YAML allows non-string keys (numbers, symbols, sequences); JSON
/// does not. This walker:
///   - drops null mappings (matches `YAML.safe_load` returning `{}`
///     for an empty doc),
///   - stringifies every key via Display (numbers → "1", symbols →
///     "foo", strings → unchanged),
///   - rejects non-stringifiable keys (sequences, mappings as keys)
///     with a typed error.
pub fn yaml_to_json_with_string_keys(value: serde_yaml::Value) -> Result<Json, EvalError> {
    match value {
        serde_yaml::Value::Null => Ok(Json::Null),
        serde_yaml::Value::Bool(b) => Ok(Json::Bool(b)),
        serde_yaml::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Json::Number(i.into()))
            } else if let Some(u) = n.as_u64() {
                Ok(Json::Number(u.into()))
            } else if let Some(f) = n.as_f64() {
                serde_json::Number::from_f64(f).map(Json::Number).ok_or_else(|| {
                    EvalError::Conversion(format!("non-finite YAML float: {f}"))
                })
            } else {
                Err(EvalError::Conversion(format!(
                    "YAML number not representable: {n:?}"
                )))
            }
        }
        serde_yaml::Value::String(s) => Ok(Json::String(s)),
        serde_yaml::Value::Sequence(seq) => {
            let mut out = Vec::with_capacity(seq.len());
            for item in seq {
                out.push(yaml_to_json_with_string_keys(item)?);
            }
            Ok(Json::Array(out))
        }
        serde_yaml::Value::Mapping(map) => {
            let mut out = Map::with_capacity(map.len());
            for (k, v) in map {
                let key = yaml_key_to_string(k)?;
                out.insert(key, yaml_to_json_with_string_keys(v)?);
            }
            Ok(Json::Object(out))
        }
        serde_yaml::Value::Tagged(tagged) => {
            // Drop the tag; recurse on the wrapped value. Pangea
            // fixtures don't use tagged nodes meaningfully — any
            // round-trip behavior depending on the tag is out of
            // scope.
            yaml_to_json_with_string_keys(tagged.value)
        }
    }
}

fn yaml_key_to_string(key: serde_yaml::Value) -> Result<String, EvalError> {
    match key {
        serde_yaml::Value::String(s) => Ok(s),
        serde_yaml::Value::Bool(b) => Ok(b.to_string()),
        serde_yaml::Value::Number(n) => Ok(n.to_string()),
        serde_yaml::Value::Null => Ok(String::new()),
        other => Err(EvalError::Conversion(format!(
            "YAML mapping key not stringifiable: {other:?}"
        ))),
    }
}
