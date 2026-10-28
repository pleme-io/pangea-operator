use std::collections::BTreeMap;

use serde_json::Value;

pub const LEAF_COLLECTION_ANNOTATION: &str = "pangea.pleme.io/leaf-collection";
pub const LEAF_KEY_ANNOTATION: &str = "pangea.pleme.io/leaf-key";
pub const LEAF_KEY_FIELD_ANNOTATION: &str = "pangea.pleme.io/leaf-key-field";
pub const LEAF_ADDRESSES_ANNOTATION: &str = "pangea.pleme.io/leaf-addresses";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliceContract {
    pub collection: String,
    pub key: Option<String>,
    pub key_field: Option<String>,
}

impl SliceContract {
    #[must_use]
    pub fn from_annotations(a: &BTreeMap<String, String>) -> Option<Self> {
        let collection = a.get(LEAF_COLLECTION_ANNOTATION)?.trim();
        if collection.is_empty() {
            return None;
        }
        let opt = |k: &str| {
            a.get(k)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        Some(Self {
            collection: collection.to_string(),
            key: opt(LEAF_KEY_ANNOTATION),
            key_field: opt(LEAF_KEY_FIELD_ANNOTATION),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SliceError {
    #[error("leaf declares collection `{0}` but carries no value for it")]
    Missing(String),
    #[error("leaf collection `{collection}` holds {count} entries; a leaf holds exactly one")]
    NotOne { collection: String, count: usize },
    #[error("leaf collection `{0}` is neither a list nor a map")]
    NotACollection(String),
    #[error("leaf entry key `{found}` does not match its declared key `{declared}`")]
    KeyMismatch { declared: String, found: String },
}

fn entry_key(entry: &Value, map_key: Option<&str>, field: Option<&str>) -> Option<String> {
    if let Some(f) = field {
        return entry.get(f).and_then(Value::as_str).map(str::to_string);
    }
    map_key.map(str::to_string)
}

pub fn isolate(
    effective: &mut BTreeMap<String, Value>,
    own: &BTreeMap<String, Value>,
    contract: &SliceContract,
) -> Result<(), SliceError> {
    let c = &contract.collection;
    let value = own.get(c).ok_or_else(|| SliceError::Missing(c.clone()))?;
    let (count, found) = match value {
        Value::Array(a) => (
            a.len(),
            a.first()
                .and_then(|e| entry_key(e, None, contract.key_field.as_deref())),
        ),
        Value::Object(o) => (
            o.len(),
            o.iter()
                .next()
                .and_then(|(k, e)| entry_key(e, Some(k), contract.key_field.as_deref())),
        ),
        _ => return Err(SliceError::NotACollection(c.clone())),
    };
    if count != 1 {
        return Err(SliceError::NotOne {
            collection: c.clone(),
            count,
        });
    }
    if let (Some(declared), Some(found)) = (&contract.key, found) {
        if declared != &found {
            return Err(SliceError::KeyMismatch {
                declared: declared.clone(),
                found,
            });
        }
    }
    effective.insert(c.clone(), value.clone());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn m(v: Value) -> BTreeMap<String, Value> {
        serde_json::from_value(v).unwrap()
    }

    fn contract(key: Option<&str>, field: Option<&str>) -> SliceContract {
        SliceContract {
            collection: "repos".into(),
            key: key.map(str::to_string),
            key_field: field.map(str::to_string),
        }
    }

    #[test]
    fn a_catalog_collection_never_reaches_the_leaf() {
        let mut eff =
            m(json!({"owner": "o", "repos": {"a": {"x": 1}, "b": {"x": 2}, "c": {"x": 3}}}));
        let own = m(json!({"repos": {"b": {"x": 2}}}));
        isolate(&mut eff, &own, &contract(Some("b"), None)).unwrap();
        assert_eq!(eff["repos"], json!({"b": {"x": 2}}));
        assert_eq!(eff["owner"], json!("o"));
    }

    #[test]
    fn a_list_slice_matches_by_key_field() {
        let mut eff = m(json!({"repos": [{"slug": "a"}, {"slug": "b"}]}));
        let own = m(json!({"repos": [{"slug": "b"}]}));
        isolate(&mut eff, &own, &contract(Some("b"), Some("slug"))).unwrap();
        assert_eq!(eff["repos"], json!([{"slug": "b"}]));
    }

    #[test]
    fn a_leaf_with_no_slice_or_many_entries_or_the_wrong_key_is_refused() {
        let mut eff = m(json!({"repos": [{"slug": "a"}, {"slug": "b"}]}));
        assert_eq!(
            isolate(&mut eff, &m(json!({})), &contract(None, None)),
            Err(SliceError::Missing("repos".into()))
        );
        assert!(matches!(
            isolate(
                &mut eff,
                &m(json!({"repos": [{"slug": "a"}, {"slug": "b"}]})),
                &contract(None, Some("slug"))
            ),
            Err(SliceError::NotOne { count: 2, .. })
        ));
        assert!(matches!(
            isolate(
                &mut eff,
                &m(json!({"repos": [{"slug": "a"}]})),
                &contract(Some("b"), Some("slug"))
            ),
            Err(SliceError::KeyMismatch { .. })
        ));
        assert!(matches!(
            isolate(&mut eff, &m(json!({"repos": "a"})), &contract(None, None)),
            Err(SliceError::NotACollection(_))
        ));
        assert_eq!(eff["repos"], json!([{"slug": "a"}, {"slug": "b"}]));
    }

    #[test]
    fn the_contract_reads_from_annotations_and_is_absent_without_a_collection() {
        let mut a = BTreeMap::new();
        assert_eq!(SliceContract::from_annotations(&a), None);
        a.insert(LEAF_COLLECTION_ANNOTATION.into(), "repos".into());
        a.insert(LEAF_KEY_ANNOTATION.into(), "b".into());
        a.insert(LEAF_KEY_FIELD_ANNOTATION.into(), "slug".into());
        assert_eq!(
            SliceContract::from_annotations(&a),
            Some(contract(Some("b"), Some("slug")))
        );
    }
}
