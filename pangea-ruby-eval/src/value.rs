//! Conversion between Ruby values and `serde_json::Value`.
//!
//! The evaluator's public API is JSON-shaped on the Rust side. Pangea
//! synthesizers produce nested-Hash structures (Terraform JSON, Packer
//! JSON, etc.) that map cleanly to `serde_json::Value`. This module
//! handles the round-trip.

use crate::EvalError;
use magnus::{value::ReprValue, RArray, RHash, RString, Symbol, TryConvert, Value};
use serde_json::{Map, Value as Json};

/// Type alias for the typical Pangea synthesis output: a JSON object.
pub type JsonHash = Map<String, Json>;

/// Convert a Ruby value to `serde_json::Value`.
///
/// Supported Ruby types:
/// - `nil` → `Null`
/// - `true` / `false` → `Bool`
/// - `Integer` → `Number`
/// - `Float` → `Number` (NaN/Inf rejected)
/// - `String` → `String`
/// - `Symbol` → `String` (Pangea uses symbols liberally as keys)
/// - `Array` → `Array`
/// - `Hash` → `Object` (keys coerced via `to_s`)
///
/// Anything else returns `EvalError::Conversion`.
pub fn ruby_value_to_json(value: Value) -> Result<Json, EvalError> {
    if value.is_nil() {
        return Ok(Json::Null);
    }

    // True/false detection by raw VALUE comparison. magnus's
    // `bool::try_convert` runs Ruby's RTEST macro which is "truthy-check",
    // not "is literal true/false" — every non-nil non-false value
    // converts to true under that path. Compare raw VALUE bits against
    // the canonical singletons instead.
    // Check class explicitly. Avoids the RTEST trap where every truthy
    // Ruby value (Hash, Array, …) would convert to `true` under
    // `bool::try_convert`. `value.class` returns an RClass; we need its
    // `name` to compare strings.
    let cls: Option<String> = value
        .funcall::<_, _, magnus::Value>("class", ())
        .and_then(|c| c.funcall::<_, _, RString>("name", ()))
        .ok()
        .and_then(|s| s.to_string().ok());
    match cls.as_deref() {
        Some("TrueClass") => return Ok(Json::Bool(true)),
        Some("FalseClass") => return Ok(Json::Bool(false)),
        _ => {}
    }

    // Try integer (only succeeds on Integer; Float and Hash do not pass).
    if let Ok(n) = i64::try_convert(value) {
        return Ok(Json::Number(n.into()));
    }

    // Try float (succeeds on Float; not Integer because Integer's float
    // coercion would have been hit above on the integer path first).
    if let Ok(f) = f64::try_convert(value) {
        return serde_json::Number::from_f64(f)
            .map(Json::Number)
            .ok_or_else(|| EvalError::Conversion(format!("non-finite float: {f}")));
    }

    // Try string.
    if let Some(s) = RString::from_value(value) {
        return Ok(Json::String(s.to_string().map_err(|e| {
            EvalError::Conversion(format!("string contains invalid utf-8: {e:?}"))
        })?));
    }

    // Try symbol.
    if let Some(sym) = Symbol::from_value(value) {
        return Ok(Json::String(sym.name().map_err(|e| {
            EvalError::Conversion(format!("symbol name() failed: {e:?}"))
        })?.into_owned()));
    }

    // Try array.
    if let Some(arr) = RArray::from_value(value) {
        let mut out = Vec::with_capacity(arr.len());
        unsafe {
            // SAFETY: as_slice does not allocate or trigger GC; we copy
            // the values out before any further Ruby calls.
            for v in arr.as_slice() {
                out.push(ruby_value_to_json(*v)?);
            }
        }
        return Ok(Json::Array(out));
    }

    // Try hash.
    if let Some(hash) = RHash::from_value(value) {
        return Ok(Json::Object(ruby_hash_to_json(hash)?));
    }

    let class_name: String = value
        .funcall::<_, _, magnus::Value>("class", ())
        .and_then(|c| c.funcall::<_, _, RString>("name", ()))
        .ok()
        .and_then(|s| s.to_string().ok())
        .unwrap_or_else(|| "<unknown>".into());
    Err(EvalError::Conversion(format!(
        "unsupported ruby type: {class_name}"
    )))
}

/// Convert a Ruby `Hash` into a `serde_json::Map<String, Value>`.
///
/// Hash keys are stringified via `to_s` (so `:foo` and `"foo"` collapse
/// to `"foo"`) — matches the existing pangea-compiler `_stringify_keys`
/// helper.
pub fn ruby_hash_to_json(hash: RHash) -> Result<JsonHash, EvalError> {
    let mut out = JsonHash::new();
    let result = hash.foreach(|key: Value, val: Value| {
        let ruby = magnus::Ruby::get_with(key);
        let key_str = if let Some(s) = RString::from_value(key) {
            s.to_string().map_err(|e| {
                magnus::Error::new(
                    ruby.exception_runtime_error(),
                    format!("hash key string utf-8 error: {e:?}"),
                )
            })?
        } else if let Some(sym) = Symbol::from_value(key) {
            sym.name()
                .map_err(|e| {
                    magnus::Error::new(
                        ruby.exception_runtime_error(),
                        format!("hash key symbol error: {e:?}"),
                    )
                })?
                .into_owned()
        } else {
            // Fallback: call to_s on whatever the key is.
            let s: String = key.funcall("to_s", ()).map_err(|e| {
                magnus::Error::new(
                    ruby.exception_runtime_error(),
                    format!("hash key to_s failed: {e:?}"),
                )
            })?;
            s
        };

        let json_val = ruby_value_to_json(val).map_err(|e| {
            magnus::Error::new(
                ruby.exception_runtime_error(),
                format!("value conversion: {e}"),
            )
        })?;
        out.insert(key_str, json_val);
        Ok(magnus::r_hash::ForEach::Continue)
    });

    result.map_err(|e| EvalError::Conversion(format!("hash foreach failed: {e:?}")))?;
    Ok(out)
}

/// Convert a `serde_json::Value` to a Ruby value via the active
/// interpreter.
///
/// Inverse of [`ruby_value_to_json`]. Used to inject typed inputs (parsed
/// from YAML/JSON on the Rust side) into the Ruby evaluator without
/// going through Ruby's own JSON/YAML parsers.
pub fn json_to_ruby(ruby: &magnus::Ruby, value: &Json) -> Result<Value, EvalError> {
    match value {
        Json::Null => Ok(ruby.qnil().as_value()),
        Json::Bool(b) => Ok(if *b {
            ruby.qtrue().as_value()
        } else {
            ruby.qfalse().as_value()
        }),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ruby.integer_from_i64(i).as_value())
            } else if let Some(u) = n.as_u64() {
                Ok(ruby.integer_from_u64(u).as_value())
            } else if let Some(f) = n.as_f64() {
                Ok(ruby.float_from_f64(f).as_value())
            } else {
                Err(EvalError::Conversion(format!(
                    "json number not representable: {n}"
                )))
            }
        }
        Json::String(s) => Ok(ruby.str_new(s).as_value()),
        Json::Array(arr) => {
            let r_arr = ruby.ary_new_capa(arr.len());
            for item in arr {
                r_arr
                    .push(json_to_ruby(ruby, item)?)
                    .map_err(|e| EvalError::Conversion(format!("array push: {e:?}")))?;
            }
            Ok(r_arr.as_value())
        }
        Json::Object(obj) => {
            let r_hash = ruby.hash_new();
            for (k, v) in obj {
                let key = ruby.str_new(k).as_value();
                let val = json_to_ruby(ruby, v)?;
                r_hash
                    .aset(key, val)
                    .map_err(|e| EvalError::Conversion(format!("hash aset: {e:?}")))?;
            }
            Ok(r_hash.as_value())
        }
    }
}

