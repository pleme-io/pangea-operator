use std::rc::Rc;

use crate::core::*;
use crate::fmt::*;
use crate::interp::*;
use crate::value::*;

pub fn generate(it: &Rc<Interp>, v: &V, pretty: bool) -> Result<String, Ctrl> {
    let mut out = String::new();
    gen(it, v, pretty, 0, &mut out)?;
    Ok(out)
}

fn indent(out: &mut String, d: usize) {
    for _ in 0..d {
        out.push_str("  ");
    }
}

fn key_string(it: &Rc<Interp>, k: &V) -> Result<String, Ctrl> {
    match k {
        V::Str(s) => Ok(s.s.borrow().clone()),
        V::Sym(s) => Ok(s.to_string()),
        other => it.to_s(other),
    }
}

fn gen(it: &Rc<Interp>, v: &V, pretty: bool, d: usize, out: &mut String) -> Result<(), Ctrl> {
    match v {
        V::Nil => out.push_str("null"),
        V::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        V::Int(i) => out.push_str(&i.to_string()),
        V::Float(f) => {
            if !f.is_finite() {
                return Err(it.err(
                    "JSON::GeneratorError",
                    format!("{} not allowed in JSON", float_to_s(*f)),
                ));
            }
            out.push_str(&float_to_s(*f))
        }
        V::Str(s) => json_escape(&s.s.borrow(), out),
        V::Sym(s) => json_escape(s, out),
        V::Hash(h) => {
            out.push('{');
            let entries = h.entries();
            for (i, (k, val)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                if pretty {
                    out.push('\n');
                    indent(out, d + 1);
                }
                json_escape(&key_string(it, k)?, out);
                out.push(':');
                if pretty {
                    out.push(' ');
                }
                gen(it, val, pretty, d + 1, out)?;
            }
            if pretty {
                out.push('\n');
                indent(out, d);
            }
            out.push('}');
        }
        V::Arr(a) => {
            let items = a.borrow().clone();
            out.push('[');
            if pretty {
                out.push('\n');
            }
            for (i, x) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                    if pretty {
                        out.push('\n');
                    }
                }
                if pretty {
                    indent(out, d + 1);
                }
                gen(it, x, pretty, d + 1, out)?;
            }
            if pretty {
                out.push('\n');
                indent(out, d);
            }
            out.push(']');
        }
        V::Obj(_) => {
            if it.find_method(v, "to_json").is_some() {
                let r = it.call(v, "to_json", Args::default())?;
                out.push_str(&to_s_plain(&r));
            } else if it.find_method(v, "to_h").is_some() && it.is_a(v, "Dry::Struct") {
                let h = it.call(v, "to_h", Args::default())?;
                gen(it, &h, pretty, d, out)?;
            } else {
                json_escape(&it.to_s(v)?, out);
            }
        }
        other => json_escape(&it.to_s(other)?, out),
    }
    Ok(())
}

pub fn parse(it: &Rc<Interp>, s: &str, symbolize: bool) -> R {
    let j: serde_json::Value =
        serde_json::from_str(s).map_err(|e| it.err("JSON::ParserError", e.to_string()))?;
    Ok(from_json(&j, symbolize))
}

pub fn from_json(j: &serde_json::Value, symbolize: bool) -> V {
    match j {
        serde_json::Value::Null => V::Nil,
        serde_json::Value::Bool(b) => V::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                V::Int(i)
            } else {
                V::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => V::str(s.clone()),
        serde_json::Value::Array(a) => V::arr(a.iter().map(|x| from_json(x, symbolize)).collect()),
        serde_json::Value::Object(o) => new_hash_from(
            o.iter()
                .map(|(k, v)| {
                    (
                        if symbolize {
                            V::sym(k)
                        } else {
                            V::str(k.clone())
                        },
                        from_json(v, symbolize),
                    )
                })
                .collect(),
        ),
    }
}
