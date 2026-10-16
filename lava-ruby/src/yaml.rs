use std::collections::HashMap;
use std::rc::Rc;

use yaml_rust2::parser::{Event, EventReceiver, Parser};
use yaml_rust2::scanner::TScalarStyle;

use crate::core::*;
use crate::interp::*;
use crate::value::*;

enum Build {
    Seq(Vec<V>, usize),
    Map(Rc<RHash>, Option<V>, usize),
}

struct Rx {
    stack: Vec<Build>,
    docs: Vec<V>,
    anchors: HashMap<usize, V>,
    symbolize: bool,
    err: Option<String>,
}

impl Rx {
    fn push(&mut self, v: V, anchor: usize) {
        if anchor > 0 {
            self.anchors.insert(anchor, v.clone());
        }
        match self.stack.last_mut() {
            None => self.docs.push(v),
            Some(Build::Seq(items, _)) => items.push(v),
            Some(Build::Map(h, key, _)) => match key.take() {
                None => *key = Some(v),
                Some(k) => {
                    if let V::Str(s) = &k {
                        if *s.s.borrow() == "<<" {
                            if let V::Hash(src) = &v {
                                for (mk, mv) in src.entries() {
                                    if h.get(&mk).is_none() {
                                        h.insert(mk, mv);
                                    }
                                }
                                return;
                            }
                        }
                    }
                    let k = if self.symbolize {
                        if let V::Str(s) = &k {
                            V::sym(&s.s.borrow())
                        } else {
                            k
                        }
                    } else {
                        k
                    };
                    h.insert(k, v);
                }
            },
        }
    }
}

fn resolve_plain(s: &str) -> Result<V, String> {
    match s {
        "" | "~" | "null" | "Null" | "NULL" => return Ok(V::Nil),
        _ => {}
    }
    let l = s.to_ascii_lowercase();
    if matches!(l.as_str(), "yes" | "true" | "on") {
        return Ok(V::Bool(true));
    }
    if matches!(l.as_str(), "no" | "false" | "off") {
        return Ok(V::Bool(false));
    }
    if let Some(rest) = s.strip_prefix(':') {
        if !rest.is_empty() && !rest.starts_with(':') {
            let inner = rest.trim_matches('"').trim_matches('\'');
            return Ok(V::sym(inner));
        }
    }
    let int_re = |t: &str| -> Option<i64> {
        let (neg, body) = match t.strip_prefix('-') {
            Some(b) => (true, b),
            None => (false, t.strip_prefix('+').unwrap_or(t)),
        };
        let v = if let Some(h) = body.strip_prefix("0x") {
            i64::from_str_radix(&h.replace('_', ""), 16).ok()?
        } else if let Some(b) = body.strip_prefix("0b") {
            i64::from_str_radix(&b.replace('_', ""), 2).ok()?
        } else if body.len() > 1
            && body.starts_with('0')
            && body.chars().all(|c| c.is_ascii_digit() || c == '_')
        {
            i64::from_str_radix(&body[1..].replace('_', ""), 8).ok()?
        } else if !body.is_empty()
            && body.chars().next()?.is_ascii_digit()
            && body
                .chars()
                .all(|c| c.is_ascii_digit() || c == '_' || c == ',')
        {
            body.replace(['_', ','], "").parse().ok()?
        } else {
            return None;
        };
        Some(if neg { -v } else { v })
    };
    if let Some(i) = int_re(s) {
        return Ok(V::Int(i));
    }
    match l.as_str() {
        ".inf" | "+.inf" => return Ok(V::Float(f64::INFINITY)),
        "-.inf" => return Ok(V::Float(f64::NEG_INFINITY)),
        ".nan" => return Ok(V::Float(f64::NAN)),
        _ => {}
    }
    let fl =
        fancy_regex::Regex::new(r"^[-+]?(\.[0-9]+|[0-9][0-9_,]*(\.[0-9]*)?)([eE][-+]?[0-9]+)?$")
            .unwrap();
    if fl.is_match(s).unwrap_or(false) && (s.contains('.') || s.contains('e') || s.contains('E')) {
        if let Ok(f) = s.replace(['_', ','], "").parse::<f64>() {
            return Ok(V::Float(f));
        }
    }
    let date = fancy_regex::Regex::new(r"^\d{4}-\d{1,2}-\d{1,2}").unwrap();
    if date.is_match(s).unwrap_or(false) {
        let full = fancy_regex::Regex::new(r"^\d{4}-\d{1,2}-\d{1,2}$").unwrap();
        if full.is_match(s).unwrap_or(false) {
            return Err(format!("Tried to load unspecified class: Date ({s})"));
        }
    }
    Ok(V::str(s))
}

impl EventReceiver for Rx {
    fn on_event(&mut self, ev: Event) {
        if self.err.is_some() {
            return;
        }
        match ev {
            Event::Scalar(val, style, anchor, tag) => {
                let v = match (&tag, style) {
                    (Some(t), _) if t.suffix == "str" || t.suffix == "binary" => V::str(val),
                    (Some(t), _) if t.suffix == "int" => V::Int(val.parse().unwrap_or(0)),
                    (Some(t), _) if t.suffix == "float" => V::Float(val.parse().unwrap_or(0.0)),
                    (Some(t), _) if t.suffix == "bool" => V::Bool(val == "true"),
                    (Some(t), _) if t.suffix == "null" => V::Nil,
                    (Some(t), _) if t.suffix == "ruby/symbol" || t.suffix == "ruby/sym" => {
                        V::sym(&val)
                    }
                    (_, TScalarStyle::Plain) => match resolve_plain(&val) {
                        Ok(v) => v,
                        Err(e) => {
                            self.err = Some(e);
                            return;
                        }
                    },
                    _ => V::str(val),
                };
                self.push(v, anchor);
            }
            Event::SequenceStart(anchor, _) => self.stack.push(Build::Seq(vec![], anchor)),
            Event::SequenceEnd => {
                if let Some(Build::Seq(items, anchor)) = self.stack.pop() {
                    self.push(V::arr(items), anchor);
                }
            }
            Event::MappingStart(anchor, _) => {
                self.stack
                    .push(Build::Map(Rc::new(RHash::default()), None, anchor))
            }
            Event::MappingEnd => {
                if let Some(Build::Map(h, _, anchor)) = self.stack.pop() {
                    self.push(V::Hash(h), anchor);
                }
            }
            Event::Alias(id) => {
                let v = self.anchors.get(&id).cloned().unwrap_or(V::Nil);
                self.push(v, 0);
            }
            _ => {}
        }
    }
}

pub fn load(it: &Rc<Interp>, src: &str, symbolize: bool) -> R {
    let mut rx = Rx {
        stack: vec![],
        docs: vec![],
        anchors: HashMap::new(),
        symbolize,
        err: None,
    };
    let mut p = Parser::new_from_str(src);
    p.load(&mut rx, false)
        .map_err(|e| it.err("Psych::SyntaxError", e.to_string()))?;
    if let Some(e) = rx.err {
        return Err(it.err("Psych::DisallowedClass", e));
    }
    Ok(rx.docs.into_iter().next().unwrap_or(V::Bool(false)))
}

pub fn dump(it: &Rc<Interp>, v: &V) -> Result<String, Ctrl> {
    let _ = v;
    Err(it.err("NotImplementedError", "YAML.dump"))
}

pub fn _unused() -> V {
    new_hash_from(vec![])
}
