use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::core::*;
use crate::fmt::*;
use crate::interp::*;
use crate::value::*;

type NR = Result<Option<V>, Ctrl>;

const BUILTIN_FEATURES: &[&str] = &[
    "yaml",
    "psych",
    "ipaddr",
    "resolv",
    "json",
    "set",
    "digest",
    "digest/sha2",
    "digest/sha256",
    "digest/md5",
    "fileutils",
    "net/http",
    "uri",
    "open3",
    "pathname",
    "time",
    "date",
    "securerandom",
    "ostruct",
    "erb",
    "shellwords",
    "stringio",
    "tempfile",
    "tmpdir",
    "logger",
    "forwardable",
    "singleton",
    "base64",
    "openssl",
    "socket",
    "timeout",
    "benchmark",
    "English",
    "pp",
    "optparse",
    "rbconfig",
    "etc",
    "zlib",
    "bigdecimal",
    "monitor",
    "delegate",
    "observer",
    "weakref",
    "objspace",
    "io/console",
    "rubygems",
    "bundler/setup",
    "bundler",
    "dry-struct",
    "dry-types",
    "dry/struct",
    "dry/types",
    "dry-core",
    "dry-inflector",
    "terraform-synthesizer",
    "terraform_synthesizer",
    "abstract-synthesizer",
    "abstract_synthesizer",
];

pub fn is_builtin_feature(f: &str) -> bool {
    BUILTIN_FEATURES.contains(&f)
        || f.starts_with("dry/")
        || f.starts_with("dry-")
        || f.starts_with("net/")
        || f.starts_with("openssl")
}

pub fn bootstrap(it: &Rc<Interp>) {
    let o = it.object.clone();
    for m in [
        "YAML",
        "JSON",
        "Digest",
        "FileUtils",
        "Shellwords",
        "Base64",
        "SecureRandom",
        "URI",
        "Open3",
        "Etc",
    ] {
        it.define_module(m, &o);
    }
    let yaml_mod = o.consts.borrow().get("YAML").cloned().unwrap();
    o.consts.borrow_mut().insert("Psych".into(), yaml_mod);
    let json = it.cls("JSON");
    let perr = it.define_class("ParserError", Some(it.cls("StandardError")), &json);
    let _ = perr;
    let gerr = it.define_class("GeneratorError", Some(it.cls("StandardError")), &json);
    let _ = gerr;
    json.consts
        .borrow_mut()
        .insert("VERSION".into(), V::str("2.7.2"));
    let yaml = it.cls("YAML");
    let ye = it.define_class("SyntaxError", Some(it.cls("StandardError")), &yaml);
    let _ = ye;
    it.define_class("DisallowedClass", Some(it.cls("StandardError")), &yaml);
    let digest = it.cls("Digest");
    it.define_module("SHA256", &digest);
    it.define_module("SHA1", &digest);
    it.define_module("MD5", &digest);
    let net = it.define_module("Net", &o);
    let http = it.define_class("HTTP", None, &net);
    let _ = http;
    for c in [
        "HTTPSuccess",
        "HTTPResponse",
        "HTTPNotFound",
        "HTTPRequest",
        "OpenTimeout",
        "ReadTimeout",
    ] {
        it.define_class(c, Some(it.cls("StandardError")), &net);
    }
    let src = include_str!("../prelude/prelude.rb");
    it.virtual_files
        .borrow_mut()
        .insert(PathBuf::from("/lava-ruby/prelude.rb"), src.to_string());
    let r = it.load_file(Path::new("/lava-ruby/prelude.rb"));
    if let Err(Ctrl::Raise(e)) = r {
        panic!(
            "prelude failed: {}",
            to_s_plain(&it.ivar_get(&e, "@message"))
        );
    }
}

pub fn autoload_const(_it: &Rc<Interp>, _name: &str) -> Result<Option<V>, Ctrl> {
    Ok(None)
}

pub fn native_included(
    _it: &Rc<Interp>,
    _inc: &Rc<Module>,
    _into: &Rc<Module>,
) -> Result<(), Ctrl> {
    Ok(())
}

fn arg(a: &Args, i: usize) -> V {
    a.flat().get(i).cloned().unwrap_or(V::Nil)
}

fn s_arg(it: &Rc<Interp>, a: &Args, i: usize) -> Result<String, Ctrl> {
    match arg(a, i) {
        V::Str(s) => Ok(s.s.borrow().clone()),
        V::Sym(s) => Ok(s.to_string()),
        other => {
            if it.responds(&other, "to_path") {
                return Ok(to_s_plain(&it.call(&other, "to_path", Args::default())?));
            }
            if it.responds(&other, "to_str") {
                return Ok(to_s_plain(&it.call(&other, "to_str", Args::default())?));
            }
            Err(it.err(
                "TypeError",
                format!(
                    "no implicit conversion of {} into String",
                    it.class_of(&other).display_name()
                ),
            ))
        }
    }
}

fn kw(a: &Args, k: &str) -> Option<V> {
    if let Some(h) = &a.kw {
        return h.get(&V::sym(k));
    }
    if let Some(V::Hash(h)) = a.pos.last() {
        return h.get(&V::sym(k));
    }
    None
}

pub fn native_new(it: &Rc<Interp>, m: &Rc<Module>, a: &Args) -> NR {
    let n = m.display_name();
    let v = match n.as_str() {
        "Hash" => {
            let h = Rc::new(RHash::default());
            if let Some(b) = &a.blk {
                *h.default_proc.borrow_mut() = Some(b.clone());
            } else if let Some(d) = a.pos.first() {
                *h.default.borrow_mut() = d.clone();
            }
            V::Hash(h)
        }
        "Array" => {
            let n = match arg(a, 0) {
                V::Int(n) => n.max(0) as usize,
                V::Arr(x) => return Ok(Some(V::arr(x.borrow().clone()))),
                _ => 0,
            };
            let mut out = vec![];
            for i in 0..n {
                out.push(match &a.blk {
                    Some(b) => it.call_proc(b, vec![V::Int(i as i64)], None)?,
                    None => arg(a, 1),
                });
            }
            V::arr(out)
        }
        "String" => V::str(match arg(a, 0) {
            V::Str(s) => s.s.borrow().clone(),
            _ => String::new(),
        }),
        "Object" => it.new_obj(m.clone()),
        "Regexp" => match arg(a, 0) {
            V::Regex(r) => V::Regex(r),
            other => {
                let opts = match arg(a, 1) {
                    V::Int(i) if i & 1 != 0 => "i",
                    V::Bool(true) => "i",
                    _ => "",
                };
                make_regex(it, &to_s_plain(&other), opts)?
            }
        },
        "Proc" => match &a.blk {
            Some(b) => V::Proc(b.clone()),
            None => {
                return Err(it.err(
                    "ArgumentError",
                    "tried to create Proc object without a block",
                ))
            }
        },
        "Time" | "Random" | "Thread" => {
            return Err(it.err(
                "NotImplementedError",
                format!("{n} is nondeterministic and not available in lava-ruby"),
            ))
        }
        _ => return Ok(None),
    };
    Ok(Some(v))
}

pub fn env_m(it: &Rc<Interp>, name: &str, a: &Args) -> NR {
    let env = it.env.borrow().clone();
    let v = match name {
        "[]" => {
            let k = s_arg(it, a, 0)?;
            env.get(&k).map(|v| V::str(v.clone())).unwrap_or(V::Nil)
        }
        "fetch" => {
            let k = s_arg(it, a, 0)?;
            match env.get(&k) {
                Some(v) => V::str(v.clone()),
                None => {
                    if let Some(b) = &a.blk {
                        it.call_proc(b, vec![V::str(k)], None)?
                    } else if a.flat().len() > 1 {
                        arg(a, 1)
                    } else {
                        return Err(
                            it.err("KeyError", format!("key not found: {}", str_inspect(&k)))
                        );
                    }
                }
            }
        }
        "key?" | "has_key?" | "include?" | "member?" => {
            V::Bool(env.contains_key(&s_arg(it, a, 0)?))
        }
        "[]=" | "store" => {
            let k = s_arg(it, a, 0)?;
            match arg(a, 1) {
                V::Nil => {
                    it.env.borrow_mut().shift_remove(&k);
                }
                v => {
                    it.env.borrow_mut().insert(k, to_s_plain(&v));
                }
            }
            arg(a, 1)
        }
        "delete" => {
            let k = s_arg(it, a, 0)?;
            it.env
                .borrow_mut()
                .shift_remove(&k)
                .map(V::str)
                .unwrap_or(V::Nil)
        }
        "to_h" | "to_hash" => new_hash_from(
            env.iter()
                .map(|(k, v)| (V::str(k.clone()), V::str(v.clone())))
                .collect(),
        ),
        "keys" => V::arr(env.keys().map(|k| V::str(k.clone())).collect()),
        "values" => V::arr(env.values().map(|k| V::str(k.clone())).collect()),
        "each" | "each_pair" | "select" | "filter_map" | "map" | "any?" | "find" | "reject" => {
            let h = new_hash_from(
                env.iter()
                    .map(|(k, v)| (V::str(k.clone()), V::str(v.clone())))
                    .collect(),
            );
            return Ok(Some(it.call(&h, name, a.clone())?));
        }
        _ => return Ok(None),
    };
    Ok(Some(v))
}

pub fn kernel_fn(
    it: &Rc<Interp>,
    recv: &V,
    name: &str,
    a: &Args,
    caller: Option<&Rc<Frame>>,
) -> NR {
    if let V::Mod(m) = recv {
        if let Some(v) = module_fn(it, m, name, a)? {
            return Ok(Some(v));
        }
    }
    let v = match name {
        "to_json" if !matches!(recv, V::Obj(_) | V::Mod(_)) => {
            V::str(crate::json::generate(it, recv, false)?)
        }
        "require" => {
            let f = s_arg(it, a, 0)?;
            it.load_feature(&f, None)?
        }
        "load" => {
            let f = s_arg(it, a, 0)?;
            let p = PathBuf::from(&f);
            let p = if p.is_absolute() {
                p
            } else {
                it.cwd.borrow().clone().join(p)
            };
            it.load_file(&normalize(&p))?;
            V::Bool(true)
        }
        "raise" | "fail" => {
            let exc = match (arg(a, 0), a.pos.get(1).cloned()) {
                (V::Nil, _) if a.pos.is_empty() => {
                    let cur = it.gvars.borrow().get("$!").cloned();
                    cur.unwrap_or_else(|| it.make_exc("RuntimeError", "unhandled exception"))
                }
                (V::Str(s), _) => it.make_exc("RuntimeError", &s.s.borrow()),
                (V::Mod(c), msg) => {
                    let mut args = vec![];
                    if let Some(m) = msg {
                        args.push(m);
                    }
                    let mut na = Args::new(args);
                    na.kw = a.kw.clone();
                    let e = it.call(&V::Mod(c.clone()), "new", na)?;
                    if it.ivar_get(&e, "@message").is_nil() {
                        it.ivar_set(&e, "@message", V::str(c.display_name()))?;
                    }
                    e
                }
                (obj @ V::Obj(_), msg) => {
                    if let Some(m) = msg {
                        it.ivar_set(&obj, "@message", m)?;
                    }
                    obj
                }
                (other, _) => {
                    return Err(it.err(
                        "TypeError",
                        format!(
                            "exception class/object expected, got {}",
                            inspect_plain(&other)
                        ),
                    ))
                }
            };
            it.stamp(&exc);
            return Err(Ctrl::Raise(exc));
        }
        "puts" | "print" | "warn" | "p" | "pp" | "printf" | "putc" => {
            if name == "p" || name == "pp" {
                for x in a.flat() {
                    eprintln!("{}", rb_inspect(it, &x)?);
                }
                return Ok(Some(a.pos.first().cloned().unwrap_or(V::Nil)));
            }
            for x in a.pos.iter() {
                eprintln!("{}", it.to_s(x)?);
            }
            V::Nil
        }
        "format" | "sprintf" => {
            let f = s_arg(it, a, 0)?;
            let rest: Vec<V> = a.flat().into_iter().skip(1).collect();
            V::str(format_str(it, &f, &rest)?)
        }
        "Integer" => match arg(a, 0) {
            V::Int(i) => V::Int(i),
            V::Float(f) => V::Int(f as i64),
            V::Str(s) => {
                let t = s.s.borrow().trim().replace('_', "");
                let parsed = if let Some(h) = t.strip_prefix("0x") {
                    i64::from_str_radix(h, 16).ok()
                } else {
                    t.parse::<i64>().ok()
                };
                match parsed {
                    Some(i) => V::Int(i),
                    None => {
                        if matches!(kw(a, "exception"), Some(V::Bool(false))) {
                            V::Nil
                        } else {
                            return Err(it.err(
                                "ArgumentError",
                                format!(
                                    "invalid value for Integer(): {}",
                                    str_inspect(&s.s.borrow())
                                ),
                            ));
                        }
                    }
                }
            }
            V::Nil => {
                if matches!(kw(a, "exception"), Some(V::Bool(false))) {
                    V::Nil
                } else {
                    return Err(it.err("TypeError", "can't convert nil into Integer"));
                }
            }
            other => it.call(&other, "to_i", Args::default())?,
        },
        "Float" => match arg(a, 0) {
            V::Int(i) => V::Float(i as f64),
            V::Float(f) => V::Float(f),
            V::Str(s) => match s.s.borrow().trim().parse::<f64>() {
                Ok(f) => V::Float(f),
                Err(_) => {
                    return Err(it.err(
                        "ArgumentError",
                        format!("invalid value for Float(): {}", str_inspect(&s.s.borrow())),
                    ))
                }
            },
            _ => return Err(it.err("TypeError", "can't convert into Float")),
        },
        "String" => V::str(it.to_s(&arg(a, 0))?),
        "Array" => match arg(a, 0) {
            V::Arr(x) => V::Arr(x),
            V::Nil => V::arr(vec![]),
            V::Hash(h) => V::arr(
                h.entries()
                    .into_iter()
                    .map(|(k, v)| V::arr(vec![k, v]))
                    .collect(),
            ),
            other => {
                if it.responds(&other, "to_a") && !matches!(other, V::Str(_)) {
                    it.call(&other, "to_a", Args::default())?
                } else {
                    V::arr(vec![other])
                }
            }
        },
        "Hash" => match arg(a, 0) {
            V::Nil => V::empty_hash(),
            V::Arr(x) if x.borrow().is_empty() => V::empty_hash(),
            other => it.call(&other, "to_h", Args::default())?,
        },
        "catch" => {
            let tag = a
                .pos
                .first()
                .cloned()
                .unwrap_or_else(|| it.new_obj(it.object.clone()));
            let b = a
                .blk
                .clone()
                .ok_or_else(|| it.err("LocalJumpError", "no block"))?;
            match it.call_proc(&b, vec![tag.clone()], None) {
                Err(Ctrl::Throw(t, v)) if rb_eq(it, &t, &tag)? => v,
                other => other?,
            }
        }
        "throw" => return Err(Ctrl::Throw(arg(a, 0), arg(a, 1))),
        "caller" | "caller_locations" => V::arr(vec![]),
        "at_exit" | "trap" | "srand" | "sleep" | "gem" => V::Nil,
        "rand" | "system" | "exec" | "spawn" | "fork" | "`" | "open" | "gets" | "readline"
        | "exit!" => {
            return Err(it.err(
                "NotImplementedError",
                format!("Kernel#{name} is not available in lava-ruby"),
            ));
        }
        "exit" | "abort" => return Err(it.err("SystemExit", format!("exit via Kernel#{name}"))),
        "binding" => V::Nil,
        "frozen?" if recv.is_nil() => V::Bool(true),
        "lambda" | "proc" => match &a.blk {
            Some(b) => V::Proc(b.clone()),
            None => {
                return Err(it.err(
                    "ArgumentError",
                    "tried to create Proc object without a block",
                ))
            }
        },
        "block_given?" => V::Bool(caller.is_some_and(|c| c.block.is_some())),
        "loop" => {
            let b = a
                .blk
                .clone()
                .ok_or_else(|| it.err("LocalJumpError", "no block"))?;
            loop {
                match it.call_proc(&b, vec![], None) {
                    Ok(_) => {}
                    Err(Ctrl::Break(v, id)) if id == b.id => return Ok(Some(v)),
                    Err(Ctrl::Raise(e)) if it.is_a(&e, "StopIteration") => {
                        return Ok(Some(it.ivar_get(&e, "@result")))
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        "Rational" | "Complex" | "BigDecimal" => {
            return Err(it.err("NotImplementedError", format!("{name}()")))
        }
        "__lava_today" => {
            if let Some(d) = it.env.borrow().get("LAVA_RUBY_TODAY").cloned() {
                V::str(d)
            } else {
                let secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                let z = secs.div_euclid(86400) + 719468;
                let era = z.div_euclid(146097);
                let doe = z - era * 146097;
                let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
                let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
                let mp = (5 * doy + 2) / 153;
                let d = doy - (153 * mp + 2) / 5 + 1;
                let m = if mp < 10 { mp + 3 } else { mp - 9 };
                let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
                V::str(format!("{y:04}-{m:02}-{d:02}"))
            }
        }
        "autoload" => {
            let holder = match recv {
                V::Mod(m) => m.clone(),
                _ => it.object.clone(),
            };
            let n = s_arg(it, a, 0)?;
            let p = s_arg(it, a, 1)?;
            if !holder.consts.borrow().contains_key(&n) {
                it.autoloads.borrow_mut().insert((holder.id, n), p);
            }
            V::Nil
        }
        "autoload?" => {
            let holder = match recv {
                V::Mod(m) => m.clone(),
                _ => it.object.clone(),
            };
            let n = s_arg(it, a, 0)?;
            it.autoloads
                .borrow()
                .get(&(holder.id, n))
                .map(|p| V::str(p.clone()))
                .unwrap_or(V::Nil)
        }
        "require_relative" => {
            let f = s_arg(it, a, 0)?;
            let base = caller
                .map(|c| {
                    Path::new(&*c.file)
                        .parent()
                        .map(Path::to_path_buf)
                        .unwrap_or_default()
                })
                .unwrap_or_default();
            it.load_feature(&f, Some(&base))?
        }
        "__dir__" => V::str(
            caller
                .and_then(|c| {
                    Path::new(&*c.file)
                        .parent()
                        .map(|p| p.display().to_string())
                })
                .unwrap_or_default(),
        ),
        _ => return Ok(None),
    };
    Ok(Some(v))
}

fn module_fn(it: &Rc<Interp>, m: &Rc<Module>, name: &str, a: &Args) -> NR {
    let mn = m.display_name();
    let v = match (mn.as_str(), name) {
        ("YAML" | "Psych", "safe_load" | "load" | "unsafe_load") => {
            let s = s_arg(it, a, 0)?;
            crate::yaml::load(it, &s, kw(a, "symbolize_names").is_some_and(|v| v.truthy()))?
        }
        ("YAML" | "Psych", "safe_load_file" | "load_file" | "unsafe_load_file") => {
            let p = s_arg(it, a, 0)?;
            let s = it.read_file(Path::new(&p)).ok_or_else(|| {
                it.err(
                    "Errno::ENOENT",
                    format!("No such file or directory @ rb_sysopen - {p}"),
                )
            })?;
            crate::yaml::load(it, &s, kw(a, "symbolize_names").is_some_and(|v| v.truthy()))?
        }
        ("YAML" | "Psych", "dump") => V::str(crate::yaml::dump(it, &arg(a, 0))?),
        ("JSON", "generate" | "dump" | "fast_generate") => {
            V::str(crate::json::generate(it, &arg(a, 0), false)?)
        }
        ("JSON", "pretty_generate") => V::str(crate::json::generate(it, &arg(a, 0), true)?),
        ("JSON", "parse" | "load") => {
            let s = s_arg(it, a, 0)?;
            crate::json::parse(it, &s, kw(a, "symbolize_names").is_some_and(|v| v.truthy()))?
        }
        (
            "Digest::SHA256" | "Digest::SHA1" | "Digest::MD5",
            "hexdigest" | "digest" | "base64digest",
        ) => {
            let s = s_arg(it, a, 0)?;
            let hex = match mn.as_str() {
                "Digest::SHA256" => {
                    use sha2::Digest;
                    format!("{:x}", sha2::Sha256::digest(s.as_bytes()))
                }
                "Digest::MD5" => {
                    use md5::Digest;
                    format!("{:x}", md5::Md5::digest(s.as_bytes()))
                }
                _ => return Err(it.err("NotImplementedError", "SHA1")),
            };
            if name != "hexdigest" {
                return Err(it.err("NotImplementedError", format!("Digest {name}")));
            }
            V::str(hex)
        }
        ("File" | "FileTest", _) => return file_fn(it, name, a),
        ("Dir", _) => return dir_fn(it, name, a),
        (
            "FileUtils",
            "mkdir_p" | "makedirs" | "mkdir" | "touch" | "chmod" | "rm_rf" | "rm_f" | "cp" | "mv"
            | "ln_s" | "rm",
        ) => {
            if matches!(name, "mkdir_p" | "makedirs" | "mkdir") {
                arg(a, 0)
            } else {
                return Err(it.err(
                    "NotImplementedError",
                    format!("FileUtils.{name} is not available in lava-ruby"),
                ));
            }
        }
        ("Shellwords", "escape" | "shellescape") => V::str(shell_escape(&s_arg(it, a, 0)?)),
        ("Shellwords", "join") => V::str(
            to_list(it, &arg(a, 0))?
                .iter()
                .map(|x| shell_escape(&to_s_plain(x)))
                .collect::<Vec<_>>()
                .join(" "),
        ),
        ("Base64", "strict_encode64" | "encode64") => {
            let s = s_arg(it, a, 0)?;
            let e = base64_encode(s.as_bytes());
            V::str(if name == "encode64" {
                format!("{e}\n")
            } else {
                e
            })
        }
        ("Regexp", "escape" | "quote") => {
            V::str(fancy_regex::escape(&s_arg(it, a, 0)?).into_owned())
        }
        ("Regexp", "union") => {
            let parts: Vec<String> = a
                .flat()
                .iter()
                .flat_map(|x| match x {
                    V::Arr(l) => l.borrow().clone(),
                    o => vec![o.clone()],
                })
                .map(|x| match &x {
                    V::Regex(r) => r.src.clone(),
                    o => fancy_regex::escape(&to_s_plain(o)).into_owned(),
                })
                .collect();
            make_regex(it, &parts.join("|"), "")?
        }
        ("Regexp", "last_match") => {
            let lm = it.last_match.borrow().clone();
            match arg(a, 0) {
                V::Nil => lm,
                i => {
                    if lm.is_nil() {
                        V::Nil
                    } else {
                        it.call(&lm, "[]", Args::new(vec![i]))?
                    }
                }
            }
        }
        ("Integer", "sqrt") => V::Int(
            (match arg(a, 0) {
                V::Int(i) => i as f64,
                _ => 0.0,
            })
            .sqrt() as i64,
        ),
        ("Math", "sqrt" | "log" | "log2" | "log10" | "ceil") => {
            let x = match arg(a, 0) {
                V::Int(i) => i as f64,
                V::Float(f) => f,
                _ => 0.0,
            };
            V::Float(match name {
                "sqrt" => x.sqrt(),
                "log" => x.ln(),
                "log2" => x.log2(),
                _ => x.log10(),
            })
        }
        ("SecureRandom" | "Random", _) | ("Time", "now") | ("Process", "clock_gettime" | "pid") => {
            return Err(it.err(
                "NotImplementedError",
                format!("{mn}.{name} is nondeterministic and not available in lava-ruby"),
            ));
        }
        ("Struct", "new") => return Ok(None),
        _ => return Ok(None),
    };
    Ok(Some(v))
}

fn shell_escape(s: &str) -> String {
    if s.is_empty() {
        return "''".into();
    }
    let mut o = String::new();
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || "_-.,:/@+=".contains(c) {
            o.push(c);
        } else if c == '\n' {
            o.push_str("'\n'");
        } else {
            o.push('\\');
            o.push(c);
        }
    }
    o
}

fn base64_encode(b: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut o = String::new();
    for ch in b.chunks(3) {
        let n = (ch[0] as u32) << 16
            | (*ch.get(1).unwrap_or(&0) as u32) << 8
            | *ch.get(2).unwrap_or(&0) as u32;
        o.push(T[(n >> 18) as usize & 63] as char);
        o.push(T[(n >> 12) as usize & 63] as char);
        o.push(if ch.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        o.push(if ch.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    o
}

fn file_fn(it: &Rc<Interp>, name: &str, a: &Args) -> NR {
    let v = match name {
        "expand_path" | "absolute_path" => {
            let p = s_arg(it, a, 0)?;
            let base = match arg(a, 1) {
                V::Nil => it.cwd.borrow().clone(),
                _ => PathBuf::from(s_arg(it, a, 1)?),
            };
            let p = if let Some(rest) = p.strip_prefix('~') {
                PathBuf::from(format!(
                    "{}{}",
                    it.env.borrow().get("HOME").cloned().unwrap_or_default(),
                    rest
                ))
            } else {
                PathBuf::from(&p)
            };
            let full = if p.is_absolute() { p } else { base.join(p) };
            V::str(normalize(&full).display().to_string())
        }
        "join" => {
            let mut parts = vec![];
            for x in a.flat() {
                match x {
                    V::Arr(l) => parts.extend(l.borrow().iter().map(to_s_plain)),
                    o => parts.push(to_s_plain(&o)),
                }
            }
            let mut out = String::new();
            for (i, p) in parts.iter().enumerate() {
                if i > 0 {
                    if out.ends_with('/') && p.starts_with('/') {
                        out.pop();
                    } else if !out.ends_with('/') && !p.starts_with('/') {
                        out.push('/');
                    }
                }
                out.push_str(p);
            }
            V::str(out)
        }
        "basename" => {
            let p = s_arg(it, a, 0)?;
            let trimmed = p.trim_end_matches('/');
            let mut b = trimmed.rsplit('/').next().unwrap_or("").to_string();
            if let V::Str(ext) = arg(a, 1) {
                let ext = ext.s.borrow().clone();
                if ext == ".*" {
                    if let Some(i) = b.rfind('.') {
                        if i > 0 {
                            b.truncate(i);
                        }
                    }
                } else if let Some(x) = b.strip_suffix(ext.as_str()) {
                    b = x.to_string();
                }
            }
            V::str(b)
        }
        "dirname" => {
            let p = s_arg(it, a, 0)?;
            let t = p.trim_end_matches('/');
            V::str(match t.rfind('/') {
                Some(0) => "/".to_string(),
                Some(i) => t[..i].to_string(),
                None => ".".to_string(),
            })
        }
        "extname" => {
            let p = s_arg(it, a, 0)?;
            let b = p.rsplit('/').next().unwrap_or("");
            V::str(match b.rfind('.') {
                Some(i) if i > 0 => b[i..].to_string(),
                _ => String::new(),
            })
        }
        "exist?" | "file?" | "directory?" | "readable?" | "exists?" => {
            let p = it.abs(&s_arg(it, a, 0)?);
            let virt = it.virtual_files.borrow().contains_key(&p);
            V::Bool(match name {
                "file?" => p.is_file() || virt,
                "directory?" => p.is_dir(),
                _ => p.exists() || virt,
            })
        }
        "read" => {
            let p = it.abs(&s_arg(it, a, 0)?).display().to_string();
            V::str(it.read_file(Path::new(&p)).ok_or_else(|| {
                it.err(
                    "Errno::ENOENT",
                    format!("No such file or directory @ rb_sysopen - {p}"),
                )
            })?)
        }
        "readlines" | "foreach" => {
            let p = s_arg(it, a, 0)?;
            let s = it.read_file(Path::new(&p)).ok_or_else(|| {
                it.err(
                    "Errno::ENOENT",
                    format!("No such file or directory @ rb_sysopen - {p}"),
                )
            })?;
            V::arr(s.split_inclusive('\n').map(V::str).collect())
        }
        "write" | "open" | "delete" | "unlink" | "rename" | "symlink" | "chmod" => {
            return Err(it.err(
                "NotImplementedError",
                format!("File.{name} is not available in lava-ruby"),
            ));
        }
        "split" => {
            let p = s_arg(it, a, 0)?;
            let d = file_fn(it, "dirname", a)?.unwrap();
            let b = file_fn(it, "basename", &Args::new(vec![V::str(p)]))?.unwrap();
            V::arr(vec![d, b])
        }
        "fnmatch" | "fnmatch?" => {
            let pat = s_arg(it, a, 0)?;
            let p = s_arg(it, a, 1)?;
            V::Bool(glob_match(&pat, &p))
        }
        _ => return Ok(None),
    };
    Ok(Some(v))
}

pub fn glob_match(pat: &str, s: &str) -> bool {
    let re = glob_to_regex(pat);
    fancy_regex::Regex::new(&re)
        .map(|r| r.is_match(s).unwrap_or(false))
        .unwrap_or(false)
}

fn glob_to_regex(pat: &str) -> String {
    let mut out = String::from("^");
    let chars: Vec<char> = pat.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '*' => {
                if i + 1 < chars.len() && chars[i + 1] == '*' {
                    if i + 2 < chars.len() && chars[i + 2] == '/' {
                        out.push_str("(?:.*/)?");
                        i += 3;
                        continue;
                    }
                    out.push_str(".*");
                    i += 2;
                    continue;
                }
                out.push_str("[^/]*");
            }
            '?' => out.push_str("[^/]"),
            '{' => {
                let end = chars[i..]
                    .iter()
                    .position(|c| *c == '}')
                    .map(|p| p + i)
                    .unwrap_or(chars.len() - 1);
                let inner: String = chars[i + 1..end].iter().collect();
                out.push_str("(?:");
                out.push_str(
                    &inner
                        .split(',')
                        .map(|p| fancy_regex::escape(p).into_owned())
                        .collect::<Vec<_>>()
                        .join("|"),
                );
                out.push(')');
                i = end + 1;
                continue;
            }
            '.' | '(' | ')' | '+' | '|' | '^' | '$' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
        i += 1;
    }
    out.push('$');
    out
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    let mut ents: Vec<PathBuf> = rd.flatten().map(|e| e.path()).collect();
    ents.sort();
    for p in ents {
        let name = p
            .file_name()
            .map(|n| n.to_string_lossy().starts_with('.'))
            .unwrap_or(false);
        if name {
            continue;
        }
        out.push(p.clone());
        if p.is_dir() {
            walk(&p, out);
        }
    }
}

pub fn glob(pat: &str) -> Vec<String> {
    let first_meta = pat.find(['*', '?', '{', '[']).unwrap_or(pat.len());
    let base_end = pat[..first_meta].rfind('/').map(|i| i + 1).unwrap_or(0);
    let base = if base_end == 0 { "." } else { &pat[..base_end] };
    if first_meta == pat.len() {
        return if Path::new(pat).exists() {
            vec![pat.to_string()]
        } else {
            vec![]
        };
    }
    let mut all = vec![];
    walk(Path::new(base), &mut all);
    let re = glob_to_regex(pat);
    let Ok(r) = fancy_regex::Regex::new(&re) else {
        return vec![];
    };
    let mut out: Vec<String> = all
        .into_iter()
        .map(|p| {
            let s = p.display().to_string();
            if base == "." {
                s.trim_start_matches("./").to_string()
            } else {
                s
            }
        })
        .filter(|s| r.is_match(s).unwrap_or(false))
        .collect();
    out.sort();
    out
}

fn dir_fn(it: &Rc<Interp>, name: &str, a: &Args) -> NR {
    let v = match name {
        "[]" | "glob" => {
            let mut out = vec![];
            for p in a.flat() {
                match p {
                    V::Arr(l) => {
                        for x in l.borrow().iter() {
                            out.extend(glob(&to_s_plain(x)));
                        }
                    }
                    V::Str(s) => out.extend(glob(&s.s.borrow())),
                    _ => {}
                }
            }
            let list = V::arr(out.into_iter().map(V::str).collect());
            if let Some(b) = &a.blk {
                for x in to_list(it, &list)? {
                    it.call_proc(b, vec![x], None)?;
                }
                V::Nil
            } else {
                list
            }
        }
        "pwd" | "getwd" => V::str(it.cwd.borrow().clone().display().to_string()),
        "exist?" => V::Bool(Path::new(&s_arg(it, a, 0)?).is_dir()),
        "children" | "entries" | "each_child" => {
            let p = s_arg(it, a, 0)?;
            let mut names: Vec<String> = std::fs::read_dir(&p)
                .map(|rd| {
                    rd.flatten()
                        .map(|e| e.file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            names.sort();
            if name == "entries" {
                names.insert(0, "..".into());
                names.insert(0, ".".into());
            }
            V::arr(names.into_iter().map(V::str).collect())
        }
        "home" => V::str(it.env.borrow().get("HOME").cloned().unwrap_or_default()),
        "chdir" => {
            let target = PathBuf::from(s_arg(it, a, 0)?);
            let target = normalize(&if target.is_absolute() {
                target
            } else {
                it.cwd.borrow().join(target)
            });
            let prev = std::mem::replace(&mut *it.cwd.borrow_mut(), target);
            match &a.blk {
                Some(b) => {
                    let r = it.call_proc(b, vec![], None);
                    *it.cwd.borrow_mut() = prev;
                    r?
                }
                None => V::Int(0),
            }
        }
        "mkdir" | "rmdir" | "mktmpdir" | "tmpdir" => {
            return Err(it.err(
                "NotImplementedError",
                format!("Dir.{name} is not available in lava-ruby"),
            ))
        }
        _ => return Ok(None),
    };
    Ok(Some(v))
}

pub fn format_str(it: &Rc<Interp>, f: &str, args: &[V]) -> Result<String, Ctrl> {
    let mut out = String::new();
    let chars: Vec<char> = f.chars().collect();
    let mut i = 0;
    let mut ai = 0;
    let named = match args.first() {
        Some(V::Hash(h)) => Some(h.clone()),
        _ => None,
    };
    while i < chars.len() {
        let c = chars[i];
        if c != '%' {
            out.push(c);
            i += 1;
            continue;
        }
        i += 1;
        if i >= chars.len() {
            out.push('%');
            break;
        }
        if chars[i] == '%' {
            out.push('%');
            i += 1;
            continue;
        }
        let mut flags = String::new();
        while i < chars.len() && "-+ 0#".contains(chars[i]) {
            flags.push(chars[i]);
            i += 1;
        }
        let mut name_val = None;
        if i < chars.len() && (chars[i] == '<' || chars[i] == '{') {
            let close = if chars[i] == '<' { '>' } else { '}' };
            let brace = chars[i] == '{';
            let st = i + 1;
            while i < chars.len() && chars[i] != close {
                i += 1;
            }
            let key: String = chars[st..i].iter().collect();
            i += 1;
            let v = named
                .as_ref()
                .and_then(|h| h.get(&V::sym(&key)))
                .ok_or_else(|| it.err("KeyError", format!("key<{key}> not found")))?;
            if brace {
                out.push_str(&it.to_s(&v)?);
                continue;
            }
            name_val = Some(v);
        }
        let mut width = String::new();
        while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '*') {
            if chars[i] == '*' {
                width = to_s_plain(args.get(ai).unwrap_or(&V::Int(0)));
                ai += 1;
            } else {
                width.push(chars[i]);
            }
            i += 1;
        }
        let mut prec = None;
        if i < chars.len() && chars[i] == '.' {
            i += 1;
            let mut p = String::new();
            while i < chars.len() && chars[i].is_ascii_digit() {
                p.push(chars[i]);
                i += 1;
            }
            prec = Some(p.parse::<usize>().unwrap_or(0));
        }
        let conv = chars.get(i).copied().unwrap_or('s');
        i += 1;
        let v = match name_val {
            Some(v) => v,
            None => {
                let v = args.get(ai).cloned().unwrap_or(V::Nil);
                ai += 1;
                v
            }
        };
        let body = match conv {
            's' => {
                let s = it.to_s(&v)?;
                match prec {
                    Some(p) => s.chars().take(p).collect(),
                    None => s,
                }
            }
            'p' => rb_inspect(it, &v)?,
            'd' | 'i' | 'u' => {
                let n = match &v {
                    V::Int(n) => *n,
                    V::Float(f) => f.floor() as i64,
                    V::Str(s) => s.s.borrow().trim().parse().unwrap_or(0),
                    _ => 0,
                };
                let mut s = n.abs().to_string();
                if n < 0 {
                    s = format!("-{s}");
                } else if flags.contains('+') {
                    s = format!("+{s}");
                }
                s
            }
            'f' => {
                let x = match &v {
                    V::Int(n) => *n as f64,
                    V::Float(f) => *f,
                    _ => 0.0,
                };
                format!("{:.*}", prec.unwrap_or(6), x)
            }
            'x' | 'X' => {
                let n = if let V::Int(n) = v { n } else { 0 };
                let s = format!("{n:x}");
                if conv == 'X' {
                    s.to_uppercase()
                } else {
                    s
                }
            }
            'o' => format!("{:o}", if let V::Int(n) = v { n } else { 0 }),
            'b' => format!("{:b}", if let V::Int(n) = v { n } else { 0 }),
            'e' => format!("{:e}", if let V::Float(f) = v { f } else { 0.0 }),
            'c' => to_s_plain(&v)
                .chars()
                .next()
                .map(|c| c.to_string())
                .unwrap_or_default(),
            other => {
                return Err(it.err(
                    "ArgumentError",
                    format!("malformed format string - %{other}"),
                ))
            }
        };
        let w: usize = width.parse().unwrap_or(0);
        let n = body.chars().count();
        if n < w {
            let pad = w - n;
            if flags.contains('-') {
                out.push_str(&body);
                out.push_str(&" ".repeat(pad));
            } else if flags.contains('0') && matches!(conv, 'd' | 'i' | 'f' | 'x' | 'X' | 'o' | 'b')
            {
                let (sign, digits) = if body.starts_with('-') || body.starts_with('+') {
                    body.split_at(1)
                } else {
                    ("", body.as_str())
                };
                out.push_str(sign);
                out.push_str(&"0".repeat(pad));
                out.push_str(digits);
            } else {
                out.push_str(&" ".repeat(pad));
                out.push_str(&body);
            }
        } else {
            out.push_str(&body);
        }
    }
    Ok(out)
}
