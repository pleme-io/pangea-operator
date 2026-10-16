use std::cell::RefCell;
use std::rc::Rc;

use indexmap::IndexMap;

use crate::fmt::*;
use crate::interp::*;
use crate::value::*;

type NR = Result<Option<V>, Ctrl>;

pub fn bootstrap(it: &Rc<Interp>) {
    let o = it.object.clone();
    let module = it.define_class("Module", None, &o);
    it.define_class("Class", Some(module), &o);
    for k in [
        "Kernel",
        "Comparable",
        "Enumerable",
        "Math",
        "Process",
        "GC",
        "ObjectSpace",
        "Signal",
        "Marshal",
        "Warning",
    ] {
        it.define_module(k, &o);
    }
    o.includes_mut().push(it.cls("Kernel"));
    for c in [
        "NilClass",
        "TrueClass",
        "FalseClass",
        "String",
        "Symbol",
        "Array",
        "Hash",
        "Range",
        "Proc",
        "Regexp",
        "MatchData",
        "Method",
        "UnboundMethod",
        "Time",
        "Random",
        "IO",
        "Encoding",
        "Binding",
        "Thread",
        "Mutex",
        "Data",
    ] {
        it.define_class(c, None, &o);
    }
    let numeric = it.define_class("Numeric", None, &o);
    numeric.includes_mut().push(it.cls("Comparable"));
    it.define_class("Integer", Some(numeric.clone()), &o);
    it.define_class("Float", Some(numeric), &o);
    it.cls("String").includes_mut().push(it.cls("Comparable"));
    for c in ["Array", "Hash", "Range"] {
        it.cls(c).includes_mut().push(it.cls("Enumerable"));
    }
    let exc = it.define_class("Exception", None, &o);
    let tree: &[(&'static str, &'static str)] = &[
        ("ScriptError", "Exception"),
        ("LoadError", "ScriptError"),
        ("NotImplementedError", "ScriptError"),
        ("SyntaxError", "ScriptError"),
        ("NoMemoryError", "Exception"),
        ("SecurityError", "Exception"),
        ("SystemExit", "Exception"),
        ("SignalException", "Exception"),
        ("Interrupt", "SignalException"),
        ("SystemStackError", "Exception"),
        ("StandardError", "Exception"),
        ("ArgumentError", "StandardError"),
        ("UncaughtThrowError", "ArgumentError"),
        ("EncodingError", "StandardError"),
        ("FiberError", "StandardError"),
        ("IOError", "StandardError"),
        ("EOFError", "IOError"),
        ("IndexError", "StandardError"),
        ("KeyError", "IndexError"),
        ("StopIteration", "IndexError"),
        ("LocalJumpError", "StandardError"),
        ("NameError", "StandardError"),
        ("NoMethodError", "NameError"),
        ("RangeError", "StandardError"),
        ("FloatDomainError", "RangeError"),
        ("RegexpError", "StandardError"),
        ("RuntimeError", "StandardError"),
        ("FrozenError", "RuntimeError"),
        ("SystemCallError", "StandardError"),
        ("ThreadError", "StandardError"),
        ("TypeError", "StandardError"),
        ("ZeroDivisionError", "StandardError"),
    ];
    let _ = exc;
    for (c, s) in tree {
        let sup = it.cls(s);
        it.define_class(c, Some(sup), &o);
    }
    let errno = it.define_module("Errno", &o);
    for c in ["ENOENT", "EACCES", "EEXIST", "ENOTDIR", "ECONNREFUSED"] {
        it.define_class(c, Some(it.cls("SystemCallError")), &errno);
    }
    let fc = it.define_class("File", Some(it.cls("IO")), &o);
    fc.consts
        .borrow_mut()
        .insert("SEPARATOR".into(), V::str("/"));
    it.define_class("Dir", None, &o);
    it.define_module("FileTest", &o);
    let ft = it.define_module("Float", &o);
    let _ = ft;
    it.cls("Float")
        .consts
        .borrow_mut()
        .insert("INFINITY".into(), V::Float(f64::INFINITY));
    it.cls("Float")
        .consts
        .borrow_mut()
        .insert("NAN".into(), V::Float(f64::NAN));
    o.consts
        .borrow_mut()
        .insert("RUBY_VERSION".into(), V::str("3.3.10"));
    o.consts
        .borrow_mut()
        .insert("RUBY_PLATFORM".into(), V::str("lava-ruby"));
    o.consts
        .borrow_mut()
        .insert("RUBY_ENGINE".into(), V::str("lava-ruby"));
    o.consts.borrow_mut().insert("ARGV".into(), V::arr(vec![]));
    let io = it.cls("IO");
    o.consts
        .borrow_mut()
        .insert("STDERR".into(), it.new_obj(io.clone()));
    o.consts
        .borrow_mut()
        .insert("STDOUT".into(), it.new_obj(io));
    let env = it.define_class("LavaEnv", None, &o);
    o.consts.borrow_mut().insert("ENV".into(), it.new_obj(env));
    crate::stdlib::bootstrap(it);
}

pub fn to_s_plain(v: &V) -> String {
    match v {
        V::Nil => String::new(),
        V::Bool(b) => b.to_string(),
        V::Int(i) => i.to_string(),
        V::Float(f) => float_to_s(*f),
        V::Str(s) => s.s.borrow().clone(),
        V::Sym(s) => s.to_string(),
        V::Mod(m) => m.display_name(),
        V::Regex(r) => format!("(?-mix:{})", r.src),
        other => inspect_plain(other),
    }
}

pub fn inspect_plain(v: &V) -> String {
    match v {
        V::Nil => "nil".into(),
        V::Str(s) => str_inspect(&s.s.borrow()),
        V::Sym(s) => sym_inspect(s),
        V::Arr(a) => format!(
            "[{}]",
            a.borrow()
                .iter()
                .map(inspect_plain)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        V::Hash(h) => {
            if h.len() == 0 {
                return "{}".into();
            }
            format!(
                "{{{}}}",
                h.entries()
                    .iter()
                    .map(|(k, v)| format!("{}=>{}", inspect_plain(k), inspect_plain(v)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        V::Range(r) => format!(
            "{}{}{}",
            inspect_plain(&r.0),
            if r.2 { "..." } else { ".." },
            if r.1.is_nil() {
                String::new()
            } else {
                inspect_plain(&r.1)
            }
        ),
        V::Regex(r) => format!("/{}/", r.src),
        V::Proc(_) => "#<Proc>".into(),
        V::Method(m) => format!("#<Method: {}>", m.name),
        V::Obj(o) => format!("#<{}>", o.class.borrow().display_name()),
        other => to_s_plain(other),
    }
}

pub fn describe_recv(it: &Interp, v: &V) -> String {
    match v {
        V::Nil => "nil".into(),
        V::Bool(true) => "true".into(),
        V::Bool(false) => "false".into(),
        V::Mod(m) => format!(
            "{} {}",
            if m.is_class { "class" } else { "module" },
            m.display_name()
        ),
        V::Obj(o) if Rc::ptr_eq(&o.class.borrow(), &it.object) => "main:Object".into(),
        other => format!("an instance of {}", it.class_of(other).display_name()),
    }
}

pub fn make_regex(it: &Interp, src: &str, opts: &str) -> R {
    let mut flags = String::new();
    if opts.contains('i') {
        flags.push('i');
    }
    if opts.contains('m') {
        flags.push('s');
    }
    if opts.contains('x') {
        flags.push('x');
    }
    let translated = translate_regex(src);
    let pat = if flags.is_empty() {
        translated
    } else {
        format!("(?{flags}){translated}")
    };
    let re = fancy_regex::Regex::new(&pat)
        .map_err(|e| it.err("RegexpError", format!("{e}: /{src}/")))?;
    Ok(V::Regex(Rc::new(RRegex {
        src: src.to_string(),
        re,
    })))
}

fn translate_regex(src: &str) -> String {
    let mut out = String::new();
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('h') => out.push_str("[0-9a-fA-F]"),
                Some('A') => out.push_str("\\A"),
                Some('z') => out.push_str("\\z"),
                Some('Z') => out.push_str("(?=\\n?\\z)"),
                Some('/') => out.push('/'),
                Some(x) => {
                    out.push('\\');
                    out.push(x);
                }
                None => out.push('\\'),
            }
        } else if c == '(' && chars.peek() == Some(&'?') {
            out.push('(');
            let mut look = chars.clone();
            look.next();
            if look.peek() == Some(&'<') {
                let mut l2 = look.clone();
                l2.next();
                if l2.peek().is_some_and(|c| c.is_alphabetic()) {
                    chars.next();
                    chars.next();
                    out.push_str("?P<");
                    continue;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

pub fn to_list(it: &Rc<Interp>, v: &V) -> Result<Vec<V>, Ctrl> {
    match v {
        V::Arr(a) => Ok(a.borrow().clone()),
        V::Hash(h) => Ok(h
            .entries()
            .into_iter()
            .map(|(k, v)| V::arr(vec![k, v]))
            .collect()),
        V::Range(r) => match (&r.0, &r.1) {
            (V::Int(a), V::Int(b)) => {
                let end = if r.2 { *b } else { *b + 1 };
                Ok((*a..end).map(V::Int).collect())
            }
            (V::Int(a), V::Nil) => Ok((*a..*a + 100_000).map(V::Int).collect()),
            (V::Str(a), V::Str(b)) => {
                let (a, b) = (a.s.borrow().clone(), b.s.borrow().clone());
                let mut out = vec![];
                let mut cur = a;
                loop {
                    if r.2 && cur == b {
                        break;
                    }
                    out.push(V::str(cur.clone()));
                    if cur == b || out.len() > 100_000 {
                        break;
                    }
                    cur = str_succ(&cur);
                }
                Ok(out)
            }
            _ => Err(it.err("TypeError", "can't iterate range")),
        },
        V::Nil => Ok(vec![]),
        other => {
            if it.find_method(other, "each").is_some() {
                let acc = Rc::new(RefCell::new(vec![]));
                let acc2 = acc.clone();
                let p = native_proc(it, move |_it, args| {
                    let v = if args.len() == 1 {
                        args[0].clone()
                    } else {
                        V::arr(args)
                    };
                    acc2.borrow_mut().push(v);
                    Ok(V::Nil)
                });
                it.call(other, "each", Args::with_blk(vec![], Some(p)))?;
                let out = acc.borrow().clone();
                return Ok(out);
            }
            if it.find_method(other, "to_a").is_some() {
                let r = it.call(other, "to_a", Args::default())?;
                return to_list(it, &r);
            }
            Err(it.err(
                "TypeError",
                format!(
                    "no implicit conversion of {} into Array",
                    it.class_of(other).display_name()
                ),
            ))
        }
    }
}

thread_local! {
    pub static NATIVE_PROCS: RefCell<Vec<Rc<dyn Fn(&Rc<Interp>, Vec<V>) -> R>>> = RefCell::new(vec![]);
}

pub fn native_proc(it: &Rc<Interp>, f: impl Fn(&Rc<Interp>, Vec<V>) -> R + 'static) -> Rc<Proc> {
    let idx = NATIVE_PROCS.with(|n| {
        let mut n = n.borrow_mut();
        n.push(Rc::new(f));
        n.len() - 1
    });
    let fr = it.top_frame.borrow().clone().unwrap();
    Rc::new(Proc {
        body: ProcBody::SymToProc(format!("__lava_native_proc_{idx}")),
        scope: Scope::new(None),
        frame: fr,
        lambda: false,
        id: it.id(),
    })
}

pub fn str_succ(s: &str) -> String {
    let mut chars: Vec<char> = s.chars().collect();
    let mut i = chars.len();
    while i > 0 {
        i -= 1;
        let c = chars[i];
        match c {
            'z' => chars[i] = 'a',
            'Z' => chars[i] = 'A',
            '9' => chars[i] = '0',
            c if c.is_ascii_alphanumeric() => {
                chars[i] = (c as u8 + 1) as char;
                return chars.into_iter().collect();
            }
            _ => continue,
        }
        if i == 0 {
            let first = match c {
                'z' => 'a',
                'Z' => 'A',
                _ => '1',
            };
            chars.insert(0, first);
            return chars.into_iter().collect();
        }
    }
    chars.into_iter().collect()
}

pub fn native_responds(it: &Interp, v: &V, name: &str) -> bool {
    const OBJ: &[&str] = &[
        "class",
        "is_a?",
        "kind_of?",
        "nil?",
        "respond_to?",
        "send",
        "public_send",
        "inspect",
        "to_s",
        "==",
        "!=",
        "freeze",
        "frozen?",
        "dup",
        "tap",
        "then",
        "instance_variable_get",
        "instance_variable_set",
        "hash",
        "object_id",
        "instance_eval",
        "instance_exec",
        "extend",
        "method",
        "itself",
        "equal?",
        "eql?",
        "instance_of?",
        "define_singleton_method",
        "singleton_class",
        "instance_variables",
        "instance_variable_defined?",
        "===",
        "!",
    ];
    if OBJ.contains(&name) {
        return true;
    }
    let list: &[&str] = match v {
        V::Str(_) => &[
            "to_str",
            "to_sym",
            "length",
            "size",
            "empty?",
            "upcase",
            "downcase",
            "strip",
            "split",
            "gsub",
            "sub",
            "start_with?",
            "end_with?",
            "include?",
            "match?",
            "tr",
            "+",
            "*",
            "%",
            "=~",
            "to_i",
            "to_f",
            "each_char",
            "chars",
            "bytes",
            "lines",
            "capitalize",
            "<=>",
            "[]",
            "encoding",
            "force_encoding",
            "unpack1",
            "ljust",
            "rjust",
            "center",
            "squeeze",
            "delete",
            "chomp",
            "chop",
            "succ",
            "ord",
            "hex",
            "match",
            "scan",
            "casecmp?",
            "count",
            "index",
            "reverse",
            "replace",
            "<<",
            "concat",
            "freeze",
            "each_line",
            "unicode_normalize",
            "slice",
            "tr_s",
            "swapcase",
            "intern",
            "to_str",
            "dump",
            "b",
            "bytesize",
            "format",
            "lstrip",
            "rstrip",
            "partition",
            "rpartition",
            "prepend",
            "insert",
            "upcase!",
            "downcase!",
            "gsub!",
            "sub!",
            "strip!",
            "squish",
            "empty?",
            "any?",
            "to_s",
            "to_h",
        ],
        V::Arr(_) => &[
            "each",
            "map",
            "select",
            "reject",
            "to_a",
            "to_ary",
            "length",
            "size",
            "empty?",
            "any?",
            "all?",
            "first",
            "last",
            "include?",
            "join",
            "sort",
            "sort_by",
            "uniq",
            "flatten",
            "compact",
            "each_with_index",
            "each_with_object",
            "inject",
            "reduce",
            "sum",
            "min",
            "max",
            "group_by",
            "partition",
            "zip",
            "push",
            "<<",
            "pop",
            "shift",
            "unshift",
            "concat",
            "+",
            "-",
            "&",
            "|",
            "[]",
            "[]=",
            "dig",
            "to_h",
            "count",
            "find",
            "detect",
            "index",
            "flat_map",
            "filter_map",
            "tally",
            "each_slice",
            "take",
            "drop",
            "reverse",
            "rotate",
            "sample",
            "shuffle",
            "to_set",
            "none?",
            "one?",
            "min_by",
            "max_by",
            "sum",
            "hash",
            "fetch",
            "values_at",
            "product",
            "combination",
            "cycle",
            "lazy",
            "chunk_while",
            "slice_when",
            "each_cons",
            "step",
            "insert",
            "delete",
            "delete_at",
            "delete_if",
            "keep_if",
            "fill",
            "clear",
            "replace",
            "assoc",
            "transpose",
            "minmax",
            "find_index",
            "pack",
            "sort!",
            "map!",
            "select!",
            "reject!",
            "uniq!",
            "compact!",
            "flatten!",
            "filter",
            "collect",
            "entries",
        ],
        V::Hash(_) => &[
            "each",
            "each_pair",
            "map",
            "select",
            "reject",
            "keys",
            "values",
            "key?",
            "has_key?",
            "include?",
            "member?",
            "fetch",
            "dig",
            "merge",
            "merge!",
            "update",
            "to_h",
            "to_a",
            "length",
            "size",
            "empty?",
            "any?",
            "all?",
            "transform_keys",
            "transform_values",
            "each_with_object",
            "inject",
            "reduce",
            "[]",
            "[]=",
            "delete",
            "sort_by",
            "group_by",
            "min_by",
            "max_by",
            "sum",
            "count",
            "find",
            "detect",
            "filter_map",
            "flat_map",
            "sort",
            "values_at",
            "slice",
            "except",
            "compact",
            "invert",
            "key",
            "value?",
            "has_value?",
            "default",
            "default=",
            "store",
            "to_hash",
            "each_key",
            "each_value",
            "partition",
            "sum",
            "tally",
            "none?",
            "freeze",
            "deep_merge",
            "symbolize_keys",
            "stringify_keys",
            "filter",
            "filter_map",
            "compare_by_identity",
            "fetch_values",
            "select!",
            "reject!",
            "keep_if",
            "delete_if",
            "clear",
            "replace",
            "first",
            "entries",
            "zip",
            "uniq",
            "min",
            "max",
            "each_with_index",
            "find_all",
            "collect",
            "transform_keys!",
            "transform_values!",
            "dup",
            "to_set",
        ],
        V::Sym(_) => &[
            "to_sym",
            "to_s",
            "to_proc",
            "length",
            "size",
            "upcase",
            "downcase",
            "<=>",
            "start_with?",
            "end_with?",
            "id2name",
            "name",
            "[]",
            "succ",
            "empty?",
        ],
        V::Int(_) | V::Float(_) => &[
            "+",
            "-",
            "*",
            "/",
            "%",
            "**",
            "to_i",
            "to_f",
            "to_s",
            "positive?",
            "negative?",
            "zero?",
            "times",
            "upto",
            "downto",
            "abs",
            "round",
            "floor",
            "ceil",
            "<=>",
            "even?",
            "odd?",
            "succ",
            "pred",
            "between?",
            "clamp",
            "integer?",
            "nonzero?",
            "divmod",
            "fdiv",
            "digits",
            "pow",
            "step",
            "ord",
            "chr",
            "finite?",
            "nan?",
            "infinite?",
            "truncate",
        ],
        V::Nil => &["to_a", "to_s", "to_i", "to_h", "inspect", "&", "|"],
        V::Proc(_) => &[
            "call",
            "()",
            "yield",
            "[]",
            "to_proc",
            "arity",
            "lambda?",
            "curry",
            "parameters",
            "===",
        ],
        V::Range(_) => &[
            "each", "map", "to_a", "include?", "cover?", "first", "last", "min", "max", "begin",
            "end", "step", "size", "count", "select", "sum", "member?", "===",
        ],
        V::Regex(_) => &["match", "match?", "=~", "source", "===", "to_s", "names"],
        V::Mod(_) => &[
            "new",
            "name",
            "const_get",
            "constants",
            "instance_methods",
            "method_defined?",
            "ancestors",
            "include?",
            "define_method",
            "class_eval",
            "module_eval",
            "attr_accessor",
            "attr_reader",
            "superclass",
            "allocate",
            "const_defined?",
            "instance_method",
            "private_method_defined?",
            "public_method_defined?",
            "to_s",
            "<",
            "<=",
            "class_variable_get",
            "class_variable_set",
            "class_variables",
        ],
        V::Obj(o) => {
            let cname = o.class.borrow().display_name();
            if cname == "LavaEnv" {
                return [
                    "[]", "fetch", "key?", "to_h", "[]=", "delete", "include?", "has_key?", "keys",
                    "values", "each",
                ]
                .contains(&name);
            }
            if it.is_a(v, "Exception") {
                return [
                    "message",
                    "backtrace",
                    "full_message",
                    "cause",
                    "set_backtrace",
                    "exception",
                ]
                .contains(&name);
            }
            if it.is_a(v, "Enumerable") {
                return [
                    "map",
                    "select",
                    "to_a",
                    "each_with_object",
                    "each_with_index",
                    "include?",
                    "count",
                    "find",
                    "reduce",
                    "inject",
                    "sort",
                    "sort_by",
                    "any?",
                    "all?",
                    "empty?",
                    "first",
                    "min",
                    "max",
                    "sum",
                    "group_by",
                    "reject",
                    "flat_map",
                    "to_h",
                    "min_by",
                    "max_by",
                    "partition",
                    "filter_map",
                    "zip",
                    "tally",
                    "uniq",
                    "entries",
                ]
                .contains(&name);
            }
            if it.is_a(v, "Comparable") {
                return ["<", ">", "<=", ">=", "between?", "clamp", "=="].contains(&name);
            }
            &[]
        }
        _ => &[],
    };
    list.contains(&name)
}

pub fn native_inherited(
    _it: &Rc<Interp>,
    _sup: &Rc<Module>,
    _sub: &Rc<Module>,
) -> Result<(), Ctrl> {
    Ok(())
}

pub fn native_call(
    it: &Rc<Interp>,
    recv: &V,
    name: &str,
    args: Args,
    caller: Option<&Rc<Frame>>,
) -> NR {
    if let Some(rest) = name.strip_prefix("__lava_native_proc_") {
        let idx: usize = rest.parse().unwrap_or(0);
        let f = NATIVE_PROCS.with(|n| n.borrow()[idx].clone());
        let mut a = vec![recv.clone()];
        a.extend(args.flat());
        return f(it, a).map(Some);
    }
    let r = match recv {
        V::Str(s) => crate::builtins::str_m(it, recv, s, name, &args)?,
        V::Arr(a) => crate::builtins::arr_m(it, recv, a, name, &args)?,
        V::Hash(h) => crate::builtins::hash_m(it, recv, h, name, &args)?,
        V::Int(_) | V::Float(_) => crate::builtins::num_m(it, recv, name, &args)?,
        V::Sym(s) => crate::builtins::sym_m(it, recv, s, name, &args)?,
        V::Range(r) => crate::builtins::range_m(it, recv, r, name, &args)?,
        V::Proc(p) => crate::builtins::proc_m(it, recv, p, name, &args)?,
        V::Regex(r) => crate::builtins::regex_m(it, recv, r, name, &args)?,
        V::Method(m) => crate::builtins::method_m(it, m, name, &args)?,
        V::Nil => crate::builtins::nil_m(it, name, &args)?,
        V::Bool(b) => crate::builtins::bool_m(*b, name, &args)?,
        V::Mod(m) => crate::builtins::module_m(it, recv, m, name, &args, caller)?,
        V::Obj(_) => crate::builtins::obj_special(it, recv, name, &args)?,
    };
    if r.is_some() {
        return Ok(r);
    }
    if let Some(v) = crate::stdlib::kernel_fn(it, recv, name, &args, caller)? {
        return Ok(Some(v));
    }
    crate::builtins::object_m(it, recv, name, args, caller)
}

pub fn rb_eq(it: &Rc<Interp>, a: &V, b: &V) -> Result<bool, Ctrl> {
    Ok(match (a, b) {
        (V::Nil, V::Nil) => true,
        (V::Bool(x), V::Bool(y)) => x == y,
        (V::Int(x), V::Int(y)) => x == y,
        (V::Float(x), V::Float(y)) => x == y,
        (V::Int(x), V::Float(y)) | (V::Float(y), V::Int(x)) => (*x as f64) == *y,
        (V::Str(x), V::Str(y)) => *x.s.borrow() == *y.s.borrow(),
        (V::Sym(x), V::Sym(y)) => x == y,
        (V::Arr(x), V::Arr(y)) => {
            if Rc::ptr_eq(x, y) {
                return Ok(true);
            }
            let (x, y) = (x.borrow().clone(), y.borrow().clone());
            if x.len() != y.len() {
                return Ok(false);
            }
            for (p, q) in x.iter().zip(y.iter()) {
                if !rb_eq(it, p, q)? {
                    return Ok(false);
                }
            }
            true
        }
        (V::Hash(x), V::Hash(y)) => {
            if x.len() != y.len() {
                return Ok(false);
            }
            for (k, v) in x.entries() {
                match y.get(&k) {
                    Some(w) => {
                        if !rb_eq(it, &v, &w)? {
                            return Ok(false);
                        }
                    }
                    None => return Ok(false),
                }
            }
            true
        }
        (V::Range(x), V::Range(y)) => {
            rb_eq(it, &x.0, &y.0)? && rb_eq(it, &x.1, &y.1)? && x.2 == y.2
        }
        (V::Mod(x), V::Mod(y)) => x.id == y.id,
        (V::Obj(_), _) => {
            if it.find_method(a, "==").is_some() {
                return Ok(it.call(a, "==", Args::new(vec![b.clone()]))?.truthy());
            }
            a.ptr() == b.ptr()
        }
        _ => a.ptr() == b.ptr() && std::mem::discriminant(a) == std::mem::discriminant(b),
    })
}

pub fn rb_cmp(it: &Rc<Interp>, a: &V, b: &V) -> Result<std::cmp::Ordering, Ctrl> {
    use std::cmp::Ordering;
    Ok(match (a, b) {
        (V::Int(x), V::Int(y)) => x.cmp(y),
        (V::Float(x), V::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (V::Int(x), V::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (V::Float(x), V::Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (V::Str(x), V::Str(y)) => x.s.borrow().as_bytes().cmp(y.s.borrow().as_bytes()),
        (V::Sym(x), V::Sym(y)) => x.as_bytes().cmp(y.as_bytes()),
        (V::Arr(x), V::Arr(y)) => {
            let (x, y) = (x.borrow().clone(), y.borrow().clone());
            for (p, q) in x.iter().zip(y.iter()) {
                let c = rb_cmp(it, p, q)?;
                if c != Ordering::Equal {
                    return Ok(c);
                }
            }
            x.len().cmp(&y.len())
        }
        (V::Bool(_), V::Bool(_)) | (V::Nil, V::Nil) => {
            return Err(it.err("NoMethodError", "undefined method '<=>' for comparison"));
        }
        _ => {
            if it.find_method(a, "<=>").is_some() {
                let r = it.call(a, "<=>", Args::new(vec![b.clone()]))?;
                return match r {
                    V::Int(i) => Ok(i.cmp(&0)),
                    _ => Err(it.err(
                        "ArgumentError",
                        format!(
                            "comparison of {} with {} failed",
                            it.class_of(a).display_name(),
                            inspect_plain(b)
                        ),
                    )),
                };
            }
            return Err(it.err(
                "ArgumentError",
                format!(
                    "comparison of {} with {} failed",
                    it.class_of(a).display_name(),
                    inspect_plain(b)
                ),
            ));
        }
    })
}

pub fn rb_inspect(it: &Rc<Interp>, v: &V) -> Result<String, Ctrl> {
    match v {
        V::Arr(a) => {
            let items = a.borrow().clone();
            let mut parts = vec![];
            for x in &items {
                parts.push(rb_inspect(it, x)?);
            }
            Ok(format!("[{}]", parts.join(", ")))
        }
        V::Hash(h) => {
            if h.len() == 0 {
                return Ok("{}".into());
            }
            let mut parts = vec![];
            for (k, x) in h.entries() {
                parts.push(format!("{}=>{}", rb_inspect(it, &k)?, rb_inspect(it, &x)?));
            }
            Ok(format!("{{{}}}", parts.join(", ")))
        }
        V::Obj(_) => {
            if it.find_method(v, "inspect").is_some() {
                let r = it.call(v, "inspect", Args::default())?;
                return Ok(to_s_plain(&r));
            }
            if it.is_a(v, "Exception") {
                let m = it.ivar_get(v, "@message");
                return Ok(format!(
                    "#<{}: {}>",
                    it.class_of(v).display_name(),
                    to_s_plain(&m)
                ));
            }
            Ok(inspect_plain(v))
        }
        V::Mod(_) => {
            if it.find_method(v, "inspect").is_some() {
                let r = it.call(v, "inspect", Args::default())?;
                return Ok(to_s_plain(&r));
            }
            Ok(inspect_plain(v))
        }
        other => Ok(inspect_plain(other)),
    }
}

pub fn new_hash_from(entries: Vec<(V, V)>) -> V {
    let h = Rc::new(RHash::default());
    for (k, v) in entries {
        h.insert(k, v);
    }
    V::Hash(h)
}

pub fn _im(_: IndexMap<HKey, (V, V)>) {}
