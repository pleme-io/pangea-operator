use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use indexmap::IndexMap;
use lib_ruby_parser::{Node, Parser, ParserOptions};

use crate::value::*;

pub enum Ctrl {
    Raise(V),
    Return(V, usize),
    Break(V, usize),
    Next(V),
    Throw(V, V),
    Retry,
    StopIter(V),
}

pub type R = Result<V, Ctrl>;

#[derive(Default, Clone)]
pub struct Args {
    pub pos: Vec<V>,
    pub kw: Option<Rc<RHash>>,
    pub blk: Option<Rc<Proc>>,
}

impl Args {
    pub fn new(pos: Vec<V>) -> Args {
        Args {
            pos,
            kw: None,
            blk: None,
        }
    }
    pub fn with_blk(pos: Vec<V>, blk: Option<Rc<Proc>>) -> Args {
        Args { pos, kw: None, blk }
    }
    pub fn flat(&self) -> Vec<V> {
        let mut v = self.pos.clone();
        if let Some(k) = &self.kw {
            v.push(V::Hash(k.clone()));
        }
        v
    }
}

pub struct Interp {
    pub next_id: Cell<usize>,
    pub classes: RefCell<HashMap<&'static str, Rc<Module>>>,
    pub object: Rc<Module>,
    pub main: V,
    pub load_path: RefCell<Vec<PathBuf>>,
    pub loaded: RefCell<HashSet<PathBuf>>,
    pub env: RefCell<IndexMap<String, String>>,
    pub gvars: RefCell<HashMap<String, V>>,
    pub last_match: RefCell<V>,
    pub stub_features: RefCell<HashSet<String>>,
    pub top_frame: RefCell<Option<Rc<Frame>>>,
    pub depth: Cell<usize>,
    pub virtual_files: RefCell<HashMap<PathBuf, String>>,
    pub sources: RefCell<HashMap<String, Rc<str>>>,
    pub cur_file: RefCell<Rc<str>>,
    pub cur_pos: Cell<usize>,
    pub autoloads: RefCell<HashMap<(usize, String), String>>,
    pub stack: RefCell<Vec<(String, Rc<str>, usize)>>,
    pub cwd: RefCell<PathBuf>,
    pub chain_cache: RefCell<HashMap<(u8, usize), (u64, Rc<Vec<Rc<Module>>>)>>,
    pub method_cache: RefCell<HashMap<(u8, usize, String), (u64, Option<(Meth, Rc<Module>)>)>>,
}

pub fn leak_parse(src: &str, file: &str) -> Result<Option<&'static Node>, String> {
    let opts = ParserOptions {
        buffer_name: file.to_string(),
        ..Default::default()
    };
    let res = Parser::new(src.as_bytes().to_vec(), opts).do_parse();
    for d in &res.diagnostics {
        if d.is_error() {
            return Err(format!("{file}: {}", d.render_message()));
        }
    }
    Ok(res.ast.map(|b| &*Box::leak(b)))
}

pub fn bytes_str(b: &lib_ruby_parser::Bytes) -> String {
    String::from_utf8_lossy(&b.raw).into_owned()
}

impl Interp {
    pub fn new() -> Rc<Interp> {
        let basic = new_module(Some("BasicObject"), true, None, 1);
        let object = new_module(Some("Object"), true, Some(basic.clone()), 2);
        let main = V::Obj(Rc::new(Obj {
            class: RefCell::new(object.clone()),
            ivars: RefCell::new(IndexMap::new()),
            singleton: RefCell::new(None),
            frozen: Cell::new(false),
        }));
        let it = Rc::new(Interp {
            next_id: Cell::new(100),
            classes: RefCell::new(HashMap::new()),
            object: object.clone(),
            main,
            load_path: RefCell::new(vec![]),
            loaded: RefCell::new(HashSet::new()),
            env: RefCell::new(IndexMap::new()),
            gvars: RefCell::new(HashMap::new()),
            last_match: RefCell::new(V::Nil),
            stub_features: RefCell::new(HashSet::new()),
            top_frame: RefCell::new(None),
            depth: Cell::new(0),
            virtual_files: RefCell::new(HashMap::new()),
            sources: RefCell::new(HashMap::new()),
            cur_file: RefCell::new(Rc::from("main")),
            cur_pos: Cell::new(0),
            autoloads: RefCell::new(HashMap::new()),
            stack: RefCell::new(vec![]),
            cwd: RefCell::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"))),
            chain_cache: RefCell::new(HashMap::new()),
            method_cache: RefCell::new(HashMap::new()),
        });
        it.classes.borrow_mut().insert("BasicObject", basic.clone());
        it.classes.borrow_mut().insert("Object", object.clone());
        object
            .consts
            .borrow_mut()
            .insert("BasicObject".into(), V::Mod(basic));
        object
            .consts
            .borrow_mut()
            .insert("Object".into(), V::Mod(object.clone()));
        let tf = Rc::new(Frame {
            self_v: it.main.clone(),
            def_target: object.clone(),
            cref: Rc::new(vec![object.clone()]),
            block: None,
            method: None,
            args: vec![],
            id: 0,
            file: Rc::from("main"),
        });
        *it.top_frame.borrow_mut() = Some(tf);
        crate::core::bootstrap(&it);
        it
    }

    pub fn id(&self) -> usize {
        let i = self.next_id.get();
        self.next_id.set(i + 1);
        i
    }

    pub fn cls(&self, n: &str) -> Rc<Module> {
        if let Some(c) = self.classes.borrow().get(n) {
            return c.clone();
        }
        match self.object.consts.borrow().get(n) {
            Some(V::Mod(m)) => m.clone(),
            _ => panic!("missing builtin class {n}"),
        }
    }

    pub fn define_class(
        &self,
        name: &'static str,
        sup: Option<Rc<Module>>,
        under: &Rc<Module>,
    ) -> Rc<Module> {
        let full = if Rc::ptr_eq(under, &self.object) {
            name.to_string()
        } else {
            format!("{}::{}", under.display_name(), name)
        };
        let m = new_module(
            Some(&full),
            true,
            sup.or_else(|| Some(self.object.clone())),
            self.id(),
        );
        *m.lexical_parent.borrow_mut() = Some(under.clone());
        under
            .consts
            .borrow_mut()
            .insert(name.to_string(), V::Mod(m.clone()));
        if Rc::ptr_eq(under, &self.object) {
            self.classes.borrow_mut().insert(name, m.clone());
        }
        m
    }

    pub fn define_module(&self, name: &'static str, under: &Rc<Module>) -> Rc<Module> {
        let full = if Rc::ptr_eq(under, &self.object) {
            name.to_string()
        } else {
            format!("{}::{}", under.display_name(), name)
        };
        let m = new_module(Some(&full), false, None, self.id());
        *m.lexical_parent.borrow_mut() = Some(under.clone());
        under
            .consts
            .borrow_mut()
            .insert(name.to_string(), V::Mod(m.clone()));
        if Rc::ptr_eq(under, &self.object) {
            self.classes.borrow_mut().insert(name, m.clone());
        }
        m
    }

    pub fn new_obj(&self, class: Rc<Module>) -> V {
        V::Obj(Rc::new(Obj {
            class: RefCell::new(class),
            ivars: RefCell::new(IndexMap::new()),
            singleton: RefCell::new(None),
            frozen: Cell::new(false),
        }))
    }

    pub fn make_exc(&self, class: &str, msg: &str) -> V {
        let c = self
            .lookup_const_path(class)
            .unwrap_or_else(|| self.cls("RuntimeError"));
        let o = self.new_obj(c);
        if let V::Obj(ob) = &o {
            ob.ivars.borrow_mut().insert("@message".into(), V::str(msg));
        }
        self.stamp(&o);
        o
    }

    pub fn stamp(&self, o: &V) {
        if let V::Obj(ob) = o {
            if ob.ivars.borrow().contains_key("@__lava_bt") {
                return;
            }
            let mut lines = vec![self.where_now()];
            for (name, file, pos) in self.stack.borrow().iter().rev().take(14) {
                let line = self
                    .sources
                    .borrow()
                    .get(&**file)
                    .map(|s| s[..(*pos).min(s.len())].matches('\n').count() + 1)
                    .unwrap_or(0);
                lines.push(format!("{name} called at {file}:{line}"));
            }
            ob.ivars
                .borrow_mut()
                .insert("@__lava_bt".into(), V::str(lines.join("\n  ")));
        }
    }

    pub fn where_now(&self) -> String {
        let f = self.cur_file.borrow().clone();
        let line = self
            .sources
            .borrow()
            .get(&*f)
            .map(|s| s[..self.cur_pos.get().min(s.len())].matches('\n').count() + 1)
            .unwrap_or(0);
        format!("{f}:{line}")
    }

    pub fn err(&self, class: &str, msg: impl AsRef<str>) -> Ctrl {
        Ctrl::Raise(self.make_exc(class, msg.as_ref()))
    }

    pub fn lookup_const_path(&self, path: &str) -> Option<Rc<Module>> {
        let mut cur = self.object.clone();
        for part in path.split("::") {
            let v = cur.consts.borrow().get(part).cloned();
            match v {
                Some(V::Mod(m)) => cur = m,
                _ => return None,
            }
        }
        Some(cur)
    }

    pub fn singleton_of(&self, v: &V) -> Result<Rc<Module>, Ctrl> {
        match v {
            V::Obj(o) => {
                if let Some(s) = o.singleton.borrow().as_ref() {
                    return Ok(s.clone());
                }
                let s = new_module(None, true, Some(o.class.borrow().clone()), self.id());
                *s.attached.borrow_mut() = Some(v.clone());
                *o.singleton_mut() = Some(s.clone());
                Ok(s)
            }
            V::Mod(m) => Ok(self.meta_of(m)),
            _ => {
                if let V::Str(_) | V::Arr(_) | V::Hash(_) = v {
                    return Err(self.err("TypeError", "singleton on core value unsupported"));
                }
                Err(self.err("TypeError", "can't define singleton"))
            }
        }
    }

    pub fn meta_of(&self, m: &Rc<Module>) -> Rc<Module> {
        if let Some(s) = m.meta.borrow().as_ref() {
            return s.clone();
        }
        let s = new_module(None, true, None, self.id());
        *s.attached.borrow_mut() = Some(V::Mod(m.clone()));
        *m.meta_mut() = Some(s.clone());
        s
    }

    pub fn class_of(&self, v: &V) -> Rc<Module> {
        match v {
            V::Nil => self.cls("NilClass"),
            V::Bool(true) => self.cls("TrueClass"),
            V::Bool(false) => self.cls("FalseClass"),
            V::Int(_) => self.cls("Integer"),
            V::Float(_) => self.cls("Float"),
            V::Str(_) => self.cls("String"),
            V::Sym(_) => self.cls("Symbol"),
            V::Arr(_) => self.cls("Array"),
            V::Hash(_) => self.cls("Hash"),
            V::Range(_) => self.cls("Range"),
            V::Proc(_) => self.cls("Proc"),
            V::Regex(_) => self.cls("Regexp"),
            V::Method(_) => self.cls("Method"),
            V::Obj(o) => o.class.borrow().clone(),
            V::Mod(m) => {
                if m.is_class {
                    self.cls("Class")
                } else {
                    self.cls("Module")
                }
            }
        }
    }

    pub fn ancestors(&self, m: &Rc<Module>) -> Vec<Rc<Module>> {
        let mut out: Vec<Rc<Module>> = vec![];
        let mut cur = Some(m.clone());
        while let Some(c) = cur {
            for p in c.prepends.borrow().iter().rev() {
                self.push_module_with_includes(p, &mut out);
            }
            out.push(c.clone());
            for inc in c.includes.borrow().iter().rev() {
                self.push_module_with_includes(inc, &mut out);
            }
            cur = c.superclass.borrow().clone();
        }
        let mut seen = HashSet::new();
        let mut dedup = vec![];
        for m in out.into_iter().rev() {
            if seen.insert(m.id) {
                dedup.push(m);
            }
        }
        dedup.reverse();
        dedup
    }

    fn push_module_with_includes(&self, m: &Rc<Module>, out: &mut Vec<Rc<Module>>) {
        out.push(m.clone());
        for inc in m.includes.borrow().iter().rev() {
            self.push_module_with_includes(inc, out);
        }
    }

    fn chain_key(&self, v: &V) -> (u8, usize) {
        match v {
            V::Obj(o) => match o.singleton.borrow().as_ref() {
                Some(s) => (1, s.id),
                None => (0, o.class.borrow().id),
            },
            V::Mod(m) => (2, m.id),
            other => (0, self.class_of(other).id),
        }
    }

    pub fn lookup_chain(&self, v: &V) -> Vec<Rc<Module>> {
        (*self.lookup_chain_rc(v)).clone()
    }

    pub fn lookup_chain_rc(&self, v: &V) -> Rc<Vec<Rc<Module>>> {
        let key = self.chain_key(v);
        let ep = epoch();
        if let Some((e, c)) = self.chain_cache.borrow().get(&key) {
            if *e == ep {
                return c.clone();
            }
        }
        let c = Rc::new(self.lookup_chain_uncached(v));
        self.chain_cache.borrow_mut().insert(key, (ep, c.clone()));
        c
    }

    fn lookup_chain_uncached(&self, v: &V) -> Vec<Rc<Module>> {
        match v {
            V::Obj(o) => {
                if let Some(s) = o.singleton.borrow().as_ref() {
                    let mut c = vec![];
                    for inc in s.includes.borrow().iter().rev() {
                        c.push(inc.clone());
                        c.extend(self.ancestors(inc).into_iter().skip(1));
                    }
                    let mut out = vec![s.clone()];
                    out.extend(c);
                    out.extend(self.ancestors(&o.class.borrow()));
                    return out;
                }
                self.ancestors(&o.class.borrow())
            }
            V::Mod(m) => {
                let mut out = vec![];
                let mut cur = Some(m.clone());
                while let Some(c) = cur {
                    if let Some(meta) = c.meta.borrow().as_ref() {
                        out.push(meta.clone());
                        for inc in meta.includes.borrow().iter().rev() {
                            out.extend(self.ancestors(inc));
                        }
                    }
                    if !c.is_class {
                        break;
                    }
                    cur = c.superclass.borrow().clone();
                }
                let k = if m.is_class {
                    self.cls("Class")
                } else {
                    self.cls("Module")
                };
                out.extend(self.ancestors(&k));
                out
            }
            other => self.ancestors(&self.class_of(other)),
        }
    }

    pub fn find_method(&self, v: &V, name: &str) -> Option<(Meth, Rc<Module>)> {
        let (t, id) = self.chain_key(v);
        let ep = epoch();
        let key = (t, id, name.to_string());
        if let Some((e, r)) = self.method_cache.borrow().get(&key) {
            if *e == ep {
                return r.clone();
            }
        }
        let mut found = None;
        for m in self.lookup_chain_rc(v).iter() {
            if let Some(me) = m.methods.borrow().get(name) {
                if !matches!(me, Meth::Undefined) {
                    found = Some((me.clone(), m.clone()));
                }
                break;
            }
        }
        self.method_cache
            .borrow_mut()
            .insert(key, (ep, found.clone()));
        found
    }

    pub fn find_super(&self, v: &V, owner: &Rc<Module>, name: &str) -> Option<(Meth, Rc<Module>)> {
        let chain = self.lookup_chain(v);
        let mut past = false;
        for m in chain {
            if past {
                if let Some(me) = m.methods.borrow().get(name) {
                    return Some((me.clone(), m.clone()));
                }
            } else if m.id == owner.id {
                past = true;
            }
        }
        None
    }

    pub fn responds(&self, v: &V, name: &str) -> bool {
        self.find_method(v, name).is_some() || crate::core::native_responds(self, v, name)
    }

    pub fn load_feature(self: &Rc<Self>, feature: &str, relative_to: Option<&Path>) -> R {
        if self.stub_features.borrow().contains(feature) {
            return Ok(V::Bool(false));
        }
        let path = self.resolve_feature(feature, relative_to);
        let Some(path) = path else {
            if crate::stdlib::is_builtin_feature(feature) {
                self.stub_features.borrow_mut().insert(feature.to_string());
                return Ok(V::Bool(true));
            }
            return Err(self.err("LoadError", format!("cannot load such file -- {feature}")));
        };
        if self.loaded.borrow().contains(&path) {
            return Ok(V::Bool(false));
        }
        self.loaded.borrow_mut().insert(path.clone());
        if std::env::var_os("LAVA_RUBY_TRACE_REQUIRE").is_some() {
            eprintln!("[require] {}", path.display());
        }
        self.load_file(&path)?;
        Ok(V::Bool(true))
    }

    fn resolve_feature(&self, feature: &str, relative_to: Option<&Path>) -> Option<PathBuf> {
        let with_ext = |p: PathBuf| -> Option<PathBuf> {
            let cand = if p.extension().is_some_and(|e| e == "rb") {
                p.clone()
            } else {
                PathBuf::from(format!("{}.rb", p.display()))
            };
            if cand.is_file() || self.virtual_files.borrow().contains_key(&cand) {
                return Some(normalize(&cand));
            }
            None
        };
        if let Some(base) = relative_to {
            return with_ext(base.join(feature));
        }
        if feature.starts_with('/') {
            return with_ext(PathBuf::from(feature));
        }
        for lp in self.load_path.borrow().iter() {
            if let Some(p) = with_ext(lp.join(feature)) {
                return Some(p);
            }
        }
        None
    }

    pub fn abs(&self, p: &str) -> PathBuf {
        let pb = PathBuf::from(p);
        if pb.is_absolute() {
            pb
        } else {
            normalize(&self.cwd.borrow().join(pb))
        }
    }

    pub fn read_file(&self, path: &Path) -> Option<String> {
        if let Some(s) = self.virtual_files.borrow().get(path) {
            return Some(s.clone());
        }
        std::fs::read_to_string(path).ok()
    }

    pub fn load_file(self: &Rc<Self>, path: &Path) -> R {
        let src = self
            .read_file(path)
            .ok_or_else(|| self.err("LoadError", format!("cannot read {}", path.display())))?;
        let file = path.display().to_string();
        self.sources
            .borrow_mut()
            .insert(file.clone(), Rc::from(src.as_str()));
        let ast = leak_parse(&src, &file).map_err(|e| self.err("SyntaxError", e))?;
        let Some(ast) = ast else { return Ok(V::Nil) };
        let top = self.top_frame.borrow().clone().unwrap();
        let fr = Rc::new(Frame {
            self_v: self.main.clone(),
            def_target: self.object.clone(),
            cref: Rc::new(vec![self.object.clone()]),
            block: None,
            method: None,
            args: vec![],
            id: self.id(),
            file: Rc::from(file.as_str()),
        });
        let _ = top;
        let scope = Scope::new(None);
        match self.eval(ast, &fr, &scope) {
            Ok(v) => Ok(v),
            Err(Ctrl::Return(v, _)) => Ok(v),
            Err(e) => Err(e),
        }
    }

    pub fn eval_string(
        self: &Rc<Self>,
        src: &str,
        file: &str,
        fr: &Rc<Frame>,
        scope: &Rc<Scope>,
    ) -> R {
        self.sources
            .borrow_mut()
            .insert(file.to_string(), Rc::from(src));
        let ast = leak_parse(src, file).map_err(|e| self.err("SyntaxError", e))?;
        let Some(ast) = ast else { return Ok(V::Nil) };
        let fr2 = Rc::new(Frame {
            self_v: fr.self_v.clone(),
            def_target: fr.def_target.clone(),
            cref: fr.cref.clone(),
            block: fr.block.clone(),
            method: fr.method.clone(),
            args: fr.args.clone(),
            id: fr.id,
            file: Rc::from(file),
        });
        self.eval(ast, &fr2, scope)
    }
}

pub fn normalize(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}
