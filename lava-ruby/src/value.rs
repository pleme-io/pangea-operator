use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering as AOrd};

pub static EPOCH: AtomicU64 = AtomicU64::new(1);

pub fn epoch() -> u64 {
    EPOCH.load(AOrd::Relaxed)
}

fn bump() {
    EPOCH.fetch_add(1, AOrd::Relaxed);
}

use indexmap::IndexMap;
use lib_ruby_parser::Node;

pub type Sym = Rc<str>;

#[derive(Clone)]
pub enum V {
    Nil,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(Rc<RStr>),
    Sym(Sym),
    Arr(Rc<RefCell<Vec<V>>>),
    Hash(Rc<RHash>),
    Range(Rc<(V, V, bool)>),
    Proc(Rc<Proc>),
    Obj(Rc<Obj>),
    Mod(Rc<Module>),
    Regex(Rc<RRegex>),
    Method(Rc<MethodObj>),
}

pub struct RStr {
    pub s: RefCell<String>,
    pub frozen: Cell<bool>,
}

pub struct RRegex {
    pub src: String,
    pub re: fancy_regex::Regex,
}

pub struct MethodObj {
    pub recv: V,
    pub name: String,
    pub unbound_owner: Option<Rc<Module>>,
}

#[derive(Default)]
pub struct RHash {
    pub map: RefCell<IndexMap<HKey, (V, V)>>,
    pub default: RefCell<V>,
    pub default_proc: RefCell<Option<Rc<Proc>>>,
    pub frozen: Cell<bool>,
    pub compare_by_identity: Cell<bool>,
}

impl Default for V {
    fn default() -> Self {
        V::Nil
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum HKey {
    Nil,
    Bool(bool),
    Int(i64),
    Float(u64),
    Str(String),
    Sym(String),
    Arr(Vec<HKey>),
    Ptr(usize),
}

pub struct Obj {
    pub class: RefCell<Rc<Module>>,
    pub ivars: RefCell<IndexMap<String, V>>,
    pub singleton: RefCell<Option<Rc<Module>>>,
    pub frozen: Cell<bool>,
}

pub struct Module {
    pub name: RefCell<Option<String>>,
    pub is_class: bool,
    pub superclass: RefCell<Option<Rc<Module>>>,
    pub methods: RefCell<HashMap<String, Meth>>,
    pub consts: RefCell<IndexMap<String, V>>,
    pub includes: RefCell<Vec<Rc<Module>>>,
    pub prepends: RefCell<Vec<Rc<Module>>>,
    pub meta: RefCell<Option<Rc<Module>>>,
    pub ivars: RefCell<IndexMap<String, V>>,
    pub cvars: RefCell<IndexMap<String, V>>,
    pub lexical_parent: RefCell<Option<Rc<Module>>>,
    pub attached: RefCell<Option<V>>,
    pub id: usize,
    pub module_function: Cell<bool>,
    pub default_private: Cell<bool>,
}

#[derive(Clone)]
pub enum Meth {
    User(Rc<UserMethod>),
    Proc(Rc<Proc>, Rc<Module>),
    Undefined,
    Native(&'static str),
}

pub struct UserMethod {
    pub name: String,
    pub args: Option<&'static Node>,
    pub body: Option<&'static Node>,
    pub owner: Rc<Module>,
    pub cref: Rc<Vec<Rc<Module>>>,
    pub file: Rc<str>,
    pub private: bool,
}

pub struct Scope {
    pub vars: RefCell<HashMap<String, V>>,
    pub parent: Option<Rc<Scope>>,
}

impl Scope {
    pub fn new(parent: Option<Rc<Scope>>) -> Rc<Scope> {
        Rc::new(Scope {
            vars: RefCell::new(HashMap::new()),
            parent,
        })
    }
    pub fn get(&self, n: &str) -> Option<V> {
        if let Some(v) = self.vars.borrow().get(n) {
            return Some(v.clone());
        }
        self.parent.as_ref().and_then(|p| p.get(n))
    }
    pub fn has(&self, n: &str) -> bool {
        self.vars.borrow().contains_key(n) || self.parent.as_ref().is_some_and(|p| p.has(n))
    }
    pub fn set(&self, n: &str, v: V) {
        if self.vars.borrow().contains_key(n) {
            self.vars.borrow_mut().insert(n.to_string(), v);
            return;
        }
        let mut p = self.parent.clone();
        while let Some(s) = p {
            if s.vars.borrow().contains_key(n) {
                s.vars.borrow_mut().insert(n.to_string(), v);
                return;
            }
            p = s.parent.clone();
        }
        self.vars.borrow_mut().insert(n.to_string(), v);
    }
    pub fn declare(&self, n: &str, v: V) {
        self.vars.borrow_mut().insert(n.to_string(), v);
    }
}

pub struct Frame {
    pub self_v: V,
    pub def_target: Rc<Module>,
    pub cref: Rc<Vec<Rc<Module>>>,
    pub block: Option<Rc<Proc>>,
    pub method: Option<(Rc<Module>, String)>,
    pub args: Vec<V>,
    pub id: usize,
    pub file: Rc<str>,
}

pub enum ProcBody {
    Ast {
        args: Option<&'static Node>,
        body: Option<&'static Node>,
        numargs: u8,
    },
    SymToProc(String),
    Method(Rc<MethodObj>),
}

pub struct Proc {
    pub body: ProcBody,
    pub scope: Rc<Scope>,
    pub frame: Rc<Frame>,
    pub lambda: bool,
    pub id: usize,
}

impl Obj {
    pub fn singleton_mut(&self) -> std::cell::RefMut<'_, Option<Rc<Module>>> {
        bump();
        self.singleton.borrow_mut()
    }
}

impl V {
    pub fn str(s: impl Into<String>) -> V {
        V::Str(Rc::new(RStr {
            s: RefCell::new(s.into()),
            frozen: Cell::new(false),
        }))
    }
    pub fn sym(s: &str) -> V {
        V::Sym(Rc::from(s))
    }
    pub fn arr(v: Vec<V>) -> V {
        V::Arr(Rc::new(RefCell::new(v)))
    }
    pub fn hash(m: IndexMap<HKey, (V, V)>) -> V {
        V::Hash(Rc::new(RHash {
            map: RefCell::new(m),
            ..Default::default()
        }))
    }
    pub fn empty_hash() -> V {
        V::hash(IndexMap::new())
    }
    pub fn truthy(&self) -> bool {
        !matches!(self, V::Nil | V::Bool(false))
    }
    pub fn is_nil(&self) -> bool {
        matches!(self, V::Nil)
    }
    pub fn as_str(&self) -> Option<String> {
        match self {
            V::Str(s) => Some(s.s.borrow().clone()),
            _ => None,
        }
    }
    pub fn ptr(&self) -> usize {
        match self {
            V::Str(r) => Rc::as_ptr(r) as *const u8 as usize,
            V::Arr(r) => Rc::as_ptr(r) as *const u8 as usize,
            V::Hash(r) => Rc::as_ptr(r) as *const u8 as usize,
            V::Range(r) => Rc::as_ptr(r) as *const u8 as usize,
            V::Proc(r) => Rc::as_ptr(r) as *const u8 as usize,
            V::Obj(r) => Rc::as_ptr(r) as *const u8 as usize,
            V::Mod(r) => Rc::as_ptr(r) as *const u8 as usize,
            V::Regex(r) => Rc::as_ptr(r) as *const u8 as usize,
            V::Method(r) => Rc::as_ptr(r) as *const u8 as usize,
            V::Nil => 8,
            V::Bool(true) => 20,
            V::Bool(false) => 0,
            V::Int(i) => (*i as usize).wrapping_mul(2).wrapping_add(1),
            V::Float(f) => f.to_bits() as usize,
            V::Sym(s) => {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                s.hash(&mut h);
                h.finish() as usize
            }
        }
    }
}

pub fn hkey(v: &V) -> HKey {
    match v {
        V::Nil => HKey::Nil,
        V::Bool(b) => HKey::Bool(*b),
        V::Int(i) => HKey::Int(*i),
        V::Float(f) => HKey::Float(f.to_bits()),
        V::Str(s) => HKey::Str(s.s.borrow().clone()),
        V::Sym(s) => HKey::Sym(s.to_string()),
        V::Arr(a) => HKey::Arr(a.borrow().iter().map(hkey).collect()),
        other => HKey::Ptr(other.ptr()),
    }
}

impl RHash {
    pub fn get(&self, k: &V) -> Option<V> {
        self.map.borrow().get(&hkey(k)).map(|(_, v)| v.clone())
    }
    pub fn insert(&self, k: V, v: V) {
        let k = match &k {
            V::Str(s) if !s.frozen.get() => {
                let c = V::str(s.s.borrow().clone());
                if let V::Str(r) = &c {
                    r.frozen.set(true);
                }
                c
            }
            _ => k,
        };
        let hk = hkey(&k);
        let mut m = self.map.borrow_mut();
        if let Some(slot) = m.get_mut(&hk) {
            slot.1 = v;
        } else {
            m.insert(hk, (k, v));
        }
    }
    pub fn entries(&self) -> Vec<(V, V)> {
        self.map.borrow().values().cloned().collect()
    }
    pub fn len(&self) -> usize {
        self.map.borrow().len()
    }
}

pub fn new_module(
    name: Option<&str>,
    is_class: bool,
    superclass: Option<Rc<Module>>,
    id: usize,
) -> Rc<Module> {
    Rc::new(Module {
        name: RefCell::new(name.map(str::to_string)),
        is_class,
        superclass: RefCell::new(superclass),
        methods: RefCell::new(HashMap::new()),
        consts: RefCell::new(IndexMap::new()),
        includes: RefCell::new(vec![]),
        prepends: RefCell::new(vec![]),
        meta: RefCell::new(None),
        ivars: RefCell::new(IndexMap::new()),
        cvars: RefCell::new(IndexMap::new()),
        lexical_parent: RefCell::new(None),
        attached: RefCell::new(None),
        id,
        module_function: Cell::new(false),
        default_private: Cell::new(false),
    })
}

impl Module {
    pub fn methods_mut(&self) -> std::cell::RefMut<'_, HashMap<String, Meth>> {
        bump();
        self.methods.borrow_mut()
    }
    pub fn includes_mut(&self) -> std::cell::RefMut<'_, Vec<Rc<Module>>> {
        bump();
        self.includes.borrow_mut()
    }
    pub fn prepends_mut(&self) -> std::cell::RefMut<'_, Vec<Rc<Module>>> {
        bump();
        self.prepends.borrow_mut()
    }
    pub fn meta_mut(&self) -> std::cell::RefMut<'_, Option<Rc<Module>>> {
        bump();
        self.meta.borrow_mut()
    }
    pub fn superclass_mut(&self) -> std::cell::RefMut<'_, Option<Rc<Module>>> {
        bump();
        self.superclass.borrow_mut()
    }
    pub fn display_name(&self) -> String {
        self.name
            .borrow()
            .clone()
            .unwrap_or_else(|| format!("#<Module:0x{:x}>", self.id))
    }
}
