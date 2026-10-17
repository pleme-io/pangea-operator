use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use indexmap::IndexMap;
use lib_ruby_parser::Node;

use crate::interp::*;
use crate::value::*;

impl Interp {
    pub fn eval_opt(
        self: &Rc<Self>,
        n: Option<&'static Node>,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> R {
        match n {
            Some(n) => self.eval(n, fr, sc),
            None => Ok(V::Nil),
        }
    }

    pub fn eval_seq(self: &Rc<Self>, ns: &'static [Node], fr: &Rc<Frame>, sc: &Rc<Scope>) -> R {
        let mut last = V::Nil;
        for n in ns {
            last = self.eval(n, fr, sc)?;
        }
        Ok(last)
    }

    pub fn eval(self: &Rc<Self>, n: &'static Node, fr: &Rc<Frame>, sc: &Rc<Scope>) -> R {
        match n {
            Node::Begin(b) => self.eval_seq(&b.statements, fr, sc),
            Node::KwBegin(b) => self.eval_seq(&b.statements, fr, sc),
            Node::Int(i) => {
                let s = i.value.replace('_', "");
                let v = if let Some(h) = s.strip_prefix("0x") {
                    i64::from_str_radix(h, 16)
                } else if let Some(b) = s.strip_prefix("0b") {
                    i64::from_str_radix(b, 2)
                } else if let Some(o) = s.strip_prefix("0o") {
                    i64::from_str_radix(o, 8)
                } else if s.len() > 1 && s.starts_with('0') && s.chars().all(|c| c.is_ascii_digit())
                {
                    i64::from_str_radix(&s[1..], 8)
                } else {
                    s.parse::<i64>()
                };
                v.map(V::Int)
                    .map_err(|_| self.err("RangeError", format!("integer literal {s}")))
            }
            Node::Float(f) => Ok(V::Float(f.value.replace('_', "").parse().unwrap_or(0.0))),
            Node::Str(s) => Ok(V::str(bytes_str(&s.value))),
            Node::Dstr(d) => Ok(V::str(self.interp_parts(&d.parts, fr, sc)?)),
            Node::Heredoc(d) => Ok(V::str(self.interp_parts(&d.parts, fr, sc)?)),
            Node::Sym(s) => Ok(V::sym(&bytes_str(&s.name))),
            Node::Dsym(d) => Ok(V::sym(&self.interp_parts(&d.parts, fr, sc)?)),
            Node::Xstr(_) | Node::XHeredoc(_) => Err(self.err(
                "NotImplementedError",
                "backticks are not available in lava-ruby",
            )),
            Node::Regexp(r) => {
                let src = self.interp_parts(&r.parts, fr, sc)?;
                let opts = match r.options.as_deref() {
                    Some(Node::RegOpt(o)) => o.options.clone().unwrap_or_default(),
                    _ => String::new(),
                };
                crate::core::make_regex(self, &src, &opts)
            }
            Node::True(_) => Ok(V::Bool(true)),
            Node::False(_) => Ok(V::Bool(false)),
            Node::Nil(_) => Ok(V::Nil),
            Node::Self_(_) => Ok(fr.self_v.clone()),
            Node::File(_) => Ok(V::str(fr.file.to_string())),
            Node::Line(_) => Ok(V::Int(0)),
            Node::Encoding(_) => Ok(V::str("UTF-8")),
            Node::Array(a) => {
                let mut out = vec![];
                for e in &a.elements {
                    self.eval_elem(e, fr, sc, &mut out)?;
                }
                Ok(V::arr(out))
            }
            Node::Hash(h) => self.eval_hash(&h.pairs, fr, sc),
            Node::Kwargs(h) => self.eval_hash(&h.pairs, fr, sc),
            Node::Irange(r) => Ok(V::Range(Rc::new((
                self.eval_opt(r.left.as_deref(), fr, sc)?,
                self.eval_opt(r.right.as_deref(), fr, sc)?,
                false,
            )))),
            Node::Erange(r) => Ok(V::Range(Rc::new((
                self.eval_opt(r.left.as_deref(), fr, sc)?,
                self.eval_opt(r.right.as_deref(), fr, sc)?,
                true,
            )))),
            Node::Lvar(l) => Ok(sc.get(&l.name).unwrap_or(V::Nil)),
            Node::Lvasgn(a) => {
                let v = self.eval_opt(a.value.as_deref(), fr, sc)?;
                sc.set(&a.name, v.clone());
                Ok(v)
            }
            Node::Ivar(i) => Ok(self.ivar_get(&fr.self_v, &i.name)),
            Node::Ivasgn(a) => {
                let v = self.eval_opt(a.value.as_deref(), fr, sc)?;
                self.ivar_set(&fr.self_v, &a.name, v.clone())?;
                Ok(v)
            }
            Node::Gvar(g) => Ok(self.gvar_get(&g.name)),
            Node::Gvasgn(a) => {
                let v = self.eval_opt(a.value.as_deref(), fr, sc)?;
                self.gvars.borrow_mut().insert(a.name.clone(), v.clone());
                Ok(v)
            }
            Node::Cvar(c) => {
                let m = self.cvar_holder(fr, &c.name);
                let v = m.cvars.borrow().get(&c.name).cloned();
                v.ok_or_else(|| {
                    self.err(
                        "NameError",
                        format!("uninitialized class variable {}", c.name),
                    )
                })
            }
            Node::Cvasgn(a) => {
                let v = self.eval_opt(a.value.as_deref(), fr, sc)?;
                let m = self.cvar_holder(fr, &a.name);
                m.cvars.borrow_mut().insert(a.name.clone(), v.clone());
                Ok(v)
            }
            Node::NthRef(r) => {
                let idx: usize = r.name.trim_start_matches('$').parse().unwrap_or(0);
                let lm = self.last_match.borrow().clone();
                if lm.is_nil() {
                    return Ok(V::Nil);
                }
                self.call(&lm, "[]", Args::new(vec![V::Int(idx as i64)]))
            }
            Node::BackRef(_) => Ok(self.last_match.borrow().clone()),
            Node::Const(c) => self.eval_const(c.scope.as_deref(), &c.name, fr, sc),
            Node::Cbase(_) => Ok(V::Mod(self.object.clone())),
            Node::Casgn(a) => {
                let v = self.eval_opt(a.value.as_deref(), fr, sc)?;
                let target = match a.scope.as_deref() {
                    Some(s) => match self.eval(s, fr, sc)? {
                        V::Mod(m) => m,
                        _ => return Err(self.err("TypeError", "not a class/module")),
                    },
                    None => fr.cref.last().unwrap().clone(),
                };
                self.const_set(&target, &a.name, v.clone());
                Ok(v)
            }
            Node::And(a) => {
                let l = self.eval(&a.lhs, fr, sc)?;
                if !l.truthy() {
                    return Ok(l);
                }
                self.eval(&a.rhs, fr, sc)
            }
            Node::Or(a) => {
                let l = self.eval(&a.lhs, fr, sc)?;
                if l.truthy() {
                    return Ok(l);
                }
                self.eval(&a.rhs, fr, sc)
            }
            Node::If(i) => {
                if self.eval(&i.cond, fr, sc)?.truthy() {
                    self.eval_opt(i.if_true.as_deref(), fr, sc)
                } else {
                    self.eval_opt(i.if_false.as_deref(), fr, sc)
                }
            }
            Node::IfMod(i) => {
                if self.eval(&i.cond, fr, sc)?.truthy() {
                    self.eval_opt(i.if_true.as_deref(), fr, sc)
                } else {
                    self.eval_opt(i.if_false.as_deref(), fr, sc)
                }
            }
            Node::IfTernary(i) => {
                if self.eval(&i.cond, fr, sc)?.truthy() {
                    self.eval(&i.if_true, fr, sc)
                } else {
                    self.eval(&i.if_false, fr, sc)
                }
            }
            Node::While(w) => {
                while self.eval(&w.cond, fr, sc)?.truthy() {
                    match self.eval_opt(w.body.as_deref(), fr, sc) {
                        Ok(_) | Err(Ctrl::Next(_)) => {}
                        Err(Ctrl::Break(v, 0)) => return Ok(v),
                        Err(e) => return Err(e),
                    }
                }
                Ok(V::Nil)
            }
            Node::Until(w) => {
                while !self.eval(&w.cond, fr, sc)?.truthy() {
                    match self.eval_opt(w.body.as_deref(), fr, sc) {
                        Ok(_) | Err(Ctrl::Next(_)) => {}
                        Err(Ctrl::Break(v, 0)) => return Ok(v),
                        Err(e) => return Err(e),
                    }
                }
                Ok(V::Nil)
            }
            Node::WhilePost(w) => {
                loop {
                    match self.eval(&w.body, fr, sc) {
                        Ok(_) | Err(Ctrl::Next(_)) => {}
                        Err(Ctrl::Break(v, 0)) => return Ok(v),
                        Err(e) => return Err(e),
                    }
                    if !self.eval(&w.cond, fr, sc)?.truthy() {
                        break;
                    }
                }
                Ok(V::Nil)
            }
            Node::UntilPost(w) => {
                loop {
                    match self.eval(&w.body, fr, sc) {
                        Ok(_) | Err(Ctrl::Next(_)) => {}
                        Err(Ctrl::Break(v, 0)) => return Ok(v),
                        Err(e) => return Err(e),
                    }
                    if self.eval(&w.cond, fr, sc)?.truthy() {
                        break;
                    }
                }
                Ok(V::Nil)
            }
            Node::For(f) => {
                let coll = self.eval(&f.iteratee, fr, sc)?;
                let items = crate::core::to_list(self, &coll)?;
                for it in items {
                    self.assign(&f.iterator, it, fr, sc)?;
                    match self.eval_opt(f.body.as_deref(), fr, sc) {
                        Ok(_) | Err(Ctrl::Next(_)) => {}
                        Err(Ctrl::Break(v, 0)) => return Ok(v),
                        Err(e) => return Err(e),
                    }
                }
                Ok(coll)
            }
            Node::Case(c) => {
                let subj = match c.expr.as_deref() {
                    Some(e) => Some(self.eval(e, fr, sc)?),
                    None => None,
                };
                for w in &c.when_bodies {
                    if let Node::When(w) = w {
                        for p in &w.patterns {
                            let hit = if let Node::Splat(s) = p {
                                let list = self.eval_opt(s.value.as_deref(), fr, sc)?;
                                let mut any = false;
                                for x in crate::core::to_list(self, &list)? {
                                    if self.case_eq(&x, subj.as_ref())? {
                                        any = true;
                                        break;
                                    }
                                }
                                any
                            } else {
                                let pv = self.eval(p, fr, sc)?;
                                self.case_eq(&pv, subj.as_ref())?
                            };
                            if hit {
                                return self.eval_opt(w.body.as_deref(), fr, sc);
                            }
                        }
                    }
                }
                self.eval_opt(c.else_body.as_deref(), fr, sc)
            }
            Node::Send(s) => {
                self.cur_pos.set(s.expression_l.begin);
                if !Rc::ptr_eq(&self.cur_file.borrow(), &fr.file) {
                    *self.cur_file.borrow_mut() = fr.file.clone();
                }
                self.eval_send(
                    s.recv.as_deref(),
                    &s.method_name,
                    &s.args,
                    None,
                    false,
                    fr,
                    sc,
                )
            }
            Node::CSend(s) => {
                self.eval_send(Some(&s.recv), &s.method_name, &s.args, None, true, fr, sc)
            }
            Node::Block(b) => {
                self.eval_block_call(&b.call, b.args.as_deref(), b.body.as_deref(), 0, fr, sc)
            }
            Node::Numblock(b) => {
                self.eval_block_call(&b.call, None, Some(&b.body), b.numargs, fr, sc)
            }
            Node::Lambda(_) => Err(self.err("SyntaxError", "bare lambda")),
            Node::Index(i) => {
                let recv = self.eval(&i.recv, fr, sc)?;
                let args = self.eval_args(&i.indexes, fr, sc)?;
                self.call(&recv, "[]", args)
            }
            Node::IndexAsgn(i) => {
                let recv = self.eval(&i.recv, fr, sc)?;
                let mut args = self.eval_args(&i.indexes, fr, sc)?;
                let v = self.eval_opt(i.value.as_deref(), fr, sc)?;
                args.pos.push(v.clone());
                self.call(&recv, "[]=", args)?;
                Ok(v)
            }
            Node::OpAsgn(o) => self.eval_op_asgn(&o.recv, &o.operator, &o.value, fr, sc),
            Node::OrAsgn(o) => self.eval_logic_asgn(&o.recv, &o.value, true, fr, sc),
            Node::AndAsgn(o) => self.eval_logic_asgn(&o.recv, &o.value, false, fr, sc),
            Node::Masgn(m) => {
                let rhs = self.eval(&m.rhs, fr, sc)?;
                let list = match &rhs {
                    V::Arr(a) => a.borrow().clone(),
                    other => {
                        if self.responds(other, "to_ary") {
                            crate::core::to_list(
                                self,
                                &self.call(other, "to_ary", Args::default())?,
                            )?
                        } else {
                            vec![other.clone()]
                        }
                    }
                };
                self.destructure(&m.lhs, list, fr, sc)?;
                Ok(rhs)
            }
            Node::Def(d) => {
                let um = Rc::new(UserMethod {
                    name: d.name.clone(),
                    args: d.args.as_deref(),
                    body: d.body.as_deref(),
                    owner: fr.def_target.clone(),
                    cref: fr.cref.clone(),
                    file: fr.file.clone(),
                    private: fr.def_target.default_private.get(),
                });
                let target = fr.def_target.clone();
                target
                    .methods_mut()
                    .insert(d.name.clone(), Meth::User(um.clone()));
                if target.module_function.get() {
                    self.meta_of(&target)
                        .methods_mut()
                        .insert(d.name.clone(), Meth::User(um));
                }
                self.method_added_hook(&target, &d.name)?;
                Ok(V::sym(&d.name))
            }
            Node::Defs(d) => {
                let recv = self.eval(&d.definee, fr, sc)?;
                let target = self.singleton_of(&recv)?;
                let um = Rc::new(UserMethod {
                    name: d.name.clone(),
                    args: d.args.as_deref(),
                    body: d.body.as_deref(),
                    owner: target.clone(),
                    cref: fr.cref.clone(),
                    file: fr.file.clone(),
                    private: false,
                });
                target.methods_mut().insert(d.name.clone(), Meth::User(um));
                Ok(V::sym(&d.name))
            }
            Node::Class(c) => {
                let (holder, name) = self.const_target(&c.name, fr, sc)?;
                let sup = match c.superclass.as_deref() {
                    Some(s) => match self.eval(s, fr, sc)? {
                        V::Mod(m) => Some(m),
                        _ => return Err(self.err("TypeError", "superclass must be a Class")),
                    },
                    None => None,
                };
                let existing = holder.consts.borrow().get(&name).cloned();
                let m = match existing {
                    Some(V::Mod(m)) => m,
                    _ => {
                        let full = self.qualified(&holder, &name);
                        let m = new_module(
                            Some(&full),
                            true,
                            Some(sup.clone().unwrap_or_else(|| self.object.clone())),
                            self.id(),
                        );
                        *m.lexical_parent.borrow_mut() = Some(holder.clone());
                        holder
                            .consts
                            .borrow_mut()
                            .insert(name.clone(), V::Mod(m.clone()));
                        if let Some(s) = &sup {
                            self.inherited_hook(s, &m)?;
                        }
                        m
                    }
                };
                self.eval_module_body(&m, c.body.as_deref(), fr)
            }
            Node::Module(c) => {
                let (holder, name) = self.const_target(&c.name, fr, sc)?;
                let existing = holder.consts.borrow().get(&name).cloned();
                let m = match existing {
                    Some(V::Mod(m)) => m,
                    _ => {
                        let full = self.qualified(&holder, &name);
                        let m = new_module(Some(&full), false, None, self.id());
                        *m.lexical_parent.borrow_mut() = Some(holder.clone());
                        holder
                            .consts
                            .borrow_mut()
                            .insert(name.clone(), V::Mod(m.clone()));
                        m
                    }
                };
                self.eval_module_body(&m, c.body.as_deref(), fr)
            }
            Node::SClass(s) => {
                let target = self.eval(&s.expr, fr, sc)?;
                let meta = self.singleton_of(&target)?;
                let nf = Rc::new(Frame {
                    self_v: V::Mod(meta.clone()),
                    def_target: meta,
                    cref: fr.cref.clone(),
                    block: None,
                    method: None,
                    args: vec![],
                    id: self.id(),
                    file: fr.file.clone(),
                });
                self.eval_opt(s.body.as_deref(), &nf, &Scope::new(None))
            }
            Node::Alias(a) => {
                let to = self.sym_name(&a.to, fr, sc)?;
                let from = self.sym_name(&a.from, fr, sc)?;
                self.alias_method(&fr.def_target, &to, &from)?;
                Ok(V::Nil)
            }
            Node::Undef(u) => {
                for nm in &u.names {
                    let n = self.sym_name(nm, fr, sc)?;
                    fr.def_target.methods_mut().insert(n, Meth::Undefined);
                }
                Ok(V::Nil)
            }
            Node::Return(r) => {
                let v = self.ret_value(&r.args, fr, sc)?;
                Err(Ctrl::Return(v, fr.id))
            }
            Node::Next(r) => Err(Ctrl::Next(self.ret_value(&r.args, fr, sc)?)),
            Node::Break(r) => Err(Ctrl::Break(self.ret_value(&r.args, fr, sc)?, 0)),
            Node::Retry(_) => Err(Ctrl::Retry),
            Node::Redo(_) => Err(self.err("NotImplementedError", "redo")),
            Node::Rescue(r) => self.eval_rescue(r, fr, sc),
            Node::Ensure(e) => {
                let res = self.eval_opt(e.body.as_deref(), fr, sc);
                self.eval_opt(e.ensure.as_deref(), fr, sc)?;
                res
            }
            Node::Yield(y) => {
                let args = self.eval_args(&y.args, fr, sc)?;
                let Some(b) = fr.block.clone() else {
                    return Err(self.err("LocalJumpError", "no block given (yield)"));
                };
                self.call_proc(&b, args.flat(), args.blk)
            }
            Node::Super(s) => {
                let args = self.eval_args(&s.args, fr, sc)?;
                self.call_super(fr, args)
            }
            Node::ZSuper(_) => {
                let args = Args::with_blk(fr.args.clone(), fr.block.clone());
                self.call_super(fr, args)
            }
            Node::Defined(d) => self.eval_defined(&d.value, fr, sc),
            Node::Splat(s) => self.eval_opt(s.value.as_deref(), fr, sc),
            Node::BlockPass(_) => Err(self.err("SyntaxError", "block pass outside call")),
            Node::MatchWithLvasgn(m) => {
                let re = self.eval(&m.re, fr, sc)?;
                let v = self.eval(&m.value, fr, sc)?;
                self.call(&re, "=~", Args::new(vec![v]))
            }
            Node::EmptyElse(_) => Ok(V::Nil),
            Node::Preexe(_) | Node::Postexe(_) => Ok(V::Nil),
            Node::CaseMatch(_) => Err(self.err(
                "NotImplementedError",
                "pattern matching (case/in) is not supported",
            )),
            other => Err(self.err(
                "NotImplementedError",
                format!("node {} not supported", node_name(other)),
            )),
        }
    }

    fn ret_value(self: &Rc<Self>, args: &'static [Node], fr: &Rc<Frame>, sc: &Rc<Scope>) -> R {
        match args.len() {
            0 => Ok(V::Nil),
            1 if !matches!(args[0], Node::Splat(_)) => self.eval(&args[0], fr, sc),
            _ => {
                let mut out = vec![];
                for a in args {
                    self.eval_elem(a, fr, sc, &mut out)?;
                }
                Ok(V::arr(out))
            }
        }
    }

    fn qualified(&self, holder: &Rc<Module>, name: &str) -> String {
        if Rc::ptr_eq(holder, &self.object) {
            name.to_string()
        } else {
            format!("{}::{}", holder.display_name(), name)
        }
    }

    fn const_target(
        self: &Rc<Self>,
        n: &'static Node,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> Result<(Rc<Module>, String), Ctrl> {
        match n {
            Node::Const(c) => {
                let holder = match c.scope.as_deref() {
                    Some(s) => match self.eval(s, fr, sc)? {
                        V::Mod(m) => m,
                        _ => return Err(self.err("TypeError", "not a module")),
                    },
                    None => fr.cref.last().unwrap().clone(),
                };
                Ok((holder, c.name.clone()))
            }
            _ => Err(self.err("TypeError", "bad class name")),
        }
    }

    pub fn eval_module_body(
        self: &Rc<Self>,
        m: &Rc<Module>,
        body: Option<&'static Node>,
        fr: &Rc<Frame>,
    ) -> R {
        let mut cref = (*fr.cref).clone();
        cref.push(m.clone());
        let nf = Rc::new(Frame {
            self_v: V::Mod(m.clone()),
            def_target: m.clone(),
            cref: Rc::new(cref),
            block: None,
            method: None,
            args: vec![],
            id: self.id(),
            file: fr.file.clone(),
        });
        let saved_mf = m.module_function.get();
        let saved_priv = m.default_private.get();
        let r = self.eval_opt(body, &nf, &Scope::new(None));
        m.module_function.set(saved_mf);
        m.default_private.set(saved_priv);
        r
    }

    fn sym_name(
        self: &Rc<Self>,
        n: &'static Node,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> Result<String, Ctrl> {
        match n {
            Node::Sym(s) => Ok(bytes_str(&s.name)),
            other => {
                let v = self.eval(other, fr, sc)?;
                Ok(crate::core::to_s_plain(&v))
            }
        }
    }

    pub fn alias_method(&self, target: &Rc<Module>, to: &str, from: &str) -> Result<(), Ctrl> {
        let probe = if let Some(V::Mod(_)) = target.attached.borrow().as_ref() {
            None
        } else {
            None::<()>
        };
        let _ = probe;
        let mut found = None;
        for m in self.ancestors(target) {
            if let Some(me) = m.methods.borrow().get(from) {
                found = Some(me.clone());
                break;
            }
        }
        let me = found.unwrap_or(Meth::Native(Box::leak(from.to_string().into_boxed_str())));
        target.methods_mut().insert(to.to_string(), me);
        Ok(())
    }

    pub fn interp_parts(
        self: &Rc<Self>,
        parts: &'static [Node],
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> Result<String, Ctrl> {
        let mut s = String::new();
        for p in parts {
            match p {
                Node::Str(x) => s.push_str(&bytes_str(&x.value)),
                Node::Begin(b) => {
                    let v = self.eval_seq(&b.statements, fr, sc)?;
                    s.push_str(&self.to_s(&v)?);
                }
                other => {
                    let v = self.eval(other, fr, sc)?;
                    s.push_str(&self.to_s(&v)?);
                }
            }
        }
        Ok(s)
    }

    pub fn to_s(self: &Rc<Self>, v: &V) -> Result<String, Ctrl> {
        match v {
            V::Str(s) => Ok(s.s.borrow().clone()),
            V::Obj(_) | V::Mod(_) => {
                if self.find_method(v, "to_s").is_some() {
                    let r = self.call(v, "to_s", Args::default())?;
                    return Ok(crate::core::to_s_plain(&r));
                }
                Ok(crate::core::to_s_plain(v))
            }
            other => Ok(crate::core::to_s_plain(other)),
        }
    }

    fn eval_elem(
        self: &Rc<Self>,
        e: &'static Node,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
        out: &mut Vec<V>,
    ) -> Result<(), Ctrl> {
        match e {
            Node::Splat(s) => {
                let v = self.eval_opt(s.value.as_deref(), fr, sc)?;
                out.extend(self.splat_list(&v)?);
            }
            Node::Kwargs(_) | Node::Hash(_) => out.push(self.eval(e, fr, sc)?),
            other => out.push(self.eval(other, fr, sc)?),
        }
        Ok(())
    }

    pub fn splat_list(self: &Rc<Self>, v: &V) -> Result<Vec<V>, Ctrl> {
        match v {
            V::Arr(a) => Ok(a.borrow().clone()),
            V::Nil => Ok(vec![]),
            V::Hash(h) => Ok(h
                .entries()
                .into_iter()
                .map(|(k, v)| V::arr(vec![k, v]))
                .collect()),
            V::Range(_) => crate::core::to_list(self, v),
            other => {
                if self.responds(other, "to_a") {
                    let r = self.call(other, "to_a", Args::default())?;
                    crate::core::to_list(self, &r)
                } else {
                    Ok(vec![other.clone()])
                }
            }
        }
    }

    fn eval_hash(self: &Rc<Self>, pairs: &'static [Node], fr: &Rc<Frame>, sc: &Rc<Scope>) -> R {
        let h = Rc::new(RHash::default());
        for p in pairs {
            match p {
                Node::Pair(pr) => {
                    let k = self.eval(&pr.key, fr, sc)?;
                    let v = self.eval(&pr.value, fr, sc)?;
                    h.insert(k, v);
                }
                Node::Kwsplat(k) => {
                    let v = self.eval(&k.value, fr, sc)?;
                    let v = if let V::Hash(_) = v {
                        v
                    } else if v.is_nil() {
                        V::empty_hash()
                    } else {
                        self.call(&v, "to_h", Args::default())?
                    };
                    if let V::Hash(src) = v {
                        for (k, v) in src.entries() {
                            h.insert(k, v);
                        }
                    }
                }
                other => {
                    return Err(
                        self.err("SyntaxError", format!("hash element {}", node_name(other)))
                    )
                }
            }
        }
        Ok(V::Hash(h))
    }

    pub fn eval_args(
        self: &Rc<Self>,
        nodes: &'static [Node],
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> Result<Args, Ctrl> {
        let mut a = Args::default();
        for n in nodes {
            match n {
                Node::Splat(s) => {
                    let v = self.eval_opt(s.value.as_deref(), fr, sc)?;
                    a.pos.extend(self.splat_list(&v)?);
                }
                Node::Kwargs(k) => {
                    if let V::Hash(h) = self.eval_hash(&k.pairs, fr, sc)? {
                        a.kw = Some(h);
                    }
                }
                Node::BlockPass(b) => {
                    let v = match b.value.as_deref() {
                        Some(x) => self.eval(x, fr, sc)?,
                        None => fr.block.clone().map(V::Proc).unwrap_or(V::Nil),
                    };
                    a.blk = self.to_proc(&v, fr)?;
                }
                Node::ForwardedArgs(_) => {
                    match sc.get(crate::call::FORWARDED_ARGS) {
                        Some(V::Arr(tail)) => a.pos.extend(tail.borrow().iter().cloned()),
                        _ => a.pos.extend(fr.args.clone()),
                    }
                    if let Some(V::Hash(k)) = sc.get(crate::call::FORWARDED_KWARGS) {
                        a.kw = Some(k);
                    }
                    a.blk = fr.block.clone();
                }
                other => a.pos.push(self.eval(other, fr, sc)?),
            }
        }
        if let Some(k) = &a.kw {
            if k.len() == 0 && nodes.iter().any(|n| matches!(n, Node::Kwargs(kk) if kk.pairs.iter().all(|p| matches!(p, Node::Kwsplat(_))))) {
                a.kw = None;
            }
        }
        Ok(a)
    }

    pub fn to_proc(self: &Rc<Self>, v: &V, fr: &Rc<Frame>) -> Result<Option<Rc<Proc>>, Ctrl> {
        match v {
            V::Nil => Ok(None),
            V::Proc(p) => Ok(Some(p.clone())),
            V::Sym(s) => Ok(Some(Rc::new(Proc {
                body: ProcBody::SymToProc(s.to_string()),
                scope: Scope::new(None),
                frame: fr.clone(),
                lambda: false,
                id: self.id(),
            }))),
            V::Method(m) => Ok(Some(Rc::new(Proc {
                body: ProcBody::Method(m.clone()),
                scope: Scope::new(None),
                frame: fr.clone(),
                lambda: true,
                id: self.id(),
            }))),
            other => {
                let r = self.call(other, "to_proc", Args::default())?;
                match r {
                    V::Proc(p) => Ok(Some(p)),
                    _ => Err(self.err("TypeError", "wrong argument type (expected Proc)")),
                }
            }
        }
    }

    fn eval_send(
        self: &Rc<Self>,
        recv: Option<&'static Node>,
        name: &str,
        argn: &'static [Node],
        blk: Option<Rc<Proc>>,
        safe: bool,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> R {
        if recv.is_none() {
            match name {
                "block_given?" => return Ok(V::Bool(fr.block.is_some())),
                "binding" => return Ok(V::Nil),
                "__method__" => {
                    return Ok(fr.method.as_ref().map(|m| V::sym(&m.1)).unwrap_or(V::Nil))
                }
                "__dir__" => {
                    let p = Path::new(&*fr.file);
                    return Ok(V::str(
                        p.parent()
                            .map(|x| x.display().to_string())
                            .unwrap_or_default(),
                    ));
                }
                "require_relative" => {
                    let a = self.eval_args(argn, fr, sc)?;
                    let f = crate::core::to_s_plain(&a.pos[0]);
                    let base = Path::new(&*fr.file)
                        .parent()
                        .map(Path::to_path_buf)
                        .unwrap_or_default();
                    return self.load_feature(&f, Some(&base));
                }
                "eval" | "instance_eval" if name == "eval" => {
                    let a = self.eval_args(argn, fr, sc)?;
                    let src = crate::core::to_s_plain(&a.pos[0]);
                    let file = a
                        .pos
                        .get(2)
                        .map(crate::core::to_s_plain)
                        .unwrap_or_else(|| "(eval)".into());
                    return self.eval_string(&src, &file, fr, sc);
                }
                "lambda" | "proc" if blk.is_some() => {
                    let p = blk.unwrap();
                    if name == "lambda" {
                        return Ok(V::Proc(Rc::new(Proc {
                            body: clone_body(&p.body),
                            scope: p.scope.clone(),
                            frame: p.frame.clone(),
                            lambda: true,
                            id: p.id,
                        })));
                    }
                    return Ok(V::Proc(p));
                }
                "loop" if blk.is_some() => {
                    let p = blk.unwrap();
                    loop {
                        match self.call_proc(&p, vec![], None) {
                            Ok(_) => {}
                            Err(Ctrl::Break(v, id)) if id == p.id => return Ok(v),
                            Err(Ctrl::StopIter(v)) => return Ok(v),
                            Err(Ctrl::Raise(e)) if self.is_a(&e, "StopIteration") => {
                                return Ok(self.ivar_get(&e, "@result"));
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                _ => {}
            }
        }
        let rv = match recv {
            Some(r) => {
                let v = self.eval(r, fr, sc)?;
                if safe && v.is_nil() {
                    return Ok(V::Nil);
                }
                Some(v)
            }
            None => None,
        };
        let mut args = self.eval_args(argn, fr, sc)?;
        if blk.is_some() {
            args.blk = blk;
        }
        if name.ends_with('=')
            && !matches!(name, "==" | "!=" | "<=" | ">=" | "===" | "[]=")
            && rv.is_some()
            && args.pos.len() == 1
        {
            let v = args.pos[0].clone();
            self.call_full(rv.as_ref().unwrap(), name, args, false, Some(fr))?;
            return Ok(v);
        }
        match rv {
            Some(v) => self.call_full(&v, name, args, false, Some(fr)),
            None => {
                let s = fr.self_v.clone();
                self.call_full(&s, name, args, true, Some(fr))
            }
        }
    }

    fn eval_block_call(
        self: &Rc<Self>,
        call: &'static Node,
        args: Option<&'static Node>,
        body: Option<&'static Node>,
        numargs: u8,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> R {
        let p = Rc::new(Proc {
            body: ProcBody::Ast {
                args,
                body,
                numargs,
            },
            scope: sc.clone(),
            frame: fr.clone(),
            lambda: matches!(call, Node::Lambda(_)),
            id: self.id(),
        });
        let pid = p.id;
        let res = match call {
            Node::Lambda(_) => return Ok(V::Proc(p)),
            Node::Send(s) => self.eval_send(
                s.recv.as_deref(),
                &s.method_name,
                &s.args,
                Some(p),
                false,
                fr,
                sc,
            ),
            Node::CSend(s) => self.eval_send(
                Some(&s.recv),
                &s.method_name,
                &s.args,
                Some(p),
                true,
                fr,
                sc,
            ),
            Node::Super(s) => {
                let mut a = self.eval_args(&s.args, fr, sc)?;
                a.blk = Some(p);
                self.call_super(fr, a)
            }
            Node::ZSuper(_) => self.call_super(fr, Args::with_blk(fr.args.clone(), Some(p))),
            other => return Err(self.err("SyntaxError", format!("block on {}", node_name(other)))),
        };
        match res {
            Err(Ctrl::Break(v, id)) if id == pid => Ok(v),
            other => other,
        }
    }

    fn eval_op_asgn(
        self: &Rc<Self>,
        target: &'static Node,
        op: &str,
        value: &'static Node,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> R {
        match target {
            Node::Index(_) | Node::IndexAsgn(_) => {
                let (rn, ixn): (&'static Node, &'static [Node]) = match target {
                    Node::Index(i) => (&i.recv, &i.indexes),
                    Node::IndexAsgn(i) => (&i.recv, &i.indexes),
                    _ => unreachable!(),
                };
                let recv = self.eval(rn, fr, sc)?;
                let idx = self.eval_args(ixn, fr, sc)?;
                let cur = self.call(&recv, "[]", idx.clone())?;
                let rhs = self.eval(value, fr, sc)?;
                let nv = self.call(&cur, op, Args::new(vec![rhs]))?;
                let mut a = idx;
                a.pos.push(nv.clone());
                self.call(&recv, "[]=", a)?;
                Ok(nv)
            }
            Node::Send(s) => {
                let recv = self.eval_opt(s.recv.as_deref(), fr, sc)?;
                let mname = s.method_name.trim_end_matches('=').to_string();
                let s = &lib_ruby_parser::nodes::Send {
                    method_name: mname,
                    ..s.clone()
                };
                let cur = self.call(&recv, &s.method_name, Args::default())?;
                let rhs = self.eval(value, fr, sc)?;
                let nv = self.call(&cur, op, Args::new(vec![rhs]))?;
                self.call(
                    &recv,
                    &format!("{}=", s.method_name),
                    Args::new(vec![nv.clone()]),
                )?;
                Ok(nv)
            }
            other => {
                let cur = self.read_target(other, fr, sc)?;
                let rhs = self.eval(value, fr, sc)?;
                let nv = self.call(&cur, op, Args::new(vec![rhs]))?;
                self.assign(other, nv.clone(), fr, sc)?;
                Ok(nv)
            }
        }
    }

    fn eval_logic_asgn(
        self: &Rc<Self>,
        target: &'static Node,
        value: &'static Node,
        or: bool,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> R {
        match target {
            Node::Index(_) | Node::IndexAsgn(_) => {
                let (rn, ixn): (&'static Node, &'static [Node]) = match target {
                    Node::Index(i) => (&i.recv, &i.indexes),
                    Node::IndexAsgn(i) => (&i.recv, &i.indexes),
                    _ => unreachable!(),
                };
                let recv = self.eval(rn, fr, sc)?;
                let idx = self.eval_args(ixn, fr, sc)?;
                let cur = self.call(&recv, "[]", idx.clone())?;
                if cur.truthy() == or {
                    return Ok(cur);
                }
                let nv = self.eval(value, fr, sc)?;
                let mut a = idx;
                a.pos.push(nv.clone());
                self.call(&recv, "[]=", a)?;
                Ok(nv)
            }
            Node::Send(s) => {
                let recv = self.eval_opt(s.recv.as_deref(), fr, sc)?;
                let mname = s.method_name.trim_end_matches('=').to_string();
                let s = &lib_ruby_parser::nodes::Send {
                    method_name: mname,
                    ..s.clone()
                };
                let cur = self.call(&recv, &s.method_name, Args::default())?;
                if cur.truthy() == or {
                    return Ok(cur);
                }
                let nv = self.eval(value, fr, sc)?;
                self.call(
                    &recv,
                    &format!("{}=", s.method_name),
                    Args::new(vec![nv.clone()]),
                )?;
                Ok(nv)
            }
            other => {
                let cur = match other {
                    Node::Casgn(c) => {
                        let holder = fr.cref.last().unwrap().clone();
                        let cv = holder.consts.borrow().get(&c.name).cloned();
                        cv.unwrap_or(V::Nil)
                    }
                    Node::Cvasgn(c) => {
                        let m = self.cvar_holder(fr, &c.name);
                        let cv = m.cvars.borrow().get(&c.name).cloned();
                        cv.unwrap_or(V::Nil)
                    }
                    _ => self.read_target(other, fr, sc)?,
                };
                if cur.truthy() == or {
                    return Ok(cur);
                }
                let nv = self.eval(value, fr, sc)?;
                self.assign(other, nv.clone(), fr, sc)?;
                Ok(nv)
            }
        }
    }

    fn read_target(self: &Rc<Self>, n: &'static Node, fr: &Rc<Frame>, sc: &Rc<Scope>) -> R {
        match n {
            Node::Lvasgn(a) => Ok(sc.get(&a.name).unwrap_or(V::Nil)),
            Node::Ivasgn(a) => Ok(self.ivar_get(&fr.self_v, &a.name)),
            Node::Gvasgn(a) => Ok(self.gvar_get(&a.name)),
            Node::Cvasgn(a) => {
                let m = self.cvar_holder(fr, &a.name);
                let v = m.cvars.borrow().get(&a.name).cloned();
                Ok(v.unwrap_or(V::Nil))
            }
            Node::Casgn(c) => self.eval_const(c.scope.as_deref(), &c.name, fr, sc),
            other => Err(self.err(
                "SyntaxError",
                format!("op-assign target {}", node_name(other)),
            )),
        }
    }

    pub fn assign(
        self: &Rc<Self>,
        n: &'static Node,
        v: V,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> Result<(), Ctrl> {
        match n {
            Node::Lvasgn(a) => sc.set(&a.name, v),
            Node::Ivasgn(a) => self.ivar_set(&fr.self_v, &a.name, v)?,
            Node::Gvasgn(a) => {
                self.gvars.borrow_mut().insert(a.name.clone(), v);
            }
            Node::Cvasgn(a) => {
                let m = self.cvar_holder(fr, &a.name);
                m.cvars.borrow_mut().insert(a.name.clone(), v);
            }
            Node::Casgn(c) => {
                let target = match c.scope.as_deref() {
                    Some(s) => match self.eval(s, fr, sc)? {
                        V::Mod(m) => m,
                        _ => return Err(self.err("TypeError", "not a module")),
                    },
                    None => fr.cref.last().unwrap().clone(),
                };
                self.const_set(&target, &c.name, v);
            }
            Node::IndexAsgn(i) => {
                let recv = self.eval(&i.recv, fr, sc)?;
                let mut a = self.eval_args(&i.indexes, fr, sc)?;
                a.pos.push(v);
                self.call(&recv, "[]=", a)?;
            }
            Node::Send(s) => {
                let recv = self.eval_opt(s.recv.as_deref(), fr, sc)?;
                let nm = if s.method_name.ends_with('=') {
                    s.method_name.clone()
                } else {
                    format!("{}=", s.method_name)
                };
                self.call(&recv, &nm, Args::new(vec![v]))?;
            }
            Node::Mlhs(_) => {
                let list = match &v {
                    V::Arr(a) => a.borrow().clone(),
                    other => vec![other.clone()],
                };
                self.destructure(n, list, fr, sc)?;
            }
            Node::Splat(s) => {
                if let Some(t) = s.value.as_deref() {
                    self.assign(t, v, fr, sc)?;
                }
            }
            Node::Arg(a) => sc.declare(&a.name, v),
            Node::Procarg0(p) => {
                if p.args.len() == 1 {
                    self.assign(&p.args[0], v, fr, sc)?;
                } else {
                    let list = match &v {
                        V::Arr(a) => a.borrow().clone(),
                        other => vec![other.clone()],
                    };
                    self.destructure_list(&p.args, list, fr, sc)?;
                }
            }
            Node::Restarg(r) => {
                if let Some(nm) = &r.name {
                    sc.declare(nm, v);
                }
            }
            other => return Err(self.err("SyntaxError", format!("assign to {}", node_name(other)))),
        }
        Ok(())
    }

    pub fn destructure(
        self: &Rc<Self>,
        lhs: &'static Node,
        list: Vec<V>,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> Result<(), Ctrl> {
        match lhs {
            Node::Mlhs(m) => self.destructure_list(&m.items, list, fr, sc),
            other => self.assign(other, V::arr(list), fr, sc),
        }
    }

    pub fn destructure_list(
        self: &Rc<Self>,
        items: &'static [Node],
        list: Vec<V>,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> Result<(), Ctrl> {
        let splat_at = items
            .iter()
            .position(|i| matches!(i, Node::Splat(_) | Node::Restarg(_)));
        match splat_at {
            None => {
                for (i, it) in items.iter().enumerate() {
                    self.assign(it, list.get(i).cloned().unwrap_or(V::Nil), fr, sc)?;
                }
            }
            Some(si) => {
                let after = items.len() - si - 1;
                for i in 0..si {
                    self.assign(&items[i], list.get(i).cloned().unwrap_or(V::Nil), fr, sc)?;
                }
                let end = list.len().saturating_sub(after).max(si);
                let mid: Vec<V> = if si < end {
                    list[si..end].to_vec()
                } else {
                    vec![]
                };
                self.assign(&items[si], V::arr(mid), fr, sc)?;
                for j in 0..after {
                    self.assign(
                        &items[si + 1 + j],
                        list.get(end + j).cloned().unwrap_or(V::Nil),
                        fr,
                        sc,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn eval_rescue(
        self: &Rc<Self>,
        r: &'static lib_ruby_parser::nodes::Rescue,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> R {
        loop {
            match self.eval_opt(r.body.as_deref(), fr, sc) {
                Ok(v) => {
                    if let Some(e) = r.else_.as_deref() {
                        return self.eval(e, fr, sc);
                    }
                    return Ok(v);
                }
                Err(Ctrl::Raise(exc)) => {
                    let mut handled = None;
                    for rb in &r.rescue_bodies {
                        let Node::RescueBody(rb) = rb else { continue };
                        let classes: Vec<V> = match rb.exc_list.as_deref() {
                            None => vec![V::Mod(self.cls("StandardError"))],
                            Some(Node::Array(a)) => {
                                let mut out = vec![];
                                for e in &a.elements {
                                    self.eval_elem(e, fr, sc, &mut out)?;
                                }
                                out
                            }
                            Some(other) => {
                                let v = self.eval(other, fr, sc)?;
                                match v {
                                    V::Arr(a) => a.borrow().clone(),
                                    v => vec![v],
                                }
                            }
                        };
                        let hit = classes.iter().any(|c| match c {
                            V::Mod(m) => self.is_a_mod(&exc, m),
                            _ => false,
                        });
                        if hit {
                            handled = Some(rb);
                            break;
                        }
                    }
                    match handled {
                        Some(rb) => {
                            if let Some(var) = rb.exc_var.as_deref() {
                                self.assign(var, exc.clone(), fr, sc)?;
                            }
                            self.gvars.borrow_mut().insert("$!".into(), exc.clone());
                            match self.eval_opt(rb.body.as_deref(), fr, sc) {
                                Err(Ctrl::Retry) => continue,
                                other => return other,
                            }
                        }
                        None => return Err(Ctrl::Raise(exc)),
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn eval_defined(self: &Rc<Self>, n: &'static Node, fr: &Rc<Frame>, sc: &Rc<Scope>) -> R {
        let r = match n {
            Node::Lvar(_) => Some("local-variable"),
            Node::Ivar(i) => {
                let has = match &fr.self_v {
                    V::Obj(o) => o.ivars.borrow().contains_key(&i.name),
                    V::Mod(m) => m.ivars.borrow().contains_key(&i.name),
                    _ => false,
                };
                has.then_some("instance-variable")
            }
            Node::Gvar(g) => self
                .gvars
                .borrow()
                .contains_key(&g.name)
                .then_some("global-variable"),
            Node::Cvar(c) => {
                let m = self.cvar_holder(fr, &c.name);
                let has = m.cvars.borrow().contains_key(&c.name);
                has.then_some("class variable")
            }
            Node::Const(c) => self
                .eval_const(c.scope.as_deref(), &c.name, fr, sc)
                .ok()
                .map(|_| "constant"),
            Node::Send(s) => {
                let ok = match s.recv.as_deref() {
                    None => self.responds(&fr.self_v, &s.method_name),
                    Some(r) => match self.eval(r, fr, sc) {
                        Ok(v) => self.responds(&v, &s.method_name),
                        Err(_) => false,
                    },
                };
                ok.then_some("method")
            }
            Node::Yield(_) => fr.block.is_some().then_some("yield"),
            Node::Super(_) | Node::ZSuper(_) => Some("super"),
            Node::Self_(_) => Some("self"),
            Node::Nil(_) => Some("expression"),
            _ => Some("expression"),
        };
        Ok(r.map(V::str).unwrap_or(V::Nil))
    }

    pub fn case_eq(self: &Rc<Self>, pat: &V, subj: Option<&V>) -> Result<bool, Ctrl> {
        let Some(s) = subj else {
            return Ok(pat.truthy());
        };
        Ok(self.call(pat, "===", Args::new(vec![s.clone()]))?.truthy())
    }

    pub fn eval_const(
        self: &Rc<Self>,
        scope: Option<&'static Node>,
        name: &str,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
    ) -> R {
        if let Some(s) = scope {
            let holder = self.eval(s, fr, sc)?;
            let V::Mod(m) = holder else {
                return Err(self.err(
                    "TypeError",
                    format!(
                        "{} is not a class/module",
                        crate::core::inspect_plain(&holder)
                    ),
                ));
            };
            if let Some(v) = self.const_lookup_in(&m, name)? {
                return Ok(v);
            }
            return self.const_missing(&m, name);
        }
        for m in fr.cref.iter().rev() {
            if Rc::ptr_eq(m, &self.object) {
                continue;
            }
            if let Some(v) = self.own_const(m, name)? {
                return Ok(v);
            }
        }
        if let Some(innermost) = fr.cref.last() {
            if let Some(v) = self.const_lookup_in(innermost, name)? {
                return Ok(v);
            }
        }
        if let Some(v) = self.own_const(&self.object.clone(), name)? {
            return Ok(v);
        }
        self.const_missing(fr.cref.last().unwrap(), name)
    }

    fn const_missing(self: &Rc<Self>, m: &Rc<Module>, name: &str) -> R {
        let mv = V::Mod(m.clone());
        if self.find_method(&mv, "const_missing").is_some() {
            return self.call_full(
                &mv,
                "const_missing",
                Args::new(vec![V::sym(name)]),
                true,
                None,
            );
        }
        if let Some(v) = crate::stdlib::autoload_const(self, name)? {
            return Ok(v);
        }
        Err(self.err(
            "NameError",
            format!(
                "uninitialized constant {}{}",
                if Rc::ptr_eq(m, &self.object) {
                    String::new()
                } else {
                    format!("{}::", m.display_name())
                },
                name
            ),
        ))
    }

    pub fn own_const(self: &Rc<Self>, m: &Rc<Module>, name: &str) -> Result<Option<V>, Ctrl> {
        if let Some(v) = m.consts.borrow().get(name) {
            return Ok(Some(v.clone()));
        }
        let key = (m.id, name.to_string());
        let path = self.autoloads.borrow_mut().remove(&key);
        if let Some(path) = path {
            self.load_feature(&path, None)?;
            return Ok(m.consts.borrow().get(name).cloned());
        }
        Ok(None)
    }

    pub fn const_lookup_in(self: &Rc<Self>, m: &Rc<Module>, name: &str) -> Result<Option<V>, Ctrl> {
        for a in self.ancestors(m) {
            if let Some(v) = self.own_const(&a, name)? {
                return Ok(Some(v));
            }
        }
        if !m.is_class {
            if let Some(v) = self.own_const(&self.object.clone(), name)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    pub fn const_set(&self, target: &Rc<Module>, name: &str, v: V) {
        if let V::Mod(m) = &v {
            if m.name.borrow().is_none() {
                *m.name.borrow_mut() = Some(self.qualified(target, name));
            }
        }
        target.consts.borrow_mut().insert(name.to_string(), v);
    }

    pub fn cvar_holder(&self, fr: &Rc<Frame>, name: &str) -> Rc<Module> {
        let base = match &fr.self_v {
            V::Mod(m) => {
                if let Some(V::Mod(att)) = m.attached.borrow().clone() {
                    att
                } else {
                    m.clone()
                }
            }
            other => self.class_of(other),
        };
        let base = fr
            .cref
            .last()
            .filter(|c| !Rc::ptr_eq(c, &self.object))
            .cloned()
            .unwrap_or(base);
        for a in self.ancestors(&base) {
            if a.cvars.borrow().contains_key(name) {
                return a;
            }
        }
        base
    }

    pub fn ivar_get(&self, s: &V, n: &str) -> V {
        match s {
            V::Obj(o) => o.ivars.borrow().get(n).cloned().unwrap_or(V::Nil),
            V::Mod(m) => m.ivars.borrow().get(n).cloned().unwrap_or(V::Nil),
            _ => V::Nil,
        }
    }

    pub fn ivar_set(&self, s: &V, n: &str, v: V) -> Result<(), Ctrl> {
        match s {
            V::Obj(o) => {
                if o.frozen.get() {
                    return Err(self.err(
                        "FrozenError",
                        format!("can't modify frozen {}", o.class.borrow().display_name()),
                    ));
                }
                o.ivars.borrow_mut().insert(n.to_string(), v);
            }
            V::Mod(m) => {
                m.ivars.borrow_mut().insert(n.to_string(), v);
            }
            _ => {}
        }
        Ok(())
    }

    pub fn gvar_get(&self, n: &str) -> V {
        match n {
            "$LOAD_PATH" | "$:" => V::arr(
                self.load_path
                    .borrow()
                    .iter()
                    .map(|p| V::str(p.display().to_string()))
                    .collect(),
            ),
            "$stderr" | "$stdout" => self
                .object
                .consts
                .borrow()
                .get("STDERR")
                .cloned()
                .unwrap_or(V::Nil),
            "$~" => self.last_match.borrow().clone(),
            _ => self.gvars.borrow().get(n).cloned().unwrap_or(V::Nil),
        }
    }

    fn method_added_hook(self: &Rc<Self>, _m: &Rc<Module>, _name: &str) -> Result<(), Ctrl> {
        Ok(())
    }

    fn inherited_hook(self: &Rc<Self>, sup: &Rc<Module>, sub: &Rc<Module>) -> Result<(), Ctrl> {
        let sv = V::Mod(sup.clone());
        if self.find_method(&sv, "inherited").is_some() {
            self.call_full(
                &sv,
                "inherited",
                Args::new(vec![V::Mod(sub.clone())]),
                true,
                None,
            )?;
        }
        crate::core::native_inherited(self, sup, sub)
    }

    pub fn is_a_mod(&self, v: &V, m: &Rc<Module>) -> bool {
        self.lookup_chain(v).iter().any(|a| a.id == m.id)
    }

    pub fn is_a(&self, v: &V, cname: &str) -> bool {
        match self.lookup_const_path(cname) {
            Some(m) => self.is_a_mod(v, &m),
            None => false,
        }
    }
}

fn clone_body(b: &ProcBody) -> ProcBody {
    match b {
        ProcBody::Ast {
            args,
            body,
            numargs,
        } => ProcBody::Ast {
            args: *args,
            body: *body,
            numargs: *numargs,
        },
        ProcBody::SymToProc(s) => ProcBody::SymToProc(s.clone()),
        ProcBody::Method(m) => ProcBody::Method(m.clone()),
    }
}

pub fn node_name(n: &Node) -> String {
    let s = format!("{n:?}");
    s.split(|c: char| c == '(' || c == ' ')
        .next()
        .unwrap_or("?")
        .to_string()
}

pub fn _unused(_: RefCell<IndexMap<String, V>>) {}
