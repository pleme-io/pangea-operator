use std::rc::Rc;

use lib_ruby_parser::Node;

use crate::interp::*;
use crate::value::*;

pub const FORWARDED_ARGS: &str = "(...)";
pub const FORWARDED_KWARGS: &str = "(**...)";

const MAX_DEPTH: usize = 2000;

impl Interp {
    pub fn call(self: &Rc<Self>, recv: &V, name: &str, args: Args) -> R {
        self.call_full(recv, name, args, true, None)
    }

    pub fn call_full(
        self: &Rc<Self>,
        recv: &V,
        name: &str,
        args: Args,
        fcall: bool,
        caller: Option<&Rc<Frame>>,
    ) -> R {
        let d = self.depth.get();
        if d > MAX_DEPTH {
            return Err(self.err("SystemStackError", "stack level too deep"));
        }
        self.depth.set(d + 1);
        let r = self.dispatch(recv, name, args, fcall, caller);
        self.depth.set(d);
        r
    }

    fn dispatch(
        self: &Rc<Self>,
        recv: &V,
        name: &str,
        args: Args,
        fcall: bool,
        caller: Option<&Rc<Frame>>,
    ) -> R {
        if let Some((m, owner)) = self.find_method(recv, name) {
            if let Meth::User(um) = &m {
                if um.private && !fcall && !name.ends_with('=') {
                    let _ = um;
                }
            }
            return self.invoke(recv, name, &m, &owner, args, caller);
        }
        match crate::core::native_call(self, recv, name, args.clone(), caller)? {
            Some(v) => Ok(v),
            None => self.method_missing(recv, name, args, fcall),
        }
    }

    pub fn method_missing(self: &Rc<Self>, recv: &V, name: &str, args: Args, fcall: bool) -> R {
        if name != "method_missing" {
            if let Some((m, owner)) = self.find_method(recv, "method_missing") {
                let mut a = args;
                a.pos.insert(0, V::sym(name));
                return self.invoke(recv, "method_missing", &m, &owner, a, None);
            }
        }
        let desc = crate::core::describe_recv(self, recv);
        let what = if fcall {
            "undefined local variable or method"
        } else {
            "undefined method"
        };
        let cls = if fcall && args.pos.is_empty() {
            "NameError"
        } else {
            "NoMethodError"
        };
        Err(self.err(cls, format!("{what} '{name}' for {desc}")))
    }

    pub fn invoke(
        self: &Rc<Self>,
        recv: &V,
        name: &str,
        m: &Meth,
        owner: &Rc<Module>,
        args: Args,
        caller: Option<&Rc<Frame>>,
    ) -> R {
        match m {
            Meth::User(um) => self.call_user(recv, um, args),
            Meth::Proc(p, _) => {
                let fr = Rc::new(Frame {
                    self_v: recv.clone(),
                    def_target: self.def_target_for(recv),
                    cref: p.frame.cref.clone(),
                    block: args.blk.clone(),
                    method: Some((owner.clone(), name.to_string())),
                    args: args.flat(),
                    id: self.id(),
                    file: p.frame.file.clone(),
                });
                let fid = fr.id;
                match self.call_proc_in(
                    p,
                    args.flat(),
                    args.blk.clone(),
                    Some(fr),
                    true,
                    args.kw.clone(),
                ) {
                    Err(Ctrl::Return(v, id)) if id == fid => Ok(v),
                    other => other,
                }
            }
            Meth::Native(n) => match crate::core::native_call(self, recv, n, args.clone(), caller)?
            {
                Some(v) => Ok(v),
                None => self.method_missing(recv, n, args, true),
            },
            Meth::Undefined => self.method_missing(recv, name, args, false),
        }
    }

    pub fn def_target_for(&self, recv: &V) -> Rc<Module> {
        match recv {
            V::Mod(m) => m.clone(),
            other => self.class_of(other),
        }
    }

    pub fn call_user(self: &Rc<Self>, recv: &V, um: &Rc<UserMethod>, args: Args) -> R {
        let fr = Rc::new(Frame {
            self_v: recv.clone(),
            def_target: match recv {
                V::Mod(_) => um.owner.clone(),
                _ => um.owner.clone(),
            },
            cref: um.cref.clone(),
            block: args.blk.clone(),
            method: Some((um.owner.clone(), um.name.clone())),
            args: args.flat(),
            id: self.id(),
            file: um.file.clone(),
        });
        let sc = Scope::new(None);
        let caller_file = self.cur_file.borrow().clone();
        self.stack
            .borrow_mut()
            .push((um.name.clone(), caller_file, self.cur_pos.get()));
        let r = match self.bind_params(um.args, &args, &fr, &sc, true, &um.name) {
            Ok(()) => match self.eval_opt(um.body, &fr, &sc) {
                Err(Ctrl::Return(v, id)) if id == fr.id => Ok(v),
                other => other,
            },
            Err(e) => Err(e),
        };
        self.stack.borrow_mut().pop();
        r
    }

    pub fn call_super(self: &Rc<Self>, fr: &Rc<Frame>, args: Args) -> R {
        let Some((owner, name)) = fr.method.clone() else {
            return Err(self.err("RuntimeError", "super called outside of method"));
        };
        if let Some((m, o)) = self.find_super(&fr.self_v, &owner, &name) {
            return self.invoke(&fr.self_v, &name, &m, &o, args, Some(fr));
        }
        match crate::core::native_call(self, &fr.self_v, &name, args.clone(), Some(fr))? {
            Some(v) => Ok(v),
            None => {
                if name == "method_missing" {
                    let n = args
                        .pos
                        .first()
                        .map(crate::core::to_s_plain)
                        .unwrap_or_default();
                    let desc = crate::core::describe_recv(self, &fr.self_v);
                    return Err(self.err(
                        "NoMethodError",
                        format!("undefined method '{n}' for {desc}"),
                    ));
                }
                if name == "respond_to_missing?" {
                    return Ok(V::Bool(false));
                }
                if name == "initialize"
                    || name == "inherited"
                    || name == "included"
                    || name == "extended"
                {
                    return Ok(V::Nil);
                }
                Err(self.err(
                    "NoMethodError",
                    format!("super: no superclass method '{name}'"),
                ))
            }
        }
    }

    pub fn call_proc(self: &Rc<Self>, p: &Rc<Proc>, args: Vec<V>, blk: Option<Rc<Proc>>) -> R {
        self.call_proc_in(p, args, blk, None, p.lambda, None)
    }

    pub fn call_proc_self(self: &Rc<Self>, p: &Rc<Proc>, self_v: V, args: Vec<V>) -> R {
        let def_target = match &self_v {
            V::Mod(m) => m.clone(),
            other => self
                .singleton_of(other)
                .unwrap_or_else(|_| self.class_of(other)),
        };
        let fr = Rc::new(Frame {
            self_v,
            def_target,
            cref: p.frame.cref.clone(),
            block: p.frame.block.clone(),
            method: p.frame.method.clone(),
            args: p.frame.args.clone(),
            id: p.frame.id,
            file: p.frame.file.clone(),
        });
        self.call_proc_in(p, args, None, Some(fr), false, None)
    }

    pub fn call_proc_in(
        self: &Rc<Self>,
        p: &Rc<Proc>,
        args: Vec<V>,
        blk: Option<Rc<Proc>>,
        frame: Option<Rc<Frame>>,
        lambda: bool,
        kw: Option<Rc<RHash>>,
    ) -> R {
        match &p.body {
            ProcBody::SymToProc(name) => {
                let mut it = args.into_iter();
                let recv = it.next().unwrap_or(V::Nil);
                self.call_full(&recv, name, Args::with_blk(it.collect(), blk), false, None)
            }
            ProcBody::Method(m) => {
                let a = Args { pos: args, kw, blk };
                self.call_full(&m.recv, &m.name, a, true, None)
            }
            ProcBody::Ast {
                args: params,
                body,
                numargs,
            } => {
                let fr = frame.unwrap_or_else(|| p.frame.clone());
                let fr = if blk.is_some() && frame_is_procs(&fr, p) {
                    Rc::new(Frame {
                        self_v: fr.self_v.clone(),
                        def_target: fr.def_target.clone(),
                        cref: fr.cref.clone(),
                        block: fr.block.clone(),
                        method: fr.method.clone(),
                        args: fr.args.clone(),
                        id: fr.id,
                        file: fr.file.clone(),
                    })
                } else {
                    fr
                };
                let sc = Scope::new(Some(p.scope.clone()));
                let a = Args {
                    pos: args,
                    kw,
                    blk: blk.clone(),
                };
                if *numargs > 0 {
                    let mut list = a.pos.clone();
                    if *numargs > 1 && list.len() == 1 {
                        let inner = if let V::Arr(x) = &list[0] {
                            Some(x.borrow().clone())
                        } else {
                            None
                        };
                        if let Some(i) = inner {
                            list = i;
                        }
                    }
                    for i in 0..*numargs {
                        sc.declare(
                            &format!("_{}", i + 1),
                            list.get(i as usize).cloned().unwrap_or(V::Nil),
                        );
                    }
                } else {
                    self.bind_params(*params, &a, &fr, &sc, lambda, "block")?;
                }
                if let Some(b) = blk {
                    sc.declare("__lava_blockarg", V::Proc(b));
                }
                let r = self.eval_opt(*body, &fr, &sc);
                match r {
                    Err(Ctrl::Next(v)) => Ok(v),
                    Err(Ctrl::Break(v, 0)) => Err(Ctrl::Break(v, p.id)),
                    Err(Ctrl::Return(v, id)) if lambda && p.lambda && id == fr.id => Ok(v),
                    other => other,
                }
            }
        }
    }

    pub fn bind_params(
        self: &Rc<Self>,
        params: Option<&'static Node>,
        a: &Args,
        fr: &Rc<Frame>,
        sc: &Rc<Scope>,
        strict: bool,
        mname: &str,
    ) -> Result<(), Ctrl> {
        let Some(Node::Args(pa)) = params else {
            if strict && params.is_none() && !a.pos.is_empty() && mname != "block" {
                return Err(self.err(
                    "ArgumentError",
                    format!(
                        "wrong number of arguments (given {}, expected 0)",
                        a.pos.len()
                    ),
                ));
            }
            return Ok(());
        };
        let list: &'static [Node] = &pa.args;
        let mut pos = a.pos.clone();
        let has_kw = list.iter().any(|n| {
            matches!(
                n,
                Node::Kwarg(_) | Node::Kwoptarg(_) | Node::Kwrestarg(_) | Node::Kwnilarg(_)
            )
        });
        let mut kw = a.kw.clone();
        let has_fwd = list.iter().any(|n| matches!(n, Node::ForwardArg(_)));
        if !has_kw && !has_fwd {
            if let Some(k) = kw.take() {
                if k.len() > 0 || mname == "block" {
                    pos.push(V::Hash(k));
                }
            }
        }
        let positional: Vec<&'static Node> = list
            .iter()
            .filter(|n| {
                matches!(
                    n,
                    Node::Arg(_)
                        | Node::Optarg(_)
                        | Node::Restarg(_)
                        | Node::Mlhs(_)
                        | Node::Procarg0(_)
                )
            })
            .collect();
        let req_count = positional
            .iter()
            .filter(|n| matches!(n, Node::Arg(_) | Node::Mlhs(_) | Node::Procarg0(_)))
            .count();
        let opt_count = positional
            .iter()
            .filter(|n| matches!(n, Node::Optarg(_)))
            .count();
        let has_rest = has_fwd || positional.iter().any(|n| matches!(n, Node::Restarg(_)));
        if !strict {
            let simple_multi =
                positional.len() > 1 || has_rest && positional.len() > 1 || positional.len() > 1;
            let procarg_multi =
                matches!(positional.first(), Some(Node::Procarg0(p)) if p.args.len() > 1);
            if pos.len() == 1 && simple_multi && !procarg_multi {
                let inner = if let V::Arr(x) = &pos[0] {
                    Some(x.borrow().clone())
                } else {
                    None
                };
                if let Some(i) = inner {
                    pos = i;
                }
            }
        } else if pos.len() < req_count || (!has_rest && pos.len() > req_count + opt_count) {
            let expected = if has_rest {
                format!("{req_count}+")
            } else if opt_count > 0 {
                format!("{}..{}", req_count, req_count + opt_count)
            } else {
                req_count.to_string()
            };
            return Err(self.err(
                "ArgumentError",
                format!(
                    "wrong number of arguments (given {}, expected {})",
                    pos.len(),
                    expected
                ),
            ));
        }
        let avail_opt = pos.len().saturating_sub(req_count).min(opt_count);
        if has_fwd {
            let tail = pos
                .get(req_count + avail_opt..)
                .map(<[V]>::to_vec)
                .unwrap_or_default();
            sc.declare(FORWARDED_ARGS, V::arr(tail));
            if let Some(k) = &kw {
                sc.declare(FORWARDED_KWARGS, V::Hash(k.clone()));
            }
        }
        let rest_len = pos.len().saturating_sub(req_count + avail_opt);
        let rest_idx = positional
            .iter()
            .position(|n| matches!(n, Node::Restarg(_)));
        let mut i = 0usize;
        let mut opt_used = 0usize;
        for (pi, p) in positional.iter().enumerate() {
            match p {
                Node::Arg(x) => {
                    sc.declare(&x.name, pos.get(i).cloned().unwrap_or(V::Nil));
                    i += 1;
                }
                Node::Procarg0(_) | Node::Mlhs(_) => {
                    let v = pos.get(i).cloned().unwrap_or(V::Nil);
                    i += 1;
                    if let Node::Mlhs(_) = p {
                        let l = match &v {
                            V::Arr(a) => a.borrow().clone(),
                            o => vec![o.clone()],
                        };
                        self.destructure(p, l, fr, sc)?;
                    } else {
                        self.assign(p, v, fr, sc)?;
                    }
                }
                Node::Optarg(o) => {
                    if opt_used < avail_opt {
                        sc.declare(&o.name, pos.get(i).cloned().unwrap_or(V::Nil));
                        i += 1;
                        opt_used += 1;
                    } else {
                        let v = self.eval(&o.default, fr, sc)?;
                        sc.declare(&o.name, v);
                    }
                }
                Node::Restarg(r) => {
                    let take = if strict || rest_idx == Some(pi) {
                        rest_len
                    } else {
                        0
                    };
                    let end = (i + take).min(pos.len());
                    let slice = if i <= end {
                        pos[i..end].to_vec()
                    } else {
                        vec![]
                    };
                    i = end;
                    if let Some(n) = &r.name {
                        sc.declare(n, V::arr(slice));
                    } else {
                        sc.declare("__lava_rest", V::arr(slice));
                    }
                }
                _ => {}
            }
        }
        let kwh = kw.unwrap_or_default();
        let mut used = std::collections::HashSet::new();
        for p in list {
            match p {
                Node::Kwarg(k) => {
                    let v = kwh.get(&V::sym(&k.name));
                    match v {
                        Some(v) => {
                            sc.declare(&k.name, v);
                            used.insert(k.name.clone());
                        }
                        None => {
                            return Err(
                                self.err("ArgumentError", format!("missing keyword: :{}", k.name))
                            )
                        }
                    }
                }
                Node::Kwoptarg(k) => {
                    let v = kwh.get(&V::sym(&k.name));
                    match v {
                        Some(v) => {
                            sc.declare(&k.name, v);
                            used.insert(k.name.clone());
                        }
                        None => {
                            let d = self.eval(&k.default, fr, sc)?;
                            sc.declare(&k.name, d);
                        }
                    }
                }
                _ => {}
            }
        }
        let rest_kw = list.iter().find_map(|n| {
            if let Node::Kwrestarg(k) = n {
                Some(k)
            } else {
                None
            }
        });
        if let Some(k) = rest_kw {
            let h = Rc::new(RHash::default());
            for (kk, vv) in kwh.entries() {
                if let V::Sym(s) = &kk {
                    if used.contains(&**s) {
                        continue;
                    }
                }
                h.insert(kk, vv);
            }
            sc.declare(k.name.as_deref().unwrap_or("__lava_kwrest"), V::Hash(h));
        } else if has_kw && strict {
            for (kk, _) in kwh.entries() {
                if let V::Sym(s) = &kk {
                    if !used.contains(&**s) {
                        return Err(self.err("ArgumentError", format!("unknown keyword: :{s}")));
                    }
                }
            }
        }
        for p in list {
            if let Node::Blockarg(b) = p {
                if let Some(n) = &b.name {
                    sc.declare(n, a.blk.clone().map(V::Proc).unwrap_or(V::Nil));
                }
            }
            if let Node::Shadowarg(s) = p {
                sc.declare(&s.name, V::Nil);
            }
        }
        Ok(())
    }
}

fn frame_is_procs(_fr: &Rc<Frame>, _p: &Rc<Proc>) -> bool {
    false
}
