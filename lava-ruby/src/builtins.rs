use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

use crate::core::*;
use crate::fmt::*;
use crate::interp::*;
use crate::value::*;

type NR = Result<Option<V>, Ctrl>;

fn some(v: V) -> NR {
    Ok(Some(v))
}

fn arg(a: &Args, i: usize) -> V {
    a.flat().get(i).cloned().unwrap_or(V::Nil)
}

fn int_arg(it: &Rc<Interp>, a: &Args, i: usize) -> Result<i64, Ctrl> {
    match arg(a, i) {
        V::Int(x) => Ok(x),
        V::Float(f) => Ok(f as i64),
        V::Nil => Err(it.err("TypeError", "no implicit conversion from nil to integer")),
        other => Err(it.err(
            "TypeError",
            format!(
                "no implicit conversion of {} into Integer",
                it.class_of(&other).display_name()
            ),
        )),
    }
}

pub fn yield1(it: &Rc<Interp>, b: &Rc<Proc>, v: V) -> R {
    it.call_proc(b, vec![v], None)
}

fn yieldn(it: &Rc<Interp>, b: &Rc<Proc>, v: Vec<V>) -> R {
    it.call_proc(b, v, None)
}

fn blk_or_err<'a>(it: &Rc<Interp>, a: &'a Args, name: &str) -> Result<&'a Rc<Proc>, Ctrl> {
    a.blk
        .as_ref()
        .ok_or_else(|| it.err("LocalJumpError", format!("no block given ({name})")))
}

fn str_of(it: &Rc<Interp>, v: &V) -> Result<String, Ctrl> {
    match v {
        V::Str(s) => Ok(s.s.borrow().clone()),
        V::Sym(s) => Ok(s.to_string()),
        other => Err(it.err(
            "TypeError",
            format!(
                "no implicit conversion of {} into String",
                it.class_of(other).display_name()
            ),
        )),
    }
}

fn regex_of(it: &Rc<Interp>, v: &V) -> Result<Rc<RRegex>, Ctrl> {
    match v {
        V::Regex(r) => Ok(r.clone()),
        V::Str(s) => {
            let src = fancy_regex::escape(&s.s.borrow()).into_owned();
            match make_regex(it, &src, "")? {
                V::Regex(r) => Ok(r),
                _ => unreachable!(),
            }
        }
        _ => Err(it.err("TypeError", "wrong argument type (expected Regexp)")),
    }
}

pub fn match_data(it: &Rc<Interp>, re: &RRegex, s: &str, caps: &fancy_regex::Captures) -> V {
    let md = it.new_obj(it.cls("MatchData"));
    let groups: Vec<V> = (0..caps.len())
        .map(|i| caps.get(i).map(|m| V::str(m.as_str())).unwrap_or(V::Nil))
        .collect();
    let named = Rc::new(RHash::default());
    for n in re.re.capture_names().flatten() {
        named.insert(
            V::str(n),
            caps.name(n).map(|m| V::str(m.as_str())).unwrap_or(V::Nil),
        );
    }
    let m0 = caps.get(0).unwrap();
    if let V::Obj(o) = &md {
        let mut iv = o.ivars.borrow_mut();
        iv.insert("@groups".into(), V::arr(groups));
        iv.insert("@named".into(), V::Hash(named));
        iv.insert("@pre".into(), V::str(&s[..m0.start()]));
        iv.insert("@post".into(), V::str(&s[m0.end()..]));
        iv.insert(
            "@begin".into(),
            V::Int(s[..m0.start()].chars().count() as i64),
        );
    }
    md
}

fn do_match(it: &Rc<Interp>, re: &Rc<RRegex>, s: &str, set_last: bool) -> R {
    match re.re.captures(s) {
        Ok(Some(c)) => {
            let md = match_data(it, re, s, &c);
            if set_last {
                *it.last_match.borrow_mut() = md.clone();
            }
            Ok(md)
        }
        _ => {
            if set_last {
                *it.last_match.borrow_mut() = V::Nil;
            }
            Ok(V::Nil)
        }
    }
}

fn expand_sub(repl: &str, caps: &fancy_regex::Captures) -> String {
    let mut out = String::new();
    let mut ch = repl.chars().peekable();
    while let Some(c) = ch.next() {
        if c == '\\' {
            match ch.peek().copied() {
                Some(d) if d.is_ascii_digit() => {
                    ch.next();
                    let i = d.to_digit(10).unwrap() as usize;
                    if let Some(m) = caps.get(i) {
                        out.push_str(m.as_str());
                    }
                }
                Some('k') => {
                    ch.next();
                    if ch.peek() == Some(&'<') {
                        ch.next();
                        let mut name = String::new();
                        for c in ch.by_ref() {
                            if c == '>' {
                                break;
                            }
                            name.push(c);
                        }
                        if let Some(m) = caps.name(&name) {
                            out.push_str(m.as_str());
                        }
                    }
                }
                Some('\\') => {
                    ch.next();
                    out.push('\\');
                }
                Some('&') => {
                    ch.next();
                    out.push_str(caps.get(0).map(|m| m.as_str()).unwrap_or(""));
                }
                _ => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn gsub_impl(it: &Rc<Interp>, s: &str, a: &Args, global: bool) -> Result<String, Ctrl> {
    let pat = arg(a, 0);
    if let V::Hash(map) = arg(a, 1) {
        let re = regex_of(it, &pat)?;
        let mut out = String::new();
        let mut last = 0;
        for m in re.re.find_iter(s) {
            let m = m.map_err(|e| it.err("RegexpError", e.to_string()))?;
            out.push_str(&s[last..m.start()]);
            let r = map.get(&V::str(m.as_str())).unwrap_or(V::Nil);
            out.push_str(&to_s_plain(&r));
            last = m.end();
            if !global {
                break;
            }
        }
        out.push_str(&s[last..]);
        return Ok(out);
    }
    if let (V::Str(p), true) = (&pat, a.blk.is_none()) {
        let p = p.s.borrow().clone();
        let r = str_of(it, &arg(a, 1))?;
        if !r.contains('\\') {
            return Ok(if global {
                s.replace(&p, &r)
            } else {
                s.replacen(&p, &r, 1)
            });
        }
    }
    let re = regex_of(it, &pat)?;
    let mut out = String::new();
    let mut last = 0;
    let mut pos = 0;
    while pos <= s.len() {
        let caps = match re.re.captures_from_pos(s, pos) {
            Ok(Some(c)) => c,
            _ => break,
        };
        let m = caps.get(0).unwrap();
        out.push_str(&s[last..m.start()]);
        if let Some(b) = &a.blk {
            let md = match_data(it, &re, s, &caps);
            *it.last_match.borrow_mut() = md;
            let r = yield1(it, b, V::str(m.as_str()))?;
            out.push_str(&it.to_s(&r)?);
        } else {
            let r = str_of(it, &arg(a, 1))?;
            out.push_str(&expand_sub(&r, &caps));
        }
        last = m.end();
        pos = if m.end() == m.start() {
            let next = s[m.end()..]
                .chars()
                .next()
                .map(|c| c.len_utf8())
                .unwrap_or(1);
            if m.end() < s.len() {
                out.push_str(&s[m.end()..m.end() + next]);
            }
            last = (m.end() + next).min(s.len());
            m.end() + next
        } else {
            m.end()
        };
        if !global {
            break;
        }
    }
    if last <= s.len() {
        out.push_str(&s[last..]);
    }
    Ok(out)
}

fn char_index(s: &str, i: i64) -> Option<usize> {
    let n = s.chars().count() as i64;
    let i = if i < 0 { n + i } else { i };
    if i < 0 || i > n {
        None
    } else {
        Some(i as usize)
    }
}

fn substr(s: &str, start: usize, len: usize) -> String {
    s.chars().skip(start).take(len).collect()
}

fn tr_expand(set: &str) -> (bool, Vec<char>) {
    let mut chars: Vec<char> = set.chars().collect();
    let neg = chars.len() > 1 && chars[0] == '^';
    if neg {
        chars.remove(0);
    }
    let mut out = vec![];
    let mut i = 0;
    while i < chars.len() {
        if i + 2 < chars.len() && chars[i + 1] == '-' {
            for c in chars[i]..=chars[i + 2] {
                out.push(c);
            }
            i += 3;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    (neg, out)
}

pub fn str_m(it: &Rc<Interp>, recv: &V, rs: &Rc<RStr>, name: &str, a: &Args) -> NR {
    let s = rs.s.borrow().clone();
    let check_frozen = || -> Result<(), Ctrl> {
        if rs.frozen.get() {
            return Err(it.err(
                "FrozenError",
                format!("can't modify frozen String: {}", str_inspect(&s)),
            ));
        }
        Ok(())
    };
    let v = match name {
        "to_s" | "to_str" => recv.clone(),
        "inspect" | "dump" => V::str(str_inspect(&s)),
        "to_sym" | "intern" => V::sym(&s),
        "length" | "size" => V::Int(s.chars().count() as i64),
        "bytesize" => V::Int(s.len() as i64),
        "empty?" => V::Bool(s.is_empty()),
        "upcase" => V::str(s.to_uppercase()),
        "downcase" => V::str(s.to_lowercase()),
        "swapcase" => V::str(
            s.chars()
                .map(|c| {
                    if c.is_uppercase() {
                        c.to_lowercase().collect::<String>()
                    } else {
                        c.to_uppercase().collect()
                    }
                })
                .collect::<String>(),
        ),
        "capitalize" => {
            let mut c = s.chars();
            V::str(match c.next() {
                Some(f) => f.to_uppercase().collect::<String>() + &c.as_str().to_lowercase(),
                None => String::new(),
            })
        }
        "strip" => V::str(s.trim_matches(|c: char| c.is_whitespace() || c == '\0')),
        "lstrip" => V::str(s.trim_start()),
        "rstrip" => V::str(s.trim_end()),
        "chomp" => {
            let p = arg(a, 0);
            if let V::Str(p) = p {
                let p = p.s.borrow().clone();
                V::str(s.strip_suffix(p.as_str()).unwrap_or(&s))
            } else {
                V::str(
                    s.strip_suffix("\r\n")
                        .or_else(|| s.strip_suffix('\n'))
                        .or_else(|| s.strip_suffix('\r'))
                        .unwrap_or(&s),
                )
            }
        }
        "chop" => {
            let mut c: Vec<char> = s.chars().collect();
            c.pop();
            V::str(c.into_iter().collect::<String>())
        }
        "chars" => V::arr(s.chars().map(|c| V::str(c.to_string())).collect()),
        "bytes" => V::arr(s.bytes().map(|b| V::Int(b as i64)).collect()),
        "lines" => V::arr(s.split_inclusive('\n').map(V::str).collect()),
        "each_line" => {
            let b = blk_or_err(it, a, "each_line")?;
            for l in s.split_inclusive('\n') {
                yield1(it, b, V::str(l))?;
            }
            recv.clone()
        }
        "each_char" => {
            let b = blk_or_err(it, a, "each_char")?;
            for c in s.chars() {
                yield1(it, b, V::str(c.to_string()))?;
            }
            recv.clone()
        }
        "ord" => V::Int(s.chars().next().map(|c| c as i64).unwrap_or(0)),
        "hex" => V::Int(i64::from_str_radix(s.trim_start_matches("0x"), 16).unwrap_or(0)),
        "to_i" => {
            let base = match arg(a, 0) {
                V::Int(b) => b as u32,
                _ => 10,
            };
            let t = s.trim().replace('_', "");
            let mut end = 0;
            for (i, c) in t.char_indices() {
                if c.is_digit(base) || (i == 0 && (c == '-' || c == '+')) {
                    end = i + c.len_utf8();
                } else {
                    break;
                }
            }
            V::Int(i64::from_str_radix(&t[..end], base).unwrap_or(0))
        }
        "to_f" => {
            let t = s.trim();
            let mut end = 0;
            for (i, c) in t.char_indices() {
                if c.is_ascii_digit()
                    || c == '.'
                    || c == 'e'
                    || c == 'E'
                    || (c == '-' || c == '+') && (i == 0 || t[..i].ends_with(['e', 'E']))
                {
                    end = i + 1;
                } else {
                    break;
                }
            }
            V::Float(t[..end].parse().unwrap_or(0.0))
        }
        "to_r" | "to_c" => return Ok(None),
        "+" => {
            let o = arg(a, 0);
            match &o {
                V::Str(x) => V::str(s.clone() + &x.s.borrow()),
                other => {
                    return Err(it.err(
                        "TypeError",
                        format!(
                            "no implicit conversion of {} into String",
                            it.class_of(other).display_name()
                        ),
                    ))
                }
            }
        }
        "*" => V::str(s.repeat(int_arg(it, a, 0)?.max(0) as usize)),
        "%" => {
            let o = arg(a, 0);
            let list = match o {
                V::Arr(x) => x.borrow().clone(),
                other => vec![other],
            };
            V::str(crate::stdlib::format_str(it, &s, &list)?)
        }
        "==" | "eql?" | "===" => V::Bool(matches!(arg(a, 0), V::Str(x) if *x.s.borrow() == s)),
        "!=" => V::Bool(!matches!(arg(a, 0), V::Str(x) if *x.s.borrow() == s)),
        "<=>" => match arg(a, 0) {
            V::Str(x) => V::Int(match s.as_bytes().cmp(x.s.borrow().as_bytes()) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }),
            _ => V::Nil,
        },
        "<" | ">" | "<=" | ">=" => {
            let o = str_of(it, &arg(a, 0))?;
            let c = s.as_bytes().cmp(o.as_bytes());
            V::Bool(match name {
                "<" => c == Ordering::Less,
                ">" => c == Ordering::Greater,
                "<=" => c != Ordering::Greater,
                _ => c != Ordering::Less,
            })
        }
        "hash" => V::Int(hkey_hash(&hkey(recv))),
        "start_with?" => {
            let mut r = false;
            for p in a.flat() {
                match &p {
                    V::Regex(re) => {
                        if let Ok(Some(m)) = re.re.find(&s) {
                            if m.start() == 0 {
                                r = true;
                            }
                        }
                    }
                    other => {
                        if s.starts_with(&str_of(it, other)?) {
                            r = true;
                        }
                    }
                }
            }
            V::Bool(r)
        }
        "end_with?" => {
            let mut r = false;
            for p in a.flat() {
                if s.ends_with(&str_of(it, &p)?) {
                    r = true;
                }
            }
            V::Bool(r)
        }
        "include?" => V::Bool(s.contains(&str_of(it, &arg(a, 0))?)),
        "index" => {
            let start = match arg(a, 1) {
                V::Int(i) => char_index(&s, i).unwrap_or(0),
                _ => 0,
            };
            let byte_start = s.char_indices().nth(start).map(|x| x.0).unwrap_or(s.len());
            let found = match arg(a, 0) {
                V::Regex(re) => re
                    .re
                    .find_from_pos(&s, byte_start)
                    .ok()
                    .flatten()
                    .map(|m| m.start()),
                other => s[byte_start..]
                    .find(&str_of(it, &other)?)
                    .map(|i| i + byte_start),
            };
            found
                .map(|b| V::Int(s[..b].chars().count() as i64))
                .unwrap_or(V::Nil)
        }
        "rindex" => {
            let p = str_of(it, &arg(a, 0))?;
            s.rfind(&p)
                .map(|b| V::Int(s[..b].chars().count() as i64))
                .unwrap_or(V::Nil)
        }
        "count" => {
            let (neg, set) = tr_expand(&str_of(it, &arg(a, 0))?);
            V::Int(s.chars().filter(|c| set.contains(c) != neg).count() as i64)
        }
        "delete" => {
            let (neg, set) = tr_expand(&str_of(it, &arg(a, 0))?);
            V::str(
                s.chars()
                    .filter(|c| set.contains(c) == neg)
                    .collect::<String>(),
            )
        }
        "squeeze" => {
            let mut out = String::new();
            let mut prev = None;
            for c in s.chars() {
                if Some(c) != prev {
                    out.push(c);
                }
                prev = Some(c);
            }
            V::str(out)
        }
        "tr" | "tr_s" => {
            let (neg, from) = tr_expand(&str_of(it, &arg(a, 0))?);
            let (_, to) = tr_expand(&str_of(it, &arg(a, 1))?);
            let mut out = String::new();
            for c in s.chars() {
                if neg {
                    if from.contains(&c) {
                        out.push(c);
                    } else if let Some(l) = to.last() {
                        out.push(*l);
                    }
                } else if let Some(i) = from.iter().position(|x| *x == c) {
                    if let Some(t) = to.get(i).or(to.last()) {
                        out.push(*t);
                    }
                } else {
                    out.push(c);
                }
            }
            V::str(out)
        }
        "reverse" => V::str(s.chars().rev().collect::<String>()),
        "succ" | "next" => V::str(str_succ(&s)),
        "ljust" | "rjust" | "center" => {
            let w = int_arg(it, a, 0)? as usize;
            let pad = match arg(a, 1) {
                V::Str(p) => p.s.borrow().clone(),
                _ => " ".into(),
            };
            let n = s.chars().count();
            if n >= w || pad.is_empty() {
                V::str(s.clone())
            } else {
                let fill = |k: usize| -> String { pad.chars().cycle().take(k).collect() };
                V::str(match name {
                    "ljust" => s.clone() + &fill(w - n),
                    "rjust" => fill(w - n) + &s,
                    _ => {
                        let l = (w - n) / 2;
                        fill(l) + &s + &fill(w - n - l)
                    }
                })
            }
        }
        "split" => {
            let limit = match arg(a, 1) {
                V::Int(l) => l,
                _ => 0,
            };
            let parts: Vec<String> = match arg(a, 0) {
                V::Nil => s.split_whitespace().map(str::to_string).collect(),
                V::Str(p) if *p.s.borrow() == " " => {
                    s.split_whitespace().map(str::to_string).collect()
                }
                V::Str(p) => {
                    let p = p.s.borrow().clone();
                    if p.is_empty() {
                        s.chars().map(|c| c.to_string()).collect()
                    } else if limit > 0 {
                        s.splitn(limit as usize, p.as_str())
                            .map(str::to_string)
                            .collect()
                    } else {
                        s.split(p.as_str()).map(str::to_string).collect()
                    }
                }
                V::Regex(re) => {
                    let mut out = vec![];
                    let mut last = 0;
                    for m in re.re.find_iter(&s) {
                        let m = m.map_err(|e| it.err("RegexpError", e.to_string()))?;
                        if m.end() == m.start() {
                            continue;
                        }
                        if limit > 0 && out.len() as i64 == limit - 1 {
                            break;
                        }
                        out.push(s[last..m.start()].to_string());
                        last = m.end();
                    }
                    out.push(s[last..].to_string());
                    out
                }
                _ => return Err(it.err("TypeError", "bad split pattern")),
            };
            let mut parts = parts;
            if limit == 0 {
                while parts.last().is_some_and(|p| p.is_empty()) {
                    parts.pop();
                }
            }
            V::arr(parts.into_iter().map(V::str).collect())
        }
        "partition" | "rpartition" => {
            let p = str_of(it, &arg(a, 0))?;
            let idx = if name == "partition" {
                s.find(&p)
            } else {
                s.rfind(&p)
            };
            match idx {
                Some(i) => V::arr(vec![
                    V::str(&s[..i]),
                    V::str(p.clone()),
                    V::str(&s[i + p.len()..]),
                ]),
                None => {
                    if name == "partition" {
                        V::arr(vec![V::str(s.clone()), V::str(""), V::str("")])
                    } else {
                        V::arr(vec![V::str(""), V::str(""), V::str(s.clone())])
                    }
                }
            }
        }
        "gsub" | "sub" => V::str(gsub_impl(it, &s, a, name == "gsub")?),
        "gsub!" | "sub!" => {
            check_frozen()?;
            let r = gsub_impl(it, &s, a, name == "gsub!")?;
            if r == s {
                V::Nil
            } else {
                *rs.s.borrow_mut() = r;
                recv.clone()
            }
        }
        "upcase!" | "downcase!" | "strip!" | "chomp!" | "squeeze!" | "capitalize!" | "lstrip!"
        | "rstrip!" | "tr!" | "delete!" | "reverse!" | "succ!" => {
            check_frozen()?;
            let base = &name[..name.len() - 1];
            let r = str_m(it, recv, rs, base, a)?.unwrap();
            let r = r.as_str().unwrap();
            if r == s {
                V::Nil
            } else {
                *rs.s.borrow_mut() = r;
                recv.clone()
            }
        }
        "<<" | "concat" => {
            check_frozen()?;
            for x in a.flat() {
                let add = match &x {
                    V::Int(i) => char::from_u32(*i as u32)
                        .map(|c| c.to_string())
                        .unwrap_or_default(),
                    other => str_of(it, other)?,
                };
                rs.s.borrow_mut().push_str(&add);
            }
            recv.clone()
        }
        "prepend" => {
            check_frozen()?;
            let p = str_of(it, &arg(a, 0))?;
            rs.s.borrow_mut().insert_str(0, &p);
            recv.clone()
        }
        "replace" => {
            check_frozen()?;
            *rs.s.borrow_mut() = str_of(it, &arg(a, 0))?;
            recv.clone()
        }
        "insert" => {
            check_frozen()?;
            let i = int_arg(it, a, 0)?;
            let p = str_of(it, &arg(a, 1))?;
            let ci = if i < 0 {
                s.chars().count() as i64 + i + 1
            } else {
                i
            } as usize;
            let bi = s.char_indices().nth(ci).map(|x| x.0).unwrap_or(s.len());
            rs.s.borrow_mut().insert_str(bi, &p);
            recv.clone()
        }
        "freeze" => {
            rs.frozen.set(true);
            recv.clone()
        }
        "frozen?" => V::Bool(rs.frozen.get()),
        "dup" | "+@" | "clone" | "itself" if name != "itself" => V::str(s.clone()),
        "-@" | "dedup" => {
            let c = V::str(s.clone());
            if let V::Str(r) = &c {
                r.frozen.set(true);
            }
            c
        }
        "=~" => {
            let re = regex_of(it, &arg(a, 0))?;
            let m = do_match(it, &re, &s, true)?;
            if m.is_nil() {
                V::Nil
            } else {
                it.call(&m, "begin", Args::new(vec![V::Int(0)]))?
            }
        }
        "match" => {
            let re = regex_of(it, &arg(a, 0))?;
            do_match(it, &re, &s, true)?
        }
        "match?" => {
            let re = regex_of(it, &arg(a, 0))?;
            V::Bool(re.re.is_match(&s).unwrap_or(false))
        }
        "scan" => {
            let re = regex_of(it, &arg(a, 0))?;
            let mut out = vec![];
            let mut pos = 0;
            while pos <= s.len() {
                let Ok(Some(c)) = re.re.captures_from_pos(&s, pos) else {
                    break;
                };
                let m = c.get(0).unwrap();
                let item = if c.len() > 1 {
                    V::arr(
                        (1..c.len())
                            .map(|i| c.get(i).map(|x| V::str(x.as_str())).unwrap_or(V::Nil))
                            .collect(),
                    )
                } else {
                    V::str(m.as_str())
                };
                if let Some(b) = &a.blk {
                    yield1(it, b, item)?;
                } else {
                    out.push(item);
                }
                pos = if m.end() == m.start() {
                    m.end() + 1
                } else {
                    m.end()
                };
            }
            if a.blk.is_some() {
                recv.clone()
            } else {
                V::arr(out)
            }
        }
        "[]" | "slice" => {
            let x = arg(a, 0);
            match (&x, arg(a, 1)) {
                (V::Int(i), V::Int(l)) => match char_index(&s, *i) {
                    Some(st) if l >= 0 => V::str(substr(&s, st, l as usize)),
                    _ => V::Nil,
                },
                (V::Int(i), _) => {
                    let n = s.chars().count() as i64;
                    let i = if *i < 0 { n + i } else { *i };
                    if i < 0 || i >= n {
                        V::Nil
                    } else {
                        V::str(substr(&s, i as usize, 1))
                    }
                }
                (V::Range(r), _) => {
                    let n = s.chars().count() as i64;
                    let st = match &r.0 {
                        V::Int(i) => *i,
                        _ => 0,
                    };
                    let en = match &r.1 {
                        V::Int(i) => *i,
                        _ => -1,
                    };
                    let st = if st < 0 { n + st } else { st };
                    let mut en = if en < 0 { n + en } else { en };
                    if r.2 && !r.1.is_nil() {
                        en -= 1;
                    }
                    if st < 0 || st > n {
                        V::Nil
                    } else {
                        V::str(substr(&s, st as usize, (en - st + 1).max(0) as usize))
                    }
                }
                (V::Str(p), _) => {
                    if s.contains(&*p.s.borrow()) {
                        V::str(p.s.borrow().clone())
                    } else {
                        V::Nil
                    }
                }
                (V::Regex(re), cap) => {
                    let m = do_match(it, re, &s, true)?;
                    if m.is_nil() {
                        V::Nil
                    } else {
                        let idx = if cap.is_nil() { V::Int(0) } else { cap };
                        it.call(&m, "[]", Args::new(vec![idx]))?
                    }
                }
                _ => V::Nil,
            }
        }
        "encoding" => V::str("UTF-8"),
        "force_encoding" | "encode" | "unicode_normalize" | "scrub" | "b" => V::str(s.clone()),
        "valid_encoding?" | "ascii_only?" if name == "valid_encoding?" => V::Bool(true),
        "ascii_only?" => V::Bool(s.is_ascii()),
        "unpack1" => {
            let f = str_of(it, &arg(a, 0))?;
            if f.starts_with('H') {
                V::str(s.bytes().map(|b| format!("{b:02x}")).collect::<String>())
            } else {
                return Err(it.err("NotImplementedError", format!("unpack1 {f}")));
            }
        }
        "casecmp?" => V::Bool(s.to_lowercase() == str_of(it, &arg(a, 0))?.to_lowercase()),
        "casecmp" => V::Int(
            match s
                .to_lowercase()
                .cmp(&str_of(it, &arg(a, 0))?.to_lowercase())
            {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            },
        ),
        "delete_prefix" => V::str(
            s.strip_prefix(str_of(it, &arg(a, 0))?.as_str())
                .unwrap_or(&s),
        ),
        "delete_suffix" => V::str(
            s.strip_suffix(str_of(it, &arg(a, 0))?.as_str())
                .unwrap_or(&s),
        ),
        "each_byte" => return Ok(None),
        "between?" => {
            let lo = str_of(it, &arg(a, 0))?;
            let hi = str_of(it, &arg(a, 1))?;
            V::Bool(s >= lo && s <= hi)
        }
        "unpack" => return Ok(None),
        "upto" => {
            let b = blk_or_err(it, a, "upto")?;
            let end = str_of(it, &arg(a, 0))?;
            let mut cur = s.clone();
            loop {
                yield1(it, b, V::str(cur.clone()))?;
                if cur == end || cur.len() > end.len() {
                    break;
                }
                cur = str_succ(&cur);
            }
            recv.clone()
        }
        "crypt" | "sum" => return Ok(None),
        _ => return Ok(None),
    };
    some(v)
}

pub fn hkey_hash(k: &HKey) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    k.hash(&mut h);
    h.finish() as i64
}

fn norm_index(i: i64, len: usize) -> Option<usize> {
    let i = if i < 0 { len as i64 + i } else { i };
    if i < 0 {
        None
    } else {
        Some(i as usize)
    }
}

fn truthy_call(it: &Rc<Interp>, b: &Rc<Proc>, v: &V) -> Result<bool, Ctrl> {
    Ok(yield1(it, b, v.clone())?.truthy())
}

fn sort_vals(it: &Rc<Interp>, items: &mut Vec<V>, b: Option<&Rc<Proc>>) -> Result<(), Ctrl> {
    let mut err = None;
    let mut keyed: Vec<V> = items.clone();
    merge_sort(&mut keyed, &mut |x, y| {
        if err.is_some() {
            return Ordering::Equal;
        }
        let r = match b {
            Some(b) => match yieldn(it, b, vec![x.clone(), y.clone()]) {
                Ok(V::Int(i)) => Ok(i.cmp(&0)),
                Ok(_) => Err(it.err("ArgumentError", "comparison failed")),
                Err(e) => Err(e),
            },
            None => rb_cmp(it, x, y),
        };
        match r {
            Ok(o) => o,
            Err(e) => {
                err = Some(e);
                Ordering::Equal
            }
        }
    });
    if let Some(e) = err {
        return Err(e);
    }
    *items = keyed;
    Ok(())
}

fn merge_sort(v: &mut Vec<V>, cmp: &mut dyn FnMut(&V, &V) -> Ordering) {
    let n = v.len();
    if n <= 1 {
        return;
    }
    let right = v.split_off(n / 2);
    let mut left = std::mem::take(v);
    let mut right = right;
    merge_sort(&mut left, cmp);
    merge_sort(&mut right, cmp);
    let mut out = Vec::with_capacity(n);
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        if cmp(&right[j], &left[i]) == Ordering::Less {
            out.push(right[j].clone());
            j += 1;
        } else {
            out.push(left[i].clone());
            i += 1;
        }
    }
    out.extend_from_slice(&left[i..]);
    out.extend_from_slice(&right[j..]);
    *v = out;
}

pub fn flatten(it: &Rc<Interp>, items: &[V], depth: i64, out: &mut Vec<V>) -> Result<(), Ctrl> {
    for x in items {
        match x {
            V::Arr(a) if depth != 0 => {
                let inner = a.borrow().clone();
                flatten(it, &inner, depth - 1, out)?;
            }
            other => out.push(other.clone()),
        }
    }
    Ok(())
}

fn uniq_by(it: &Rc<Interp>, items: Vec<V>, b: Option<&Rc<Proc>>) -> Result<Vec<V>, Ctrl> {
    let mut seen = std::collections::HashSet::new();
    let mut out = vec![];
    for x in items {
        let k = match b {
            Some(b) => yield1(it, b, x.clone())?,
            None => x.clone(),
        };
        if seen.insert(hkey(&k)) {
            out.push(x);
        }
    }
    Ok(out)
}

fn includes(it: &Rc<Interp>, items: &[V], v: &V) -> Result<bool, Ctrl> {
    for x in items {
        if rb_eq(it, x, v)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn enum_m(it: &Rc<Interp>, recv: &V, items: Vec<V>, name: &str, a: &Args) -> NR {
    let b = a.blk.as_ref();
    let v = match name {
        "to_a" | "entries" | "to_ary" => V::arr(items),
        "each" => {
            let Some(b) = b else {
                return some(V::arr(items));
            };
            for x in items {
                yield1(it, b, x)?;
            }
            recv.clone()
        }
        "map" | "collect" | "flat_map" | "collect_concat" => {
            let Some(b) = b else {
                return some(V::arr(items));
            };
            let mut out = vec![];
            for x in items {
                let r = yield1(it, b, x)?;
                if name == "flat_map" || name == "collect_concat" {
                    match r {
                        V::Arr(y) => out.extend(y.borrow().iter().cloned()),
                        other => out.push(other),
                    }
                } else {
                    out.push(r);
                }
            }
            V::arr(out)
        }
        "each_with_index" | "each_with_index_map" => {
            let b = blk_or_err(it, a, name)?;
            for (i, x) in items.into_iter().enumerate() {
                yieldn(it, b, vec![x, V::Int(i as i64)])?;
            }
            recv.clone()
        }
        "each_with_object" => {
            let b = blk_or_err(it, a, name)?;
            let memo = arg(a, 0);
            for x in items {
                yieldn(it, b, vec![x, memo.clone()])?;
            }
            memo
        }
        "inject" | "reduce" => {
            let mut iter = items.into_iter();
            let (mut acc, op) = match (arg(a, 0), arg(a, 1)) {
                (V::Sym(s), V::Nil) if b.is_none() => {
                    (iter.next().unwrap_or(V::Nil), Some(s.to_string()))
                }
                (init, V::Sym(s)) => (init, Some(s.to_string())),
                (V::Nil, _) if a.pos.is_empty() => (iter.next().unwrap_or(V::Nil), None),
                (init, _) => (init, None),
            };
            for x in iter {
                acc = match &op {
                    Some(o) => it.call(&acc, o, Args::new(vec![x]))?,
                    None => yieldn(it, b.unwrap(), vec![acc, x])?,
                };
            }
            acc
        }
        "sum" => {
            let mut acc = if a.pos.is_empty() {
                V::Int(0)
            } else {
                arg(a, 0)
            };
            for x in items {
                let x = match b {
                    Some(b) => yield1(it, b, x)?,
                    None => x,
                };
                acc = it.call(&acc, "+", Args::new(vec![x]))?;
            }
            acc
        }
        "select" | "filter" | "find_all" | "reject" => {
            let Some(b) = b else {
                return some(V::arr(items));
            };
            let keep = name != "reject";
            let mut out = vec![];
            for x in items {
                if truthy_call(it, b, &x)? == keep {
                    out.push(x);
                }
            }
            V::arr(out)
        }
        "filter_map" => {
            let b = blk_or_err(it, a, name)?;
            let mut out = vec![];
            for x in items {
                let r = yield1(it, b, x)?;
                if r.truthy() {
                    out.push(r);
                }
            }
            V::arr(out)
        }
        "partition" => {
            let b = blk_or_err(it, a, name)?;
            let (mut t, mut f) = (vec![], vec![]);
            for x in items {
                if truthy_call(it, b, &x)? {
                    t.push(x)
                } else {
                    f.push(x)
                }
            }
            V::arr(vec![V::arr(t), V::arr(f)])
        }
        "find" | "detect" => {
            let b = blk_or_err(it, a, name)?;
            for x in items {
                if truthy_call(it, b, &x)? {
                    return some(x);
                }
            }
            V::Nil
        }
        "find_index" | "index" => {
            for (i, x) in items.iter().enumerate() {
                let hit = match b {
                    Some(b) => truthy_call(it, b, x)?,
                    None => rb_eq(it, x, &arg(a, 0))?,
                };
                if hit {
                    return some(V::Int(i as i64));
                }
            }
            V::Nil
        }
        "any?" | "all?" | "none?" | "one?" => {
            let mut count = 0;
            let total = items.len();
            for x in &items {
                let t = match (b, a.pos.first()) {
                    (Some(b), _) => truthy_call(it, b, x)?,
                    (None, Some(p)) => it.case_eq(p, Some(x))?,
                    (None, None) => x.truthy(),
                };
                if t {
                    count += 1;
                    if name == "any?" {
                        return some(V::Bool(true));
                    }
                } else if name == "all?" {
                    return some(V::Bool(false));
                }
            }
            V::Bool(match name {
                "any?" => false,
                "all?" => count == total || total == 0 || count == total,
                "none?" => count == 0,
                _ => count == 1,
            })
        }
        "count" => {
            if let Some(b) = b {
                let mut n = 0;
                for x in &items {
                    if truthy_call(it, b, x)? {
                        n += 1;
                    }
                }
                V::Int(n)
            } else if !a.pos.is_empty() {
                let mut n = 0;
                for x in &items {
                    if rb_eq(it, x, &a.pos[0])? {
                        n += 1;
                    }
                }
                V::Int(n)
            } else {
                V::Int(items.len() as i64)
            }
        }
        "include?" | "member?" => V::Bool(includes(it, &items, &arg(a, 0))?),
        "sort" => {
            let mut v = items;
            sort_vals(it, &mut v, b)?;
            V::arr(v)
        }
        "sort_by" | "min_by" | "max_by" => {
            let b = blk_or_err(it, a, name)?;
            let mut pairs = vec![];
            for x in items {
                let k = yield1(it, b, x.clone())?;
                pairs.push(V::arr(vec![k, x]));
            }
            let mut err = None;
            merge_sort(&mut pairs, &mut |p, q| {
                let (V::Arr(p), V::Arr(q)) = (p, q) else {
                    return Ordering::Equal;
                };
                match rb_cmp(it, &p.borrow()[0], &q.borrow()[0]) {
                    Ok(o) => o,
                    Err(e) => {
                        err = Some(e);
                        Ordering::Equal
                    }
                }
            });
            if let Some(e) = err {
                return Err(e);
            }
            let sorted: Vec<V> = pairs
                .into_iter()
                .map(|p| {
                    if let V::Arr(p) = p {
                        p.borrow()[1].clone()
                    } else {
                        V::Nil
                    }
                })
                .collect();
            match name {
                "sort_by" => V::arr(sorted),
                "min_by" => sorted.first().cloned().unwrap_or(V::Nil),
                _ => sorted.last().cloned().unwrap_or(V::Nil),
            }
        }
        "min" | "max" => {
            let mut v = items;
            sort_vals(it, &mut v, b)?;
            if let V::Int(n) = arg(a, 0) {
                let n = n as usize;
                if name == "min" {
                    V::arr(v.into_iter().take(n).collect())
                } else {
                    V::arr(v.into_iter().rev().take(n).collect())
                }
            } else if name == "min" {
                v.first().cloned().unwrap_or(V::Nil)
            } else {
                v.last().cloned().unwrap_or(V::Nil)
            }
        }
        "minmax" => {
            let mut v = items;
            sort_vals(it, &mut v, b)?;
            V::arr(vec![
                v.first().cloned().unwrap_or(V::Nil),
                v.last().cloned().unwrap_or(V::Nil),
            ])
        }
        "group_by" | "chunk_while_unused" => {
            let b = blk_or_err(it, a, name)?;
            let h = Rc::new(RHash::default());
            for x in items {
                let k = yield1(it, b, x.clone())?;
                match h.get(&k) {
                    Some(V::Arr(l)) => l.borrow_mut().push(x),
                    _ => h.insert(k, V::arr(vec![x])),
                }
            }
            V::Hash(h)
        }
        "tally" => {
            let h = Rc::new(RHash::default());
            for x in items {
                let n = match h.get(&x) {
                    Some(V::Int(n)) => n + 1,
                    _ => 1,
                };
                h.insert(x, V::Int(n));
            }
            V::Hash(h)
        }
        "uniq" => V::arr(uniq_by(it, items, b)?),
        "to_h" => {
            let h = Rc::new(RHash::default());
            for x in items {
                let pair = match b {
                    Some(b) => yield1(it, b, x)?,
                    None => x,
                };
                match pair {
                    V::Arr(p) => {
                        let p = p.borrow();
                        h.insert(
                            p.first().cloned().unwrap_or(V::Nil),
                            p.get(1).cloned().unwrap_or(V::Nil),
                        );
                    }
                    other => {
                        return Err(it.err(
                            "TypeError",
                            format!(
                                "wrong element type {} (expected array)",
                                it.class_of(&other).display_name()
                            ),
                        ))
                    }
                }
            }
            V::Hash(h)
        }
        "to_set" => {
            let set = it.lookup_const_path("Set").unwrap();
            it.call(
                &V::Mod(set),
                "new",
                Args::with_blk(vec![V::arr(items)], a.blk.clone()),
            )?
        }
        "zip" => {
            let others: Vec<Vec<V>> = a
                .pos
                .iter()
                .map(|o| to_list(it, o))
                .collect::<Result<_, _>>()?;
            V::arr(
                items
                    .into_iter()
                    .enumerate()
                    .map(|(i, x)| {
                        let mut row = vec![x];
                        for o in &others {
                            row.push(o.get(i).cloned().unwrap_or(V::Nil));
                        }
                        V::arr(row)
                    })
                    .collect(),
            )
        }
        "each_slice" | "each_cons" => {
            let n = int_arg(it, a, 0)?.max(1) as usize;
            let chunks: Vec<V> = if name == "each_slice" {
                items.chunks(n).map(|c| V::arr(c.to_vec())).collect()
            } else if items.len() >= n {
                items.windows(n).map(|c| V::arr(c.to_vec())).collect()
            } else {
                vec![]
            };
            match b {
                Some(b) => {
                    for c in chunks {
                        yield1(it, b, c)?;
                    }
                    recv.clone()
                }
                None => V::arr(chunks),
            }
        }
        "first" => match arg(a, 0) {
            V::Int(n) => V::arr(items.into_iter().take(n as usize).collect()),
            _ => items.first().cloned().unwrap_or(V::Nil),
        },
        "take" => V::arr(
            items
                .into_iter()
                .take(int_arg(it, a, 0)? as usize)
                .collect(),
        ),
        "drop" => V::arr(
            items
                .into_iter()
                .skip(int_arg(it, a, 0)? as usize)
                .collect(),
        ),
        "take_while" | "drop_while" => {
            let b = blk_or_err(it, a, name)?;
            let mut idx = items.len();
            for (i, x) in items.iter().enumerate() {
                if !truthy_call(it, b, x)? {
                    idx = i;
                    break;
                }
            }
            if name == "take_while" {
                V::arr(items[..idx].to_vec())
            } else {
                V::arr(items[idx..].to_vec())
            }
        }
        "chunk_while" | "slice_when" => {
            let b = blk_or_err(it, a, name)?;
            let mut out: Vec<Vec<V>> = vec![];
            for x in items {
                if let Some(last) = out.last_mut() {
                    let prev = last.last().unwrap().clone();
                    let t = yieldn(it, b, vec![prev, x.clone()])?.truthy();
                    if t == (name == "chunk_while") {
                        last.push(x);
                        continue;
                    }
                }
                out.push(vec![x]);
            }
            V::arr(out.into_iter().map(V::arr).collect())
        }
        "lazy" | "each_entry" | "to_enum" | "each_entry_unused" => V::arr(items),
        "empty?" => V::Bool(items.is_empty()),
        "size" | "length" => V::Int(items.len() as i64),
        "reverse_each" => {
            let b = blk_or_err(it, a, name)?;
            for x in items.into_iter().rev() {
                yield1(it, b, x)?;
            }
            recv.clone()
        }
        "cycle" => return Ok(None),
        _ => return Ok(None),
    };
    some(v)
}

pub fn arr_m(it: &Rc<Interp>, recv: &V, ar: &Rc<RefCell<Vec<V>>>, name: &str, a: &Args) -> NR {
    let items = ar.borrow().clone();
    let len = items.len();
    let b = a.blk.as_ref();
    let v = match name {
        "length" | "size" => V::Int(len as i64),
        "empty?" => V::Bool(len == 0),
        "to_a" | "entries" | "to_ary" => recv.clone(),
        "inspect" | "to_s" => V::str(rb_inspect(it, recv)?),
        "each" => {
            let Some(b) = b else {
                return some(recv.clone());
            };
            let mut i = 0;
            loop {
                let x = {
                    let v = ar.borrow();
                    if i >= v.len() {
                        break;
                    }
                    v[i].clone()
                };
                yield1(it, b, x)?;
                i += 1;
            }
            recv.clone()
        }
        "each_index" => {
            let b = blk_or_err(it, a, name)?;
            for i in 0..len {
                yield1(it, b, V::Int(i as i64))?;
            }
            recv.clone()
        }
        "first" if a.pos.is_empty() => items.first().cloned().unwrap_or(V::Nil),
        "last" => match arg(a, 0) {
            V::Int(n) => V::arr(items[len.saturating_sub(n as usize)..].to_vec()),
            _ => items.last().cloned().unwrap_or(V::Nil),
        },
        "push" | "append" | "<<" => {
            ar.borrow_mut().extend(a.flat());
            recv.clone()
        }
        "pop" => ar.borrow_mut().pop().unwrap_or(V::Nil),
        "shift" => {
            if ar.borrow().is_empty() {
                V::Nil
            } else {
                ar.borrow_mut().remove(0)
            }
        }
        "unshift" | "prepend" => {
            let mut v = a.flat();
            v.extend(items);
            *ar.borrow_mut() = v;
            recv.clone()
        }
        "insert" => {
            let i = int_arg(it, a, 0)?;
            let idx = if i < 0 {
                (len as i64 + i + 1) as usize
            } else {
                i as usize
            };
            let mut v = items;
            for (k, x) in a.flat().into_iter().skip(1).enumerate() {
                v.insert((idx + k).min(v.len()), x);
            }
            *ar.borrow_mut() = v;
            recv.clone()
        }
        "concat" => {
            for o in a.flat() {
                ar.borrow_mut().extend(to_list(it, &o)?);
            }
            recv.clone()
        }
        "+" => {
            let o = to_list(it, &arg(a, 0))?;
            let mut v = items;
            v.extend(o);
            V::arr(v)
        }
        "-" | "difference" => {
            let o = to_list(it, &arg(a, 0))?;
            let keys: std::collections::HashSet<HKey> = o.iter().map(hkey).collect();
            V::arr(
                items
                    .into_iter()
                    .filter(|x| !keys.contains(&hkey(x)))
                    .collect(),
            )
        }
        "&" | "intersection" => {
            let o = to_list(it, &arg(a, 0))?;
            let keys: std::collections::HashSet<HKey> = o.iter().map(hkey).collect();
            V::arr(uniq_by(
                it,
                items
                    .into_iter()
                    .filter(|x| keys.contains(&hkey(x)))
                    .collect(),
                None,
            )?)
        }
        "|" | "union" => {
            let mut v = items;
            for o in a.flat() {
                v.extend(to_list(it, &o)?);
            }
            V::arr(uniq_by(it, v, None)?)
        }
        "intersect?" => {
            let o = to_list(it, &arg(a, 0))?;
            let keys: std::collections::HashSet<HKey> = o.iter().map(hkey).collect();
            V::Bool(items.iter().any(|x| keys.contains(&hkey(x))))
        }
        "*" => match arg(a, 0) {
            V::Str(sep) => {
                let sep = sep.s.borrow().clone();
                let mut parts = vec![];
                for x in &items {
                    parts.push(it.to_s(x)?);
                }
                V::str(parts.join(&sep))
            }
            V::Int(n) => V::arr(
                items
                    .iter()
                    .cloned()
                    .cycle()
                    .take(len * n.max(0) as usize)
                    .collect(),
            ),
            _ => return Err(it.err("TypeError", "bad Array#*")),
        },
        "==" | "eql?" => V::Bool(rb_eq(it, recv, &arg(a, 0))?),
        "!=" => V::Bool(!rb_eq(it, recv, &arg(a, 0))?),
        "<=>" => V::Int(match rb_cmp(it, recv, &arg(a, 0))? {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }),
        "hash" => V::Int(hkey_hash(&hkey(recv))),
        "[]" | "slice" => {
            let x = arg(a, 0);
            match (&x, arg(a, 1)) {
                (V::Int(i), V::Int(l)) => match norm_index(*i, len) {
                    Some(st) if st <= len && l >= 0 => {
                        V::arr(items.into_iter().skip(st).take(l as usize).collect())
                    }
                    _ => V::Nil,
                },
                (V::Int(i), _) => norm_index(*i, len)
                    .and_then(|i| items.get(i).cloned())
                    .unwrap_or(V::Nil),
                (V::Range(r), _) => {
                    let st = match &r.0 {
                        V::Int(i) => *i,
                        _ => 0,
                    };
                    let en = match &r.1 {
                        V::Int(i) => *i,
                        _ => -1,
                    };
                    let st = if st < 0 { len as i64 + st } else { st };
                    let mut en = if en < 0 { len as i64 + en } else { en };
                    if r.2 && !r.1.is_nil() {
                        en -= 1;
                    }
                    if st < 0 || st as usize > len {
                        V::Nil
                    } else {
                        V::arr(
                            items
                                .into_iter()
                                .skip(st as usize)
                                .take((en - st + 1).max(0) as usize)
                                .collect(),
                        )
                    }
                }
                _ => return Err(it.err("TypeError", "no implicit conversion into Integer")),
            }
        }
        "[]=" => {
            let i = int_arg(it, a, 0)?;
            let v = arg(a, 1);
            let idx =
                norm_index(i, len).ok_or_else(|| it.err("IndexError", "index out of array"))?;
            let mut m = ar.borrow_mut();
            while m.len() <= idx {
                m.push(V::Nil);
            }
            m[idx] = v.clone();
            v
        }
        "at" => norm_index(int_arg(it, a, 0)?, len)
            .and_then(|i| items.get(i).cloned())
            .unwrap_or(V::Nil),
        "fetch" => {
            let i = int_arg(it, a, 0)?;
            match norm_index(i, len).and_then(|i| items.get(i).cloned()) {
                Some(v) => v,
                None => {
                    if let Some(b) = b {
                        yield1(it, b, V::Int(i))?
                    } else if a.pos.len() > 1 {
                        arg(a, 1)
                    } else {
                        return Err(it.err(
                            "IndexError",
                            format!(
                                "index {i} outside of array bounds: {}...{}",
                                -(len as i64),
                                len
                            ),
                        ));
                    }
                }
            }
        }
        "dig" => {
            let mut cur = recv.clone();
            for k in a.flat() {
                if cur.is_nil() {
                    break;
                }
                cur = it.call(&cur, "[]", Args::new(vec![k]))?;
            }
            cur
        }
        "values_at" => V::arr(
            a.flat()
                .iter()
                .map(|i| {
                    if let V::Int(i) = i {
                        norm_index(*i, len)
                            .and_then(|i| items.get(i).cloned())
                            .unwrap_or(V::Nil)
                    } else {
                        V::Nil
                    }
                })
                .collect(),
        ),
        "join" => {
            let sep = match arg(a, 0) {
                V::Str(s) => s.s.borrow().clone(),
                _ => String::new(),
            };
            let mut flat = vec![];
            flatten(it, &items, -1, &mut flat)?;
            let mut parts = vec![];
            for x in &flat {
                parts.push(it.to_s(x)?);
            }
            V::str(parts.join(&sep))
        }
        "reverse" => V::arr(items.into_iter().rev().collect()),
        "rotate" => {
            let n = match arg(a, 0) {
                V::Int(n) => n,
                _ => 1,
            };
            if len == 0 {
                V::arr(vec![])
            } else {
                let k = n.rem_euclid(len as i64) as usize;
                let mut v = items;
                v.rotate_left(k);
                V::arr(v)
            }
        }
        "compact" => V::arr(items.into_iter().filter(|x| !x.is_nil()).collect()),
        "flatten" => {
            let d = match arg(a, 0) {
                V::Int(d) => d,
                _ => -1,
            };
            let mut out = vec![];
            flatten(it, &items, d, &mut out)?;
            V::arr(out)
        }
        "uniq!" | "compact!" | "flatten!" | "sort!" | "map!" | "select!" | "reject!"
        | "collect!" | "filter!" | "reverse!" | "shuffle!" | "sort_by!" => {
            let base = &name[..name.len() - 1];
            let r = arr_m(it, recv, ar, base, a)?.unwrap();
            let nv = to_list(it, &r)?;
            let changed = nv.len() != len;
            *ar.borrow_mut() = nv;
            if matches!(
                name,
                "uniq!" | "compact!" | "select!" | "reject!" | "flatten!"
            ) && !changed
            {
                V::Nil
            } else {
                recv.clone()
            }
        }
        "delete" => {
            let t = arg(a, 0);
            let mut found = V::Nil;
            let mut keep = vec![];
            for x in items {
                if rb_eq(it, &x, &t)? {
                    found = x;
                } else {
                    keep.push(x);
                }
            }
            *ar.borrow_mut() = keep;
            found
        }
        "delete_at" => {
            let i = int_arg(it, a, 0)?;
            match norm_index(i, len) {
                Some(i) if i < len => ar.borrow_mut().remove(i),
                _ => V::Nil,
            }
        }
        "delete_if" | "keep_if" => {
            let b = blk_or_err(it, a, name)?;
            let mut keep = vec![];
            for x in items {
                if truthy_call(it, b, &x)? == (name == "keep_if") {
                    keep.push(x);
                }
            }
            *ar.borrow_mut() = keep;
            recv.clone()
        }
        "clear" => {
            ar.borrow_mut().clear();
            recv.clone()
        }
        "replace" => {
            *ar.borrow_mut() = to_list(it, &arg(a, 0))?;
            recv.clone()
        }
        "fill" => {
            let v = arg(a, 0);
            *ar.borrow_mut() = vec![v; len];
            recv.clone()
        }
        "freeze" | "itself" => recv.clone(),
        "frozen?" => V::Bool(false),
        "dup" | "clone" => V::arr(items),
        "sample" | "shuffle" => {
            return Err(it.err(
                "NotImplementedError",
                format!("Array#{name} is nondeterministic and refused"),
            ))
        }
        "product" => {
            let mut acc: Vec<Vec<V>> = items.into_iter().map(|x| vec![x]).collect();
            for o in a.flat() {
                let ol = to_list(it, &o)?;
                let mut next = vec![];
                for row in &acc {
                    for y in &ol {
                        let mut r = row.clone();
                        r.push(y.clone());
                        next.push(r);
                    }
                }
                acc = next;
            }
            V::arr(acc.into_iter().map(V::arr).collect())
        }
        "transpose" => {
            let rows: Vec<Vec<V>> = items
                .iter()
                .map(|r| to_list(it, r))
                .collect::<Result<_, _>>()?;
            let w = rows.first().map(|r| r.len()).unwrap_or(0);
            V::arr(
                (0..w)
                    .map(|j| {
                        V::arr(
                            rows.iter()
                                .map(|r| r.get(j).cloned().unwrap_or(V::Nil))
                                .collect(),
                        )
                    })
                    .collect(),
            )
        }
        "combination" => {
            let k = int_arg(it, a, 0)? as usize;
            let mut out = vec![];
            fn rec(items: &[V], k: usize, start: usize, cur: &mut Vec<V>, out: &mut Vec<V>) {
                if cur.len() == k {
                    out.push(V::arr(cur.clone()));
                    return;
                }
                for i in start..items.len() {
                    cur.push(items[i].clone());
                    rec(items, k, i + 1, cur, out);
                    cur.pop();
                }
            }
            rec(&items, k, 0, &mut vec![], &mut out);
            V::arr(out)
        }
        "assoc" => {
            let t = arg(a, 0);
            let mut r = V::Nil;
            for x in items {
                if let V::Arr(p) = &x {
                    if let Some(f) = p.borrow().first() {
                        if rb_eq(it, f, &t)? {
                            r = x.clone();
                            break;
                        }
                    }
                }
            }
            r
        }
        "pack" => return Err(it.err("NotImplementedError", "Array#pack")),
        "max" | "min" | "sum"
            if b.is_none() && a.pos.is_empty() && items.iter().all(|x| matches!(x, V::Int(_))) =>
        {
            let ints: Vec<i64> = items
                .iter()
                .map(|x| if let V::Int(i) = x { *i } else { 0 })
                .collect();
            match name {
                "max" => ints.iter().max().map(|x| V::Int(*x)).unwrap_or(V::Nil),
                "min" => ints.iter().min().map(|x| V::Int(*x)).unwrap_or(V::Nil),
                _ => V::Int(ints.iter().sum()),
            }
        }
        "to_h" if b.is_none() => {
            let h = Rc::new(RHash::default());
            for x in items {
                if let V::Arr(p) = x {
                    let p = p.borrow();
                    h.insert(
                        p.first().cloned().unwrap_or(V::Nil),
                        p.get(1).cloned().unwrap_or(V::Nil),
                    );
                } else {
                    return Err(it.err("TypeError", "wrong element type (expected array)"));
                }
            }
            V::Hash(h)
        }
        "each_with_index" if b.is_none() => V::arr(
            items
                .into_iter()
                .enumerate()
                .map(|(i, x)| V::arr(vec![x, V::Int(i as i64)]))
                .collect(),
        ),
        "map" | "collect" if b.is_none() => recv.clone(),
        "step" => return Ok(None),
        "with_index" | "each_with_index_unused" => return Ok(None),
        _ => return enum_m(it, recv, items, name, a),
    };
    some(v)
}

pub fn hash_m(it: &Rc<Interp>, recv: &V, h: &Rc<RHash>, name: &str, a: &Args) -> NR {
    let b = a.blk.as_ref();
    let pairs = || h.entries();
    let pair_args = |k: V, v: V| vec![k, v];
    let check_frozen = || -> Result<(), Ctrl> {
        if h.frozen.get() {
            return Err(it.err(
                "FrozenError",
                format!("can't modify frozen Hash: {}", inspect_plain(recv)),
            ));
        }
        Ok(())
    };
    let new_h = |es: Vec<(V, V)>| new_hash_from(es);
    let v = match name {
        "length" | "size" | "count" if b.is_none() && a.pos.is_empty() => V::Int(h.len() as i64),
        "empty?" => V::Bool(h.len() == 0),
        "inspect" | "to_s" => V::str(rb_inspect(it, recv)?),
        "to_h" | "to_hash" if b.is_none() => recv.clone(),
        "to_h" => {
            let out = Rc::new(RHash::default());
            for (k, v) in pairs() {
                match yieldn(it, b.unwrap(), pair_args(k, v))? {
                    V::Arr(p) => {
                        let p = p.borrow();
                        out.insert(p[0].clone(), p.get(1).cloned().unwrap_or(V::Nil));
                    }
                    _ => return Err(it.err("TypeError", "wrong element type (expected array)")),
                }
            }
            V::Hash(out)
        }
        "[]" => {
            let k = arg(a, 0);
            match h.get(&k) {
                Some(v) => v,
                None => {
                    let dp = h.default_proc.borrow().clone();
                    match dp {
                        Some(p) => it.call_proc(&p, vec![recv.clone(), k], None)?,
                        None => h.default.borrow().clone(),
                    }
                }
            }
        }
        "[]=" | "store" => {
            check_frozen()?;
            let v = arg(a, 1);
            h.insert(arg(a, 0), v.clone());
            v
        }
        "fetch" => {
            let k = arg(a, 0);
            match h.get(&k) {
                Some(v) => v,
                None => {
                    if let Some(b) = b {
                        yield1(it, b, k)?
                    } else if a.flat().len() > 1 {
                        arg(a, 1)
                    } else {
                        return Err(it.err(
                            "KeyError",
                            format!("key not found: {}", rb_inspect(it, &k)?),
                        ));
                    }
                }
            }
        }
        "key?" | "has_key?" | "include?" | "member?" => V::Bool(h.get(&arg(a, 0)).is_some()),
        "value?" | "has_value?" => {
            let t = arg(a, 0);
            let mut r = false;
            for (_, v) in pairs() {
                if rb_eq(it, &v, &t)? {
                    r = true;
                    break;
                }
            }
            V::Bool(r)
        }
        "key" => {
            let t = arg(a, 0);
            let mut r = V::Nil;
            for (k, v) in pairs() {
                if rb_eq(it, &v, &t)? {
                    r = k;
                    break;
                }
            }
            r
        }
        "keys" => V::arr(pairs().into_iter().map(|(k, _)| k).collect()),
        "values" => V::arr(pairs().into_iter().map(|(_, v)| v).collect()),
        "values_at" => V::arr(
            a.flat()
                .iter()
                .map(|k| h.get(k).unwrap_or(V::Nil))
                .collect(),
        ),
        "fetch_values" => {
            let mut out = vec![];
            for k in a.flat() {
                match h.get(&k) {
                    Some(v) => out.push(v),
                    None => {
                        return Err(it.err(
                            "KeyError",
                            format!("key not found: {}", rb_inspect(it, &k)?),
                        ))
                    }
                }
            }
            V::arr(out)
        }
        "dig" => {
            let mut cur = recv.clone();
            for k in a.flat() {
                if cur.is_nil() {
                    break;
                }
                cur = it.call(&cur, "[]", Args::new(vec![k]))?;
            }
            cur
        }
        "delete" => {
            check_frozen()?;
            let k = arg(a, 0);
            let r = h.map.borrow_mut().shift_remove(&hkey(&k)).map(|(_, v)| v);
            match r {
                Some(v) => v,
                None => match b {
                    Some(b) => yield1(it, b, k)?,
                    None => V::Nil,
                },
            }
        }
        "each" | "each_pair" => {
            let Some(b) = b else {
                return some(V::arr(
                    pairs()
                        .into_iter()
                        .map(|(k, v)| V::arr(vec![k, v]))
                        .collect(),
                ));
            };
            for (k, v) in pairs() {
                yield1(it, b, V::arr(vec![k, v]))?;
            }
            recv.clone()
        }
        "each_key" => {
            let b = blk_or_err(it, a, name)?;
            for (k, _) in pairs() {
                yield1(it, b, k)?;
            }
            recv.clone()
        }
        "each_value" => {
            let b = blk_or_err(it, a, name)?;
            for (_, v) in pairs() {
                yield1(it, b, v)?;
            }
            recv.clone()
        }
        "map" | "collect" | "flat_map" => {
            let b = blk_or_err(it, a, name)?;
            let mut out = vec![];
            for (k, v) in pairs() {
                let r = yield1(it, b, V::arr(vec![k, v]))?;
                if name == "flat_map" {
                    match r {
                        V::Arr(x) => out.extend(x.borrow().iter().cloned()),
                        o => out.push(o),
                    }
                } else {
                    out.push(r);
                }
            }
            V::arr(out)
        }
        "select" | "filter" | "reject" => {
            let b = blk_or_err(it, a, name)?;
            let mut out = vec![];
            for (k, v) in pairs() {
                let t = yieldn(it, b, pair_args(k.clone(), v.clone()))?.truthy();
                if t == (name != "reject") {
                    out.push((k, v));
                }
            }
            new_h(out)
        }
        "select!" | "keep_if" | "reject!" | "delete_if" | "filter!" => {
            check_frozen()?;
            let b = blk_or_err(it, a, name)?;
            let keep_when = matches!(name, "select!" | "keep_if" | "filter!");
            let mut out = vec![];
            for (k, v) in pairs() {
                if yieldn(it, b, pair_args(k.clone(), v.clone()))?.truthy() == keep_when {
                    out.push((k, v));
                }
            }
            let mut m = indexmap::IndexMap::new();
            for (k, v) in out {
                m.insert(hkey(&k), (k, v));
            }
            *h.map.borrow_mut() = m;
            recv.clone()
        }
        "filter_map" => {
            let b = blk_or_err(it, a, name)?;
            let mut out = vec![];
            for (k, v) in pairs() {
                let r = yieldn(it, b, pair_args(k, v))?;
                if r.truthy() {
                    out.push(r);
                }
            }
            V::arr(out)
        }
        "find" | "detect" => {
            let b = blk_or_err(it, a, name)?;
            for (k, v) in pairs() {
                if yieldn(it, b, pair_args(k.clone(), v.clone()))?.truthy() {
                    return some(V::arr(vec![k, v]));
                }
            }
            V::Nil
        }
        "any?" | "all?" | "none?" | "count" if b.is_some() => {
            let b = b.unwrap();
            let mut n = 0;
            let total = h.len();
            for (k, v) in pairs() {
                if yieldn(it, b, pair_args(k, v))?.truthy() {
                    n += 1;
                }
            }
            match name {
                "any?" => V::Bool(n > 0),
                "all?" => V::Bool(n == total),
                "none?" => V::Bool(n == 0),
                _ => V::Int(n as i64),
            }
        }
        "any?" => V::Bool(h.len() > 0),
        "partition" => {
            let b = blk_or_err(it, a, name)?;
            let (mut t, mut f) = (vec![], vec![]);
            for (k, v) in pairs() {
                let p = V::arr(vec![k.clone(), v.clone()]);
                if yieldn(it, b, pair_args(k, v))?.truthy() {
                    t.push(p)
                } else {
                    f.push(p)
                }
            }
            V::arr(vec![V::arr(t), V::arr(f)])
        }
        "each_with_object" => {
            let b = blk_or_err(it, a, name)?;
            let memo = arg(a, 0);
            for (k, v) in pairs() {
                yieldn(it, b, vec![V::arr(vec![k, v]), memo.clone()])?;
            }
            memo
        }
        "merge" | "merge!" | "update" => {
            if name != "merge" {
                check_frozen()?;
            }
            let target = if name == "merge" {
                let n = Rc::new(RHash::default());
                for (k, v) in pairs() {
                    n.insert(k, v);
                }
                *n.default.borrow_mut() = h.default.borrow().clone();
                *n.default_proc.borrow_mut() = h.default_proc.borrow().clone();
                n
            } else {
                h.clone()
            };
            for o in a.flat() {
                let oh = match &o {
                    V::Hash(x) => x.clone(),
                    other => match it.call(other, "to_h", Args::default())? {
                        V::Hash(x) => x,
                        _ => return Err(it.err("TypeError", "no implicit conversion into Hash")),
                    },
                };
                for (k, v) in oh.entries() {
                    let nv = match (b, target.get(&k)) {
                        (Some(b), Some(old)) => yieldn(it, b, vec![k.clone(), old, v])?,
                        _ => v,
                    };
                    target.insert(k, nv);
                }
            }
            V::Hash(target)
        }
        "transform_keys" | "transform_keys!" => {
            let mapping = match arg(a, 0) {
                V::Hash(m) => Some(m),
                _ => None,
            };
            let mut out = vec![];
            for (k, v) in pairs() {
                let nk = if let Some(m) = &mapping {
                    m.get(&k).unwrap_or(k.clone())
                } else {
                    yield1(it, blk_or_err(it, a, name)?, k)?
                };
                out.push((nk, v));
            }
            if name.ends_with('!') {
                check_frozen()?;
                h.map.borrow_mut().clear();
                for (k, v) in out {
                    h.insert(k, v);
                }
                recv.clone()
            } else {
                new_h(out)
            }
        }
        "transform_values" | "transform_values!" => {
            let b = blk_or_err(it, a, name)?;
            let mut out = vec![];
            for (k, v) in pairs() {
                out.push((k, yield1(it, b, v)?));
            }
            if name.ends_with('!') {
                check_frozen()?;
                for (k, v) in out {
                    h.insert(k, v);
                }
                recv.clone()
            } else {
                new_h(out)
            }
        }
        "slice" => {
            let mut out = vec![];
            for k in a.flat() {
                if let Some(v) = h.get(&k) {
                    out.push((k, v));
                }
            }
            new_h(out)
        }
        "except" => {
            let ks: std::collections::HashSet<HKey> = a.flat().iter().map(hkey).collect();
            new_h(
                pairs()
                    .into_iter()
                    .filter(|(k, _)| !ks.contains(&hkey(k)))
                    .collect(),
            )
        }
        "compact" => new_h(pairs().into_iter().filter(|(_, v)| !v.is_nil()).collect()),
        "invert" => new_h(pairs().into_iter().map(|(k, v)| (v, k)).collect()),
        "default" => h.default.borrow().clone(),
        "default=" => {
            *h.default.borrow_mut() = arg(a, 0);
            arg(a, 0)
        }
        "freeze" => {
            h.frozen.set(true);
            recv.clone()
        }
        "frozen?" => V::Bool(h.frozen.get()),
        "dup" | "clone" => {
            let n = Rc::new(RHash::default());
            for (k, v) in pairs() {
                n.insert(k, v);
            }
            *n.default.borrow_mut() = h.default.borrow().clone();
            *n.default_proc.borrow_mut() = h.default_proc.borrow().clone();
            if name == "clone" {
                n.frozen.set(h.frozen.get());
            }
            V::Hash(n)
        }
        "clear" => {
            check_frozen()?;
            h.map.borrow_mut().clear();
            recv.clone()
        }
        "replace" => {
            check_frozen()?;
            h.map.borrow_mut().clear();
            if let V::Hash(o) = arg(a, 0) {
                for (k, v) in o.entries() {
                    h.insert(k, v);
                }
            }
            recv.clone()
        }
        "==" | "eql?" => V::Bool(rb_eq(it, recv, &arg(a, 0))?),
        "!=" => V::Bool(!rb_eq(it, recv, &arg(a, 0))?),
        "hash" => V::Int(hkey_hash(&hkey(recv))),
        "compare_by_identity" => {
            h.compare_by_identity.set(true);
            recv.clone()
        }
        "sort_by" | "min_by" | "max_by" | "group_by" | "sum" | "inject" | "reduce" | "sort"
        | "to_a" | "entries" | "first" | "each_with_index" | "zip" | "uniq" | "min" | "max"
        | "tally" | "to_set" | "each_slice" | "find_all" | "count" | "none?" | "all?" | "one?"
        | "take" | "drop" | "include_unused" => {
            let items: Vec<V> = pairs()
                .into_iter()
                .map(|(k, v)| V::arr(vec![k, v]))
                .collect();
            return enum_m(it, recv, items, name, a);
        }
        "shift" => {
            check_frozen()?;
            let r = h
                .map
                .borrow_mut()
                .shift_remove_index(0)
                .map(|(_, (k, v))| V::arr(vec![k, v]));
            r.unwrap_or(V::Nil)
        }
        "itself" => recv.clone(),
        _ => return Ok(None),
    };
    some(v)
}

fn num_f(v: &V) -> f64 {
    match v {
        V::Int(i) => *i as f64,
        V::Float(f) => *f,
        _ => 0.0,
    }
}

pub fn num_m(it: &Rc<Interp>, recv: &V, name: &str, a: &Args) -> NR {
    let o = arg(a, 0);
    let both_int = matches!((recv, &o), (V::Int(_), V::Int(_)));
    let x = num_f(recv);
    let is_num = matches!(o, V::Int(_) | V::Float(_));
    let arith_err = |op: &str| -> Ctrl {
        it.err(
            "TypeError",
            format!(
                "{} can't be coerced into {} ({op})",
                if o.is_nil() {
                    "nil".to_string()
                } else {
                    it.class_of(&o).display_name()
                },
                it.class_of(recv).display_name()
            ),
        )
    };
    let v = match name {
        "+" | "-" | "*" => {
            if !is_num {
                return Err(arith_err(name));
            }
            if let (V::Int(p), V::Int(q)) = (recv, &o) {
                let r = match name {
                    "+" => p.checked_add(*q),
                    "-" => p.checked_sub(*q),
                    _ => p.checked_mul(*q),
                };
                V::Int(r.ok_or_else(|| it.err("RangeError", "integer overflow"))?)
            } else {
                let y = num_f(&o);
                V::Float(match name {
                    "+" => x + y,
                    "-" => x - y,
                    _ => x * y,
                })
            }
        }
        "/" | "div" => {
            if !is_num {
                return Err(arith_err(name));
            }
            if let (V::Int(p), V::Int(q)) = (recv, &o) {
                if *q == 0 {
                    return Err(it.err("ZeroDivisionError", "divided by 0"));
                }
                let d = p / q;
                V::Int(if p % q != 0 && ((*p < 0) != (*q < 0)) {
                    d - 1
                } else {
                    d
                })
            } else if name == "div" {
                V::Int((x / num_f(&o)).floor() as i64)
            } else {
                V::Float(x / num_f(&o))
            }
        }
        "%" | "modulo" => {
            if let (V::Int(p), V::Int(q)) = (recv, &o) {
                if *q == 0 {
                    return Err(it.err("ZeroDivisionError", "divided by 0"));
                }
                V::Int(((p % q) + q) % q)
            } else {
                let y = num_f(&o);
                V::Float(x - y * (x / y).floor())
            }
        }
        "**" | "pow" => {
            if let (V::Int(p), V::Int(q)) = (recv, &o) {
                if *q >= 0 {
                    V::Int(
                        p.checked_pow(*q as u32)
                            .ok_or_else(|| it.err("RangeError", "integer overflow"))?,
                    )
                } else {
                    V::Float((*p as f64).powf(*q as f64))
                }
            } else {
                V::Float(x.powf(num_f(&o)))
            }
        }
        "-@" => match recv {
            V::Int(i) => V::Int(-i),
            _ => V::Float(-x),
        },
        "+@" => recv.clone(),
        "==" | "===" | "eql?" | "equal?" => V::Bool(
            is_num
                && (if name == "eql?" {
                    std::mem::discriminant(recv) == std::mem::discriminant(&o)
                } else {
                    true
                })
                && x == num_f(&o),
        ),
        "!=" => V::Bool(!(is_num && x == num_f(&o))),
        "<" | ">" | "<=" | ">=" => {
            if !is_num {
                return Err(it.err(
                    "ArgumentError",
                    format!(
                        "comparison of {} with {} failed",
                        it.class_of(recv).display_name(),
                        inspect_plain(&o)
                    ),
                ));
            }
            let c = if both_int {
                if let (V::Int(p), V::Int(q)) = (recv, &o) {
                    p.cmp(q)
                } else {
                    unreachable!()
                }
            } else {
                x.partial_cmp(&num_f(&o)).unwrap_or(Ordering::Equal)
            };
            V::Bool(match name {
                "<" => c == Ordering::Less,
                ">" => c == Ordering::Greater,
                "<=" => c != Ordering::Greater,
                _ => c != Ordering::Less,
            })
        }
        "<=>" => {
            if !is_num {
                V::Nil
            } else {
                V::Int(match x.partial_cmp(&num_f(&o)).unwrap_or(Ordering::Equal) {
                    Ordering::Less => -1,
                    Ordering::Equal => 0,
                    Ordering::Greater => 1,
                })
            }
        }
        "between?" => V::Bool(x >= num_f(&arg(a, 0)) && x <= num_f(&arg(a, 1))),
        "clamp" => {
            let (lo, hi) = match &o {
                V::Range(r) => (r.0.clone(), r.1.clone()),
                _ => (arg(a, 0), arg(a, 1)),
            };
            if x < num_f(&lo) {
                lo
            } else if !hi.is_nil() && x > num_f(&hi) {
                hi
            } else {
                recv.clone()
            }
        }
        "to_i" | "to_int" | "truncate" => V::Int(x.trunc() as i64),
        "to_f" => V::Float(x),
        "to_s" | "inspect" => match (recv, &o) {
            (V::Int(i), V::Int(base)) => V::str(radix(*i, *base as u32)),
            _ => V::str(to_s_plain(recv)),
        },
        "to_r" | "to_c" | "rationalize" => recv.clone(),
        "hash" => V::Int(hkey_hash(&hkey(recv))),
        "zero?" => V::Bool(x == 0.0),
        "positive?" => V::Bool(x > 0.0),
        "negative?" => V::Bool(x < 0.0),
        "nonzero?" => {
            if x == 0.0 {
                V::Nil
            } else {
                recv.clone()
            }
        }
        "even?" => V::Bool(matches!(recv, V::Int(i) if i % 2 == 0)),
        "odd?" => V::Bool(matches!(recv, V::Int(i) if i % 2 != 0)),
        "integer?" => V::Bool(matches!(recv, V::Int(_))),
        "finite?" => V::Bool(x.is_finite()),
        "nan?" => V::Bool(x.is_nan()),
        "infinite?" => {
            if x.is_infinite() {
                V::Int(if x > 0.0 { 1 } else { -1 })
            } else {
                V::Nil
            }
        }
        "abs" | "magnitude" => match recv {
            V::Int(i) => V::Int(i.abs()),
            _ => V::Float(x.abs()),
        },
        "succ" | "next" => V::Int(x as i64 + 1),
        "pred" => V::Int(x as i64 - 1),
        "chr" => V::str(
            char::from_u32(x as u32)
                .map(|c| c.to_string())
                .unwrap_or_default(),
        ),
        "ord" => recv.clone(),
        "round" | "floor" | "ceil" => {
            let digits = match o {
                V::Int(d) => d,
                _ => 0,
            };
            if let V::Int(_) = recv {
                if digits >= 0 {
                    return some(recv.clone());
                }
            }
            if digits > 0 {
                let m = 10f64.powi(digits as i32);
                let r = match name {
                    "round" => (x * m).round() / m,
                    "floor" => (x * m).floor() / m,
                    _ => (x * m).ceil() / m,
                };
                V::Float(r)
            } else {
                let r = match name {
                    "round" => x.round(),
                    "floor" => x.floor(),
                    _ => x.ceil(),
                };
                V::Int(r as i64)
            }
        }
        "divmod" => {
            if let (V::Int(p), V::Int(q)) = (recv, &o) {
                V::arr(vec![V::Int(p.div_euclid(*q)), V::Int(p.rem_euclid(*q))])
            } else {
                let y = num_f(&o);
                V::arr(vec![
                    V::Float((x / y).floor()),
                    V::Float(x - y * (x / y).floor()),
                ])
            }
        }
        "fdiv" => V::Float(x / num_f(&o)),
        "digits" => {
            let mut n = x as i64;
            let mut d = vec![];
            loop {
                d.push(V::Int(n % 10));
                n /= 10;
                if n == 0 {
                    break;
                }
            }
            V::arr(d)
        }
        "times" => {
            let n = x as i64;
            let Some(b) = &a.blk else {
                return some(V::arr((0..n).map(V::Int).collect()));
            };
            for i in 0..n {
                yield1(it, b, V::Int(i))?;
            }
            recv.clone()
        }
        "upto" | "downto" => {
            let (s, e) = (x as i64, num_f(&o) as i64);
            let list: Vec<i64> = if name == "upto" {
                (s..=e).collect()
            } else {
                (e..=s).rev().collect()
            };
            let Some(b) = &a.blk else {
                return some(V::arr(list.into_iter().map(V::Int).collect()));
            };
            for i in list {
                yield1(it, b, V::Int(i))?;
            }
            recv.clone()
        }
        "step" => {
            let lim = num_f(&arg(a, 0));
            let st = num_f(&arg(a, 1));
            let mut out = vec![];
            let mut cur = x;
            while (st > 0.0 && cur <= lim) || (st < 0.0 && cur >= lim) {
                out.push(if both_int {
                    V::Int(cur as i64)
                } else {
                    V::Float(cur)
                });
                cur += st;
            }
            match &a.blk {
                Some(b) => {
                    for v in out {
                        yield1(it, b, v)?;
                    }
                    recv.clone()
                }
                None => V::arr(out),
            }
        }
        "&" | "|" | "^" | "<<" | ">>" => {
            let (V::Int(p), V::Int(q)) = (recv, &o) else {
                return Err(it.err("TypeError", "bit op on non-integer"));
            };
            V::Int(match name {
                "&" => p & q,
                "|" => p | q,
                "^" => p ^ q,
                "<<" => p << q,
                _ => p >> q,
            })
        }
        "bit_length" => V::Int(64 - (x as i64).leading_zeros() as i64),
        "coerce" => V::arr(vec![o.clone(), recv.clone()]),
        "dup" | "clone" | "freeze" | "itself" => recv.clone(),
        "frozen?" => V::Bool(true),
        _ => return Ok(None),
    };
    some(v)
}

fn radix(mut n: i64, base: u32) -> String {
    if n == 0 {
        return "0".into();
    }
    let neg = n < 0;
    n = n.abs();
    let mut d = vec![];
    while n > 0 {
        d.push(std::char::from_digit((n % base as i64) as u32, base).unwrap());
        n /= base as i64;
    }
    if neg {
        d.push('-');
    }
    d.into_iter().rev().collect()
}

pub fn sym_m(it: &Rc<Interp>, recv: &V, s: &Sym, name: &str, a: &Args) -> NR {
    let v = match name {
        "to_sym" => recv.clone(),
        "to_s" | "id2name" | "name" => V::str(s.to_string()),
        "inspect" => V::str(sym_inspect(s)),
        "to_proc" => {
            let fr = it.top_frame.borrow().clone().unwrap();
            V::Proc(Rc::new(Proc {
                body: ProcBody::SymToProc(s.to_string()),
                scope: Scope::new(None),
                frame: fr,
                lambda: false,
                id: it.id(),
            }))
        }
        "length" | "size" => V::Int(s.chars().count() as i64),
        "==" | "===" | "eql?" | "equal?" => V::Bool(matches!(arg(a, 0), V::Sym(o) if o == *s)),
        "!=" => V::Bool(!matches!(arg(a, 0), V::Sym(o) if o == *s)),
        "<=>" => match arg(a, 0) {
            V::Sym(o) => V::Int(match s.as_bytes().cmp(o.as_bytes()) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }),
            _ => V::Nil,
        },
        "hash" => V::Int(hkey_hash(&hkey(recv))),
        "upcase" | "downcase" | "capitalize" | "succ" | "swapcase" => {
            let r = str_m(
                it,
                &V::str(s.to_string()),
                &Rc::new(RStr {
                    s: RefCell::new(s.to_string()),
                    frozen: std::cell::Cell::new(false),
                }),
                name,
                a,
            )?
            .unwrap();
            V::sym(&to_s_plain(&r))
        }
        "start_with?" | "end_with?" | "[]" | "empty?" | "=~" | "match?" | "match" => {
            let sv = V::str(s.to_string());
            if let V::Str(rs) = &sv {
                return str_m(it, &sv, rs, name, a);
            }
            V::Nil
        }
        "dup" | "clone" | "freeze" | "itself" => recv.clone(),
        "frozen?" => V::Bool(true),
        _ => return Ok(None),
    };
    some(v)
}

pub fn range_m(it: &Rc<Interp>, recv: &V, r: &Rc<(V, V, bool)>, name: &str, a: &Args) -> NR {
    let v = match name {
        "begin" | "first" if a.pos.is_empty() => r.0.clone(),
        "end" | "last" if a.pos.is_empty() => r.1.clone(),
        "exclude_end?" => V::Bool(r.2),
        "include?" | "member?" | "cover?" | "===" => {
            let x = arg(a, 0);
            if !matches!(
                (&x, &r.0),
                (V::Int(_) | V::Float(_), V::Int(_) | V::Float(_)) | (V::Str(_), V::Str(_))
            ) {
                return some(V::Bool(false));
            }
            let lo = r.0.is_nil() || rb_cmp(it, &r.0, &x)? != Ordering::Greater;
            let hi = r.1.is_nil() || {
                let c = rb_cmp(it, &x, &r.1)?;
                if r.2 {
                    c == Ordering::Less
                } else {
                    c != Ordering::Greater
                }
            };
            V::Bool(lo && hi)
        }
        "size" | "count" if a.blk.is_none() && a.pos.is_empty() => {
            V::Int(to_list(it, recv)?.len() as i64)
        }
        "to_s" => V::str(format!(
            "{}{}{}",
            to_s_plain(&r.0),
            if r.2 { "..." } else { ".." },
            to_s_plain(&r.1)
        )),
        "inspect" => V::str(inspect_plain(recv)),
        "==" | "eql?" => V::Bool(rb_eq(it, recv, &arg(a, 0))?),
        "hash" => V::Int(hkey_hash(&HKey::Arr(vec![
            hkey(&r.0),
            hkey(&r.1),
            HKey::Bool(r.2),
        ]))),
        "step" => {
            let st = match arg(a, 0) {
                V::Int(s) => s,
                _ => 1,
            };
            let list: Vec<V> = to_list(it, recv)?
                .into_iter()
                .step_by(st.max(1) as usize)
                .collect();
            match &a.blk {
                Some(b) => {
                    for x in list {
                        yield1(it, b, x)?;
                    }
                    recv.clone()
                }
                None => V::arr(list),
            }
        }
        "dup" | "clone" | "freeze" | "itself" => recv.clone(),
        _ => {
            let items = to_list(it, recv)?;
            return enum_m(it, recv, items, name, a);
        }
    };
    some(v)
}

pub fn proc_m(it: &Rc<Interp>, recv: &V, p: &Rc<Proc>, name: &str, a: &Args) -> NR {
    let v = match name {
        "call" | "()" | "yield" | "[]" | "===" => it.call_proc_in(
            p,
            a.pos.clone(),
            a.blk.clone(),
            None,
            p.lambda,
            a.kw.clone(),
        )?,
        "to_proc" => recv.clone(),
        "lambda?" => V::Bool(p.lambda),
        "arity" => V::Int(match &p.body {
            ProcBody::Ast {
                args: Some(lib_ruby_parser::Node::Args(x)),
                ..
            } => {
                let req = x
                    .args
                    .iter()
                    .filter(|n| {
                        matches!(
                            n,
                            lib_ruby_parser::Node::Arg(_) | lib_ruby_parser::Node::Procarg0(_)
                        )
                    })
                    .count() as i64;
                if x.args.iter().any(|n| {
                    matches!(
                        n,
                        lib_ruby_parser::Node::Optarg(_) | lib_ruby_parser::Node::Restarg(_)
                    )
                }) {
                    -(req + 1)
                } else {
                    req
                }
            }
            ProcBody::SymToProc(_) => -2,
            _ => 0,
        }),
        "parameters" => V::arr(vec![]),
        "curry" => recv.clone(),
        "dup" | "clone" | "freeze" | "itself" => recv.clone(),
        _ => return Ok(None),
    };
    some(v)
}

pub fn regex_m(it: &Rc<Interp>, recv: &V, r: &Rc<RRegex>, name: &str, a: &Args) -> NR {
    let v = match name {
        "match" => {
            let s = arg(a, 0);
            if s.is_nil() {
                *it.last_match.borrow_mut() = V::Nil;
                return some(V::Nil);
            }
            let s = str_of(it, &s)?;
            do_match(it, r, &s, true)?
        }
        "match?" => match arg(a, 0) {
            V::Nil => V::Bool(false),
            s => V::Bool(r.re.is_match(&str_of(it, &s)?).unwrap_or(false)),
        },
        "=~" => match arg(a, 0) {
            V::Nil => V::Nil,
            s => {
                let st = str_of(it, &s)?;
                let m = do_match(it, r, &st, true)?;
                if m.is_nil() {
                    V::Nil
                } else {
                    it.ivar_get(&m, "@begin")
                }
            }
        },
        "===" => match arg(a, 0) {
            V::Str(s) => V::Bool(r.re.is_match(&s.s.borrow()).unwrap_or(false)),
            V::Sym(s) => V::Bool(r.re.is_match(&s).unwrap_or(false)),
            _ => V::Bool(false),
        },
        "source" => V::str(r.src.clone()),
        "to_s" => V::str(format!("(?-mix:{})", r.src)),
        "inspect" => V::str(format!("/{}/", r.src)),
        "names" => V::arr(r.re.capture_names().flatten().map(V::str).collect()),
        "freeze" | "dup" | "itself" => recv.clone(),
        "==" => V::Bool(matches!(arg(a, 0), V::Regex(o) if o.src == r.src)),
        _ => return Ok(None),
    };
    some(v)
}

pub fn method_m(it: &Rc<Interp>, m: &Rc<MethodObj>, name: &str, a: &Args) -> NR {
    let v = match name {
        "call" | "()" | "[]" | "===" => it.call_full(&m.recv, &m.name, a.clone(), true, None)?,
        "to_proc" => {
            let fr = it.top_frame.borrow().clone().unwrap();
            V::Proc(Rc::new(Proc {
                body: ProcBody::Method(m.clone()),
                scope: Scope::new(None),
                frame: fr,
                lambda: true,
                id: it.id(),
            }))
        }
        "name" => V::sym(&m.name),
        "owner" => V::Mod(it.class_of(&m.recv)),
        "receiver" => m.recv.clone(),
        "arity" => V::Int(-1),
        "unbind" => V::Method(m.clone()),
        "bind" => V::Method(Rc::new(MethodObj {
            recv: arg(a, 0),
            name: m.name.clone(),
            unbound_owner: m.unbound_owner.clone(),
        })),
        "bind_call" => {
            let mut fl = a.flat();
            let recv = fl.remove(0);
            it.call_full(
                &recv,
                &m.name,
                Args::with_blk(fl, a.blk.clone()),
                true,
                None,
            )?
        }
        _ => return Ok(None),
    };
    some(v)
}

pub fn nil_m(_it: &Rc<Interp>, name: &str, a: &Args) -> NR {
    let v = match name {
        "to_s" => V::str(""),
        "to_a" => V::arr(vec![]),
        "to_h" => V::empty_hash(),
        "to_i" => V::Int(0),
        "to_f" => V::Float(0.0),
        "inspect" => V::str("nil"),
        "nil?" => V::Bool(true),
        "&" => V::Bool(false),
        "|" => V::Bool(arg(a, 0).truthy()),
        "==" | "===" | "eql?" | "equal?" => V::Bool(arg(a, 0).is_nil()),
        "!" => V::Bool(true),
        "dup" | "clone" | "freeze" | "itself" => V::Nil,
        _ => return Ok(None),
    };
    some(v)
}

pub fn bool_m(b: bool, name: &str, a: &Args) -> NR {
    let o = arg(a, 0);
    let v = match name {
        "to_s" | "inspect" => V::str(b.to_string()),
        "&" => V::Bool(b && o.truthy()),
        "|" => V::Bool(b || o.truthy()),
        "^" => V::Bool(b != o.truthy()),
        "!" => V::Bool(!b),
        "==" | "===" | "eql?" | "equal?" => V::Bool(matches!(o, V::Bool(x) if x == b)),
        "dup" | "clone" | "freeze" | "itself" => V::Bool(b),
        _ => return Ok(None),
    };
    some(v)
}

fn sym_or_str(it: &Rc<Interp>, v: &V) -> Result<String, Ctrl> {
    match v {
        V::Sym(s) => Ok(s.to_string()),
        V::Str(s) => Ok(s.s.borrow().clone()),
        other => Err(it.err(
            "TypeError",
            format!("{} is not a symbol nor a string", inspect_plain(other)),
        )),
    }
}

pub fn define_attr(it: &Rc<Interp>, m: &Rc<Module>, name: &str, reader: bool, writer: bool) {
    let _ = it;
    if reader {
        m.methods_mut().insert(
            name.to_string(),
            Meth::Native(Box::leak(format!("__attr_r_{name}").into_boxed_str())),
        );
    }
    if writer {
        m.methods_mut().insert(
            format!("{name}="),
            Meth::Native(Box::leak(format!("__attr_w_{name}").into_boxed_str())),
        );
    }
}

pub fn module_m(
    it: &Rc<Interp>,
    recv: &V,
    m: &Rc<Module>,
    name: &str,
    a: &Args,
    caller: Option<&Rc<Frame>>,
) -> NR {
    let v = match name {
        "name" => m.name.borrow().clone().map(V::str).unwrap_or(V::Nil),
        "to_s" | "inspect" => {
            if let Some(V::Obj(_)) | Some(V::Mod(_)) = m.attached.borrow().as_ref() {
                let att = m.attached.borrow().clone().unwrap();
                return some(V::str(format!("#<Class:{}>", to_s_plain(&att))));
            }
            V::str(m.display_name())
        }
        "hash" => V::Int(m.id as i64),
        "==" | "equal?" | "eql?" => V::Bool(matches!(arg(a, 0), V::Mod(o) if o.id == m.id)),
        "!=" => V::Bool(!matches!(arg(a, 0), V::Mod(o) if o.id == m.id)),
        "===" => V::Bool(it.is_a_mod(&arg(a, 0), m)),
        "<" | "<=" => match arg(a, 0) {
            V::Mod(o) => {
                if o.id == m.id {
                    V::Bool(name == "<=")
                } else if it.ancestors(m).iter().any(|x| x.id == o.id) {
                    V::Bool(true)
                } else if it.ancestors(&o).iter().any(|x| x.id == m.id) {
                    V::Bool(false)
                } else {
                    V::Nil
                }
            }
            _ => return Err(it.err("TypeError", "compared with non class/module")),
        },
        ">" | ">=" => match arg(a, 0) {
            V::Mod(o) => V::Bool(
                (name == ">=" && o.id == m.id)
                    || it
                        .ancestors(&o)
                        .iter()
                        .any(|x| x.id == m.id && x.id != o.id),
            ),
            _ => return Err(it.err("TypeError", "compared with non class/module")),
        },
        "ancestors" => V::arr(it.ancestors(m).into_iter().map(V::Mod).collect()),
        "superclass" => m.superclass.borrow().clone().map(V::Mod).unwrap_or(V::Nil),
        "include" | "prepend" => {
            for x in a.flat().into_iter().rev() {
                let V::Mod(inc) = &x else {
                    return Err(it.err("TypeError", "wrong argument type (expected Module)"));
                };
                let list = if name == "include" {
                    &m.includes
                } else {
                    &m.prepends
                };
                if !list.borrow().iter().any(|y| y.id == inc.id) {
                    list.borrow_mut().push(inc.clone());
                }
                let hook = if name == "include" {
                    "included"
                } else {
                    "prepended"
                };
                if it.find_method(&x, hook).is_some() {
                    it.call_full(&x, hook, Args::new(vec![recv.clone()]), true, None)?;
                }
                crate::stdlib::native_included(it, inc, m)?;
            }
            recv.clone()
        }
        "include?" => V::Bool(
            matches!(arg(a, 0), V::Mod(o) if it.ancestors(m).iter().any(|x| x.id == o.id && x.id != m.id)),
        ),
        "included_modules" => V::arr(
            it.ancestors(m)
                .into_iter()
                .filter(|x| !x.is_class)
                .map(V::Mod)
                .collect(),
        ),
        "new" if m.is_class => {
            if let Some(r) = crate::stdlib::native_new(it, m, a)? {
                return some(r);
            }
            if m.id == it.cls("Class").id || m.id == it.cls("Module").id {
                let is_class = m.id == it.cls("Class").id;
                let sup = match arg(a, 0) {
                    V::Mod(s) => Some(s),
                    _ => Some(it.object.clone()),
                };
                let nm = new_module(
                    None,
                    is_class,
                    if is_class { sup.clone() } else { None },
                    it.id(),
                );
                if is_class {
                    if let Some(s) = &sup {
                        let sv = V::Mod(s.clone());
                        if it.find_method(&sv, "inherited").is_some() {
                            it.call_full(
                                &sv,
                                "inherited",
                                Args::new(vec![V::Mod(nm.clone())]),
                                true,
                                None,
                            )?;
                        }
                    }
                }
                if let Some(b) = &a.blk {
                    let nv = V::Mod(nm.clone());
                    module_exec(it, &nm, b, vec![nv])?;
                }
                return some(V::Mod(nm));
            }
            let obj = it.new_obj(m.clone());
            if m.id == it.cls("Exception").id
                || it
                    .ancestors(m)
                    .iter()
                    .any(|x| x.display_name() == "Exception")
            {
                let msg = match arg(a, 0) {
                    V::Nil => V::str(m.display_name()),
                    other => other,
                };
                it.ivar_set(&obj, "@message", msg)?;
            }
            if it.find_method(&obj, "initialize").is_some() {
                it.call_full(&obj, "initialize", a.clone(), true, None)?;
            }
            obj
        }
        "allocate" => it.new_obj(m.clone()),
        "class_eval" | "module_eval" | "class_exec" | "module_exec" | "instance_eval"
        | "instance_exec" => {
            if let Some(b) = &a.blk {
                let args = if name.ends_with("exec") {
                    a.pos.clone()
                } else {
                    vec![recv.clone()]
                };
                if name.starts_with("instance") {
                    let meta = it.meta_of(m);
                    return some(module_exec_target(it, recv, &meta, b, args)?);
                }
                module_exec(it, m, b, args)?
            } else {
                let src = to_s_plain(&arg(a, 0));
                let fr = Rc::new(Frame {
                    self_v: recv.clone(),
                    def_target: m.clone(),
                    cref: Rc::new(vec![it.object.clone(), m.clone()]),
                    block: None,
                    method: None,
                    args: vec![],
                    id: it.id(),
                    file: Rc::from("(eval)"),
                });
                it.eval_string(&src, "(eval)", &fr, &Scope::new(None))?
            }
        }
        "define_method" => {
            let n = sym_or_str(it, &arg(a, 0))?;
            let body = match (&a.blk, arg(a, 1)) {
                (Some(b), _) => b.clone(),
                (None, V::Proc(p)) => p,
                (None, V::Method(mo)) => {
                    let fr = it.top_frame.borrow().clone().unwrap();
                    Rc::new(Proc {
                        body: ProcBody::Method(mo),
                        scope: Scope::new(None),
                        frame: fr,
                        lambda: true,
                        id: it.id(),
                    })
                }
                _ => {
                    return Err(it.err(
                        "ArgumentError",
                        "tried to create Proc object without a block",
                    ))
                }
            };
            m.methods_mut()
                .insert(n.clone(), Meth::Proc(body, m.clone()));
            V::sym(&n)
        }
        "attr_reader" | "attr_writer" | "attr_accessor" | "attr" => {
            for x in a.flat() {
                let n = sym_or_str(it, &x)?;
                define_attr(
                    it,
                    m,
                    &n,
                    name != "attr_writer",
                    name == "attr_writer" || name == "attr_accessor",
                );
            }
            V::Nil
        }
        "private"
        | "public"
        | "protected"
        | "private_constant"
        | "public_constant"
        | "private_class_method"
        | "public_class_method"
        | "module_function"
        | "ruby2_keywords"
        | "deprecate_constant" => {
            if name == "module_function" {
                if a.pos.is_empty() {
                    m.module_function.set(true);
                } else {
                    let meta = it.meta_of(m);
                    for x in a.flat() {
                        let n = sym_or_str(it, &x)?;
                        if let Some(me) = m.methods.borrow().get(&n) {
                            meta.methods_mut().insert(n, me.clone());
                        }
                    }
                }
            } else if name == "private" && a.pos.is_empty() {
                m.default_private.set(true);
            } else if name == "public" && a.pos.is_empty() {
                m.default_private.set(false);
            }
            if a.pos.len() == 1 {
                a.pos[0].clone()
            } else {
                V::Nil
            }
        }
        "const_get" => {
            let n = sym_or_str(it, &arg(a, 0))?;
            let mut cur = m.clone();
            let mut val = V::Nil;
            for (i, part) in n.split("::").enumerate() {
                if part.is_empty() {
                    cur = it.object.clone();
                    continue;
                }
                let found = if i == 0 {
                    match it.const_lookup_in(&cur, part)? {
                        Some(v) => Some(v),
                        None => it.own_const(&it.object.clone(), part)?,
                    }
                } else {
                    it.const_lookup_in(&cur, part)?
                };
                match found {
                    Some(v) => {
                        if let V::Mod(mm) = &v {
                            cur = mm.clone();
                        }
                        val = v;
                    }
                    None => {
                        if let Some(v) = crate::stdlib::autoload_const(it, part)? {
                            val = v;
                            continue;
                        }
                        return Err(it.err(
                            "NameError",
                            format!("uninitialized constant {}::{}", m.display_name(), part),
                        ));
                    }
                }
            }
            val
        }
        "const_set" => {
            let n = sym_or_str(it, &arg(a, 0))?;
            it.const_set(m, &n, arg(a, 1));
            arg(a, 1)
        }
        "const_defined?" => {
            let n = sym_or_str(it, &arg(a, 0))?;
            let mut cur = m.clone();
            let mut ok = true;
            for part in n.split("::") {
                match it.const_lookup_in(&cur, part)? {
                    Some(V::Mod(mm)) => cur = mm,
                    Some(_) => {}
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
            V::Bool(ok)
        }
        "constants" => V::arr(m.consts.borrow().keys().map(|k| V::sym(k)).collect()),
        "remove_const" => {
            let n = sym_or_str(it, &arg(a, 0))?;
            m.consts.borrow_mut().shift_remove(&n).unwrap_or(V::Nil)
        }
        "method_defined?"
        | "public_method_defined?"
        | "instance_methods"
        | "public_instance_methods"
        | "private_method_defined?"
        | "private_instance_methods" => {
            if name.ends_with("methods") {
                let mut names = vec![];
                let own_only = matches!(arg(a, 0), V::Bool(false));
                for x in if own_only {
                    vec![m.clone()]
                } else {
                    it.ancestors(m)
                } {
                    for (k, me) in x.methods.borrow().iter() {
                        let is_priv = matches!(me, Meth::User(u) if u.private);
                        if (name.starts_with("private")) == is_priv {
                            names.push(V::sym(k));
                        }
                    }
                }
                V::arr(names)
            } else {
                let n = sym_or_str(it, &arg(a, 0))?;
                let mut found = false;
                for x in it.ancestors(m) {
                    if let Some(me) = x.methods.borrow().get(&n) {
                        let is_priv = matches!(me, Meth::User(u) if u.private);
                        found = !matches!(me, Meth::Undefined)
                            && (name.starts_with("private") == is_priv
                                || name == "method_defined?" && !is_priv);
                        break;
                    }
                }
                V::Bool(found)
            }
        }
        "instance_method" => {
            let n = sym_or_str(it, &arg(a, 0))?;
            V::Method(Rc::new(MethodObj {
                recv: V::Nil,
                name: n,
                unbound_owner: Some(m.clone()),
            }))
        }
        "alias_method" => {
            let to = sym_or_str(it, &arg(a, 0))?;
            let from = sym_or_str(it, &arg(a, 1))?;
            it.alias_method(m, &to, &from)?;
            V::sym(&to)
        }
        "remove_method" | "undef_method" => {
            for x in a.flat() {
                let n = sym_or_str(it, &x)?;
                if name == "remove_method" {
                    m.methods_mut().remove(&n);
                } else {
                    m.methods_mut().insert(n, Meth::Undefined);
                }
            }
            recv.clone()
        }
        "class_variable_get" => {
            let n = sym_or_str(it, &arg(a, 0))?;
            let v = m.cvars.borrow().get(&n).cloned();
            v.ok_or_else(|| it.err("NameError", format!("uninitialized class variable {n}")))?
        }
        "class_variable_set" => {
            let n = sym_or_str(it, &arg(a, 0))?;
            m.cvars.borrow_mut().insert(n, arg(a, 1));
            arg(a, 1)
        }
        "class_variable_defined?" => {
            V::Bool(m.cvars.borrow().contains_key(&sym_or_str(it, &arg(a, 0))?))
        }
        "class_variables" => V::arr(m.cvars.borrow().keys().map(|k| V::sym(k)).collect()),
        "instance_variable_get" => it.ivar_get(recv, &sym_or_str(it, &arg(a, 0))?),
        "instance_variable_set" => {
            it.ivar_set(recv, &sym_or_str(it, &arg(a, 0))?, arg(a, 1))?;
            arg(a, 1)
        }
        "instance_variable_defined?" => {
            V::Bool(m.ivars.borrow().contains_key(&sym_or_str(it, &arg(a, 0))?))
        }
        "freeze" | "itself" => recv.clone(),
        "module_function_unused" => V::Nil,
        "class" => V::Mod(it.class_of(recv)),
        "singleton_class?" => V::Bool(m.attached.borrow().is_some()),
        "attached_object" => m.attached.borrow().clone().unwrap_or(V::Nil),
        "instance_methods_unused" => V::Nil,
        "extend" => {
            let meta = it.meta_of(m);
            for x in a.flat() {
                if let V::Mod(e) = &x {
                    if !meta.includes.borrow().iter().any(|y| y.id == e.id) {
                        meta.includes_mut().push(e.clone());
                    }
                    if it.find_method(&x, "extended").is_some() {
                        it.call_full(&x, "extended", Args::new(vec![recv.clone()]), true, None)?;
                    }
                }
            }
            recv.clone()
        }
        _ => {
            let _ = caller;
            return Ok(None);
        }
    };
    some(v)
}

pub fn module_exec(it: &Rc<Interp>, m: &Rc<Module>, b: &Rc<Proc>, args: Vec<V>) -> R {
    module_exec_target(it, &V::Mod(m.clone()), m, b, args)
}

pub fn module_exec_target(
    it: &Rc<Interp>,
    self_v: &V,
    target: &Rc<Module>,
    b: &Rc<Proc>,
    args: Vec<V>,
) -> R {
    let mut cref = (*b.frame.cref).clone();
    if let V::Mod(m) = self_v {
        if !cref.iter().any(|x| x.id == m.id) {
            cref.push(m.clone());
        }
    }
    let fr = Rc::new(Frame {
        self_v: self_v.clone(),
        def_target: target.clone(),
        cref: Rc::new(cref),
        block: b.frame.block.clone(),
        method: b.frame.method.clone(),
        args: b.frame.args.clone(),
        id: b.frame.id,
        file: b.frame.file.clone(),
    });
    it.call_proc_in(b, args, None, Some(fr), false, None)
}

pub fn obj_special(it: &Rc<Interp>, recv: &V, name: &str, a: &Args) -> NR {
    if let Some(attr) = name.strip_prefix("__attr_r_") {
        return some(it.ivar_get(recv, &format!("@{attr}")));
    }
    if let Some(attr) = name.strip_prefix("__attr_w_") {
        it.ivar_set(recv, &format!("@{attr}"), arg(a, 0))?;
        return some(arg(a, 0));
    }
    let cls = it.class_of(recv);
    let cname = cls.display_name();
    if cname == "MatchData" {
        let groups = to_list(it, &it.ivar_get(recv, "@groups"))?;
        let v = match name {
            "[]" => match arg(a, 0) {
                V::Int(i) => groups.get(i as usize).cloned().unwrap_or(V::Nil),
                k => {
                    let named = it.ivar_get(recv, "@named");
                    if let V::Hash(h) = named {
                        h.get(&V::str(to_s_plain(&k))).unwrap_or(V::Nil)
                    } else {
                        V::Nil
                    }
                }
            },
            "captures" => V::arr(groups[1..].to_vec()),
            "named_captures" => it.ivar_get(recv, "@named"),
            "to_a" => V::arr(groups),
            "pre_match" => it.ivar_get(recv, "@pre"),
            "post_match" => it.ivar_get(recv, "@post"),
            "begin" => it.ivar_get(recv, "@begin"),
            "to_s" => groups[0].clone(),
            _ => return Ok(None),
        };
        return some(v);
    }
    if cname == "LavaEnv" {
        return crate::stdlib::env_m(it, name, a);
    }
    if cname == "IO" {
        return match name {
            "puts" | "print" | "write" | "warn" => {
                for x in a.flat() {
                    eprintln!("{}", it.to_s(&x)?);
                }
                some(V::Nil)
            }
            "flush" | "sync" | "sync=" | "fileno" => some(V::Nil),
            "tty?" | "isatty" => some(V::Bool(false)),
            _ => Ok(None),
        };
    }
    if it.is_a(recv, "Exception") {
        let v = match name {
            "message" | "to_s" => {
                let m = it.ivar_get(recv, "@message");
                if m.is_nil() {
                    V::str(cname)
                } else {
                    V::str(it.to_s(&m)?)
                }
            }
            "full_message" | "detailed_message" => V::str(format!(
                "{} ({})",
                to_s_plain(&it.ivar_get(recv, "@message")),
                cname
            )),
            "backtrace" | "backtrace_locations" => V::arr(vec![]),
            "set_backtrace" => V::Nil,
            "cause" => it.ivar_get(recv, "@cause"),
            "exception" => {
                if let V::Nil = arg(a, 0) {
                    recv.clone()
                } else {
                    let n = it.new_obj(cls.clone());
                    it.ivar_set(&n, "@message", arg(a, 0))?;
                    n
                }
            }
            "result" => it.ivar_get(recv, "@result"),
            "key" => it.ivar_get(recv, "@key"),
            "receiver" => it.ivar_get(recv, "@receiver"),
            "name" => it.ivar_get(recv, "@name"),
            _ => return Ok(None),
        };
        return some(v);
    }
    if it.is_a(recv, "Enumerable") && name != "each" && it.find_method(recv, "each").is_some() {
        let enum_names = [
            "map",
            "collect",
            "select",
            "filter",
            "reject",
            "to_a",
            "entries",
            "each_with_object",
            "each_with_index",
            "include?",
            "member?",
            "count",
            "find",
            "detect",
            "reduce",
            "inject",
            "sort",
            "sort_by",
            "any?",
            "all?",
            "none?",
            "first",
            "min",
            "max",
            "sum",
            "group_by",
            "flat_map",
            "to_h",
            "min_by",
            "max_by",
            "partition",
            "filter_map",
            "zip",
            "tally",
            "uniq",
            "find_index",
            "each_slice",
            "take",
            "drop",
            "to_set",
            "minmax",
            "one?",
            "lazy",
            "chunk_while",
            "take_while",
            "drop_while",
        ];
        if enum_names.contains(&name) {
            let items = to_list(it, recv)?;
            return enum_m(it, recv, items, name, a);
        }
    }
    if it.is_a(recv, "Comparable") && it.find_method(recv, "<=>").is_some() {
        if let "<" | ">" | "<=" | ">=" | "==" | "between?" | "clamp" = name {
            let c = |o: &V| -> Result<Ordering, Ctrl> { rb_cmp(it, recv, o) };
            let v = match name {
                "<" => V::Bool(c(&arg(a, 0))? == Ordering::Less),
                ">" => V::Bool(c(&arg(a, 0))? == Ordering::Greater),
                "<=" => V::Bool(c(&arg(a, 0))? != Ordering::Greater),
                ">=" => V::Bool(c(&arg(a, 0))? != Ordering::Less),
                "==" => V::Bool(c(&arg(a, 0)).map(|o| o == Ordering::Equal).unwrap_or(false)),
                "between?" => {
                    V::Bool(c(&arg(a, 0))? != Ordering::Less && c(&arg(a, 1))? != Ordering::Greater)
                }
                _ => recv.clone(),
            };
            return some(v);
        }
    }
    Ok(None)
}

pub fn object_m(it: &Rc<Interp>, recv: &V, name: &str, a: Args, caller: Option<&Rc<Frame>>) -> NR {
    let v = match name {
        "class" => V::Mod(it.class_of(recv)),
        "singleton_class" => V::Mod(it.singleton_of(recv)?),
        "is_a?" | "kind_of?" | "instance_of?" => match arg(&a, 0) {
            V::Mod(m) => {
                if name == "instance_of?" {
                    V::Bool(it.class_of(recv).id == m.id)
                } else {
                    V::Bool(it.is_a_mod(recv, &m))
                }
            }
            _ => return Err(it.err("TypeError", "class or module required")),
        },
        "nil?" => V::Bool(recv.is_nil()),
        "==" | "equal?" | "eql?" => V::Bool(
            recv.ptr() == arg(&a, 0).ptr()
                && std::mem::discriminant(recv) == std::mem::discriminant(&arg(&a, 0)),
        ),
        "!=" => V::Bool(!rb_eq(it, recv, &arg(&a, 0))?),
        "!" => V::Bool(!recv.truthy()),
        "===" => V::Bool(rb_eq(it, recv, &arg(&a, 0))?),
        "=~" => V::Nil,
        "!~" => V::Bool(!it.call(recv, "=~", a.clone())?.truthy()),
        "<=>" => {
            if rb_eq(it, recv, &arg(&a, 0))? {
                V::Int(0)
            } else {
                V::Nil
            }
        }
        "hash" | "object_id" | "__id__" => V::Int(recv.ptr() as i64),
        "respond_to?" => {
            let n = to_s_plain(&arg(&a, 0));
            let mut r = it.responds(recv, &n);
            if !r {
                if let Some((m, o)) = it.find_method(recv, "respond_to_missing?") {
                    r = it
                        .invoke(
                            recv,
                            "respond_to_missing?",
                            &m,
                            &o,
                            Args::new(vec![V::sym(&n), arg(&a, 1)]),
                            None,
                        )?
                        .truthy();
                }
            }
            V::Bool(r)
        }
        "respond_to_missing?" => V::Bool(false),
        "send" | "__send__" | "public_send" => {
            let mut fl = a.pos.clone();
            if fl.is_empty() {
                return Err(it.err("ArgumentError", "no method name given"));
            }
            let n = to_s_plain(&fl.remove(0));
            return Ok(Some(it.call_full(
                recv,
                &n,
                Args {
                    pos: fl,
                    kw: a.kw.clone(),
                    blk: a.blk.clone(),
                },
                name != "public_send",
                caller,
            )?));
        }
        "method" | "public_method" => {
            let n = to_s_plain(&arg(&a, 0));
            if !it.responds(recv, &n) && it.find_method(recv, "method_missing").is_none() {
                return Err(it.err(
                    "NameError",
                    format!("undefined method '{n}' for {}", describe_recv(it, recv)),
                ));
            }
            V::Method(Rc::new(MethodObj {
                recv: recv.clone(),
                name: n,
                unbound_owner: None,
            }))
        }
        "methods" | "public_methods" => {
            let mut names = vec![];
            for m in it.lookup_chain(recv) {
                for k in m.methods.borrow().keys() {
                    names.push(V::sym(k));
                }
            }
            V::arr(names)
        }
        "instance_variable_get" => it.ivar_get(recv, &to_s_plain(&arg(&a, 0))),
        "instance_variable_set" => {
            it.ivar_set(recv, &to_s_plain(&arg(&a, 0)), arg(&a, 1))?;
            arg(&a, 1)
        }
        "instance_variable_defined?" => V::Bool(match recv {
            V::Obj(o) => o.ivars.borrow().contains_key(&to_s_plain(&arg(&a, 0))),
            _ => false,
        }),
        "instance_variables" => V::arr(match recv {
            V::Obj(o) => o.ivars.borrow().keys().map(|k| V::sym(k)).collect(),
            V::Mod(m) => m.ivars.borrow().keys().map(|k| V::sym(k)).collect(),
            _ => vec![],
        }),
        "remove_instance_variable" => match recv {
            V::Obj(o) => o
                .ivars
                .borrow_mut()
                .shift_remove(&to_s_plain(&arg(&a, 0)))
                .unwrap_or(V::Nil),
            _ => V::Nil,
        },
        "instance_eval" | "instance_exec" => {
            if let Some(b) = &a.blk {
                let args = if name == "instance_exec" {
                    a.pos.clone()
                } else {
                    vec![recv.clone()]
                };
                match &b.body {
                    ProcBody::Ast { .. } => it.call_proc_self(b, recv.clone(), args)?,
                    _ => it.call_proc(
                        b,
                        {
                            let mut v = vec![recv.clone()];
                            v.extend(args);
                            v
                        },
                        None,
                    )?,
                }
            } else {
                let src = to_s_plain(&arg(&a, 0));
                let file = a
                    .pos
                    .get(1)
                    .map(to_s_plain)
                    .unwrap_or_else(|| "(eval)".into());
                let def_target = it.singleton_of(recv).unwrap_or_else(|_| it.class_of(recv));
                let fr = Rc::new(Frame {
                    self_v: recv.clone(),
                    def_target,
                    cref: Rc::new(vec![it.object.clone()]),
                    block: None,
                    method: None,
                    args: vec![],
                    id: it.id(),
                    file: Rc::from(file.as_str()),
                });
                it.eval_string(&src, &file, &fr, &Scope::new(None))?
            }
        }
        "extend" => {
            let s = it.singleton_of(recv)?;
            for x in a.flat() {
                if let V::Mod(e) = &x {
                    if !s.includes.borrow().iter().any(|y| y.id == e.id) {
                        s.includes_mut().push(e.clone());
                    }
                    if it.find_method(&x, "extended").is_some() {
                        it.call_full(&x, "extended", Args::new(vec![recv.clone()]), true, None)?;
                    }
                }
            }
            recv.clone()
        }
        "define_singleton_method" => {
            let n = to_s_plain(&arg(&a, 0));
            let s = it.singleton_of(recv)?;
            let body = match (&a.blk, arg(&a, 1)) {
                (Some(b), _) => b.clone(),
                (None, V::Proc(p)) => p,
                (None, V::Method(mo)) => {
                    let fr = it.top_frame.borrow().clone().unwrap();
                    Rc::new(Proc {
                        body: ProcBody::Method(mo),
                        scope: Scope::new(None),
                        frame: fr,
                        lambda: true,
                        id: it.id(),
                    })
                }
                _ => return Err(it.err("ArgumentError", "no block")),
            };
            s.methods_mut()
                .insert(n.clone(), Meth::Proc(body, s.clone()));
            V::sym(&n)
        }
        "freeze" => {
            if let V::Obj(o) = recv {
                o.frozen.set(true);
            }
            recv.clone()
        }
        "frozen?" => V::Bool(match recv {
            V::Obj(o) => o.frozen.get(),
            V::Nil | V::Bool(_) | V::Int(_) | V::Float(_) | V::Sym(_) => true,
            _ => false,
        }),
        "dup" | "clone" => match recv {
            V::Obj(o) => {
                let n = Rc::new(Obj {
                    class: RefCell::new(o.class.borrow().clone()),
                    ivars: RefCell::new(o.ivars.borrow().clone()),
                    singleton: RefCell::new(if name == "clone" {
                        o.singleton.borrow().clone()
                    } else {
                        None
                    }),
                    frozen: std::cell::Cell::new(name == "clone" && o.frozen.get()),
                });
                let nv = V::Obj(n);
                if it.find_method(&nv, "initialize_copy").is_some() {
                    it.call(&nv, "initialize_copy", Args::new(vec![recv.clone()]))?;
                }
                nv
            }
            other => other.clone(),
        },
        "tap" => {
            let b = blk_or_err(it, &a, "tap")?;
            yield1(it, b, recv.clone())?;
            recv.clone()
        }
        "then" | "yield_self" => {
            let b = blk_or_err(it, &a, "then")?;
            yield1(it, b, recv.clone())?
        }
        "itself" => recv.clone(),
        "to_s" => V::str(match recv {
            V::Obj(o) => format!("#<{}>", o.class.borrow().display_name()),
            other => to_s_plain(other),
        }),
        "inspect" => {
            if let V::Obj(o) = recv {
                let iv: Vec<(String, V)> = o
                    .ivars
                    .borrow()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let cname = o.class.borrow().display_name();
                if iv.is_empty() {
                    return some(V::str(format!("#<{cname}>")));
                }
                let mut parts = vec![];
                for (k, v) in iv {
                    parts.push(format!("{k}={}", rb_inspect(it, &v)?));
                }
                V::str(format!("#<{cname} {}>", parts.join(", ")))
            } else {
                V::str(rb_inspect(it, recv)?)
            }
        }
        "display" => V::Nil,
        "instance_variables_hash" => V::Nil,
        "enum_for" | "to_enum" => {
            let n = a
                .pos
                .first()
                .map(to_s_plain)
                .unwrap_or_else(|| "each".into());
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
            let mut rest = a.pos.clone();
            if !rest.is_empty() {
                rest.remove(0);
            }
            it.call(recv, &n, Args::with_blk(rest, Some(p)))?;
            let out = acc.borrow().clone();
            V::arr(out)
        }
        _ => return Ok(None),
    };
    some(v)
}
