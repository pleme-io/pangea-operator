use crate::value::*;

pub fn float_to_s(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    if f == 0.0 {
        return if f.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }
    let abs = f.abs();
    if (1e-4..1e16).contains(&abs) {
        let s = format!("{f}");
        if s.contains('.') {
            s
        } else {
            format!("{s}.0")
        }
    } else {
        let s = format!("{f:e}");
        let (mant, exp) = s.split_once('e').unwrap();
        let mant = if mant.contains('.') {
            mant.to_string()
        } else {
            format!("{mant}.0")
        };
        let e: i32 = exp.parse().unwrap();
        format!("{mant}e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs())
    }
}

pub fn str_inspect(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\t' => o.push_str("\\t"),
            '\r' => o.push_str("\\r"),
            '\u{1b}' => o.push_str("\\e"),
            '\0' => o.push_str("\\0"),
            '#' if matches!(chars.peek(), Some('{') | Some('$') | Some('@')) => o.push_str("\\#"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\x{:02X}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

pub fn sym_inspect(s: &str) -> String {
    let simple = !s.is_empty()
        && (s
            .chars()
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_' || c == '@' || c == '$')
            && s.trim_start_matches(['@', '$'])
                .chars()
                .enumerate()
                .all(|(i, c)| {
                    c.is_alphanumeric()
                        || c == '_'
                        || ((c == '?' || c == '!' || c == '=')
                            && i == s.trim_start_matches(['@', '$']).chars().count() - 1)
                }))
        || matches!(
            s,
            "+" | "-"
                | "*"
                | "/"
                | "%"
                | "=="
                | "!="
                | "<"
                | ">"
                | "<="
                | ">="
                | "<=>"
                | "==="
                | "[]"
                | "[]="
                | "!"
                | "=~"
                | "<<"
                | ">>"
                | "**"
                | "&"
                | "|"
                | "^"
                | "~"
                | "+@"
                | "-@"
        );
    if simple {
        format!(":{s}")
    } else {
        format!(":{}", str_inspect(s))
    }
}

pub fn json_escape(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

pub fn hkey_is_sym(k: &HKey) -> bool {
    matches!(k, HKey::Sym(_))
}
