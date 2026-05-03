//! Pure string-escaping helpers used by the embedded-ruby owner
//! thread. Lifted out of `owner.rs` during U1 so the eval-safety
//! contract can be tested without the `embedded_ruby` feature
//! (which requires system Ruby + magnus + rb-sys).
//!
//! These functions have no magnus / pangea-ruby-eval dependency —
//! they're plain string transforms. Keeping them here means CI
//! covers them on the default-feature build; only the magnus-using
//! glue lives behind `embedded_ruby`.

/// Render a Rust string as a Ruby double-quoted string literal,
/// escaping the bare minimum for eval-safety. Used to embed an
/// arbitrary workspace template body inside a wrapper Ruby program
/// that the owner thread evaluates.
///
/// **Security contract**: every character that Ruby would interpret
/// specially inside a `"..."` literal must be escaped. The most
/// critical escape is `#` → `\#` to defeat Ruby's `#{...}` string
/// interpolation; without it, an unescaped `#{` in user-supplied
/// input becomes Ruby code executed at eval time (arbitrary-RCE-
/// class bug). `ruby_string_literal_lifts_eval_injection` test
/// in this module locks the contract in.
pub fn ruby_string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0' => out.push_str("\\0"),
            // `#` is escaped to defeat Ruby's `#{}` interpolation.
            '#' => out.push_str("\\#"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::ruby_string_literal;

    // ── ruby_string_literal: eval-safety contract ──
    //
    // We embed arbitrary user-supplied template bodies inside a
    // wrapper Ruby program that the owner thread evals. Drift in the
    // escaping rules turns this into an injection vector — most
    // critically, an unescaped `#` in user input would activate Ruby
    // string interpolation and execute arbitrary embedded
    // expressions. These tests lock in the per-character contract.

    #[test]
    fn empty_string_is_empty_literal() {
        assert_eq!(ruby_string_literal(""), "\"\"");
    }

    #[test]
    fn plain_ascii_passes_through() {
        assert_eq!(ruby_string_literal("hello world"), "\"hello world\"");
    }

    #[test]
    fn double_quote_is_escaped() {
        assert_eq!(ruby_string_literal("a\"b"), "\"a\\\"b\"");
    }

    #[test]
    fn backslash_is_escaped() {
        assert_eq!(ruby_string_literal("a\\b"), "\"a\\\\b\"");
    }

    #[test]
    fn newline_carriage_return_tab_become_escape_sequences() {
        assert_eq!(ruby_string_literal("a\nb"), "\"a\\nb\"");
        assert_eq!(ruby_string_literal("a\rb"), "\"a\\rb\"");
        assert_eq!(ruby_string_literal("a\tb"), "\"a\\tb\"");
    }

    #[test]
    fn ruby_string_literal_lifts_eval_injection() {
        // The most security-critical escape in this helper.
        // Without it, `#{...}` in user input becomes Ruby code
        // executed at eval time. Drift here is an arbitrary-RCE
        // class of bug.
        assert_eq!(
            ruby_string_literal("payload-#{system('rm -rf /')}"),
            "\"payload-\\#{system('rm -rf /')}\""
        );
    }

    #[test]
    fn null_byte_is_escaped() {
        assert_eq!(ruby_string_literal("a\0b"), "\"a\\0b\"");
    }

    #[test]
    fn other_control_chars_get_hex_escapes() {
        // Bell character (0x07) — non-printable, gets \x07.
        assert_eq!(ruby_string_literal("\x07"), "\"\\x07\"");
        // Vertical tab (0x0b).
        assert_eq!(ruby_string_literal("\x0b"), "\"\\x0b\"");
    }

    #[test]
    fn unicode_passes_through() {
        // Non-ASCII Unicode is not escaped — it's emitted as-is in
        // the literal because Ruby's default source encoding is
        // UTF-8 and we expect callers to feed UTF-8.
        assert_eq!(ruby_string_literal("café"), "\"café\"");
        assert_eq!(ruby_string_literal("日本語"), "\"日本語\"");
    }

    #[test]
    fn every_special_character_in_one_string() {
        // Belt-and-braces: a string containing every single character
        // that Ruby would interpret specially. The output must be
        // safe to eval.
        let evil = "\\\"\n\r\t\0#{}";
        let lit = ruby_string_literal(evil);
        assert_eq!(lit, "\"\\\\\\\"\\n\\r\\t\\0\\#{}\"");
        // Sanity: the literal starts and ends with a quote.
        assert!(lit.starts_with('"'));
        assert!(lit.ends_with('"'));
    }
}
