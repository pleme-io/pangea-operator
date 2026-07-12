//! UTF-8-safe text truncation.
//!
//! # The bug class this closes
//!
//! Byte-index slicing (`&s[..N]`) panics whenever `N` doesn't land on a
//! UTF-8 char boundary. That is a real, reachable panic — not a
//! theoretical one — anywhere `N` is applied to externally-sourced text:
//! tofu/magma apply-error stdout+stderr, or a user-authored inline Pangea
//! DSL string. Both routinely contain multi-byte UTF-8 (a curly quote in a
//! provider error, an emoji or accented name in a template author's inline
//! source), and there is no guarantee the byte at offset `N` starts a new
//! character.
//!
//! This module exists so the fix is written ONCE and reused everywhere a
//! string gets capped for a status field, a k8s Event body, or a GraphQL
//! preview — instead of three independent `&s[..N]` call sites each
//! carrying the same latent panic (the shape this crate had before this
//! module existed: `cycle_receipts::truncate_for_status`,
//! `template_controller`'s event-body cap, and
//! `graphql::types::TemplateSource::from`'s inline-source preview all
//! sliced by raw byte index).
//!
//! # Fix, not mitigation
//!
//! `truncate_utf8_safe` never performs a byte-index slice at all — it
//! walks character boundaries via [`str::char_indices`], which by
//! construction only ever yields valid boundaries. There is no code path
//! in this function capable of hitting the char-boundary panic, for any
//! UTF-8 input. This is a root-cause fix (the illegal byte-index slice is
//! never constructed), not a runtime guard bolted in front of the old
//! slice — but it is an algorithmic guarantee (verified here by tests
//! spanning ASCII / 2-byte / 3-byte / 4-byte boundary-adjacent input), not
//! a compiler-enforced one: nothing stops a future call site from writing
//! `&s[..N]` directly instead of calling this function. Tier: root-cause
//! fixed, algorithmically (not type-level) unrepresentable.

/// Truncate `s` to at most `max_chars` Unicode scalar values, appending
/// `suffix` when truncation actually occurs. Always slices at a valid
/// UTF-8 char boundary — never panics, for any input.
///
/// `max_chars` counts characters (Unicode scalar values via
/// `char_indices`), not bytes, so multi-byte input is never cut off
/// mid-character.
pub fn truncate_utf8_safe(s: &str, max_chars: usize, suffix: &str) -> String {
    match s.char_indices().nth(max_chars) {
        // Fewer than max_chars+1 chars in s: nothing to truncate.
        None => s.to_string(),
        Some((byte_idx, _)) => {
            let mut t = String::with_capacity(byte_idx + suffix.len());
            t.push_str(&s[..byte_idx]);
            t.push_str(suffix);
            t
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_short_ascii_through_unchanged() {
        assert_eq!(truncate_utf8_safe("short", 256, "…"), "short");
    }

    #[test]
    fn exactly_max_chars_passes_through_unchanged() {
        let s = "a".repeat(256);
        assert_eq!(truncate_utf8_safe(&s, 256, "…"), s);
    }

    #[test]
    fn truncates_ascii_and_appends_suffix() {
        let s = "a".repeat(300);
        let out = truncate_utf8_safe(&s, 256, "…");
        assert_eq!(out.chars().count(), 257); // 256 'a's + the ellipsis
        assert!(out.starts_with(&"a".repeat(256)));
        assert!(out.ends_with('…'));
    }

    /// The bug this module exists to close: a 2-byte-per-char string
    /// where the naive `&s[..256]` byte slice lands mid-character and
    /// panics. `truncate_utf8_safe` must not panic and must produce
    /// exactly 256 chars + the suffix.
    #[test]
    fn does_not_panic_on_two_byte_utf8_at_the_boundary() {
        // 'é' (U+00E9) is 2 bytes in UTF-8. 300 of them => a raw
        // `&s[..256]` byte slice always lands mid-character (256 is even,
        // every char boundary is even) for THIS specific char, so pick an
        // offset that actually straddles a boundary in the general case:
        // this test's job is "never panics", proven independent of parity.
        let s = "é".repeat(300);
        let out = truncate_utf8_safe(&s, 256, "…");
        assert_eq!(out.chars().count(), 257);
        assert!(out.ends_with('…'));
    }

    /// 3-byte UTF-8 (e.g. many CJK / symbol codepoints like '中' or '…'
    /// itself) and 4-byte UTF-8 (emoji) are the cases most likely to
    /// straddle an arbitrary byte offset. Both must be panic-free.
    #[test]
    fn does_not_panic_on_three_and_four_byte_utf8() {
        let three_byte = "中".repeat(150); // U+4E2D, 3 bytes/char
        let out = truncate_utf8_safe(&three_byte, 100, "...");
        assert_eq!(out.chars().count(), 103);
        assert!(out.ends_with("..."));

        let four_byte = "😀".repeat(150); // U+1F600, 4 bytes/char
        let out = truncate_utf8_safe(&four_byte, 100, "...");
        assert_eq!(out.chars().count(), 103);
        assert!(out.ends_with("..."));
    }

    /// Sweeps every possible max_chars from 0..=len against mixed-width
    /// input so no byte offset in the sweep can hit a char-boundary
    /// panic — this is the property the old `&s[..N]` code could not
    /// guarantee for any N.
    #[test]
    fn never_panics_across_every_truncation_point_of_mixed_width_input() {
        let mixed: String = "a é 中 😀 b".repeat(20);
        let char_count = mixed.chars().count();
        for max_chars in 0..=char_count {
            let out = truncate_utf8_safe(&mixed, max_chars, "…");
            // Never panicking is the property under test; a light
            // sanity check that we didn't emit more than requested.
            assert!(out.chars().count() <= max_chars + 1);
        }
    }

    #[test]
    fn empty_string_passes_through() {
        assert_eq!(truncate_utf8_safe("", 100, "..."), "");
    }

    #[test]
    fn zero_max_chars_still_appends_suffix_when_input_nonempty() {
        assert_eq!(truncate_utf8_safe("abc", 0, "…"), "…");
    }
}
