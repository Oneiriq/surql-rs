//! Shared schema-layer utilities.
//!
//! Port of the non-database helpers from `surql/schema/utils.py` and the
//! display-width / ANSI-stripping primitives inlined in
//! `surql/schema/visualize.py`. The async `fetch_db_tables` helper is tied to
//! the Python `DatabaseClient`; the Rust client API lands in a separate
//! milestone, so this module only exposes the format-agnostic helpers the
//! visualiser (and any future callers) rely on.
//!
//! ## Examples
//!
//! ```
//! use surql::schema::utils::{display_width, strip_ansi};
//!
//! // ANSI escapes are stripped, regular chars counted once.
//! assert_eq!(strip_ansi("\u{1b}[1;31mhello\u{1b}[0m"), "hello");
//! assert_eq!(display_width("\u{1b}[1;31mhello\u{1b}[0m"), 5);
//! ```

use std::sync::OnceLock;

use regex::Regex;

fn ansi_escape_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\x1b\[[0-9;]*m").expect("valid ANSI regex"))
}

/// Remove ANSI SGR escape codes from a string slice.
///
/// Mirrors the `_ANSI_ESCAPE_PATTERN.sub('', text)` step in Python's
/// `_get_display_width` helper.
///
/// ## Examples
///
/// ```
/// use surql::schema::utils::strip_ansi;
///
/// assert_eq!(strip_ansi("\u{1b}[31merror\u{1b}[0m"), "error");
/// assert_eq!(strip_ansi("plain"), "plain");
/// ```
#[must_use]
pub fn strip_ansi(text: &str) -> String {
    ansi_escape_regex().replace_all(text, "").into_owned()
}

/// Calculate terminal display width, stripping ANSI and counting wide chars.
///
/// Python's `len()` counts emoji as 1 character, but terminals display them
/// as 2 columns wide. This function returns the actual terminal display
/// width by checking each character's East Asian Width property (`W` / `F`
/// count as 2, everything else as 1).
///
/// ## Examples
///
/// ```
/// use surql::schema::utils::display_width;
///
/// assert_eq!(display_width("hi"), 2);
/// // ANSI codes are ignored.
/// assert_eq!(display_width("\u{1b}[1mhi\u{1b}[0m"), 2);
/// // Emoji (Wide) counts as 2 columns.
/// assert_eq!(display_width("\u{1F511}"), 2);
/// ```
#[must_use]
pub fn display_width(text: &str) -> usize {
    let stripped = strip_ansi(text);
    stripped.chars().map(char_display_width).sum()
}

/// Width in terminal cells for a single character.
///
/// Returns 2 for East Asian Wide (`W`) and Fullwidth (`F`) characters, 1 for
/// everything else (including Ambiguous and Narrow). This mirrors the Python
/// `unicodedata.east_asian_width` branch used by `_get_display_width`.
#[must_use]
pub fn char_display_width(c: char) -> usize {
    if is_east_asian_wide_or_full(c) {
        2
    } else {
        1
    }
}

/// Return `true` when the character's East Asian Width property is `W` or `F`.
///
/// The ranges below cover the blocks the visualiser actually encounters
/// (CJK, fullwidth forms, emoji) which is sufficient for reproducing the
/// Python display-width calculation on the constraint icons (`🔑`, `🔗`, `⭐`)
/// and on any user-supplied wide text.
fn is_east_asian_wide_or_full(c: char) -> bool {
    let cp = u32::from(c);
    matches!(cp,
        // Hangul Jamo extended range used for wide forms.
        0x1100..=0x115F
        // CJK Misc / Kangxi / Radicals Supplement / Ideographic Description
        | 0x2E80..=0x303E
        // Hiragana / Katakana / Bopomofo / Hangul compatibility / Kanbun
        | 0x3041..=0x33FF
        // CJK Unified Ideographs Extension A
        | 0x3400..=0x4DBF
        // CJK Unified Ideographs
        | 0x4E00..=0x9FFF
        // Yi Syllables / Yi Radicals
        | 0xA000..=0xA4CF
        // Hangul Syllables
        | 0xAC00..=0xD7A3
        // CJK Compatibility Ideographs
        | 0xF900..=0xFAFF
        // Vertical forms / CJK Compatibility Forms / Small Form Variants
        | 0xFE10..=0xFE6F
        // Fullwidth forms & half-width katakana
        | 0xFF00..=0xFF60
        | 0xFFE0..=0xFFE6
        // Emoji block ranges (wide at terminal)
        | 0x1F300..=0x1F64F
        | 0x1F680..=0x1F6FF
        | 0x1F700..=0x1F77F
        | 0x1F780..=0x1F7FF
        | 0x1F800..=0x1F8FF
        | 0x1F900..=0x1F9FF
        | 0x1FA00..=0x1FA6F
        | 0x1FA70..=0x1FAFF
        // Miscellaneous Symbols (contains ⭐ U+2B50 via later range)
        | 0x2600..=0x27BF
        | 0x2B00..=0x2BFF
        // CJK Extensions B–F and Supplement
        | 0x20000..=0x2FFFD
        | 0x30000..=0x3FFFD
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_ansi_removes_color_codes() {
        assert_eq!(strip_ansi("\u{1b}[31mhi\u{1b}[0m"), "hi");
        assert_eq!(strip_ansi("\u{1b}[1;91mbold red\u{1b}[0m"), "bold red");
    }

    #[test]
    fn strip_ansi_preserves_plain_text() {
        assert_eq!(strip_ansi("plain"), "plain");
        assert_eq!(strip_ansi(""), "");
    }

    #[test]
    fn display_width_plain_ascii() {
        assert_eq!(display_width(""), 0);
        assert_eq!(display_width("abc"), 3);
    }

    #[test]
    fn display_width_strips_ansi() {
        assert_eq!(display_width("\u{1b}[31mhi\u{1b}[0m"), 2);
    }

    #[test]
    fn display_width_handles_emoji_keys() {
        // 🔑 U+1F511 is wide.
        assert_eq!(display_width("\u{1F511}"), 2);
        // 🔗 U+1F517 is wide.
        assert_eq!(display_width("\u{1F517}"), 2);
        // ⭐ U+2B50 is wide.
        assert_eq!(display_width("\u{2B50}"), 2);
    }

    #[test]
    fn display_width_handles_cjk() {
        // 你好 — two CJK ideographs, each width 2.
        assert_eq!(display_width("你好"), 4);
    }

    #[test]
    fn char_display_width_matches_table() {
        assert_eq!(char_display_width('a'), 1);
        assert_eq!(char_display_width('0'), 1);
        assert_eq!(char_display_width(' '), 1);
        assert_eq!(char_display_width('你'), 2);
        assert_eq!(char_display_width('\u{1F511}'), 2);
    }
}
