//! ASCII / Unicode box-drawing generator.

use std::collections::HashMap;
use std::hash::BuildHasher;

use super::{get_field_constraint, sorted_by_key};
use crate::schema::edge::EdgeDefinition;
use crate::schema::table::TableDefinition;
use crate::schema::themes::{color_scheme_by_name, modern_color_scheme, ASCIITheme};
use crate::schema::utils::display_width;

/// A name as it may reach a terminal: control characters (ESC included) are
/// written as `\u{..}` escapes, so a stored name cannot recolour, retitle,
/// or rewrite the user's terminal.
fn terminal_text(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_control() {
                c.escape_unicode().to_string()
            } else {
                c.to_string()
            }
        })
        .collect()
}

/// Generate an ASCII-art diagram (with optional Unicode box drawing).
///
/// Mirrors `ASCIIGenerator.generate`. When `theme` is `None`, basic ASCII
/// characters are used and no colours / icons are applied. Control
/// characters in names are escaped, and box widths are measured in
/// terminal cells, so wide (CJK) names stay aligned.
#[must_use]
pub fn generate_ascii<S: BuildHasher>(
    tables: &HashMap<String, TableDefinition, S>,
    edges: &HashMap<String, EdgeDefinition, S>,
    include_fields: bool,
    include_edges: bool,
    theme: Option<&ASCIITheme>,
) -> String {
    let mut lines: Vec<String> = Vec::new();

    for (table_name, table) in sorted_by_key(tables) {
        let box_lines = build_ascii_table_box(table_name, table, include_fields, theme);
        lines.extend(box_lines);
        lines.push(String::new());
    }

    if include_edges && !edges.is_empty() {
        lines.push("Relationships:".to_string());
        lines.push("-".repeat(40));
        for (edge_name, edge) in sorted_by_key(edges) {
            let from_table = terminal_text(edge.from_table.as_deref().unwrap_or("?"));
            let to_table = terminal_text(edge.to_table.as_deref().unwrap_or("?"));
            lines.push(format!(
                "  {from_table} --[{}]--> {to_table}",
                terminal_text(edge_name)
            ));
        }
    }

    lines.join("\n")
}

/// Character set for drawing ASCII / Unicode box edges.
struct BoxChars {
    tl: &'static str,
    tr: &'static str,
    bl: &'static str,
    br: &'static str,
    h: &'static str,
    v: &'static str,
    ml: &'static str,
    mr: &'static str,
}

const ASCII_BOX: BoxChars = BoxChars {
    tl: "+",
    tr: "+",
    bl: "+",
    br: "+",
    h: "-",
    v: "|",
    ml: "+",
    mr: "+",
};

const UNICODE_SINGLE: BoxChars = BoxChars {
    tl: "\u{250C}",
    tr: "\u{2510}",
    bl: "\u{2514}",
    br: "\u{2518}",
    h: "\u{2500}",
    v: "\u{2502}",
    ml: "\u{251C}",
    mr: "\u{2524}",
};

const UNICODE_DOUBLE: BoxChars = BoxChars {
    tl: "\u{2554}",
    tr: "\u{2557}",
    bl: "\u{255A}",
    br: "\u{255D}",
    h: "\u{2550}",
    v: "\u{2551}",
    ml: "\u{2560}",
    mr: "\u{2563}",
};

const UNICODE_ROUNDED: BoxChars = BoxChars {
    tl: "\u{256D}",
    tr: "\u{256E}",
    bl: "\u{2570}",
    br: "\u{256F}",
    h: "\u{2500}",
    v: "\u{2502}",
    ml: "\u{251C}",
    mr: "\u{2524}",
};

const UNICODE_HEAVY: BoxChars = BoxChars {
    tl: "\u{250F}",
    tr: "\u{2513}",
    bl: "\u{2517}",
    br: "\u{251B}",
    h: "\u{2501}",
    v: "\u{2503}",
    ml: "\u{2523}",
    mr: "\u{252B}",
};

fn select_box_chars(theme: Option<&ASCIITheme>) -> &'static BoxChars {
    match theme {
        None => &ASCII_BOX,
        Some(t) if !t.use_unicode => &ASCII_BOX,
        Some(t) => match t.box_style {
            "double" => &UNICODE_DOUBLE,
            "rounded" => &UNICODE_ROUNDED,
            "heavy" => &UNICODE_HEAVY,
            _ => &UNICODE_SINGLE,
        },
    }
}

/// 24-bit ANSI foreground escape for a `#rrggbb` colour.
fn ansi_foreground(hex: &str) -> Option<String> {
    let hex = hex.strip_prefix('#')?;
    if hex.len() != 6 {
        return None;
    }
    let channel = |range: std::ops::Range<usize>| u8::from_str_radix(hex.get(range)?, 16).ok();
    Some(format!(
        "\u{1b}[38;2;{};{};{}m",
        channel(0..2)?,
        channel(2..4)?,
        channel(4..6)?
    ))
}

/// Wrap `text` in the theme's colour for `color_type`: PK / FK / UK markers
/// take the error / primary / accent colour of the theme's colour scheme
/// (16-colour codes when a colour is not `#rrggbb`), the header is bold.
fn colorize(text: &str, color_type: &str, theme: Option<&ASCIITheme>) -> String {
    let Some(theme) = theme.filter(|t| t.use_colors) else {
        return text.to_string();
    };
    let palette = color_scheme_by_name(theme.color_scheme).unwrap_or_else(modern_color_scheme);
    let (hex, fallback) = match color_type {
        "pk" => (palette.error, "\u{1b}[91m"),
        "fk" => (palette.primary, "\u{1b}[94m"),
        "uk" => (palette.accent, "\u{1b}[95m"),
        "header" => return format!("\u{1b}[1m{text}\u{1b}[0m"),
        _ => return text.to_string(),
    };
    let code = ansi_foreground(hex).unwrap_or_else(|| fallback.to_string());
    format!("{code}{text}\u{1b}[0m")
}

fn constraint_icon(constraint: &str, theme: Option<&ASCIITheme>) -> &'static str {
    let Some(theme) = theme else {
        return "";
    };
    if !theme.use_icons {
        return "";
    }
    match constraint {
        "PK" => "\u{1F511} ", // 🔑 + space
        "FK" => "\u{1F517} ", // 🔗 + space
        "UK" => "\u{2B50} ",  // ⭐ + space
        _ => "",
    }
}

fn center_pad(width: usize, visible_len: usize) -> (usize, usize) {
    let padding = width.saturating_sub(visible_len);
    let left = padding / 2;
    let right = padding - left;
    (left, right)
}

fn build_ascii_table_box(
    table_name: &str,
    table: &TableDefinition,
    include_fields: bool,
    theme: Option<&ASCIITheme>,
) -> Vec<String> {
    let chars = select_box_chars(theme);
    let table_name = terminal_text(table_name);

    // Build field lines.
    let mut field_lines: Vec<String> = Vec::new();
    if include_fields {
        let pk_icon = constraint_icon("PK", theme);
        let pk_text = colorize(&format!("{pk_icon}(PK)"), "pk", theme);
        field_lines.push(format!("id : string {pk_text}"));

        for field in &table.fields {
            let constraint = get_field_constraint(&field.name, table);
            let constraint_str = if constraint.is_empty() {
                String::new()
            } else {
                let icon = constraint_icon(constraint, theme);
                let color_type = match constraint {
                    "PK" => "pk",
                    "FK" => "fk",
                    "UK" => "uk",
                    _ => "field",
                };
                let inner = format!("{icon}({constraint})");
                format!(" {}", colorize(&inner, color_type, theme))
            };
            field_lines.push(format!(
                "{name} : {ty}{constraint_str}",
                name = terminal_text(&field.name),
                ty = field.field_type.as_str(),
            ));
        }
    }

    // Compute width in terminal cells.
    let min_width = std::cmp::max(display_width(&table_name) + 4, 20);
    let content_width = field_lines
        .iter()
        .map(|l| display_width(l))
        .max()
        .unwrap_or(0);
    let width = std::cmp::max(min_width, content_width + 2);
    let rule = |left: &str, right: &str| format!("{left}{}{right}", chars.h.repeat(width));

    let mut out: Vec<String> = vec![rule(chars.tl, chars.tr)];

    // Header row: centred, optional bold.
    let styled_name = colorize(&table_name, "header", theme);
    let (left, right) = center_pad(width, display_width(&styled_name));
    out.push(format!(
        "{v}{l}{name}{r}{v}",
        v = chars.v,
        l = " ".repeat(left),
        name = styled_name,
        r = " ".repeat(right),
    ));

    if include_fields {
        out.push(rule(chars.ml, chars.mr));
        for line in &field_lines {
            // Leading space + content + trailing padding = width.
            let padding = width.saturating_sub(display_width(line) + 1);
            out.push(format!(
                "{v} {line}{pad}{v}",
                v = chars.v,
                pad = " ".repeat(padding)
            ));
        }
    }

    out.push(rule(chars.bl, chars.br));
    out
}
