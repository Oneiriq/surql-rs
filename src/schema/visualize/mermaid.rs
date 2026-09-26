//! Mermaid ER-diagram generator.

use std::collections::HashMap;
use std::hash::BuildHasher;

use super::{get_field_constraint, sorted_by_key, without_controls};
use crate::schema::edge::EdgeDefinition;
use crate::schema::table::TableDefinition;
use crate::schema::themes::MermaidTheme;

/// Text for a Mermaid double-quoted string (entity name, relationship label,
/// attribute comment). Mermaid has no escape inside those strings, and its
/// parser (checked against 11.x) rejects `"`, `\`, `%` in entity names and
/// `~~` in comments, and decodes `#...;` entity codes, so those characters
/// are swapped for look-alikes and line breaks for spaces.
fn mermaid_text(text: &str) -> String {
    without_controls(text)
        .chars()
        .map(|c| match c {
            '"' => '\'',
            '\\' => '\u{FF3C}',
            '%' => '\u{FF05}',
            '#' => '\u{FF03}',
            '~' => '\u{FF5E}',
            _ => c,
        })
        .collect()
}

/// A Mermaid double-quoted name (entity or relationship label).
fn mermaid_name(name: &str) -> String {
    format!("\"{}\"", mermaid_text(name))
}

/// A field name reduced to the Mermaid attribute-name shape: a leading ASCII
/// letter or `_`, then letters, digits, `_` and `-`.
fn mermaid_ident(name: &str) -> String {
    let ident: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if ident.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
        ident
    } else {
        format!("f_{ident}")
    }
}

/// One `type name [KEY] ["comment"]` attribute line. A name the grammar
/// cannot take verbatim (`address.city`, `tags[*]`) is rewritten, and the
/// real name rides along as the comment, which must come last.
fn mermaid_attribute_line(type_name: &str, name: &str, constraint: &str) -> String {
    let ident = mermaid_ident(name);
    let mut line = format!("        {type_name} {ident}");
    if !constraint.is_empty() {
        line.push(' ');
        line.push_str(constraint);
    }
    if ident != name {
        line.push(' ');
        line.push_str(&mermaid_name(name));
    }
    line
}

/// Generate a Mermaid ER diagram.
///
/// Mirrors `MermaidGenerator.generate`. When `theme` is `Some`, a
/// `%%{init: ...}%%` directive with that theme name prefaces the output;
/// when `None`, the diagram begins directly with `erDiagram`.
///
/// Entity names and relationship labels are always double-quoted; attribute
/// names that are not plain identifiers (`address.city`, `tags[*]`) are
/// rewritten with the original kept as the attribute comment.
#[must_use]
pub fn generate_mermaid<S: BuildHasher>(
    tables: &HashMap<String, TableDefinition, S>,
    edges: &HashMap<String, EdgeDefinition, S>,
    include_fields: bool,
    include_edges: bool,
    theme: Option<&MermaidTheme>,
) -> String {
    let mut lines: Vec<String> = Vec::new();

    if let Some(theme) = theme {
        lines.push(mermaid_init_directive(theme));
    }

    lines.push("erDiagram".to_string());

    // Table entities (sorted by name).
    for (table_name, table) in sorted_by_key(tables) {
        lines.push(format!("    {} {{", mermaid_name(table_name)));
        if include_fields {
            // Always emit the implicit id field.
            lines.push("        string id PK".to_string());
            for field in &table.fields {
                lines.push(mermaid_attribute_line(
                    field.field_type.as_str(),
                    &field.name,
                    get_field_constraint(&field.name, table),
                ));
            }
        }
        lines.push("    }".to_string());
    }

    // Edge entities (also tables in SurrealDB) — only if they have fields.
    for (edge_name, edge) in sorted_by_key(edges) {
        if include_fields && !edge.fields.is_empty() {
            lines.push(format!("    {} {{", mermaid_name(edge_name)));
            for field in &edge.fields {
                lines.push(mermaid_attribute_line(
                    field.field_type.as_str(),
                    &field.name,
                    "",
                ));
            }
            lines.push("    }".to_string());
        }
    }

    if include_edges {
        lines.push(String::new());
        for (edge_name, edge) in sorted_by_key(edges) {
            let from_table = edge.from_table.as_deref().unwrap_or("unknown");
            let to_table = edge.to_table.as_deref().unwrap_or("unknown");

            // Skip edges whose endpoints are not known tables (matches Python).
            if !tables.contains_key(from_table) && from_table != "unknown" {
                continue;
            }
            if !tables.contains_key(to_table) && to_table != "unknown" {
                continue;
            }

            let cardinality = infer_mermaid_cardinality(edge);
            lines.push(format!(
                "    {} {cardinality} {} : {}",
                mermaid_name(from_table),
                mermaid_name(to_table),
                mermaid_name(edge_name),
            ));
        }
    }

    lines.join("\n")
}

/// `true` for a value safe to splice into the single-quoted JSON-ish init
/// directive: a `#hex` colour or a plain word.
fn is_plain_theme_value(value: &str) -> bool {
    let body = value.strip_prefix('#').unwrap_or(value);
    !body.is_empty() && body.chars().all(|c| c.is_ascii_alphanumeric())
}

/// The `%%{init: ...}%%` directive for a theme. With `use_custom_css` the
/// primary / secondary colours ride along as `themeVariables`. A theme
/// name or colour that is not a plain word / `#hex` value is left out
/// rather than allowed to end the directive early.
fn mermaid_init_directive(theme: &MermaidTheme) -> String {
    let name = if is_plain_theme_value(theme.theme_name) && !theme.theme_name.starts_with('#') {
        theme.theme_name
    } else {
        "default"
    };
    let variables: Vec<String> = [
        ("primaryColor", theme.primary_color),
        ("secondaryColor", theme.secondary_color),
    ]
    .into_iter()
    .filter(|(_, value)| theme.use_custom_css && is_plain_theme_value(value))
    .map(|(key, value)| format!("'{key}':'{value}'"))
    .collect();
    if variables.is_empty() {
        format!("%%{{init: {{'theme':'{name}'}}}}%%")
    } else {
        format!(
            "%%{{init: {{'theme':'{name}', 'themeVariables': {{{}}}}}}}%%",
            variables.join(", ")
        )
    }
}

fn infer_mermaid_cardinality(edge: &EdgeDefinition) -> &'static str {
    if let (Some(from), Some(to)) = (&edge.from_table, &edge.to_table) {
        if from == to {
            return "}o--o{";
        }
    }
    "||--o{"
}
