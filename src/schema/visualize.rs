//! Schema visualization: Mermaid, GraphViz (DOT), and ASCII art diagrams.
//!
//! Port of `surql/schema/visualize.py`. Generates visual diagrams of database
//! schemas from [`TableDefinition`] / [`EdgeDefinition`] values (either
//! supplied directly or pulled from the global [`SchemaRegistry`](crate::schema::registry::SchemaRegistry)) in any of
//! three formats:
//!
//! - **Mermaid** — ER-diagram syntax for rendering in Markdown or the Mermaid
//!   live editor.
//! - **GraphViz** — DOT format that can be rendered with `dot`, `neato`, etc.
//! - **ASCII** — plain-text box diagrams suitable for terminals and READMEs.
//!
//! All three paths accept a matching theme (see [`themes`](super::themes)) to
//! customise colours, fonts, and styling. Omit the theme (pass `None`) for
//! default rendering.
//!
//! Names are data, not syntax: a table, field, or edge name may hold any
//! character the database accepts, so every generator escapes what it
//! interpolates. Mermaid entity names and relationship labels are
//! double-quoted and attribute names reduced to the grammar's identifier
//! shape (the real name rides along as the attribute comment); GraphViz IDs
//! are always quoted, record labels escape `{ } | < >`, and HTML labels are
//! HTML-escaped; ASCII output escapes control characters so a name cannot
//! carry terminal escape sequences.
//!
//! ## Examples
//!
//! ```
//! use surql::schema::{
//!     string_field, table_schema, unique_index, TableDefinition,
//! };
//! use surql::schema::visualize::{generate_mermaid, OutputFormat, visualize_schema};
//! use std::collections::HashMap;
//!
//! let mut tables = HashMap::new();
//! let (email, _) = string_field("email").build().unwrap();
//! let user = table_schema("user").with_fields([email]);
//! tables.insert("user".to_string(), user);
//!
//! let diagram = generate_mermaid(&tables, &HashMap::new(), true, true, None);
//! assert!(diagram.starts_with("erDiagram"));
//!
//! // Or use the unified dispatch with no theme:
//! let also = visualize_schema(&tables, None, OutputFormat::Mermaid, true, true, None).unwrap();
//! assert_eq!(diagram, also);
//! ```

use std::collections::HashMap;
use std::hash::BuildHasher;

use crate::error::Result;

use super::edge::EdgeDefinition;
use super::fields::FieldType;
use super::registry::get_registry;
use super::table::{IndexType, TableDefinition};
use super::themes::{
    color_scheme_by_name, modern_color_scheme, ASCIITheme, ColorScheme, GraphVizTheme, MermaidTheme,
};
use super::utils::display_width;

// ---------------------------------------------------------------------------
// Output format
// ---------------------------------------------------------------------------

/// Output format for schema visualization diagrams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputFormat {
    /// Mermaid ER-diagram text.
    Mermaid,
    /// GraphViz DOT text.
    GraphViz,
    /// ASCII art (plain text with optional Unicode box drawing).
    Ascii,
}

// ---------------------------------------------------------------------------
// Constraint / type helpers
// ---------------------------------------------------------------------------

fn get_field_constraint(field_name: &str, table: &TableDefinition) -> &'static str {
    if field_name == "id" {
        return "PK";
    }
    for idx in &table.indexes {
        if idx.index_type == IndexType::Unique && idx.columns.iter().any(|c| c == field_name) {
            return "UK";
        }
    }
    for f in &table.fields {
        if f.name == field_name && f.field_type == FieldType::Record {
            return "FK";
        }
    }
    ""
}

fn sorted_by_key<V, S: BuildHasher>(map: &HashMap<String, V, S>) -> Vec<(&String, &V)> {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

/// Replace every control character (line breaks included) with a space.
fn without_controls(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

// ---------------------------------------------------------------------------
// Mermaid
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// GraphViz
// ---------------------------------------------------------------------------

/// A DOT quoted string (ID or attribute value): `"` and `\` escaped, so a
/// name can neither end the string nor turn into an escape sequence, and
/// keywords such as `node` / `edge` / `graph` stay plain IDs.
fn dot_quoted(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in without_controls(text).chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// Text inside a record label, with the record metacharacters escaped. The
/// result still goes through [`dot_quoted`]-style quoting by the caller, so
/// `"` is escaped here as well.
fn record_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in without_controls(text).chars() {
        if matches!(c, '\\' | '"' | '{' | '}' | '|' | '<' | '>') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Text inside an HTML-like label.
fn html_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in without_controls(text).chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

fn is_record_shape(shape: &str) -> bool {
    shape.eq_ignore_ascii_case("record") || shape.eq_ignore_ascii_case("mrecord")
}

/// Generate a GraphViz DOT-format diagram.
///
/// Mirrors `GraphVizGenerator.generate`. When `theme` is `None`, a
/// backward-compatible rendering matching Python's default (no gradients,
/// plain `node [shape=record]`) is produced.
///
/// Every node ID is a quoted DOT string, so names that are DOT keywords or
/// contain punctuation stay node names; record labels escape `{ } | < >`
/// and HTML labels are HTML-escaped.
#[must_use]
pub fn generate_graphviz<S: BuildHasher>(
    tables: &HashMap<String, TableDefinition, S>,
    edges: &HashMap<String, EdgeDefinition, S>,
    include_fields: bool,
    include_edges: bool,
    theme: Option<&GraphVizTheme>,
) -> String {
    let default_theme = GraphVizTheme::default().backward_compatible();
    let theme = theme.unwrap_or(&default_theme);

    let mut lines: Vec<String> = Vec::new();
    lines.push("digraph schema {".to_string());
    lines.push("    rankdir=LR;".to_string());

    // Python matches on (use_gradients or node_style != 'filled,rounded'): the
    // rounded filled style is the "rich" default. Minimal theme sets
    // node_style='filled' which triggers the rich path too.
    let rich = theme.use_gradients || theme.node_style != "filled,rounded";
    let record_shape = !rich || is_record_shape(theme.node_shape);
    if rich {
        if theme.bg_color != "transparent" {
            lines.push(format!("    bgcolor={};", dot_quoted(theme.bg_color)));
        }
        lines.push(format!("    fontname={};", dot_quoted(theme.font_name)));

        let mut node_attrs = Vec::<String>::new();
        node_attrs.push(format!("shape={}", dot_quoted(theme.node_shape)));
        if !theme.node_style.is_empty() {
            node_attrs.push(format!("style={}", dot_quoted(theme.node_style)));
        }
        node_attrs.push(format!("fontname={}", dot_quoted(theme.font_name)));
        node_attrs.push("pad=\"0.5\"".to_string());
        node_attrs.push("margin=\"0.2\"".to_string());
        lines.push(format!("    node [{}];", node_attrs.join(", ")));

        let mut edge_attrs = Vec::<String>::new();
        edge_attrs.push(format!("color={}", dot_quoted(theme.edge_color)));
        if !theme.edge_style.is_empty() {
            edge_attrs.push(format!("style={}", dot_quoted(theme.edge_style)));
        }
        edge_attrs.push(format!("fontname={}", dot_quoted(theme.font_name)));
        lines.push(format!("    edge [{}];", edge_attrs.join(", ")));
    } else {
        lines.push("    node [shape=record];".to_string());
    }

    lines.push(String::new());

    // Table nodes (sorted).
    let table_nodes: Vec<String> = sorted_by_key(tables)
        .into_iter()
        .map(|(table_name, table)| {
            let label =
                build_graphviz_table_label(table_name, table, include_fields, record_shape, theme);
            format!("{} [label={label}];", dot_quoted(table_name))
        })
        .collect();

    // Edge nodes (only if they have fields and include_fields).
    let edge_nodes: Vec<String> = sorted_by_key(edges)
        .into_iter()
        .filter(|(_, edge)| include_fields && !edge.fields.is_empty())
        .map(|(edge_name, edge)| {
            let label = build_graphviz_edge_label(edge_name, edge, theme);
            format!("{} [label={label}];", dot_quoted(edge_name))
        })
        .collect();

    for (cluster, title, nodes) in [
        ("cluster_tables", "Tables", table_nodes),
        ("cluster_edges", "Edges", edge_nodes),
    ] {
        if theme.use_clusters && !nodes.is_empty() {
            lines.push(format!("    subgraph {cluster} {{"));
            lines.push(format!("        label={};", dot_quoted(title)));
            lines.extend(nodes.iter().map(|node| format!("        {node}")));
            lines.push("    }".to_string());
        } else {
            lines.extend(nodes.iter().map(|node| format!("    {node}")));
        }
    }

    lines.push(String::new());

    // Relationships.
    if include_edges {
        for (edge_name, edge) in sorted_by_key(edges) {
            let (Some(from_table), Some(to_table)) =
                (edge.from_table.as_deref(), edge.to_table.as_deref())
            else {
                continue;
            };
            if !tables.contains_key(from_table) || !tables.contains_key(to_table) {
                continue;
            }
            let edge_style = graphviz_edge_style(edge, theme);
            lines.push(format!(
                "    {} -> {} [label={}{edge_style}];",
                dot_quoted(from_table),
                dot_quoted(to_table),
                dot_quoted(edge_name),
            ));
        }
    }

    lines.push("}".to_string());
    lines.join("\n")
}

/// A record label: `"{title|row\l|row\l}"` with every piece escaped.
fn record_label(title: &str, rows: &[String]) -> String {
    let mut parts = Vec::with_capacity(rows.len() + 1);
    parts.push(record_text(title));
    parts.extend(rows.iter().cloned());
    format!("\"{{{}}}\"", parts.join("|"))
}

fn build_graphviz_table_label(
    table_name: &str,
    table: &TableDefinition,
    include_fields: bool,
    record_shape: bool,
    theme: &GraphVizTheme,
) -> String {
    if !include_fields {
        // A record-shaped node parses even a bare label as record syntax.
        return if record_shape {
            format!("\"{}\"", record_text(table_name))
        } else {
            dot_quoted(table_name)
        };
    }

    if theme.use_gradients {
        return build_graphviz_html_label(table_name, table, theme);
    }

    // Plain record label: "{name|id : string (PK)\\l|field : ty\\l|...}"
    let mut rows = vec!["id : string (PK)\\l".to_string()];
    rows.extend(table.fields.iter().map(|field| {
        let constraint = get_field_constraint(&field.name, table);
        let constraint_str = if constraint.is_empty() {
            String::new()
        } else {
            format!(" ({constraint})")
        };
        format!(
            "{name} : {ty}{constraint_str}\\l",
            name = record_text(&field.name),
            ty = field.field_type.as_str(),
        )
    }));
    record_label(table_name, &rows)
}

/// The `<TABLE>` wrapper of an HTML-like label around a header and rows.
fn html_label(header_bg: &str, title: &str, rows: &[String]) -> String {
    format!(
        "<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\
         <TR><TD BGCOLOR=\"{bg}\" COLSPAN=\"2\"><FONT COLOR=\"#FFFFFF\"><B>{title}</B></FONT></TD></TR>\
         {rows}</TABLE>>",
        bg = html_text(header_bg),
        title = html_text(title),
        rows = rows.concat(),
    )
}

fn build_graphviz_html_label(
    table_name: &str,
    table: &TableDefinition,
    theme: &GraphVizTheme,
) -> String {
    let palette = &theme.palette;
    let mut rows = vec![format!(
        "<TR><TD ALIGN=\"LEFT\">id</TD><TD ALIGN=\"LEFT\"><FONT COLOR=\"{muted}\">string</FONT> <FONT COLOR=\"{err}\">PK</FONT></TD></TR>",
        muted = html_text(palette.muted),
        err = html_text(palette.error),
    )];
    rows.extend(table.fields.iter().map(|field| {
        let constraint = get_field_constraint(&field.name, table);
        let key = if constraint.is_empty() {
            String::new()
        } else {
            format!(
                " <FONT COLOR=\"{cc}\">{constraint}</FONT>",
                cc = html_text(constraint_color(constraint, palette)),
            )
        };
        format!(
            "<TR><TD ALIGN=\"LEFT\">{name}</TD><TD ALIGN=\"LEFT\"><FONT COLOR=\"{tc}\">{ty}</FONT>{key}</TD></TR>",
            name = html_text(&field.name),
            tc = html_text(field_type_color(field.field_type, palette)),
            ty = field.field_type.as_str(),
        )
    }));
    html_label(theme.node_color, table_name, &rows)
}

fn build_graphviz_edge_label(
    edge_name: &str,
    edge: &EdgeDefinition,
    theme: &GraphVizTheme,
) -> String {
    if theme.use_gradients {
        let rows: Vec<String> = edge
            .fields
            .iter()
            .map(|field| {
                format!(
                    "<TR><TD ALIGN=\"LEFT\">{name}</TD><TD ALIGN=\"LEFT\"><FONT COLOR=\"{tc}\">{ty}</FONT></TD></TR>",
                    name = html_text(&field.name),
                    tc = html_text(field_type_color(field.field_type, &theme.palette)),
                    ty = field.field_type.as_str(),
                )
            })
            .collect();
        return html_label(theme.node_color, edge_name, &rows);
    }

    // Plain record label for edge.
    let rows: Vec<String> = edge
        .fields
        .iter()
        .map(|field| {
            format!(
                "{name} : {ty}\\l",
                name = record_text(&field.name),
                ty = field.field_type.as_str(),
            )
        })
        .collect();
    record_label(edge_name, &rows)
}

fn graphviz_edge_style(edge: &EdgeDefinition, theme: &GraphVizTheme) -> String {
    if let (Some(from), Some(to)) = (&edge.from_table, &edge.to_table) {
        if from == to {
            if theme.use_gradients {
                return format!(
                    ", style=dashed, color={}",
                    dot_quoted(theme.palette.secondary)
                );
            }
            return ", style=dashed".to_string();
        }
    }
    String::new()
}

fn field_type_color(ty: FieldType, palette: &ColorScheme) -> &'static str {
    match ty {
        FieldType::String => palette.success,
        FieldType::Int | FieldType::Float => palette.warning,
        FieldType::Bool => palette.accent,
        FieldType::Datetime => palette.secondary,
        FieldType::Record => palette.primary,
        FieldType::Object | FieldType::Array => palette.muted,
        _ => palette.text,
    }
}

fn constraint_color(constraint: &str, palette: &ColorScheme) -> &'static str {
    match constraint {
        "PK" => palette.error,
        "FK" => palette.primary,
        "UK" => palette.accent,
        _ => palette.text,
    }
}

// ---------------------------------------------------------------------------
// ASCII
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Unified dispatch
// ---------------------------------------------------------------------------

/// Theme handle for unified [`visualize_schema`] dispatch.
///
/// Callers may supply a full [`Theme`](super::themes::Theme) or a
/// format-specific theme; convenience [`From`] impls wrap each.
#[derive(Debug, Clone)]
pub enum ThemeOption<'a> {
    /// Full bundled theme; only the matching sub-theme is applied.
    Full(&'a super::themes::Theme),
    /// Mermaid-specific theme; ignored for GraphViz / ASCII dispatch.
    Mermaid(&'a MermaidTheme),
    /// GraphViz-specific theme; ignored for Mermaid / ASCII dispatch.
    GraphViz(&'a GraphVizTheme),
    /// ASCII-specific theme; ignored for Mermaid / GraphViz dispatch.
    Ascii(&'a ASCIITheme),
    /// Named preset — resolved via [`get_theme`](super::themes::get_theme).
    Named(&'a str),
}

impl<'a> From<&'a super::themes::Theme> for ThemeOption<'a> {
    fn from(theme: &'a super::themes::Theme) -> Self {
        Self::Full(theme)
    }
}

impl<'a> From<&'a MermaidTheme> for ThemeOption<'a> {
    fn from(theme: &'a MermaidTheme) -> Self {
        Self::Mermaid(theme)
    }
}

impl<'a> From<&'a GraphVizTheme> for ThemeOption<'a> {
    fn from(theme: &'a GraphVizTheme) -> Self {
        Self::GraphViz(theme)
    }
}

impl<'a> From<&'a ASCIITheme> for ThemeOption<'a> {
    fn from(theme: &'a ASCIITheme) -> Self {
        Self::Ascii(theme)
    }
}

/// Dispatch to the requested format with an optional theme.
///
/// Mirrors `visualize_schema` in Python: picks the right generator based on
/// `output_format`, resolves a [`ThemeOption`] into the matching sub-theme,
/// and passes through `include_fields` / `include_edges`. The edge map takes
/// the table map's hasher, which keeps a bare `None` inferable.
///
/// Returns [`SurqlError::Validation`](crate::error::SurqlError::Validation)
/// only if a [`ThemeOption::Named`] theme name is unknown.
pub fn visualize_schema<S: BuildHasher + Default>(
    tables: &HashMap<String, TableDefinition, S>,
    edges: Option<&HashMap<String, EdgeDefinition, S>>,
    output_format: OutputFormat,
    include_fields: bool,
    include_edges: bool,
    theme: Option<&ThemeOption<'_>>,
) -> Result<String> {
    let empty_edges: HashMap<String, EdgeDefinition, S> = HashMap::default();
    let edges = edges.unwrap_or(&empty_edges);

    // Resolve a named theme once so the borrowed references below stay alive.
    let resolved = match theme {
        Some(ThemeOption::Named(name)) => Some(super::themes::get_theme(name)?),
        _ => None,
    };

    match output_format {
        OutputFormat::Mermaid => {
            let mermaid_theme = match (theme, &resolved) {
                (Some(ThemeOption::Full(t)), _) => Some(&t.mermaid),
                (Some(ThemeOption::Mermaid(m)), _) => Some(*m),
                (Some(ThemeOption::Named(_)), Some(t)) => Some(&t.mermaid),
                _ => None,
            };
            Ok(generate_mermaid(
                tables,
                edges,
                include_fields,
                include_edges,
                mermaid_theme,
            ))
        }
        OutputFormat::GraphViz => {
            let graphviz_theme = match (theme, &resolved) {
                (Some(ThemeOption::Full(t)), _) => Some(&t.graphviz),
                (Some(ThemeOption::GraphViz(g)), _) => Some(*g),
                (Some(ThemeOption::Named(_)), Some(t)) => Some(&t.graphviz),
                _ => None,
            };
            Ok(generate_graphviz(
                tables,
                edges,
                include_fields,
                include_edges,
                graphviz_theme,
            ))
        }
        OutputFormat::Ascii => {
            let ascii_theme = match (theme, &resolved) {
                (Some(ThemeOption::Full(t)), _) => Some(&t.ascii),
                (Some(ThemeOption::Ascii(a)), _) => Some(*a),
                (Some(ThemeOption::Named(_)), Some(t)) => Some(&t.ascii),
                _ => None,
            };
            Ok(generate_ascii(
                tables,
                edges,
                include_fields,
                include_edges,
                ascii_theme,
            ))
        }
    }
}

/// Visualise the current global [`SchemaRegistry`](crate::schema::registry::SchemaRegistry) in the requested format.
///
/// Convenience wrapper around [`visualize_schema`] that pulls tables and
/// edges from [`get_registry`].
pub fn visualize_from_registry(
    output_format: OutputFormat,
    include_fields: bool,
    include_edges: bool,
) -> Result<String> {
    let reg = get_registry();
    let tables = reg.tables();
    let edges = reg.edges();
    visualize_schema(
        &tables,
        Some(&edges),
        output_format,
        include_fields,
        include_edges,
        None,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use crate::schema::edge::{edge_schema, EdgeDefinition};
    use crate::schema::fields::{
        bool_field, datetime_field, int_field, record_field, string_field,
    };
    use crate::schema::table::{table_schema, unique_index, TableDefinition};
    use crate::schema::themes::{
        dark_theme, forest_theme, minimal_theme, modern_theme, ASCIITheme, GraphVizTheme,
        MermaidTheme,
    };

    fn user_table() -> TableDefinition {
        let (email, _) = string_field("email").build().unwrap();
        let (age, _) = int_field("age").build().unwrap();
        let (active, _) = bool_field("active").build().unwrap();
        table_schema("user")
            .with_fields([email, age, active])
            .with_indexes([unique_index("user_email_uk", ["email"])])
    }

    fn post_table() -> TableDefinition {
        let (title, _) = string_field("title").build().unwrap();
        let (author, _) = record_field("author", Some("user")).build().unwrap();
        let (posted, _) = datetime_field("posted_at").build().unwrap();
        table_schema("post").with_fields([title, author, posted])
    }

    fn minimal_tables() -> HashMap<String, TableDefinition> {
        let mut m = HashMap::new();
        m.insert("user".to_string(), user_table());
        m
    }

    fn two_tables() -> HashMap<String, TableDefinition> {
        let mut m = HashMap::new();
        m.insert("user".to_string(), user_table());
        m.insert("post".to_string(), post_table());
        m
    }

    fn likes_edge() -> EdgeDefinition {
        let (weight, _) = int_field("weight").build().unwrap();
        edge_schema("likes")
            .with_from_table("user")
            .with_to_table("post")
            .with_fields([weight])
    }

    fn knows_edge_self() -> EdgeDefinition {
        edge_schema("knows")
            .with_from_table("user")
            .with_to_table("user")
    }

    // ---------- Mermaid golden ----------

    #[test]
    fn mermaid_no_theme_starts_with_erdiagram() {
        let out = generate_mermaid(&minimal_tables(), &HashMap::new(), true, true, None);
        assert!(out.starts_with("erDiagram"));
        assert!(!out.contains("%%{init"));
    }

    #[test]
    fn mermaid_theme_emits_init_directive() {
        let t = MermaidTheme::default();
        let out = generate_mermaid(&minimal_tables(), &HashMap::new(), true, true, Some(&t));
        assert!(out.starts_with(
            "%%{init: {'theme':'default', 'themeVariables': \
             {'primaryColor':'#6366f1', 'secondaryColor':'#ec4899'}}}%%\nerDiagram"
        ));
    }

    #[test]
    fn mermaid_theme_without_custom_css_emits_the_name_only() {
        let t = MermaidTheme {
            use_custom_css: false,
            ..dark_theme().mermaid
        };
        let out = generate_mermaid(&minimal_tables(), &HashMap::new(), true, true, Some(&t));
        assert!(
            out.starts_with("%%{init: {'theme':'dark'}}%%\nerDiagram"),
            "{out}"
        );
    }

    #[test]
    fn mermaid_theme_values_cannot_break_the_directive() {
        let t = MermaidTheme {
            theme_name: "dark'}}%%\nerDiagram",
            primary_color: "red'}",
            secondary_color: "#abcdef",
            use_custom_css: true,
        };
        let out = generate_mermaid(&minimal_tables(), &HashMap::new(), true, true, Some(&t));
        assert!(
            out.starts_with(
                "%%{init: {'theme':'default', 'themeVariables': \
                 {'secondaryColor':'#abcdef'}}}%%\nerDiagram"
            ),
            "{out}"
        );
    }

    #[test]
    fn graphviz_rich_labels_use_the_theme_palette() {
        let theme = dark_theme().graphviz;
        let mut edges = HashMap::new();
        edges.insert("knows".to_string(), knows_edge_self());
        let out = generate_graphviz(&two_tables(), &edges, true, true, Some(&theme));
        let dark = dark_theme().color_scheme;
        assert!(
            out.contains(&format!("BGCOLOR=\"{}\"", theme.node_color)),
            "{out}"
        );
        assert!(
            out.contains(&format!("<FONT COLOR=\"{}\">string", dark.success)),
            "{out}"
        );
        assert!(
            out.contains(&format!("<FONT COLOR=\"{}\">PK", dark.error)),
            "{out}"
        );
        assert!(
            out.contains(&format!("color=\"{}\"", dark.secondary)),
            "{out}"
        );
        let modern = modern_theme().color_scheme;
        assert!(!out.contains(modern.success), "{out}");
    }

    #[test]
    fn graphviz_clusters_group_tables_and_edges() {
        let theme = GraphVizTheme {
            use_clusters: true,
            ..modern_theme().graphviz
        };
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let out = generate_graphviz(&two_tables(), &edges, true, true, Some(&theme));
        assert!(out.contains("    subgraph cluster_tables {\n        label=\"Tables\";\n        \"post\" [label="), "{out}");
        assert!(
            out.contains(
                "    subgraph cluster_edges {\n        label=\"Edges\";\n        \"likes\" [label="
            ),
            "{out}"
        );
        let plain = generate_graphviz(
            &two_tables(),
            &edges,
            true,
            true,
            Some(&modern_theme().graphviz),
        );
        assert!(!plain.contains("subgraph"), "{plain}");
    }

    #[test]
    fn ascii_colours_follow_the_theme_color_scheme() {
        let modern = generate_ascii(
            &two_tables(),
            &HashMap::new(),
            true,
            true,
            Some(&modern_theme().ascii),
        );
        let dark = generate_ascii(
            &two_tables(),
            &HashMap::new(),
            true,
            true,
            Some(&dark_theme().ascii),
        );
        // PK markers take the scheme's error colour: #ef4444 vs #f87171.
        assert!(modern.contains("\u{1b}[38;2;239;68;68m"), "{modern:?}");
        assert!(dark.contains("\u{1b}[38;2;248;113;113m"), "{dark:?}");
        assert_ne!(modern, dark);
    }

    #[test]
    fn mermaid_dark_theme_init() {
        let th = dark_theme();
        let out = generate_mermaid(
            &minimal_tables(),
            &HashMap::new(),
            true,
            true,
            Some(&th.mermaid),
        );
        assert!(out.contains("%%{init: {'theme':'dark', 'themeVariables': {'primaryColor':'#8b5cf6', 'secondaryColor':'#d946ef'}}}%%"));
    }

    #[test]
    fn mermaid_forest_theme_init() {
        let th = forest_theme();
        let out = generate_mermaid(
            &minimal_tables(),
            &HashMap::new(),
            true,
            true,
            Some(&th.mermaid),
        );
        assert!(out.contains("%%{init: {'theme':'forest', 'themeVariables': {'primaryColor':'#10b981', 'secondaryColor':'#14b8a6'}}}%%"));
    }

    #[test]
    fn mermaid_minimal_theme_init() {
        let th = minimal_theme();
        let out = generate_mermaid(
            &minimal_tables(),
            &HashMap::new(),
            true,
            true,
            Some(&th.mermaid),
        );
        assert!(out.contains("%%{init: {'theme':'neutral', 'themeVariables': {'primaryColor':'#6b7280', 'secondaryColor':'#64748b'}}}%%"));
    }

    #[test]
    fn mermaid_includes_table_and_id_pk() {
        let out = generate_mermaid(&minimal_tables(), &HashMap::new(), true, true, None);
        assert!(out.contains("    \"user\" {"));
        assert!(out.contains("        string id PK"));
        assert!(out.contains("        string email UK"));
        assert!(out.contains("    }"));
    }

    #[test]
    fn mermaid_without_fields_omits_fields() {
        let out = generate_mermaid(&minimal_tables(), &HashMap::new(), false, true, None);
        assert!(!out.contains("string id PK"));
        assert!(out.contains("    \"user\" {\n    }"));
    }

    #[test]
    fn mermaid_record_field_marked_fk() {
        let out = generate_mermaid(&two_tables(), &HashMap::new(), true, true, None);
        assert!(out.contains("record author FK"));
    }

    #[test]
    fn mermaid_edges_relationship_line() {
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let out = generate_mermaid(&two_tables(), &edges, true, true, None);
        assert!(out.contains("\"user\" ||--o{ \"post\" : \"likes\""));
    }

    #[test]
    fn mermaid_self_edge_uses_many_to_many() {
        let mut edges = HashMap::new();
        edges.insert("knows".to_string(), knows_edge_self());
        let out = generate_mermaid(&minimal_tables(), &edges, true, true, None);
        assert!(out.contains("\"user\" }o--o{ \"user\" : \"knows\""));
    }

    #[test]
    fn mermaid_empty_registry() {
        let out = generate_mermaid(&HashMap::new(), &HashMap::new(), true, true, None);
        assert_eq!(out, "erDiagram\n");
    }

    #[test]
    fn mermaid_edge_with_fields_emits_entity() {
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let out = generate_mermaid(&two_tables(), &edges, true, true, None);
        assert!(out.contains("    \"likes\" {"));
        assert!(out.contains("        int weight"));
    }

    #[test]
    fn mermaid_include_edges_false_omits_relationships() {
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let out = generate_mermaid(&two_tables(), &edges, true, false, None);
        assert!(!out.contains("||--o{"));
    }

    #[test]
    fn mermaid_edge_with_unknown_endpoints_skipped() {
        let mut edges = HashMap::new();
        edges.insert(
            "bogus".to_string(),
            edge_schema("bogus")
                .with_from_table("ghost")
                .with_to_table("phantom"),
        );
        let out = generate_mermaid(&minimal_tables(), &edges, true, true, None);
        assert!(!out.contains("bogus"));
    }

    // ---------- GraphViz golden ----------

    #[test]
    fn graphviz_no_theme_is_backward_compatible() {
        let out = generate_graphviz(&minimal_tables(), &HashMap::new(), true, true, None);
        assert!(out.starts_with("digraph schema {"));
        assert!(out.contains("rankdir=LR;"));
        assert!(out.contains("node [shape=record];"));
        assert!(!out.contains("bgcolor"));
    }

    #[test]
    fn graphviz_modern_theme_emits_gradients_rich() {
        let theme = modern_theme().graphviz;
        let out = generate_graphviz(&minimal_tables(), &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains("fontname=\"Arial\""));
        assert!(out.contains("<TABLE BORDER=\"0\" CELLBORDER=\"1\""));
        assert!(out.contains("<FONT COLOR=\"#FFFFFF\"><B>user</B></FONT>"));
    }

    #[test]
    fn graphviz_dark_theme_sets_bgcolor() {
        let theme = dark_theme().graphviz;
        let out = generate_graphviz(&minimal_tables(), &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains("bgcolor=\"#1e1b4b\";"));
    }

    #[test]
    fn graphviz_forest_theme_uses_emerald_edge_color() {
        let theme = forest_theme().graphviz;
        let out = generate_graphviz(&minimal_tables(), &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains("color=\"#059669\""));
    }

    #[test]
    fn graphviz_minimal_theme_uses_filled_style_no_gradients() {
        let theme = minimal_theme().graphviz;
        let out = generate_graphviz(&minimal_tables(), &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains("style=\"filled\""));
        // Gradients are disabled, so falls back to plain record labels.
        assert!(out.contains("{user|id : string (PK)\\l"));
    }

    #[test]
    fn graphviz_record_field_plain_label() {
        let out = generate_graphviz(&two_tables(), &HashMap::new(), true, true, None);
        assert!(out.contains("\"post\" [label=\"{post|id : string (PK)\\l"));
        assert!(out.contains("author : record (FK)\\l"));
    }

    #[test]
    fn graphviz_edge_relationship() {
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let out = generate_graphviz(&two_tables(), &edges, true, true, None);
        assert!(out.contains("\"user\" -> \"post\" [label=\"likes\"];"));
    }

    #[test]
    fn graphviz_self_edge_is_dashed() {
        let mut edges = HashMap::new();
        edges.insert("knows".to_string(), knows_edge_self());
        let out = generate_graphviz(&minimal_tables(), &edges, true, true, None);
        assert!(out.contains("\"user\" -> \"user\" [label=\"knows\", style=dashed];"));
    }

    #[test]
    fn graphviz_self_edge_gradient_colored() {
        let mut edges = HashMap::new();
        edges.insert("knows".to_string(), knows_edge_self());
        let theme = modern_theme().graphviz;
        let out = generate_graphviz(&minimal_tables(), &edges, true, true, Some(&theme));
        assert!(out.contains(", style=dashed, color=\"#ec4899\""));
    }

    #[test]
    fn graphviz_empty_registry() {
        let out = generate_graphviz(&HashMap::new(), &HashMap::new(), true, true, None);
        assert!(out.starts_with("digraph schema {"));
        assert!(out.trim_end().ends_with('}'));
    }

    #[test]
    fn graphviz_include_fields_false_emits_plain_label() {
        let out = generate_graphviz(&minimal_tables(), &HashMap::new(), false, true, None);
        assert!(out.contains("\"user\" [label=\"user\"];"));
    }

    #[test]
    fn graphviz_edge_label_with_fields_plain() {
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let out = generate_graphviz(&two_tables(), &edges, true, true, None);
        // Plain record label for edge when no gradients.
        assert!(out.contains("\"likes\" [label=\"{likes|weight : int\\l}\"];"));
    }

    #[test]
    fn graphviz_edge_label_with_fields_gradient() {
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let theme = modern_theme().graphviz;
        let out = generate_graphviz(&two_tables(), &edges, true, true, Some(&theme));
        assert!(out.contains("<B>likes</B>"));
    }

    #[test]
    fn graphviz_unknown_endpoints_skipped() {
        let mut edges = HashMap::new();
        edges.insert(
            "bogus".to_string(),
            edge_schema("bogus")
                .with_from_table("ghost")
                .with_to_table("phantom"),
        );
        let out = generate_graphviz(&minimal_tables(), &edges, true, true, None);
        assert!(!out.contains("ghost"));
    }

    // ---------- ASCII golden ----------

    #[test]
    fn ascii_no_theme_uses_plus_corners() {
        let out = generate_ascii(&minimal_tables(), &HashMap::new(), true, true, None);
        assert!(out.contains('+'));
        assert!(out.contains("| "));
        assert!(out.contains("id : string (PK)"));
    }

    #[test]
    fn ascii_rounded_theme_uses_rounded_corners() {
        let theme = ASCIITheme::default(); // rounded
        let out = generate_ascii(&minimal_tables(), &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains('\u{256D}')); // ╭
        assert!(out.contains('\u{256E}')); // ╮
        assert!(out.contains('\u{2570}')); // ╰
        assert!(out.contains('\u{256F}')); // ╯
    }

    #[test]
    fn ascii_double_theme_uses_double_corners() {
        let theme = ASCIITheme {
            box_style: "double",
            use_unicode: true,
            use_colors: false,
            use_icons: false,
            color_scheme: "default",
        };
        let out = generate_ascii(&minimal_tables(), &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains('\u{2554}'));
        assert!(out.contains('\u{2557}'));
    }

    #[test]
    fn ascii_heavy_theme_uses_heavy_chars() {
        let theme = ASCIITheme {
            box_style: "heavy",
            use_unicode: true,
            use_colors: false,
            use_icons: false,
            color_scheme: "default",
        };
        let out = generate_ascii(&minimal_tables(), &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains('\u{250F}'));
        assert!(out.contains('\u{2501}'));
    }

    #[test]
    fn ascii_minimal_theme_single_line_no_color_no_icon() {
        let theme = minimal_theme().ascii;
        let out = generate_ascii(&minimal_tables(), &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains('\u{250C}')); // single-line top-left
                                           // No ANSI colour sequences.
        assert!(!out.contains('\u{1b}'));
        // No key icons.
        assert!(!out.contains('\u{1F511}'));
    }

    #[test]
    fn ascii_modern_theme_has_colors_and_icons() {
        let theme = modern_theme().ascii;
        let out = generate_ascii(&minimal_tables(), &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains('\u{1F511}')); // key emoji
        assert!(out.contains('\u{1b}')); // ANSI escape
    }

    #[test]
    fn ascii_relationships_section_rendered() {
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let out = generate_ascii(&two_tables(), &edges, true, true, None);
        assert!(out.contains("Relationships:"));
        assert!(out.contains("user --[likes]--> post"));
    }

    #[test]
    fn ascii_no_edges_omits_relationships() {
        let out = generate_ascii(&minimal_tables(), &HashMap::new(), true, true, None);
        assert!(!out.contains("Relationships:"));
    }

    #[test]
    fn ascii_include_fields_false_shows_header_only() {
        let out = generate_ascii(&minimal_tables(), &HashMap::new(), false, true, None);
        assert!(!out.contains("id : string"));
        assert!(out.contains("user"));
    }

    #[test]
    fn ascii_empty_registry_is_empty_string() {
        let out = generate_ascii(&HashMap::new(), &HashMap::new(), true, true, None);
        assert_eq!(out, "");
    }

    #[test]
    fn ascii_many_fields_box_widens() {
        let mut long_fields = Vec::new();
        for i in 0..10 {
            long_fields.push(
                string_field(format!("very_long_field_name_number_{i}"))
                    .build()
                    .unwrap()
                    .0,
            );
        }
        let mut tables = HashMap::new();
        tables.insert(
            "wide".to_string(),
            table_schema("wide").with_fields(long_fields),
        );
        let out = generate_ascii(&tables, &HashMap::new(), true, true, None);
        assert!(out.contains("very_long_field_name_number_9"));
    }

    #[test]
    fn ascii_constraint_icons_appear_for_fk_and_uk() {
        let mut tables = HashMap::new();
        tables.insert("user".to_string(), user_table()); // has UK index
        tables.insert("post".to_string(), post_table()); // has FK record
        let theme = modern_theme().ascii;
        let out = generate_ascii(&tables, &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains('\u{1F517}')); // FK link
        assert!(out.contains('\u{2B50}')); // UK star
    }

    // ---------- Unified dispatch ----------

    #[test]
    fn visualize_schema_mermaid_named_theme() {
        let tables = minimal_tables();
        let theme = ThemeOption::Named("dark");
        let out = visualize_schema(
            &tables,
            None,
            OutputFormat::Mermaid,
            true,
            true,
            Some(&theme),
        )
        .unwrap();
        assert!(out.contains("%%{init: {'theme':'dark', 'themeVariables'"));
    }

    #[test]
    fn visualize_schema_graphviz_full_theme_ref() {
        let tables = minimal_tables();
        let th = modern_theme();
        let theme = ThemeOption::Full(&th);
        let out = visualize_schema(
            &tables,
            None,
            OutputFormat::GraphViz,
            true,
            true,
            Some(&theme),
        )
        .unwrap();
        assert!(out.contains("<B>user</B>"));
    }

    #[test]
    fn visualize_schema_ascii_format_theme_ref() {
        let tables = minimal_tables();
        let th = minimal_theme().ascii;
        let theme = ThemeOption::Ascii(&th);
        let out =
            visualize_schema(&tables, None, OutputFormat::Ascii, true, true, Some(&theme)).unwrap();
        assert!(out.contains('\u{250C}'));
    }

    #[test]
    fn visualize_schema_unknown_named_theme_errors() {
        let tables = minimal_tables();
        let theme = ThemeOption::Named("neon");
        let err = visualize_schema(
            &tables,
            None,
            OutputFormat::Mermaid,
            true,
            true,
            Some(&theme),
        )
        .unwrap_err();
        assert!(err.to_string().contains("Unknown theme"));
    }

    #[test]
    fn visualize_schema_none_theme_matches_direct_call() {
        let tables = minimal_tables();
        let a = visualize_schema(&tables, None, OutputFormat::Mermaid, true, true, None).unwrap();
        let b = generate_mermaid(&tables, &HashMap::new(), true, true, None);
        assert_eq!(a, b);
    }

    #[test]
    fn theme_option_from_impls() {
        let g = GraphVizTheme::default();
        let _: ThemeOption = (&g).into();
        let a = ASCIITheme::default();
        let _: ThemeOption = (&a).into();
        let m = MermaidTheme::default();
        let _: ThemeOption = (&m).into();
        let t = modern_theme();
        let _: ThemeOption = (&t).into();
    }

    #[test]
    fn visualize_schema_ascii_via_named_theme() {
        let tables = minimal_tables();
        let theme = ThemeOption::Named("forest");
        let out =
            visualize_schema(&tables, None, OutputFormat::Ascii, true, true, Some(&theme)).unwrap();
        assert!(out.contains('\u{1F511}'));
    }

    // ---------- Indexes surfaced ----------

    #[test]
    fn mermaid_multiple_unique_indexes_mark_all_as_uk() {
        let (email, _) = string_field("email").build().unwrap();
        let (username, _) = string_field("username").build().unwrap();
        let tbl = table_schema("user")
            .with_fields([email, username])
            .with_indexes([
                unique_index("email_uk", ["email"]),
                unique_index("username_uk", ["username"]),
            ]);
        let mut tables = HashMap::new();
        tables.insert("user".to_string(), tbl);
        let out = generate_mermaid(&tables, &HashMap::new(), true, true, None);
        assert!(out.contains("string email UK"));
        assert!(out.contains("string username UK"));
    }

    // ---------- Hostile names ----------

    fn named_table(name: &str, fields: &[&str]) -> HashMap<String, TableDefinition> {
        let fields = fields
            .iter()
            .map(|f| crate::schema::fields::FieldDefinition::new(*f, FieldType::String));
        let mut m = HashMap::new();
        m.insert(name.to_string(), table_schema(name).with_fields(fields));
        m
    }

    #[test]
    fn mermaid_nested_field_names_are_sanitised_with_the_real_name_as_comment() {
        let tables = named_table("user", &["address.city", "tags[*]", "ok_name"]);
        let out = generate_mermaid(&tables, &HashMap::new(), true, true, None);
        assert!(
            out.contains("        string address_city \"address.city\""),
            "{out}"
        );
        assert!(out.contains("        string tags___ \"tags[*]\""), "{out}");
        assert!(out.contains("        string ok_name\n"), "{out}");
    }

    #[test]
    fn mermaid_entity_names_are_quoted_and_cannot_inject_lines() {
        let tables = named_table("my \"odd\"\n    x ||--o{ y : z", &[]);
        let out = generate_mermaid(&tables, &HashMap::new(), false, true, None);
        assert_eq!(out.lines().count(), 3, "{out}");
        assert!(
            out.contains("    \"my 'odd'     x ||--o{ y : z\" {"),
            "{out}"
        );
    }

    #[test]
    fn mermaid_names_avoid_characters_the_parser_rejects() {
        let tables = named_table("a\\b%%c#quot;d", &["n~~e"]);
        let out = generate_mermaid(&tables, &HashMap::new(), true, false, None);
        assert!(
            out.contains("    \"a\u{FF3C}b\u{FF05}\u{FF05}c\u{FF03}quot;d\" {"),
            "{out}"
        );
        assert!(
            out.contains("        string n__e \"n\u{FF5E}\u{FF5E}e\""),
            "{out}"
        );
    }

    #[test]
    fn graphviz_dot_keywords_are_quoted_ids() {
        let mut tables = named_table("node", &[]);
        tables.extend(named_table("Edge", &[]));
        tables.extend(named_table("graph", &[]));
        let out = generate_graphviz(&tables, &HashMap::new(), true, true, None);
        for name in ["node", "Edge", "graph"] {
            assert!(out.contains(&format!("    \"{name}\" [label=")), "{out}");
            assert!(!out.contains(&format!("    {name} [label=")), "{out}");
        }
    }

    #[test]
    fn graphviz_ids_escape_quotes_and_backslashes() {
        let tables = named_table("a\"b\\c", &[]);
        let out = generate_graphviz(&tables, &HashMap::new(), false, true, None);
        assert!(out.contains("    \"a\\\"b\\\\c\" [label="), "{out}");
    }

    #[test]
    fn graphviz_record_label_metacharacters_are_escaped() {
        let tables = named_table("a|b", &["x{y}", "p<q>"]);
        let out = generate_graphviz(&tables, &HashMap::new(), true, true, None);
        assert!(out.contains("{a\\|b|id : string (PK)\\l"), "{out}");
        assert!(out.contains("x\\{y\\} : string\\l"), "{out}");
        assert!(out.contains("p\\<q\\> : string\\l"), "{out}");
    }

    #[test]
    fn graphviz_html_labels_are_html_escaped() {
        let tables = named_table("R&D", &["a<b"]);
        let theme = modern_theme().graphviz;
        let out = generate_graphviz(&tables, &HashMap::new(), true, true, Some(&theme));
        assert!(out.contains("<B>R&amp;D</B>"), "{out}");
        assert!(out.contains(">a&lt;b</TD>"), "{out}");
        assert!(!out.contains("<B>R&D</B>"), "{out}");
    }

    #[test]
    fn graphviz_edge_names_are_escaped_in_relationship_labels() {
        let mut edges = HashMap::new();
        edges.insert(
            "say \"hi\"".to_string(),
            edge_schema("say \"hi\"")
                .with_from_table("user")
                .with_to_table("post"),
        );
        let out = generate_graphviz(&two_tables(), &edges, true, true, None);
        assert!(
            out.contains("\"user\" -> \"post\" [label=\"say \\\"hi\\\"\"];"),
            "{out}"
        );
    }

    #[test]
    fn ascii_box_width_uses_display_width_for_wide_names() {
        let name = "用户表用户表用户表用户表";
        let out = generate_ascii(&named_table(name, &[]), &HashMap::new(), false, true, None);
        let widths: Vec<usize> = out
            .lines()
            .filter(|l| !l.is_empty())
            .map(display_width)
            .collect();
        assert!(widths.windows(2).all(|w| w[0] == w[1]), "{widths:?}\n{out}");
    }

    #[test]
    fn ascii_control_bytes_in_names_are_escaped() {
        let tables = named_table("evil\u{1b}[31m", &["f\u{7}ld"]);
        let mut edges = HashMap::new();
        edges.insert(
            "e\u{1b}]0;x".to_string(),
            edge_schema("e\u{1b}]0;x")
                .with_from_table("a\rb")
                .with_to_table("c"),
        );
        let out = generate_ascii(&tables, &edges, true, true, None);
        assert!(!out.chars().any(|c| c.is_control() && c != '\n'), "{out:?}");
        assert!(out.contains("evil\\u{1b}[31m"), "{out}");
    }

    #[test]
    fn graphviz_unique_index_marks_uk_in_record_label() {
        let (email, _) = string_field("email").build().unwrap();
        let tbl = table_schema("user")
            .with_fields([email])
            .with_indexes([unique_index("email_uk", ["email"])]);
        let mut tables = HashMap::new();
        tables.insert("user".to_string(), tbl);
        let out = generate_graphviz(&tables, &HashMap::new(), true, true, None);
        assert!(out.contains("email : string (UK)\\l"));
    }
}
