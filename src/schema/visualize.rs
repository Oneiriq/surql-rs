//! Schema visualization: Mermaid, GraphViz (DOT), and ASCII art diagrams.
//!
//! Port of `surql/schema/visualize.py`. Generates visual diagrams of database
//! schemas from [`TableDefinition`] / [`EdgeDefinition`] values (either
//! supplied directly or pulled from the global [`SchemaRegistry`]) in any of
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
use std::fmt::Write as _;

use crate::error::Result;

use super::edge::EdgeDefinition;
use super::fields::FieldType;
use super::registry::get_registry;
use super::table::{IndexType, TableDefinition};
use super::themes::{modern_color_scheme, ASCIITheme, GraphVizTheme, MermaidTheme};
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

fn sorted_by_key<V, S: std::hash::BuildHasher>(map: &HashMap<String, V, S>) -> Vec<(&String, &V)> {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

// ---------------------------------------------------------------------------
// Mermaid
// ---------------------------------------------------------------------------

/// Generate a Mermaid ER diagram.
///
/// Mirrors `MermaidGenerator.generate`. When `theme` is `Some`, a
/// `%%{init: ...}%%` directive with that theme name prefaces the output;
/// when `None`, the diagram begins directly with `erDiagram`.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn generate_mermaid(
    tables: &HashMap<String, TableDefinition>,
    edges: &HashMap<String, EdgeDefinition>,
    include_fields: bool,
    include_edges: bool,
    theme: Option<&MermaidTheme>,
) -> String {
    let mut lines: Vec<String> = Vec::new();

    if let Some(theme) = theme {
        lines.push(format!("%%{{init: {{'theme':'{}'}}}}%%", theme.theme_name));
    }

    lines.push("erDiagram".to_string());

    // Table entities (sorted by name).
    for (table_name, table) in sorted_by_key(tables) {
        lines.push(format!("    {table_name} {{"));
        if include_fields {
            // Always emit the implicit id field.
            lines.push("        string id PK".to_string());
            for field in &table.fields {
                let constraint = get_field_constraint(&field.name, table);
                let constraint_str = if constraint.is_empty() {
                    String::new()
                } else {
                    format!(" {constraint}")
                };
                lines.push(format!(
                    "        {ty} {name}{constraint_str}",
                    ty = field.field_type.as_str(),
                    name = field.name,
                ));
            }
        }
        lines.push("    }".to_string());
    }

    // Edge entities (also tables in SurrealDB) — only if they have fields.
    for (edge_name, edge) in sorted_by_key(edges) {
        if include_fields && !edge.fields.is_empty() {
            lines.push(format!("    {edge_name} {{"));
            for field in &edge.fields {
                lines.push(format!(
                    "        {ty} {name}",
                    ty = field.field_type.as_str(),
                    name = field.name,
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
                "    {from_table} {cardinality} {to_table} : {edge_name}"
            ));
        }
    }

    lines.join("\n")
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

/// Generate a GraphViz DOT-format diagram.
///
/// Mirrors `GraphVizGenerator.generate`. When `theme` is `None`, a
/// backward-compatible rendering matching Python's default (no gradients,
/// plain `node [shape=record]`) is produced.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn generate_graphviz(
    tables: &HashMap<String, TableDefinition>,
    edges: &HashMap<String, EdgeDefinition>,
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
    if rich {
        if theme.bg_color != "transparent" {
            lines.push(format!("    bgcolor=\"{}\";", theme.bg_color));
        }
        lines.push(format!("    fontname=\"{}\";", theme.font_name));

        let mut node_attrs = Vec::<String>::new();
        node_attrs.push(format!("shape={}", theme.node_shape));
        if !theme.node_style.is_empty() {
            node_attrs.push(format!("style=\"{}\"", theme.node_style));
        }
        node_attrs.push(format!("fontname=\"{}\"", theme.font_name));
        node_attrs.push("pad=\"0.5\"".to_string());
        node_attrs.push("margin=\"0.2\"".to_string());
        lines.push(format!("    node [{}];", node_attrs.join(", ")));

        let mut edge_attrs = Vec::<String>::new();
        edge_attrs.push(format!("color=\"{}\"", theme.edge_color));
        if !theme.edge_style.is_empty() {
            edge_attrs.push(format!("style={}", theme.edge_style));
        }
        edge_attrs.push(format!("fontname=\"{}\"", theme.font_name));
        lines.push(format!("    edge [{}];", edge_attrs.join(", ")));
    } else {
        lines.push("    node [shape=record];".to_string());
    }

    lines.push(String::new());

    // Table nodes (sorted).
    for (table_name, table) in sorted_by_key(tables) {
        let label = build_graphviz_table_label(table_name, table, include_fields, theme);
        lines.push(format!("    {table_name} [label={label}];"));
    }

    // Edge nodes (only if they have fields and include_fields).
    for (edge_name, edge) in sorted_by_key(edges) {
        if include_fields && !edge.fields.is_empty() {
            let label = build_graphviz_edge_label(edge_name, edge, theme);
            lines.push(format!("    {edge_name} [label={label}];"));
        }
    }

    lines.push(String::new());

    // Relationships.
    if include_edges {
        for (edge_name, edge) in sorted_by_key(edges) {
            let from_table = edge.from_table.as_deref().unwrap_or("unknown");
            let to_table = edge.to_table.as_deref().unwrap_or("unknown");
            if !tables.contains_key(from_table) {
                continue;
            }
            if !tables.contains_key(to_table) {
                continue;
            }
            let edge_style = graphviz_edge_style(edge, theme);
            lines.push(format!(
                "    {from_table} -> {to_table} [label=\"{edge_name}\"{edge_style}];"
            ));
        }
    }

    lines.push("}".to_string());
    lines.join("\n")
}

fn build_graphviz_table_label(
    table_name: &str,
    table: &TableDefinition,
    include_fields: bool,
    theme: &GraphVizTheme,
) -> String {
    if !include_fields {
        return format!("\"{table_name}\"");
    }

    if theme.use_gradients {
        return build_graphviz_html_label(table_name, table);
    }

    // Plain record label: "{name|id : string (PK)\\l|field : ty\\l|...}"
    let mut parts: Vec<String> = Vec::with_capacity(table.fields.len() + 2);
    parts.push(table_name.to_string());
    parts.push("id : string (PK)\\l".to_string());
    for field in &table.fields {
        let constraint = get_field_constraint(&field.name, table);
        let constraint_str = if constraint.is_empty() {
            String::new()
        } else {
            format!(" ({constraint})")
        };
        parts.push(format!(
            "{name} : {ty}{constraint_str}\\l",
            name = field.name,
            ty = field.field_type.as_str(),
        ));
    }
    format!("\"{{{}}}\"", parts.join("|"))
}

fn build_graphviz_html_label(table_name: &str, table: &TableDefinition) -> String {
    let palette = modern_color_scheme();
    let mut html = String::from("<");
    html.push_str("<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">");
    // Header row.
    write!(
        html,
        "<TR><TD BGCOLOR=\"{bg}\" COLSPAN=\"2\"><FONT COLOR=\"#FFFFFF\"><B>{name}</B></FONT></TD></TR>",
        bg = modern_color_scheme().primary,
        name = table_name,
    )
    .expect("write to String cannot fail");

    // Implicit id row.
    write!(
        html,
        "<TR><TD ALIGN=\"LEFT\">id</TD><TD ALIGN=\"LEFT\"><FONT COLOR=\"{muted}\">string</FONT> <FONT COLOR=\"{err}\">PK</FONT></TD></TR>",
        muted = palette.muted,
        err = palette.error,
    )
    .expect("write to String cannot fail");

    // Field rows.
    for field in &table.fields {
        let constraint = get_field_constraint(&field.name, table);
        let type_color = field_type_color(field.field_type);
        write!(
            html,
            "<TR><TD ALIGN=\"LEFT\">{name}</TD><TD ALIGN=\"LEFT\"><FONT COLOR=\"{tc}\">{ty}</FONT>",
            name = field.name,
            tc = type_color,
            ty = field.field_type.as_str(),
        )
        .expect("write to String cannot fail");
        if !constraint.is_empty() {
            let cc = constraint_color(constraint);
            write!(html, " <FONT COLOR=\"{cc}\">{constraint}</FONT>")
                .expect("write to String cannot fail");
        }
        html.push_str("</TD></TR>");
    }

    html.push_str("</TABLE>>");
    html
}

fn build_graphviz_edge_label(
    edge_name: &str,
    edge: &EdgeDefinition,
    theme: &GraphVizTheme,
) -> String {
    if theme.use_gradients {
        let mut html = String::from("<");
        html.push_str("<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">");
        write!(
            html,
            "<TR><TD BGCOLOR=\"{bg}\" COLSPAN=\"2\"><FONT COLOR=\"#FFFFFF\"><B>{name}</B></FONT></TD></TR>",
            bg = theme.node_color,
            name = edge_name,
        )
        .expect("write to String cannot fail");
        for field in &edge.fields {
            let type_color = field_type_color(field.field_type);
            write!(
                html,
                "<TR><TD ALIGN=\"LEFT\">{name}</TD><TD ALIGN=\"LEFT\"><FONT COLOR=\"{tc}\">{ty}</FONT></TD></TR>",
                name = field.name,
                tc = type_color,
                ty = field.field_type.as_str(),
            )
            .expect("write to String cannot fail");
        }
        html.push_str("</TABLE>>");
        return html;
    }

    // Plain record label for edge.
    let mut parts: Vec<String> = Vec::with_capacity(edge.fields.len() + 1);
    parts.push(edge_name.to_string());
    for field in &edge.fields {
        parts.push(format!(
            "{name} : {ty}\\l",
            name = field.name,
            ty = field.field_type.as_str(),
        ));
    }
    format!("\"{{{}}}\"", parts.join("|"))
}

fn graphviz_edge_style(edge: &EdgeDefinition, theme: &GraphVizTheme) -> String {
    if let (Some(from), Some(to)) = (&edge.from_table, &edge.to_table) {
        if from == to {
            if theme.use_gradients {
                let secondary = modern_color_scheme().secondary;
                return format!(", style=dashed, color=\"{secondary}\"");
            }
            return ", style=dashed".to_string();
        }
    }
    String::new()
}

fn field_type_color(ty: FieldType) -> &'static str {
    let cs = modern_color_scheme();
    match ty {
        FieldType::String => cs.success,
        FieldType::Int | FieldType::Float => cs.warning,
        FieldType::Bool => cs.accent,
        FieldType::Datetime => cs.secondary,
        FieldType::Record => cs.primary,
        FieldType::Object | FieldType::Array => cs.muted,
        _ => cs.text,
    }
}

fn constraint_color(constraint: &str) -> &'static str {
    let cs = modern_color_scheme();
    match constraint {
        "PK" => cs.error,
        "FK" => cs.primary,
        "UK" => cs.accent,
        _ => cs.text,
    }
}

// ---------------------------------------------------------------------------
// ASCII
// ---------------------------------------------------------------------------

/// Generate an ASCII-art diagram (with optional Unicode box drawing).
///
/// Mirrors `ASCIIGenerator.generate`. When `theme` is `None`, basic ASCII
/// characters are used and no colours / icons are applied.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn generate_ascii(
    tables: &HashMap<String, TableDefinition>,
    edges: &HashMap<String, EdgeDefinition>,
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
            let from_table = edge.from_table.as_deref().unwrap_or("?");
            let to_table = edge.to_table.as_deref().unwrap_or("?");
            lines.push(format!("  {from_table} --[{edge_name}]--> {to_table}"));
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

fn colorize(text: &str, color_type: &str, theme: Option<&ASCIITheme>) -> String {
    let Some(theme) = theme else {
        return text.to_string();
    };
    if !theme.use_colors {
        return text.to_string();
    }
    let code = match color_type {
        "pk" => "\u{1b}[91m",
        "fk" => "\u{1b}[94m",
        "uk" => "\u{1b}[95m",
        "header" => "\u{1b}[1m",
        _ => "",
    };
    if code.is_empty() {
        return text.to_string();
    }
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

fn repeat_str(s: &str, n: usize) -> String {
    s.repeat(n)
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
                name = field.name,
                ty = field.field_type.as_str(),
            ));
        }
    }

    // Compute width.
    let min_width = std::cmp::max(table_name.chars().count() + 4, 20);
    let content_width = field_lines
        .iter()
        .map(|l| display_width(l))
        .max()
        .unwrap_or(0);
    let width = std::cmp::max(min_width, content_width + 2);

    let mut out: Vec<String> = Vec::new();
    out.push(format!(
        "{}{}{}",
        chars.tl,
        repeat_str(chars.h, width),
        chars.tr
    ));

    // Header row: centred, optional bold.
    let styled_name = colorize(table_name, "header", theme);
    let visible = display_width(&styled_name);
    let (left, right) = center_pad(width, visible);
    out.push(format!(
        "{v}{l}{name}{r}{v}",
        v = chars.v,
        l = " ".repeat(left),
        name = styled_name,
        r = " ".repeat(right),
    ));

    if include_fields {
        out.push(format!(
            "{}{}{}",
            chars.ml,
            repeat_str(chars.h, width),
            chars.mr
        ));
        for line in &field_lines {
            let visible_len = display_width(line);
            // Leading space + content + trailing padding = width.
            let padding = width.saturating_sub(visible_len + 1);
            let padded = format!(" {line}{}", " ".repeat(padding));
            out.push(format!("{v}{padded}{v}", v = chars.v));
        }
    }

    out.push(format!(
        "{}{}{}",
        chars.bl,
        repeat_str(chars.h, width),
        chars.br
    ));
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
/// and passes through `include_fields` / `include_edges`.
///
/// Returns [`SurqlError::Validation`](crate::error::SurqlError::Validation)
/// only if a [`ThemeOption::Named`] theme name is unknown.
#[allow(clippy::implicit_hasher)]
pub fn visualize_schema(
    tables: &HashMap<String, TableDefinition>,
    edges: Option<&HashMap<String, EdgeDefinition>>,
    output_format: OutputFormat,
    include_fields: bool,
    include_edges: bool,
    theme: Option<&ThemeOption<'_>>,
) -> Result<String> {
    let empty_edges: HashMap<String, EdgeDefinition> = HashMap::new();
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

/// Visualise the current global [`SchemaRegistry`] in the requested format.
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
        assert!(out.starts_with("%%{init: {'theme':'default'}}%%\nerDiagram"));
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
        assert!(out.contains("%%{init: {'theme':'dark'}}%%"));
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
        assert!(out.contains("%%{init: {'theme':'forest'}}%%"));
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
        assert!(out.contains("%%{init: {'theme':'neutral'}}%%"));
    }

    #[test]
    fn mermaid_includes_table_and_id_pk() {
        let out = generate_mermaid(&minimal_tables(), &HashMap::new(), true, true, None);
        assert!(out.contains("    user {"));
        assert!(out.contains("        string id PK"));
        assert!(out.contains("        string email UK"));
        assert!(out.contains("    }"));
    }

    #[test]
    fn mermaid_without_fields_omits_fields() {
        let out = generate_mermaid(&minimal_tables(), &HashMap::new(), false, true, None);
        assert!(!out.contains("string id PK"));
        assert!(out.contains("    user {\n    }"));
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
        assert!(out.contains("user ||--o{ post : likes"));
    }

    #[test]
    fn mermaid_self_edge_uses_many_to_many() {
        let mut edges = HashMap::new();
        edges.insert("knows".to_string(), knows_edge_self());
        let out = generate_mermaid(&minimal_tables(), &edges, true, true, None);
        assert!(out.contains("user }o--o{ user : knows"));
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
        assert!(out.contains("    likes {"));
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
        assert!(out.contains("post [label=\"{post|id : string (PK)\\l"));
        assert!(out.contains("author : record (FK)\\l"));
    }

    #[test]
    fn graphviz_edge_relationship() {
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let out = generate_graphviz(&two_tables(), &edges, true, true, None);
        assert!(out.contains("user -> post [label=\"likes\"];"));
    }

    #[test]
    fn graphviz_self_edge_is_dashed() {
        let mut edges = HashMap::new();
        edges.insert("knows".to_string(), knows_edge_self());
        let out = generate_graphviz(&minimal_tables(), &edges, true, true, None);
        assert!(out.contains("user -> user [label=\"knows\", style=dashed];"));
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
        assert!(out.contains("user [label=\"user\"];"));
    }

    #[test]
    fn graphviz_edge_label_with_fields_plain() {
        let mut edges = HashMap::new();
        edges.insert("likes".to_string(), likes_edge());
        let out = generate_graphviz(&two_tables(), &edges, true, true, None);
        // Plain record label for edge when no gradients.
        assert!(out.contains("likes [label=\"{likes|weight : int\\l}\"];"));
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
        assert!(out.contains("%%{init: {'theme':'dark'}}%%"));
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
