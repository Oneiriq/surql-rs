//! GraphViz DOT generator.

use std::collections::HashMap;
use std::hash::BuildHasher;

use super::{get_field_constraint, sorted_by_key, without_controls};
use crate::schema::edge::EdgeDefinition;
use crate::schema::fields::FieldType;
use crate::schema::table::TableDefinition;
use crate::schema::themes::{ColorScheme, GraphVizTheme};

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
