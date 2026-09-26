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
use super::themes::{ASCIITheme, GraphVizTheme, MermaidTheme};

mod ascii;
mod graphviz;
mod mermaid;

pub use ascii::generate_ascii;
pub use graphviz::generate_graphviz;
pub use mermaid::generate_mermaid;

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

#[cfg(test)]
mod tests;
