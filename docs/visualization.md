# Visualization

Render Mermaid / GraphViz / ASCII diagrams from table and edge
definitions, either passed in directly or pulled from the global
`SchemaRegistry`.

Every generator takes the tables and edges as name-keyed maps, two
flags (`include_fields`, `include_edges`), and an optional theme.

## Mermaid

```rust
use std::collections::HashMap;

use surql::schema::themes::modern_theme;
use surql::schema::visualize::generate_mermaid;

let mermaid = generate_mermaid(&tables, &edges, true, true, Some(&modern_theme().mermaid));
println!("{mermaid}");
```

Entity names and relationship labels are always double-quoted. Mermaid
has no escape inside those strings, so the few characters its parser
rejects or decodes there (`"`, `\`, `%`, `#`, `~`) are swapped for
look-alikes and line breaks for spaces. A field name that is not a plain
identifier, such as `address.city` or `tags[*]`, is rewritten to one and
keeps its real name as the attribute comment:

```text
    "user" {
        string id PK
        string address_city "address.city"
    }
```

## GraphViz DOT

```rust
use surql::schema::themes::dark_theme;
use surql::schema::visualize::generate_graphviz;

let dot = generate_graphviz(&tables, &edges, true, true, Some(&dark_theme().graphviz));
std::fs::write("schema.dot", dot)?;
```

Node IDs are always quoted DOT strings, so a table named `node`, `edge`,
or `graph`, or one containing `-`, `:`, or spaces, stays a node. Record
labels escape `{ } | < >`, and the HTML labels the gradient themes use are
HTML-escaped.

## ASCII

```rust
use surql::schema::themes::minimal_theme;
use surql::schema::visualize::generate_ascii;

println!("{}", generate_ascii(&tables, &edges, true, true, Some(&minimal_theme().ascii)));
```

Box widths are measured in terminal cells, so wide (CJK) names stay
aligned, and control characters in names are printed as `\u{..}` escapes
so a stored name cannot inject terminal escape sequences.

## From the registry

```rust
use surql::schema::visualize::{visualize_from_registry, visualize_schema, OutputFormat, ThemeOption};

let diagram = visualize_from_registry(OutputFormat::Mermaid, true, true)?;

let themed = visualize_schema(
    &tables,
    Some(&edges),
    OutputFormat::GraphViz,
    true,
    true,
    Some(&ThemeOption::Named("forest")),
)?;
```

The CLI wraps the same call: `surql schema visualize --format graphviz --theme dark`.

## Themes

| Preset             | Mood                                        |
|--------------------|---------------------------------------------|
| `modern_theme`     | Bright accents, clean typography, emoji.    |
| `dark_theme`       | Dim palette, good on dark terminals.        |
| `forest_theme`     | Earthy greens, muted secondary color.       |
| `minimal_theme`    | Monochrome, no decorative glyphs.           |

Each preset bundles a `ColorScheme` with one sub-theme per format. The
settings that shape the output:

- **GraphViz** (`GraphVizTheme`): `palette` colours the field types, keys,
  and self-referencing edges in the gradient (HTML) labels, and
  `node_color` their headers; `use_gradients` switches between HTML and
  record labels; `use_clusters` groups table and edge nodes into
  `cluster_tables` / `cluster_edges` subgraphs; `bg_color`, `font_name`,
  `node_shape`, `node_style`, `edge_color`, and `edge_style` set the
  graph, node, and edge defaults.
- **Mermaid** (`MermaidTheme`): `theme_name` picks the built-in Mermaid
  theme; with `use_custom_css`, `primary_color` and `secondary_color` are
  passed as `themeVariables` (Mermaid applies them fully on its `base`
  theme).
- **ASCII** (`ASCIITheme`): `box_style` and `use_unicode` choose the box
  characters, `use_icons` the key glyphs, and with `use_colors` the
  `color_scheme` palette's error / primary / accent colours tint the
  PK / FK / UK markers (24-bit ANSI).

`get_theme(name)` and `list_themes()` look presets up by name, and
`color_scheme_by_name(name)` resolves an ASCII `color_scheme` name.

## What's next

- **[Schema Definition](schema.md)** -- building the registry the
  visualizer consumes.
