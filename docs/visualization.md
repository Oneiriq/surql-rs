# Visualization

Render Mermaid / GraphViz / ASCII diagrams straight from a
`SchemaRegistry`.

## Mermaid

```rust
use surql::schema::themes::modern_theme;
use surql::schema::visualize::generate_mermaid;

let mermaid = generate_mermaid(&registry, &modern_theme().mermaid);
println!("{mermaid}");
```

## GraphViz DOT

```rust
use surql::schema::themes::dark_theme;
use surql::schema::visualize::generate_graphviz;

let dot = generate_graphviz(&registry, &dark_theme().graphviz);
std::fs::write("schema.dot", dot)?;
```

## ASCII

```rust
use surql::schema::themes::minimal_theme;
use surql::schema::visualize::generate_ascii;

println!("{}", generate_ascii(&registry, &minimal_theme().ascii));
```

## Themes

| Preset             | Mood                                        |
|--------------------|---------------------------------------------|
| `modern_theme`     | Bright accents, clean typography, emoji.    |
| `dark_theme`       | Dim palette, good on dark terminals.        |
| `forest_theme`     | Earthy greens, muted secondary color.       |
| `minimal_theme`    | Monochrome, no decorative glyphs.           |

## What's next

- **[Schema Definition](schema.md)** -- building the registry the
  visualizer consumes.
