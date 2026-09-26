//! Generator tests: golden output per format, theme wiring, and names that
//! carry syntax of their own.

use super::*;

use crate::schema::edge::{edge_schema, EdgeDefinition};
use crate::schema::fields::{
    bool_field, datetime_field, int_field, record_field, string_field, FieldType,
};
use crate::schema::table::{table_schema, unique_index, TableDefinition};
use crate::schema::themes::{
    dark_theme, forest_theme, minimal_theme, modern_theme, ASCIITheme, GraphVizTheme, MermaidTheme,
};
use crate::schema::utils::display_width;

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
    assert!(
        out.contains(
            "    subgraph cluster_tables {\n        label=\"Tables\";\n        \"post\" [label="
        ),
        "{out}"
    );
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
