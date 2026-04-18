//! Theme system for schema visualization.
//!
//! Port of `surql/schema/themes.py`. Provides a comprehensive theming system
//! for surql's schema visualization outputs, supporting GraphViz, Mermaid,
//! and ASCII formats with multiple preset themes and customisation options.
//!
//! The four built-in themes are [`modern_theme`], [`dark_theme`],
//! [`forest_theme`], and [`minimal_theme`]. Look them up by name via
//! [`get_theme`] or enumerate names with [`list_themes`].

use crate::error::{Result, SurqlError};

/// Base color scheme used across all visualization themes.
///
/// Defines semantic colors for tables, fields, constraints, edges, and text.
/// All colors are specified as hex color codes (e.g., `"#3B82F6"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColorScheme {
    /// Primary accent color.
    pub primary: &'static str,
    /// Secondary accent color.
    pub secondary: &'static str,
    /// Background color.
    pub background: &'static str,
    /// Primary text color.
    pub text: &'static str,
    /// Accent color for highlights.
    pub accent: &'static str,
    /// Success / positive color.
    pub success: &'static str,
    /// Warning / caution color.
    pub warning: &'static str,
    /// Error / negative color.
    pub error: &'static str,
    /// Muted / disabled color.
    pub muted: &'static str,
}

impl ColorScheme {
    /// Default color scheme matching the Python dataclass defaults.
    #[must_use]
    pub const fn default_const() -> Self {
        Self {
            primary: "#6366f1",
            secondary: "#ec4899",
            background: "#f8fafc",
            text: "#0f172a",
            accent: "#8b5cf6",
            success: "#10b981",
            warning: "#f59e0b",
            error: "#ef4444",
            muted: "#94a3b8",
        }
    }
}

impl Default for ColorScheme {
    fn default() -> Self {
        Self::default_const()
    }
}

/// Theme configuration for GraphViz DOT format output.
///
/// Controls all visual aspects of GraphViz diagrams including node styling,
/// edge styling, layout, and advanced features like gradients and clustering.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraphVizTheme {
    /// Node border color.
    pub node_color: &'static str,
    /// Edge line color.
    pub edge_color: &'static str,
    /// Background color (use `"transparent"` for none).
    pub bg_color: &'static str,
    /// Font family for all text.
    pub font_name: &'static str,
    /// GraphViz node shape (e.g., `"record"`, `"box"`).
    pub node_shape: &'static str,
    /// Node style attributes (e.g., `"filled,rounded"`).
    pub node_style: &'static str,
    /// Edge style (e.g., `"solid"`, `"dashed"`).
    pub edge_style: &'static str,
    /// Enable gradient fills for nodes.
    pub use_gradients: bool,
    /// Enable table clustering / grouping.
    pub use_clusters: bool,
}

impl GraphVizTheme {
    /// Default GraphViz theme matching the Python dataclass defaults.
    #[must_use]
    pub const fn default_const() -> Self {
        Self {
            node_color: "#6366f1",
            edge_color: "#64748b",
            bg_color: "transparent",
            font_name: "Arial",
            node_shape: "record",
            node_style: "filled,rounded",
            edge_style: "solid",
            use_gradients: true,
            use_clusters: false,
        }
    }

    /// Produce a backward-compatible theme (gradients and clusters disabled)
    /// derived from the current configuration.
    ///
    /// Mirrors the default construction path in Python's `GraphVizGenerator`
    /// where gradients are explicitly disabled to preserve legacy test
    /// behaviour.
    #[must_use]
    pub const fn backward_compatible(self) -> Self {
        Self {
            use_gradients: false,
            use_clusters: false,
            ..self
        }
    }
}

impl Default for GraphVizTheme {
    fn default() -> Self {
        Self::default_const()
    }
}

/// Theme configuration for Mermaid diagram output.
///
/// Controls Mermaid ER diagram appearance using Mermaid's theming system.
/// Supports both built-in themes and custom CSS variables.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MermaidTheme {
    /// Built-in Mermaid theme (`"default"`, `"dark"`, `"forest"`, `"neutral"`, `"base"`).
    pub theme_name: &'static str,
    /// Primary entity color.
    pub primary_color: &'static str,
    /// Secondary UI color.
    pub secondary_color: &'static str,
    /// Enable custom CSS variable injection.
    pub use_custom_css: bool,
}

impl MermaidTheme {
    /// Default Mermaid theme matching the Python dataclass defaults.
    #[must_use]
    pub const fn default_const() -> Self {
        Self {
            theme_name: "default",
            primary_color: "#6366f1",
            secondary_color: "#ec4899",
            use_custom_css: true,
        }
    }
}

impl Default for MermaidTheme {
    fn default() -> Self {
        Self::default_const()
    }
}

/// Theme configuration for ASCII art diagram output.
///
/// Controls ASCII diagram rendering including box drawing characters,
/// ANSI colors, and Unicode icons for a modern terminal experience.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ASCIITheme {
    /// Box drawing style (`"single"`, `"double"`, `"rounded"`, `"heavy"`).
    pub box_style: &'static str,
    /// Use Unicode box-drawing characters (vs basic ASCII).
    pub use_unicode: bool,
    /// Enable ANSI color codes.
    pub use_colors: bool,
    /// Show Unicode / emoji icons for constraints.
    pub use_icons: bool,
    /// Color scheme name for ANSI colors.
    pub color_scheme: &'static str,
}

impl ASCIITheme {
    /// Default ASCII theme matching the Python dataclass defaults.
    #[must_use]
    pub const fn default_const() -> Self {
        Self {
            box_style: "rounded",
            use_unicode: true,
            use_colors: true,
            use_icons: true,
            color_scheme: "default",
        }
    }
}

impl Default for ASCIITheme {
    fn default() -> Self {
        Self::default_const()
    }
}

/// Complete theme configuration bundling all format-specific themes.
///
/// A [`Theme`] combines a color scheme with format-specific configurations
/// for GraphViz, Mermaid, and ASCII outputs into a coherent visual design.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Theme {
    /// Theme name (e.g., `"modern"`, `"dark"`).
    pub name: &'static str,
    /// Human-readable theme description.
    pub description: &'static str,
    /// Base color palette.
    pub color_scheme: ColorScheme,
    /// GraphViz-specific theme configuration.
    pub graphviz: GraphVizTheme,
    /// Mermaid-specific theme configuration.
    pub mermaid: MermaidTheme,
    /// ASCII-specific theme configuration.
    pub ascii: ASCIITheme,
}

// ---------------------------------------------------------------------------
// Preset: Modern (Default)
// ---------------------------------------------------------------------------

/// Color scheme used by the [`modern_theme`] preset.
#[must_use]
pub const fn modern_color_scheme() -> ColorScheme {
    ColorScheme {
        primary: "#6366f1",
        secondary: "#ec4899",
        background: "#f8fafc",
        text: "#0f172a",
        accent: "#8b5cf6",
        success: "#10b981",
        warning: "#f59e0b",
        error: "#ef4444",
        muted: "#94a3b8",
    }
}

/// GraphViz sub-theme used by the [`modern_theme`] preset.
#[must_use]
pub const fn modern_graphviz() -> GraphVizTheme {
    GraphVizTheme {
        node_color: "#6366f1",
        edge_color: "#64748b",
        bg_color: "transparent",
        font_name: "Arial",
        node_shape: "record",
        node_style: "filled,rounded",
        edge_style: "solid",
        use_gradients: true,
        use_clusters: false,
    }
}

/// Mermaid sub-theme used by the [`modern_theme`] preset.
#[must_use]
pub const fn modern_mermaid() -> MermaidTheme {
    MermaidTheme {
        theme_name: "default",
        primary_color: "#6366f1",
        secondary_color: "#ec4899",
        use_custom_css: true,
    }
}

/// ASCII sub-theme used by the [`modern_theme`] preset.
#[must_use]
pub const fn modern_ascii() -> ASCIITheme {
    ASCIITheme {
        box_style: "rounded",
        use_unicode: true,
        use_colors: true,
        use_icons: true,
        color_scheme: "default",
    }
}

/// The **Modern** preset: clean, professional design with indigo and pink accents.
#[must_use]
pub fn modern_theme() -> Theme {
    Theme {
        name: "modern",
        description: "Clean, professional design with indigo and pink accents",
        color_scheme: modern_color_scheme(),
        graphviz: modern_graphviz(),
        mermaid: modern_mermaid(),
        ascii: modern_ascii(),
    }
}

// ---------------------------------------------------------------------------
// Preset: Dark
// ---------------------------------------------------------------------------

/// Color scheme used by the [`dark_theme`] preset.
#[must_use]
pub const fn dark_color_scheme() -> ColorScheme {
    ColorScheme {
        primary: "#8b5cf6",
        secondary: "#d946ef",
        background: "#1e1b4b",
        text: "#f1f5f9",
        accent: "#a78bfa",
        success: "#34d399",
        warning: "#fbbf24",
        error: "#f87171",
        muted: "#64748b",
    }
}

/// GraphViz sub-theme used by the [`dark_theme`] preset.
#[must_use]
pub const fn dark_graphviz() -> GraphVizTheme {
    GraphVizTheme {
        node_color: "#8b5cf6",
        edge_color: "#64748b",
        bg_color: "#1e1b4b",
        font_name: "Arial",
        node_shape: "record",
        node_style: "filled,rounded",
        edge_style: "solid",
        use_gradients: true,
        use_clusters: false,
    }
}

/// Mermaid sub-theme used by the [`dark_theme`] preset.
#[must_use]
pub const fn dark_mermaid() -> MermaidTheme {
    MermaidTheme {
        theme_name: "dark",
        primary_color: "#8b5cf6",
        secondary_color: "#d946ef",
        use_custom_css: true,
    }
}

/// ASCII sub-theme used by the [`dark_theme`] preset.
#[must_use]
pub const fn dark_ascii() -> ASCIITheme {
    ASCIITheme {
        box_style: "rounded",
        use_unicode: true,
        use_colors: true,
        use_icons: true,
        color_scheme: "dark",
    }
}

/// The **Dark** preset: dark background with violet and fuchsia for dark-mode environments.
#[must_use]
pub fn dark_theme() -> Theme {
    Theme {
        name: "dark",
        description: "Dark background theme with violet and fuchsia for dark mode environments",
        color_scheme: dark_color_scheme(),
        graphviz: dark_graphviz(),
        mermaid: dark_mermaid(),
        ascii: dark_ascii(),
    }
}

// ---------------------------------------------------------------------------
// Preset: Forest
// ---------------------------------------------------------------------------

/// Color scheme used by the [`forest_theme`] preset.
#[must_use]
pub const fn forest_color_scheme() -> ColorScheme {
    ColorScheme {
        primary: "#10b981",
        secondary: "#14b8a6",
        background: "#f0fdf4",
        text: "#14532d",
        accent: "#059669",
        success: "#22c55e",
        warning: "#f59e0b",
        error: "#ef4444",
        muted: "#86efac",
    }
}

/// GraphViz sub-theme used by the [`forest_theme`] preset.
#[must_use]
pub const fn forest_graphviz() -> GraphVizTheme {
    GraphVizTheme {
        node_color: "#10b981",
        edge_color: "#059669",
        bg_color: "transparent",
        font_name: "Arial",
        node_shape: "record",
        node_style: "filled,rounded",
        edge_style: "solid",
        use_gradients: true,
        use_clusters: false,
    }
}

/// Mermaid sub-theme used by the [`forest_theme`] preset.
#[must_use]
pub const fn forest_mermaid() -> MermaidTheme {
    MermaidTheme {
        theme_name: "forest",
        primary_color: "#10b981",
        secondary_color: "#14b8a6",
        use_custom_css: true,
    }
}

/// ASCII sub-theme used by the [`forest_theme`] preset.
#[must_use]
pub const fn forest_ascii() -> ASCIITheme {
    ASCIITheme {
        box_style: "rounded",
        use_unicode: true,
        use_colors: true,
        use_icons: true,
        color_scheme: "forest",
    }
}

/// The **Forest** preset: nature-inspired emerald and teal on light green.
#[must_use]
pub fn forest_theme() -> Theme {
    Theme {
        name: "forest",
        description: "Nature-inspired theme with emerald and teal on light green background",
        color_scheme: forest_color_scheme(),
        graphviz: forest_graphviz(),
        mermaid: forest_mermaid(),
        ascii: forest_ascii(),
    }
}

// ---------------------------------------------------------------------------
// Preset: Minimal
// ---------------------------------------------------------------------------

/// Color scheme used by the [`minimal_theme`] preset.
#[must_use]
pub const fn minimal_color_scheme() -> ColorScheme {
    ColorScheme {
        primary: "#6b7280",
        secondary: "#64748b",
        background: "#ffffff",
        text: "#1f2937",
        accent: "#9ca3af",
        success: "#10b981",
        warning: "#f59e0b",
        error: "#ef4444",
        muted: "#d1d5db",
    }
}

/// GraphViz sub-theme used by the [`minimal_theme`] preset.
#[must_use]
pub const fn minimal_graphviz() -> GraphVizTheme {
    GraphVizTheme {
        node_color: "#6b7280",
        edge_color: "#9ca3af",
        bg_color: "transparent",
        font_name: "Arial",
        node_shape: "record",
        node_style: "filled",
        edge_style: "solid",
        use_gradients: false,
        use_clusters: false,
    }
}

/// Mermaid sub-theme used by the [`minimal_theme`] preset.
#[must_use]
pub const fn minimal_mermaid() -> MermaidTheme {
    MermaidTheme {
        theme_name: "neutral",
        primary_color: "#6b7280",
        secondary_color: "#64748b",
        use_custom_css: true,
    }
}

/// ASCII sub-theme used by the [`minimal_theme`] preset.
#[must_use]
pub const fn minimal_ascii() -> ASCIITheme {
    ASCIITheme {
        box_style: "single",
        use_unicode: true,
        use_colors: false,
        use_icons: false,
        color_scheme: "minimal",
    }
}

/// The **Minimal** preset: minimalist grayscale with subtle styling.
#[must_use]
pub fn minimal_theme() -> Theme {
    Theme {
        name: "minimal",
        description: "Minimalist grayscale theme with subtle styling",
        color_scheme: minimal_color_scheme(),
        graphviz: minimal_graphviz(),
        mermaid: minimal_mermaid(),
        ascii: minimal_ascii(),
    }
}

// ---------------------------------------------------------------------------
// Theme registry
// ---------------------------------------------------------------------------

/// Retrieve a preset theme by name.
///
/// Accepts `"modern"`, `"dark"`, `"forest"`, or `"minimal"`. Unknown names
/// return [`SurqlError::Validation`] mirroring the Python `ValueError`.
///
/// ## Examples
///
/// ```
/// use surql::schema::themes::get_theme;
///
/// let theme = get_theme("dark").unwrap();
/// assert_eq!(theme.name, "dark");
/// assert_eq!(theme.color_scheme.primary, "#8b5cf6");
/// ```
pub fn get_theme(name: &str) -> Result<Theme> {
    match name {
        "modern" => Ok(modern_theme()),
        "dark" => Ok(dark_theme()),
        "forest" => Ok(forest_theme()),
        "minimal" => Ok(minimal_theme()),
        other => Err(SurqlError::Validation {
            reason: format!(
                "Unknown theme: {other:?}. Available themes: dark, forest, minimal, modern"
            ),
        }),
    }
}

/// List the names of all built-in preset themes (sorted alphabetically).
///
/// ## Examples
///
/// ```
/// use surql::schema::themes::list_themes;
///
/// let names = list_themes();
/// assert_eq!(names, vec!["dark", "forest", "minimal", "modern"]);
/// ```
#[must_use]
pub fn list_themes() -> Vec<&'static str> {
    vec!["dark", "forest", "minimal", "modern"]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn color_scheme_defaults_match_python() {
        let cs = ColorScheme::default();
        assert_eq!(cs.primary, "#6366f1");
        assert_eq!(cs.secondary, "#ec4899");
        assert_eq!(cs.background, "#f8fafc");
        assert_eq!(cs.text, "#0f172a");
        assert_eq!(cs.accent, "#8b5cf6");
        assert_eq!(cs.success, "#10b981");
        assert_eq!(cs.warning, "#f59e0b");
        assert_eq!(cs.error, "#ef4444");
        assert_eq!(cs.muted, "#94a3b8");
    }

    #[test]
    fn graphviz_theme_defaults_match_python() {
        let t = GraphVizTheme::default();
        assert_eq!(t.node_color, "#6366f1");
        assert_eq!(t.edge_color, "#64748b");
        assert_eq!(t.bg_color, "transparent");
        assert_eq!(t.font_name, "Arial");
        assert_eq!(t.node_shape, "record");
        assert_eq!(t.node_style, "filled,rounded");
        assert_eq!(t.edge_style, "solid");
        assert!(t.use_gradients);
        assert!(!t.use_clusters);
    }

    #[test]
    fn mermaid_theme_defaults_match_python() {
        let t = MermaidTheme::default();
        assert_eq!(t.theme_name, "default");
        assert_eq!(t.primary_color, "#6366f1");
        assert_eq!(t.secondary_color, "#ec4899");
        assert!(t.use_custom_css);
    }

    #[test]
    fn ascii_theme_defaults_match_python() {
        let t = ASCIITheme::default();
        assert_eq!(t.box_style, "rounded");
        assert!(t.use_unicode);
        assert!(t.use_colors);
        assert!(t.use_icons);
        assert_eq!(t.color_scheme, "default");
    }

    #[test]
    fn modern_theme_matches_python() {
        let t = modern_theme();
        assert_eq!(t.name, "modern");
        assert_eq!(
            t.description,
            "Clean, professional design with indigo and pink accents"
        );
        assert_eq!(t.color_scheme.primary, "#6366f1");
        assert_eq!(t.graphviz.node_color, "#6366f1");
        assert_eq!(t.mermaid.theme_name, "default");
        assert_eq!(t.ascii.box_style, "rounded");
    }

    #[test]
    fn dark_theme_matches_python() {
        let t = dark_theme();
        assert_eq!(t.name, "dark");
        assert_eq!(t.color_scheme.primary, "#8b5cf6");
        assert_eq!(t.color_scheme.secondary, "#d946ef");
        assert_eq!(t.color_scheme.background, "#1e1b4b");
        assert_eq!(t.graphviz.bg_color, "#1e1b4b");
        assert_eq!(t.mermaid.theme_name, "dark");
        assert_eq!(t.ascii.color_scheme, "dark");
    }

    #[test]
    fn forest_theme_matches_python() {
        let t = forest_theme();
        assert_eq!(t.name, "forest");
        assert_eq!(t.color_scheme.primary, "#10b981");
        assert_eq!(t.color_scheme.secondary, "#14b8a6");
        assert_eq!(t.graphviz.edge_color, "#059669");
        assert_eq!(t.mermaid.theme_name, "forest");
    }

    #[test]
    fn minimal_theme_matches_python() {
        let t = minimal_theme();
        assert_eq!(t.name, "minimal");
        assert_eq!(t.color_scheme.primary, "#6b7280");
        assert_eq!(t.graphviz.node_style, "filled");
        assert!(!t.graphviz.use_gradients);
        assert_eq!(t.mermaid.theme_name, "neutral");
        assert_eq!(t.ascii.box_style, "single");
        assert!(!t.ascii.use_colors);
        assert!(!t.ascii.use_icons);
    }

    #[test]
    fn get_theme_known_names() {
        assert_eq!(get_theme("modern").unwrap().name, "modern");
        assert_eq!(get_theme("dark").unwrap().name, "dark");
        assert_eq!(get_theme("forest").unwrap().name, "forest");
        assert_eq!(get_theme("minimal").unwrap().name, "minimal");
    }

    #[test]
    fn get_theme_unknown_errors() {
        let err = get_theme("neon").unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
        assert!(err.to_string().contains("Unknown theme"));
    }

    #[test]
    fn list_themes_is_sorted() {
        let names = list_themes();
        assert_eq!(names, vec!["dark", "forest", "minimal", "modern"]);
    }

    #[test]
    fn graphviz_backward_compatible_disables_flags() {
        let t = modern_graphviz().backward_compatible();
        assert!(!t.use_gradients);
        assert!(!t.use_clusters);
        assert_eq!(t.node_color, "#6366f1");
    }
}
