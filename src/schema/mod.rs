//! Schema definition layer.
//!
//! Port of `surql/schema/` from `oneiriq-surql` (Python). This module currently
//! covers the pure definition types:
//!
//! - [`fields`]: [`FieldDefinition`] + [`FieldType`] enum and builder helpers
//!   ([`string_field`], [`int_field`], [`float_field`], [`bool_field`],
//!   [`datetime_field`], [`record_field`], [`array_field`], [`object_field`],
//!   [`computed_field`]).
//! - [`table`]: [`TableDefinition`] + [`TableMode`], [`IndexDefinition`] /
//!   [`IndexType`] / [`MTreeDistanceType`] / [`HnswDistanceType`] /
//!   [`MTreeVectorType`], and [`EventDefinition`]; plus [`table_schema`],
//!   [`index`], [`unique_index`], [`search_index`], [`mtree_index`],
//!   [`hnsw_index`], [`event`] builders.
//! - [`edge`]: [`EdgeDefinition`] + [`EdgeMode`] and [`edge_schema`] /
//!   [`typed_edge`] / [`bidirectional_edge`] helpers.
//! - [`access`]: [`AccessDefinition`] + [`AccessType`], [`JwtConfig`] /
//!   [`RecordAccessConfig`] credential-config types, and the
//!   [`access_schema`] / [`jwt_access`] / [`record_access`] helpers.
//! - [`analyzer`]: [`AnalyzerDefinition`] + [`Tokenizer`] / [`TokenFilter`] and
//!   the [`analyzer`](analyzer()) / [`standard_analyzer`] helpers for
//!   `DEFINE ANALYZER` (the lexical side of full-text `SEARCH` indexes).
//! - [`bucket`]: [`BucketDefinition`] + the [`bucket_schema`] /
//!   [`memory_bucket`] / [`file_bucket`] helpers for SurrealDB v3
//!   object-storage `DEFINE BUCKET` / `ALTER BUCKET` / `REMOVE BUCKET`.
//! - [`view`]: [`ViewDefinition`] / [`ViewGroup`], the `AS SELECT` body of a
//!   pre-computed view table the engine maintains from its sources.
//! - [`changefeed`]: [`ChangeFeed`], the `CHANGEFEED <duration>
//!   [INCLUDE ORIGINAL]` mutation log a table can retain; read it back with
//!   [`crate::query::changes`].
//! - [`index`]: [`IndexDefinition`] and the `DEFINE INDEX` builders, plus the
//!   `CONCURRENTLY` background build and its [`info_for_index_surql`] /
//!   [`IndexBuildStatus`] progress readout. Re-exported from [`table`].
//! - [`sequence`]: [`SequenceDefinition`] and [`sequence_schema`] for the
//!   monotonic `DEFINE SEQUENCE` counters behind `sequence::nextval`.
//! - [`reference`]: [`ReferenceAction`] and the rules governing
//!   `DEFINE FIELD ... REFERENCE ON DELETE ...` record-reference tracking,
//!   whose reverse half is a `COMPUTED <~table` field
//!   ([`reverse_reference_field`]).
//!
//! Each value object exposes a `to_surql*` method that renders the matching
//! `DEFINE` statement.
//!
//! - [`sql`]: free functions ([`generate_table_sql`], [`generate_edge_sql`],
//!   [`generate_access_sql`], [`generate_analyzer_sql`], [`generate_schema_sql`])
//!   composing full DEFINE-statement scripts from the definitions above.
//! - [`registry`]: process-wide [`SchemaRegistry`] singleton plus the
//!   [`get_registry`], [`register_table`], [`register_edge`],
//!   [`clear_registry`], [`get_registered_tables`], and
//!   [`get_registered_edges`] helpers.
//! - [`validator`]: cross-schema validation comparing code-defined schemas
//!   against database-observed schemas; returns a `Vec<ValidationResult>`.
//! - [`validator_utils`]: filtering, grouping, summary, and human-readable
//!   report helpers for working with validation results.
//! - [`parser`]: inverse of the definition-to-SurrealQL path — parses
//!   `INFO FOR DB` / `INFO FOR TABLE` responses back into [`TableDefinition`]
//!   / [`EdgeDefinition`] / [`AccessDefinition`] values.
//! - [`themes`]: preset themes ([`modern_theme`], [`dark_theme`],
//!   [`forest_theme`], [`minimal_theme`]) plus [`Theme`], [`ColorScheme`],
//!   [`GraphVizTheme`], [`MermaidTheme`], [`ASCIITheme`] and the
//!   [`get_theme`] / [`list_themes`] helpers.
//! - [`visualize`]: [`generate_mermaid`], [`generate_graphviz`],
//!   [`generate_ascii`] diagram generators plus [`visualize_schema`] /
//!   [`visualize_from_registry`] and the [`OutputFormat`] enum.
//! - [`utils`]: shared helpers used by the visualiser
//!   ([`display_width`], [`strip_ansi`]).

pub mod access;
pub mod analyzer;
pub mod bucket;
pub mod changefeed;
pub mod edge;
pub mod field_type;
pub mod fields;
pub mod index;
pub mod parser;
pub mod reference;
pub mod registry;
pub mod sequence;
pub mod sql;
pub mod table;
pub mod themes;
pub mod utils;
pub mod validator;
pub mod validator_utils;
pub mod view;
pub mod visualize;

pub use access::{
    access_schema, jwt_access, record_access, AccessDefinition, AccessSchemaBuilder, AccessType,
    JwtConfig, RecordAccessConfig,
};
pub use analyzer::{analyzer, standard_analyzer, AnalyzerDefinition, TokenFilter, Tokenizer};
pub use bucket::{
    bucket_schema, file_bucket, memory_bucket, BucketDefinition, BucketSchemaBuilder,
};
pub use changefeed::ChangeFeed;
pub use edge::{bidirectional_edge, edge_schema, typed_edge, EdgeDefinition, EdgeMode};
pub use fields::{
    array_field, bool_field, bytes_field, computed_field, datetime_field, field, file_field,
    float_field, int_field, object_field, record_field, reverse_reference_field, string_field,
    validate_field_name, FieldBuilder, FieldDefinition, FieldType,
};
pub use index::{info_for_index_surql, IndexBuildStatus};
pub use parser::{
    parse_access, parse_bucket, parse_db_info, parse_edge_info, parse_event, parse_field,
    parse_fields, parse_index, parse_indexes, parse_sequence, parse_table_info, parse_table_mode,
    parse_table_permissions, parse_view, DatabaseInfo,
};
pub use reference::ReferenceAction;
pub use registry::{
    clear_registry, get_registered_buckets, get_registered_edges, get_registered_tables,
    get_registry, register_bucket, register_edge, register_table, SchemaRegistry,
};
pub use sequence::{sequence_schema, SequenceDefinition, SequenceSchemaBuilder};
pub use sql::{
    generate_access_sql, generate_access_sql_with_options, generate_analyzer_sql,
    generate_analyzer_sql_with_options, generate_bucket_sql, generate_bucket_sql_with_options,
    generate_edge_sql, generate_schema_sql, generate_sequence_sql,
    generate_sequence_sql_with_options, generate_table_sql, generate_table_sql_overwrite,
};
pub use table::{
    bm25_index, event, hnsw_index, index, mtree_index, search_index, table_schema, unique_index,
    EventDefinition, HnswDistanceType, IndexDefinition, IndexType, MTreeDistanceType,
    MTreeVectorType, TableDefinition, TableMode,
};
pub use themes::{
    dark_ascii, dark_color_scheme, dark_graphviz, dark_mermaid, dark_theme, forest_ascii,
    forest_color_scheme, forest_graphviz, forest_mermaid, forest_theme, get_theme, list_themes,
    minimal_ascii, minimal_color_scheme, minimal_graphviz, minimal_mermaid, minimal_theme,
    modern_ascii, modern_color_scheme, modern_graphviz, modern_mermaid, modern_theme, ASCIITheme,
    ColorScheme, GraphVizTheme, MermaidTheme, Theme,
};
pub use utils::{char_display_width, display_width, strip_ansi};
pub use validator::{
    normalize_expression, validate_edge, validate_edges, validate_field, validate_index,
    validate_schema, validate_table, validate_tables, ValidationResult, ValidationSeverity,
};
pub use validator_utils::{
    filter_by_severity, filter_errors, filter_warnings, format_validation_report,
    get_validation_summary, group_by_table, has_errors, ValidationSummary,
};
pub use view::{ViewDefinition, ViewGroup};
pub use visualize::{
    generate_ascii, generate_graphviz, generate_mermaid, visualize_from_registry, visualize_schema,
    OutputFormat, ThemeOption,
};
