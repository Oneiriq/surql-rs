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
//!
//! Each value object exposes a `to_surql*` method that renders the matching
//! `DEFINE` statement.
//!
//! - [`sql`]: free functions ([`generate_table_sql`], [`generate_edge_sql`],
//!   [`generate_access_sql`], [`generate_schema_sql`]) composing full
//!   DEFINE-statement scripts from the definitions above.
//! - [`registry`]: process-wide [`SchemaRegistry`] singleton plus the
//!   [`get_registry`], [`register_table`], [`register_edge`],
//!   [`clear_registry`], [`get_registered_tables`], and
//!   [`get_registered_edges`] helpers.
//! - [`validator`]: cross-schema validation comparing code-defined schemas
//!   against database-observed schemas; returns a `Vec<ValidationResult>`.
//! - [`validator_utils`]: filtering, grouping, summary, and human-readable
//!   report helpers for working with validation results.
//!
//! The schema parser and the visualiser (themes / visualize / utils) land in
//! follow-up PRs.

pub mod access;
pub mod edge;
pub mod fields;
pub mod registry;
pub mod sql;
pub mod table;
pub mod validator;
pub mod validator_utils;

pub use access::{
    access_schema, jwt_access, record_access, AccessDefinition, AccessSchemaBuilder, AccessType,
    JwtConfig, RecordAccessConfig,
};
pub use edge::{bidirectional_edge, edge_schema, typed_edge, EdgeDefinition, EdgeMode};
pub use fields::{
    array_field, bool_field, computed_field, datetime_field, field, float_field, int_field,
    object_field, record_field, string_field, validate_field_name, FieldBuilder, FieldDefinition,
    FieldType,
};
pub use registry::{
    clear_registry, get_registered_edges, get_registered_tables, get_registry, register_edge,
    register_table, SchemaRegistry,
};
pub use sql::{generate_access_sql, generate_edge_sql, generate_schema_sql, generate_table_sql};
pub use table::{
    event, hnsw_index, index, mtree_index, search_index, table_schema, unique_index,
    EventDefinition, HnswDistanceType, IndexDefinition, IndexType, MTreeDistanceType,
    MTreeVectorType, TableDefinition, TableMode,
};
pub use validator::{
    normalize_expression, validate_edge, validate_edges, validate_field, validate_index,
    validate_schema, validate_table, validate_tables, ValidationResult, ValidationSeverity,
};
pub use validator_utils::{
    filter_by_severity, filter_errors, filter_warnings, format_validation_report,
    get_validation_summary, group_by_table, has_errors, ValidationSummary,
};
