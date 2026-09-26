//! Unit tests for the schema diff, split by what they exercise: expression
//! normalisation and validation, table-level walks (tables, change feeds,
//! views, whole snapshots), and the members of a table (fields, indexes,
//! events, permissions, edges).

use std::collections::BTreeSet;

use super::validate::safe_default_regex;
use super::*;
use crate::schema::edge::{EdgeDefinition, EdgeMode};
use crate::schema::fields::{FieldDefinition, FieldType};
use crate::schema::table::{
    diskann_index, event, hnsw_index, index, mtree_index, table_schema, unique_index,
    DiskAnnDistanceType, HnswDistanceType, IndexDefinition, IndexType, MTreeDistanceType,
    MTreeVectorType, TableMode,
};

mod expressions;
mod members;
mod tables;

fn tbl(name: &str) -> TableDefinition {
    table_schema(name)
}

fn f(name: &str, ty: FieldType) -> FieldDefinition {
    FieldDefinition::new(name, ty)
}

fn relation_edge(name: &str) -> EdgeDefinition {
    EdgeDefinition::new(name)
        .with_mode(EdgeMode::Relation)
        .with_from_table("user")
        .with_to_table("post")
}
