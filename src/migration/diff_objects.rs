//! Diffing for database-level named objects.
//!
//! Sequences, functions, and params are all shaped the same way: a name, a
//! definition that renders its own `DEFINE` / `DEFINE OVERWRITE` / `REMOVE`
//! statements, and no table context. [`diff_named`] captures that once so
//! each kind is three lines rather than a copy of [`super::diff::diff_buckets`].
//!
//! Lives beside [`super::diff`] rather than inside it because that module is
//! already well past the repository's 1000-LOC budget.

use std::collections::BTreeMap;

use crate::migration::models::{DiffOperation, SchemaDiff};
use crate::schema::function::FunctionDefinition;
use crate::schema::param::ParamDefinition;
use crate::schema::sequence::SequenceDefinition;

/// The three [`DiffOperation`] variants one kind of object uses.
#[derive(Debug, Clone, Copy)]
pub struct ObjectDiffKinds {
    /// Operation for an object present in code but not in the database.
    pub add: DiffOperation,
    /// Operation for an object whose definition differs.
    pub modify: DiffOperation,
    /// Operation for an object present in the database but not in code.
    pub drop: DiffOperation,
}

/// Build a [`SchemaDiff`] for a database-level object.
///
/// `table` stays empty and the name lands in [`SchemaDiff::object`]; these
/// objects are not scoped to a table.
pub fn object_diff(
    operation: DiffOperation,
    name: &str,
    forward_sql: String,
    backward_sql: String,
) -> SchemaDiff {
    SchemaDiff {
        operation,
        table: String::new(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: Some(name.to_owned()),
        description: format!("{operation} {name}"),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

/// Compare two slices of database-level objects by name.
///
/// Objects only in `code` are added, objects only in `db` are dropped, and
/// objects in both whose definitions differ are modified. A modification
/// renders the `OVERWRITE` form in both directions, so applying either way
/// replaces the stored definition rather than failing on a name clash.
///
/// The results are ordered adds, then drops, then modifications, matching
/// [`super::diff::diff_buckets`].
pub fn diff_named<T: PartialEq>(
    code: &[T],
    db: &[T],
    kinds: ObjectDiffKinds,
    name_of: impl Fn(&T) -> &str,
    define: impl Fn(&T) -> String,
    overwrite: impl Fn(&T) -> String,
    remove: impl Fn(&T) -> String,
) -> Vec<SchemaDiff> {
    let index = |items: &'_ [T]| -> BTreeMap<String, usize> {
        items
            .iter()
            .enumerate()
            .map(|(i, item)| (name_of(item).to_owned(), i))
            .collect()
    };
    let code_map = index(code);
    let db_map = index(db);
    let mut out = Vec::new();

    for (name, &i) in &code_map {
        if !db_map.contains_key(name) {
            out.push(object_diff(
                kinds.add,
                name,
                define(&code[i]),
                remove(&code[i]),
            ));
        }
    }
    for (name, &i) in &db_map {
        if !code_map.contains_key(name) {
            out.push(object_diff(
                kinds.drop,
                name,
                remove(&db[i]),
                define(&db[i]),
            ));
        }
    }
    for (name, &i) in &code_map {
        if let Some(&j) = db_map.get(name) {
            if code[i] != db[j] {
                out.push(object_diff(
                    kinds.modify,
                    name,
                    overwrite(&code[i]),
                    overwrite(&db[j]),
                ));
            }
        }
    }
    out
}

/// Compare two sequence slices.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff_objects::diff_sequences;
/// use surql::migration::DiffOperation;
/// use surql::schema::SequenceDefinition;
///
/// let code = vec![SequenceDefinition::new("invoice_no")];
/// let diffs = diff_sequences(&code, &[]);
/// assert_eq!(diffs[0].operation, DiffOperation::AddSequence);
/// assert_eq!(diffs[0].object.as_deref(), Some("invoice_no"));
/// assert_eq!(diffs[0].backward_sql, "REMOVE SEQUENCE IF EXISTS invoice_no;");
/// ```
#[must_use]
pub fn diff_sequences(code: &[SequenceDefinition], db: &[SequenceDefinition]) -> Vec<SchemaDiff> {
    diff_named(
        code,
        db,
        ObjectDiffKinds {
            add: DiffOperation::AddSequence,
            modify: DiffOperation::ModifySequence,
            drop: DiffOperation::DropSequence,
        },
        |s| s.name.as_str(),
        |s| s.to_surql().unwrap_or_default(),
        |s| s.to_surql_overwrite().unwrap_or_default(),
        SequenceDefinition::to_remove_surql,
    )
}

/// Compare two function slices.
///
/// Both sides go through [`FunctionDefinition::normalized`] first, because
/// the engine rewrites what it stores (`option<T>` to `none | T`, no trailing
/// `;`, an explicit `PERMISSIONS FULL`). Comparing the raw spellings would
/// report a modification on every reconcile.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff_objects::diff_functions;
/// use surql::migration::DiffOperation;
/// use surql::schema::FunctionDefinition;
///
/// let code = vec![FunctionDefinition::new("greet", "RETURN 'hi'")];
/// let diffs = diff_functions(&code, &[]);
/// assert_eq!(diffs[0].operation, DiffOperation::AddFunction);
/// assert_eq!(diffs[0].backward_sql, "REMOVE FUNCTION IF EXISTS fn::greet;");
/// ```
#[must_use]
pub fn diff_functions(code: &[FunctionDefinition], db: &[FunctionDefinition]) -> Vec<SchemaDiff> {
    let canonical = |items: &[FunctionDefinition]| -> Vec<FunctionDefinition> {
        items.iter().map(FunctionDefinition::normalized).collect()
    };
    diff_named(
        &canonical(code),
        &canonical(db),
        ObjectDiffKinds {
            add: DiffOperation::AddFunction,
            modify: DiffOperation::ModifyFunction,
            drop: DiffOperation::DropFunction,
        },
        |f| f.name.as_str(),
        |f| f.to_surql().unwrap_or_default(),
        |f| f.to_surql_overwrite().unwrap_or_default(),
        FunctionDefinition::to_remove_surql,
    )
}

/// Compare two param slices.
///
/// Both sides go through [`ParamDefinition::normalized`] first, for the same
/// reason as [`diff_functions`]: the engine echoes an explicit
/// `PERMISSIONS FULL` a consumer never wrote.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff_objects::diff_params;
/// use surql::migration::DiffOperation;
/// use surql::schema::ParamDefinition;
///
/// let code = vec![ParamDefinition::new("APP", "'oneiriq'")];
/// let diffs = diff_params(&code, &[]);
/// assert_eq!(diffs[0].operation, DiffOperation::AddParam);
/// assert_eq!(diffs[0].backward_sql, "REMOVE PARAM IF EXISTS $APP;");
/// ```
#[must_use]
pub fn diff_params(code: &[ParamDefinition], db: &[ParamDefinition]) -> Vec<SchemaDiff> {
    let canonical = |items: &[ParamDefinition]| -> Vec<ParamDefinition> {
        items.iter().map(ParamDefinition::normalized).collect()
    };
    diff_named(
        &canonical(code),
        &canonical(db),
        ObjectDiffKinds {
            add: DiffOperation::AddParam,
            modify: DiffOperation::ModifyParam,
            drop: DiffOperation::DropParam,
        },
        |p| p.name.as_str(),
        |p| p.to_surql().unwrap_or_default(),
        |p| p.to_surql_overwrite().unwrap_or_default(),
        ParamDefinition::to_remove_surql,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn added_param() {
        let code = vec![ParamDefinition::new("APP", "'oneiriq'")];
        let diffs = diff_params(&code, &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddParam);
        assert_eq!(diffs[0].object.as_deref(), Some("APP"));
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE PARAM $APP VALUE 'oneiriq' PERMISSIONS FULL;"
        );
        assert_eq!(diffs[0].backward_sql, "REMOVE PARAM IF EXISTS $APP;");
    }

    #[test]
    fn dropped_param() {
        let db = vec![ParamDefinition::new("OLD", "1")];
        let diffs = diff_params(&[], &db);
        assert_eq!(diffs[0].operation, DiffOperation::DropParam);
        assert_eq!(diffs[0].forward_sql, "REMOVE PARAM IF EXISTS $OLD;");
    }

    #[test]
    fn modified_param_renders_overwrite_both_ways() {
        let code = vec![ParamDefinition::new("P", "2")];
        let db = vec![ParamDefinition::new("P", "1")];
        let diffs = diff_params(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyParam);
        assert!(diffs[0].forward_sql.contains("OVERWRITE $P VALUE 2"));
        assert!(diffs[0].backward_sql.contains("OVERWRITE $P VALUE 1"));
    }

    #[test]
    fn the_engine_echo_of_a_param_is_not_a_modification() {
        let code = vec![ParamDefinition::new("P", "'hello'")];
        let db = vec![ParamDefinition::new("P", "'hello'").with_permissions("FULL")];
        assert!(diff_params(&code, &db).is_empty());
    }

    #[test]
    fn added_function() {
        let code = vec![FunctionDefinition::new("greet", "RETURN 'hi'")];
        let diffs = diff_functions(&code, &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddFunction);
        assert_eq!(diffs[0].object.as_deref(), Some("greet"));
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE FUNCTION fn::greet() { RETURN 'hi' } PERMISSIONS FULL;"
        );
        assert_eq!(
            diffs[0].backward_sql,
            "REMOVE FUNCTION IF EXISTS fn::greet;"
        );
    }

    #[test]
    fn dropped_function() {
        let db = vec![FunctionDefinition::new("old", "RETURN 1")];
        let diffs = diff_functions(&[], &db);
        assert_eq!(diffs[0].operation, DiffOperation::DropFunction);
        assert_eq!(diffs[0].forward_sql, "REMOVE FUNCTION IF EXISTS fn::old;");
    }

    #[test]
    fn modified_function_renders_overwrite_both_ways() {
        let code = vec![FunctionDefinition::new("f", "RETURN 2")];
        let db = vec![FunctionDefinition::new("f", "RETURN 1")];
        let diffs = diff_functions(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyFunction);
        assert!(diffs[0].forward_sql.contains("OVERWRITE fn::f"));
        assert!(diffs[0].forward_sql.contains("RETURN 2"));
        assert!(diffs[0].backward_sql.contains("RETURN 1"));
    }

    /// The whole point of normalising: what a consumer writes and what the
    /// engine echoes must not look like a change.
    #[test]
    fn the_engine_echo_of_a_function_is_not_a_modification() {
        let code = vec![
            crate::schema::function_schema("greet", "RETURN 'hi ' + $name;")
                .arg("name", "option<string>")
                .build()
                .unwrap(),
        ];
        let db = vec![
            crate::schema::function_schema("greet", "RETURN 'hi ' + $name")
                .arg("name", "none | string")
                .permissions("FULL")
                .build()
                .unwrap(),
        ];
        assert!(diff_functions(&code, &db).is_empty());
    }

    #[test]
    fn added_sequence() {
        let code = vec![SequenceDefinition::new("s")];
        let diffs = diff_sequences(&code, &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddSequence);
        assert_eq!(diffs[0].object.as_deref(), Some("s"));
        assert!(diffs[0].table.is_empty());
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE SEQUENCE s BATCH 1000 START 0;"
        );
        assert_eq!(diffs[0].backward_sql, "REMOVE SEQUENCE IF EXISTS s;");
    }

    #[test]
    fn dropped_sequence() {
        let db = vec![SequenceDefinition::new("old")];
        let diffs = diff_sequences(&[], &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropSequence);
        assert_eq!(diffs[0].forward_sql, "REMOVE SEQUENCE IF EXISTS old;");
        assert!(diffs[0].backward_sql.starts_with("DEFINE SEQUENCE old"));
    }

    #[test]
    fn modified_sequence_renders_overwrite_both_ways() {
        let code = vec![SequenceDefinition::new("s").with_batch(500)];
        let db = vec![SequenceDefinition::new("s")];
        let diffs = diff_sequences(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifySequence);
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE SEQUENCE OVERWRITE s BATCH 500 START 0;"
        );
        assert_eq!(
            diffs[0].backward_sql,
            "DEFINE SEQUENCE OVERWRITE s BATCH 1000 START 0;"
        );
    }

    #[test]
    fn identical_sequences_produce_nothing() {
        let s = vec![SequenceDefinition::new("s").with_timeout("1s")];
        assert!(diff_sequences(&s, &s).is_empty());
    }

    #[test]
    fn adds_drops_and_modifications_are_ordered() {
        let code = vec![
            SequenceDefinition::new("keep").with_batch(2),
            SequenceDefinition::new("new"),
        ];
        let db = vec![
            SequenceDefinition::new("keep"),
            SequenceDefinition::new("gone"),
        ];
        let ops: Vec<DiffOperation> = diff_sequences(&code, &db)
            .iter()
            .map(|d| d.operation)
            .collect();
        assert_eq!(
            ops,
            vec![
                DiffOperation::AddSequence,
                DiffOperation::DropSequence,
                DiffOperation::ModifySequence,
            ]
        );
    }
}
