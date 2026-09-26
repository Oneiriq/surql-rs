//! Rendering a [`Query`] to SurrealQL.
//!
//! Every name and target is checked again here, so a `Query` assembled by
//! setting its public fields is held to the same rules as one built through
//! the methods.

use serde_json::Value;

use crate::error::{Result, SurqlError};
use crate::query::expressions::Expression;
use crate::query::helpers::DataMap;
use crate::query::hints::{check_hints, render_hints};
use crate::query::validate::{
    render_target, validate_field_path, validate_finite, validate_identifier, validate_set_target,
};
use crate::types::operators::{quote_object_key, quote_value_public};

use super::{Operation, Query};

/// Render the `CONTENT` object of a `CREATE` / `UPSERT` / `RELATE`: the
/// data map's entries followed by the `set` / `set_expr` assignments, which
/// replace a same-named data key. Keys are quoted as object keys; an
/// assignment key must be a plain field name (a dotted path means a nested
/// assignment, which only `UPDATE ... SET` can express).
fn render_content(data: Option<&DataMap>, exprs: &[(String, Expression)]) -> Result<String> {
    let mut parts: Vec<String> = data
        .into_iter()
        .flatten()
        .filter(|(k, _)| !exprs.iter().any(|(field, _)| field == *k))
        .map(|(k, v)| format!("{}: {}", quote_object_key(k), quote_value_public(v)))
        .collect();
    for (field, expr) in exprs {
        validate_identifier(field, "content field name")?;
        parts.push(format!("{}: {}", quote_object_key(field), expr.to_surql()));
    }
    Ok(format!("{{{}}}", parts.join(", ")))
}

/// Render a vector literal, refusing non-finite values.
pub(super) fn render_vector(vector: &[f64]) -> Result<String> {
    validate_finite(vector, "vector values")?;
    let inner = vector
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("[{inner}]"))
}

fn render_where(conditions: &[String]) -> Option<String> {
    (!conditions.is_empty()).then(|| {
        let joined = conditions
            .iter()
            .map(|c| format!("({c})"))
            .collect::<Vec<_>>()
            .join(" AND ");
        format!("WHERE {joined}")
    })
}

impl Query {
    /// Render the full SurrealQL statement.
    ///
    /// Re-checks every name and target it renders and returns
    /// [`SurqlError::Validation`] for one that is not acceptable (a
    /// `group_by` field that is not a field path, a hand-set target, ...).
    pub fn to_surql(&self) -> Result<String> {
        let op = self.operation.ok_or_else(|| SurqlError::Query {
            reason: "Query operation not specified".into(),
        })?;

        let base = match op {
            Operation::Select => self.build_select()?,
            Operation::Insert => self.build_insert()?,
            Operation::Update => self.build_update()?,
            Operation::Delete => self.build_delete()?,
            Operation::Upsert => self.build_upsert()?,
            Operation::Relate => self.build_relate()?,
        };

        if self.hints.is_empty() {
            Ok(base)
        } else {
            check_hints(&self.hints)?;
            let hint_str = render_hints(&self.hints);
            Ok(format!("{hint_str}\n{base}"))
        }
    }

    /// The rendered target: the stored table or record id, re-checked.
    fn require_table(&self, op: Operation) -> Result<String> {
        let table = self
            .table_name
            .as_deref()
            .ok_or_else(|| SurqlError::Query {
                reason: format!("Table name required for {} query", op.as_str()),
            })?;
        render_target(table)
    }

    fn return_clause(&self) -> Option<String> {
        self.return_format
            .map(|fmt| format!("RETURN {}", fmt.to_surql()))
    }

    fn build_select(&self) -> Result<String> {
        let table = self.require_table(Operation::Select)?;
        let fields_str = if self.fields.is_empty() {
            "*".to_string()
        } else {
            self.fields.join(", ")
        };

        let mut parts: Vec<String> = Vec::new();
        let first = if let Some(traverse) = &self.graph_traversal {
            format!("SELECT {fields_str} FROM {table}{traverse}")
        } else {
            format!("SELECT {fields_str} FROM {table}")
        };
        parts.push(first);

        for join in &self.join_clauses {
            parts.push(join.clone());
        }

        // Build WHERE conditions (vector search first, then regular).
        let mut where_parts: Vec<String> = Vec::new();
        if let (Some(field), Some(k), false) = (
            &self.vector_field,
            self.vector_k,
            self.vector_value.is_empty(),
        ) {
            validate_field_path(field, "vector search field")?;
            let vector_str = render_vector(&self.vector_value)?;
            // An integer second operand selects the index; a metric name
            // makes the engine compare every row.
            let operator = match (self.vector_ef, self.vector_distance, self.vector_threshold) {
                (Some(ef), _, _) => Some(format!("<|{k},{ef}|>")),
                (None, Some(distance), Some(t)) => {
                    validate_finite(&[t], "vector threshold")?;
                    Some(format!("<|{k},{},{t}|>", distance.to_surql()))
                }
                (None, Some(distance), None) => Some(format!("<|{k},{}|>", distance.to_surql())),
                (None, None, _) => None,
            };
            if let Some(operator) = operator {
                where_parts.push(format!("{field} {operator} {vector_str}"));
            }
        }
        if let (Some(field), Some(reference), Some(query)) = (
            &self.fulltext_field,
            self.fulltext_reference,
            &self.fulltext_query,
        ) {
            validate_field_path(field, "full-text search field")?;
            let quoted = quote_value_public(&Value::String(query.clone()));
            where_parts.push(format!("{field} @{reference}@ {quoted}"));
        }
        for cond in &self.conditions {
            where_parts.push(format!("({cond})"));
        }
        if !where_parts.is_empty() {
            parts.push(format!("WHERE {}", where_parts.join(" AND ")));
        }

        if self.group_all_flag {
            parts.push("GROUP ALL".to_string());
        } else if !self.group_fields.is_empty() {
            for field in &self.group_fields {
                validate_field_path(field, "group field")?;
            }
            parts.push(format!("GROUP BY {}", self.group_fields.join(", ")));
        }

        if !self.order_fields.is_empty() {
            let mut rendered = Vec::with_capacity(self.order_fields.len());
            for o in &self.order_fields {
                validate_field_path(&o.field, "order field")?;
                let direction = match o.direction.to_ascii_uppercase().as_str() {
                    "ASC" => "ASC",
                    "DESC" => "DESC",
                    other => {
                        return Err(SurqlError::Validation {
                            reason: format!("Invalid direction: {other}. Must be ASC or DESC"),
                        })
                    }
                };
                rendered.push(format!("{} {direction}", o.field));
            }
            parts.push(format!("ORDER BY {}", rendered.join(", ")));
        }

        if let Some(n) = self.limit_value {
            parts.push(format!("LIMIT {n}"));
        }
        if let Some(n) = self.offset_value {
            parts.push(format!("START {n}"));
        }

        Ok(parts.join(" "))
    }

    fn build_insert(&self) -> Result<String> {
        let table = self.require_table(Operation::Insert)?;
        let data = self.insert_data.as_ref().ok_or_else(|| SurqlError::Query {
            reason: "Insert data required for INSERT query".into(),
        })?;

        let data_str = render_content(Some(data), &self.update_set_exprs)?;
        let mut parts = vec![format!("CREATE {table} CONTENT {data_str}")];
        parts.extend(self.return_clause());
        Ok(parts.join(" "))
    }

    fn build_update(&self) -> Result<String> {
        let table = self.require_table(Operation::Update)?;

        let mut assignments: Vec<String> = Vec::new();
        for (k, v) in self.update_data.iter().flatten() {
            validate_set_target(k)?;
            assignments.push(format!("{k} = {}", quote_value_public(v)));
        }
        for (k, expr) in &self.update_set_exprs {
            validate_set_target(k)?;
            assignments.push(format!("{k} = {}", expr.to_surql()));
        }
        if assignments.is_empty() {
            return Err(SurqlError::Query {
                reason: "Update data required for UPDATE query".into(),
            });
        }
        let set_str = assignments.join(", ");

        let mut parts = vec![format!("UPDATE {table} SET {set_str}")];
        parts.extend(render_where(&self.conditions));
        parts.extend(self.return_clause());
        Ok(parts.join(" "))
    }

    fn build_delete(&self) -> Result<String> {
        let table = self.require_table(Operation::Delete)?;
        let mut parts = vec![format!("DELETE {table}")];
        parts.extend(render_where(&self.conditions));
        parts.extend(self.return_clause());
        Ok(parts.join(" "))
    }

    fn build_upsert(&self) -> Result<String> {
        let table = self.require_table(Operation::Upsert)?;
        let data = self.update_data.as_ref().ok_or_else(|| SurqlError::Query {
            reason: "Data required for UPSERT query".into(),
        })?;

        let data_str = render_content(Some(data), &self.update_set_exprs)?;
        let mut parts = vec![format!("UPSERT {table} CONTENT {data_str}")];
        parts.extend(render_where(&self.conditions));
        parts.extend(self.return_clause());
        Ok(parts.join(" "))
    }

    fn build_relate(&self) -> Result<String> {
        let table = self
            .table_name
            .as_deref()
            .ok_or_else(|| SurqlError::Query {
                reason: "Table name required for RELATE query".into(),
            })?;
        validate_identifier(table, "edge table name")?;
        let (Some(from), Some(to)) = (self.relate_from.as_deref(), self.relate_to.as_deref())
        else {
            return Err(SurqlError::Query {
                reason: "From and to records required for RELATE query".into(),
            });
        };
        let from = render_target(from)?;
        let to = render_target(to)?;

        let mut parts = vec![format!("RELATE {from}->{table}->{to}")];
        if self.relate_data.is_some() || !self.update_set_exprs.is_empty() {
            let content = render_content(self.relate_data.as_ref(), &self.update_set_exprs)?;
            parts.push(format!("CONTENT {content}"));
        }
        parts.extend(self.return_clause());
        Ok(parts.join(" "))
    }
}
