//! Immutable SurrealQL query builder.
//!
//! Port of `surql/query/builder.py`. Mirrors the Pydantic `frozen=True`
//! behaviour of the Python `Query` model: every chainable method returns
//! a new [`Query`] (via `Clone` + field updates), so prior states remain
//! valid and reusable.
//!
//! ## Examples
//!
//! ```
//! use surql::query::builder::Query;
//!
//! let q = Query::new()
//!     .select(Some(vec!["name".into(), "email".into()]))
//!     .from_table("user").unwrap()
//!     .where_str("age > 18")
//!     .order_by("name", "ASC").unwrap()
//!     .limit(10).unwrap();
//!
//! assert_eq!(
//!     q.to_surql().unwrap(),
//!     "SELECT name, email FROM user WHERE (age > 18) ORDER BY name ASC LIMIT 10",
//! );
//! ```

use std::sync::OnceLock;

use regex::Regex;

use crate::error::{Result, SurqlError};
use crate::types::operators::{quote_value_public, Operator, OperatorExpr};

use super::helpers::{DataMap, ReturnFormat, VectorDistanceType};
use super::hints::{render_hints, QueryHint};

/// SurrealQL operation kind held by [`Query`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    /// `SELECT ... FROM ...`
    Select,
    /// `CREATE ... CONTENT {...}`
    Insert,
    /// `UPDATE ... SET ...`
    Update,
    /// `DELETE ...`
    Delete,
    /// `UPSERT ... CONTENT {...}`
    Upsert,
    /// `RELATE from->edge->to [CONTENT {...}]`
    Relate,
}

impl Operation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Upsert => "UPSERT",
            Self::Relate => "RELATE",
        }
    }
}

/// A single `ORDER BY` entry (`field ASC | DESC`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderField {
    /// Field name.
    pub field: String,
    /// Sort direction (always `ASC` or `DESC` post-validation).
    pub direction: String,
}

/// Condition trait implemented by both [`String`] (raw) and [`Operator`].
///
/// Allows [`Query::where_`] to accept either form without overloading:
///
/// ```
/// use surql::query::builder::Query;
/// use surql::types::operators::{eq, OperatorExpr};
///
/// let q = Query::new().select(None).from_table("user").unwrap();
/// let by_str = q.clone().where_str("age > 18");
/// let by_op = q.where_(eq("status", "active"));
/// ```
pub trait WhereCondition {
    /// Render this condition as a SurrealQL fragment.
    fn to_condition(self) -> String;
}

impl WhereCondition for String {
    fn to_condition(self) -> String {
        self
    }
}

impl WhereCondition for &str {
    fn to_condition(self) -> String {
        self.to_owned()
    }
}

impl WhereCondition for Operator {
    fn to_condition(self) -> String {
        self.to_surql()
    }
}

impl WhereCondition for &Operator {
    fn to_condition(self) -> String {
        self.to_surql()
    }
}

/// Immutable query builder.
///
/// Most methods return a new [`Query`] instance; the receiver is taken by
/// value (`self`) to encourage chained usage. Existing bindings remain
/// valid because the struct derives [`Clone`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Query {
    /// Which SurrealQL verb this query will emit (once set).
    pub operation: Option<Operation>,
    /// Target table or record id.
    pub table_name: Option<String>,
    /// Projected fields (`SELECT` field list).
    pub fields: Vec<String>,
    /// Accumulated `WHERE` fragments (wrapped in `(...)` and joined with `AND`).
    pub conditions: Vec<String>,
    /// `ORDER BY` entries.
    pub order_fields: Vec<OrderField>,
    /// `GROUP BY` fields.
    pub group_fields: Vec<String>,
    /// When `true`, emits `GROUP ALL`.
    pub group_all_flag: bool,
    /// `LIMIT` value.
    pub limit_value: Option<i64>,
    /// `START` (offset) value.
    pub offset_value: Option<i64>,
    /// Data for `INSERT` / `CREATE CONTENT`.
    pub insert_data: Option<DataMap>,
    /// Data for `UPDATE SET` / `UPSERT CONTENT`.
    pub update_data: Option<DataMap>,
    /// Source record id for `RELATE`.
    pub relate_from: Option<String>,
    /// Target record id for `RELATE`.
    pub relate_to: Option<String>,
    /// Optional edge data for `RELATE`.
    pub relate_data: Option<DataMap>,
    /// Raw `JOIN` clauses appended verbatim.
    pub join_clauses: Vec<String>,
    /// Optional graph traversal suffix appended after `FROM <table>`.
    pub graph_traversal: Option<String>,
    /// `RETURN` format.
    pub return_format: Option<ReturnFormat>,
    /// Vector-search field name.
    pub vector_field: Option<String>,
    /// Vector-search query vector.
    pub vector_value: Vec<f64>,
    /// `K` (nearest-neighbours) for the MTREE operator.
    pub vector_k: Option<i64>,
    /// Distance metric for the MTREE operator.
    pub vector_distance: Option<VectorDistanceType>,
    /// Optional threshold for the MTREE operator.
    pub vector_threshold: Option<f64>,
    /// Optimization hints appended as a `/* ... */` prefix.
    pub hints: Vec<QueryHint>,
}

fn identifier_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").expect("valid regex"))
}

pub(crate) fn validate_identifier(name: &str, context: &str) -> Result<()> {
    if name.is_empty() {
        let capitalized = capitalize(context);
        return Err(SurqlError::Validation {
            reason: format!("{capitalized} cannot be empty"),
        });
    }
    if !identifier_pattern().is_match(name) {
        return Err(SurqlError::Validation {
            reason: format!(
                "Invalid {context}: {name:?}. Must contain only alphanumeric \
                 characters and underscores, and cannot start with a digit"
            ),
        });
    }
    Ok(())
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

pub(crate) fn table_part(target: &str) -> &str {
    target.split_once(':').map_or(target, |(t, _)| t)
}

fn render_data_object(data: &DataMap) -> String {
    let parts: Vec<String> = data
        .iter()
        .map(|(k, v)| format!("{k}: {}", quote_value_public(v)))
        .collect();
    format!("{{{}}}", parts.join(", "))
}

fn render_vector(vector: &[f64]) -> String {
    let inner = vector
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

impl Query {
    /// Construct an empty query. All builder methods start from here.
    pub fn new() -> Self {
        Self::default()
    }

    // -----------------------------------------------------------------------
    // SELECT / FROM
    // -----------------------------------------------------------------------

    /// Start a `SELECT` query. Pass `None` for `SELECT *`.
    pub fn select(self, fields: Option<Vec<String>>) -> Self {
        let fields = fields.unwrap_or_else(|| vec!["*".to_string()]);
        Self {
            operation: Some(Operation::Select),
            fields,
            ..self
        }
    }

    /// Start a `SELECT` query whose projection is a list of typed
    /// [`Expression`](crate::query::expressions::Expression) fragments.
    ///
    /// Each expression is rendered via `expression.to_surql()` and joined
    /// with `, ` so callers can mix aggregate factories (`count()`,
    /// `math_mean(...)`) with plain field references without stringifying
    /// them by hand.
    ///
    /// ## Examples
    ///
    /// ```
    /// use surql::query::builder::Query;
    /// use surql::query::expressions::{as_, count_all, math_mean};
    ///
    /// let q = Query::new()
    ///     .select_expr(vec![
    ///         as_(&count_all(), "total"),
    ///         as_(&math_mean("strength"), "mean_strength"),
    ///     ])
    ///     .from_table("memory_entry").unwrap()
    ///     .group_all();
    ///
    /// assert_eq!(
    ///     q.to_surql().unwrap(),
    ///     "SELECT count() AS total, math::mean(strength) AS mean_strength \
    ///      FROM memory_entry GROUP ALL",
    /// );
    /// ```
    pub fn select_expr(
        self,
        fields: impl IntoIterator<Item = crate::query::expressions::Expression>,
    ) -> Self {
        let rendered: Vec<String> = fields.into_iter().map(|e| e.to_surql()).collect();
        self.select(Some(rendered))
    }

    /// Set the target table.
    ///
    /// Accepts either a bare table (`"user"`) or a record id
    /// (`"user:alice"`). In the latter case only the table part is
    /// validated against the identifier regex.
    pub fn from_table(self, table: impl Into<String>) -> Result<Self> {
        let table = table.into();
        let part = table_part(&table);
        validate_identifier(part, "table name")?;
        Ok(Self {
            table_name: Some(table),
            ..self
        })
    }

    // -----------------------------------------------------------------------
    // WHERE
    // -----------------------------------------------------------------------

    /// Append a condition to `WHERE`.
    ///
    /// Accepts either a raw string (`"age > 18"`) or an [`Operator`].
    pub fn where_<C: WhereCondition>(self, condition: C) -> Self {
        let mut conditions = self.conditions;
        conditions.push(condition.to_condition());
        Self { conditions, ..self }
    }

    /// String-specialised convenience (helps type inference when the caller
    /// passes a `&str`).
    pub fn where_str(self, condition: impl Into<String>) -> Self {
        self.where_::<String>(condition.into())
    }

    /// [`Operator`]-specialised convenience.
    pub fn where_op(self, op: Operator) -> Self {
        self.where_(op)
    }

    // -----------------------------------------------------------------------
    // ORDER BY / GROUP BY / LIMIT / OFFSET
    // -----------------------------------------------------------------------

    /// Append an `ORDER BY` entry. `direction` must be `ASC` or `DESC`.
    pub fn order_by(self, field: impl Into<String>, direction: impl Into<String>) -> Result<Self> {
        let direction = direction.into().to_ascii_uppercase();
        if direction != "ASC" && direction != "DESC" {
            return Err(SurqlError::Validation {
                reason: format!("Invalid direction: {direction}. Must be ASC or DESC"),
            });
        }
        let mut order_fields = self.order_fields;
        order_fields.push(OrderField {
            field: field.into(),
            direction,
        });
        Ok(Self {
            order_fields,
            ..self
        })
    }

    /// Append one or more `GROUP BY` fields.
    pub fn group_by<I, S>(self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut group_fields = self.group_fields;
        group_fields.extend(fields.into_iter().map(Into::into));
        Self {
            group_fields,
            ..self
        }
    }

    /// Emit `GROUP ALL` (aggregate across all rows).
    pub fn group_all(self) -> Self {
        Self {
            group_all_flag: true,
            ..self
        }
    }

    /// Set `LIMIT`. `n` must be non-negative.
    pub fn limit(self, n: i64) -> Result<Self> {
        if n < 0 {
            return Err(SurqlError::Validation {
                reason: format!("Limit must be non-negative, got {n}"),
            });
        }
        Ok(Self {
            limit_value: Some(n),
            ..self
        })
    }

    /// Set `START` (offset). `n` must be non-negative.
    pub fn offset(self, n: i64) -> Result<Self> {
        if n < 0 {
            return Err(SurqlError::Validation {
                reason: format!("Offset must be non-negative, got {n}"),
            });
        }
        Ok(Self {
            offset_value: Some(n),
            ..self
        })
    }

    // -----------------------------------------------------------------------
    // INSERT / UPDATE / UPSERT / DELETE
    // -----------------------------------------------------------------------

    /// Build an `INSERT` query (emits `CREATE <table> CONTENT {...}`).
    pub fn insert(self, table: impl Into<String>, data: DataMap) -> Result<Self> {
        let table = table.into();
        validate_identifier(&table, "table name")?;
        for key in data.keys() {
            validate_identifier(key, "field name")?;
        }
        Ok(Self {
            operation: Some(Operation::Insert),
            table_name: Some(table),
            insert_data: Some(data),
            ..self
        })
    }

    /// Build an `UPDATE` query.
    pub fn update(self, target: impl Into<String>, data: DataMap) -> Result<Self> {
        let target = target.into();
        validate_identifier(table_part(&target), "table name")?;
        for key in data.keys() {
            validate_identifier(key, "field name")?;
        }
        Ok(Self {
            operation: Some(Operation::Update),
            table_name: Some(target),
            update_data: Some(data),
            ..self
        })
    }

    /// Build an `UPSERT` query.
    pub fn upsert(self, target: impl Into<String>, data: DataMap) -> Result<Self> {
        let target = target.into();
        validate_identifier(table_part(&target), "table name")?;
        for key in data.keys() {
            validate_identifier(key, "field name")?;
        }
        Ok(Self {
            operation: Some(Operation::Upsert),
            table_name: Some(target),
            update_data: Some(data),
            ..self
        })
    }

    /// Build a `DELETE` query.
    pub fn delete(self, target: impl Into<String>) -> Result<Self> {
        let target = target.into();
        validate_identifier(table_part(&target), "table name")?;
        Ok(Self {
            operation: Some(Operation::Delete),
            table_name: Some(target),
            ..self
        })
    }

    // -----------------------------------------------------------------------
    // RELATE / traversal / join
    // -----------------------------------------------------------------------

    /// Build a `RELATE` query.
    pub fn relate(
        self,
        edge_table: impl Into<String>,
        from_record: impl Into<String>,
        to_record: impl Into<String>,
        data: Option<DataMap>,
    ) -> Result<Self> {
        let edge_table = edge_table.into();
        let from_record = from_record.into();
        let to_record = to_record.into();

        validate_identifier(&edge_table, "edge table name")?;
        validate_identifier(table_part(&from_record), "from table name")?;
        validate_identifier(table_part(&to_record), "to table name")?;
        if let Some(d) = &data {
            for key in d.keys() {
                validate_identifier(key, "field name")?;
            }
        }

        Ok(Self {
            operation: Some(Operation::Relate),
            table_name: Some(edge_table),
            relate_from: Some(from_record),
            relate_to: Some(to_record),
            relate_data: data,
            ..self
        })
    }

    /// Append a graph traversal path (e.g. `"->likes->post"`).
    pub fn traverse(self, path: impl Into<String>) -> Self {
        Self {
            graph_traversal: Some(path.into()),
            ..self
        }
    }

    /// Append a raw `JOIN` clause.
    pub fn join(self, join_clause: impl Into<String>) -> Self {
        let mut joins = self.join_clauses;
        joins.push(join_clause.into());
        Self {
            join_clauses: joins,
            ..self
        }
    }

    // -----------------------------------------------------------------------
    // Vector search
    // -----------------------------------------------------------------------

    /// Configure MTREE vector search.
    pub fn vector_search(
        self,
        field: impl Into<String>,
        vector: Vec<f64>,
        k: i64,
        distance: VectorDistanceType,
        threshold: Option<f64>,
    ) -> Result<Self> {
        if k < 1 {
            return Err(SurqlError::Validation {
                reason: format!("k must be at least 1, got {k}"),
            });
        }
        if vector.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Vector cannot be empty".into(),
            });
        }
        Ok(Self {
            vector_field: Some(field.into()),
            vector_value: vector,
            vector_k: Some(k),
            vector_distance: Some(distance),
            vector_threshold: threshold,
            ..self
        })
    }

    /// Append `vector::similarity::<metric>(field, [..]) AS alias` to the
    /// projected field list.
    pub fn similarity_score(
        self,
        field: &str,
        vector: &[f64],
        metric: VectorDistanceType,
        alias: impl Into<String>,
    ) -> Self {
        let vector_str = render_vector(vector);
        let alias = alias.into();
        let expr = format!(
            "vector::similarity::{}({field}, {vector_str}) AS {alias}",
            metric.as_func_suffix()
        );
        let mut fields = self.fields;
        fields.push(expr);
        Self { fields, ..self }
    }

    // -----------------------------------------------------------------------
    // RETURN convenience
    // -----------------------------------------------------------------------

    /// Set the `RETURN` clause to the given format.
    pub fn return_format(self, format: ReturnFormat) -> Self {
        Self {
            return_format: Some(format),
            ..self
        }
    }

    /// `RETURN NONE`.
    pub fn return_none(self) -> Self {
        self.return_format(ReturnFormat::None)
    }
    /// `RETURN DIFF`.
    pub fn return_diff(self) -> Self {
        self.return_format(ReturnFormat::Diff)
    }
    /// `RETURN FULL`.
    pub fn return_full(self) -> Self {
        self.return_format(ReturnFormat::Full)
    }
    /// `RETURN BEFORE`.
    pub fn return_before(self) -> Self {
        self.return_format(ReturnFormat::Before)
    }
    /// `RETURN AFTER`.
    pub fn return_after(self) -> Self {
        self.return_format(ReturnFormat::After)
    }

    // -----------------------------------------------------------------------
    // Hints
    // -----------------------------------------------------------------------

    /// Append a single hint.
    pub fn hint(self, hint: QueryHint) -> Self {
        let mut hints = self.hints;
        hints.push(hint);
        Self { hints, ..self }
    }

    /// Convenience alias for [`Query::hint`] matching the Python `add_hint`.
    pub fn add_hint(self, hint: QueryHint) -> Self {
        self.hint(hint)
    }

    /// Append multiple hints.
    pub fn with_hints<I>(self, hints: I) -> Self
    where
        I: IntoIterator<Item = QueryHint>,
    {
        let mut all = self.hints;
        all.extend(hints);
        Self { hints: all, ..self }
    }

    // -----------------------------------------------------------------------
    // Rendering
    // -----------------------------------------------------------------------

    /// Render the full SurrealQL statement.
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
            let hint_str = render_hints(&self.hints);
            Ok(format!("{hint_str}\n{base}"))
        }
    }

    /// Convenience wrapper for doc-tests: render after forcing a table. Not
    /// part of the stable API.
    #[doc(hidden)]
    pub fn to_surql_or_panic_with_table(self, table: &str) -> String {
        self.from_table(table)
            .expect("valid table")
            .to_surql()
            .expect("valid select")
    }

    fn require_table(&self, op: Operation) -> Result<&str> {
        self.table_name.as_deref().ok_or_else(|| SurqlError::Query {
            reason: format!("Table name required for {} query", op.as_str()),
        })
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
        if let (Some(field), Some(k), Some(distance), false) = (
            &self.vector_field,
            self.vector_k,
            self.vector_distance,
            self.vector_value.is_empty(),
        ) {
            let vector_str = render_vector(&self.vector_value);
            let operator = match self.vector_threshold {
                Some(t) => format!("<|{k},{},{t}|>", distance.to_surql()),
                None => format!("<|{k},{}|>", distance.to_surql()),
            };
            where_parts.push(format!("{field} {operator} {vector_str}"));
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
            parts.push(format!("GROUP BY {}", self.group_fields.join(", ")));
        }

        if !self.order_fields.is_empty() {
            let rendered = self
                .order_fields
                .iter()
                .map(|o| format!("{} {}", o.field, o.direction))
                .collect::<Vec<_>>()
                .join(", ");
            parts.push(format!("ORDER BY {rendered}"));
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

        let data_str = render_data_object(data);
        let mut parts = vec![format!("CREATE {table} CONTENT {data_str}")];
        if let Some(fmt) = self.return_format {
            parts.push(format!("RETURN {}", fmt.to_surql()));
        }
        Ok(parts.join(" "))
    }

    fn build_update(&self) -> Result<String> {
        let table = self.require_table(Operation::Update)?;
        let data = self.update_data.as_ref().ok_or_else(|| SurqlError::Query {
            reason: "Update data required for UPDATE query".into(),
        })?;

        let set_str = data
            .iter()
            .map(|(k, v)| format!("{k} = {}", quote_value_public(v)))
            .collect::<Vec<_>>()
            .join(", ");

        let mut parts = vec![format!("UPDATE {table} SET {set_str}")];
        if !self.conditions.is_empty() {
            let joined = self
                .conditions
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ");
            parts.push(format!("WHERE {joined}"));
        }
        if let Some(fmt) = self.return_format {
            parts.push(format!("RETURN {}", fmt.to_surql()));
        }
        Ok(parts.join(" "))
    }

    fn build_delete(&self) -> Result<String> {
        let table = self.require_table(Operation::Delete)?;
        let mut parts = vec![format!("DELETE {table}")];
        if !self.conditions.is_empty() {
            let joined = self
                .conditions
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ");
            parts.push(format!("WHERE {joined}"));
        }
        if let Some(fmt) = self.return_format {
            parts.push(format!("RETURN {}", fmt.to_surql()));
        }
        Ok(parts.join(" "))
    }

    fn build_upsert(&self) -> Result<String> {
        let table = self.require_table(Operation::Upsert)?;
        let data = self.update_data.as_ref().ok_or_else(|| SurqlError::Query {
            reason: "Data required for UPSERT query".into(),
        })?;

        let data_str = render_data_object(data);
        let mut parts = vec![format!("UPSERT {table} CONTENT {data_str}")];
        if !self.conditions.is_empty() {
            let joined = self
                .conditions
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ");
            parts.push(format!("WHERE {joined}"));
        }
        if let Some(fmt) = self.return_format {
            parts.push(format!("RETURN {}", fmt.to_surql()));
        }
        Ok(parts.join(" "))
    }

    fn build_relate(&self) -> Result<String> {
        let table = self.require_table(Operation::Relate)?;
        let from = self
            .relate_from
            .as_deref()
            .ok_or_else(|| SurqlError::Query {
                reason: "From and to records required for RELATE query".into(),
            })?;
        let to = self.relate_to.as_deref().ok_or_else(|| SurqlError::Query {
            reason: "From and to records required for RELATE query".into(),
        })?;

        let mut parts = vec![format!("RELATE {from}->{table}->{to}")];
        if let Some(data) = &self.relate_data {
            parts.push(format!("CONTENT {}", render_data_object(data)));
        }
        if let Some(fmt) = self.return_format {
            parts.push(format!("RETURN {}", fmt.to_surql()));
        }
        Ok(parts.join(" "))
    }
}

// ---------------------------------------------------------------------------
// Client-feature execution shim (sub-feature 4: builder.execute)
// ---------------------------------------------------------------------------

#[cfg(feature = "client")]
impl Query {
    /// Render this query to SurrealQL and execute it against `client`.
    ///
    /// Thin async wrapper over
    /// [`execute_query`](crate::query::executor::execute_query) so callers
    /// can write `.execute(&client).await` directly on the builder. Returns
    /// the raw `serde_json::Value` produced by the driver - pass through
    /// [`crate::query::results::extract_many`] /
    /// [`crate::query::results::extract_one`] /
    /// [`crate::query::results::extract_scalar`] to pull values out.
    ///
    /// For typed deserialisation use
    /// [`crate::query::executor::fetch_all`] /
    /// [`crate::query::executor::fetch_one`] instead.
    pub async fn execute(
        &self,
        client: &crate::connection::DatabaseClient,
    ) -> Result<serde_json::Value> {
        crate::query::executor::execute_query(client, self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::hints::{IndexHint, ParallelHint, TimeoutHint};
    use crate::types::operators::{eq, gt};
    use serde_json::Value;

    fn data(pairs: &[(&str, Value)]) -> DataMap {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    // -----------------------------------------------------------------------
    // Basic rendering
    // -----------------------------------------------------------------------

    #[test]
    fn select_star_from_table() {
        let q = Query::new().select(None).from_table("user").unwrap();
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user");
    }

    #[test]
    fn select_projection_renders_comma_separated() {
        let q = Query::new()
            .select(Some(vec!["name".into(), "email".into()]))
            .from_table("user")
            .unwrap();
        assert_eq!(q.to_surql().unwrap(), "SELECT name, email FROM user");
    }

    #[test]
    fn insert_renders_create_content() {
        let q = Query::new()
            .insert(
                "user",
                data(&[
                    ("name", Value::String("Alice".into())),
                    ("email", Value::String("alice@example.com".into())),
                ]),
            )
            .unwrap();
        // BTreeMap => alphabetical order: email, name.
        assert_eq!(
            q.to_surql().unwrap(),
            "CREATE user CONTENT {email: 'alice@example.com', name: 'Alice'}"
        );
    }

    #[test]
    fn update_renders_set_clauses() {
        let q = Query::new()
            .update(
                "user:alice",
                data(&[("status", Value::String("active".into()))]),
            )
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "UPDATE user:alice SET status = 'active'"
        );
    }

    #[test]
    fn upsert_renders_content_object() {
        let q = Query::new()
            .upsert(
                "user:alice",
                data(&[("status", Value::String("active".into()))]),
            )
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "UPSERT user:alice CONTENT {status: 'active'}"
        );
    }

    #[test]
    fn delete_renders_record_id() {
        let q = Query::new().delete("user:alice").unwrap();
        assert_eq!(q.to_surql().unwrap(), "DELETE user:alice");
    }

    #[test]
    fn delete_with_where() {
        let q = Query::new()
            .delete("user")
            .unwrap()
            .where_str("deleted_at IS NOT NULL");
        assert_eq!(
            q.to_surql().unwrap(),
            "DELETE user WHERE (deleted_at IS NOT NULL)"
        );
    }

    #[test]
    fn relate_renders_arrow_chain() {
        let q = Query::new()
            .relate("likes", "user:alice", "post:123", None)
            .unwrap();
        assert_eq!(q.to_surql().unwrap(), "RELATE user:alice->likes->post:123");
    }

    #[test]
    fn relate_with_data_renders_content() {
        let q = Query::new()
            .relate(
                "follows",
                "user:alice",
                "user:bob",
                Some(data(&[("since", Value::String("2024-01-01".into()))])),
            )
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "RELATE user:alice->follows->user:bob CONTENT {since: '2024-01-01'}"
        );
    }

    // -----------------------------------------------------------------------
    // Fluent chaining
    // -----------------------------------------------------------------------

    #[test]
    fn chaining_produces_full_select() {
        let q = Query::new()
            .select(Some(vec!["name".into(), "email".into()]))
            .from_table("user")
            .unwrap()
            .where_str("age > 18")
            .order_by("name", "ASC")
            .unwrap()
            .limit(10)
            .unwrap()
            .offset(20)
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT name, email FROM user WHERE (age > 18) ORDER BY name ASC LIMIT 10 START 20"
        );
    }

    #[test]
    fn immutability_preserved_across_chain() {
        let base = Query::new().select(None).from_table("user").unwrap();
        let extended = base.clone().where_str("age > 18");
        assert!(base.conditions.is_empty());
        assert_eq!(extended.conditions.len(), 1);
        assert_eq!(base.to_surql().unwrap(), "SELECT * FROM user");
        assert_eq!(
            extended.to_surql().unwrap(),
            "SELECT * FROM user WHERE (age > 18)"
        );
    }

    // -----------------------------------------------------------------------
    // WHERE variants
    // -----------------------------------------------------------------------

    #[test]
    fn where_accepts_string_condition() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .where_str("age > 18");
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user WHERE (age > 18)");
    }

    #[test]
    fn where_accepts_operator_condition() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .where_(gt("age", 18));
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user WHERE (age > 18)");
    }

    #[test]
    fn multiple_where_conditions_join_with_and() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .where_(gt("age", 18))
            .where_(eq("status", "active"));
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT * FROM user WHERE (age > 18) AND (status = 'active')"
        );
    }

    // -----------------------------------------------------------------------
    // ORDER / GROUP / LIMIT / OFFSET
    // -----------------------------------------------------------------------

    #[test]
    fn order_by_desc_renders() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .order_by("created_at", "DESC")
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT * FROM user ORDER BY created_at DESC"
        );
    }

    #[test]
    fn order_by_is_case_insensitive() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .order_by("name", "asc")
            .unwrap();
        assert!(q.to_surql().unwrap().contains("ORDER BY name ASC"));
    }

    #[test]
    fn order_by_rejects_invalid_direction() {
        let err = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .order_by("name", "SIDEWAYS");
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn order_by_multiple_fields() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .order_by("last_name", "ASC")
            .unwrap()
            .order_by("first_name", "ASC")
            .unwrap();
        assert!(q
            .to_surql()
            .unwrap()
            .contains("ORDER BY last_name ASC, first_name ASC"));
    }

    #[test]
    fn group_by_renders() {
        let q = Query::new()
            .select(Some(vec!["status".into(), "COUNT(*)".into()]))
            .from_table("user")
            .unwrap()
            .group_by(["status"]);
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT status, COUNT(*) FROM user GROUP BY status"
        );
    }

    #[test]
    fn group_all_renders() {
        let q = Query::new()
            .select(Some(vec!["count()".into()]))
            .from_table("user")
            .unwrap()
            .group_all();
        assert_eq!(q.to_surql().unwrap(), "SELECT count() FROM user GROUP ALL");
    }

    #[test]
    fn limit_and_offset_render_start() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .limit(10)
            .unwrap()
            .offset(5)
            .unwrap();
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user LIMIT 10 START 5");
    }

    #[test]
    fn negative_limit_rejected() {
        let err = Query::new().limit(-1);
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn negative_offset_rejected() {
        let err = Query::new().offset(-1);
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    // -----------------------------------------------------------------------
    // RETURN formats
    // -----------------------------------------------------------------------

    #[test]
    fn return_diff_on_update() {
        let q = Query::new()
            .update("user:alice", data(&[("age", Value::from(30))]))
            .unwrap()
            .return_diff();
        assert_eq!(
            q.to_surql().unwrap(),
            "UPDATE user:alice SET age = 30 RETURN DIFF"
        );
    }

    #[test]
    fn return_none_on_delete() {
        let q = Query::new().delete("user:alice").unwrap().return_none();
        assert_eq!(q.to_surql().unwrap(), "DELETE user:alice RETURN NONE");
    }

    #[test]
    fn return_full_on_insert() {
        let q = Query::new()
            .insert("user", data(&[("name", Value::String("Alice".into()))]))
            .unwrap()
            .return_full();
        assert!(q.to_surql().unwrap().ends_with("RETURN FULL"));
    }

    #[test]
    fn return_before_and_after() {
        let before = Query::new().delete("user:alice").unwrap().return_before();
        let after = Query::new().delete("user:alice").unwrap().return_after();
        assert!(before.to_surql().unwrap().contains("RETURN BEFORE"));
        assert!(after.to_surql().unwrap().contains("RETURN AFTER"));
    }

    // -----------------------------------------------------------------------
    // Vector search
    // -----------------------------------------------------------------------

    #[test]
    fn vector_search_without_threshold() {
        let q = Query::new()
            .select(None)
            .from_table("documents")
            .unwrap()
            .vector_search(
                "embedding",
                vec![0.1, 0.2, 0.3],
                10,
                VectorDistanceType::Cosine,
                None,
            )
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT * FROM documents WHERE embedding <|10,COSINE|> [0.1, 0.2, 0.3]"
        );
    }

    #[test]
    fn vector_search_with_threshold() {
        let q = Query::new()
            .select(None)
            .from_table("documents")
            .unwrap()
            .vector_search(
                "embedding",
                vec![0.1, 0.2, 0.3],
                10,
                VectorDistanceType::Cosine,
                Some(0.7),
            )
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT * FROM documents WHERE embedding <|10,COSINE,0.7|> [0.1, 0.2, 0.3]"
        );
    }

    #[test]
    fn vector_search_rejects_k_zero() {
        let err = Query::new()
            .select(None)
            .from_table("documents")
            .unwrap()
            .vector_search("embedding", vec![0.1], 0, VectorDistanceType::Cosine, None);
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn vector_search_rejects_empty_vector() {
        let err = Query::new()
            .select(None)
            .from_table("documents")
            .unwrap()
            .vector_search("embedding", vec![], 10, VectorDistanceType::Cosine, None);
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn similarity_score_adds_function_field() {
        let q = Query::new()
            .select(Some(vec!["id".into()]))
            .from_table("chunk")
            .unwrap()
            .similarity_score(
                "embedding",
                &[0.1, 0.2],
                VectorDistanceType::Cosine,
                "score",
            );
        let sql = q.to_surql().unwrap();
        assert!(sql.contains("vector::similarity::cosine(embedding, [0.1, 0.2]) AS score"));
    }

    // -----------------------------------------------------------------------
    // Hints
    // -----------------------------------------------------------------------

    #[test]
    fn hint_prepends_comment() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .hint(QueryHint::Timeout(TimeoutHint::new(30.0).unwrap()));
        let sql = q.to_surql().unwrap();
        assert!(sql.starts_with("/* TIMEOUT 30s */"));
        assert!(sql.contains("SELECT * FROM user"));
    }

    #[test]
    fn with_hints_composes_multiple() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .with_hints([
                QueryHint::Timeout(TimeoutHint::new(30.0).unwrap()),
                QueryHint::Parallel(ParallelHint::enabled()),
            ]);
        let sql = q.to_surql().unwrap();
        assert!(sql.contains("/* TIMEOUT 30s */"));
        assert!(sql.contains("/* PARALLEL ON */"));
    }

    #[test]
    fn index_hint_references_table() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .hint(QueryHint::Index(IndexHint::new("user", "email_idx")));
        assert!(q
            .to_surql()
            .unwrap()
            .contains("/* USE INDEX user.email_idx */"));
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    #[test]
    fn invalid_table_name_rejected() {
        let err = Query::new().from_table("1user");
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn invalid_field_name_in_insert_rejected() {
        let err = Query::new().insert("user", data(&[("bad-field", Value::from(1))]));
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn invalid_edge_table_rejected() {
        let err = Query::new().relate("bad-edge", "user:a", "user:b", None);
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn empty_table_rejected() {
        let err = Query::new().from_table("");
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn to_surql_without_operation_errors() {
        let err = Query::new().to_surql();
        assert!(matches!(err, Err(SurqlError::Query { .. })));
    }

    #[test]
    fn select_without_table_errors() {
        let err = Query::new().select(None).to_surql();
        assert!(matches!(err, Err(SurqlError::Query { .. })));
    }

    // -----------------------------------------------------------------------
    // Traversal / join
    // -----------------------------------------------------------------------

    #[test]
    fn traverse_appends_path_to_from() {
        let q = Query::new()
            .select(None)
            .from_table("user:alice")
            .unwrap()
            .traverse("->likes->post");
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT * FROM user:alice->likes->post"
        );
    }

    #[test]
    fn join_clause_appended() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .join("JOIN post ON user.id = post.author");
        assert!(q
            .to_surql()
            .unwrap()
            .contains("JOIN post ON user.id = post.author"));
    }

    // -----------------------------------------------------------------------
    // Sub-feature 4: select_expr accepts typed Expressions
    // -----------------------------------------------------------------------

    #[test]
    fn select_expr_renders_projection() {
        use crate::query::expressions::{as_, count_all, math_mean};

        let q = Query::new()
            .select_expr(vec![
                as_(&count_all(), "total"),
                as_(&math_mean("strength"), "mean"),
            ])
            .from_table("memory_entry")
            .unwrap()
            .group_all();

        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT count() AS total, math::mean(strength) AS mean FROM memory_entry GROUP ALL",
        );
    }

    #[test]
    fn select_expr_empty_falls_back_to_empty_list() {
        // Empty iterator yields no fields, so the default "*" (populated by
        // the non-expr `select(None)` helper) is NOT applied here; ensure
        // we still render a valid statement with just FROM.
        let q = Query::new()
            .select_expr(Vec::<crate::query::expressions::Expression>::new())
            .from_table("user")
            .unwrap();
        // Empty fields -> "*" by build_select's fallback.
        assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user");
    }
}
