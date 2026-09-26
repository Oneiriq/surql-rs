//! Immutable SurrealQL query builder.
//!
//! Port of `surql/query/builder.py`. Mirrors the Pydantic `frozen=True`
//! behaviour of the Python `Query` model: every chainable method returns
//! a new [`Query`] (via `Clone` + field updates), so prior states remain
//! valid and reusable.
//!
//! ## Escaping
//!
//! Names (tables, fields, edges, aliases) are validated as identifiers or
//! field paths, record-id targets are parsed and re-rendered through
//! [`RecordID`](crate::types::RecordID), and every data value is rendered
//! as a literal. A `serde_json::Value` is always data, whatever its shape;
//! raw SurrealQL enters only through the explicitly raw inputs: projections
//! passed to [`Query::select`], `WHERE` fragments passed as strings,
//! [`Query::join`], [`Query::traverse`], and [`Expression`]s.
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

use serde_json::Value;

use crate::error::{Result, SurqlError};
use crate::types::operators::{quote_object_key, quote_value_public, Operator, OperatorExpr};

use super::expressions::Expression;
use super::helpers::{DataMap, ReturnFormat, VectorDistanceType};
use super::hints::{check_hints, render_hints, QueryHint};
use super::references::reverse_reference_projection;
use super::validate::{quote_field_path, render_target, validate_field_path, validate_finite};

pub(crate) use super::validate::{validate_identifier, validate_set_target};

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

/// A single filter entry: either a raw SurrealQL fragment or an [`Operator`].
///
/// [`WhereCondition`] takes `self` by value, so it cannot be used behind a
/// trait object and a heterogeneous slice of conditions is not expressible
/// through it alone. `Condition` is the owned carrier that makes one slice
/// able to mix both forms, matching the `str | Operator` union the graph
/// helpers accept in the sibling ports.
///
/// ```
/// use surql::query::Condition;
/// use surql::types::operators::eq;
///
/// let filters: Vec<Condition> = vec![
///     eq("tenant_id", "acme").into(),
///     "age > 18".into(),
/// ];
/// assert_eq!(filters.len(), 2);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    /// A raw SurrealQL predicate fragment (e.g. `"age > 18"`).
    Raw(String),
    /// A builder-constructed operator (e.g. `eq("tenant_id", "acme")`).
    Op(Operator),
}

impl From<&str> for Condition {
    fn from(value: &str) -> Self {
        Self::Raw(value.to_owned())
    }
}

impl From<String> for Condition {
    fn from(value: String) -> Self {
        Self::Raw(value)
    }
}

impl From<&String> for Condition {
    fn from(value: &String) -> Self {
        Self::Raw(value.clone())
    }
}

impl From<Operator> for Condition {
    fn from(value: Operator) -> Self {
        Self::Op(value)
    }
}

impl From<&Operator> for Condition {
    fn from(value: &Operator) -> Self {
        Self::Op(value.clone())
    }
}

impl WhereCondition for Condition {
    fn to_condition(self) -> String {
        match self {
            Self::Raw(fragment) => fragment,
            Self::Op(op) => op.to_surql(),
        }
    }
}

impl WhereCondition for &Condition {
    fn to_condition(self) -> String {
        match self {
            Condition::Raw(fragment) => fragment.clone(),
            Condition::Op(op) => op.to_surql(),
        }
    }
}

/// Immutable query builder.
///
/// Most methods return a new [`Query`] instance; the receiver is taken by
/// value (`self`) to encourage chained usage. Existing bindings remain
/// valid because the struct derives [`Clone`].
///
/// The fields are public, and [`Query::to_surql`] re-checks every name and
/// target it renders, so a query assembled by hand is held to the same rules
/// as one built through the methods.
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
    /// Assignments added with [`Query::set`] / [`Query::set_expr`]. In an
    /// `UPDATE` they render as `SET` assignments after `update_data`, and
    /// their right-hand side may reference the row's current fields (e.g.
    /// `n = n + 1`). In a `CREATE` / `UPSERT` / `RELATE` they join the
    /// `CONTENT` object, replacing a same-named key of the data map.
    pub update_set_exprs: Vec<(String, Expression)>,
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
    /// Search effort for the index-backed operator. When set, the
    /// operator renders `<|k,ef|>` and the metric is left to the index.
    pub vector_ef: Option<i64>,
    /// Full-text search field (the `@@` / `@n@` matches operator).
    pub fulltext_field: Option<String>,
    /// Full-text match reference number, tying the predicate to
    /// `search::score(n)` (the `@n@` form).
    pub fulltext_reference: Option<u8>,
    /// Full-text query text (inlined as a quoted, escaped literal).
    pub fulltext_query: Option<String>,
    /// Hints rendered as a `/* ... */` comment prefix (see
    /// [`hints`](super::hints): the server ignores them).
    pub hints: Vec<QueryHint>,
}

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

fn render_vector(vector: &[f64]) -> Result<String> {
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
    /// Construct an empty query. All builder methods start from here.
    pub fn new() -> Self {
        Self::default()
    }

    // -----------------------------------------------------------------------
    // SELECT / FROM
    // -----------------------------------------------------------------------

    /// Start a `SELECT` query. Pass `None` for `SELECT *`.
    ///
    /// The projection list is raw SurrealQL (it may hold expressions such
    /// as `count()`), so it must not carry untrusted text.
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
    /// (`"user:alice"`). A table must be an identifier; a record id is
    /// parsed and re-rendered so its key is escaped (`"user:a-b"` renders
    /// `user:⟨a-b⟩`).
    pub fn from_table(self, table: impl Into<String>) -> Result<Self> {
        let table = render_target(&table.into())?;
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

    /// Append an `ORDER BY` entry. `field` must be a field path and
    /// `direction` must be `ASC` or `DESC`.
    pub fn order_by(self, field: impl Into<String>, direction: impl Into<String>) -> Result<Self> {
        let field = field.into();
        validate_field_path(&field, "order field")?;
        let direction = direction.into().to_ascii_uppercase();
        if direction != "ASC" && direction != "DESC" {
            return Err(SurqlError::Validation {
                reason: format!("Invalid direction: {direction}. Must be ASC or DESC"),
            });
        }
        let mut order_fields = self.order_fields;
        order_fields.push(OrderField { field, direction });
        Ok(Self {
            order_fields,
            ..self
        })
    }

    /// Append one or more `GROUP BY` fields. Each must be a field path;
    /// [`Query::to_surql`] reports one that is not.
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
    ///
    /// Add expression-valued fields (`time::now()`, a record reference) with
    /// [`Query::set_expr`]; the data map's values are always literals.
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

    /// Build an `UPDATE` query. `target` is a table or a record id (see
    /// [`Query::from_table`]).
    pub fn update(self, target: impl Into<String>, data: DataMap) -> Result<Self> {
        let target = render_target(&target.into())?;
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

    /// Begin an `UPDATE <target> SET ...` whose assignments are supplied via
    /// [`set`](Self::set) (literal) and [`set_expr`](Self::set_expr)
    /// (expression-valued, may reference current fields), instead of a literal
    /// data map. Combine with [`where_`](Self::where_) for a guarded, atomic
    /// read-modify-write — `UPDATE t SET n = n + 1 WHERE ...` in one statement.
    pub fn update_set(self, target: impl Into<String>) -> Result<Self> {
        let target = render_target(&target.into())?;
        Ok(Self {
            operation: Some(Operation::Update),
            table_name: Some(target),
            ..self
        })
    }

    /// Add a literal-valued assignment (`set("updated_at", now)` ⇒
    /// `updated_at = '<now>'`): a `SET` assignment in an `UPDATE`, a
    /// `CONTENT` field in a `CREATE` / `UPSERT` / `RELATE`.
    pub fn set(mut self, field: impl Into<String>, value: impl Into<Value>) -> Result<Self> {
        let field = field.into();
        validate_set_target(&field)?;
        self.update_set_exprs
            .push((field, Expression::value(value)));
        Ok(self)
    }

    /// Add an expression-valued assignment, where the new value may be a
    /// function call, a record reference, or (in an `UPDATE`) reference the
    /// row's current fields (`set_expr("n", field("n") + 1)` ⇒ `n = n + 1`).
    ///
    /// In an `UPDATE` it renders as a `SET` assignment; in a `CREATE` /
    /// `UPSERT` / `RELATE` it joins the `CONTENT` object, which is how a raw
    /// value such as `time::now()` gets into inserted data:
    ///
    /// ```
    /// use surql::query::builder::Query;
    /// use surql::query::expressions::time_now;
    ///
    /// let q = Query::new()
    ///     .insert("event", Default::default()).unwrap()
    ///     .set_expr("created_at", time_now()).unwrap();
    /// assert_eq!(q.to_surql().unwrap(), "CREATE event CONTENT {created_at: time::now()}");
    /// ```
    pub fn set_expr(mut self, field: impl Into<String>, expr: Expression) -> Result<Self> {
        let field = field.into();
        validate_set_target(&field)?;
        self.update_set_exprs.push((field, expr));
        Ok(self)
    }

    /// Build an `UPSERT` query. `target` is a table or a record id (see
    /// [`Query::from_table`]).
    pub fn upsert(self, target: impl Into<String>, data: DataMap) -> Result<Self> {
        let target = render_target(&target.into())?;
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

    /// Build a `DELETE` query. `target` is a table or a record id (see
    /// [`Query::from_table`]).
    pub fn delete(self, target: impl Into<String>) -> Result<Self> {
        let target = render_target(&target.into())?;
        Ok(Self {
            operation: Some(Operation::Delete),
            table_name: Some(target),
            ..self
        })
    }

    // -----------------------------------------------------------------------
    // RELATE / traversal / join
    // -----------------------------------------------------------------------

    /// Build a `RELATE` query. `from_record` / `to_record` are record ids
    /// (see [`Query::from_table`]).
    pub fn relate(
        self,
        edge_table: impl Into<String>,
        from_record: impl Into<String>,
        to_record: impl Into<String>,
        data: Option<DataMap>,
    ) -> Result<Self> {
        let edge_table = edge_table.into();
        validate_identifier(&edge_table, "edge table name")?;
        let from_record = render_target(&from_record.into())?;
        let to_record = render_target(&to_record.into())?;
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
    ///
    /// The path is raw SurrealQL and must not carry untrusted text.
    pub fn traverse(self, path: impl Into<String>) -> Self {
        Self {
            graph_traversal: Some(path.into()),
            ..self
        }
    }

    /// Append a reverse-reference lookup (`<~<table>`) to the projection.
    ///
    /// This is the read half of
    /// [`DEFINE FIELD ... REFERENCE`](crate::schema::reference): it walks
    /// incoming links back to the records that point at each selected row.
    /// `fields` narrows the returned shape via the `.{ a, b }` destructuring
    /// form; pass `None` for whole records. The table, fields, and alias are
    /// quoted as identifiers.
    ///
    /// ## Examples
    ///
    /// ```
    /// use surql::query::builder::Query;
    ///
    /// let q = Query::new()
    ///     .select(None)
    ///     .from_table("comic_book").unwrap()
    ///     .reverse_traverse("person", None, "owners");
    /// assert_eq!(
    ///     q.to_surql().unwrap(),
    ///     "SELECT *, <~person AS owners FROM comic_book",
    /// );
    ///
    /// let projected = Query::new()
    ///     .select(None)
    ///     .from_table("comic_book").unwrap()
    ///     .reverse_traverse("person", Some(&["id", "name"]), "owners");
    /// assert_eq!(
    ///     projected.to_surql().unwrap(),
    ///     "SELECT *, <~person.{ id, name } AS owners FROM comic_book",
    /// );
    /// ```
    pub fn reverse_traverse(
        self,
        table: impl AsRef<str>,
        fields: Option<&[&str]>,
        alias: impl AsRef<str>,
    ) -> Self {
        let expr = reverse_reference_projection(table.as_ref(), fields, alias.as_ref());
        let mut projection = self.fields;
        projection.push(expr);
        Self {
            fields: projection,
            ..self
        }
    }

    /// Append a raw `JOIN` clause.
    ///
    /// The clause is raw SurrealQL and must not carry untrusted text.
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

    /// Configure MTREE vector search. `field` must be a field path, and the
    /// vector and threshold must be finite.
    pub fn vector_search(
        self,
        field: impl Into<String>,
        vector: Vec<f64>,
        k: i64,
        distance: VectorDistanceType,
        threshold: Option<f64>,
    ) -> Result<Self> {
        let field = field.into();
        validate_field_path(&field, "vector search field")?;
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
        validate_finite(&vector, "vector values")?;
        validate_finite(threshold.as_slice(), "vector threshold")?;
        Ok(Self {
            vector_field: Some(field),
            vector_value: vector,
            vector_k: Some(k),
            vector_distance: Some(distance),
            vector_threshold: threshold,
            vector_ef: None,
            ..self
        })
    }

    /// Configure index-backed KNN search, rendering `<|k,ef|>`.
    ///
    /// The second operand decides the plan. An integer is the search
    /// effort and makes the engine use the field's vector index; a
    /// metric name (what [`Query::vector_search`] renders) makes it
    /// compare every row, which is correct and slow. Use this whenever
    /// the field carries an HNSW or DiskANN index, and let the index's
    /// own metric apply.
    ///
    /// `ef` bounds the candidate list the search keeps. Higher values
    /// cost more and recall more.
    ///
    /// SurrealDB 3.x has no distance threshold on this operator; floats
    /// in the second position are refused outright. Project the distance
    /// with `vector::distance::knn()` and filter on it in the same
    /// `WHERE` when a relevance floor is needed.
    pub fn vector_search_indexed(
        self,
        field: impl Into<String>,
        vector: Vec<f64>,
        k: i64,
        ef: i64,
    ) -> Result<Self> {
        let field = field.into();
        validate_field_path(&field, "vector search field")?;
        if k < 1 {
            return Err(SurqlError::Validation {
                reason: format!("k must be at least 1, got {k}"),
            });
        }
        if ef < 1 {
            return Err(SurqlError::Validation {
                reason: format!("ef must be at least 1, got {ef}"),
            });
        }
        if vector.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Vector cannot be empty".into(),
            });
        }
        validate_finite(&vector, "vector values")?;
        Ok(Self {
            vector_field: Some(field),
            vector_value: vector,
            vector_k: Some(k),
            vector_distance: None,
            vector_threshold: None,
            vector_ef: Some(ef),
            ..self
        })
    }

    /// Append `vector::similarity::<metric>(field, [..]) AS alias` to the
    /// projected field list. `field` and `alias` must be field paths and the
    /// vector must be finite.
    pub fn similarity_score(
        self,
        field: &str,
        vector: &[f64],
        metric: VectorDistanceType,
        alias: impl Into<String>,
    ) -> Result<Self> {
        validate_field_path(field, "similarity field")?;
        let alias = alias.into();
        validate_field_path(&alias, "similarity alias")?;
        let vector_str = render_vector(vector)?;
        let expr = format!(
            "vector::similarity::{}({field}, {vector_str}) AS {alias}",
            metric.as_func_suffix()
        );
        let mut fields = self.fields;
        fields.push(expr);
        Ok(Self { fields, ..self })
    }

    // -----------------------------------------------------------------------
    // Full-text search
    // -----------------------------------------------------------------------

    /// Configure a full-text `SEARCH` predicate rendered as
    /// `<field> @<reference>@ <query>` in the `WHERE` clause.
    ///
    /// The `reference` integer ties the match to a
    /// [`search_score`](Self::search_score) (or `search::highlight`) call, so a
    /// row's BM25 relevance can be projected and ordered on. Requires a BM25
    /// `SEARCH` index on `field` (see [`bm25_index`](crate::schema::bm25_index)).
    /// `field` must be a field path; the query text is inlined as a quoted,
    /// escaped literal.
    ///
    /// ## Examples
    ///
    /// ```
    /// use surql::query::builder::Query;
    ///
    /// let q = Query::new()
    ///     .select(None)
    ///     .search_score(1, "score")
    ///     .from_table("memory").unwrap()
    ///     .fulltext_search("content", 1, "insider buying").unwrap()
    ///     .order_by("score", "DESC").unwrap()
    ///     .limit(10).unwrap();
    ///
    /// assert_eq!(
    ///     q.to_surql().unwrap(),
    ///     "SELECT *, search::score(1) AS score FROM memory \
    ///      WHERE content @1@ 'insider buying' ORDER BY score DESC LIMIT 10",
    /// );
    /// ```
    pub fn fulltext_search(
        self,
        field: impl Into<String>,
        reference: u8,
        query: impl Into<String>,
    ) -> Result<Self> {
        let field = field.into();
        if field.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Full-text search field cannot be empty".into(),
            });
        }
        validate_field_path(&field, "full-text search field")?;
        let query = query.into();
        if query.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Full-text search query cannot be empty".into(),
            });
        }
        Ok(Self {
            fulltext_field: Some(field),
            fulltext_reference: Some(reference),
            fulltext_query: Some(query),
            ..self
        })
    }

    /// Append `search::score(<reference>) AS <alias>` to the projected fields —
    /// the BM25 relevance for the match registered at `reference` by
    /// [`fulltext_search`](Self::fulltext_search). Order by `alias` to rank.
    /// The alias is quoted as an identifier path.
    pub fn search_score(self, reference: u8, alias: impl Into<String>) -> Self {
        let alias = quote_field_path(&alias.into());
        let expr = format!("search::score({reference}) AS {alias}");
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

// ---------------------------------------------------------------------------
// Client-feature execution shim (sub-feature 4: builder.execute)
// ---------------------------------------------------------------------------

#[cfg(any(feature = "client", feature = "client-rustls", feature = "client-wasm"))]
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
    fn update_set_expr_renders_atomic_guarded_update() {
        use crate::query::expressions::field;
        use crate::types::operators::{and_, eq, is_none};

        // An atomic, tombstone-guarded read-modify-write in one statement: the
        // increment references the row's own value, and the guard skips a row
        // that was forgotten (deleted_at set) between read and write.
        let q = Query::new()
            .update_set("memory:abc")
            .unwrap()
            .set_expr("reinforcement", field("reinforcement") + 1)
            .unwrap()
            .set("updated_at", "2026-06-06T00:00:00+00:00")
            .unwrap()
            .where_(and_(eq("tenant_id", "t"), is_none("deleted_at")))
            .return_after();
        assert_eq!(
            q.to_surql().unwrap(),
            "UPDATE memory:abc SET reinforcement = (reinforcement + 1), \
             updated_at = '2026-06-06T00:00:00+00:00' \
             WHERE ((tenant_id = 't') AND (deleted_at IS NONE)) RETURN AFTER"
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
    fn indexed_vector_search_renders_the_effort_operand() {
        let q = Query::new()
            .select(None)
            .from_table("documents")
            .unwrap()
            .vector_search_indexed("embedding", vec![0.1, 0.2, 0.3], 10, 64)
            .unwrap();
        // An INTEGER second operand is what selects the index; the
        // metric form compares every row instead.
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT * FROM documents WHERE embedding <|10,64|> [0.1, 0.2, 0.3]"
        );

        for (k, ef) in [(0, 64), (10, 0)] {
            let err = Query::new()
                .select(None)
                .from_table("documents")
                .unwrap()
                .vector_search_indexed("embedding", vec![0.1], k, ef);
            assert!(
                matches!(err, Err(SurqlError::Validation { .. })),
                "{k} {ef}"
            );
        }
        let err = Query::new()
            .select(None)
            .from_table("documents")
            .unwrap()
            .vector_search_indexed("embedding", vec![], 10, 64);
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
            )
            .unwrap();
        let sql = q.to_surql().unwrap();
        assert!(sql.contains("vector::similarity::cosine(embedding, [0.1, 0.2]) AS score"));
    }

    #[test]
    fn similarity_score_validates_names_and_values() {
        let base = Query::new().select(None).from_table("chunk").unwrap();
        let cosine = VectorDistanceType::Cosine;
        assert!(base
            .clone()
            .similarity_score("e) AS x FROM user; --", &[0.1], cosine, "s")
            .is_err());
        assert!(base
            .clone()
            .similarity_score("e", &[0.1], cosine, "s FROM user; --")
            .is_err());
        assert!(base
            .clone()
            .similarity_score("e", &[f64::NAN], cosine, "s")
            .is_err());
    }

    #[test]
    fn non_finite_vectors_and_thresholds_are_refused() {
        let base = Query::new().select(None).from_table("doc").unwrap();
        let cosine = VectorDistanceType::Cosine;
        assert!(base
            .clone()
            .vector_search("e", vec![f64::INFINITY], 1, cosine, None)
            .is_err());
        assert!(base
            .clone()
            .vector_search("e", vec![0.1], 1, cosine, Some(f64::NAN))
            .is_err());
        assert!(base
            .clone()
            .vector_search_indexed("e", vec![f64::NEG_INFINITY], 1, 8)
            .is_err());
        // Hand-set values are re-checked when rendering.
        let mut q = base.vector_search("e", vec![0.1], 1, cosine, None).unwrap();
        q.vector_value = vec![f64::NAN];
        assert!(q.to_surql().is_err());
    }

    #[test]
    fn set_expr_adds_raw_values_to_content() {
        use crate::query::expressions::time_now;
        use crate::types::record_ref;

        let q = Query::new()
            .insert("post", data(&[("title", Value::from("hi"))]))
            .unwrap()
            .set_expr("created_at", time_now())
            .unwrap()
            .set_expr("author", record_ref("user", "alice").into())
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "CREATE post CONTENT {title: 'hi', created_at: time::now(), \
             author: type::record('user', 'alice')}"
        );
        // An assignment replaces the same-named data key.
        let q = Query::new()
            .upsert("post:1", data(&[("n", Value::from(1))]))
            .unwrap()
            .set_expr("n", time_now())
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "UPSERT post:1 CONTENT {n: time::now()}"
        );
        // RELATE takes them as edge content, with or without a data map.
        let q = Query::new()
            .relate("likes", "user:a", "post:1", None)
            .unwrap()
            .set_expr("at", time_now())
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "RELATE user:a->likes->post:1 CONTENT {at: time::now()}"
        );
        // A nested path cannot be a CONTENT key.
        let q = Query::new()
            .insert("post", DataMap::new())
            .unwrap()
            .set("meta.n", 1)
            .unwrap();
        assert!(q.to_surql().is_err());
    }

    #[test]
    fn hand_set_names_are_rechecked_when_rendering() {
        let mut q = Query::new().select(None).from_table("user").unwrap();
        q.table_name = Some("user; DELETE user".into());
        assert!(q.to_surql().is_err());

        let mut q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .order_by("name", "ASC")
            .unwrap();
        q.order_fields[0].field = "name; DELETE user".into();
        assert!(q.to_surql().is_err());
        q.order_fields[0].field = "name".into();
        q.order_fields[0].direction = "ASC; DELETE user".into();
        assert!(q.to_surql().is_err());

        let mut q = Query::new()
            .relate("likes", "user:a", "post:1", None)
            .unwrap();
        q.relate_to = Some("post:1; DELETE post".into());
        assert_eq!(
            q.to_surql().unwrap(),
            "RELATE user:a->likes->post:⟨1; DELETE post⟩"
        );
        q.table_name = Some("likes; DELETE".into());
        assert!(q.to_surql().is_err());
    }

    #[test]
    fn index_hints_are_checked_when_rendering() {
        let q = Query::new()
            .select(None)
            .from_table("user")
            .unwrap()
            .hint(QueryHint::Index(IndexHint::new(
                "user",
                "x */ DELETE user; /*",
            )));
        assert!(matches!(q.to_surql(), Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn record_targets_are_normalised() {
        let q = Query::new().delete("user:a-b").unwrap();
        assert_eq!(q.to_surql().unwrap(), "DELETE user:⟨a-b⟩");
        assert!(Query::new().delete("user:[1, 2]").is_err());
        assert!(Query::new().from_table("user; DELETE user").is_err());
    }

    // -----------------------------------------------------------------------
    // Full-text search
    // -----------------------------------------------------------------------

    #[test]
    fn fulltext_search_renders_match_operator() {
        let q = Query::new()
            .select(None)
            .from_table("memory")
            .unwrap()
            .fulltext_search("content", 1, "insider buying")
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT * FROM memory WHERE content @1@ 'insider buying'"
        );
    }

    #[test]
    fn fulltext_search_with_score_and_order() {
        let q = Query::new()
            .select(None)
            .search_score(1, "score")
            .from_table("memory")
            .unwrap()
            .fulltext_search("content", 1, "form 4")
            .unwrap()
            .order_by("score", "DESC")
            .unwrap()
            .limit(5)
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT *, search::score(1) AS score FROM memory \
             WHERE content @1@ 'form 4' ORDER BY score DESC LIMIT 5"
        );
    }

    #[test]
    fn fulltext_search_escapes_quotes() {
        let q = Query::new()
            .select(None)
            .from_table("memory")
            .unwrap()
            .fulltext_search("content", 0, "o'brien")
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "SELECT * FROM memory WHERE content @0@ 'o\\'brien'"
        );
    }

    #[test]
    fn fulltext_search_rejects_empty_field() {
        let err = Query::new()
            .select(None)
            .from_table("memory")
            .unwrap()
            .fulltext_search("", 1, "x");
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn fulltext_search_rejects_empty_query() {
        let err = Query::new()
            .select(None)
            .from_table("memory")
            .unwrap()
            .fulltext_search("content", 1, "");
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn fulltext_and_vector_both_render_in_where() {
        let q = Query::new()
            .select(None)
            .from_table("memory")
            .unwrap()
            .vector_search(
                "embedding",
                vec![0.1, 0.2],
                5,
                VectorDistanceType::Cosine,
                None,
            )
            .unwrap()
            .fulltext_search("content", 1, "term")
            .unwrap();
        let sql = q.to_surql().unwrap();
        assert!(sql.contains("embedding <|5,COSINE|> [0.1, 0.2]"));
        assert!(sql.contains("content @1@ 'term'"));
        assert!(sql.contains(" AND "));
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
    fn expression_shaped_insert_data_stays_data() {
        let q = Query::new()
            .insert(
                "post",
                data(&[(
                    "body",
                    serde_json::json!({"expression": "1}; DELETE user; --"}),
                )]),
            )
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "CREATE post CONTENT {body: { expression: '1}; DELETE user; --' }}"
        );
    }

    #[test]
    fn data_keys_set_directly_are_quoted() {
        let q = Query {
            operation: Some(Operation::Insert),
            table_name: Some("user".into()),
            insert_data: Some(data(&[("x: 1}; DELETE user; --", Value::from(1))])),
            ..Query::default()
        };
        assert_eq!(
            q.to_surql().unwrap(),
            "CREATE user CONTENT {'x: 1}; DELETE user; --': 1}"
        );
        let q = Query {
            operation: Some(Operation::Update),
            table_name: Some("user".into()),
            update_data: Some(data(&[("x = 1; DELETE user; --", Value::from(1))])),
            ..Query::default()
        };
        assert!(matches!(q.to_surql(), Err(SurqlError::Validation { .. })));
    }

    #[test]
    fn record_targets_cannot_carry_a_second_statement() {
        let q = Query::new()
            .delete("user:x; REMOVE TABLE user; --")
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "DELETE user:⟨x; REMOVE TABLE user; --⟩"
        );
        let q = Query::new()
            .update_set("user:a; REMOVE TABLE user")
            .unwrap()
            .set("n", 1)
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "UPDATE user:⟨a; REMOVE TABLE user⟩ SET n = 1"
        );
        let q = Query::new()
            .relate(
                "likes",
                "user:a->likes->post:b; REMOVE TABLE post",
                "post:c",
                None,
            )
            .unwrap();
        assert_eq!(
            q.to_surql().unwrap(),
            "RELATE user:⟨a->likes->post:b; REMOVE TABLE post⟩->likes->post:c"
        );
    }

    #[test]
    fn identifier_sinks_reject_injection() {
        let base = Query::new().select(None).from_table("user").unwrap();
        assert!(base.clone().order_by("name; DELETE user", "ASC").is_err());
        assert!(base
            .clone()
            .group_by(["status; DELETE user"])
            .to_surql()
            .is_err());
        assert!(base
            .clone()
            .fulltext_search("content; DELETE user", 1, "x")
            .is_err());
        assert!(base
            .clone()
            .vector_search(
                "e; DELETE user",
                vec![0.1],
                1,
                VectorDistanceType::Cosine,
                None
            )
            .is_err());
        let sql = base
            .clone()
            .search_score(1, "s FROM user; DELETE user; --")
            .to_surql()
            .unwrap();
        assert!(sql.contains("AS `s FROM user; DELETE user; --`"), "{sql}");
    }

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
    fn set_accepts_dotted_paths_into_nested_objects() {
        let q = Query::new()
            .update_set("file:abc")
            .unwrap()
            .set(
                "metadata.processing",
                serde_json::json!({"verdict": "clean"}),
            )
            .unwrap()
            .to_surql()
            .unwrap();
        assert!(
            q.contains("SET metadata.processing = "),
            "nested assignment must render: {q}",
        );
        // Hostile segments still refuse.
        assert!(Query::new()
            .update_set("file:abc")
            .unwrap()
            .set("metadata.bad segment", 1)
            .is_err());
        assert!(Query::new()
            .update_set("file:abc")
            .unwrap()
            .set("metadata..double", 1)
            .is_err());
        assert!(Query::new()
            .update_set("file:abc")
            .unwrap()
            .set("", 1)
            .is_err());
    }

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
