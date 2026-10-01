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
use crate::types::operators::{Operator, OperatorExpr};

use super::expressions::Expression;
use super::helpers::{DataMap, ReturnFormat, VectorDistanceType};
use super::hints::QueryHint;
use super::references::reverse_reference_projection;
use super::validate::{quote_field_path, render_target, validate_field_path, validate_finite};

pub(crate) use super::validate::{validate_identifier, validate_set_target};

mod render;

use render::render_vector;

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
    /// Lock the selected records until the transaction ends
    /// (`SELECT ... FOR UPDATE`, SurrealDB 3.3+).
    pub for_update: bool,
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

    /// Lock the selected records until the surrounding transaction ends
    /// (`FOR UPDATE`, SurrealDB 3.3+), so a read-then-write in one
    /// transaction cannot race another writer. Only a `SELECT` renders it,
    /// and only on a record id target: the engine refuses a table, and so
    /// does [`Query::to_surql`].
    ///
    /// ## Examples
    ///
    /// ```
    /// use surql::query::builder::Query;
    ///
    /// let q = Query::new()
    ///     .select(None)
    ///     .from_table("account:alice")
    ///     .unwrap()
    ///     .for_update();
    /// assert_eq!(q.to_surql().unwrap(), "SELECT * FROM account:alice FOR UPDATE");
    ///
    /// let table = Query::new().select(None).from_table("account").unwrap().for_update();
    /// assert!(table.to_surql().is_err());
    /// ```
    pub fn for_update(self) -> Self {
        Self {
            for_update: true,
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
    /// the field carries an HNSW or `DiskANN` index, and let the index's
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
mod tests;
