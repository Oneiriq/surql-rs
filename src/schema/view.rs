//! Pre-computed view tables (`DEFINE TABLE ... AS SELECT ...`).
//!
//! A view table is maintained by the engine: every write to one of its source
//! tables updates the view in place, so an aggregate that would otherwise be
//! recomputed per query is read straight off a table.
//!
//! The `AS SELECT` body is a restricted `SELECT` — projections, sources, an
//! optional `WHERE`, and an optional `GROUP` — so it is modelled here rather
//! than reusing [`Query`](crate::query::builder::Query), whose clauses
//! (`ORDER BY`, `LIMIT`, traversals, hints) a view cannot carry.
//!
//! Every fragment is passed to the engine verbatim; SurrealDB is the
//! authority on what a projection or predicate may contain.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

/// The `GROUP` clause of a view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewGroup {
    /// `GROUP ALL` — collapse every source row into one.
    All,
    /// `GROUP BY <fields>`.
    By(Vec<String>),
}

impl ViewGroup {
    /// Group by the given fields.
    pub fn by<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::By(fields.into_iter().map(Into::into).collect())
    }

    /// Render the ` GROUP ...` clause.
    pub fn to_clause(&self) -> String {
        match self {
            Self::All => " GROUP ALL".to_string(),
            Self::By(fields) => format!(" GROUP BY {}", fields.join(", ")),
        }
    }
}

/// The `AS SELECT` body of a pre-computed view table.
///
/// ## Examples
///
/// ```
/// use surql::schema::{table_schema, ViewDefinition, ViewGroup, TableMode};
///
/// let view = ViewDefinition::new(["count() AS total", "author"], ["comment"])
///     .with_condition("archived = false")
///     .with_group(ViewGroup::by(["author"]));
/// let t = table_schema("comment_stats")
///     .with_mode(TableMode::Schemaless)
///     .with_view(view);
/// assert_eq!(
///     t.to_surql(),
///     "DEFINE TABLE comment_stats TYPE NORMAL SCHEMALESS \
///      AS SELECT count() AS total, author FROM comment \
///      WHERE archived = false GROUP BY author;",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// Projected expressions, rendered comma-joined after `SELECT`.
    pub projections: Vec<String>,
    /// Source tables, rendered comma-joined after `FROM`.
    pub tables: Vec<String>,
    /// Optional `WHERE` predicate.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub condition: Option<String>,
    /// Optional `GROUP` clause.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub group: Option<ViewGroup>,
}

impl ViewDefinition {
    /// Construct a view over `tables` projecting `projections`.
    pub fn new<P, PS, T, TS>(projections: P, tables: T) -> Self
    where
        P: IntoIterator<Item = PS>,
        PS: Into<String>,
        T: IntoIterator<Item = TS>,
        TS: Into<String>,
    {
        Self {
            projections: projections.into_iter().map(Into::into).collect(),
            tables: tables.into_iter().map(Into::into).collect(),
            condition: None,
            group: None,
        }
    }

    /// Filter the source rows.
    pub fn with_condition(mut self, condition: impl Into<String>) -> Self {
        self.condition = Some(condition.into());
        self
    }

    /// Set the `GROUP` clause.
    pub fn with_group(mut self, group: ViewGroup) -> Self {
        self.group = Some(group);
        self
    }

    /// Validate the view body.
    ///
    /// Returns [`SurqlError::Validation`] when there is nothing to project or
    /// no source table — the engine rejects both.
    pub fn validate(&self) -> Result<()> {
        if self.projections.iter().all(|p| p.trim().is_empty()) {
            return Err(SurqlError::Validation {
                reason: "View must project at least one expression".into(),
            });
        }
        if self.tables.iter().all(|t| t.trim().is_empty()) {
            return Err(SurqlError::Validation {
                reason: "View must select from at least one table".into(),
            });
        }
        Ok(())
    }

    /// Render the ` AS SELECT ... FROM ... [WHERE ...] [GROUP ...]` clause,
    /// ready to append to a `DEFINE TABLE` statement.
    pub fn to_clause(&self) -> String {
        let mut sql = format!(
            " AS SELECT {} FROM {}",
            self.projections.join(", "),
            self.tables.join(", "),
        );
        if let Some(condition) = &self.condition {
            write!(sql, " WHERE {condition}").expect("writing to String cannot fail");
        }
        if let Some(group) = &self.group {
            sql.push_str(&group.to_clause());
        }
        sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{table_schema, TableMode};

    #[test]
    fn minimal_clause() {
        let view = ViewDefinition::new(["id"], ["comment"]);
        assert_eq!(view.to_clause(), " AS SELECT id FROM comment");
    }

    #[test]
    fn multiple_projections_and_sources() {
        let view = ViewDefinition::new(["count() AS total", "author"], ["comment", "reply"]);
        assert_eq!(
            view.to_clause(),
            " AS SELECT count() AS total, author FROM comment, reply"
        );
    }

    #[test]
    fn condition_and_group_by() {
        let view = ViewDefinition::new(["count() AS total"], ["comment"])
            .with_condition("n > 2")
            .with_group(ViewGroup::by(["author", "day"]));
        assert_eq!(
            view.to_clause(),
            " AS SELECT count() AS total FROM comment WHERE n > 2 GROUP BY author, day"
        );
    }

    #[test]
    fn group_all() {
        let view = ViewDefinition::new(["count() AS c"], ["comment"]).with_group(ViewGroup::All);
        assert_eq!(
            view.to_clause(),
            " AS SELECT count() AS c FROM comment GROUP ALL"
        );
    }

    #[test]
    fn table_renders_type_normal_before_the_mode() {
        let t = table_schema("stats")
            .with_mode(TableMode::Schemaless)
            .with_view(
                ViewDefinition::new(["count() AS c"], ["comment"]).with_group(ViewGroup::All),
            );
        assert_eq!(
            t.to_surql(),
            "DEFINE TABLE stats TYPE NORMAL SCHEMALESS AS SELECT count() AS c FROM comment GROUP ALL;"
        );
    }

    #[test]
    fn view_composes_with_permissions_and_guards() {
        let t = table_schema("stats")
            .with_mode(TableMode::Schemaless)
            .with_view(ViewDefinition::new(["id"], ["comment"]))
            .with_permissions([("select", "true")]);
        assert_eq!(
            t.to_surql_overwrite(),
            "DEFINE TABLE OVERWRITE stats TYPE NORMAL SCHEMALESS AS SELECT id FROM comment \
             PERMISSIONS FOR select WHERE true;"
        );
        assert!(t
            .to_surql_with_options(true)
            .starts_with("DEFINE TABLE IF NOT EXISTS stats TYPE NORMAL SCHEMALESS AS SELECT"));
    }

    #[test]
    fn a_table_without_a_view_renders_unchanged() {
        assert_eq!(
            table_schema("plain").to_surql(),
            "DEFINE TABLE plain SCHEMAFULL;"
        );
    }

    #[test]
    fn validate_rejects_an_empty_projection_or_source() {
        assert!(ViewDefinition::new(Vec::<String>::new(), ["t"])
            .validate()
            .is_err());
        assert!(ViewDefinition::new(["id"], Vec::<String>::new())
            .validate()
            .is_err());
        assert!(ViewDefinition::new([" "], ["t"]).validate().is_err());
        assert!(ViewDefinition::new(["id"], ["t"]).validate().is_ok());
    }

    #[test]
    fn table_validate_propagates_view_errors() {
        let t = table_schema("stats").with_view(ViewDefinition::new(["id"], Vec::<String>::new()));
        assert!(t.validate().is_err());
    }

    #[test]
    fn table_validate_rejects_fields_on_a_view() {
        let t = table_schema("stats")
            .with_view(ViewDefinition::new(["id"], ["comment"]))
            .with_fields([crate::schema::FieldDefinition::new(
                "id",
                crate::schema::FieldType::String,
            )]);
        let err = t.validate().expect_err("fields on a view are rejected");
        assert!(err.to_string().contains("view"), "{err}");
    }

    #[test]
    fn serde_roundtrip() {
        let view = ViewDefinition::new(["count() AS c"], ["comment"])
            .with_condition("n > 2")
            .with_group(ViewGroup::by(["author"]));
        let json = serde_json::to_string(&view).unwrap();
        let back: ViewDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(view, back);

        let legacy: crate::schema::TableDefinition =
            serde_json::from_str(r#"{"name":"a","mode":"SCHEMAFULL"}"#).unwrap();
        assert!(legacy.view.is_none());
    }
}
