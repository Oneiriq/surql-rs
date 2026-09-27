//! Whether two definitions describe the same stored thing: the comparisons
//! the diff runs on fields, permissions, indexes, and events, with what the
//! engine fills in or spells differently folded away.

use std::collections::BTreeMap;

use super::normalize::{expr_eq, normalize_expression, type_eq};
use crate::schema::fields::{FieldDefinition, FieldType};
use crate::schema::index_vector::{
    DISKANN_DEFAULT_ALPHA, DISKANN_DEFAULT_DEGREE, DISKANN_DEFAULT_L_BUILD,
};
use crate::schema::table::{
    DiskAnnDistanceType, EventDefinition, HnswDistanceType, IndexDefinition, IndexType,
    MTreeDistanceType, MTreeVectorType,
};

/// Whether two field definitions render the same stored field.
///
/// Every clause [`FieldDefinition::to_surql`] renders is compared: the type
/// with its `option<...>` wrapper and record target, `FLEXIBLE`, `READONLY`,
/// `REFERENCE`, the `ASSERT` / `DEFAULT` / `VALUE` / `COMPUTED` expressions
/// (through [`normalize_expression`]), and the permissions. What the engine
/// is free to spell differently compares equal: a record target on a type
/// that never renders one, and field permission rules that say `FULL`, the
/// field default the engine writes out for every action a rule set leaves
/// unnamed.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::fields_equal;
/// use surql::schema::{FieldDefinition, FieldType};
///
/// let text = FieldDefinition::new("title", FieldType::String);
/// let code = text.clone().with_assertion("$value  !=  NONE");
/// assert!(fields_equal(&code, &text.clone().with_assertion("$value != NONE")));
/// assert!(!fields_equal(&text, &text.clone().with_nullable(true)));
/// ```
#[must_use]
pub fn fields_equal(a: &FieldDefinition, b: &FieldDefinition) -> bool {
    a.name == b.name
        && a.field_type == b.field_type
        && type_eq(a.custom_type.as_deref(), b.custom_type.as_deref())
        && a.nullable == b.nullable
        && rendered_target(a) == rendered_target(b)
        && a.readonly == b.readonly
        && a.flexible == b.flexible
        && a.reference == b.reference
        && expr_eq(a.assertion.as_deref(), b.assertion.as_deref())
        && expr_eq(a.default.as_deref(), b.default.as_deref())
        && expr_eq(a.value.as_deref(), b.value.as_deref())
        && expr_eq(a.computed.as_deref(), b.computed.as_deref())
        && field_permissions_equal(a.permissions.as_ref(), b.permissions.as_ref())
}

/// The record target a field actually renders: only `record<...>` and
/// `array<record<...>>` carry one.
fn rendered_target(field: &FieldDefinition) -> Option<&str> {
    match field.field_type {
        FieldType::Record | FieldType::Array => field.target_table.as_deref(),
        _ => None,
    }
}

/// Whether two table (or edge) permission maps grant the same thing.
///
/// As [`permissions_equal`], with an action whose rule is `NONE` counting as
/// left out: `NONE` is a table's default, and the engine's echo, as the
/// schema parser reads it, drops it.
///
/// ## Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use surql::migration::diff::table_permissions_equal;
///
/// let code = BTreeMap::from([
///     ("select".to_owned(), "true".to_owned()),
///     ("delete".to_owned(), "NONE".to_owned()),
/// ]);
/// let echo = BTreeMap::from([("select".to_owned(), "true".to_owned())]);
/// assert!(table_permissions_equal(Some(&code), Some(&echo)));
/// ```
#[must_use]
pub fn table_permissions_equal(
    a: Option<&BTreeMap<String, String>>,
    b: Option<&BTreeMap<String, String>>,
) -> bool {
    permissions_equal(
        without_default(a, "NONE").as_ref(),
        without_default(b, "NONE").as_ref(),
    )
}

/// Whether two field permission maps grant the same thing.
///
/// As [`permissions_equal`], with an action whose rule is `FULL` counting as
/// left out: `FULL` is a field's default, which the engine spells out for
/// every action a rule set leaves unnamed.
///
/// ## Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use surql::migration::diff::field_permissions_equal;
///
/// let code = BTreeMap::from([("update".to_owned(), "$auth.admin".to_owned())]);
/// let echo = BTreeMap::from([
///     ("select, create".to_owned(), "FULL".to_owned()),
///     ("update".to_owned(), "$auth.admin".to_owned()),
/// ]);
/// assert!(field_permissions_equal(Some(&code), Some(&echo)));
/// ```
#[must_use]
pub fn field_permissions_equal(
    a: Option<&BTreeMap<String, String>>,
    b: Option<&BTreeMap<String, String>>,
) -> bool {
    permissions_equal(
        without_default(a, "FULL").as_ref(),
        without_default(b, "FULL").as_ref(),
    )
}

/// `perms` split into one entry per action, without the actions whose rule
/// is the `default` posture.
fn without_default(
    perms: Option<&BTreeMap<String, String>>,
    default: &str,
) -> Option<BTreeMap<String, String>> {
    let kept: BTreeMap<String, String> = expand_actions(perms?)
        .into_iter()
        .filter(|(_, rule)| !rule.trim().eq_ignore_ascii_case(default))
        .collect();
    (!kept.is_empty()).then_some(kept)
}

/// Expand comma-grouped action keys (`"select, create"`) into one
/// entry per action, the shape the engine echoes. Code that groups
/// actions and a database that splits them must compare equal.
fn expand_actions(map: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (key, value) in map {
        for action in key.split(',') {
            out.insert(action.trim().to_ascii_lowercase(), value.clone());
        }
    }
    out
}

/// A permission rule as a comparable string: the fixed postures in one case,
/// anything else through [`normalize_expression`].
fn normalize_rule(rule: &str) -> String {
    let trimmed = rule.trim();
    if trimmed.eq_ignore_ascii_case("NONE") || trimmed.eq_ignore_ascii_case("FULL") {
        trimmed.to_ascii_uppercase()
    } else {
        normalize_expression(trimmed)
    }
}

/// Whether two per-action permission maps grant the same thing.
///
/// Comma-grouped keys (`"select, create"`) are split into one entry per
/// action, the shape the engine echoes, and each rule compares through
/// [`normalize_expression`] (the `NONE` / `FULL` postures in any case). An
/// absent map equals an empty one. No action is treated as a default here;
/// [`table_permissions_equal`] and [`field_permissions_equal`] add that.
///
/// ## Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use surql::migration::diff::permissions_equal;
///
/// let grouped = BTreeMap::from([("select, create".to_owned(), "$auth.id = id".to_owned())]);
/// let split = BTreeMap::from([
///     ("select".to_owned(), "$auth.id  =  id".to_owned()),
///     ("create".to_owned(), "$auth.id = id".to_owned()),
/// ]);
/// assert!(permissions_equal(Some(&grouped), Some(&split)));
/// assert!(permissions_equal(None, Some(&BTreeMap::new())));
/// ```
#[must_use]
pub fn permissions_equal(
    a: Option<&BTreeMap<String, String>>,
    b: Option<&BTreeMap<String, String>>,
) -> bool {
    let expanded_a = a.map(expand_actions);
    let expanded_b = b.map(expand_actions);
    let (a, b) = (expanded_a.as_ref(), expanded_b.as_ref());
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            if x.len() != y.len() {
                return false;
            }
            for (k, vx) in x {
                let Some(vy) = y.get(k) else { return false };
                if normalize_rule(vx) != normalize_rule(vy) {
                    return false;
                }
            }
            true
        }
        (Some(m), None) | (None, Some(m)) => m.is_empty(),
    }
}

/// HNSW construction defaults the engine fills in when a statement leaves
/// them out, and always echoes (`EFC 150 M 12`); from its `DEFINE INDEX`
/// parser.
const HNSW_DEFAULT_EFC: u32 = 150;
/// See [`HNSW_DEFAULT_EFC`].
const HNSW_DEFAULT_M: u32 = 12;

/// Whether two index definitions describe the same stored index.
///
/// The name, kind, and columns always count, and so does every member the
/// kind renders. A member the engine fills with a default when the statement
/// leaves it out compares as that default (an HNSW index's
/// `DIST EUCLIDEAN TYPE F32 EFC 150 M 12`, the DISKANN tail, a full-text
/// index's `ascii` analyzer), and a member the kind does not render is
/// ignored. So are `CONCURRENTLY`, a build directive the engine does not
/// store, and a full-text index's `BM25` flag: the engine scores every
/// full-text index with BM25 whether or not the statement asked for it.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::indexes_equal;
/// use surql::schema::{index, unique_index};
///
/// assert!(indexes_equal(&index("i", ["a"]), &index("i", ["a"]).with_concurrently(true)));
/// assert!(!indexes_equal(&index("i", ["a"]), &unique_index("i", ["a"])));
/// assert!(!indexes_equal(&index("i", ["a"]), &index("i", ["a", "b"])));
/// ```
#[must_use]
pub fn indexes_equal(a: &IndexDefinition, b: &IndexDefinition) -> bool {
    comparable_index(a) == comparable_index(b)
}

/// `idx` reduced to what the engine stores for its kind, with the engine's
/// defaults filled in.
fn comparable_index(idx: &IndexDefinition) -> IndexDefinition {
    let columns = idx
        .columns
        .iter()
        .map(|column| normalize_expression(column));
    let base = IndexDefinition::new(idx.name.clone(), columns).with_type(idx.index_type);
    #[allow(deprecated)]
    match idx.index_type {
        IndexType::Unique | IndexType::Standard => base,
        IndexType::Count => IndexDefinition {
            condition: idx
                .condition
                .as_deref()
                .map(normalize_expression)
                .filter(|condition| !condition.is_empty()),
            ..base
        },
        IndexType::Search => IndexDefinition {
            analyzer: idx
                .analyzer
                .clone()
                .filter(|analyzer| !analyzer.eq_ignore_ascii_case("ascii")),
            highlights: idx.highlights,
            ..base
        },
        IndexType::Mtree => IndexDefinition {
            dimension: idx.dimension,
            distance: Some(idx.distance.unwrap_or(MTreeDistanceType::Euclidean)),
            vector_type: Some(idx.vector_type.unwrap_or(MTreeVectorType::F64)),
            ..base
        },
        IndexType::Hnsw => IndexDefinition {
            dimension: idx.dimension,
            hnsw_distance: Some(idx.hnsw_distance.unwrap_or(HnswDistanceType::Euclidean)),
            vector_type: Some(idx.vector_type.unwrap_or(MTreeVectorType::F32)),
            efc: Some(idx.efc.unwrap_or(HNSW_DEFAULT_EFC)),
            m: Some(idx.m.unwrap_or(HNSW_DEFAULT_M)),
            ..base
        },
        IndexType::Diskann => IndexDefinition {
            dimension: idx.dimension,
            diskann_distance: Some(
                idx.diskann_distance
                    .unwrap_or(DiskAnnDistanceType::Euclidean),
            ),
            vector_type: Some(idx.vector_type.unwrap_or(MTreeVectorType::F32)),
            degree: Some(idx.degree.unwrap_or(DISKANN_DEFAULT_DEGREE)),
            l_build: Some(idx.l_build.unwrap_or(DISKANN_DEFAULT_L_BUILD)),
            alpha: Some(
                idx.alpha
                    .clone()
                    .unwrap_or_else(|| DISKANN_DEFAULT_ALPHA.to_owned()),
            ),
            hashed_vector: idx.hashed_vector,
            ..base
        },
    }
}

/// Whether two event definitions fire on the same condition and run the
/// same action.
///
/// Both halves compare through [`normalize_expression`]. The action also
/// drops the block braces and trailing `;` the engine may add or remove: it
/// stores a block action as `{ ... }` and a bare one wrapped in parentheses.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::events_equal;
/// use surql::schema::event;
///
/// let code = event("audit", "$event = 'CREATE'", "CREATE log SET n = 1");
/// let echo = event("audit", "$event = \"CREATE\"", "(CREATE log SET n = 1)");
/// assert!(events_equal(&code, &echo));
/// assert!(!events_equal(&code, &event("audit", "true", "CREATE log SET n = 1")));
/// ```
#[must_use]
pub fn events_equal(a: &EventDefinition, b: &EventDefinition) -> bool {
    a.name == b.name
        && normalize_expression(&a.condition) == normalize_expression(&b.condition)
        && event_body(&a.action) == event_body(&b.action)
}

/// An event action without its block braces or trailing `;`, normalised.
fn event_body(action: &str) -> String {
    let trimmed = action.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .unwrap_or(trimmed);
    normalize_expression(inner.trim().trim_end_matches(';'))
}
