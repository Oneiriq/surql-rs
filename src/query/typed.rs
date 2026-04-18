//! Typed CRUD helpers that round-trip through `serde`.
//!
//! Port of `surql/query/typed.py`. Each helper accepts (and/or returns)
//! user-defined types implementing [`Serialize`] / [`DeserializeOwned`] and
//! composes the lower-level JSON helpers in [`super::crud`] /
//! [`super::executor`].
//!
//! All functions are `#[cfg(feature = "client")]`.

use std::collections::BTreeMap;

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::query::builder::Query;
use crate::query::executor::extract_rows;
use crate::types::record_id::RecordID;

/// Create a typed record in `table`.
///
/// Serializes the payload to JSON, binds it as `$data`, and dispatches
/// `CREATE <table> CONTENT $data` through the raw query channel. The
/// first returned row is deserialized back into `T`.
pub async fn create_typed<T>(client: &DatabaseClient, table: &str, data: &T) -> Result<T>
where
    T: Serialize + DeserializeOwned,
{
    let mut vars = BTreeMap::new();
    vars.insert("data".to_owned(), to_value(data)?);
    let surql = format!("CREATE {table} CONTENT $data");
    let raw = client.query_with_vars(&surql, vars).await?;
    extract_rows::<T>(&raw)?
        .into_iter()
        .next()
        .ok_or_else(|| SurqlError::Query {
            reason: format!("CREATE on {table} returned no record"),
        })
}

/// Fetch a typed record by [`RecordID`].
pub async fn get_typed<T, Tag>(
    client: &DatabaseClient,
    record_id: &RecordID<Tag>,
) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    let target = record_id.to_string();
    let surql = format!("SELECT * FROM {target}");
    let raw = client.query(&surql).await?;
    Ok(extract_rows::<T>(&raw)?.into_iter().next())
}

/// Execute a rendered [`Query`] and deserialize every row into `T`.
pub async fn query_typed<T: DeserializeOwned>(
    client: &DatabaseClient,
    query: &Query,
) -> Result<Vec<T>> {
    let surql = query.to_surql()?;
    let raw = client.query(&surql).await?;
    extract_rows::<T>(&raw)
}

/// Update (replace) a typed record identified by [`RecordID`].
pub async fn update_typed<T, Tag>(
    client: &DatabaseClient,
    record_id: &RecordID<Tag>,
    data: &T,
) -> Result<T>
where
    T: Serialize + DeserializeOwned,
{
    let target = record_id.to_string();
    let mut vars = BTreeMap::new();
    vars.insert("data".to_owned(), to_value(data)?);
    let surql = format!("UPDATE {target} CONTENT $data");
    let raw = client.query_with_vars(&surql, vars).await?;
    extract_rows::<T>(&raw)?
        .into_iter()
        .next()
        .ok_or_else(|| SurqlError::Query {
            reason: format!("UPDATE on {target} returned no record"),
        })
}

/// Upsert (create-or-replace) a typed record identified by [`RecordID`].
///
/// Uses `UPSERT <id> CONTENT $data` so the record is inserted if missing
/// or wholly replaced otherwise - matching the Python semantics.
pub async fn upsert_typed<T, Tag>(
    client: &DatabaseClient,
    record_id: &RecordID<Tag>,
    data: &T,
) -> Result<T>
where
    T: Serialize + DeserializeOwned,
{
    let target = record_id.to_string();
    let mut vars = BTreeMap::new();
    vars.insert("data".to_owned(), to_value(data)?);
    let surql = format!("UPSERT {target} CONTENT $data");
    let raw = client.query_with_vars(&surql, vars).await?;
    extract_rows::<T>(&raw)?
        .into_iter()
        .next()
        .ok_or_else(|| SurqlError::Query {
            reason: format!("UPSERT on {target} returned no record"),
        })
}

fn to_value<T: Serialize>(value: &T) -> Result<Value> {
    serde_json::to_value(value).map_err(|e| SurqlError::Serialization {
        reason: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct User {
        name: String,
        age: u32,
    }

    #[test]
    fn to_value_round_trips() {
        let u = User {
            name: "Alice".into(),
            age: 30,
        };
        let v = to_value(&u).unwrap();
        let back: User = serde_json::from_value(v).unwrap();
        assert_eq!(back, u);
    }

    #[test]
    fn to_value_preserves_field_types() {
        let u = User {
            name: "Bob".into(),
            age: 42,
        };
        let v = to_value(&u).unwrap();
        assert_eq!(v["name"], "Bob");
        assert_eq!(v["age"], 42);
    }
}
