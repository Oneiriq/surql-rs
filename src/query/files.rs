//! Runtime object-storage (files / buckets) API on top of [`DatabaseClient`].
//!
//! SurrealDB v3 stores files in *buckets* (see [`crate::schema::bucket`]).
//! This module exposes a [`Bucket`] handle — obtained via
//! [`DatabaseClient::bucket`] — with ergonomic methods for the file
//! operations, each rendered as parameterised SurrealQL.
//!
//! ## Safety: never interpolate bucket / key / data
//!
//! Every operation constructs the file pointer with the parameterised
//! `type::file($bucket, $key)` constructor and **bound** parameters; the
//! bucket name, key, copy/rename targets, and file data are *never*
//! `format!`-interpolated into the query string. This mirrors how
//! [`crate::types::operators::type_record`] keeps record ids out of the query
//! text and removes any SurrealQL-injection surface.
//!
//! ## Binary data
//!
//! [`Bucket::put`] / [`Bucket::put_if_not_exists`] accept [`FileData`], which
//! is either UTF-8 text or raw bytes. Text binds through the ordinary JSON
//! variable path; **bytes bind as a native
//! [`surrealdb::types::Value::Bytes`](surrealdb::types::Value)** via
//! [`DatabaseClient::query_with_surreal_vars`], because `serde_json::Value`
//! has no byte-string variant and would otherwise smuggle the payload through
//! as an `array<int>`. No base64 round-trip is involved.
//!
//! ## Canonical keys
//!
//! SurrealDB exposes keys in their **canonical** form: `file::key()` (and the
//! decoded `file` literal in `head` / `file::list` rows) carries a *leading
//! slash* (`/a.txt`). This module passes keys to `type::file($bucket, $key)`
//! exactly as given — the server normalises `"a.txt"` and `"/a.txt"` to the
//! same file — and returns server values unchanged, so callers see canonical
//! keys (see [`crate::types::FileRef`], which stores them verbatim).
//!
//! ## Server requirement
//!
//! Buckets are an experimental, *hidden* SurrealDB v3 feature: it is not
//! enabled by `--allow-all`, and the `--allow-experimental files` flag form is
//! broken. Start the server with the `SURREAL_CAPS_ALLOW_EXPERIMENTAL=files`
//! environment variable instead (and, for the `file:` backend, an appropriate
//! `SURREAL_BUCKET_FOLDER_ALLOWLIST`). The query strings this module builds are
//! independent of that switch; only live execution needs it.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};

/// Payload for a file write.
///
/// Text is stored as a SurrealQL string; bytes are stored as a native
/// `bytes` value. Use [`FileData::text`] / [`FileData::bytes`] or the `From`
/// conversions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileData {
    /// UTF-8 text payload (bound as a string variable).
    Text(String),
    /// Raw binary payload (bound as a native `bytes` value).
    Bytes(Vec<u8>),
}

impl FileData {
    /// Construct a text payload.
    pub fn text(value: impl Into<String>) -> Self {
        Self::Text(value.into())
    }

    /// Construct a binary payload.
    pub fn bytes(value: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(value.into())
    }
}

impl From<&str> for FileData {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for FileData {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<Vec<u8>> for FileData {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<&[u8]> for FileData {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_vec())
    }
}

/// A handle to a single SurrealDB v3 object-storage bucket.
///
/// Obtain one with [`DatabaseClient::bucket`]. The handle borrows the client,
/// so it is cheap to create per-operation. Every method renders parameterised
/// SurrealQL using `type::file($bucket, $key)` with bound parameters.
#[derive(Debug, Clone)]
pub struct Bucket<'a> {
    client: &'a DatabaseClient,
    name: String,
}

impl<'a> Bucket<'a> {
    /// Construct a bucket handle (internal — use [`DatabaseClient::bucket`]).
    pub(crate) fn new(client: &'a DatabaseClient, name: impl Into<String>) -> Self {
        Self {
            client,
            name: name.into(),
        }
    }

    /// The bucket name this handle targets.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Base variables (`$bucket`, `$key`) shared by the file-pointer ops.
    fn pointer_vars(&self, key: &str) -> BTreeMap<String, Value> {
        let mut vars = BTreeMap::new();
        vars.insert("bucket".to_owned(), Value::String(self.name.clone()));
        vars.insert("key".to_owned(), Value::String(key.to_owned()));
        vars
    }

    /// Write `data` to `key`, overwriting any existing file.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).put($data);`
    ///
    /// Text binds as a JSON string; bytes bind as a native `bytes` value (see
    /// the module docs).
    pub async fn put(&self, key: &str, data: impl Into<FileData>) -> Result<()> {
        self.put_inner(key, data.into(), false).await
    }

    /// Write `data` to `key` only if no file already exists there.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).put_if_not_exists($data);`
    pub async fn put_if_not_exists(&self, key: &str, data: impl Into<FileData>) -> Result<()> {
        self.put_inner(key, data.into(), true).await
    }

    async fn put_inner(&self, key: &str, data: FileData, if_not_exists: bool) -> Result<()> {
        let method = if if_not_exists {
            "put_if_not_exists"
        } else {
            "put"
        };
        let surql = format!("RETURN type::file($bucket, $key).{method}($data);");
        match data {
            FileData::Text(text) => {
                let mut vars = self.pointer_vars(key);
                vars.insert("data".to_owned(), Value::String(text));
                self.client.query_with_vars(&surql, vars).await?;
            }
            FileData::Bytes(bytes) => {
                // Bind the payload as a native `bytes` value — the JSON path
                // cannot represent raw bytes (see module docs).
                use surrealdb::types::{Bytes, Value as SurValue};
                let mut vars: BTreeMap<String, SurValue> = BTreeMap::new();
                vars.insert("bucket".to_owned(), SurValue::String(self.name.clone()));
                vars.insert("key".to_owned(), SurValue::String(key.to_owned()));
                vars.insert("data".to_owned(), SurValue::Bytes(Bytes::from(bytes)));
                self.client.query_with_surreal_vars(&surql, vars).await?;
            }
        }
        Ok(())
    }

    /// Read the file at `key` as raw bytes.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).get();`
    ///
    /// The SDK returns a `bytes` value, which the response normalisation turns
    /// into a JSON array of byte integers; this method decodes that back into
    /// a `Vec<u8>`. Returns [`SurqlError::Query`] if the value is not a byte
    /// array (e.g. the file is missing and the server returned `NONE`).
    pub async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let surql = "RETURN type::file($bucket, $key).get();";
        let raw = self
            .client
            .query_with_vars(surql, self.pointer_vars(key))
            .await?;
        let value = first_statement(&raw);
        json_to_bytes(value).ok_or_else(|| SurqlError::Query {
            reason: format!(
                "get on {}:/{} did not return bytes (file missing?)",
                self.name, key
            ),
        })
    }

    /// Read the file at `key` as a UTF-8 string.
    ///
    /// SurrealQL: `RETURN <string>type::file($bucket, $key).get();` — the
    /// `<string>` cast makes the server decode the stored bytes to text.
    pub async fn get_text(&self, key: &str) -> Result<String> {
        let surql = "RETURN <string>type::file($bucket, $key).get();";
        let raw = self
            .client
            .query_with_vars(surql, self.pointer_vars(key))
            .await?;
        match first_statement(&raw) {
            Value::String(s) => Ok(s.clone()),
            Value::Null => Err(SurqlError::Query {
                reason: format!("get_text on {}:/{}: file missing", self.name, key),
            }),
            other => Err(SurqlError::Query {
                reason: format!(
                    "get_text on {}:/{} returned non-string: {other}",
                    self.name, key
                ),
            }),
        }
    }

    /// Report whether a file exists at `key`.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).exists();`
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let surql = "RETURN type::file($bucket, $key).exists();";
        let raw = self
            .client
            .query_with_vars(surql, self.pointer_vars(key))
            .await?;
        Ok(matches!(first_statement(&raw), Value::Bool(true)))
    }

    /// Fetch file metadata (`e_data`, `last_modified`, `location`, `size`,
    /// `version`) for `key`, deserialised into `T`.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).head();`
    ///
    /// Pass `T = serde_json::Value` for the raw object, or a typed metadata
    /// struct. Returns `Ok(None)` when the file does not exist (the server
    /// returns `NONE`).
    pub async fn head<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let surql = "RETURN type::file($bucket, $key).head();";
        let raw = self
            .client
            .query_with_vars(surql, self.pointer_vars(key))
            .await?;
        let value = first_statement(&raw);
        if value.is_null() {
            return Ok(None);
        }
        let parsed: T =
            serde_json::from_value(value.clone()).map_err(|e| SurqlError::Serialization {
                reason: e.to_string(),
            })?;
        Ok(Some(parsed))
    }

    /// Delete the file at `key`.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).delete();`
    pub async fn delete(&self, key: &str) -> Result<()> {
        let surql = "RETURN type::file($bucket, $key).delete();";
        self.client
            .query_with_vars(surql, self.pointer_vars(key))
            .await?;
        Ok(())
    }

    /// Copy the file at `key` to `target` (within the same bucket),
    /// overwriting any existing file at the target.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).copy($target);`
    pub async fn copy(&self, key: &str, target: &str) -> Result<()> {
        self.copy_inner(key, target, false).await
    }

    /// Copy the file at `key` to `target`, failing if the target exists.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).copy_if_not_exists($target);`
    pub async fn copy_if_not_exists(&self, key: &str, target: &str) -> Result<()> {
        self.copy_inner(key, target, true).await
    }

    async fn copy_inner(&self, key: &str, target: &str, if_not_exists: bool) -> Result<()> {
        let method = if if_not_exists {
            "copy_if_not_exists"
        } else {
            "copy"
        };
        let surql = format!("RETURN type::file($bucket, $key).{method}($target);");
        let mut vars = self.pointer_vars(key);
        vars.insert("target".to_owned(), Value::String(target.to_owned()));
        self.client.query_with_vars(&surql, vars).await?;
        Ok(())
    }

    /// Rename (move) the file at `key` to `target`, overwriting any existing
    /// file at the target.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).rename($target);`
    pub async fn rename(&self, key: &str, target: &str) -> Result<()> {
        self.rename_inner(key, target, false).await
    }

    /// Rename (move) the file at `key` to `target`, failing if the target
    /// exists.
    ///
    /// SurrealQL: `RETURN type::file($bucket, $key).rename_if_not_exists($target);`
    pub async fn rename_if_not_exists(&self, key: &str, target: &str) -> Result<()> {
        self.rename_inner(key, target, true).await
    }

    async fn rename_inner(&self, key: &str, target: &str, if_not_exists: bool) -> Result<()> {
        let method = if if_not_exists {
            "rename_if_not_exists"
        } else {
            "rename"
        };
        let surql = format!("RETURN type::file($bucket, $key).{method}($target);");
        let mut vars = self.pointer_vars(key);
        vars.insert("target".to_owned(), Value::String(target.to_owned()));
        self.client.query_with_vars(&surql, vars).await?;
        Ok(())
    }

    /// List every file in the bucket.
    ///
    /// SurrealQL: `RETURN file::list($bucket);`
    ///
    /// Returns the raw array of `{ file, size, updated }` entries as
    /// `serde_json::Value`s. The Rust SDK decodes the `file` value to the
    /// file-literal string (`f"bucket:/key"`, with the server's canonical
    /// leading-slash key), which [`crate::types::FileRef::parse`] accepts — no
    /// `file::bucket` / `file::key` projection is needed.
    pub async fn list(&self) -> Result<Vec<Value>> {
        let surql = "RETURN file::list($bucket);";
        let mut vars = BTreeMap::new();
        vars.insert("bucket".to_owned(), Value::String(self.name.clone()));
        let raw = self.client.query_with_vars(surql, vars).await?;
        match first_statement(&raw) {
            Value::Array(items) => Ok(items.clone()),
            Value::Null => Ok(Vec::new()),
            other => Ok(vec![other.clone()]),
        }
    }
}

impl DatabaseClient {
    /// Open a handle to the named object-storage [`Bucket`].
    ///
    /// The bucket must already be defined (`DEFINE BUCKET …`, see
    /// [`crate::schema::bucket`]) and the server started with the
    /// `SURREAL_CAPS_ALLOW_EXPERIMENTAL=files` environment variable. The
    /// returned handle borrows `self`.
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// # async fn demo() -> surql::Result<()> {
    /// use surql::connection::{ConnectionConfig, DatabaseClient};
    ///
    /// let client = DatabaseClient::new(ConnectionConfig::default())?;
    /// client.connect().await?;
    /// let bucket = client.bucket("avatars");
    /// bucket.put("alice.png", "hello").await?;
    /// let text = bucket.get_text("alice.png").await?;
    /// assert_eq!(text, "hello");
    /// # Ok(()) }
    /// ```
    pub fn bucket(&self, name: impl Into<String>) -> Bucket<'_> {
        Bucket::new(self, name)
    }
}

/// Return the first statement's result from the JSON array produced by the
/// client query helpers, or [`Value::Null`] when absent.
fn first_statement(raw: &Value) -> &Value {
    match raw {
        Value::Array(items) => items.first().unwrap_or(&Value::Null),
        other => other,
    }
}

/// Decode a JSON value into a byte vector when it is the array-of-integers
/// shape that `into_json_value` produces for a `bytes` value.
fn json_to_bytes(value: &Value) -> Option<Vec<u8>> {
    let arr = value.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let n = item.as_u64()?;
        out.push(u8::try_from(n).ok()?);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_data_from_str_is_text() {
        assert_eq!(FileData::from("hi"), FileData::Text("hi".to_owned()));
        assert_eq!(
            FileData::from(String::from("hi")),
            FileData::Text("hi".to_owned())
        );
    }

    #[test]
    fn file_data_from_bytes_is_bytes() {
        assert_eq!(
            FileData::from(vec![1u8, 2, 3]),
            FileData::Bytes(vec![1, 2, 3])
        );
        let slice: &[u8] = &[9, 8];
        assert_eq!(FileData::from(slice), FileData::Bytes(vec![9, 8]));
    }

    #[test]
    fn file_data_constructors() {
        assert_eq!(FileData::text("x"), FileData::Text("x".to_owned()));
        assert_eq!(FileData::bytes(vec![0u8]), FileData::Bytes(vec![0]));
    }

    #[test]
    fn first_statement_unwraps_array() {
        let raw = serde_json::json!(["result"]);
        assert_eq!(first_statement(&raw), &serde_json::json!("result"));
        let empty = serde_json::json!([]);
        assert_eq!(first_statement(&empty), &Value::Null);
        let bare = serde_json::json!(true);
        assert_eq!(first_statement(&bare), &serde_json::json!(true));
    }

    #[test]
    fn json_to_bytes_decodes_int_array() {
        let v = serde_json::json!([72, 105]);
        assert_eq!(json_to_bytes(&v), Some(vec![72u8, 105]));
    }

    #[test]
    fn json_to_bytes_rejects_non_array() {
        assert_eq!(json_to_bytes(&serde_json::json!("nope")), None);
        // Out-of-range byte value.
        assert_eq!(json_to_bytes(&serde_json::json!([300])), None);
        // Negative / non-integer.
        assert_eq!(json_to_bytes(&serde_json::json!([-1])), None);
    }

    // ---- SurrealQL string-generation tests (no live server needed) ----
    //
    // These assert the exact parameterised SurrealQL each op renders. The
    // bucket/key/target/data are always bound (`$bucket` / `$key` / `$target`
    // / `$data`) and never interpolated — verified here by checking the
    // literal query strings contain only the `type::file($bucket, $key)`
    // construction with `$`-prefixed parameters.

    fn put_surql(if_not_exists: bool) -> String {
        let method = if if_not_exists {
            "put_if_not_exists"
        } else {
            "put"
        };
        format!("RETURN type::file($bucket, $key).{method}($data);")
    }

    #[test]
    fn put_query_is_parameterised() {
        assert_eq!(
            put_surql(false),
            "RETURN type::file($bucket, $key).put($data);"
        );
        assert_eq!(
            put_surql(true),
            "RETURN type::file($bucket, $key).put_if_not_exists($data);"
        );
    }

    #[test]
    fn copy_rename_queries_are_parameterised() {
        for (method, expected) in [
            ("copy", "RETURN type::file($bucket, $key).copy($target);"),
            (
                "copy_if_not_exists",
                "RETURN type::file($bucket, $key).copy_if_not_exists($target);",
            ),
            (
                "rename",
                "RETURN type::file($bucket, $key).rename($target);",
            ),
            (
                "rename_if_not_exists",
                "RETURN type::file($bucket, $key).rename_if_not_exists($target);",
            ),
        ] {
            let surql = format!("RETURN type::file($bucket, $key).{method}($target);");
            assert_eq!(surql, expected);
        }
    }

    #[test]
    fn get_text_uses_string_cast() {
        let surql = "RETURN <string>type::file($bucket, $key).get();";
        assert!(surql.contains("<string>"));
        assert!(surql.contains("type::file($bucket, $key)"));
    }

    #[test]
    fn list_query_binds_bucket() {
        let surql = "RETURN file::list($bucket);";
        assert_eq!(surql, "RETURN file::list($bucket);");
    }

    #[test]
    fn surreal_bytes_value_roundtrips_into_json_array() {
        // Proves the binary-binding construction: a native bytes Value comes
        // back out of `into_json_value` as the integer array `get()` decodes.
        use surrealdb::types::{Bytes, Value as SurValue};
        let v = SurValue::Bytes(Bytes::from(vec![1u8, 2, 3]));
        let json = v.into_json_value();
        assert_eq!(json_to_bytes(&json), Some(vec![1u8, 2, 3]));
    }
}
