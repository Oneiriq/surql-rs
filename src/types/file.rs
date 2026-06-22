//! Type-safe `FileRef` wrapper for SurrealDB v3 file pointers.
//!
//! A SurrealDB v3 file value points at an object inside a bucket
//! (see [`crate::schema::bucket`]). On the wire SurrealDB renders it as the
//! file-literal `f"<bucket>:/<key>"`; as a structured value it is the object
//! `{ bucket, key }`. [`FileRef`] models that pointer on the client side.
//!
//! ## Canonical keys
//!
//! SurrealDB exposes the **canonical** key form: `file::key()` returns keys
//! with a *leading slash* (e.g. `/a.txt`) regardless of how the file was
//! written, and the server normalises input itself — `type::file($b, "a.txt")`
//! and `type::file($b, "/a.txt")` resolve to the *same* file. [`FileRef`]
//! therefore stores the key **verbatim** (it may or may not carry a leading
//! slash) and never strips or rewrites it. Two refs are equal iff their stored
//! keys are byte-for-byte equal, so a ref built from the canonical `/a.txt`
//! the server returns is preserved exactly.
//!
//! ## Display vs. serde
//!
//! - [`FileRef::Display`](FileRef) renders the `"<bucket>:/<key>"` pointer with
//!   exactly one leading slash on the key, for *any* stored key: both `"a.txt"`
//!   and `"/a.txt"` render `"<bucket>:/a.txt"` (no `f"…"` quoting — that is a
//!   *SurrealQL literal* concern handled by the runtime file API, which never
//!   string-interpolates a key).
//! - serde round-trips the structured `{ "bucket": …, "key": … }` shape
//!   ("SQON"), matching `surrealdb::types::File`. The key is serialised with a
//!   single leading slash (the form `surrealdb::types::File` stores) and read
//!   back verbatim, so the canonical `{ "bucket": "b", "key": "/a.txt" }` the
//!   server emits round-trips unchanged.
//!
//! ## Normalising query responses
//!
//! When a `file` value flows back through
//! [`DatabaseClient::query`](crate::connection::DatabaseClient::query) it is
//! converted by the SDK's `into_json_value` into the **string** form
//! `f"<bucket>:/<key>"` (not the object form). [`FileRef::parse`] accepts that
//! literal — including the canonical `/key` shape — so callers can recover a
//! [`FileRef`] from a response the same way
//! [`RecordID::parse`](crate::types::RecordID::parse) recovers a record id.

use std::fmt;

use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Result, SurqlError};

/// Type-safe SurrealDB v3 file pointer (`f"bucket:/key"`).
///
/// The key is stored **verbatim**: SurrealDB's canonical key form carries a
/// leading slash (`/a.txt`), and a `FileRef` preserves whatever it is given so
/// a value recovered from the server round-trips unchanged.
/// [`Display`](FileRef::fmt) always renders a single-slash pointer regardless
/// of whether the stored key has a leading slash.
///
/// ## Examples
///
/// ```
/// use surql::types::FileRef;
///
/// // A bare key and the canonical leading-slash key both render the same
/// // single-slash pointer.
/// let f = FileRef::new("avatars", "alice.png");
/// assert_eq!(f.to_string(), "avatars:/alice.png");
/// assert_eq!(FileRef::new("avatars", "/alice.png").to_string(), "avatars:/alice.png");
/// assert_eq!(f.bucket(), "avatars");
/// assert_eq!(f.key(), "alice.png");
///
/// // The SurrealQL file literal round-trips through `parse`. The server
/// // returns the canonical `/key` form, which is preserved verbatim.
/// let parsed = FileRef::parse("f\"avatars:/alice.png\"").unwrap();
/// assert_eq!(parsed.key(), "/alice.png");
/// assert_eq!(parsed.to_string(), "avatars:/alice.png");
///
/// // The bare `bucket:/key` form parses too.
/// assert_eq!(FileRef::parse("avatars:/alice.png").unwrap(), parsed);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileRef {
    bucket: String,
    /// Stored verbatim — may or may not carry a leading `/`.
    /// [`FileRef::Display`] always emits exactly one `/` after the `:`.
    key: String,
}

impl FileRef {
    /// Construct a [`FileRef`] from a bucket and key.
    ///
    /// The key is stored **verbatim** (no slash stripping or rewriting); this
    /// preserves SurrealDB's canonical leading-slash key form. Note that this
    /// means `FileRef::new("b", "k")` and `FileRef::new("b", "/k")` are *not*
    /// equal even though they Display identically and address the same file —
    /// equality is over the stored key. Use [`FileRef::parse`] when you have a
    /// pointer string and want a single canonical value.
    pub fn new(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    /// The bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The object key, **verbatim** as stored (may include a leading `/`).
    ///
    /// SurrealDB's canonical form carries a leading slash (`/a.txt`); this
    /// returns whatever the ref holds without normalising it.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Parse a file pointer from either the SurrealQL literal `f"bucket:/key"`
    /// or the bare `bucket:/key` form.
    ///
    /// The key is stored **verbatim** after the bucket separator, including its
    /// leading slash, so the canonical `f"b:/a.txt"` the server emits parses to
    /// a key of `/a.txt`.
    ///
    /// Returns [`SurqlError::Validation`] when the input has no `:` separator
    /// or an empty bucket/key.
    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        // Strip the optional SurrealQL file-literal wrapper: f"...".
        let inner = trimmed
            .strip_prefix("f\"")
            .and_then(|s| s.strip_suffix('"'))
            .or_else(|| trimmed.strip_prefix('f').and_then(strip_matching_quotes))
            .unwrap_or(trimmed);

        let Some((bucket, key)) = inner.split_once(':') else {
            return Err(SurqlError::Validation {
                reason: format!("Invalid file pointer {input:?}: expected bucket:/key"),
            });
        };
        let bucket = bucket.trim();
        // Keep the key verbatim (canonical keys carry a leading slash); only
        // trim surrounding whitespace.
        let key = key.trim();
        if bucket.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Invalid file pointer {input:?}: empty bucket"),
            });
        }
        // A key consisting solely of slashes (e.g. `bucket:/` or `bucket:`) is
        // not a usable key.
        if key.trim_start_matches('/').is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Invalid file pointer {input:?}: empty key"),
            });
        }
        Ok(Self::new(bucket, key))
    }

    /// `true` when `value` is the SurrealQL file-literal string form
    /// (`f"bucket:/key"`) that the SDK's `into_json_value` emits for a `file`
    /// value. Used by the query-response normalisation path to recognise file
    /// values the same way record ids are recognised.
    pub fn is_file_literal(value: &str) -> bool {
        let v = value.trim();
        v.starts_with("f\"") && v.ends_with('"') && v.len() >= 3 && v.contains(":/")
    }
}

/// Strip a matching pair of single or double quotes from the ends of `s`.
fn strip_matching_quotes(s: &str) -> Option<&str> {
    let mut chars = s.chars();
    let first = chars.next()?;
    let last = chars.next_back()?;
    if (first == '"' || first == '\'') && first == last {
        s.strip_prefix(first).and_then(|t| t.strip_suffix(last))
    } else {
        None
    }
}

impl fmt::Display for FileRef {
    /// Render the `<bucket>:/<key>` pointer with exactly one leading slash on
    /// the key, regardless of whether the stored key has one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:/{}", self.bucket, self.key.trim_start_matches('/'))
    }
}

impl Serialize for FileRef {
    /// Serialise as the structured `{ "bucket", "key" }` object (SQON), with a
    /// single leading-`/` key to match `surrealdb::types::File`'s own
    /// representation.
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FileRef", 2)?;
        state.serialize_field("bucket", &self.bucket)?;
        state.serialize_field("key", &format!("/{}", self.key.trim_start_matches('/')))?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for FileRef {
    /// Accept either the structured `{ bucket, key }` object or a plain string
    /// in the `f"bucket:/key"` / `bucket:/key` form. The key is stored verbatim
    /// from the object form (canonical keys keep their leading slash).
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FileRefVisitor;

        impl<'de> Visitor<'de> for FileRefVisitor {
            type Value = FileRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a file pointer string or { bucket, key } object")
            }

            fn visit_str<E>(self, v: &str) -> std::result::Result<FileRef, E>
            where
                E: de::Error,
            {
                FileRef::parse(v).map_err(de::Error::custom)
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<FileRef, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut bucket: Option<String> = None;
                let mut key: Option<String> = None;
                while let Some(field) = map.next_key::<String>()? {
                    match field.as_str() {
                        "bucket" => bucket = Some(map.next_value()?),
                        "key" => key = Some(map.next_value()?),
                        _ => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                let bucket = bucket.ok_or_else(|| de::Error::missing_field("bucket"))?;
                let key = key.ok_or_else(|| de::Error::missing_field("key"))?;
                Ok(FileRef::new(bucket, key))
            }
        }

        deserializer.deserialize_any(FileRefVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stores_key_verbatim() {
        // Canonical (leading-slash) keys are preserved, not stripped.
        let f = FileRef::new("b", "/key");
        assert_eq!(f.key(), "/key");
        assert_eq!(f.bucket(), "b");
        // A bare key is likewise kept verbatim.
        let g = FileRef::new("b", "key");
        assert_eq!(g.key(), "key");
    }

    #[test]
    fn display_emits_single_slash_for_any_input() {
        // Both the bare and the canonical leading-slash key render identically.
        assert_eq!(
            FileRef::new("avatars", "alice.png").to_string(),
            "avatars:/alice.png"
        );
        assert_eq!(
            FileRef::new("avatars", "/alice.png").to_string(),
            "avatars:/alice.png"
        );
    }

    #[test]
    fn parse_bare_form_keeps_slash() {
        // The canonical `/key` is preserved on the stored key.
        let f = FileRef::parse("avatars:/alice.png").unwrap();
        assert_eq!(f.key(), "/alice.png");
        assert_eq!(f.bucket(), "avatars");
        assert_eq!(f.to_string(), "avatars:/alice.png");
    }

    #[test]
    fn parse_literal_form_keeps_slash() {
        let f = FileRef::parse("f\"avatars:/alice.png\"").unwrap();
        assert_eq!(f.key(), "/alice.png");
        assert_eq!(f.to_string(), "avatars:/alice.png");
    }

    #[test]
    fn parse_nested_key_path() {
        let f = FileRef::parse("docs:/folder/sub/file.txt").unwrap();
        assert_eq!(f.key(), "/folder/sub/file.txt");
        assert_eq!(f.to_string(), "docs:/folder/sub/file.txt");
    }

    #[test]
    fn parse_rejects_missing_colon() {
        assert!(FileRef::parse("nokey").is_err());
    }

    #[test]
    fn parse_rejects_empty_bucket() {
        assert!(FileRef::parse(":/key").is_err());
    }

    #[test]
    fn parse_rejects_empty_key() {
        assert!(FileRef::parse("bucket:/").is_err());
        assert!(FileRef::parse("bucket:").is_err());
    }

    #[test]
    fn is_file_literal_recognises_sql_form() {
        assert!(FileRef::is_file_literal("f\"b:/k\""));
        assert!(!FileRef::is_file_literal("b:/k"));
        assert!(!FileRef::is_file_literal("user:alice"));
        assert!(!FileRef::is_file_literal("just a string"));
    }

    #[test]
    fn serde_serializes_structured_with_single_slash_key() {
        // A bare key gains the canonical single leading slash on the wire.
        let f = FileRef::new("b", "k");
        let json = serde_json::to_value(&f).unwrap();
        assert_eq!(json, serde_json::json!({ "bucket": "b", "key": "/k" }));
        // An already-canonical key is not double-slashed.
        let g = FileRef::new("b", "/k");
        assert_eq!(
            serde_json::to_value(&g).unwrap(),
            serde_json::json!({ "bucket": "b", "key": "/k" })
        );
    }

    #[test]
    fn serde_roundtrip_canonical_object() {
        // The canonical server shape round-trips byte-for-byte.
        let f = FileRef::new("avatars", "/alice.png");
        let json = serde_json::to_string(&f).unwrap();
        let back: FileRef = serde_json::from_str(&json).unwrap();
        assert_eq!(f, back);
        assert_eq!(back.key(), "/alice.png");
    }

    #[test]
    fn serde_deserializes_from_string_literal_keeps_slash() {
        let back: FileRef = serde_json::from_value(serde_json::json!("f\"b:/k\"")).unwrap();
        assert_eq!(back, FileRef::new("b", "/k"));
        assert_eq!(back.key(), "/k");
    }

    #[test]
    fn serde_deserializes_from_object_verbatim() {
        // Object key is stored exactly as given.
        let back: FileRef =
            serde_json::from_value(serde_json::json!({ "bucket": "b", "key": "/k" })).unwrap();
        assert_eq!(back.key(), "/k");
    }

    #[test]
    fn serde_matches_surrealdb_file_shape() {
        // surrealdb::types::File serialises as { bucket, key: "/..." }; our
        // deserialiser must accept that verbatim and our serialiser produce it.
        let surreal_shape = serde_json::json!({ "bucket": "b", "key": "/folder/file" });
        let back: FileRef = serde_json::from_value(surreal_shape.clone()).unwrap();
        assert_eq!(back.key(), "/folder/file");
        assert_eq!(serde_json::to_value(&back).unwrap(), surreal_shape);
    }

    #[test]
    fn equality_is_over_verbatim_key() {
        // Distinct stored keys are not equal even if they address the same file.
        assert_ne!(FileRef::new("b", "k"), FileRef::new("b", "/k"));
        // Identical stored keys are equal.
        assert_eq!(FileRef::new("b", "/k"), FileRef::new("b", "/k"));
    }

    #[test]
    fn ord_is_stable() {
        let a = FileRef::new("a", "/1");
        let b = FileRef::new("a", "/2");
        assert!(a < b);
    }
}
