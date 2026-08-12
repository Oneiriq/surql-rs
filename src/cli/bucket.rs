//! `surql bucket` subcommands.
//!
//! Manage SurrealDB v3 object-storage buckets and their files from the CLI.
//! Mirrors the structure of [`crate::cli::schema`]: a thin wrapper that
//! delegates every side effect to the library
//! ([`BucketDefinition`](crate::schema::BucketDefinition),
//! [`Bucket`](crate::query::files::Bucket), and the
//! [`parse_db_info`](crate::schema::parse_db_info) parser).
//!
//! The `define` / `list` / `rm` commands operate on bucket *definitions*; the
//! `put` / `get` / `delete` / `exists` / `files` commands operate on the
//! *files* inside a bucket. Buckets require the server to be started with the
//! `SURREAL_CAPS_ALLOW_EXPERIMENTAL=files` environment variable (the feature is
//! hidden and not enabled by `--allow-all`; the `--allow-experimental files`
//! flag form is broken).

use std::path::PathBuf;

use clap::Subcommand;

use crate::cli::fmt;
use crate::cli::GlobalOpts;
use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::query::files::FileData;
use crate::schema::{bucket_schema, parse_db_info};

/// `surql bucket <subcommand>` commands.
#[derive(Debug, Subcommand)]
pub enum BucketCommand {
    /// Define (create) a bucket: `DEFINE BUCKET <name> BACKEND "<backend>"`.
    Define {
        /// Bucket name.
        name: String,
        /// Storage backend (`memory` / `file:/path` / `s3://bucket`).
        #[arg(long, default_value = "memory")]
        backend: String,
        /// Mark the bucket read-only.
        #[arg(long)]
        readonly: bool,
        /// Optional comment.
        #[arg(long)]
        comment: Option<String>,
        /// Use `IF NOT EXISTS` so re-running is idempotent.
        #[arg(long = "if-not-exists")]
        if_not_exists: bool,
    },
    /// List every bucket defined in the database.
    List,
    /// Remove a bucket: `REMOVE BUCKET <name>`.
    Rm {
        /// Bucket name.
        name: String,
    },
    /// Write a file into a bucket.
    Put {
        /// Bucket name.
        bucket: String,
        /// File key (the object name within the bucket).
        key: String,
        /// Inline text content to store. Mutually exclusive with `--file`.
        #[arg(long, conflicts_with = "file")]
        text: Option<String>,
        /// Read the (binary) content from this local file. Mutually exclusive
        /// with `--text`.
        #[arg(long, value_name = "PATH", conflicts_with = "text")]
        file: Option<PathBuf>,
        /// Only write if the key does not already exist.
        #[arg(long = "if-not-exists")]
        if_not_exists: bool,
    },
    /// Read a file from a bucket and print it (text) or write it to a path.
    Get {
        /// Bucket name.
        bucket: String,
        /// File key.
        key: String,
        /// Write the raw bytes to this path instead of printing as text.
        #[arg(long, short = 'o', value_name = "PATH")]
        output: Option<PathBuf>,
    },
    /// Delete a file from a bucket.
    Delete {
        /// Bucket name.
        bucket: String,
        /// File key.
        key: String,
    },
    /// Report whether a file exists in a bucket (exit-style boolean print).
    Exists {
        /// Bucket name.
        bucket: String,
        /// File key.
        key: String,
    },
    /// List every file inside a bucket.
    Files {
        /// Bucket name.
        bucket: String,
    },
}

/// Execute a `surql bucket` subcommand.
///
/// # Errors
///
/// Propagates [`SurqlError`] values from the underlying library calls.
pub async fn run(cmd: BucketCommand, global: &GlobalOpts) -> Result<()> {
    let settings = global.settings()?;
    match cmd {
        BucketCommand::Define {
            name,
            backend,
            readonly,
            comment,
            if_not_exists,
        } => {
            define(
                &settings,
                &name,
                &backend,
                readonly,
                comment.as_deref(),
                if_not_exists,
            )
            .await
        }
        BucketCommand::List => list(&settings).await,
        BucketCommand::Rm { name } => rm(&settings, &name).await,
        BucketCommand::Put {
            bucket,
            key,
            text,
            file,
            if_not_exists,
        } => {
            put(
                &settings,
                &bucket,
                &key,
                text,
                file.as_deref(),
                if_not_exists,
            )
            .await
        }
        BucketCommand::Get {
            bucket,
            key,
            output,
        } => get(&settings, &bucket, &key, output.as_deref()).await,
        BucketCommand::Delete { bucket, key } => delete(&settings, &bucket, &key).await,
        BucketCommand::Exists { bucket, key } => exists(&settings, &bucket, &key).await,
        BucketCommand::Files { bucket } => files(&settings, &bucket).await,
    }
}

async fn connected_client(settings: &crate::settings::Settings) -> Result<DatabaseClient> {
    let client = DatabaseClient::new(settings.database().clone())?;
    client.connect().await?;
    Ok(client)
}

async fn define(
    settings: &crate::settings::Settings,
    name: &str,
    backend: &str,
    readonly: bool,
    comment: Option<&str>,
    if_not_exists: bool,
) -> Result<()> {
    let mut builder = bucket_schema(name, backend).readonly(readonly);
    if let Some(c) = comment {
        builder = builder.comment(c);
    }
    let definition = builder.build()?;
    let surql = definition.to_surql_with_options(if_not_exists, false)?;
    let client = connected_client(settings).await?;
    client.query(&surql).await?;
    fmt::success(format!("defined bucket {name}"));
    Ok(())
}

async fn list(settings: &crate::settings::Settings) -> Result<()> {
    let client = connected_client(settings).await?;
    let info = client.query("INFO FOR DB;").await?;
    let parsed = parse_db_info(&info)?;
    if parsed.buckets.is_empty() {
        fmt::info("no buckets defined");
        return Ok(());
    }
    let mut table = fmt::make_table();
    table.set_header(vec!["bucket", "backend", "readonly"]);
    for (name, def) in &parsed.buckets {
        table.add_row(vec![
            name.clone(),
            def.backend.clone(),
            def.readonly.to_string(),
        ]);
    }
    println!("{table}");
    Ok(())
}

async fn rm(settings: &crate::settings::Settings, name: &str) -> Result<()> {
    let client = connected_client(settings).await?;
    client
        .query(&crate::schema::BucketDefinition::remove_surql(name))
        .await?;
    fmt::success(format!("removed bucket {name}"));
    Ok(())
}

async fn put(
    settings: &crate::settings::Settings,
    bucket: &str,
    key: &str,
    text: Option<String>,
    file: Option<&std::path::Path>,
    if_not_exists: bool,
) -> Result<()> {
    let data: FileData = match (text, file) {
        (Some(t), None) => FileData::Text(t),
        (None, Some(path)) => {
            let bytes = std::fs::read(path)?;
            FileData::Bytes(bytes)
        }
        (None, None) => {
            return Err(SurqlError::Validation {
                reason: "provide either --text or --file".into(),
            })
        }
        // clap's `conflicts_with` makes this unreachable, but stay total.
        (Some(_), Some(_)) => {
            return Err(SurqlError::Validation {
                reason: "--text and --file are mutually exclusive".into(),
            })
        }
    };
    let client = connected_client(settings).await?;
    let handle = client.bucket(bucket);
    if if_not_exists {
        handle.put_if_not_exists(key, data).await?;
    } else {
        handle.put(key, data).await?;
    }
    fmt::success(format!("wrote {bucket}:/{key}"));
    Ok(())
}

async fn get(
    settings: &crate::settings::Settings,
    bucket: &str,
    key: &str,
    output: Option<&std::path::Path>,
) -> Result<()> {
    let client = connected_client(settings).await?;
    let handle = client.bucket(bucket);
    if let Some(path) = output {
        let bytes = handle.get(key).await?;
        std::fs::write(path, &bytes)?;
        fmt::success(format!(
            "wrote {} byte(s) to {}",
            bytes.len(),
            path.display()
        ));
    } else {
        let text = handle.get_text(key).await?;
        println!("{text}");
    }
    Ok(())
}

async fn delete(settings: &crate::settings::Settings, bucket: &str, key: &str) -> Result<()> {
    let client = connected_client(settings).await?;
    client.bucket(bucket).delete(key).await?;
    fmt::success(format!("deleted {bucket}:/{key}"));
    Ok(())
}

async fn exists(settings: &crate::settings::Settings, bucket: &str, key: &str) -> Result<()> {
    let client = connected_client(settings).await?;
    let present = client.bucket(bucket).exists(key).await?;
    fmt::info(format!("{present}"));
    Ok(())
}

async fn files(settings: &crate::settings::Settings, bucket: &str) -> Result<()> {
    let client = connected_client(settings).await?;
    let entries = client.bucket(bucket).list().await?;
    if entries.is_empty() {
        fmt::info(format!("no files in bucket {bucket}"));
        return Ok(());
    }
    fmt::print_json(&entries)?;
    Ok(())
}
