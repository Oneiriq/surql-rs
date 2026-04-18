//! # surql
//!
//! Code-first database toolkit for SurrealDB in Rust.
//!
//! Rust port of [`oneiriq-surql`](https://github.com/Oneiriq/surql-py) (Python) and
//! [`@oneiriq/surql`](https://github.com/Oneiriq/surql) (TypeScript).
//! Target: 1:1 feature parity.
//!
//! ## Modules
//!
//! - [`error`]: [`SurqlError`](error::SurqlError) and [`Result`](error::Result).
//! - [`types`]: Type-safe wrappers ([`RecordID`](types::RecordID),
//!   [`RecordRef`](types::RecordRef), [`SurrealFn`](types::SurrealFn),
//!   operators, reserved-word checks, datetime coercion).
//! - [`connection`]: Connection [`ConnectionConfig`](connection::ConnectionConfig)
//!   and credential types ([`RootCredentials`](connection::RootCredentials),
//!   [`NamespaceCredentials`](connection::NamespaceCredentials),
//!   [`DatabaseCredentials`](connection::DatabaseCredentials),
//!   [`ScopeCredentials`](connection::ScopeCredentials)).
//!
//! Additional modules (`query`, `schema`, `migration`, `cache`,
//! `orchestration`) are under active port and will land incrementally.

#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::return_self_not_must_use)]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod connection;
pub mod error;
pub mod types;

pub use error::{Result, SurqlError};
