//! Sessions: why `surql-rs` does not expose a multiplexed-session API.
//!
//! The other ports in the `surql` family (`surql-py`, `surql` / TypeScript)
//! expose a *session* abstraction: a single physical connection that can be
//! split into multiple logically-isolated contexts, each with its own
//! namespace / database / authentication, multiplexed over the one socket.
//!
//! **The Rust `surrealdb` crate (3.x) provides no such multiplexed-session
//! API.** A [`surrealdb::Surreal<Any>`](surrealdb::Surreal) handle owns a
//! single session: `use_ns` / `use_db` / `signin` / `authenticate` /
//! `invalidate` all mutate that one session in place. There is no
//! `Surreal::session()` / per-call session token that would let one handle
//! carry several independent namespace/auth contexts concurrently. Faking it
//! — e.g. by toggling `use_ns` / `use_db` around each call — would not be
//! isolated (a concurrent task on the same handle would observe the switch)
//! and would silently corrupt results, so this crate deliberately does **not**
//! offer a `Session` type or a `DatabaseClient::new_session` method.
//!
//! ## What to do instead
//!
//! For isolated namespace / database / authentication contexts, create a
//! separate [`DatabaseClient`](crate::connection::DatabaseClient) per context.
//! Each client owns its own connection and therefore its own independent
//! session; they cannot interfere with one another.
//!
//! ```no_run
//! # async fn demo() -> surql::Result<()> {
//! use surql::connection::{ConnectionConfig, DatabaseClient};
//!
//! // One client per (namespace, database) context — each is fully isolated.
//! let tenant_a = DatabaseClient::new(
//!     ConnectionConfig::builder()
//!         .url("ws://localhost:8000")
//!         .namespace("tenant_a")
//!         .database("app")
//!         .build()?,
//! )?;
//! tenant_a.connect().await?;
//!
//! let tenant_b = DatabaseClient::new(
//!     ConnectionConfig::builder()
//!         .url("ws://localhost:8000")
//!         .namespace("tenant_b")
//!         .database("app")
//!         .build()?,
//! )?;
//! tenant_b.connect().await?;
//!
//! // Queries on `tenant_a` and `tenant_b` never share session state.
//! # let _ = (tenant_a, tenant_b);
//! # Ok(())
//! # }
//! ```
//!
//! If you only need to switch the active namespace/database on a single
//! logical actor (and accept that it is *not* concurrent-isolated), call
//! `client.inner().use_ns(...).use_db(...)` on the underlying SDK handle
//! directly — but prefer separate clients whenever isolation matters.
//!
//! This module intentionally contains no runtime types; it exists so the
//! "where are sessions?" question has a documented, discoverable answer that
//! lives next to the connection code.

// (No public items: sessions are unsupported by design — see the module docs.)
