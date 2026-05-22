# Caching

The `cache` module provides a pluggable cache layer with two built-in
backends (in-memory and Redis), a global manager for read-through
helpers, and a stable cache-key helper for query memoization.

Everything in `surql::cache` is gated behind the `cache` feature.
The Redis backend additionally requires `cache-redis`.

```toml
[dependencies]
oneiriq-surql = { version = "0.2", features = ["cache", "cache-redis"] }
```

## Concepts

| Type                | Role                                                                                                  |
|---------------------|-------------------------------------------------------------------------------------------------------|
| `CacheBackend`      | Async trait: `get`, `set`, `delete`, `exists`, `clear`.                                               |
| `MemoryCache`       | In-process LRU+TTL backend backed by a `tokio::sync::RwLock<HashMap>`.                                |
| `RedisCache`        | Redis-backed backend with lazy connection setup and JSON-on-the-wire values.                          |
| `CacheManager`      | Owns a backend, tracks table to keys associations for invalidation, records hit/miss statistics.      |
| `CacheConfig`       | Layered configuration (`CacheConfigBuilder`, `CacheOptions`) covering backend kind, TTLs, key prefix. |
| `CacheStatsSnapshot`| Cloneable view of hit, miss, set, and eviction counters.                                              |

## Quick start

### Configure the global manager

```rust
use surql::cache::{configure_cache, CacheConfigBuilder, CacheBackendKind};

let config = CacheConfigBuilder::new()
    .backend(CacheBackendKind::Memory)
    .default_ttl_secs(60)
    .max_size(1024)
    .build();

let manager = configure_cache(config)?;
```

`configure_cache` builds the `CacheManager` from the supplied config
and installs it as the process-wide default returned by
`get_cache_manager`. Subsequent calls overwrite the previous manager.

### Read-through with the `cached` helper

```rust
use surql::cache::cached;

let users: Vec<User> = cached("users:active", Some(30), || async {
    db.query("SELECT * FROM user WHERE active = true").await
}).await?;
```

The closure runs only on a miss. When no manager is configured the
closure runs every call and the result is returned directly, so
library code can call `cached` unconditionally and let the consumer
opt in to caching by configuring the global manager.

`ttl_secs` is `Option<u64>`; `None` falls back to the manager's
configured `default_ttl_secs`.

### Decorator-style API

```rust
use surql::cache::{cached_with, get_or_init_manager};

let manager = get_or_init_manager();
let value = cached_with(&manager, "key", Some(30), || async {
    fetch_value().await
}).await?;
```

`cached_with` lets you pass an explicit manager instead of the global
one. Use it when you need separate cache surfaces (for example a
per-tenant cache) inside the same process.

### Stable keys for memoised functions

```rust
use surql::cache::cache_key_for;

let key = cache_key_for("queries", "find_user_by_email", &("alice@example.com",))?;
let user: Option<User> = cached(&key, Some(30), || async {
    db.query("SELECT * FROM user WHERE email = $email")
        .bind(("email", "alice@example.com"))
        .await
}).await?;
```

`cache_key_for` hashes the supplied identifier and serialisable
argument list into a stable string of the form
`{module}.{name}:{8-byte hex}`.

## Redis backend

```rust
use surql::cache::{configure_cache, CacheConfigBuilder, CacheBackendKind};

let config = CacheConfigBuilder::new()
    .backend(CacheBackendKind::Redis)
    .redis_url("redis://127.0.0.1:6379")
    .key_prefix("surql:")
    .build();

let manager = configure_cache(config)?;
```

`RedisCache` lazily opens its connection on the first `get` / `set`.
Values are JSON-encoded on the wire so the backend can be shared with
non-Rust consumers that adhere to the same prefix and value contract.

## Statistics and invalidation

```rust
use surql::cache::get_cache_manager;

let manager = get_cache_manager().expect("cache configured");
let stats = manager.stats_snapshot();
println!("hit ratio: {:.2}", stats.hit_ratio());

manager.invalidate_table("user").await?;
manager.invalidate_key("users:active").await?;
manager.invalidate_pattern("users:*").await?;
manager.clear().await?;
```

`invalidate_table` deletes every key the manager has associated with
the given table; associations are recorded when callers tag a
`get_or_set` invocation with the relevant table list. `invalidate_key`
removes a single key, `invalidate_pattern` accepts the backend's
native wildcard form (Redis `KEYS` style), and `clear` drops every
entry.

## Installing a custom backend

```rust
use std::sync::Arc;
use surql::cache::{install_backend, CacheBackend, CacheConfigBuilder};

let backend: Arc<dyn CacheBackend> = Arc::new(MyBackend::default());
let manager = install_backend(CacheConfigBuilder::new().build(), backend);
```

`install_backend` skips the `CacheBackendKind` resolution path and
mounts the supplied backend directly. The resulting `CacheManager`
honours the rest of the supplied `CacheConfig` (TTL defaults, prefix,
tracking flags).
