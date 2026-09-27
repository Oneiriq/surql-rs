//! Whether two access definitions grant the same access, with what the
//! engine redacts, fills in, or spells differently folded away.
//!
//! The engine echoes a `DEFINE ACCESS` with its secrets as `'[REDACTED]'`,
//! its durations normalised (`24h` as `1d`, `90m` as `1h30m`), the default
//! token duration (`1h`) written out, and a record access declared without
//! a verifier given one (`WITH JWT ALGORITHM HS512 KEY '[REDACTED]'`, a
//! random key). None of that is a difference.

use super::normalize::expr_eq;
use crate::schema::access::{AccessDefinition, AccessType, JwtConfig, RecordAccessConfig};

/// What the engine echoes in place of a secret.
const REDACTED: &str = "[REDACTED]";
/// The token duration the engine applies when none is declared.
const DEFAULT_TOKEN_DURATION: &str = "1h";
/// The algorithm of the verifier the engine gives a record access declared
/// without one.
const IMPLIED_RECORD_ALGORITHM: &str = "HS512";

/// Whether two access definitions grant the same access.
///
/// Compares the kind, the verifier (algorithm, key or JWKS url, audience,
/// issuer key), the signup / signin / `AUTHENTICATE` / `CONTEXT`
/// expressions (through [`normalize_expression`](super::normalize_expression))
/// and the durations by length. A `[REDACTED]` secret matches any secret:
/// the engine never shows one, so a changed key cannot be detected from its
/// echo.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::accesses_equal;
/// use surql::schema::{jwt_access, parse_access, record_access, JwtConfig, RecordAccessConfig};
///
/// let code = jwt_access("api", JwtConfig::hs256("secret")).with_session("24h");
/// let echo = parse_access(
///     "api",
///     "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' \
///      WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION 1d",
/// )
/// .unwrap();
/// assert!(accesses_equal(&code, &echo));
/// assert!(!accesses_equal(&code.clone().with_session("12h"), &echo));
/// ```
#[must_use]
pub fn accesses_equal(a: &AccessDefinition, b: &AccessDefinition) -> bool {
    a.name == b.name
        && a.access_type == b.access_type
        && durations_equal(
            a.duration_session.as_deref(),
            b.duration_session.as_deref(),
            None,
        )
        && durations_equal(
            a.duration_token.as_deref(),
            b.duration_token.as_deref(),
            Some(DEFAULT_TOKEN_DURATION),
        )
        && expr_eq(a.authenticate.as_deref(), b.authenticate.as_deref())
        && expr_eq(a.context.as_deref(), b.context.as_deref())
        && match a.access_type {
            AccessType::Jwt => match (&a.jwt, &b.jwt) {
                (Some(x), Some(y)) => jwt_equal(x, y),
                (x, y) => x == y,
            },
            AccessType::Record => records_equal(a.record.as_ref(), b.record.as_ref()),
        }
}

fn records_equal(a: Option<&RecordAccessConfig>, b: Option<&RecordAccessConfig>) -> bool {
    let empty = RecordAccessConfig::default();
    let (a, b) = (a.unwrap_or(&empty), b.unwrap_or(&empty));
    let verifier = |r: &RecordAccessConfig| r.jwt.clone().filter(|jwt| !is_implied(jwt));
    expr_eq(a.signup.as_deref(), b.signup.as_deref())
        && expr_eq(a.signin.as_deref(), b.signin.as_deref())
        && match (verifier(a), verifier(b)) {
            (Some(x), Some(y)) => jwt_equal(&x, &y),
            (x, y) => x == y,
        }
}

/// The verifier the engine gives a record access declared without one.
fn is_implied(jwt: &JwtConfig) -> bool {
    jwt.algorithm.eq_ignore_ascii_case(IMPLIED_RECORD_ALGORITHM)
        && jwt.key.as_deref() == Some(REDACTED)
        && jwt.url.is_none()
        && jwt.audience.is_empty()
        && jwt.issuer.is_none()
}

fn jwt_equal(a: &JwtConfig, b: &JwtConfig) -> bool {
    // A JWKS verifier takes its algorithm from the key set; the algorithm
    // only matters once a key (verifying or issuing) names it.
    let keyed = |j: &JwtConfig| j.key.is_some() || j.issuer.is_some();
    let algorithm = !(keyed(a) || keyed(b)) || a.algorithm.eq_ignore_ascii_case(&b.algorithm);
    algorithm
        && secrets_equal(a.key.as_deref(), b.key.as_deref())
        && a.url == b.url
        && a.audience == b.audience
        && secrets_equal(a.issuer.as_deref(), b.issuer.as_deref())
}

/// Two secrets are equal when they are, or when either is redacted.
fn secrets_equal(a: Option<&str>, b: Option<&str>) -> bool {
    match (a, b) {
        (Some(REDACTED), Some(_)) | (Some(_), Some(REDACTED)) => true,
        (a, b) => a == b,
    }
}

/// Whether two durations are the same length, an unset one taking
/// `default` and `NONE` meaning no limit.
fn durations_equal(a: Option<&str>, b: Option<&str>, default: Option<&str>) -> bool {
    let length = |d: Option<&str>| {
        d.or(default)
            .filter(|d| !d.trim().eq_ignore_ascii_case("NONE"))
            .map(|d| duration_nanos(d).ok_or_else(|| d.trim().to_string()))
    };
    length(a) == length(b)
}

/// The length of a SurrealQL duration literal (`1h30m`, `4w2d`, `250ms`) in
/// nanoseconds, or `None` when it is not one.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::duration_nanos;
///
/// assert_eq!(duration_nanos("24h"), duration_nanos("1d"));
/// assert_eq!(duration_nanos("90m"), duration_nanos("1h30m"));
/// assert_eq!(duration_nanos("1s"), Some(1_000_000_000));
/// assert_eq!(duration_nanos("soon"), None);
/// ```
#[must_use]
pub fn duration_nanos(text: &str) -> Option<u128> {
    const UNITS: &[(&str, u128)] = &[
        ("ns", 1),
        ("us", 1_000),
        ("µs", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60 * 1_000_000_000),
        ("h", 3_600 * 1_000_000_000),
        ("d", 86_400 * 1_000_000_000),
        ("w", 7 * 86_400 * 1_000_000_000),
        ("y", 365 * 86_400 * 1_000_000_000),
    ];
    let mut rest = text.trim();
    if rest.is_empty() {
        return None;
    }
    let mut total: u128 = 0;
    while !rest.is_empty() {
        let digits = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        let amount: u128 = rest.get(..digits)?.parse().ok()?;
        rest = rest.get(digits..)?;
        let unit_len = rest
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(rest.len());
        let unit = rest.get(..unit_len)?;
        let scale = UNITS.iter().find(|(u, _)| *u == unit)?.1;
        total = total.checked_add(amount.checked_mul(scale)?)?;
        rest = rest.get(unit_len..)?;
    }
    Some(total)
}
