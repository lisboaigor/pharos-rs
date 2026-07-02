use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;

/// A pooled set of PostgreSQL connections shared by the adapters in this crate.
///
/// `Pool` is a `sqlx::PgPool`, which is cheap to clone (reference-counted)
/// and can be shared across every repository and store in an application.
pub type Pool = sqlx::PgPool;

/// Errors produced while building or using a connection [`Pool`].
#[derive(Debug, thiserror::Error)]
pub enum PgPoolError {
    /// The connection string could not be parsed or the pool could not be built.
    #[error("failed to configure connection pool: {0}")]
    Config(sqlx::Error),
    /// A database operation failed.
    #[error("database error: {0}")]
    Db(sqlx::Error),
}

impl From<sqlx::Error> for PgPoolError {
    fn from(e: sqlx::Error) -> Self {
        PgPoolError::Db(e)
    }
}

/// Builds a connection [`Pool`] from a libpq-style or URL connection string.
///
/// The pool is constructed lazily: connections are opened on first use.
/// `max_connections` bounds the maximum number of concurrent connections.
///
/// Accepts both URL-style (`postgres://user:pass@host/db`) and key-value style
/// (`host=localhost user=postgres dbname=app`).
///
/// This helper uses the default TLS configuration from `sqlx`. For custom TLS
/// setup, build a `PgPool` directly via `PgPoolOptions` and pass it to the
/// adapter constructors.
pub fn connect_pool(connection_string: &str, max_connections: u32) -> Result<Pool, PgPoolError> {
    let options = PgConnectOptions::from_str(connection_string).map_err(PgPoolError::Config)?;
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_lazy_with(options))
}
