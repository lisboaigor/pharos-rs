//! A process profile that makes "this binary never writes" a property the
//! compiler checks, not a convention someone has to remember.
//!
//! [`#[command(internal)]`](pharos_app::Command::INTERNAL_ONLY) and
//! [`run_command`](crate::run_command)'s guard protect *individual*
//! commands from being reachable over HTTP; neither says anything about a
//! whole process. The only way to guarantee "this binary has no write
//! surface at all" has always been "don't write the code that registers a
//! command route" — true, but unverified: nothing stops the next commit
//! from adding one by accident, and nothing about the binary itself records
//! that the property was ever intended.
//!
//! [`ProcessProfile`] carries that intent in its type. A [`ReadOnly`]
//! profile has no method that accepts a command route — not a runtime
//! check that rejects one, an absence, so a command route slipped into a
//! query-only binary is a compile error naming the exact line, not a
//! behavior discovered in production.

use std::marker::PhantomData;

use axum::Router;

/// Marker: this [`ProcessProfile`] may register command (write) routes.
#[derive(Debug, Clone, Copy)]
pub struct ReadWrite(());

/// Marker: this [`ProcessProfile`] can never register a command route —
/// [`ProcessProfile::command_routes`] does not exist for
/// `ProcessProfile<ReadOnly, _>`, so attempting to call it is a compile
/// error, not a runtime rejection.
#[derive(Debug, Clone, Copy)]
pub struct ReadOnly(());

/// Router builder whose type records whether it may carry command routes.
///
/// `S` is the shared application state, exactly as in [`axum::Router<S>`] —
/// [`Self::query_routes`] and [`Self::command_routes`] each take a
/// sub-router already built (and typically already `with_state`-applied)
/// the way [`run_query`](crate::run_query)/[`run_command`](crate::run_command)
/// routes normally are, and merge it in.
///
/// ```
/// use axum::{Router, routing::get};
/// use pharos_axum::profile::ProcessProfile;
///
/// async fn health() -> &'static str { "ok" }
///
/// // A read-only process: only `query_routes` exists on this type.
/// let profile = ProcessProfile::read_only()
///     .query_routes(Router::new().route("/health", get(health)));
/// let _router: Router = profile.into_router();
/// ```
///
/// Registering a command route on a read-only profile does not compile:
///
/// ```compile_fail
/// use axum::Router;
/// use pharos_axum::profile::ProcessProfile;
///
/// let profile = ProcessProfile::read_only();
/// // error[E0599]: no method named `command_routes` found for struct
/// // `ProcessProfile<ReadOnly>` — the method exists only on
/// // `ProcessProfile<ReadWrite>`.
/// let profile = profile.command_routes(Router::new());
/// ```
pub struct ProcessProfile<Mode, S = ()> {
    router: Router<S>,
    _mode: PhantomData<Mode>,
}

impl<S> ProcessProfile<ReadWrite, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// A profile that may register both query and command routes — the
    /// unrestricted default, equivalent to building an [`axum::Router`]
    /// directly. Reach for [`Self::read_only`] instead when a binary's
    /// write surface should be a compile-time guarantee rather than an
    /// intention.
    pub fn read_write() -> Self {
        Self {
            router: Router::new(),
            _mode: PhantomData,
        }
    }

    /// Merges in routes that issue commands (writes).
    ///
    /// Only defined for `ProcessProfile<ReadWrite, _>` — a
    /// `ProcessProfile<ReadOnly, _>` has no method by this name, so a
    /// command route added to a read-only profile fails to compile instead
    /// of silently becoming reachable.
    pub fn command_routes(mut self, router: Router<S>) -> Self {
        self.router = self.router.merge(router);
        self
    }
}

impl<S> ProcessProfile<ReadOnly, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// A profile that can never register a command route. Build the
    /// process's write surface, if it needs one, as a separate
    /// `ProcessProfile<ReadWrite, _>` — typically a different binary or
    /// entry point, so the read-only one keeps the guarantee for its
    /// entire lifetime, not just until someone edits it.
    pub fn read_only() -> Self {
        Self {
            router: Router::new(),
            _mode: PhantomData,
        }
    }
}

impl<Mode, S> ProcessProfile<Mode, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Merges in read-only routes. Available on either profile.
    pub fn query_routes(mut self, router: Router<S>) -> Self {
        self.router = self.router.merge(router);
        self
    }

    /// Returns the assembled router.
    pub fn into_router(self) -> Router<S> {
        self.router
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use axum::routing::{get, post};
    use tower::ServiceExt;

    use super::*;

    async fn ok() -> &'static str {
        "ok"
    }

    #[tokio::test]
    async fn read_write_serves_both_query_and_command_routes() {
        let router = ProcessProfile::read_write()
            .query_routes(Router::new().route("/query", get(ok)))
            .command_routes(Router::new().route("/command", post(ok)))
            .into_router();

        for (method, path) in [(Method::GET, "/query"), (Method::POST, "/command")] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .expect("request builds"),
                )
                .await
                .expect("router must respond");
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn read_only_serves_query_routes() {
        let router = ProcessProfile::read_only()
            .query_routes(Router::new().route("/query", get(ok)))
            .into_router();

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/query")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("router must respond");
        assert_eq!(response.status(), StatusCode::OK);
    }
}
