/// Checks whether a "current principal" — whatever authenticated-user type
/// the host application already extracts (a JWT claims wrapper, a session
/// user, anything) — is allowed a set of roles.
///
/// `R` and the principal type are both opaque to this trait: `pharos-app`
/// has no opinion on their shape and no default implementation. An
/// application implements this once for its own principal type, bridging to
/// whatever authorization check it already has.
///
/// Pairs with [`pharos_macros::requires_roles`](https://docs.rs/pharos-macros),
/// an attribute macro applied directly to a route function that inserts an
/// `Authorize::authorize` call as the function's first statement — the role
/// requirement lives on the route, not on any `Command` it dispatches, since
/// the same command can be reachable through more than one route with
/// different access policies (or not be HTTP-reachable at all).
pub trait Authorize<R> {
    /// Error returned when authorization fails (e.g. mapped to a 403).
    type Error;

    /// Checks `self` against the required roles.
    fn authorize(&self, required: R) -> Result<(), Self::Error>;
}
