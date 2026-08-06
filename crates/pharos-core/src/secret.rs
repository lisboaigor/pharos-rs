//! Envelope for sensitive values that must never reach a log.
//!
//! Commands and queries record their fields on the tracing span by default, so
//! the safety of a secret cannot rest on someone remembering to opt out: a
//! field added next month would be logged before anyone noticed. [`Secret`]
//! moves that guarantee into the type system — its `Debug` and `Display` write
//! a placeholder, so the value simply has no formatting path that reveals it.

use core::fmt;

/// What every formatting impl writes in place of the wrapped value.
pub const REDACTED: &str = "[redacted]";

/// Wraps a sensitive value (password, token, private key) so it cannot be
/// printed.
///
/// Reading the value requires naming the intent — [`Secret::expose`] or
/// [`Secret::into_inner`] — which makes the disclosure visible at the call site
/// and greppable across a codebase.
///
/// `Serialize` is deliberately **not** implemented: a command carrying a secret
/// should not be silently persisted to an outbox or echoed into a JSON body.
/// Serialize the fields that are safe to store instead.
///
/// ```
/// use pharos_core::Secret;
///
/// let senha = Secret::new(String::from("hunter2"));
/// assert_eq!(format!("{senha:?}"), "[redacted]");
/// assert_eq!(format!("{senha}"), "[redacted]");
/// assert_eq!(senha.expose(), "hunter2");
/// ```
pub struct Secret<T>(T);

impl<T> Secret<T> {
    /// Wraps a value.
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    /// Borrows the wrapped value.
    ///
    /// Named to stand out in review: every disclosure of a secret is one
    /// `expose()` at the call site.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Consumes the wrapper and returns the value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

// `Display` is redacted too: without it, `{}` would print a secret that `{:?}`
// refuses to, which is exactly the kind of asymmetry that leaks in a hurry.
impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl<T> From<T> for Secret<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T: Clone> Clone for Secret<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Default> Default for Secret<T> {
    fn default() -> Self {
        Self(T::default())
    }
}

#[cfg(feature = "serde")]
impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Secret<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_and_display_do_not_reveal_the_value() {
        let s = Secret::new("hunter2");
        assert_eq!(format!("{s:?}"), REDACTED);
        assert_eq!(format!("{s}"), REDACTED);
    }

    #[test]
    fn value_stays_reachable_through_expose() {
        let s = Secret::new(String::from("hunter2"));
        assert_eq!(s.expose(), "hunter2");
        assert_eq!(s.into_inner(), "hunter2");
    }

    /// The point of the type: nested in a struct, the container's derived
    /// `Debug` does not leak either — which is what protects the span built by
    /// `#[derive(Command)]`.
    #[test]
    fn does_not_leak_through_a_derived_debug() {
        #[derive(Debug)]
        #[expect(
            dead_code,
            reason = "read only by the derived Debug, which is what this test exercises"
        )]
        struct Credentials {
            username: String,
            password: Secret<String>,
        }

        let out = format!(
            "{:?}",
            Credentials {
                username: "admin".into(),
                password: Secret::new("hunter2".into()),
            }
        );
        assert!(out.contains("admin"));
        assert!(!out.contains("hunter2"));
    }
}
