//! Integration coverage for `#[requires_roles(...)]`.
//!
//! Proves the authorization check the macro inserts runs before the rest of
//! the function body — an unauthorized principal never reaches the body at
//! all — and that an authorized principal lets the body run normally.

use std::sync::atomic::{AtomicU32, Ordering};

use pharos_app::Authorize;

// A minimal bitset, standing in for whatever role-set type a host
// application actually uses — `Authorize<R>` has no opinion on its shape.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Roles(u8);

#[derive(Clone, Copy)]
enum Role {
    Reader,
    Writer,
}

impl std::ops::BitOr for Role {
    type Output = Roles;
    fn bitor(self, rhs: Role) -> Roles {
        Roles(1 << self as u8 | 1 << rhs as u8)
    }
}

#[derive(Debug)]
struct Forbidden;

impl std::fmt::Display for Forbidden {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "forbidden")
    }
}

impl std::error::Error for Forbidden {}

/// Test stand-in for an app's authenticated-user extractor (e.g. `AuthUser`).
struct Principal {
    granted: Roles,
}

impl Authorize<Roles> for Principal {
    type Error = Forbidden;

    fn authorize(&self, required: Roles) -> Result<(), Forbidden> {
        if self.granted.0 & required.0 == required.0 {
            Ok(())
        } else {
            Err(Forbidden)
        }
    }
}

#[pharos_macros::requires_roles(Role::Reader | Role::Writer, principal = user)]
fn handle(user: Principal, ran: &AtomicU32) -> Result<(), Forbidden> {
    ran.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[test]
fn authorized_principal_reaches_the_body() {
    let ran = AtomicU32::new(0);
    let user = Principal {
        granted: Role::Reader | Role::Writer,
    };

    handle(user, &ran).unwrap_or_else(|e| panic!("expected Ok, got {e}"));
    assert_eq!(ran.load(Ordering::SeqCst), 1);
}

#[test]
fn unauthorized_principal_never_reaches_the_body() {
    let ran = AtomicU32::new(0);
    let user = Principal {
        granted: Roles(0), // no roles granted at all
    };

    let Err(Forbidden) = handle(user, &ran) else {
        panic!("expected the authorization check to fail before the body ran");
    };
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "the body must not run when authorization fails"
    );
}
