//! Regression coverage for `#[derive(Command)]`'s `validate_input` bridge —
//! in particular, that it is generated for tuple-struct commands, not just
//! named-field ones.
//!
//! `has_garde_fields` used to inspect only `Fields::Named`, so a tuple-struct
//! command with `#[garde(...)]` rules compiled cleanly, derived
//! `garde::Validate` correctly, and then silently kept the trait's no-op
//! `validate_input` default — `dispatch` called it, got `Ok(())` regardless of
//! the field's value, and the handler ran on data garde itself would reject.

use garde::Validate;
use pharos_app::{CommandHandler, dispatch};
use pharos_macros::Command;

#[derive(Command, Validate)]
struct WithdrawNamed {
    #[garde(range(min = 1))]
    amount_cents: u32,
}

#[derive(Command, Validate)]
struct WithdrawTuple(#[garde(range(min = 1))] u32);

struct Handler;

impl CommandHandler<WithdrawNamed> for Handler {
    type Output = u32;
    type Error = std::convert::Infallible;
    async fn handle(&self, c: WithdrawNamed) -> Result<u32, Self::Error> {
        Ok(c.amount_cents)
    }
}

impl CommandHandler<WithdrawTuple> for Handler {
    type Output = u32;
    type Error = std::convert::Infallible;
    async fn handle(&self, c: WithdrawTuple) -> Result<u32, Self::Error> {
        Ok(c.0)
    }
}

#[tokio::test]
async fn a_named_field_command_is_rejected_by_dispatch() {
    let result = dispatch(&Handler, WithdrawNamed { amount_cents: 0 }).await;
    assert!(result.is_err(), "garde's `range(min = 1)` must reject 0");
}

#[tokio::test]
async fn a_tuple_struct_command_is_rejected_by_dispatch_too() {
    let result = dispatch(&Handler, WithdrawTuple(0)).await;
    assert!(
        result.is_err(),
        "a tuple-struct command must not bypass the same garde rule a named-field one enforces"
    );
}

#[tokio::test]
async fn valid_input_still_reaches_the_handler_in_both_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        dispatch(&Handler, WithdrawNamed { amount_cents: 500 }).await?,
        500
    );
    assert_eq!(dispatch(&Handler, WithdrawTuple(500)).await?, 500);
    Ok(())
}
