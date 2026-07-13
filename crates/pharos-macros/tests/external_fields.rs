//! Integration coverage for `#[external_fields]` + `#[external]`.
//!
//! Proves the marked field is exempt from `Deserialize`'s body requirement
//! (missing from the JSON still deserializes, via `Default`) while an
//! unmarked sibling field stays required as normal.

use pharos_macros::Command;
use serde::Deserialize;
use uuid::Uuid;

#[pharos_macros::external_fields]
#[derive(Debug, Command, Deserialize)]
struct ConfirmOrder {
    #[external]
    order_id: Uuid,
    note: String,
}

#[test]
fn external_field_is_not_required_in_the_body() -> Result<(), Box<dyn std::error::Error>> {
    let cmd: ConfirmOrder = serde_json::from_str(r#"{"note": "ship it"}"#)?;
    assert_eq!(cmd.order_id, Uuid::nil());
    assert_eq!(cmd.note, "ship it");
    Ok(())
}

#[test]
fn external_field_present_in_the_body_is_still_ignored() -> Result<(), Box<dyn std::error::Error>> {
    // skip_deserializing means a client-supplied value never overrides the
    // default — the handler is the only thing allowed to set the real id.
    let raw = format!(r#"{{"order_id": "{}", "note": "ship it"}}"#, Uuid::new_v4());
    let cmd: ConfirmOrder = serde_json::from_str(&raw)?;
    assert_eq!(cmd.order_id, Uuid::nil());
    Ok(())
}

#[test]
fn unmarked_required_field_still_required() {
    let Err(err) = serde_json::from_str::<ConfirmOrder>(r#"{}"#) else {
        panic!("expected deserialization to fail without `note`");
    };
    assert!(err.to_string().contains("note"));
}
