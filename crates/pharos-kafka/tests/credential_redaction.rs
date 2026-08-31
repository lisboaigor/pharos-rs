//! Do the schema registry clients keep their basic-auth credentials out of
//! `{:?}`?

use pharos_kafka::{ApicurioSchemaRegistry, ConfluentSchemaRegistry};

#[test]
fn schema_registry_clients_must_not_print_their_api_secret() {
    let confluent = ConfluentSchemaRegistry::new("https://psrc-1.confluent.cloud")
        .with_basic_auth("SR_API_KEY", "sr-secret-do-not-log");
    let apicurio = ApicurioSchemaRegistry::new("https://registry.internal")
        .with_basic_auth("svc", "apicurio-secret-do-not-log");

    let confluent_debug = format!("{confluent:?}");
    let apicurio_debug = format!("{apicurio:?}");
    println!("Confluent Debug: {confluent_debug}");
    println!("Apicurio  Debug: {apicurio_debug}");

    assert!(
        !confluent_debug.contains("sr-secret-do-not-log"),
        "the derived Debug must not print the registry API secret in cleartext"
    );
    assert!(
        !apicurio_debug.contains("apicurio-secret-do-not-log"),
        "the derived Debug must not print the registry API secret in cleartext"
    );
}
