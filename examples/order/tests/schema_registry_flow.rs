use pharos_app::{EventSchema, SchemaRegistry};
use pharos_memory::InMemorySchemaRegistry;

#[tokio::test]
async fn schema_registry_versions_order_integration_event_contracts()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = InMemorySchemaRegistry::new();

    registry
        .register(EventSchema::new(
            "OrderConfirmed",
            1,
            "json-schema",
            r#"{
                "type": "object",
                "required": ["order_id", "total_cents"],
                "properties": {
                    "order_id": { "type": "string", "format": "uuid" },
                    "total_cents": { "type": "integer", "minimum": 0 }
                }
            }"#,
        ))
        .await?;
    registry
        .register(EventSchema::new(
            "OrderConfirmed",
            2,
            "json-schema",
            r#"{
                "type": "object",
                "required": ["order_id", "total_cents", "currency"],
                "properties": {
                    "order_id": { "type": "string", "format": "uuid" },
                    "total_cents": { "type": "integer", "minimum": 0 },
                    "currency": { "type": "string" }
                }
            }"#,
        ))
        .await?;

    let version_1 = registry
        .get("OrderConfirmed", 1)
        .await?
        .ok_or("expected schema version 1")?;
    assert_eq!(version_1.version, 1);
    assert_eq!(version_1.format, "json-schema");

    let latest = registry
        .latest("OrderConfirmed")
        .await?
        .ok_or("expected latest schema")?;
    assert_eq!(latest.version, 2);
    assert!(latest.schema.contains("currency"));

    Ok(())
}
