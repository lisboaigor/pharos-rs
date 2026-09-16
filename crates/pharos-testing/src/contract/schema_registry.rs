//! Conformance suite for [`SchemaRegistry`].
use pharos_app::{EventSchema, SchemaRegistry};

/// Runs the [`SchemaRegistry`] conformance suite against `registry`.
///
/// Covers register/get round-tripping by exact version, `latest` tracking
/// the highest registered version while every earlier version stays
/// retrievable by its own number, and a not-yet-registered event type/
/// version returning `None` rather than an error.
///
/// ```ignore
/// #[tokio::test]
/// async fn schema_registry_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::schema_registry::run(&registry).await
/// }
/// ```
pub async fn run<R>(registry: &R) -> Result<(), Box<dyn std::error::Error>>
where
    R: SchemaRegistry,
{
    assert!(registry.get("OrderPlaced", 1).await?.is_none());
    assert!(registry.latest("OrderPlaced").await?.is_none());

    registry
        .register(EventSchema::new("OrderPlaced", 1, "json-schema", "{}"))
        .await?;
    let fetched = registry
        .get("OrderPlaced", 1)
        .await?
        .ok_or("a registered schema must be gettable by its exact version")?;
    assert_eq!(fetched.format, "json-schema");

    let latest = registry
        .latest("OrderPlaced")
        .await?
        .ok_or("latest must return the only registered version")?;
    assert_eq!(latest.version, 1);

    registry
        .register(EventSchema::new(
            "OrderPlaced",
            2,
            "json-schema",
            "{\"v\":2}",
        ))
        .await?;
    let latest = registry
        .latest("OrderPlaced")
        .await?
        .ok_or("latest must return a version after a second register")?;
    assert_eq!(
        latest.version, 2,
        "latest must track the highest registered version"
    );

    let still_v1 = registry
        .get("OrderPlaced", 1)
        .await?
        .ok_or("registering v2 must not make v1 unreachable")?;
    assert_eq!(still_v1.version, 1);

    Ok(())
}
