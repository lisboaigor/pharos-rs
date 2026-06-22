use dashmap::DashMap;
use pharos_app::{EventSchema, SchemaRegistry, SchemaRegistryError};
use tracing::{Instrument, info_span};

/// In-memory schema registry for tests and local development.
#[derive(Debug, Default)]
pub struct InMemorySchemaRegistry {
    schemas: DashMap<(String, u32), EventSchema>,
}

impl InMemorySchemaRegistry {
    /// Creates an empty schema registry.
    pub fn new() -> Self {
        Self::default()
    }
}

impl SchemaRegistry for InMemorySchemaRegistry {
    async fn register(&self, schema: EventSchema) -> Result<(), SchemaRegistryError> {
        async move {
            self.schemas
                .insert((schema.event_type.clone(), schema.version), schema);
            Ok(())
        }
        .instrument(info_span!("schema_registry.in_memory.register"))
        .await
    }

    async fn get(
        &self,
        event_type: &str,
        version: u32,
    ) -> Result<Option<EventSchema>, SchemaRegistryError> {
        async move {
            Ok(self
                .schemas
                .get(&(event_type.to_string(), version))
                .map(|entry| entry.value().clone()))
        }
        .instrument(info_span!(
            "schema_registry.in_memory.get",
            event_type,
            version
        ))
        .await
    }

    async fn latest(&self, event_type: &str) -> Result<Option<EventSchema>, SchemaRegistryError> {
        async move {
            Ok(self
                .schemas
                .iter()
                .filter(|entry| entry.key().0 == event_type)
                .max_by_key(|entry| entry.key().1)
                .map(|entry| entry.value().clone()))
        }
        .instrument(info_span!("schema_registry.in_memory.latest", event_type))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn registers_and_finds_latest_schema() {
        let registry = InMemorySchemaRegistry::new();
        registry
            .register(EventSchema::new("OrderConfirmed", 1, "json-schema", "{}"))
            .await
            .unwrap();
        registry
            .register(EventSchema::new("OrderConfirmed", 2, "json-schema", "{}"))
            .await
            .unwrap();

        assert_eq!(
            registry
                .latest("OrderConfirmed")
                .await
                .unwrap()
                .unwrap()
                .version,
            2
        );
    }
}
