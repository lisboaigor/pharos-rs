//! Schema evolution for integration events: JSON upcasters.
//!
//! An event's `schema_version` only helps if something consumes it. This
//! module closes that loop for JSON envelopes: a [`JsonUpcasterRegistry`]
//! holds payload transformations keyed by `(event_type, from_version)`, and
//! [`VersionedJsonCodec`] applies them during decode, stepping the payload one
//! version at a time until no further upcaster applies.
//!
//! Producers keep publishing whatever version they know; consumers register
//! the chain of upcasts they need and always deserialize the latest shape:
//!
//! ```
//! use pharos_app::{IntegrationEvent, MessageCodec};
//! use pharos_app::upcast::{JsonUpcasterRegistry, VersionedJsonCodec};
//! use serde::{Deserialize, Serialize};
//! use serde_json::json;
//!
//! #[derive(Debug, Serialize, Deserialize, PartialEq)]
//! struct OrderPlacedV2 {
//!     quantity: u32, // renamed from `qty` in v1
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let registry = JsonUpcasterRegistry::new().with_upcaster(
//!     "OrderPlaced",
//!     1, // upcasts v1 → v2
//!     |mut payload| -> Result<serde_json::Value, &'static str> {
//!         if let Some(qty) = payload.get("qty").cloned() {
//!             let obj = payload
//!                 .as_object_mut()
//!                 .ok_or("OrderPlaced v1 payload must be an object")?;
//!             obj.remove("qty");
//!             obj.insert("quantity".into(), qty);
//!         }
//!         Ok(payload)
//!     },
//! );
//! let codec = VersionedJsonCodec::new(registry);
//!
//! // A v1 event arrives on the wire…
//! let v1 = IntegrationEvent::new("OrderPlaced", 1, "orders", json!({ "qty": 3 }));
//! let wire = codec.encode(&v1)?;
//!
//! // …and decodes as the current shape, with the version stepped forward.
//! let current: IntegrationEvent<OrderPlacedV2> = codec.decode(&wire)?;
//! assert_eq!(current.schema_version, 2);
//! assert_eq!(current.payload, OrderPlacedV2 { quantity: 3 });
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;

use crate::integration_event::IntegrationEvent;
use crate::serialization::{EventSerializer, JsonEventSerializer, MessageCodec, SerializedEvent};

/// Error produced while upcasting or (de)serializing a versioned envelope.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UpcastError {
    /// JSON serialization/deserialization failed.
    #[error("json serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    /// The wire bytes are not a JSON envelope with `event_type` and
    /// `schema_version` fields.
    #[error("envelope is not a JSON object with event_type and schema_version")]
    MalformedEnvelope,
    /// A registered upcaster rejected the payload.
    #[error("upcast of '{event_type}' from version {from_version} failed: {source}")]
    Transform {
        /// Logical event type being upcast.
        event_type: String,
        /// Version the failing upcaster consumes.
        from_version: u32,
        /// Error returned by the upcaster.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

type UpcastFn = Box<
    dyn Fn(Value) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>>
        + Send
        + Sync,
>;

/// Registry of JSON payload upcasters keyed by `(event_type, from_version)`.
///
/// Each upcaster transforms a payload from `from_version` to
/// `from_version + 1`. Chains compose automatically: registering upcasters for
/// versions 1 and 2 lets a v1 payload decode as v3.
#[derive(Default)]
pub struct JsonUpcasterRegistry {
    upcasters: HashMap<(String, u32), UpcastFn>,
}

impl std::fmt::Debug for JsonUpcasterRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonUpcasterRegistry")
            .field("registered", &self.upcasters.len())
            .finish()
    }
}

impl JsonUpcasterRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an upcaster that transforms `event_type` payloads from
    /// `from_version` to `from_version + 1`.
    ///
    /// The closure receives the event's `payload` JSON value and returns the
    /// upgraded payload. Any error aborts decoding with
    /// [`UpcastError::Transform`].
    pub fn with_upcaster<F, E>(
        mut self,
        event_type: impl Into<String>,
        from_version: u32,
        upcast: F,
    ) -> Self
    where
        F: Fn(Value) -> Result<Value, E> + Send + Sync + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        self.upcasters.insert(
            (event_type.into(), from_version),
            Box::new(move |payload| upcast(payload).map_err(Into::into)),
        );
        self
    }

    /// Returns the number of registered upcasters.
    pub fn len(&self) -> usize {
        self.upcasters.len()
    }

    /// Returns `true` when no upcaster is registered.
    pub fn is_empty(&self) -> bool {
        self.upcasters.is_empty()
    }

    fn get(&self, event_type: &str, from_version: u32) -> Option<&UpcastFn> {
        self.upcasters.get(&(event_type.to_string(), from_version))
    }
}

/// JSON [`MessageCodec`] that applies registered upcasters during decode.
///
/// Encoding is plain JSON (identical to [`JsonEventSerializer`]). Decoding
/// inspects the envelope's `event_type` and `schema_version`, applies every
/// matching upcaster in version order (stepping `schema_version` as it goes),
/// and only then deserializes the payload into `P` — so `P` always models the
/// **latest** schema.
pub struct VersionedJsonCodec {
    registry: JsonUpcasterRegistry,
}

impl VersionedJsonCodec {
    /// Creates a codec over an upcaster registry.
    pub fn new(registry: JsonUpcasterRegistry) -> Self {
        Self { registry }
    }

    /// Returns the underlying registry.
    pub fn registry(&self) -> &JsonUpcasterRegistry {
        &self.registry
    }
}

impl std::fmt::Debug for VersionedJsonCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionedJsonCodec")
            .field("registry", &self.registry)
            .finish()
    }
}

impl<P> MessageCodec<P> for VersionedJsonCodec
where
    P: Serialize + DeserializeOwned + 'static,
{
    type Error = UpcastError;

    fn encode(&self, event: &IntegrationEvent<P>) -> Result<SerializedEvent, Self::Error> {
        JsonEventSerializer
            .serialize(event)
            .map_err(|crate::serialization::EventSerializationError::Json(e)| UpcastError::Json(e))
    }

    fn decode(&self, wire: &SerializedEvent) -> Result<IntegrationEvent<P>, Self::Error> {
        let mut envelope: Value = serde_json::from_slice(&wire.payload)?;
        let obj = envelope
            .as_object_mut()
            .ok_or(UpcastError::MalformedEnvelope)?;

        let event_type = obj
            .get("event_type")
            .and_then(Value::as_str)
            .ok_or(UpcastError::MalformedEnvelope)?
            .to_string();
        let mut version = obj
            .get("schema_version")
            .and_then(Value::as_u64)
            .ok_or(UpcastError::MalformedEnvelope)? as u32;

        while let Some(upcast) = self.registry.get(&event_type, version) {
            let payload = obj
                .remove("payload")
                .ok_or(UpcastError::MalformedEnvelope)?;
            let upgraded = upcast(payload).map_err(|source| UpcastError::Transform {
                event_type: event_type.clone(),
                from_version: version,
                source,
            })?;
            obj.insert("payload".to_string(), upgraded);
            version += 1;
            obj.insert("schema_version".to_string(), Value::from(version));
        }

        Ok(serde_json::from_value(envelope)?)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct V3 {
        quantity: u32,
        currency: String,
    }

    fn registry_v1_to_v3() -> JsonUpcasterRegistry {
        JsonUpcasterRegistry::new()
            // v1 → v2: rename `qty` to `quantity`.
            .with_upcaster("OrderPlaced", 1, |mut payload| {
                let Some(obj) = payload.as_object_mut() else {
                    return Err("payload must be an object");
                };
                if let Some(qty) = obj.remove("qty") {
                    obj.insert("quantity".into(), qty);
                }
                Ok(payload)
            })
            // v2 → v3: introduce `currency` with a default.
            .with_upcaster("OrderPlaced", 2, |mut payload| {
                let Some(obj) = payload.as_object_mut() else {
                    return Err("payload must be an object");
                };
                obj.entry("currency").or_insert(json!("BRL"));
                Ok(payload)
            })
    }

    #[test]
    fn upcasts_v1_through_the_chain_to_the_latest_shape() -> Result<(), Box<dyn std::error::Error>>
    {
        let codec = VersionedJsonCodec::new(registry_v1_to_v3());

        let v1 = IntegrationEvent::new("OrderPlaced", 1, "orders", json!({ "qty": 3 }));
        let wire = codec.encode(&v1)?;

        let decoded: IntegrationEvent<V3> = codec.decode(&wire)?;
        assert_eq!(decoded.schema_version, 3);
        assert_eq!(
            decoded.payload,
            V3 {
                quantity: 3,
                currency: "BRL".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn current_version_passes_through_untouched() -> Result<(), Box<dyn std::error::Error>> {
        let codec = VersionedJsonCodec::new(registry_v1_to_v3());

        let v3 = IntegrationEvent::new(
            "OrderPlaced",
            3,
            "orders",
            json!({ "quantity": 5, "currency": "USD" }),
        );
        let wire = codec.encode(&v3)?;

        let decoded: IntegrationEvent<V3> = codec.decode(&wire)?;
        assert_eq!(decoded.schema_version, 3);
        assert_eq!(decoded.payload.currency, "USD");
        Ok(())
    }

    #[test]
    fn other_event_types_are_not_upcast() -> Result<(), Box<dyn std::error::Error>> {
        let codec = VersionedJsonCodec::new(registry_v1_to_v3());

        #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
        struct Other {
            qty: u32,
        }

        let event = IntegrationEvent::new("OtherEvent", 1, "orders", json!({ "qty": 7 }));
        let wire = codec.encode(&event)?;

        let decoded: IntegrationEvent<Other> = codec.decode(&wire)?;
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded.payload, Other { qty: 7 });
        Ok(())
    }

    #[test]
    fn transform_failures_carry_event_type_and_version() {
        let registry = JsonUpcasterRegistry::new().with_upcaster("OrderPlaced", 1, |_payload| {
            Err::<Value, _>("cannot upgrade")
        });
        let codec = VersionedJsonCodec::new(registry);

        let v1 = IntegrationEvent::new("OrderPlaced", 1, "orders", json!({}));
        let Ok(wire) = MessageCodec::<Value>::encode(&codec, &v1) else {
            panic!("encoding a plain JSON envelope must succeed");
        };

        let result: Result<IntegrationEvent<Value>, _> = codec.decode(&wire);
        let Err(UpcastError::Transform {
            event_type,
            from_version,
            ..
        }) = result
        else {
            panic!("expected a transform error, got {result:?}");
        };
        assert_eq!(event_type, "OrderPlaced");
        assert_eq!(from_version, 1);
    }
}
