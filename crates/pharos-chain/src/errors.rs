//! Errors surfaced by the chain layer.

use thiserror::Error;

/// Error produced by the chain bridge and shared across the crate.
///
/// Port traits keep their own associated `Error` types (an adapter's error is
/// its own concern); `ChainError` covers the core's own failure modes — chiefly
/// serializing a [`ChainEvent`](crate::ChainEvent) into a broker message.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ChainError {
    /// A chain event could not be serialized into a message payload.
    #[error("failed to serialize chain event: {0}")]
    Serialization(#[from] serde_json::Error),
}
