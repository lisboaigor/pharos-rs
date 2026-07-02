use std::future::Future;

use thiserror::Error;

/// Transport protocol supported by a transport adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportProtocol {
    /// HTTP/REST-style transport.
    Http,
    /// gRPC transport.
    Grpc,
}

/// Transport endpoint descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportEndpoint {
    /// Protocol used by the endpoint.
    pub protocol: TransportProtocol,
    /// Endpoint name.
    pub name: String,
    /// Route, path, service method, or operation identifier.
    pub route: String,
}

impl TransportEndpoint {
    /// Creates a transport endpoint descriptor.
    pub fn new(
        protocol: TransportProtocol,
        name: impl Into<String>,
        route: impl Into<String>,
    ) -> Self {
        Self {
            protocol,
            name: name.into(),
            route: route.into(),
        }
    }
}

/// Generic transport request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportRequest {
    /// Target endpoint.
    pub endpoint: TransportEndpoint,
    /// Raw request body.
    pub body: Vec<u8>,
    /// Content type.
    pub content_type: String,
}

/// Generic transport response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportResponse {
    /// Status code or transport-specific status.
    pub status: u16,
    /// Raw response body.
    pub body: Vec<u8>,
    /// Content type.
    pub content_type: String,
}

/// Errors produced by transport adapters.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// Adapter failed to serve or call a request.
    #[error("transport failed: {0}")]
    Failed(String),
}

/// Minimal abstraction for HTTP/gRPC-style transport adapters.
pub trait TransportAdapter: Send + Sync + 'static {
    /// Protocol implemented by this adapter.
    fn protocol(&self) -> TransportProtocol;
    /// Sends a request through the adapter.
    fn send(
        &self,
        request: TransportRequest,
    ) -> impl Future<Output = Result<TransportResponse, TransportError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_transport_endpoint() {
        let endpoint = TransportEndpoint::new(TransportProtocol::Http, "create-order", "/orders");
        assert_eq!(endpoint.protocol, TransportProtocol::Http);
        assert_eq!(endpoint.name, "create-order");
        assert_eq!(endpoint.route, "/orders");
    }
}
