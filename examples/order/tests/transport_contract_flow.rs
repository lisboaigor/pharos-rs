use pharos_app::{
    TransportAdapter, TransportEndpoint, TransportError, TransportProtocol, TransportRequest,
    TransportResponse,
};

#[derive(Debug, Default)]
struct FakeHttpTransport;

impl TransportAdapter for FakeHttpTransport {
    fn protocol(&self) -> TransportProtocol {
        TransportProtocol::Http
    }

    async fn send(&self, request: TransportRequest) -> Result<TransportResponse, TransportError> {
        assert_eq!(request.endpoint.name, "confirm-order");
        assert_eq!(request.endpoint.route, "/orders/order-123/confirm");
        assert_eq!(request.content_type, "application/json");

        Ok(TransportResponse {
            status: 202,
            body: br#"{"accepted":true}"#.to_vec(),
            content_type: "application/json".to_string(),
        })
    }
}

#[tokio::test]
async fn transport_adapter_contract_models_http_order_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = FakeHttpTransport;
    let request = TransportRequest {
        endpoint: TransportEndpoint::new(
            TransportProtocol::Http,
            "confirm-order",
            "/orders/order-123/confirm",
        ),
        body: br#"{"command":"ConfirmOrder"}"#.to_vec(),
        content_type: "application/json".to_string(),
    };

    let response = transport.send(request).await?;

    assert_eq!(transport.protocol(), TransportProtocol::Http);
    assert_eq!(response.status, 202);
    assert_eq!(response.content_type, "application/json");

    Ok(())
}
