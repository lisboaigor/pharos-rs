//! Conformance suite for [`DeadLetterQueue`].
use pharos_app::{DeadLetterMessage, DeadLetterQueue, Message};

/// Runs the [`DeadLetterQueue`] conformance suite against `dlq`.
///
/// ```ignore
/// #[tokio::test]
/// async fn dead_letter_queue_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::dead_letter::run(&dlq).await
/// }
/// ```
pub async fn run<Q>(dlq: &Q) -> Result<(), Box<dyn std::error::Error>>
where
    Q: DeadLetterQueue,
{
    let message = DeadLetterMessage::new(
        Message::new("orders", b"payload".to_vec(), "application/json"),
        "handler panicked",
        3,
    );
    let id = message.id;
    dlq.dead_letter(message).await?;

    let listed = dlq.list(10).await?;
    let record = listed
        .iter()
        .find(|m| m.id == id)
        .ok_or("a dead-lettered message must appear in list")?;
    assert_eq!(record.reason, "handler panicked");
    assert_eq!(record.attempts, 3);

    for i in 0..3 {
        dlq.dead_letter(DeadLetterMessage::new(
            Message::new("orders", vec![i], "application/json"),
            "reason",
            1,
        ))
        .await?;
    }
    let limited = dlq.list(2).await?;
    assert!(limited.len() <= 2, "list must respect the requested limit");

    Ok(())
}
