use pharos_app::{NoopUnitOfWork, UnitOfWork, UnitOfWorkError};

#[tokio::test]
async fn noop_unit_of_work_wraps_order_application_operation()
-> Result<(), Box<dyn std::error::Error>> {
    let unit_of_work = NoopUnitOfWork;

    let order_number = unit_of_work
        .run(|| async {
            // A real adapter would start a database transaction before this closure
            // and commit it after aggregate persistence plus outbox insert succeed.
            Ok::<_, UnitOfWorkError>("order-123".to_string())
        })
        .await?;

    assert_eq!(order_number, "order-123");

    Ok(())
}
