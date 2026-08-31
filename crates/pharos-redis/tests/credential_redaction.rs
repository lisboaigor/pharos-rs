//! Does the Redis broker keep its connection password out of `{:?}`?

#[test]
fn redis_broker_must_not_print_its_connection_password() {
    let Ok(broker) = pharos_redis::RedisMessageBroker::from_url("redis://:hunter2@localhost:6379")
    else {
        panic!("the url should parse");
    };
    let debug = format!("{broker:?}");
    println!("Redis Debug: {debug}");
    assert!(
        !debug.contains("hunter2"),
        "the derived Debug must not print the Redis password in cleartext"
    );
}
