//! Adapter conformance kit.
//!
//! Implementing `Repository<A>`, `OutboxRepository`, `InboxStore`, and the
//! rest of Pharos's storage/messaging traits is easy to get *compiling* and
//! easy to get subtly wrong: an optimistic-concurrency check that races a
//! concurrent writer, an outbox claim two dispatchers can both win, a saga
//! `save` that upserts unconditionally. The framework's own adapters
//! (`pharos-memory`, and historically `pharos-postgres`) earned every one of
//! these contracts the hard way, from a real bug. This module is that
//! experience turned into something you run against your own adapter
//! instead of discovering the hard way yourself.
//!
//! Each submodule exercises one trait's full contract — not just its happy
//! path — as a plain `async fn` you call from a `#[tokio::test]` in your
//! adapter crate:
//!
//! ```ignore
//! #[tokio::test]
//! async fn repository_contract() -> Result<(), Box<dyn std::error::Error>> {
//!     let repo = YourRepository::<pharos_testing::contract::ContractAggregate>::new(pool.clone());
//!     pharos_testing::contract::repository::run(&repo).await
//! }
//! ```
//!
//! A contract violation surfaces as an ordinary test failure (an `assert!`
//! or `assert_eq!` panic with a message naming the violated guarantee), not
//! as a special error type — the same as any other test in your suite.
//!
//! Requires the `contract` feature.
pub mod dead_letter;
pub mod event_store;
pub mod fixtures;
pub mod inbox;
pub mod messaging;
pub mod outbox;
pub mod repository;
pub mod saga;
pub mod schema_registry;
pub mod transactional;

pub use fixtures::{ContractAggregate, ContractEvent};
