//! Integration coverage for `#[derive(Command)]` / `#[derive(Query)]`.
//!
//! These derive the real `pharos-app` traits, so they prove the generated impls
//! satisfy the trait bounds, that `NAME` resolves (default and overridden), and
//! that `dispatch` runs the handler. Span *field contents* are asserted
//! end-to-end in the `order` example's instrumentation test.

use pharos_app::{Command, CommandHandler, Query, QueryHandler, dispatch, query_dispatch};
use pharos_macros::{Command, Query};

// ── Command: default NAME + `#[trace]` fields ────────────────────────────────

#[derive(Command)]
struct CreateThing {
    #[trace(display)]
    id: u64,
    #[trace]
    count: u32,
    _ignored: String,
}

struct CreateThingHandler;
impl CommandHandler<CreateThing> for CreateThingHandler {
    type Output = u64;
    type Error = std::convert::Infallible;

    async fn handle(&self, cmd: CreateThing) -> Result<u64, Self::Error> {
        Ok(cmd.id + cmd.count as u64)
    }
}

#[test]
fn command_name_defaults_to_type_name() {
    assert_eq!(CreateThing::NAME, "CreateThing");
}

#[tokio::test]
async fn dispatch_runs_derived_command_handler() {
    let out = dispatch(
        &CreateThingHandler,
        CreateThing {
            id: 40,
            count: 2,
            _ignored: "x".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(out, 42);
}

// ── Command: name override + no `#[trace]` fields (inherits default span) ─────

#[derive(Command)]
#[command(name = "thing.archived")]
struct ArchiveThing {
    id: u64,
}

struct ArchiveThingHandler;
impl CommandHandler<ArchiveThing> for ArchiveThingHandler {
    type Output = u64;
    type Error = std::convert::Infallible;

    async fn handle(&self, cmd: ArchiveThing) -> Result<u64, Self::Error> {
        Ok(cmd.id)
    }
}

#[tokio::test]
async fn command_name_can_be_overridden() {
    assert_eq!(ArchiveThing::NAME, "thing.archived");
    let out = dispatch(&ArchiveThingHandler, ArchiveThing { id: 7 })
        .await
        .unwrap();
    assert_eq!(out, 7);
}

// ── Query: result type + NAME + `#[trace]` field ─────────────────────────────

#[derive(Query)]
#[query(result = Option<u64>)]
struct GetThing {
    #[trace(display)]
    id: u64,
}

struct GetThingHandler;
impl QueryHandler<GetThing> for GetThingHandler {
    type Error = std::convert::Infallible;

    async fn handle(&self, q: GetThing) -> Result<Option<u64>, Self::Error> {
        Ok(Some(q.id))
    }
}

#[tokio::test]
async fn dispatch_runs_derived_query_handler() {
    assert_eq!(GetThing::NAME, "GetThing");
    let out = query_dispatch(&GetThingHandler, GetThing { id: 99 })
        .await
        .unwrap();
    assert_eq!(out, Some(99));
}

// Asserts the generated `Query::Result` associated type matches `#[query(result)]`.
const _: fn() = || {
    fn assert_result<Q: Query<Result = Option<u64>>>() {}
    assert_result::<GetThing>();
};
