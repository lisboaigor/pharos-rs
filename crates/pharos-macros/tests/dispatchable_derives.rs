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
async fn dispatch_runs_derived_command_handler() -> Result<(), Box<dyn std::error::Error>> {
    let out = dispatch(
        &CreateThingHandler,
        CreateThing {
            id: 40,
            count: 2,
            _ignored: "x".into(),
        },
    )
    .await?;
    assert_eq!(out, 42);
    Ok(())
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
async fn command_name_can_be_overridden() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(ArchiveThing::NAME, "thing.archived");
    let out = dispatch(&ArchiveThingHandler, ArchiveThing { id: 7 }).await?;
    assert_eq!(out, 7);
    Ok(())
}

// ── Command: `#[command(internal)]` sets INTERNAL_ONLY ───────────────────────

// A command with no `internal` marker inherits the trait default (`false`) and
// stays HTTP-reachable — asserted against `CreateThing`/`ArchiveThing` above.
const _: () = assert!(!CreateThing::INTERNAL_ONLY);
const _: () = assert!(!ArchiveThing::INTERNAL_ONLY);

// The derived `trace_span` reads every field, so `wager_id` is not dead code
// even though no test body touches it.
#[derive(Command)]
#[command(internal)]
struct ReleasePayout {
    wager_id: u64,
}

// The marker flips the generated const, which is what the `pharos-axum` HTTP
// entry points check to refuse the command. Combining it with `name` proves the
// two struct-level options coexist.
#[derive(Command)]
#[command(internal, name = "wager.refunded")]
struct RefundStakes {
    wager_id: u64,
}

#[test]
fn command_internal_marker_sets_internal_only() {
    const { assert!(ReleasePayout::INTERNAL_ONLY) };
    assert_eq!(ReleasePayout::NAME, "ReleasePayout");

    const { assert!(RefundStakes::INTERNAL_ONLY) };
    assert_eq!(RefundStakes::NAME, "wager.refunded");
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
async fn dispatch_runs_derived_query_handler() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(GetThing::NAME, "GetThing");
    let out = query_dispatch(&GetThingHandler, GetThing { id: 99 }).await?;
    assert_eq!(out, Some(99));
    Ok(())
}

// Asserts the generated `Query::Result` associated type matches `#[query(result)]`.
const _: fn() = || {
    fn assert_result<Q: Query<Result = Option<u64>>>() {}
    assert_result::<GetThing>();
};

// ── Span contents: recorded by default, `skip` opts out, `Secret` redacts ─────

#[derive(Command)]
struct PayInvoice {
    // No annotation: recorded via `Debug`, which is the whole point — nobody
    // has to remember anything for the command to be traceable.
    invoice_id: u64,
    // Wrapped rather than skipped: the value is unprintable by construction, so
    // a future edit cannot expose it.
    password: pharos_core::Secret<String>,
    // Opted out: a blob would drown the span. Never read precisely because it
    // is skipped — that absence is what the test asserts.
    #[trace(skip)]
    #[expect(dead_code, reason = "exists to prove #[trace(skip)] omits it")]
    payload: String,
}

struct PayInvoiceHandler;
impl CommandHandler<PayInvoice> for PayInvoiceHandler {
    type Output = ();
    type Error = std::convert::Infallible;

    async fn handle(&self, _cmd: PayInvoice) -> Result<(), Self::Error> {
        // The span is only observable from a log emitted inside the handler —
        // this stands in for a real handler's own logging.
        tracing::info!("paying");
        Ok(())
    }
}

#[tokio::test]
async fn span_records_every_field_unless_skipped() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = pharos_testing::TestSubscriber::new();
    let _guard = subscriber.install();

    dispatch(
        &PayInvoiceHandler,
        PayInvoice {
            invoice_id: 4242,
            password: pharos_core::Secret::new("hunter2".into()),
            payload: "a very large blob".into(),
        },
    )
    .await?;

    let lines = subscriber.lines();
    let span_line = lines
        .iter()
        .find(|l| l.contains("command.handle"))
        .ok_or("no line was emitted inside the command.handle span")?;

    // Recorded without any annotation at all — the behaviour this derive exists
    // to guarantee.
    assert!(
        span_line.contains("4242"),
        "an unannotated field should appear on the span, got:\n{span_line}"
    );
    // Present as evidence the field was filled, but never as the value.
    assert!(
        span_line.contains(pharos_core::REDACTED),
        "Secret should appear redacted, got:\n{span_line}"
    );
    assert!(
        !span_line.contains("hunter2"),
        "SECRET LEAKED onto the span:\n{span_line}"
    );
    assert!(
        !span_line.contains("a very large blob"),
        "a #[trace(skip)] field should not appear on the span, got:\n{span_line}"
    );
    Ok(())
}
