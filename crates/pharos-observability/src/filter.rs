//! The log filter, with the framework's own spans kept enabled.
//!
//! `tracing`'s `info_span!` takes its target from the module where the macro is
//! *written*, not where it runs. Every span this framework builds on an
//! application's behalf therefore carries a `pharos_*` target, and a perfectly
//! ordinary `RUST_LOG=my_app=info` filters all of them out.
//!
//! Nothing reports that. The request span vanishes, so every event inside it
//! loses method, URI, trace id and user; the `event_handler` spans vanish, so a
//! whole cross-context cascade disappears from the trace. The logs still look
//! healthy — they are simply missing the structure that makes them useful.
//!
//! So the directives are added here, in code, rather than documented as
//! something to remember in `RUST_LOG`: a deployment that preserves an old
//! environment file would otherwise silently reintroduce the problem.

use tracing_subscriber::EnvFilter;

/// Targets whose spans an application almost never wants filtered out, because
/// the framework builds them on its behalf.
///
/// `pharos_messaging` is on the list for the outbox dispatcher's own span: it is
/// born in that crate, not in `pharos_app`, so leaving it out hides the relay
/// that turns a persisted event into its effects.
pub const PHAROS_TARGETS: &[&str] = &[
    "pharos_axum",
    "pharos_app",
    "pharos_messaging",
    "pharos_postgres",
];

/// Level applied to a framework target the caller did not mention.
const DEFAULT_LEVEL: &str = "info";

/// Builds the filter from `RUST_LOG` (falling back to `default_directives`),
/// then enables any framework target the result left out.
///
/// A target the caller *did* mention is left exactly as written, so
/// `RUST_LOG=pharos_app=debug` still means debug — the additions fill gaps, they
/// do not overrule.
pub fn build_filter(default_directives: &str) -> EnvFilter {
    EnvFilter::new(merge_directives(
        &std::env::var("RUST_LOG").unwrap_or_else(|_| default_directives.to_owned()),
    ))
}

/// Appends `target=info` for every framework target absent from `directives`.
fn merge_directives(directives: &str) -> String {
    let mut merged = directives.trim().to_owned();

    for target in PHAROS_TARGETS {
        if mentions_target(&merged, target) {
            continue;
        }
        if !merged.is_empty() {
            merged.push(',');
        }
        merged.push_str(target);
        merged.push('=');
        merged.push_str(DEFAULT_LEVEL);
    }
    merged
}

/// Whether any directive already addresses `target`.
///
/// A directive is `target=level`, `target[span{field}]=level`, or a bare level
/// with no target at all; only the part naming the target matters here.
fn mentions_target(directives: &str, target: &str) -> bool {
    directives.split(',').any(|directive| {
        let named = directive
            .split('=')
            .next()
            .unwrap_or_default()
            .split('[')
            .next()
            .unwrap_or_default()
            .trim();
        named == target
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_every_framework_target_when_absent() {
        let merged = merge_directives("my_app=info");
        for target in PHAROS_TARGETS {
            assert!(
                merged.contains(&format!("{target}=info")),
                "{target} missing from {merged}"
            );
        }
    }

    /// The whole reason this is not a blind `add_directive`: an explicit choice
    /// by the operator has to survive.
    #[test]
    fn never_overrules_a_target_the_caller_set() {
        let merged = merge_directives("my_app=info,pharos_app=debug");
        assert!(merged.contains("pharos_app=debug"));
        assert!(
            !merged.contains("pharos_app=info"),
            "an explicit directive was overruled: {merged}"
        );
    }

    #[test]
    fn does_not_duplicate_on_repeated_merges() {
        let once = merge_directives("my_app=info");
        assert_eq!(merge_directives(&once), once);
    }

    /// Span and field syntax still names the target before the bracket.
    #[test]
    fn recognises_span_scoped_directives() {
        let merged = merge_directives("pharos_axum[request]=trace");
        assert!(merged.contains("pharos_axum[request]=trace"));
        assert!(!merged.contains("pharos_axum=info"));
    }

    /// A bare level names no target, so every framework target is still missing.
    #[test]
    fn handles_a_bare_level() {
        let merged = merge_directives("warn");
        assert!(merged.starts_with("warn,"));
        assert!(merged.contains("pharos_app=info"));
    }

    #[test]
    fn produces_a_filter_the_subscriber_accepts() {
        // `EnvFilter::new` silently drops malformed directives, so the check
        // that matters is that a known target survives the round trip.
        let filter = EnvFilter::new(merge_directives("my_app=info"));
        assert!(filter.to_string().contains("pharos_app"));
    }
}
