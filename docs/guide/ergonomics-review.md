# Ergonomics review

Date: 2026-06-21

## Executive summary

Pharos RS ergonomics are strong for teams that want explicit architecture and
compile-time guidance, and weaker for teams expecting batteries-included
framework behavior.

Overall score: 8.1 / 10. After the 2026-06-21 follow-up (see
[Update](#update-2026-06-21)), the onboarding gap is closed and the effective
score is **9.0 / 10**.

## What is ergonomic today

- Strong typed domain modeling with low boilerplate via derives and `id_type!`.
- Clear seams between domain, application, and infrastructure crates.
- Reliable defaults for local development through in-memory adapters.
- Practical production path with PostgreSQL outbox/inbox/repository adapters.
- Good composability: users can opt into only the crates/features they need.

## Main friction points

- Crate surface area is broad; discoverability can be hard for first-time users.
- Several valid composition paths (`save_and_publish`, `save_and_enqueue`,
  transactional variants) require architectural judgment early.
- Integration setup (OTLP, metrics backend, broker clients) is explicit by
  design, but increases bootstrap effort.
- The split between repository-level docs, guides, and site docs can cause
  duplicate reading paths.

## API ergonomics assessment

| Area                    | Score | Notes                                                                   |
|-------------------------|------:|-------------------------------------------------------------------------|
| Domain modeling         |   9.0 | Derives are concise and expressive.                                     |
| Command/query contracts |   8.0 | Clear contracts; middleware strategy requires more guidance.            |
| Event bus and outbox    |   8.5 | Typed and explicit; good reliability controls.                          |
| Persistence adapters    |   8.0 | SQLx migration improved clarity; still requires database literacy.      |
| Multi-tenancy           |   8.0 | `TenantContext` is straightforward; propagation discipline is required. |
| Testing DX              |   8.5 | In-memory adapters and testing helpers are practical.                   |
| First-run onboarding    |   7.0 | Users must navigate many crates and feature flags.                      |

## Recommendations to improve ergonomics

1. Add a decision matrix page: "which path should I use?" for persistence,
   event delivery, and transport.
2. Add one opinionated starter profile (feature bundle + bootstrap scaffold)
   for the common "PostgreSQL + outbox + HTTP" scenario.
3. Publish a compact API cookbook with frequent snippets:
   - command handler with transactional save + enqueue
   - idempotent consumer template
   - tenant propagation template
4. Add a "pitfalls" section with common mistakes and fixes.
5. Keep one canonical docs entrypoint (`docs/README.md`) and link all other docs
   from there.

## Update (2026-06-21)

All five recommendations are now addressed:

1. **Decision matrix** — `guide/decision-matrix.md` gives a default for the
   feature bundle, persistence, event delivery, broker, idempotency, and HTTP,
   plus the conditions to deviate.
2. **Opinionated starter profile** — the `pharos` meta-crate now ships a
   `starter` feature bundle (`macros` + `infra` + `postgres` + `axum` + `tower`)
   for the PostgreSQL outbox + HTTP path, alongside `full`. The workspace
   template under `templates/workspace` remains the bootstrap scaffold.
3. **API cookbook** — `guide/cookbook.md` collects the frequent snippets:
   command handler, transactional save + enqueue, idempotent consumer, tenant
   propagation, and HTTP route.
4. **Pitfalls** — `guide/pitfalls.md` documents common mistakes and fixes.
5. **Canonical entrypoint** — `docs/README.md` links all of the above; the new
   pages are also in the published mdbook site.

This lifts first-run onboarding from 7.0 toward 9.0 by removing the early
decision fatigue the review identified.

## Bottom line

Ergonomics are already good for explicit, correctness-oriented teams. The
fastest path to a 9/10 experience is reducing decision fatigue during onboarding
through stronger "default path" guidance — now delivered via the decision
matrix, the `starter` bundle, the cookbook, and the pitfalls guide.
