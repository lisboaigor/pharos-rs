# RFC Process

Use an RFC when a change does at least one of these things:

- adds a new top-level crate
- changes a public trait, derive macro, or feature flag
- introduces a new persistence or messaging model
- changes the expected way users compose Pharos crates

## Workflow

1. Copy `docs/rfc/0000-template.md` to `docs/rfc/NNNN-short-title.md`.
2. Fill in motivation, guide-level explanation, reference-level explanation, drawbacks, and alternatives.
3. Open a pull request with the RFC and mark it clearly as an RFC.
4. Merge the RFC before, or alongside, the implementation.

Critical correctness or security fixes can land first and be documented retroactively.
