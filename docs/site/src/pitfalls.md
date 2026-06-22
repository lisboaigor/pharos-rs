# Pitfalls

The common-mistakes guide lives in [docs/guide/pitfalls.md](../../guide/pitfalls.md).

It covers the recurring traps and their fixes, including:

- publishing to a broker directly from a command handler
- assuming consumers never see duplicates
- ignoring `ConcurrencyConflict`
- losing the tenant across an async boundary
- enabling more features than you ship
