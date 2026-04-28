# Bitswarm Repo Instructions

Bitswarm is a standalone protocol/runtime project. Keep it product-neutral.

## Scope

- Bitswarm transfers immutable file trees with strict verification.
- Bitswarm must not carry application authority, optimizer state, leases, proposal packets, replay records, line-search records, commit records, or dense training updates.
- Keep public protocol identifiers product-neutral. Use `bitswarm-1.0-alpha.1` for the initial implementation target.

## Commit Gate

- Use small scoped commits.
- Add or update tests for every substantive behavior change.
- Update docs when public protocol behavior changes.
- Do not claim an item complete until review has checked bugs, regressions, missing tests, and operational risks.

## Protocol Quality

- Public schemas must reject unknown fields.
- Manifest generation must be deterministic.
- Cache promotion must happen only after complete verification.
- Corrupt or partial data must never be promoted.

