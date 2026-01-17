Puffgres — Agent Issues (Parallelizable)

Each issue is scoped so a coding agent can complete it with minimal coordination. Target: small PRs, tests-first, no refactors.

Issue 1 — Workspace scaffold + CI + fixtures harness

Goal: Create Rust workspace and testing harness that all other issues build on.

Acceptance criteria

cargo test passes in CI

puffgres-cli binary builds

tests/fixtures/*.json runner exists (even if minimal)

repo includes SPEC.md, ARCHITECTURE.md, AGENTS.md (agent rules)

Issue 2 — Migration/config parsing + validation

Goal: Implement TOML parsing and validation for migrations.

Acceptance criteria

parse migration file into Mapping struct

validation errors are actionable (missing id column, bad predicate syntax, etc.)

unit tests for parsing + validation

Issue 3 — Predicate DSL parser + evaluator

Goal: MVP membership predicate DSL.

Acceptance criteria

supports =, !=, IS NULL, IS NOT NULL, AND/OR/NOT, literals

evaluates against RowEvent new/old maps

property-based or table-driven unit tests

Issue 4 — Core engine: routing + batching + request builder

Goal: Turn RowEvent stream into turbopuffer write requests (no network).

Acceptance criteria

routes events to correct mapping(s) by source relation

builds per-namespace batches honoring size limits

outputs a request struct suitable for TP adapter

golden tests from fixtures

Issue 5 — Turbopuffer client abstraction + adapter

Goal: Implement TurbopufferClient trait and one real adapter (wrapping an existing Rust client if desired) plus a mock.

Acceptance criteria

mock client captures writes for tests

retry policy scaffolding present (can be stubbed)

integration-ish test verifies batching → adapter calls

Issue 6 — Rust transform interface + identity transform

Goal: Transform pipeline without JS.

Acceptance criteria

Transformer trait

identity transform: doc is selected columns

transform errors produce Action::Error

unit tests for transform plumbing

Issue 7 — JS transform adapter (feature flag)

Goal: Optional JS transforms with deterministic execution.

Acceptance criteria

--features js enables JS transforms

loads module, calls entry function

provides ctx with stubbed helpers

tests with a tiny JS transform fixture

Issue 8 — Postgres introspection for generate wizard

Goal: puffgres generate can list tables, columns, PK/unique candidates.

Acceptance criteria

interactive wizard (basic) works against a live Postgres

writes migration + optional stub transform file

includes at least one integration test (can be ignored in CI if docker not available; but preferred to run)

Issue 9 — wal2json poll-mode CDC adapter

Goal: Read changes via pg_logical_slot_get_changes and produce RowEvents.

Acceptance criteria

can create slot, poll changes, parse wal2json v2

integration test: do INSERT/UPDATE/DELETE and assert produced RowEvents

Issue 10 — End-to-end poll-mode: PG → engine → mock TP

Goal: Full pipeline with mock turbopuffer.

Acceptance criteria

run command consumes changes and issues writes

checkpoint stored in sqlite state store

restart test: no missing events; duplicates don’t regress due to versioning attribute logic

Issue 11 — Backfill implementation + progress UI

Goal: puffgres backfill scans relation and writes in batches.

Acceptance criteria

resumable cursor

progress output (rows/sec, processed, cursor)

integration test on small table

Issue 12 — DLQ + puffgres dlq commands

Goal: Persist and inspect permanent failures.

Acceptance criteria

sqlite-backed DLQ

dlq list/show/retry

tests for classification (retryable vs permanent)

Issue 13 — wal2json streaming replication adapter

Goal: Streaming logical replication with correct acking.

Acceptance criteria

can start from checkpoint LSN

only ack after successful write+state

restart integration test

Issue 14 (Future) — pgoutput adapter

Goal: Add pgoutput as another input adapter.

Acceptance criteria

produces identical RowEvent model

end-to-end parity tests against wal2json fixtures