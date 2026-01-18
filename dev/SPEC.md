Puffgres — Spec
1. Purpose

puffgres mirrors data from Postgres into turbopuffer namespaces using logical decoding (WAL). Users define mappings via “faux migrations” that specify:

source table (or view)

membership predicate (“which rows belong”)

id mapping (Postgres column → turbopuffer id)

columns to ingest

transform function (Rust or JS) that maps a row → turbopuffer document

turbopuffer schema hints and write behavior

backfill and long-running jobs (later milestones)

2. Non-goals (initial releases)

Full arbitrary SQL WHERE support without constraints

Exactly-once delivery (we provide effectively-once outcomes via idempotent writes + anti-regression conditions)

In-place turbopuffer attribute type changes or attribute deletions (handled via new-namespace migrations)

3. Terminology

Mapping: a relationship from one Postgres relation (table/view) into one turbopuffer namespace.

Membership predicate: rule that determines if a given row belongs in the namespace.

RowEvent: change event derived from WAL: Insert / Update / Delete (+ row images).

Checkpoint: persistent progress marker (LSN + per-mapping state).

DLQ: dead-letter queue for events that cannot be applied.

4. Supported execution modes
4.1 CDC (change-data-capture)

Consumes logical decoding stream and applies changes to turbopuffer.

v0: wal2json poll mode (SQL pg_logical_slot_get_changes)

v1: wal2json streaming replication protocol

v2: optional pgoutput adapter (native logical replication output)

4.2 Backfill

Scans source relation and writes full dataset to turbopuffer; resumable and progress-reporting.

5. Project layout on disk

puffgres init creates:

puffgres.toml — project settings (pg conn, tp config defaults)

puffgres/migrations/*.toml — mapping revisions

puffgres/transforms/*.{js,rs} — transform sources (optional)

puffgres/state/ — local state (dev) or external state store config (prod)

6. Faux migration format (TOML)

Each migration defines a mapping version.

6.1 Required fields

version: integer, monotonically increasing

mapping_name: stable identifier (e.g. pages_public)

namespace: turbopuffer namespace string

source: { schema, table } OR { schema, view }

id: { column, type } where type ∈ {uint,int,uuid,string}

columns: list of columns required for transform and membership evaluation

transform: transform spec (Rust or JS)

6.2 Membership predicate

membership block:

mode = "dsl": MVP DSL evaluable from row data (recommended for v0)

mode = "view": source is a view that already filters membership (preferred for “not hacky”)

mode = "lookup": membership checked by querying Postgres by PK (later)

DSL supports:

comparisons: =, !=

null checks: IS NULL, IS NOT NULL

boolean ops: AND, OR, NOT

literals: string, int/float/bool, null

6.3 Turbopuffer schema hints (optional)

tp_schema provides types/indexing hints. (Engine may also infer.) Examples: uuid, datetime, full-text config, filterable flags.

6.4 Write behavior

default: upsert_columns for inserts/updates; deletes for deletes

batching limits:

batch_max_rows

batch_max_bytes

flush_interval_ms

6.5 Anti-regression (ordering safety)

versioning.mode = "source_lsn": write __source_lsn attribute; conditional upsert ensures newer LSN wins

or versioning.mode = "column": use Postgres updated_at/version column

7. Transform interface

Transforms map RowEvent -> Action:

Actions:

Upsert(doc) — full document write

Delete(id) — delete document id

Skip — ignore event

Error(e) — handled via retry/DLQ rules

7.1 Rust transform

Configured by crate path / function symbol.

7.2 JS transform

Configured by file path + entry export. JS runtime must provide:

deterministic execution

bounded I/O: external calls only through ctx helpers (e.g. ctx.embed()), so it can be mocked/retried

8. CLI commands
8.1 puffgres init

Creates project structure and minimal config.

8.2 puffgres generate (interactive wizard)

Guides user through:

selecting table/view (introspects Postgres)

choosing id column (prefers PK/unique)

choosing membership mode (dsl/view/lookup)

selecting columns

selecting/creating transform (identity / stub)

selecting turbopuffer schema hints (optional)
Outputs new migration migrations/0001_*.toml and optional transform stub.

8.3 puffgres apply

validates migrations

optionally performs turbopuffer schema-only update

records applied migration version in state store

8.4 puffgres run

Starts CDC loop.
Options:

--slot <name> / --create-slot

--poll (v0 poll mode)

--from-lsn <lsn> (expert)

--strict (do not advance checkpoint past DLQ events)

8.5 puffgres backfill

Scans source relation, applies transform, batches writes, resumable.
Shows progress: rows processed, rows/sec, last cursor, retry/DLQ counts.

8.6 puffgres status

Shows:

current LSN checkpoint

lag (LSN delta)

batches/sec, events/sec

DLQ counts and last errors

8.7 puffgres dlq …

dlq list

dlq show <id>

dlq retry <id|all>

9. Failure semantics
9.1 Delivery

At-least-once processing from WAL to turbopuffer.

Achieve effectively-once outcomes via:

idempotent upserts/deletes

anti-regression conditional writes

9.2 Retries

Retry on:

network errors

429 throttling

5xx
Do not retry on:

schema/type validation errors (permanent)

transform exceptions (unless configured)

9.3 DLQ

Permanent failures go to DLQ with:

lsn, mapping version, raw event, error, retry_count

10. Observability & progress reporting

CDC: events/sec, lag, last flush time, retries

Backfill: rows processed / estimate, rows/sec, current cursor, write latency histogram (local)

Optional metrics export (Prometheus) later