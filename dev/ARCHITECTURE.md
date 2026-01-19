ARCHITECTURE.md
Puffgres — Architecture
1. Principles

Adapter pattern: inputs (wal2json, pgoutput) and outputs (turbopuffer client) are replaceable adapters.

Pure core engine: core logic is deterministic and unit-testable with fixtures.

State + side effects isolated: checkpointing, DLQ, network I/O behind traits.

Small incremental milestones: always runnable end-to-end early.

2. High-level dataflow

Source Adapter produces RowEvent stream

Router selects mappings that match (by table + membership)

Transformer yields Action (Upsert/Delete/Skip/Error)

Batcher groups actions per namespace within size limits

Writer calls turbopuffer client with retries

StateStore records checkpoint and DLQ

3. Crate/workspace layout

puffgres-cli

clap CLI, wizard UI, progress rendering

puffgres-config

migration parsing/validation, schema types, predicate DSL parsing

puffgres-core

RowEvent, router, transform traits, batching logic, write request builder, anti-regression logic

puffgres-state

StateStore trait

implementations: in-memory (tests), sqlite (default), optional postgres/redis later

puffgres-tp

TurbopufferClient trait + adapter(s)

default adapter may wrap an existing Rust client crate

puffgres-pg

postgres adapters:

wal2json_poll

wal2json_stream

pgoutput_stream (future)

puffgres-js (feature-flag)

JS runtime embedding and transform adapter

4. Core types
4.1 RowEvent

op: Insert | Update | Delete

schema, table

new: map<string, Value> (nullable)

old: map<string, Value> (nullable; identity keys or old row)

lsn: monotonic source position

txid, timestamp (optional)

4.2 Action

Upsert { id, doc }

Delete { id }

Skip

Error { kind, message }

4.3 Mapping

migration-defined config: source relation, namespace, id mapping, membership, columns, transform, schema hints, versioning mode

5. Checkpointing

Checkpoint is persisted as:

per mapping: applied_lsn (or global minimum safe lsn)

optional: per mapping backfill cursor (pk, last_id)

Rule:

checkpoint only advances after successful write and state persistence.

6. Anti-regression strategy

Default is versioning.mode = source_lsn:

Each upserted doc includes __source_lsn

Use turbopuffer conditional upsert so a write is applied only if incoming __source_lsn is greater than stored value (or stored is null).
This makes retries and reordering safe.

7. Membership predicate modes

dsl: evaluate from new/old row maps

view: membership is pre-applied by source view

lookup: fetch current row by PK to evaluate and/or fill missing columns

8. Testing strategy

Unit tests:

predicate eval, transform adapter mocks, batching, request shaping

Integration tests:

docker Postgres with wal2json

mock turbopuffer server or mock client trait

Fixture tests:

JSON rowevent fixtures → golden turbopuffer request JSON

9. Observability

structured logs (tracing)

progress renderer (indicatif)

later: metrics export