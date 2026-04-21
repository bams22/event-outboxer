# ADR-0008: Three statuses + optional archive in a separate table

## Status

Accepted

## Date

2026-04-19

## Context

We need to decide how to model an event's lifecycle in the DB:
- Which statuses (PENDING / PROCESSING / DONE / FAILED / ...)?
- Where to store successfully processed events (stay in the table? deleted?
  archived?)?

This affects the size of the hot table, index effectiveness, retention
policies, and the ease of auditing.

## Alternatives considered

- **A. Four statuses with COMPLETED**: PENDING / PROCESSING / DISABLED /
  COMPLETED. Processed events stay in the table with COMPLETED status. A
  periodic cleanup task is required.
- **B. Three statuses, successful events deleted**: PENDING / PROCESSING /
  DISABLED. DONE = DELETE. No archive.
- **C. Three statuses + optional archive table**: like B, but with an
  opt-in archive in a separate table for audit purposes.

## Decision

**Option C was chosen**.

Active statuses: **PENDING / PROCESSING / DISABLED**. Successfully processed
events are deleted from `outbox.events`. An optional `outbox.event_archive`
table is activated via `outbox.postgres.archive.enabled=true`.

### Status transitions

```
           publish()              claim              success
     ────▶ PENDING  ───────────▶  PROCESSING  ──────▶  [DELETE]
              ▲                        │                 or
              │                        │          [ARCHIVED] (opt-in)
              │                        │
              │                        └──── failure+retry ───▶ PENDING
              │                                            (attempts++, version++)
              │
              │                        └──── max attempts ────▶ DISABLED
              │                              or explicit Fail
              │
              └── orphan/watchdog reclaim
```

### Archive through the application, NOT a trigger

When `markProcessed` is called with `archiveEnabled=true`, the application
runs:

```sql
BEGIN;
INSERT INTO outbox.event_archive (... , archived_at, archived_by)
SELECT ..., now(), :workerId FROM outbox.events WHERE id = :eventId;
DELETE FROM outbox.events WHERE id = :eventId AND version = :claimedVersion;
COMMIT;
```

No trigger — an explicit INSERT from the application is more understandable
and allows selectively not archiving (e.g. Skip events) without DDL changes.

### Archive schema

```sql
CREATE TABLE outbox.event_archive (
    id               UUID         PRIMARY KEY,
    event_type       VARCHAR(128) NOT NULL,
    payload          JSONB        NOT NULL,
    payload_class    VARCHAR(512) NOT NULL,
    priority         SMALLINT     NOT NULL,
    attempts         INT          NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL,
    run_at           TIMESTAMPTZ  NOT NULL,
    last_fail_reason TEXT,
    trace_context    JSONB,
    archived_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    archived_by      VARCHAR(64)  NOT NULL
);

CREATE INDEX idx_archive_archived_at ON outbox.event_archive (archived_at);
CREATE INDEX idx_archive_event_type_created_at
    ON outbox.event_archive (event_type, created_at);
```

Absent fields: `status`, `claimed_by`, `version` — they have no meaning in
the archive.

## Rationale

### Why not COMPLETED in the same table

- **The hot table does not grow unbounded**. The partial index
  `idx_events_ready WHERE status='PENDING'` stays small.
- With 10M processed events, the index would still cover all 10M rows even
  though claim queries only touch the PENDING subset. With a separate
  table, that problem is gone.
- Vacuum is heavier on larger tables.
- A cleanup task for expired COMPLETED rows adds operational overhead.

### Why a separate table, not a partition

Partitioning requires PG-specific DDL and ties us to a specific PG version.
A separate table is simpler and works everywhere. Archive retention
(deleting older than X days) is a separate scheduled task.

### Why archive is opt-in

Most outbox use cases do not require audit of processed events. An audit
trail is expensive (the table grows, retention must be managed). Users
explicitly enable it when they need it.

### `findById` and the archive

`EventStore.findById(id)` looks only in the active table. There is a
separate `findByIdIncludingArchived(id)` that searches active first, then
archive (for admin investigation). The application chooses explicitly.

## Consequences

### For users

- A successful event disappears from the DB (or moves to the archive if
  enabled).
- For audit: set `outbox.archive.enabled=true` plus a retention policy in
  code.
- DISABLED events remain in `outbox.events` — visible, investigable,
  manually revivable via the admin API.

### For maintainers

- The `EventStatus` enum = {PENDING, PROCESSING, DISABLED}. There is no
  COMPLETED.
- `EventStore.markProcessed()` — the implementation depends on the archive
  configuration: either DELETE, or INSERT+DELETE in a single TX.
- Archive migration is separate: `archive/V002__outbox_archive.sql`.
- `findByIdIncludingArchived` is a separate method in EventStore.

### Positive consequences

- The hot table stays at constant size (bounded by active events).
- Partial index efficiency is preserved.
- Retention policy is an independent job that does not interfere with the
  engine.
- Opt-in archive keeps the base install simple.

### Negative consequences

- No "history" in a single table — investigations must JOIN/UNION archive
  and active data.
- The archive is enabled by a separate migration (the user must add the
  path to Flyway locations).

## Related decisions

- [ADR-0014](0014-optimistic-locking-via-version-field.md) —
  `markProcessed` checks `version`.
- [ADR-0005](0005-workers-heartbeat-table.md) — separate tables for
  separate concerns — a general architectural pattern in this library.
