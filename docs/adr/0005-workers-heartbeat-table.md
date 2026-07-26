# ADR-0005: Worker heartbeat in a separate table

## Status

Accepted — amended 2026-07-26 (a DB-side stale-claim sweeper added as
the last line of defence; see the Amendment section at the bottom)

## Date

2026-04-19

## Context

To detect crashed workers (so their events can be redistributed) we need a
heartbeat mechanism. Two approaches were considered:

1. **Per-event lease**: every `PROCESSING` event carries a `lease_until`;
   the worker periodically extends the lease for every in-flight event.
   This is how db-scheduler does it — `last_heartbeat` column directly on
   `scheduled_tasks`.

2. **Separate workers table**: one row per JVM; the worker updates the
   `last_heartbeat` on its own row. Events reference the worker via
   `claimed_by`. This is how jobrunr does it with the
   `jobrunr_backgroundjobservers` table.

Option (1) was initially chosen, but the write-traffic problem on large
backlogs was pointed out: with 1000+ in-flight events and a heartbeat every
30s we would be writing 2000+ rows/min into the hot `event_outboxer.events` table
just for keepalive.

## Alternatives considered

- **A. Per-event lease**: `lease_until` on the `event_outboxer.events` row.
  Heartbeat is a batch UPDATE of all in-flight rows.
- **B. Separate table**: `event_outboxer.workers(worker_id PK, last_heartbeat,
  ...)`. Heartbeat is a single-row UPDATE.
- **C. Hybrid**: both together. Rejected as redundant.

## Decision

**Option B was chosen**: `event_outboxer.workers` is a dedicated heartbeat table,
with a single row per JVM.

### Schema

```sql
CREATE TABLE event_outboxer.workers (
    worker_id       VARCHAR(64)  PRIMARY KEY,
    host            VARCHAR(256) NOT NULL,
    pid             INT,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    graceful_stop   BOOLEAN      NOT NULL DEFAULT FALSE,
    metadata        JSONB
);

CREATE INDEX idx_workers_heartbeat
    ON event_outboxer.workers (last_heartbeat)
    WHERE graceful_stop = FALSE;
```

### Lifecycle

1. **Startup**: `INSERT` into `event_outboxer.workers` with
   `worker_id = "host-pid-uuid"`.
2. **Heartbeat**: every 30s (default) `UPDATE ... SET last_heartbeat=now()
   WHERE worker_id=?` — exactly one row.
3. **Graceful shutdown**: `UPDATE ... SET graceful_stop=TRUE` → wait for
   in-flight events → `DELETE`.
4. **Crash**: DELETE is not executed; after `deadThreshold`
   (`3 × heartbeatInterval` = 90s by default) orphan recovery marks the
   worker as dead, returns its events to PENDING, and deletes the row.

## Rationale

### Quantitative benefits

| | Per-event lease | Workers table |
|---|---|---|
| Writes per heartbeat cycle | N (in-flight count) | 1 |
| 10 in-flight, 30s heartbeat | ~20 writes/min | 2 writes/min |
| 1000 in-flight, 30s heartbeat | ~2000 writes/min | 2 writes/min |

But the key point is not just the count — the critical property is that
**the hot `event_outboxer.events` table is not mutated every 30s just to say "I'm
alive"**. That means:
- Less MVCC bloat (PG keeps old and new row versions until vacuum).
- Fewer WAL writes.
- Fewer index updates, including the partial `idx_events_ready` index.
- Less replication lag if standbys are present.

### Watchdog for stuck handlers

A per-event lease gives a passive defense: if the handler hangs, the
watchdog simply stops extending the lease → orphan recovery picks it up.
With a separate workers table, we need **active reclaim**: the watchdog
explicitly calls `EventStore.forceReclaim()`.

That is slightly more code, but semantically even cleaner: we explicitly
decide "this event is stuck" rather than passively waiting for a timeout.

### Bonus: almost-free observability

```sql
-- live cluster workers
SELECT worker_id, host, started_at, last_heartbeat, metadata
FROM event_outboxer.workers
WHERE last_heartbeat > now() - interval '90 seconds';

-- how many events each worker owns right now
SELECT w.worker_id, w.host, count(e.id) AS in_flight
FROM event_outboxer.workers w
LEFT JOIN event_outboxer.events e ON e.claimed_by = w.worker_id AND e.status = 'PROCESSING'
GROUP BY w.worker_id, w.host;
```

## Consequences

### For users

- The database gains an `event_outboxer.workers` table, included in Flyway
  migrations (it is part of `V001__outbox_core.sql`).
- SQL-based admin visibility of the cluster is almost free.

### For maintainers

- `WorkerRegistry` is a separate SPI port (not part of `EventStore`).
- Orphan recovery runs in a two-phase transaction: find dead workers →
  reclaim their events → delete workers. Concurrency safety is ensured by
  `FOR UPDATE SKIP LOCKED` on the workers row.
- Edge case: events with `claimed_by` that no longer has a worker row
  (from a partially completed graceful shutdown). An additional query in
  orphan recovery handles this.
- The watchdog for stuck handlers is an active reclaim, not a passive lease
  timeout.

### Positive consequences

- O(1) heartbeat write traffic, independent of in-flight count.
- A clean hot table: mutated only for business reasons (claim, finalize,
  reclaim).
- A ready-made dashboard via SQL.
- `metadata` in the workers table (version, git-sha) helps with debugging.

### Negative consequences

- One additional table.
- Active-reclaim watchdog is slightly more complex than a passive timeout.
- Requires an explicit cleanup for the "event without worker row" edge
  case.

## Amendment (2026-07-26): stale-claim sweeper as the last line of defence

The original recovery model had a blind spot: the watchdog only sees
the LOCAL in-flight registry, and orphan recovery only reclaims events
of DEAD workers. A row stranded in `PROCESSING` on a live worker
without an in-flight registration — an `unknown-handler-policy=FAIL`
row, a claim lost to a hang before registration, a double release
failure during a storage outage — was invisible to both, forever.

Two changes (ADR-0019 era):

1. **In-flight registration now brackets the whole dispatch** —
   deserialization, lock acquisition, handler, finalize — so the
   watchdog catches hangs anywhere in the pipeline.
   `handlerMaxRuntime` therefore budgets the full processing of a
   claim, not just `handler.handle()`.
2. **`StaleClaimSweeperTask`** periodically runs
   `EventStore.sweepStale(threshold, batch)`: every `PROCESSING` row
   with `claimed_at` older than the threshold returns to `PENDING`
   (attempts+1, crash-path semantics), regardless of owner — served by
   the `idx_events_processing_claimed_at` partial index V001 created
   for exactly this scan (previously unused). The threshold must
   exceed every per-type `handlerMaxRuntime` (the sweeper cannot see
   any JVM's registry): unset, it derives `2 × max handlerMaxRuntime`;
   an explicit smaller value fails the engine build. Registered rows
   never live long enough to be swept — the watchdog force-reclaims
   them first.

Recovery layers, in firing order: watchdog (local, `handlerMaxRuntime`)
→ orphan recovery (dead workers, `dead-threshold`) → stale-claim
sweeper (everything else, `stale-claim-threshold`).

## Related decisions

- [ADR-0014](0014-optimistic-locking-via-version-field.md) — how finalize
  and orphan recovery synchronize via `version`.
- [ADR-0004](0004-per-event-type-worker-isolation.md) — workers are shared
  across types, events are per type.
