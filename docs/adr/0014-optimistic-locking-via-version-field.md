# ADR-0014: Optimistic locking via a version field

## Status

Accepted

## Date

2026-04-19

## Context

In distributed scenarios the same event may fall under the attention of
several components simultaneously:
- Worker A claims it.
- While A is processing, a watchdog on another instance fires (a GC stall
  on A makes the watchdog think the handler is stuck) → force reclaim
  attempt.
- Orphan recovery on a third instance sees a stale heartbeat and also
  tries to reclaim.

We need a coordination mechanism: whoever commits the change first wins,
and the others must learn that their change did not go through.

jobrunr uses a **mixed** mechanism: claim via `SELECT FOR UPDATE SKIP
LOCKED` (pessimistic), save via `UPDATE ... WHERE version=?` (optimistic).
This leads to quirky behavior: you can lock a row, yet during finalize it
is "stolen" — a retry loop is required.

## Alternatives considered

- **A. Pessimistic locking everywhere**: `SELECT FOR UPDATE` in every
  operation. Potential deadlocks, blocking, poor horizontal scalability.
- **B. Optimistic locking via version everywhere**: every operation checks
  `version=?`; a conflict surfaces as a visible `false`/rowCount=0. Works
  independently of the DB lock behavior.
- **C. Mixed model (like jobrunr)**: claim pessimistic (SKIP LOCKED),
  finalize optimistic (version). Requires a retry loop on finalize.

## Decision

**Option B was chosen** with one refinement: `SKIP LOCKED` is used only for
**speed** (to avoid blocking on claim), but correctness is guaranteed by
`version`.

### Contract

Every event has `version BIGINT NOT NULL DEFAULT 0`.

| Operation | Changes version? | Checks version? |
|---|---|---|
| `save()` / publish | creates version=0 | — |
| `claim()` | **YES (+1)** | — (just picks it up and bumps) |
| `markProcessed(DELETE)` | — (deletion) | **YES** (WHERE version=?) |
| `markForRetry()` | **YES (+1)** | **YES** (WHERE version=?) |
| `markDisabled()` | **YES (+1)** | **YES** (WHERE version=?) |
| `forceReclaim()` (watchdog) | **YES (+1)** | **YES** (WHERE version=?) |
| `reclaimOrphans()` | **YES (+1)** | — (picks up by claimed_by=dead) |
| `heartbeat` (in workers!) | N/A | N/A |

### Return `false` instead of exception

The finalize methods return a `boolean`:
- `true` — exactly 1 row updated (success).
- `false` — no row found with that `version` (someone beat us).

Under at-least-once semantics, `false` is an **expected race**, not
exception-worthy. The core engine catches it, increments the metric
`event_outboxer.events.concurrent_completion_conflict`, and continues.

`StorageException` is thrown only when the storage itself has a real
problem (network, deadlock, constraint violation).

### Heartbeat does NOT change `version`

Important note: lease renewal (if we had per-event leases) must not
increment `version`, otherwise finalize after renewal would fail. In our
design the heartbeat is stored in a separate `event_outboxer.workers` table (see
ADR-0005), so the question does not even arise.

## Rationale

### Correctness

The master invariant:

> Finalizing an event succeeds if and only if:
>   (a) event.version == claimedVersion,
>   (b) event.claimed_by == myWorkerId,
>   (c) event.status == PROCESSING.

These conditions protect against every race:
- Orphan recovery changed `version` → finalize fails (false).
- Watchdog force-reclaimed → finalize fails.
- Some other worker somehow claimed (should not happen, but just in case)
  → fails.

### Simplicity

One mechanism instead of two. There is no "what if `version` diverges
between SELECT FOR UPDATE and UPDATE" — there is no such window.

### Scalability

`SELECT FOR UPDATE SKIP LOCKED` is used only to let **concurrent** claims
by several workers proceed without blocking each other. After claim the
row is no longer locked. `version` enforces correctness without
long-held locks.

### Race-condition metric

`event_outboxer.events.concurrent_completion_conflict{reason}` reveals how often
races occur in production. Rare — all good. Frequent — a signal that the
watchdog or orphan recovery is too aggressive (`handlerMaxRuntime` too
short, `deadThreshold` too short).

## Consequences

### For users

- Observability: the metric
  `event_outboxer.events.concurrent_completion_conflict` (from OutboxListener →
  Micrometer).
- Under normal operation — ~0 conflicts. Frequent conflicts — tune
  `handlerMaxRuntime` and `deadThreshold`.
- Idempotent handlers are mandatory (ADR-0015).

### For maintainers

- **Critical invariant**: every operation that changes event state MUST
  increment `version`.
- Exceptions: heartbeat does not touch `event_outboxer.events` at all; lease
  renewal (in workers model) likewise.
- SQL always includes `WHERE id=:id AND version=:claimedVersion AND
  claimed_by=:me` for safety.
- `RETURNING id` or `rowCount` is the mandatory way to detect success.

### Positive consequences

- Correctness without deadlocks.
- Horizontally scalable.
- A single mechanism, a single mental model.
- `false` is not-an-error, a normal part of the semantics.

### Negative consequences

- Useless `version++` on happy-path processing (but that is a single
  update, cheap).
- Requires discipline when writing new SQL — do not forget the
  `version` check.

## Related decisions

- [ADR-0005](0005-workers-heartbeat-table.md) — the heartbeat was moved to
  its own table to avoid conflicting with the event's `version`
  invariants.
- [ADR-0015](0015-at-least-once-semantics.md) — version races are part of
  the at-least-once model: duplicates are possible, the handler is
  idempotent.
