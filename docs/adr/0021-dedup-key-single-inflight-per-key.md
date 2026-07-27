# ADR-0021: Dedup key — single in-flight event per key

## Status

Accepted

## Date

2026-07-26

## Context

A common pattern: a business transaction (or a burst of them) modifies
an aggregate several times, and every modification wants to schedule
the same follow-up work — sync the order to an external system,
rebuild a projection, refresh a cache. Only one execution is needed;
publishing N events means N redundant runs and forces business code to
track "did I already schedule this?".

The naive fix — a unique dedup key over all events — has a **lost
update** failure mode, spotted during design review of this feature:

1. T1 commits event `SYNC_ORDER:42` → PENDING.
2. A worker claims it; the handler reads the order — a snapshot
   *before* T2.
3. T2 modifies the order and publishes `SYNC_ORDER:42` → swallowed by
   dedup (an event with that key exists).
4. The handler finishes, the row is deleted, T2 commits.

T2's changes are never synced: no event remains, and no retry or
handler idempotency can recover them.

## Decision

**Coalescing with a visibility guarantee: at most one PENDING event
per {@code (event_type, dedup_key)}, and a coalesced-into event is
never handled before the coalescing transaction commits.**

Mechanics (PostgreSQL):

1. **Partial unique index over PENDING only** (migration V004):
   `UNIQUE (event_type, dedup_key) WHERE status = 'PENDING' AND
   dedup_key IS NOT NULL`. The insert uses
   `ON CONFLICT ... DO NOTHING`. Consequences:
   - a **PROCESSING** event does not block the key — step 2 of the
     race above cannot swallow T2: a fresh event inserts and runs
     afterwards with T2's data;
   - **DISABLED** events do not block the key;
   - key-less inserts never touch the index.
2. **`FOR UPDATE` pin closes the residual window** (the existing row
   is still PENDING but gets claimed before our commit): on conflict,
   the publisher locks the existing PENDING row with
   `SELECT ... FOR UPDATE` *inside the caller's transaction*
   (`EventStore.lockPendingByDedupKey`). The claim query already uses
   `FOR UPDATE SKIP LOCKED`, so it skips the pinned row until the
   publisher commits — after which the handler sees the committed
   changes. If the row vanished in between (claimed and finalized),
   the publisher retries the insert (bounded loop).
3. **API**: `PublishOptions.dedupKey` (≤ 256 chars). A coalesced
   publish returns the **existing** event's id; `onEventPublished` and
   the poller wake fire only for real inserts. `publishAll` routes
   keyed requests through the single-insert path (batch `saveAll`
   rejects keyed events — coalescing needs per-row feedback).
4. **SPI**: `EventStore.save` returns `boolean` (inserted vs
   coalesced); new `lockPendingByDedupKey`. The in-memory adapter
   emulates the constraint with a monitor; it has no transactions, so
   the visibility race does not exist there by construction.

### Guarantee, stated precisely

- Within one transaction, N publishes with the same key produce one
  event (`ON CONFLICT` sees the transaction's own uncommitted row).
- Across transactions: at most one PENDING event per key, and **the
  effect of every transaction whose publish was coalesced is visible
  to the handler when the event runs**.
- This is work coalescing — *single in-flight per key* — **not
  exactly-once**: once the event is processed the key is free again.
  Handler idempotency remains mandatory (ADR-0015).

## Alternatives considered

- **Unique key over all statuses / forever-dedup** — the lost-update
  race above, or a separate dedup table with TTL and retention
  (heavier, slower inserts). Rejected.
- **Transaction-scoped dedup only** (in-memory set per TX, no schema)
  — covers the single-transaction case but no cross-transaction
  coalescing. Rejected in favour of the full variant.
- **State-transition-guarded publishing** (`UPDATE ... WHERE status =
  'NEW'` then publish only if 1 row) — the idiomatic application-level
  answer to *accidental* duplicates from retried operations; it
  remains the recommended pattern for that case and needs nothing from
  the library. The dedup key targets *intentional* duplicates
  (coalescing), which state guards cannot express.

## Consequences

- Business code can publish on every modification without tracking
  what was already scheduled.
- On the dedup path the publishing transaction briefly holds a row
  lock on the pending event until commit; concurrent publishers of
  the same key serialize on it (and then coalesce). Key-less publishes
  are unaffected.
- The archive table does not carry the dedup key — the key's meaning
  ends when the row leaves PENDING.
- Schema V004 (column + partial unique index); `EventStore.save`
  signature change (pre-1.0 SPI break).

## Related decisions

- [ADR-0002](0002-participate-in-client-transaction.md) — the pin
  works precisely because publish runs inside the caller's
  transaction.
- [ADR-0015](0015-at-least-once-semantics.md) — at-least-once and
  handler idempotency are unchanged by coalescing.
- [ADR-0014](0014-optimistic-locking-via-version-field.md) — claim's
  `SKIP LOCKED` is what the pin leans on.
