# ADR-0037: Claim-time dedup coalescing

## Status

Accepted — proposed 2026-09-05 on the measurements of the claim-time
spike (see §Measured), implemented and accepted 2026-09-07. Supersedes
[ADR-0021](0021-dedup-key-single-inflight-per-key.md); amends
[ADR-0033](0033-archive-dedup-key-and-replay-from-archive.md),
[ADR-0013](0013-outbox-listener-for-observability.md) and
[ADR-0023](0023-tracing-spi-port-and-adapters.md) as listed in
§Implementation plan. The migration is **V010** (V009 was already the
ADR-0033 replay index when this ADR was drafted; the draft said V009).

## Date

2026-09-05

## Context

ADR-0021 collapses duplicates of a `(event_type, dedup_key)` at
publish time. The mechanics: a conditional insert against the V004
partial unique index (`WHERE status = 'PENDING' AND dedup_key IS NOT
NULL`, `ON CONFLICT DO NOTHING`), and on conflict a `SELECT ... FOR
UPDATE` *pin* of the existing PENDING row inside the publishing
transaction, so that the claim (`FOR UPDATE SKIP LOCKED`) passes the
row by until the publisher commits and its handler is guaranteed to
see the coalesced transaction's changes. The index deliberately covers
PENDING rows only: a publish that finds the key's event PROCESSING must
insert a fresh row, otherwise the lost-update race of ADR-0021 returns.

A design review of the self-rescheduling ("recurring event") pattern —
a handler that publishes the next tick under the same dedup key while
its own event is PROCESSING — exposed a defect in that combination.
The twin is inserted legitimately. Every later transition of the
PROCESSING row *back* to PENDING then collides with the partial unique
index: `markForRetry` (the handler returned `Retry` or threw),
`release` (busy entity lock, lock error, saturated executor, missing
handler, failed finalize), `releaseClaimed` (shutdown), `forceReclaim`
(watchdog), `reclaimOrphans` (dead worker) and the operator's
`reenable`. None of them handles `23505`. The finalize throws, the
emergency release throws too, and the row is stuck in PROCESSING until
the stale-claim sweep; `reclaimOrphans` is one multi-row UPDATE, so a
single such row fails the recovery of *every* event of a dead worker.
The trigger is not exotic: it is the motivating scenario of ADR-0021
itself — a keyed publish while the previous event of the key is
PROCESSING — followed by the most ordinary non-success outcome, a busy
entity lock.

The review also questioned the pin. It costs a second statement per
coalesced publish; it makes publishers of one key wait on each other's
commits (the unique index blocks a concurrent insert of the same key
until the first transaction ends, and the pin holds the row lock until
commit); and a transaction that publishes two keys in one order while
another publishes them in the other order can deadlock on the index.
Under a burst on a few keys the PENDING row is pinned by *someone* most
of the time and the claim keeps skipping it.

The alternative on the table: coalesce at the **claim** instead. Every
keyed publish inserts unconditionally; the claim statement collapses
the *due* duplicates of the keys it picked, before any handler runs.

## Alternatives considered

- **Keep insert-time coalescing, narrow the index to rows that were
  never claimed** — predicate `... AND version = 0` (every state change
  increments `version`, ADR-0014, so a row that ever left PENDING can
  never re-enter the index). Fixes the collision by construction with
  one migration and no new code path; fails safe (a future operation
  that bumps `version` on a fresh row costs a redundant run, not a
  stuck row). Rejected only because it keeps the pin, and the pin is
  the expensive half of the design (§Measured). Recorded because it is
  the right *minimal* fix should claim-time coalescing ever be
  reverted.
- **Keep the pin, null `dedup_key` on the way back to PENDING when a
  twin exists** — `CASE WHEN EXISTS (...)` in eight statements plus
  their batch forms, still check-then-act against a concurrent insert
  (needs a `23505` retry loop in each), loses the archive's audit copy
  of the key. Rejected.
- **Treat the returning row as coalesced into its twin and drop it.**
  After a watchdog or orphan reclaim the handler may never have run,
  and the twin's payload and `run_at` may differ (a deferred tick).
  Violates at-least-once (ADR-0015). Rejected.
- **Re-arm instead of a twin** — a publish that finds the key
  PROCESSING sets a flag on that row and the finalize returns it to
  PENDING; no twin ever exists. Discards the twin's payload and `runAt`,
  so a deferred twin and the recurring-event pattern that started this
  review become impossible; a rewrite of ADR-0021 rather than a fix.
  Rejected.
- **Unique index over PENDING and PROCESSING.** The lost-update race
  ADR-0021 was written to prevent. Rejected again.
- **Both mechanisms behind a property.** Two code paths to test and
  document, and the insert-time path keeps the collision unless it is
  fixed as well. Rejected: one mechanism. The spike's flag is
  scaffolding and is retired by the implementing change.

## Decision

**Duplicates of a `(event_type, dedup_key)` are collapsed by the claim
statement, not by the insert.** The partial unique index and the
publisher-side pin are removed.

### 1. Publish inserts unconditionally

`EventStore.save` is a plain insert for keyed and keyless events alike
and returns nothing; `lockPendingByDedupKey` is removed from the SPI;
`saveAll` accepts keyed events, so `publishAll` batches them like any
other. `OutboxEventPublisher.publish` returns the id of the row it
inserted — always its own — and `onEventPublished` fires for every
insert. Whether the row will run or be swept is not knowable at
publish time and is not reported there.

### 2. Index (migration V010)

`uq_events_pending_dedup_key` (V004) is dropped and replaced by a plain
partial index with the same columns and predicate,
`ix_events_pending_dedup_key ON events (event_type, dedup_key) WHERE
status = 'PENDING' AND dedup_key IS NOT NULL` — the sweep's lookup, no
uniqueness. The `dedup_key` column and its archive copy (V008) are
unchanged.

### 3. The claim statement sweeps

The claim keeps its locking CTE unchanged and grows three CTEs that do
nothing when the batch carries no key:

```sql
WITH picked AS (                                   -- as today
    SELECT id, dedup_key, priority, run_at FROM events
    WHERE event_type = :type AND status = 'PENDING' AND run_at <= now()
    ORDER BY priority DESC, run_at LIMIT :batch FOR UPDATE SKIP LOCKED),
keep AS (                                          -- one representative per key
    SELECT DISTINCT ON (dedup_key) id, dedup_key FROM picked
    WHERE dedup_key IS NOT NULL ORDER BY dedup_key, priority DESC, run_at, id),
dup AS (                                           -- its due duplicates, in and beyond the batch
    SELECT d.id FROM events d JOIN keep k ON d.dedup_key = k.dedup_key
    WHERE d.event_type = :type AND d.status = 'PENDING' AND d.run_at <= now()
      AND d.id <> k.id LIMIT :cap FOR UPDATE OF d SKIP LOCKED),
gone AS (DELETE FROM events e USING dup WHERE e.id = dup.id
         RETURNING e.id, e.dedup_key, e.trace_context /* + archive columns */)
UPDATE events e SET status = 'PROCESSING', claimed_by = :worker, claimed_at = now(),
       version = version + 1
FROM picked WHERE e.id = picked.id AND NOT EXISTS (SELECT 1 FROM dup WHERE dup.id = e.id)
RETURNING ...
```

- `DISTINCT ON` is legal in `keep` because the locking already
  happened in `picked`; PostgreSQL forbids a locking clause together
  with `DISTINCT`, `GROUP BY` or window functions, which is why the
  sweep cannot be folded into the locking CTE.
- The representative is the best row of the key by the claim's own
  order — highest priority, then oldest `run_at`. Its payload runs; the
  duplicates' payloads do not, exactly as the coalesced-into row's
  payload ran before.
- `dup` reaches beyond the batch (a burst of a thousand duplicates
  collapses in one claim, not in twenty) but never blocks: rows locked
  by another worker's claim are skipped. A duplicate that lands in
  another worker's batch runs there — redundant runs are bounded by
  the number of workers claiming the type concurrently, not by the
  size of the burst.
- Every row is touched by exactly one data-modifying sub-statement
  (`gone` deletes the duplicates, the UPDATE claims the rest), as
  PostgreSQL requires.
- The sweep is capped at **1 000 rows per claim** (`:cap`, a constant
  of the adapter, not a property). The spike reached the cap at 5 ms
  per statement; whatever is left is swept by the next claim of the
  type.

### 4. What is never swept

- **Rows with a future `run_at`.** A deferred twin is a separate intent
  (`PublishOptions.runAt`); it becomes due later and is claimed or
  swept then. This is what keeps the self-rescheduling pattern alive:
  the next tick, published under the chain's key while the current
  tick runs, survives.
- **Uncommitted rows.** Invisible to the claim's snapshot; they run on
  their own after commit.
- **Rows locked by another claim**, see above.
- **Rows of another event type**, trivially — keys are scoped per type
  as before.

### 5. Swept rows follow the archive setting

With `archive-enabled: false` a swept duplicate is deleted. With
archiving on it is copied to `event_archive` in the same statement,
`last_fail_reason = 'coalesced at claim'`, `archived_by` = the
claiming worker, `dedup_key` intact — the audit trail ADR-0033 added
the column for. The replay path (ADR-0033 §3) inserts plainly: its
`ON CONFLICT` arbiter referenced the V004 index and is removed;
replaying a key that is also PENDING produces a row the next claim
collapses.

### 6. The claim reports what it swept

`ClaimedEvent` gains `dedupKey()` and `coalesced()`: the key the row
was published under, and the ids and trace contexts of the duplicates
swept on behalf of that representative, empty for keyless rows. The PostgreSQL adapter returns them in the same round trip (the
`gone` CTE's `RETURNING` joined to the final result). The dispatcher
uses them for the two observability surfaces below before it invokes
the handler; nothing else in the engine changes.

### 7. Observability moves to the consumer side

- `OutboxListener.onEventCoalesced` fires **from the dispatcher**, once
  per swept duplicate, after the claim and before the handler.
  `EventCoalescedInfo` becomes `(eventId, coalescedIntoEventId,
  eventType, dedupKey)` — the swept event first, the representative
  second. The `event_outboxer.events.coalesced{event_type}` counter
  keeps its name; its meaning is now "publishes collapsed by a claim".
  The dedup ratio is `coalesced / published`.
- `OutboxTracer.PublishSpan.coalesced(UUID)` and the producer attribute
  `event_outboxer.coalesced_into` are removed — the publish span cannot
  know. The **process span** gains
  `event_outboxer.coalesced_count` and one span *link* per swept
  duplicate's trace context, so the handler run is causally linked to
  every publish it covers, in the same way the deferred-event link of
  ADR-0023 works.

### 8. Guarantee, stated precisely

- **Visibility (unchanged in substance).** Every keyed publish is
  either handled under its own id or swept by a claim whose snapshot
  contained it. In both cases the handler that covers it started after
  the publish committed — the sweep sees committed rows only, and it
  precedes `handle` by construction. The invariant the benchmark
  harness grades is exactly this: for every key, the last publish is
  followed by a successful handling that started after that publish
  committed.
- **Coalescing is best-effort, not a table invariant.** "At most one
  PENDING event per key" no longer holds; duplicates accumulate until
  the next claim of the type, and duplicates claimed concurrently by
  different workers run separately. Not exactly-once (ADR-0015);
  handler idempotency remains mandatory.
- **Within one transaction** N publishes of one key produce N rows and,
  being due together, one run — previously one row. The extra rows cost
  what any event costs.
- **Deferred rows are separate intents** (§4).

## Rationale

The visibility guarantee of ADR-0021 was bought with a lock because the
decision was taken at insert time, when the coalescing transaction is
by definition uncommitted. Taken at claim time, the same guarantee
falls out of READ COMMITTED for free: a claim can only sweep what has
committed, and it sweeps before the handler starts. Once the guarantee
no longer needs the pin, the unique index has no job left either — and
with it goes the `23505` collision, by construction rather than by
handling it in eight places.

The spike measured what the move costs and buys (§Measured). It costs
nothing on the keyless path, about five microseconds per swept
duplicate on the keyed one, and — where duplicates are dense — two row
writes and a kilobyte of WAL per duplicate that the insert-time design
paid as a conflicting no-op. It buys one statement instead of two per
keyed publish, publishers that no longer wait on each other, a tail
under a hot-key burst an order of magnitude shorter, and twice the
coalescing where duplicates are scattered, because the pin's "vanished"
fallback (the pinned row got claimed in the microseconds between
conflict and lock) no longer forces a publisher into a row of its own.

One trade the numbers cannot settle: under a burst on very few keys the
pin *absorbed* publishes into one long-lived row (302 handler runs for
20 000 publishes, at a two-second tail), while the sweep runs whatever
is due at each poll (3 146 runs, at a tenth of a second). This ADR
chooses the latter: the library promises delivery and visibility, not
a minimum number of runs; a handler that wants fewer runs on a hot key
publishes with a `runAt` a few seconds ahead, which the sweep respects
(§4) and which collapses the burst into one deferred row per key — the
debounce idiom, expressed in the public API rather than in a lock.

## Measured

Spike of 2026-09-05 (`docs/benchmarks/2026-09-05-laptop-dedup-claim-time.md`;
harness preset `dedup-burst`, 20 000 publishes from 8 threads, 3
workers, 2 ms of work; all cells pass the freshness invariant):

| Cell | insert-time (ADR-0021) | claim-time (this ADR) |
|---|---|---|
| `throughput`, no key: claim mean / writes per event | 0.77 ms / 3.00 | 0.77 ms / 3.00 |
| claim mean, 64 and 1 024 keys | 0.47 / 0.42 ms | 0.47 / 0.50 ms |
| claim mean, 8 keys (~48 duplicates swept per claim) | 0.27 ms | 2.00 ms |
| claim mean, backlog (~900 swept per claim, cap reached) | 0.29 ms | 5.09 ms |
| statements per keyed publish | 2.0 | 1.0–1.8 |
| publish p95, 64 keys | 12.0 ms | 6.1 ms |
| publish rate, 64 keys | 1 074/s | 1 602/s |
| end-to-end p99, 8 keys | 1 970 ms | 116 ms |
| handler runs per 20 000 publishes, 8 keys | 302 | 3 146 |
| coalesced, 64 / 1 024 keys | 9.2 % / 12.1 % | 22.5 % / 20.5 % |
| row writes per publish, 8 keys | 0.05 | 2.16 |
| WAL per publish, backlog | 107 B | 984 B |

## Consequences

**Users of the library**

- `publish` with a dedup key always returns the new event's id;
  `onEventPublished` fires for every publish. Code that correlated on
  "the id of the event my publish coalesced into" must move to
  `onEventCoalesced`, which now carries both ids.
- `publishAll` accepts keyed requests in one batch.
- Publishers of one key no longer serialize on each other; the
  cross-key deadlock hazard of the unique index is gone.
- A retry, release, reclaim or `reenable` of a keyed event can no
  longer fail because a twin exists. The self-rescheduling pattern
  with a fixed key per chain is safe.
- Under a burst on a hot key expect more handler runs and a much
  shorter tail than before; use `runAt` to debounce if fewer runs
  matter more than freshness.
- Pending-backlog metrics count duplicates until the next claim.

**Library maintainers**

- SPI (pre-1.0 break, the second on this surface): `EventStore.save`
  returns `void`, `lockPendingByDedupKey` is gone, `saveAll` accepts
  keys, `ClaimedEvent.dedupKey()` and `ClaimedEvent.coalesced()` are
  new, `ReplayOutcome.COALESCED` and `ReplayAllResult.coalesced` are
  gone (a replay never coalesces any more). The in-memory adapter's
  monitor-based emulation of the unique index is deleted; its claim
  collapses due duplicates per key under the same monitor.
- `EventCoalescedInfo` and `OutboxTracer.PublishSpan` change shape
  (§7); ADR-0013 and ADR-0023 are amended by the implementing change.
- The regression test for the collision — twin published while
  PROCESSING, then `markForRetry`, `release`, `forceReclaim`,
  `reclaimOrphans` and `reenable` — enters the contract suite, so the
  defect cannot return under any storage adapter.
- Invariant 6 (optimistic locking) and the claim query's hot path are
  untouched; the added CTEs are empty for keyless batches.

**Operations**

- **Migration V010 is a coordinated step.** The old adapter's insert
  names the V004 index in its `ON CONFLICT` clause; once the index is
  dropped, a keyed publish from an old JVM fails with *no unique or
  exclusion constraint matching the ON CONFLICT specification*. Apply
  V010 with the new version and do not run a mixed fleet that
  publishes keyed events during the rollout. Keyless publishes are
  unaffected.
- Dense duplicates now cost row writes, WAL and dead tuples (two
  writes and ~1 KB per duplicate) where they cost a conflicting no-op
  before; autovacuum settings on `events` matter for hot-key
  workloads. Sparse duplicates cost the same as before.
- The claim statement's server time grows with the duplicates it
  sweeps (~5 µs per row, capped at 1 000 rows); the
  `pg_stat_statements` mean of the claim statement is the figure to
  watch on a hot-key deployment.

## Implementation plan

1. Migration V010 (drop `uq_events_pending_dedup_key`, create
   `ix_events_pending_dedup_key`); `docs/STORAGE.md` §Indexes, §Insert
   and §Claim.
2. PostgreSQL adapter: plain insert, the sweeping claim statement with
   the archive variant, `RETURNING` of swept ids and trace contexts;
   remove the spike flag (`PostgresStorageProperties.dedupMode`,
   `event-outboxer.storage.dedup-mode`) and the harness's index swap
   (`DedupIndex`).
3. SPI and in-memory adapter: `save` → `void`, drop
   `lockPendingByDedupKey`, `saveAll` accepts keys,
   `ClaimedEvent.dedupKey()` and `coalesced()`, `ReplayOutcome` and
   `ReplayAllResult` without the coalesced outcome; contract tests for the sweep (in-batch,
   beyond-batch, deferred twin survives, other worker's lock skipped,
   uncommitted twin invisible) and the collision regression above.
4. Core: `DefaultOutboxEventPublisher` loses `saveCoalescing`;
   `HandlerDispatcher` fires `onEventCoalesced` per swept duplicate and
   records `coalesced_count` plus span links on the process span.
5. API: `EventCoalescedInfo(eventId, coalescedIntoEventId, eventType,
   dedupKey)`; `OutboxTracer.PublishSpan.coalesced` removed; ADR-0013,
   ADR-0023, `docs/OBSERVABILITY.md` (metric row, callback row, span
   attributes), `docs/CONFIGURATION.md` (drop the experimental
   property).
6. ADR-0033: replay inserts plainly (§5 here); ADR-0021: status
   *Superseded by ADR-0037*.
7. Benchmark: keep `dedup-burst`, `--bench.dedup-keys` and the
   freshness invariant; drop `--bench.dedup`; a longer hot-key cell
   with dead-tuple counts for the vacuum question left open by the
   spike.

## Related decisions

- [ADR-0021](0021-dedup-key-single-inflight-per-key.md) — superseded:
  the dedup key, its scope per type and the visibility guarantee stay;
  the unique index and the pin go.
- [ADR-0033](0033-archive-dedup-key-and-replay-from-archive.md) —
  amended: the archive keeps the key and now also receives swept
  duplicates; replay loses its `ON CONFLICT` arbiter.
- [ADR-0013](0013-outbox-listener-for-observability.md) — amended:
  `onEventCoalesced` moves from the publication group to the
  consumer side and carries both ids.
- [ADR-0023](0023-tracing-spi-port-and-adapters.md) — amended:
  coalescing is recorded on the process span (count and links), not
  on the publish span.
- [ADR-0014](0014-optimistic-locking-via-version-field.md) — the
  claim's `SKIP LOCKED` is what keeps the sweep from blocking; the
  `version = 0` alternative rests on this ADR's invariant.
- [ADR-0015](0015-at-least-once-semantics.md) — coalescing remains
  best-effort under at-least-once; idempotency is unchanged.
- [ADR-0034](0034-benchmark-and-invariant-harness.md) — the decision
  was taken on the harness's numbers and its new per-key freshness
  invariant.
