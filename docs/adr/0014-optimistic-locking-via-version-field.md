# ADR-0014: Optimistic locking via a version field

## Status

Accepted (amended 2026-07-27: batch form of the finalize invariant —
group-commit finalize batching; amended 2026-08-16: the losing side is
interrupted — stuck-handler cancellation and abandoned-thread
reporting; amended 2026-09-04: convoy-free flush path — waiters wait on
their future, not on the flush lock)

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

### Batch form of the invariant (amendment, 2026-07-27)

Finalize round-trips dominate storage traffic ~10:1 over claims (claims
are batched via `claimBatchSize`; finalizes were one statement per
event). The engine therefore batches `markProcessed` / `markForRetry`
via **group commit**: a finalizing handler thread enqueues its mark and
takes a flush lock; while one thread's batch statement is in flight,
other threads accumulate, and the next lock owner flushes them all in
one multi-row statement (`EventStore.markProcessedAll` /
`markForRetryAll`). The batch forms out of the SQL round-trip time —
no timers, no dedicated thread; an idle engine degrades to the plain
single-row call. On by default
(`event-outboxer.dispatcher.finalize-batching`, kill-switch).

The master invariant carries over unchanged to the batch form:

- every row of the batch keeps its **own** guard — the PostgreSQL
  statements join a `VALUES (id, version), ...` list against
  `WHERE e.id = k.id AND e.version = k.ver AND e.claimed_by = :me AND
  e.status = 'PROCESSING'`;
- `RETURNING id` is the batch replacement for `rowCount`: the returned
  id set is the per-row verdict list, and an id missing from it is the
  same expected at-least-once race as a single-row `false`;
- the archive-mode batch uses the same single-statement CTE as the
  single-row variant, so a lost race still cannot leave an orphan
  archive row.

Because the group-commit call is **synchronous**, nothing else in the
model shifts: finalize still completes before the entity lock is
released and before the event leaves the in-flight registry, listeners
still fire per event with per-row verdicts, and a failed batch
statement surfaces to every affected caller, whose finalize-failure
release path runs per event as before. A batch statement does finalize
its rows atomically (all-or-nothing on storage failure), which is
strictly within at-least-once semantics.

### Convoy-free flush path (amendment, 2026-09-04)

**What the benchmark harness found (ADR-0034, sessions 4–6 of
2026-09-04).** The group-commit implementation of the 2026-07-27
amendment made every finalizing thread take the flush lock — even a
thread whose entry a previous owner had already flushed, which needed
the lock only to observe `isDone()`. The lock is unfair. Under a
commit-bound round trip (a ~5 ms `fsync` on the benchmark host) a
newly arriving thread whose entry was *not* yet flushed could barge
ahead of parked threads whose entries *were*, run a full round trip,
and leave them parked for another cycle. Wall-clock JFR showed one
park per event in `awaitFlushed`, 32–44 ms each — six to eight flush
cycles — while batches stayed at 2–3 rows because threads stuck in the
convoy kept the executor saturated and the capacity-coupled poller
topped up one event per freed slot. Net effect: with batching on, more
statements per event than with it off (2.37 vs 2.21), and 1.5–6.4×
lower drain rates in 15 of 16 measured configurations. A locker in the
path (two Redis round trips per event) jittered the arrivals enough to
flip the sign in some cells, which only confirmed that the outcome
depended on arrival *phase*, not rate.

**Change.** `GroupCommitEventStore.awaitFlushed` no longer blocks on
the lock:

1. enqueue the entry; `tryLock()`;
2. the winner drains everything queued (up to `maxBatchSize` per
   statement kind), flushes it as one statement, completes every
   drained future, releases the lock — **one flush per acquisition**, so
   the owner's own entity lock and in-flight slot are held for one
   statement only;
3. if the queues are still non-empty after the release, the owner
   completes the current *hand-off generation* (a `CompletableFuture`
   that is replaced before being completed), which wakes every waiter;
   one of them wins the next `tryLock`;
4. a thread that loses `tryLock` snapshots the generation, re-checks
   its own future and the lock (an owner that released between the
   failed `tryLock` and the snapshot may have signalled an older
   generation, so it retries instead of waiting), then waits on
   `anyOf(ownFuture, generation)`.

Liveness sketch: an entry enqueued at `t` is drained either by its own
thread (won the lock), by the owner whose drain came after `t`, or —
when the owner's drain came before `t` — by the hand-off the owner
signals after its post-unlock check, which necessarily sees the entry.
The generation snapshot is taken after the failed `tryLock`, hence
before that signal. Every drained future is completed, exceptionally
on a delegate failure, so the final `join()` never blocks. Waiters
never touch the lock after their failed `tryLock`, so no convoy can
form; batches are bounded by the arrival rate during one round trip.

**Measured (same host, same day, 10 000 events, backlog mode;
`finalize-batching` on vs off, events/s):**

| Configuration | before: on | before: off | after: on | after: off |
|---|---|---|---|---|
| fsync, 1 JVM × 4 types, pool 8 | 318 | 1 390 | **1 662** | 1 342 |
| fsync, 1 × 4, pool 32 | 365 | 2 326 | 1 981 | **3 012** |
| fsync, 3 JVMs × 4 types, pool 8 | 1 626 | 3 564 | **4 836** | 3 187 |
| fsync, 1 × 4, pool 8, `claim-min-free 25` | 499 | 1 525 | 1 648 | 1 687 |
| fsync, Redis locker, unique keys, 1 × 4 | 1 478 | 1 202 | 1 293 | 1 285 |
| fsync, Redis locker, unique keys, 3 × 4 | 3 889 | 2 465 | **4 172** | 2 472 |
| no fsync, 1 × 4, pool 8 | 2 679 | 10 681 | 5 537 | **10 270** |
| no fsync, 3 × 4, pool 8 | 6 225 | 6 219 | 6 004 | 6 146 |

Batched finalizes now carry 8–10 rows (before: 2–3); single-row
finalizes vanish (before: thousands); statements per event drop to
1.5–1.8 with batching on against 2.2–2.5 off, in every cell.

**What the numbers say about the feature.** Group commit trades
parallelism for statements: one flusher per JVM, one statement per
round trip. It wins where a round trip is expensive and connections
are scarce (commit-bound storage, a pool of 8: +24 % in one JVM,
+52 % in three, +69 % with the Redis locker in three), is neutral where
the claim side is already batched, and loses where commits are cheap
or the pool is large enough for every thread to commit concurrently
(−35 % at pool 32 under fsync, −46 % in one JVM without fsync) — there
PostgreSQL's own WAL group commit does the batching. It always cuts
statements per event by about a third.

**Default.** `event-outboxer.dispatcher.finalize-batching` stays
`true`: it now reduces database round trips by a third and is
throughput-neutral or better under the storage most services run on.
Turn it off for a single JVM that must sustain more than ~5 000
finalizes per second on storage that commits in sub-millisecond time,
or whenever a pool large enough for every handler thread to commit in
parallel is cheaper than the statements saved. The recommendation
recorded in the benchmark sessions of 2026-09-04 ("`false` until the
flush path is fixed") is superseded by this amendment.

### The losing side is interrupted (amendment, 2026-08-16)

The invariant above says a force-reclaimed handler's finalize *will*
fail. Until this amendment nothing followed from that: the watchdog
fixed the row and left the handler running. A handler that never
returns — the common shape is blocking I/O with no timeout on the
client — then held its slot of the per-type handler pool forever.
Because the retry chain only runs when a handler *returns*, a
permanently blocked handler never reaches `MaxRetriesFailureHandler`
either: the event is re-claimed, blocks again, and burns one more
thread per `handlerMaxRuntime`. After `handlerPoolSize` rounds the type
stops processing entirely while every other type keeps running
(ADR-0004), which reads in metrics as a mysteriously idle event type.

The watchdog therefore **interrupts the dispatching thread** right
after a successful `forceReclaim`. This adds no new race: the interrupt
happens strictly after the version bump, so whatever the handler does
next already loses the finalize race, and the interrupt only converts a
guaranteed-worthless computation into an early exit. Specifics:

- The in-flight registry entry carries a `DispatchHandle` bound to the
  dispatching thread. `interruptIfActive()` and the dispatcher's
  `deactivate()` are mutually exclusive, so an interrupt can never land
  on a pool thread that has already moved on to the next event.
- The dispatcher clears a watchdog-issued interrupt **as soon as the
  handler unwinds** — before the finalize and the entity-lock release —
  and once more before returning the thread to the pool. The interrupt's
  only job is to unblock the handler; carrying it any further would
  break the cleanup instead. An interrupted finalize can kill a pooled
  JDBC connection, and under group-commit batching that one failure is
  shared by every other event in the same statement; an interrupted lock
  release would leave the entity lock to expire on its TTL (`>=
  handlerMaxRuntime`) and stall redelivery of that key. Each interrupt is
  consumed exactly once, so the interrupt status a `shutdownNow()` sets
  on the same thread afterwards is left standing — the flag belongs to
  the thread, not to us.
- A handler that unwinds on the interrupt reports the resulting
  exception through the normal `HandlerErrorInfo` → failure-chain path;
  its finalize then loses the race and is logged at debug, as before.
- Per-type opt-out `interruptStuckHandler` (default on) for handlers
  that are not interrupt-safe. The row is force-reclaimed either way.

Nothing can force a thread that ignores interrupts, and pretending
otherwise would be worse than not trying. Such dispatches are therefore
tracked as **abandoned** — the row belongs to whoever claims it next,
the thread is still ours and still holds its pool slot — and reported
once, after `abandonedHandlerGrace`, via
`OutboxListener.onHandlerAbandoned` plus the
`event_outboxer.handler.abandoned_threads` gauge. That gauge growing
toward `handler.executor.capacity` is the mechanical warning that the
type is about to stall, and the actionable message is always the same:
set a timeout on whatever the handler is blocked on. A dispatch of a
type that opted out of the interrupt is tracked and reported the same
way, carrying `interrupted=false` — its thread holds a slot just as
long, it was simply never asked to stop, so it is logged as a warning
rather than an error.

The abandoned set is keyed by **dispatch**, not by event id. A
force-reclaimed row goes straight back to `PENDING` and is routinely
re-claimed by this same JVM while the previous dispatch is still
running, so two dispatches of one event id overlap by design;
bookkeeping keyed by id alone would let the newer dispatch erase the
record of the thread the older one is still burning — exactly the leak
the gauge exists to expose.

Also note that `pgjdbc` may close a connection interrupted mid-I/O.
That is a property of interrupt-based cancellation, not of this
design; a handler doing long database work can opt out per type and
rely on a `statement_timeout` instead.

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
- A handler exceeding `handlerMaxRuntime` gets interrupted; handlers
  that are not interrupt-safe set
  `event-outboxer.event-types.overrides.<TYPE>.interrupt-stuck-handler:
  false`.
- `event_outboxer.handler.abandoned_threads` > 0 means this JVM has lost
  handler threads for good — the fix belongs in the handler's client
  timeouts, not in outbox configuration.

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
- [ADR-0034](0034-benchmark-and-invariant-harness.md) — the harness
  that found the flush-lock convoy and measured the 2026-09-04
  amendment; sessions under `docs/benchmarks/`.
