# ADR-0035: Bounded wait for a busy entity lock

## Status

Proposed — design agreed 2026-09-04, implementation deferred. Nothing
in the code implements this yet; the current behaviour is the one
ADR-0012 specifies (one non-blocking attempt, then reschedule).

## Date

2026-09-04

## Context

ADR-0012 fixed the worker-side flow for a handler that declares a lock
key: claim → deserialize → `extractLockKey` → one `tryLock` → on
"busy", put the event back and move on. The dispatcher implements it
literally (`HandlerDispatcher.tryAcquireLock`): a busy lock means
`store.release(...)` with `run_at = now + dispatcher.lock-busy-retry-delay`
(default 1 s), the attempt is not counted against the retry budget,
and the handler thread takes the next event. The lock itself is taken
on the handler thread immediately before `handle()` and held through
finalize — never while the event sits in the executor queue.

The first recorded benchmark session
([2026-09-04](../benchmarks/2026-09-04-laptop-first-run.md), `hot-key`
preset: 5 000 events on 8 lock keys, 5 ms of work, 3 workers × 3
threads, lease locker) showed what this costs under contention:

| Variant | handled/s | writes/event | e2e p50 |
|---|---|---|---|
| lease locker | 213 | 6.88 | 4.6 s |
| lease, `lock-busy-retry-delay=50ms` | 255 | 6.68 | 5.1 s |
| lease, busy 50 ms + `poll-max-interval=100ms` | 298 | 6.72 | 4.3 s |
| Redis locker (second session) | 336 | 5.52 | 1.5 s |
| `lock.type=noop` (incorrect baseline, 1 140 overlaps) | 612 | 3.00 | 0.2 s |
| per-key serial ideal (8 keys × 1/5 ms) | ~1 600 | — | — |

Three mechanisms compound:

1. **Collisions are frequent.** Nine handler threads on eight keys
   with 5 ms holds means most keys are held at any instant; the
   counters show 0.8–0.9 busy hits per event with the lease locker
   (1.3 with Redis, whose cheaper failed attempts allow more of them),
   each costing two row writes — the release back to `PENDING` and the
   later re-claim. *(Figure corrected on 2026-09-04 from an earlier
   "1.6–1.8", which had counted both writes as separate hits; see the
   [locks session](../benchmarks/2026-09-04-laptop-locks.md).)*
2. **The penalty for a busy hit is the queue position, not the
   delay.** The claim query orders by `priority DESC, run_at`
   (`PostgresEventStore`), and a released event carries
   `run_at = now + delay`: it re-enters *behind* every pending event
   with an earlier `run_at`, i.e. at the back of the backlog. While a
   backlog of thousands drains at ~200/s, the event waits seconds,
   then very likely collides again. This is why shortening
   `lock-busy-retry-delay` and the poller's back-off barely moved the
   result — they shorten the wrong wait.
3. **Claimed inventory is invisible to other workers.** A worker
   claims up to `claim-batch-size` events without knowing their keys;
   while they sit in its executor queue as `PROCESSING`, no other
   worker can take them even when their keys are free and its threads
   idle.

The same shape appears outside benchmarks: after a JVM crash the dead
holder's lease blocks a key until `lock-ttl` expires while orphan
recovery keeps returning its events (see the `lock-busy-retry-delay`
note in CONFIGURATION.md) — hundreds of claim → busy → release cycles.

## Alternatives considered

- **Tune the timers** (`lock-busy-retry-delay`, `poll-min/max-interval`).
  Measured; refuted (table above). They do not address mechanism 2.
- **Redisson `RLock.tryLock(wait, lease)`** as a new locker. Brings a
  second Redis client stack next to the starter-managed Lettuce
  connection (ADR-0027) — its own connection management, Netty,
  codecs — for one feature. Its lock watchdog renews the lease while
  the JVM lives, which contradicts the `lockTtl >= handlerMaxRuntime`
  contract (a stuck handler ignoring interrupts would hold the key
  forever, the advisory-locker failure mode ADR-0022 moved away from).
  With an explicit `leaseTime` the watchdog is off and `RLock` is
  `SET NX PX` plus a pub/sub wait — the existing Lettuce locker plus a
  wait loop gives the same. Rejected.
- **Key-aware claiming** (`... AND lock_key NOT IN (held keys)` in the
  claim query). The real fix for mechanism 3, but it needs the key in
  the `events` table, which ADR-0012 deliberately keeps out of the
  schema, and it only sees keys held in PostgreSQL — Redis-held keys
  are invisible to SQL. A separate, larger decision; the benchmark
  numbers are the first measured argument to reopen it. Not chosen
  here.
- **Local key affinity** (when the holder is in this JVM, queue the
  event behind it locally instead of releasing to the database).
  Cheap and complementary — a batch of ten events over eight keys
  contains same-key pairs routinely — but it does nothing for
  cross-worker collisions, which dominate with three workers walking
  the same key sequence. Possible follow-up, not a substitute.
- **Wait without bound** (block until the lock frees). Mechanism 1
  solved, but a slow holder then stalls every thread of the type: the
  capacity-coupled poller sees a saturated pool and stops claiming,
  and events of *other* keys wait in the database. Rejected in favour
  of the bounded form below.

## Decision (proposed)

### 1. A bounded wait in front of the existing busy path

When `tryLock` reports busy, the dispatcher keeps the claimed,
deserialized event on the handler thread and retries the acquisition
for at most `lock-wait`. If the lock is obtained within that window,
the handler runs as today; if not, the event takes the existing path
unchanged: `release` with `lock-busy-retry-delay`, no attempt
consumed, `onLockAcquisitionFailed(BUSY)`, `onEventRetryScheduled(LOCK_BUSY)`.
The wait therefore optimises the common case (holds of milliseconds)
and degrades to the current behaviour on a slow holder, with a cost
bounded by `lock-wait` of thread time per busy event.

What the waiting thread gives up: it cannot process another event
meanwhile. What it saves: two row writes, the trip to the back of the
backlog, and a second deserialization. The thread spends comparable
milliseconds today on the release and the next claim.

### 2. Configuration

- `event-outboxer.event-types.defaults.lock-wait` (thin-merged per
  type like every other `EventTypeConfig` field). `0` = today's
  behaviour, no waiting.
- Proposed default: **100 ms** — covers handlers in the millisecond
  range and gives up quickly on slow ones. **A hypothesis, to be set
  by measurement** (see Validation), not by taste.
- Validation in `EventTypeConfig`: `lock-wait < handler-max-runtime`.
  The wait runs inside the in-flight window and spends the watchdog's
  budget.
- The wait loop must return promptly on a watchdog interrupt and on
  engine shutdown, so a graceful stop never waits out `lock-wait`
  for every queued event.

### 3. SPI shape

```java
public interface EntityLocker {
    Optional<LockHandle> tryLock(String key, Duration ttl);           // unchanged

    /**
     * Like tryLock, but keeps trying for up to maxWait before giving up.
     * Default: polls tryLock with a short back-off; adapters override
     * where the backend can wait natively.
     */
    default Optional<LockHandle> tryLock(String key, Duration ttl, Duration maxWait) { ... }
}
```

- The default implementation polls `tryLock` every 5–10 ms with
  `Thread.sleep`, checking the interrupt flag. Every existing adapter
  and every third-party adapter gets the wait for free.
- `PgAdvisoryLocker` overrides with a native blocking
  `pg_advisory_lock` under a statement timeout equal to `maxWait`
  (the connection is pinned anyway).
- `PgLeaseEntityLocker` keeps the default: each probe is the same
  autocommit upsert, cheap at a 5–10 ms cadence, and bounded by
  `maxWait`. The `setQueryTimeout` lease-shortening note in ADR-0022
  applies per probe, not to the whole wait.
- `RedisEntityLocker` keeps the default polling in the first cut.
  A pub/sub wake-up (release script `PUBLISH`es, waiters subscribe)
  is a possible refinement with no new dependency.
- No fairness among waiters is promised — none of the backends offer
  it, and per-key ordering is not part of the outbox contract.

### 4. Observability

The existing `LockAcquisitionInfo` grows a `waited` duration so the
Micrometer listener can publish a timer of time spent waiting and the
share of acquisitions that succeeded after a wait; the `BUSY` outcome
keeps meaning "gave up after `lock-wait`".

## Validation plan

The benchmark harness (ADR-0034) exists for exactly this decision:

1. `hot-key` preset against the same standalone PostgreSQL with
   `lock-wait` = 0, 20, 100 and 500 ms. Success criteria: `lost = 0`,
   `lockOverlaps = 0`, handled/s and writes/event moving towards the
   `noop` baseline (612/s, 3.00) without exceeding it — the locker
   must still serialize.
2. The same at `--bench.workers=1` to separate in-JVM from cross-JVM
   collisions.
3. `throughput` (no lock key) before/after, to confirm the change is
   invisible where no key is declared.
4. `crash` preset: after the kill, the survivors' busy hits against
   the dead holder's lease must fall back to the release path within
   `lock-wait`, not stall the type until `lock-ttl`.

Independently of this ADR, a cheaper experiment is worth running
first: `hot-key` with `claim-batch-size=3` and
`handler-queue-capacity=0`, i.e. no claimed inventory beyond the
thread count. It measures mechanism 3 alone and may inform the
defaults recommended for hot-key workloads.

The executor comparison of 2026-09-04
([session](../benchmarks/2026-09-04-laptop-executors.md)) measured the
other side of the same mechanism: with `handler-executor.type:
virtual` every claimed event of the in-flight budget dispatches at
once, so 103 dispatches of a type attempt eight keys simultaneously
instead of three. Busy hits went from 0.75 to 7.7 per event, row
writes from 6.6 to 21, throughput from 237/s to 96/s. A bounded wait
would turn most of those attempts into a short park instead of a
release-and-reclaim round trip; the session is a fifth validation
cell for this ADR, and until it lands, virtual threads on keyed types
should be configured with the concurrency close to the live key count.

## Consequences

**Users.** Contended keys get faster and cheaper on the database with
no API change; `lock-wait: 0` restores today's semantics. Per-type
tuning follows the existing thin-merge pattern.

**Maintainers.** ADR-0012's worker-side flow gains one step (wait,
then release); the amendment goes into ADR-0012 when this ADR is
accepted. Third-party `EntityLocker` implementations keep working
through the default method.

**Operations.** A new timer to watch; a large `lock-wait` next to a
slow hot handler shows up as a saturated pool and a stalled type, the
exact symptom the bound is there to limit.

**Negative.** Thread time is spent waiting where today it would be
spent on other keys; on workloads with one slow hot key and many cool
ones a non-zero default can cost throughput until tuned down. The
bound and the per-type override are the mitigation, and the
validation plan must include such a mixed workload before the default
is fixed.

## Related decisions

- [ADR-0012](0012-extract-lock-key-on-handler.md) — the busy-means-
  reschedule flow this ADR extends; to be amended on acceptance.
- [ADR-0022](0022-lease-table-postgres-entity-locker.md) — the lease
  locker whose probe is the default wait's building block.
- [ADR-0004](0004-per-event-type-worker-isolation.md) — capacity-
  coupled polling, which turns unbounded waiting into a stalled type.
- [ADR-0034](0034-benchmark-and-invariant-harness.md) — the harness
  that produced the evidence and will grade the implementation.
