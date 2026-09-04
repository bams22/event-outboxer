# ADR-0035: Bounded wait for a busy entity lock

## Status

Accepted — design agreed 2026-09-04; implemented the same day behind a
`lock-wait: 0` default, then validated on the benchmark harness (see
§Validation results) and accepted with the default fixed at **100 ms**.
[ADR-0012](0012-extract-lock-key-on-handler.md) is amended
accordingly (worker-side flow: wait, then release).

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

## Decision

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
- Default: **100 ms** — covers handlers in the millisecond range and
  gives up quickly on slow ones. Proposed as a hypothesis and confirmed
  by the validation session below: the code shipped with `0` until the
  session had run, so the value is set by measurement, not by taste.
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
  applies per probe, not to the whole wait. A `LISTEN/NOTIFY` wake-up
  was built, measured and removed on 2026-09-05 — no gain, a commit
  serialization trap, unusable behind pgBouncer transaction pooling;
  see the ADR-0022 amendment before trying again.
- `RedisEntityLocker` kept the default polling in the first cut; on
  2026-09-05 it gained the pub/sub wake-up (release script `PUBLISH`es
  on the key's channel, waiters subscribe through a second Lettuce
  connection and park; a 25 ms fallback probe covers lost messages and
  expiry). The starter opens that connection next to its command
  connection (ADR-0027 amendment); `lock.wakeup: false` restores
  polling.
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

## Validation results (2026-09-04)

Recorded in the
[lock-wait session](../benchmarks/2026-09-04-laptop-lock-wait.md);
same laptop and standalone PostgreSQL 15 / Redis 7 containers as the
earlier sessions, one run per cell, so read cells against each other.
Every cell passed its invariants: `lost = 0`, `lockOverlaps = 0`,
storage clean. The busy-hit figure is the `release` statement count per
event.

| Cell | `lock-wait` | handled/s | busy hits/event | PG writes/event | e2e p50 / p95 |
|---|---|---|---|---|---|
| `hot-key`, lease, 3 workers | 0 | 256 | 0.79 | 6.65 | 4.6 s / 13.1 s |
| | 20 ms | 253 | 0.04 | 5.16 | 5.5 s / 9.6 s |
| | **100 ms** | **337** | **0.00** | **5.04** | 4.7 s / 8.3 s |
| | 500 ms | 294 | 0.00 | 5.08 | 5.5 s / 9.8 s |
| `hot-key`, lease, 1 worker | 0 / 100 ms | 137 / 141 | 0.00 / 0.00 | 5.00 / 5.00 | 14.6 s / 27 s both |
| `throughput`, no key | 0 / 100 ms | 1 496 / 1 311 | — | 3.00 / 3.00 | 51 ms / 92–94 ms |
| `hot-key`, Redis locker | 0 / 100 ms | 315 / 412 | 1.19 / 0.00 | 5.38 / **3.01** | 1.4 s / 7.2 s → 2.9 s / 4.8 s |
| mixed: 64 keys, 2 % on a 200 ms key | 0 | 80 | 0.23 | 5.48 | 3.3 s / 5.9 s (p99 12.7 s, max 57 s) |
| | 100 ms | 180 | 0.04 | 5.09 | 3.9 s / 7.1 s (p99 7.5 s, max 24 s) |
| | 500 ms | 219 | 0.01 | 5.04 | 5.2 s / 10.1 s |
| `crash`, lease, forked fleet | 0 / 100 ms | 175 / 190 | 0.66 / 0.10 | 6.44 / 5.34 | 13.8 s / 25 s → 16.0 s / 27 s |
| `hot-key`, **virtual** executor, uncapped | 0 / 100 ms | 89 / **53** | 7.7 / 7.4 | 21.1 / 20.9 | 17 s / 36 s → 44 s / 87 s |

What the cells say:

1. **The wait removes the round trip.** At 100 ms the lease locker's
   write cost drops to its floor of 5.00 rows per event (the lease
   insert and delete) and the Redis locker's to the engine's floor of
   3.00; busy hits per event go from 0.8–1.2 to zero. Drain rate rises
   by about a third with either locker. 20 ms already removes 95 % of
   the busy hits but not the throughput gap — the remaining releases
   still land at the back of the backlog; 500 ms gains nothing over
   100 ms on 5 ms holds.
2. **Cross-JVM collisions are the whole story.** One worker alone
   produces two busy hits in 5 000 events with or without the wait;
   the local key-affinity alternative would have had nothing to do.
3. **Invisible where no key is declared.** `throughput` is unchanged
   in cost and shape; the rate difference is run-to-run noise
   (the publisher was slower by the same amount).
4. **The mixed workload did not produce the feared regression.** With
   a 200 ms holder next to 63 cool keys, `lock-wait: 0` is the worst
   cell (80/s, max latency 57 s): every collision on the slow key costs
   a 1 s delay plus a trip to the back of the backlog. 100 ms more than
   doubles the drain rate at the price of 0.6 s on the cool keys'
   median; 500 ms chains waiters behind the slow holder and is faster
   still, but the cool keys pay 2 s of median latency for it — the
   trade-off the default has to strike, and 100 ms strikes it.
5. **After a crash the wait degrades as designed.** Survivors probing
   the dead holder's lease give up after 100 ms and fall back to the
   release path: busy cycles drop from 0.66 to 0.10 per event, nothing
   stalls until `lock-ttl`, duplicates stay attributable to the kill.
6. **The one loser: an uncapped virtual executor on hot keys.** With
   every claimed event dispatching at once, ~5 000 waiters poll eight
   keys every 2–10 ms; the probes (35 statements per event) cost more
   than the round trips they save and the cell gets slower (89 → 53/s).
   The executor session already required capping the in-flight budget
   near the key count on keyed virtual types; this ADR makes that a
   documented rule rather than a new mechanism. A wake-up instead of
   polling removes the probe cost where probes are what costs: done
   for the Redis locker on 2026-09-05 (pub/sub, measured in the
   [wake-up addendum](../benchmarks/2026-09-04-laptop-lock-wait.md#addendum-2026-09-05-redis-pubsub-wake-up));
   the lease locker's `LISTEN/NOTIFY` variant was built the same day,
   measured no gain — its probes are not what costs, its commits are —
   and was removed (ADR-0022 amendment).

## Consequences

**Users.** Contended keys get faster and cheaper on the database with
no API change; `lock-wait: 0` restores today's semantics. Per-type
tuning follows the existing thin-merge pattern. Upgrade note: a type
whose `handler-max-runtime` is 100 ms or less — a test configuration,
never a production one — now fails validation until its `lock-wait` is
lowered as well; the message names both knobs, and the testkit's own
defaults keep `lock-wait: 0` so contention surfaces immediately in
tests.

**Maintainers.** ADR-0012's worker-side flow gains one step (wait,
then release; amended 2026-09-04). Third-party `EntityLocker`
implementations keep working through the default method. The
dispatcher consumes a watchdog interrupt before the busy-path release
and skips storage on a foreign (`shutdownNow`) interrupt, since an
interrupted JDBC call can kill a pooled connection — the engine's
shutdown release returns the row.

**Operations.** A new timer to watch; a large `lock-wait` next to a
slow hot handler shows up as a saturated pool and a stalled type, the
exact symptom the bound is there to limit.

**Negative.** Thread time is spent waiting where today it would be
spent on other keys; on workloads with one slow hot key and many cool
ones the cool keys pay latency for it — measured at 0.6 s of median
on the mixed cell at 100 ms, 2 s at 500 ms — and an uncapped virtual
executor on a hot key turns the polling wait into a probe storm that
costs more than it saves. The bound, the per-type override and the
in-flight cap on keyed virtual types are the mitigation.

## Related decisions

- [ADR-0012](0012-extract-lock-key-on-handler.md) — the busy-means-
  reschedule flow this ADR extends; amended 2026-09-04.
- [ADR-0022](0022-lease-table-postgres-entity-locker.md) — the lease
  locker whose probe is the default wait's building block.
- [ADR-0004](0004-per-event-type-worker-isolation.md) — capacity-
  coupled polling, which turns unbounded waiting into a stalled type.
- [ADR-0034](0034-benchmark-and-invariant-harness.md) — the harness
  that produced the evidence and will grade the implementation.
